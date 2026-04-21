#!/usr/bin/env python3
"""Typing gate — mechanical enforcement of type discipline.

This gate blocks a PR that:

  1. Weakens the pyright configuration (gate-config ratchet).
  2. Introduces new ``Any`` in production code (escape-hatch ratchet).
  3. Introduces new ``# type: ignore`` / ``# pyright: ignore`` / ``# noqa``
     comments (escape-hatch ratchet).
  4. Introduces new ``cast(..., Any)`` / ``cast(Any, ...)`` calls.
  5. Increases the total pyright-strict error count (pyright-error ratchet).

The gate is a ratchet, not a flat hard-fail. The budget file at
``.github/typing_budget.json`` caps the total count of each escape-hatch
pattern and the pyright error count. Exceeding any cap fails the build.
Decreasing the cap is allowed — any PR may lower the numbers in the
budget to lock in improvements.

Usage:

  python tools/typing_gate.py                       # run all gates
  python tools/typing_gate.py --pyright-json <path> # include pyright gate
  python tools/typing_gate.py --update-budget       # regenerate the budget

Exit codes:

  0 — all gates pass
  1 — one or more gates failed
  2 — gate itself could not run (missing config, file-system error, etc.)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path
from typing import Final

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
BUDGET_PATH: Final[Path] = REPO_ROOT / '.github' / 'typing_budget.json'
PYPROJECT_PATH: Final[Path] = REPO_ROOT / 'pyproject.toml'


# -------------------------------------------------------------------
# File walking
# -------------------------------------------------------------------

def find_python_files(root: Path, excludes: list[str]) -> list[Path]:
    files: list[Path] = []
    for p in sorted(root.rglob('*.py')):
        rel = str(p.relative_to(REPO_ROOT))
        if any(ex in rel for ex in excludes):
            continue
        files.append(p)
    return files


def count_pattern(files: list[Path], pattern: str) -> int:
    rx = re.compile(pattern)
    total = 0
    for f in files:
        try:
            text = f.read_text(encoding='utf-8', errors='ignore')
        except OSError:
            continue
        total += len(rx.findall(text))
    return total


# -------------------------------------------------------------------
# GATE 1 — pyright config must be strict and must ban explicit Any
# -------------------------------------------------------------------

REQUIRED_PYRIGHT: Final[dict[str, object]] = {
    'typeCheckingMode': 'strict',
    'reportExplicitAny': 'error',
    'reportMissingImports': 'error',
    'reportMissingTypeStubs': 'error',
    'reportUnknownArgumentType': 'error',
    'reportUnknownMemberType': 'error',
    'reportUnknownVariableType': 'error',
    'reportUnknownLambdaType': 'error',
    'reportUnknownParameterType': 'error',
    'reportMissingParameterType': 'error',
    'reportConstantRedefinition': 'error',
    'reportImportCycles': 'error',
}

FORBIDDEN_VALUES: Final[frozenset[object]] = frozenset(
    {'none', 'warning', 'information', 'info', 'false', False}
)


def gate_pyright_config(config: dict[str, object]) -> list[str]:
    failures: list[str] = []
    tool = config.get('tool')
    pyright = tool.get('pyright') if isinstance(tool, dict) else None
    if not isinstance(pyright, dict):
        return ['[tool.pyright] section is missing from pyproject.toml']

    for key, required in REQUIRED_PYRIGHT.items():
        actual = pyright.get(key)
        if actual != required:
            failures.append(
                f'pyright.{key} must be {required!r}, got {actual!r}'
            )

    for key, value in pyright.items():
        if not isinstance(key, str):
            continue
        if key.startswith('report') and value in FORBIDDEN_VALUES:
            failures.append(
                f'pyright.{key} = {value!r} -- gate weakening disallowed; '
                f"must be 'error' (or absent to inherit strict default)"
            )

    return failures


# -------------------------------------------------------------------
# GATE 2 — escape-hatch pattern ratchet against committed budget
# -------------------------------------------------------------------

def gate_escape_hatch_ratchet(budget: dict[str, object]) -> list[str]:
    failures: list[str] = []
    package_root_name = str(budget.get('package_root', ''))
    if not package_root_name:
        return ['typing_budget.json must set package_root']
    package_root = REPO_ROOT / package_root_name
    if not package_root.is_dir():
        return [f'package_root {package_root_name!r} not found under repo root']

    excludes_raw = budget.get('excludes', [])
    excludes = [str(x) for x in excludes_raw] if isinstance(excludes_raw, list) else []
    files = find_python_files(package_root, excludes)

    patterns = budget.get('patterns')
    if not isinstance(patterns, dict):
        return ['typing_budget.json must define patterns']

    for name, spec in patterns.items():
        if not isinstance(spec, dict):
            continue
        pattern = str(spec.get('pattern', ''))
        if not pattern:
            continue
        budget_total = int(spec.get('total', 0))
        current = count_pattern(files, pattern)
        if current > budget_total:
            failures.append(
                f'[{name}] budget={budget_total} current={current} '
                f'(pattern={pattern!r}) -- ratchet exceeded. '
                f'Remove the new escape hatch or lower the budget.'
            )

    return failures


# -------------------------------------------------------------------
# GATE 3 — pyright total error count ratchet
# -------------------------------------------------------------------

def gate_pyright_errors(
    pyright_json_path: str | None,
    budget: dict[str, object],
) -> list[str]:
    if pyright_json_path is None:
        return []
    path = Path(pyright_json_path)
    if not path.is_file():
        return [f'pyright output not found at {path}']
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        return [f'pyright output is not valid JSON: {e}']

    summary = data.get('summary', {}) if isinstance(data, dict) else {}
    current_errors = int(summary.get('errorCount', 0))

    py_budget_raw = budget.get('pyright_errors')
    if not isinstance(py_budget_raw, dict):
        return [
            'typing_budget.json must include a pyright_errors section '
            "with {'total': <int>}"
        ]
    budget_total = int(py_budget_raw.get('total', 0))

    if current_errors > budget_total:
        # Per-rule summary for the failure message.
        per_rule: dict[str, int] = {}
        diagnostics = data.get('generalDiagnostics') if isinstance(data, dict) else None
        if isinstance(diagnostics, list):
            for diag in diagnostics:
                if not isinstance(diag, dict):
                    continue
                if diag.get('severity') != 'error':
                    continue
                rule = str(diag.get('rule', '<no-rule>'))
                per_rule[rule] = per_rule.get(rule, 0) + 1
        top = sorted(per_rule.items(), key=lambda kv: -kv[1])[:5]
        top_str = '; '.join(f'{r}={n}' for r, n in top)
        return [
            f'pyright errors: budget={budget_total} current={current_errors} '
            f'(delta={current_errors - budget_total}). Top rules: {top_str}'
        ]

    return []


# -------------------------------------------------------------------
# Baseline regeneration (--update-budget)
# -------------------------------------------------------------------

DEFAULT_PATTERNS: Final[dict[str, dict[str, object]]] = {
    'any_annotation':  {'pattern': r':\s*Any\b', 'total': 0},
    'any_return':      {'pattern': r'->\s*Any\b', 'total': 0},
    'any_import':      {'pattern': r'from typing import[^\n]*\bAny\b', 'total': 0},
    'cast_any':        {'pattern': r'cast\([^)]*\bAny\b', 'total': 0},
    'dict_any':        {'pattern': r'dict\[[^]]*\bAny\b', 'total': 0},
    'list_any':        {'pattern': r'list\[\s*Any\b', 'total': 0},
    'tuple_any':       {'pattern': r'tuple\[[^]]*\bAny\b', 'total': 0},
    'type_ignore':     {'pattern': r'#\s*type:\s*ignore', 'total': 0},
    'pyright_ignore':  {'pattern': r'#\s*pyright:\s*ignore', 'total': 0},
    'noqa':            {'pattern': r'#\s*noqa', 'total': 0},
}


def update_budget(pyright_json_path: str | None) -> None:
    if BUDGET_PATH.exists():
        budget = json.loads(BUDGET_PATH.read_text())
    else:
        budget = {
            'schema_version': 1,
            'package_root': 'tdw_control_plane',
            'excludes': ['__pycache__', 'quickstart_etl_tests', 'build', 'dist'],
            'patterns': {k: dict(v) for k, v in DEFAULT_PATTERNS.items()},
            'pyright_errors': {'total': 0},
        }

    package_root = REPO_ROOT / str(budget['package_root'])
    excludes = [str(x) for x in budget.get('excludes', [])]
    files = find_python_files(package_root, excludes)

    for name, spec in budget['patterns'].items():
        spec['total'] = count_pattern(files, str(spec['pattern']))

    if pyright_json_path is not None:
        try:
            data = json.loads(Path(pyright_json_path).read_text())
            summary = data.get('summary', {})
            budget['pyright_errors']['total'] = int(summary.get('errorCount', 0))
        except (OSError, json.JSONDecodeError) as e:
            print(f'warning: could not read pyright output: {e}', file=sys.stderr)

    BUDGET_PATH.parent.mkdir(parents=True, exist_ok=True)
    BUDGET_PATH.write_text(json.dumps(budget, indent=2) + '\n')

    total_hatches = sum(int(s['total']) for s in budget['patterns'].values())
    pyright_total = int(budget.get('pyright_errors', {}).get('total', 0))
    print(f'budget updated -> {BUDGET_PATH.relative_to(REPO_ROOT)}')
    print(f'  escape-hatch occurrences: {total_hatches}')
    print(f'  pyright errors:           {pyright_total}')


# -------------------------------------------------------------------
# Orchestration
# -------------------------------------------------------------------

def load_pyproject() -> dict[str, object]:
    with open(PYPROJECT_PATH, 'rb') as f:
        return tomllib.load(f)


def load_budget() -> dict[str, object]:
    if not BUDGET_PATH.exists():
        raise SystemExit(
            f'typing gate cannot run: {BUDGET_PATH.relative_to(REPO_ROOT)} '
            f'is missing. Run with --update-budget to create it.'
        )
    return json.loads(BUDGET_PATH.read_text())


def main() -> int:
    parser = argparse.ArgumentParser(description='Typing gate')
    parser.add_argument(
        '--pyright-json',
        default=None,
        help='Path to pyright --outputjson output; enables pyright-error ratchet',
    )
    parser.add_argument(
        '--update-budget',
        action='store_true',
        help='Regenerate .github/typing_budget.json from current repo state',
    )
    args = parser.parse_args()

    if args.update_budget:
        update_budget(args.pyright_json)
        return 0

    try:
        config = load_pyproject()
        budget = load_budget()
    except SystemExit:
        raise
    except Exception as e:
        print(f'typing gate setup failed: {e}', file=sys.stderr)
        return 2

    failures: list[tuple[str, str]] = []

    for msg in gate_pyright_config(config):
        failures.append(('pyright-config', msg))

    for msg in gate_escape_hatch_ratchet(budget):
        failures.append(('escape-hatch-ratchet', msg))

    for msg in gate_pyright_errors(args.pyright_json, budget):
        failures.append(('pyright-error-ratchet', msg))

    if failures:
        print('TYPING GATE -- FAIL')
        print('')
        by_gate: dict[str, list[str]] = {}
        for gate, msg in failures:
            by_gate.setdefault(gate, []).append(msg)
        for gate_name, msgs in by_gate.items():
            print(f'  gate: {gate_name}')
            for m in msgs:
                print(f'    - {m}')
        print('')
        print(f'{len(failures)} failure(s). Merge blocked.')
        return 1

    print('TYPING GATE -- PASS')
    return 0


if __name__ == '__main__':
    sys.exit(main())

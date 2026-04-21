#!/usr/bin/env python3
"""Typing gate — mechanical enforcement of type discipline.

This gate blocks a PR that:

  1. Weakens the pyright configuration (gate-config ratchet).
  2. Adds a ``pyrightconfig.json`` anywhere in the repo (pyright would
     prefer it over ``pyproject.toml`` and bypass the strict config).
  3. Changes ``pyright.include`` away from the required package list.
  4. Introduces new ``Any`` in production code (escape-hatch ratchet).
  5. Introduces new ``# type: ignore`` / ``# pyright: ignore`` / ``# noqa``
     comments (escape-hatch ratchet).
  6. Introduces new ``cast(..., Any)`` / ``cast(Any, ...)`` calls.
  7. Increases the total pyright-strict error count (pyright-error ratchet).
  8. Raises ANY budget value compared to the protected base ref
     (budget-source ratchet — the oracle cannot be weakened by the
     same PR it gates).
  9. Deletes a pattern key from the budget (same bypass class as #8).
  10. Reports ``filesAnalyzed`` below the number of Python files under the
      package root (shrinking the analysis surface is a trivial bypass).

The gate is a ratchet, not a flat hard-fail. The budget file at
``.github/typing_budget.json`` caps the total count of each escape-hatch
pattern and the pyright error count. Exceeding any cap fails the build.
Decreasing the cap is allowed — a PR may lower the numbers to lock in
improvements — but all decreases are checked against the base-ref
budget so the oracle cannot be weakened in the same PR that gates.

Usage:

  python tools/typing_gate.py                                # head-only checks
  python tools/typing_gate.py --pyright-json <path>          # + pyright ratchet
  python tools/typing_gate.py --base-budget <path>           # + base-vs-head ratchet
  python tools/typing_gate.py --update-budget [--pyright-json <path>]

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

def _is_excluded(relative_path: Path, excludes: list[str]) -> bool:
    """Path-part match, not substring. An exclude entry matches only if
    its parts appear as a contiguous slice of the path's parts. This
    prevents an entry like 'dist' from spuriously matching a file such
    as 'tdw_control_plane/distance.py'."""
    parts = relative_path.parts
    for ex in excludes:
        ex_parts = Path(ex).parts
        if not ex_parts:
            continue
        w = len(ex_parts)
        for i in range(0, max(0, len(parts) - w + 1)):
            if parts[i:i + w] == ex_parts:
                return True
    return False


def find_python_files(root: Path, excludes: list[str]) -> list[Path]:
    files: list[Path] = []
    for p in sorted(root.rglob('*.py')):
        rel = p.relative_to(REPO_ROOT)
        if _is_excluded(rel, excludes):
            continue
        files.append(p)
    return files


def count_pattern(files: list[Path], pattern: str) -> int:
    """Count non-overlapping matches of `pattern` across `files`. Any
    read or decode error is a fatal setup failure (not a silent skip);
    a silently skipped file would under-count escape hatches and let
    regressions through."""
    try:
        rx = re.compile(pattern)
    except re.error as exc:
        raise SystemExit(
            f'typing_gate: invalid regex in budget ({pattern!r}): {exc}'
        ) from exc
    total = 0
    for f in files:
        try:
            text = f.read_text(encoding='utf-8')
        except (OSError, UnicodeDecodeError) as exc:
            raise SystemExit(
                f'typing_gate: cannot read {f}: {exc}'
            ) from exc
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

# The `include` list pyright must analyze. Shrinking this to an empty
# list or a non-existent path drops filesAnalyzed to zero, trivially
# passing the error-count ratchet. Gate asserts exact match.
REQUIRED_PYRIGHT_INCLUDE: Final[list[str]] = ['tdw_control_plane']

FORBIDDEN_VALUES: Final[frozenset[object]] = frozenset(
    {'none', 'warning', 'information', 'info', 'false', False}
)


def gate_pyright_config(config: dict[str, object]) -> list[str]:
    failures: list[str] = []

    # Pyright prefers pyrightconfig.json over [tool.pyright] in pyproject.toml.
    # A PR that drops such a file anywhere in the repo silently shadows every
    # setting this gate audits. Ban it outright.
    for cfg in sorted(REPO_ROOT.rglob('pyrightconfig.json')):
        rel = cfg.relative_to(REPO_ROOT)
        # Skip vendored/dependency paths (.venv, node_modules, etc.)
        parts = rel.parts
        if any(p in {'.venv', 'venv', 'node_modules', 'build', 'dist', '.git'} for p in parts):
            continue
        failures.append(
            f'pyrightconfig.json found at {rel}. '
            f'Pyright prefers this file over pyproject.toml and would '
            f'bypass the strict [tool.pyright] config. Delete it.'
        )

    tool = config.get('tool')
    pyright = tool.get('pyright') if isinstance(tool, dict) else None
    if not isinstance(pyright, dict):
        failures.append('[tool.pyright] section is missing from pyproject.toml')
        return failures

    for key, required in REQUIRED_PYRIGHT.items():
        actual = pyright.get(key)
        if actual != required:
            failures.append(
                f'pyright.{key} must be {required!r}, got {actual!r}'
            )

    # `include` must match exactly. Shrinking it drops filesAnalyzed.
    actual_include = pyright.get('include')
    if actual_include != REQUIRED_PYRIGHT_INCLUDE:
        failures.append(
            f'pyright.include must be exactly {REQUIRED_PYRIGHT_INCLUDE!r}, '
            f'got {actual_include!r}. Changing this shrinks the analysis '
            f'surface and lets errors escape the gate.'
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
            return [f'typing_budget.json: pattern {name!r} must be an object']
        pattern = spec.get('pattern')
        if not isinstance(pattern, str) or not pattern:
            return [f'typing_budget.json: pattern {name!r} has no regex']
        raw_total = spec.get('total', 0)
        if isinstance(raw_total, bool) or not isinstance(raw_total, int):
            return [
                f'typing_budget.json: pattern {name!r} total must be a '
                f'non-negative integer (got {raw_total!r})'
            ]
        if raw_total < 0:
            return [
                f'typing_budget.json: pattern {name!r} total must be '
                f'non-negative (got {raw_total})'
            ]
        current = count_pattern(files, pattern)
        if current > raw_total:
            failures.append(
                f'[{name}] budget={raw_total} current={current} '
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

    if not isinstance(data, dict):
        return ['pyright output must be a JSON object']
    summary = data.get('summary', {})
    if not isinstance(summary, dict):
        return ['pyright output .summary must be an object']
    raw_current = summary.get('errorCount', 0)
    if isinstance(raw_current, bool) or not isinstance(raw_current, int):
        return [
            f'pyright output .summary.errorCount must be an integer '
            f'(got {raw_current!r})'
        ]
    current_errors = raw_current

    py_budget_raw = budget.get('pyright_errors')
    if not isinstance(py_budget_raw, dict):
        return [
            'typing_budget.json must include a pyright_errors section '
            "with {'total': <int>}"
        ]
    raw_budget_total = py_budget_raw.get('total', 0)
    if isinstance(raw_budget_total, bool) or not isinstance(raw_budget_total, int):
        return [
            f'typing_budget.json: pyright_errors.total must be a '
            f'non-negative integer (got {raw_budget_total!r})'
        ]
    if raw_budget_total < 0:
        return [
            f'typing_budget.json: pyright_errors.total must be '
            f'non-negative (got {raw_budget_total})'
        ]
    budget_total = raw_budget_total

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
# GATE 4 — budget-source ratchet. The PR's own budget (``head``) must
# not raise any value and must not delete any pattern, compared to
# the budget at the protected base ref. Without this, a PR can raise
# its own ceiling and pass.
# -------------------------------------------------------------------

def gate_budget_source(
    base_budget_path: str | None,
    head_budget: dict[str, object],
) -> list[str]:
    if base_budget_path is None:
        # First-time run or unable to fetch base; the workflow must
        # provide --base-budget in PR / merge_group / push-to-main.
        # Absence here is a config error, not a graceful skip.
        return [
            'typing_gate: --base-budget was not provided; '
            'the workflow must fetch the budget from the protected base ref '
            'and pass it as --base-budget'
        ]

    base_path = Path(base_budget_path)
    if not base_path.is_file():
        # Base ref has no budget file — this is the first commit that
        # introduces the gate. In that case head is the baseline; nothing
        # to compare against.
        print(
            f'typing_gate: base-ref budget not found at {base_path} '
            f'(first commit introducing the gate?); skipping '
            f'budget-source ratchet',
            file=sys.stderr,
        )
        return []

    try:
        base_budget = json.loads(base_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return [f'typing_gate: cannot read base budget {base_path}: {exc}']

    if not isinstance(base_budget, dict):
        return [f'typing_gate: base budget {base_path} is not a JSON object']

    failures: list[str] = []

    # Pyright error ceiling
    base_py = base_budget.get('pyright_errors')
    head_py = head_budget.get('pyright_errors')
    if isinstance(base_py, dict) and isinstance(head_py, dict):
        b_total = base_py.get('total', 0)
        h_total = head_py.get('total', 0)
        if (
            isinstance(b_total, int)
            and not isinstance(b_total, bool)
            and isinstance(h_total, int)
            and not isinstance(h_total, bool)
            and h_total > b_total
        ):
            failures.append(
                f'pyright_errors.total was raised from {b_total} (base) '
                f'to {h_total} (head). The oracle cannot be weakened '
                f'by the PR it gates.'
            )

    # Pattern ceilings and key preservation
    base_patterns_raw = base_budget.get('patterns')
    head_patterns_raw = head_budget.get('patterns')
    base_patterns: dict[str, object] = (
        base_patterns_raw if isinstance(base_patterns_raw, dict) else {}
    )
    head_patterns: dict[str, object] = (
        head_patterns_raw if isinstance(head_patterns_raw, dict) else {}
    )

    for key in base_patterns:
        if key not in head_patterns:
            failures.append(
                f'pattern {key!r} was deleted from the budget. '
                f'Keys present in the base-ref budget must be preserved.'
            )

    for key, base_spec in base_patterns.items():
        if key not in head_patterns:
            continue
        head_spec = head_patterns[key]
        if not (isinstance(base_spec, dict) and isinstance(head_spec, dict)):
            continue
        b_total = base_spec.get('total', 0)
        h_total = head_spec.get('total', 0)
        if (
            isinstance(b_total, int)
            and not isinstance(b_total, bool)
            and isinstance(h_total, int)
            and not isinstance(h_total, bool)
            and h_total > b_total
        ):
            failures.append(
                f'pattern {key!r} total was raised from {b_total} (base) '
                f'to {h_total} (head). Only decreases are allowed.'
            )

    return failures


# -------------------------------------------------------------------
# GATE 5 — pyright filesAnalyzed must match the number of Python
# files under the package root. Shrinking include (or hiding files
# behind exclude) trivially drops filesAnalyzed to a smaller set and
# lets errors escape.
# -------------------------------------------------------------------

def gate_files_analyzed(
    pyright_json_path: str | None,
    budget: dict[str, object],
) -> list[str]:
    if pyright_json_path is None:
        return []
    path = Path(pyright_json_path)
    if not path.is_file():
        return []

    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        return [f'pyright output is not valid JSON: {e}']
    if not isinstance(data, dict):
        return ['pyright output must be a JSON object']
    summary = data.get('summary', {})
    if not isinstance(summary, dict):
        return ['pyright output .summary must be an object']

    raw_analyzed = summary.get('filesAnalyzed', 0)
    if isinstance(raw_analyzed, bool) or not isinstance(raw_analyzed, int):
        return [
            f'pyright output .summary.filesAnalyzed must be an integer '
            f'(got {raw_analyzed!r})'
        ]
    analyzed = raw_analyzed

    package_root_name = str(budget.get('package_root', ''))
    if not package_root_name:
        return ['typing_budget.json must set package_root']
    package_root = REPO_ROOT / package_root_name
    excludes_raw = budget.get('excludes', [])
    excludes = [str(x) for x in excludes_raw] if isinstance(excludes_raw, list) else []
    expected = len(find_python_files(package_root, excludes))

    if analyzed < expected:
        return [
            f'pyright filesAnalyzed={analyzed} but the package has '
            f'{expected} Python files. The analysis surface was '
            f'shrunk (likely via pyright.include or pyright.exclude) '
            f'and errors are hidden outside the analyzed set.'
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
        '--base-budget',
        default=None,
        help=(
            'Path to the budget JSON at the protected base ref. Without '
            'this, the gate cannot check whether the head PR has raised '
            'its own ceiling.'
        ),
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

    for msg in gate_budget_source(args.base_budget, budget):
        failures.append(('budget-source-ratchet', msg))

    for msg in gate_escape_hatch_ratchet(budget):
        failures.append(('escape-hatch-ratchet', msg))

    for msg in gate_pyright_errors(args.pyright_json, budget):
        failures.append(('pyright-error-ratchet', msg))

    for msg in gate_files_analyzed(args.pyright_json, budget):
        failures.append(('files-analyzed-ratchet', msg))

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

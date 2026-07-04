from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
from types import ModuleType
from typing import Final

REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[2]

TYPING_BUDGET: Final[dict[str, object]] = {
    'schema_version': 2,
    'package_root': 'tools',
    'excludes': [],
    'patterns': {},
    'pyright_errors': {'total': 5},
    'any_references': {'total': 0},
}

FAIL_LOUD_BUDGET: Final[dict[str, object]] = {
    'schema_version': 1,
    'package_root': 'tools',
    'excludes': [],
    'categories': {'bare_except': {'total': 2}},
}


def _load(module_name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        module_name, REPO_ROOT / 'tools' / f'{module_name}.py'
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _budget(template: dict[str, object], package_root: str) -> dict[str, object]:
    budget = copy.deepcopy(template)
    budget['package_root'] = package_root
    return budget


def _absent_root(tmp_path: Path) -> str:
    root = f'renamed_away_{tmp_path.name}'
    assert not (REPO_ROOT / root).is_dir()
    return root


def _root_failures(failures: list[str]) -> list[str]:
    return [failure for failure in failures if 'package_root changed' in failure]


def test_typing_gate_allows_root_rename_when_base_root_absent(tmp_path: Path) -> None:
    typing_gate = _load('typing_gate')
    base_path = tmp_path / 'base_budget.json'
    base_path.write_text(json.dumps(_budget(TYPING_BUDGET, _absent_root(tmp_path))))

    failures = typing_gate.gate_budget_source(
        str(base_path), False, _budget(TYPING_BUDGET, 'tools')
    )

    assert _root_failures(failures) == []


def test_typing_gate_blocks_root_change_when_base_root_present(tmp_path: Path) -> None:
    typing_gate = _load('typing_gate')
    base_path = tmp_path / 'base_budget.json'
    base_path.write_text(json.dumps(_budget(TYPING_BUDGET, 'tools')))

    failures = typing_gate.gate_budget_source(
        str(base_path), False, _budget(TYPING_BUDGET, 'tests')
    )

    assert len(_root_failures(failures)) == 1


def test_typing_gate_blocks_root_rename_with_total_change(tmp_path: Path) -> None:
    typing_gate = _load('typing_gate')
    base_path = tmp_path / 'base_budget.json'
    base_path.write_text(json.dumps(_budget(TYPING_BUDGET, _absent_root(tmp_path))))
    head_budget = _budget(TYPING_BUDGET, 'tools')
    head_budget['pyright_errors'] = {'total': 4}

    failures = typing_gate.gate_budget_source(str(base_path), False, head_budget)

    assert any('totals-neutral' in failure for failure in _root_failures(failures))


def test_fail_loud_gate_allows_root_rename_when_base_root_absent(tmp_path: Path) -> None:
    fail_loud_gate = _load('fail_loud_gate')

    failures = fail_loud_gate.gate(
        _budget(FAIL_LOUD_BUDGET, 'tools'),
        _budget(FAIL_LOUD_BUDGET, _absent_root(tmp_path)),
    )

    assert _root_failures(failures) == []


def test_fail_loud_gate_blocks_root_change_when_base_root_present() -> None:
    fail_loud_gate = _load('fail_loud_gate')

    failures = fail_loud_gate.gate(
        _budget(FAIL_LOUD_BUDGET, 'tests'),
        _budget(FAIL_LOUD_BUDGET, 'tools'),
    )

    assert len(_root_failures(failures)) == 1


def test_fail_loud_gate_blocks_root_rename_with_total_change(tmp_path: Path) -> None:
    fail_loud_gate = _load('fail_loud_gate')
    head_budget = _budget(FAIL_LOUD_BUDGET, 'tools')
    head_budget['categories'] = {'bare_except': {'total': 1}}

    failures = fail_loud_gate.gate(
        head_budget,
        _budget(FAIL_LOUD_BUDGET, _absent_root(tmp_path)),
    )

    assert any('totals-neutral' in failure for failure in _root_failures(failures))

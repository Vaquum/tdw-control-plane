from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import types
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
AUDIT_TOOL = REPO_ROOT / 'tools/privileged_ruleset_audit.py'
RULESET_WORKFLOW = REPO_ROOT / '.github/workflows/pr_checks_ruleset.yml'
AUDIT_WORKFLOW = REPO_ROOT / '.github/workflows/audit_main_ruleset.yml'
SNAPSHOT = REPO_ROOT / '.github/rulesets/main.json'
FIXTURES = REPO_ROOT / 'tests/fixtures/github'


def _load_audit_module() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location('privileged_ruleset_audit', AUDIT_TOOL)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_ruleset_gate_module() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location('ruleset_gate_independent', REPO_ROOT / 'tools/ruleset_gate.py')
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_audit_fixture(live_fixture: str, tmp_path: Path) -> tuple[int, str, str]:
    module = _load_audit_module()
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        try:
            code = module.run_audit(
                ruleset_file=str(SNAPSHOT),
                repo=None,
                ruleset_id=5406599,
                output_dir=str(tmp_path),
                live_json=str(FIXTURES / live_fixture),
            )
        except SystemExit as exc:
            code = int(exc.code)
    return code, stdout.getvalue(), stderr.getvalue()


def test_privileged_ruleset_audit_accepts_matching_live_payload_with_bypass_actors(
    tmp_path: Path,
) -> None:
    code, stdout, stderr = _run_audit_fixture('ruleset_live_target.json', tmp_path)

    assert code == 0
    assert 'PRIVILEGED RULESET AUDIT -- PASS' in stdout
    assert stderr == ''
    assert not (tmp_path / 'live_ruleset.json').exists()


def test_privileged_ruleset_audit_fails_on_bypass_actor_drift(tmp_path: Path) -> None:
    code, _, stderr = _run_audit_fixture('ruleset_live_with_bypass_actor.json', tmp_path)

    assert code == 1
    assert 'privileged_ruleset_audit: ruleset drift detected' in stderr


def test_privileged_ruleset_audit_writes_live_payload_snapshot_on_failure(
    tmp_path: Path,
) -> None:
    code, _, stderr = _run_audit_fixture('ruleset_live_with_bypass_actor.json', tmp_path)
    snapshot = tmp_path / 'live_ruleset.json'

    assert code == 1
    assert snapshot.exists()
    assert json.loads(snapshot.read_text(encoding='utf-8')) == json.loads(
        (FIXTURES / 'ruleset_live_with_bypass_actor.json').read_text(encoding='utf-8')
    )
    assert str(snapshot) in stderr


def test_privileged_ruleset_audit_fails_loud_when_live_payload_omits_bypass_actors(
    tmp_path: Path,
) -> None:
    code, _, stderr = _run_audit_fixture(
        'ruleset_live_target_without_bypass_actors.json',
        tmp_path,
    )
    snapshot = tmp_path / 'live_ruleset.json'

    assert code == 2
    assert (
        "privileged live ruleset missing required observable field(s): ['bypass_actors']"
        in stderr
    )
    assert snapshot.exists()
    assert json.loads(snapshot.read_text(encoding='utf-8')) == json.loads(
        (FIXTURES / 'ruleset_live_target_without_bypass_actors.json').read_text(
            encoding='utf-8'
        )
    )


def test_privileged_ruleset_audit_shares_ruleset_gate_semantics() -> None:
    module = _load_audit_module()
    ruleset_gate = _load_ruleset_gate_module()

    assert (
        module.shared_ruleset_gate.REQUIRED_TOP_LEVEL_FIELDS
        == ruleset_gate.REQUIRED_TOP_LEVEL_FIELDS
    )
    assert (
        module.shared_ruleset_gate.OPTIONAL_LIVE_TOP_LEVEL_FIELDS
        == ruleset_gate.OPTIONAL_LIVE_TOP_LEVEL_FIELDS
    )
    assert module.shared_ruleset_gate.SNAPSHOT_TOP_LEVEL_FIELDS == ruleset_gate.SNAPSHOT_TOP_LEVEL_FIELDS
    assert module.shared_ruleset_gate.IGNORED_LIVE_FIELDS == ruleset_gate.IGNORED_LIVE_FIELDS


def test_audit_main_ruleset_workflow_contract() -> None:
    workflow = AUDIT_WORKFLOW.read_text(encoding='utf-8')

    assert 'name: audit_main_ruleset' in workflow
    assert 'push:' in workflow
    assert 'branches: [main]' in workflow
    assert 'workflow_dispatch:' in workflow
    assert 'pull_request:' not in workflow
    assert 'RULESET_AUDIT_TOKEN' in workflow
    assert 'tools/privileged_ruleset_audit.py' in workflow
    assert '--ruleset-file .github/rulesets/main.json' in workflow
    assert '--repo "${{ github.repository }}"' in workflow
    assert '--ruleset-id 5406599' in workflow
    assert '--output-dir .ruleset-audit' in workflow
    assert 'actions/upload-artifact@v4' in workflow
    assert "failure()" in workflow
    assert '.ruleset-audit' in workflow


def test_pr_checks_ruleset_runs_privileged_audit_contract() -> None:
    workflow = RULESET_WORKFLOW.read_text(encoding='utf-8')

    assert 'tests/tools/test_privileged_ruleset_audit.py' in workflow
    assert 'continue-on-error' not in workflow

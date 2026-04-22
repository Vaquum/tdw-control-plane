from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RULESET_GATE = REPO_ROOT / 'tools/ruleset_gate.py'
SNAPSHOT = REPO_ROOT / '.github/rulesets/main.json'
FIXTURES = REPO_ROOT / 'tests/fixtures/github'


def run_gate(live_fixture: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(RULESET_GATE),
            '--live-json',
            str(FIXTURES / live_fixture),
            '--ruleset-file',
            str(SNAPSHOT),
            '--ruleset-id',
            '5406599',
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )


def test_matches_target_snapshot() -> None:
    result = run_gate('ruleset_live_target.json')
    assert result.returncode == 0, result.stderr
    assert 'RULESET GATE -- PASS' in result.stdout


def test_unexpected_top_level_field_in_live_is_drift() -> None:
    result = run_gate('ruleset_live_unexpected_field.json')
    assert result.returncode == 1
    assert 'unexpected live ruleset field(s)' in result.stderr

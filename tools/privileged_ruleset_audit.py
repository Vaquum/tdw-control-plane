#!/usr/bin/env python3
"""Privileged post-merge ruleset audit for full live parity on main."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import types
from pathlib import Path
from typing import Any

TOOLS_DIR = Path(__file__).resolve().parent

LIVE_PAYLOAD_SNAPSHOT = 'live_ruleset.json'


def _load_shared_ruleset_gate_module() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location('ruleset_gate', TOOLS_DIR / 'ruleset_gate.py')
    if spec is None or spec.loader is None:
        raise SystemExit('privileged_ruleset_audit: cannot load tools/ruleset_gate.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


shared_ruleset_gate = _load_shared_ruleset_gate_module()


def _write_live_payload_snapshot(output_dir: Path, payload: dict[str, Any]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / LIVE_PAYLOAD_SNAPSHOT
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    return path


def normalize_privileged_live_ruleset(payload: dict[str, Any]) -> dict[str, Any]:
    live_fields = set(payload)
    unexpected = (
        live_fields
        - shared_ruleset_gate.REQUIRED_TOP_LEVEL_FIELDS
        - shared_ruleset_gate.OPTIONAL_LIVE_TOP_LEVEL_FIELDS
        - shared_ruleset_gate.IGNORED_LIVE_FIELDS
    )
    if unexpected:
        raise SystemExit(
            shared_ruleset_gate.fail(
                f'unexpected live ruleset field(s): {sorted(unexpected)}',
                code=1,
            )
        )

    missing_required = shared_ruleset_gate.REQUIRED_TOP_LEVEL_FIELDS - live_fields
    if missing_required:
        raise SystemExit(
            shared_ruleset_gate.fail(
                f'expected live ruleset field(s) missing: {sorted(missing_required)}',
                code=1,
            )
        )

    missing_optional = shared_ruleset_gate.OPTIONAL_LIVE_TOP_LEVEL_FIELDS - live_fields
    if missing_optional:
        raise SystemExit(
            shared_ruleset_gate.fail(
                'privileged live ruleset missing required observable field(s): '
                f'{sorted(missing_optional)}',
                code=2,
            )
        )

    comparable_fields = (
        shared_ruleset_gate.REQUIRED_TOP_LEVEL_FIELDS
        | shared_ruleset_gate.OPTIONAL_LIVE_TOP_LEVEL_FIELDS
    )
    return {key: payload[key] for key in sorted(comparable_fields)}


def run_audit(
    *,
    ruleset_file: str,
    repo: str | None,
    ruleset_id: str,
    output_dir: str,
    live_json: str | None = None,
) -> int:
    expected_ruleset = shared_ruleset_gate.normalize_snapshot_ruleset(
        shared_ruleset_gate.load_json_file(Path(ruleset_file))
    )
    live_payload = shared_ruleset_gate.load_live_ruleset(
        repo=repo,
        ruleset_id=ruleset_id,
        live_json=live_json,
    )
    snapshot_dir = Path(output_dir)

    try:
        live_ruleset = normalize_privileged_live_ruleset(live_payload)
    except SystemExit:
        _write_live_payload_snapshot(snapshot_dir, live_payload)
        raise

    expected_comparable = {
        key: expected_ruleset[key]
        for key in sorted(live_ruleset)
    }

    if live_ruleset != expected_comparable:
        snapshot_path = _write_live_payload_snapshot(snapshot_dir, live_payload)
        print('privileged_ruleset_audit: ruleset drift detected', file=sys.stderr)
        print('expected:', file=sys.stderr)
        print(
            json.dumps(expected_comparable, indent=2, sort_keys=True),
            file=sys.stderr,
        )
        print('live:', file=sys.stderr)
        print(
            json.dumps(live_ruleset, indent=2, sort_keys=True),
            file=sys.stderr,
        )
        print(
            'privileged_ruleset_audit: wrote compared live payload to '
            f'{snapshot_path}',
            file=sys.stderr,
        )
        return 1

    print('PRIVILEGED RULESET AUDIT -- PASS')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description='Privileged ruleset audit')
    parser.add_argument('--ruleset-file', required=True)
    parser.add_argument('--repo', required=True)
    parser.add_argument('--ruleset-id', required=True, type=int)
    parser.add_argument('--output-dir', required=True)
    args = parser.parse_args()

    return run_audit(
        ruleset_file=args.ruleset_file,
        repo=args.repo,
        ruleset_id=str(args.ruleset_id),
        output_dir=args.output_dir,
    )


if __name__ == '__main__':
    sys.exit(main())

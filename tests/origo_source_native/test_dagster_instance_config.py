from __future__ import annotations

from pathlib import Path
from typing import Final

import yaml

from origo.assets.daily_trades_to_origo import insert_daily_binance_spot_trades_to_origo

REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
DAGSTER_YAML: Final[Path] = REPO_ROOT / 'dagster.yaml'


def _instance_config() -> dict[str, object]:
    loaded = yaml.safe_load(DAGSTER_YAML.read_text(encoding='utf-8'))
    assert isinstance(loaded, dict)
    return loaded


def test_run_monitoring_is_enabled_with_zombie_guards() -> None:
    config = _instance_config()
    run_monitoring = config['run_monitoring']
    assert isinstance(run_monitoring, dict)
    assert run_monitoring['enabled'] is True
    assert isinstance(run_monitoring['start_timeout_seconds'], int)
    assert run_monitoring['start_timeout_seconds'] >= 60
    assert isinstance(run_monitoring['max_runtime_seconds'], int)


def test_max_runtime_exceeds_daily_asset_retry_envelope() -> None:
    """A run legitimately held in STARTED by op retries must never be reaped.

    The daily ingestion assets retry in-process for max_retries * delay
    seconds; the global max runtime has to clear that envelope or run
    monitoring would kill runs that are behaving as designed.
    """
    config = _instance_config()
    run_monitoring = config['run_monitoring']
    assert isinstance(run_monitoring, dict)
    max_runtime = run_monitoring['max_runtime_seconds']
    assert isinstance(max_runtime, int)

    retry_policy = insert_daily_binance_spot_trades_to_origo.op.retry_policy
    assert retry_policy is not None
    assert retry_policy.delay is not None
    retry_envelope_seconds = int(retry_policy.max_retries * retry_policy.delay)
    assert max_runtime > retry_envelope_seconds


def test_run_retries_stay_disabled_until_replacement_is_atomic() -> None:
    """Tracker #275 item 21: retries re-run delete-then-insert partitions.

    Until partition replacement is atomic, an automatic run-level retry of
    a partially-applied run risks double-applied inserts, so run_retries
    must not appear in the instance config.
    """
    assert 'run_retries' not in _instance_config()

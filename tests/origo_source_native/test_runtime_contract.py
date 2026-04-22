from __future__ import annotations

from pathlib import Path

from tdw_control_plane.assets.create_origo_database import _get_clickhouse_settings

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_dockerfile_uses_python_311() -> None:
    assert (REPO_ROOT / 'Dockerfile').read_text(encoding='utf-8').startswith(
        'FROM python:3.11'
    )


def test_origo_clickhouse_settings_preserve_compose_defaults(monkeypatch) -> None:
    monkeypatch.delenv('CLICKHOUSE_HOST', raising=False)
    monkeypatch.delenv('CLICKHOUSE_PORT', raising=False)
    monkeypatch.delenv('CLICKHOUSE_USER', raising=False)
    monkeypatch.delenv('CLICKHOUSE_DATABASE', raising=False)
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'test-password')

    settings = _get_clickhouse_settings()

    assert settings.host == 'clickhouse'
    assert settings.port == 9000
    assert settings.user == 'default'
    assert settings.database == 'origo'

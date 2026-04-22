from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_dockerfile_uses_python_311() -> None:
    assert (REPO_ROOT / 'Dockerfile').read_text(encoding='utf-8').startswith(
        'FROM python:3.11'
    )

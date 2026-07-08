from __future__ import annotations

from pathlib import Path
from typing import Final

import yaml

REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
DEPLOY_COMPOSE: Final[Path] = REPO_ROOT / 'docker-compose.deploy.yml'

LOOPBACK: Final[str] = '127.0.0.1'


def _published_ports() -> list[tuple[str, str]]:
    """(service, port-mapping) for every published port in the deploy compose."""
    compose = yaml.safe_load(DEPLOY_COMPOSE.read_text(encoding='utf-8'))
    pairs: list[tuple[str, str]] = []
    for service, spec in compose['services'].items():
        for mapping in spec.get('ports', []):
            pairs.append((service, str(mapping)))
    return pairs


def test_all_published_ports_bind_loopback() -> None:
    # A published port with fewer than three colon-separated parts
    # (i.e. "HOST:CONTAINER" or bare "CONTAINER") binds every interface.
    # Loopback binding requires the explicit "127.0.0.1:HOST:CONTAINER" form.
    exposed = [
        f'{service}: {mapping}'
        for service, mapping in _published_ports()
        if not mapping.startswith(f'{LOOPBACK}:')
    ]
    assert exposed == []


def test_dagit_bound_to_loopback() -> None:
    dagit_ports = [mapping for service, mapping in _published_ports() if service == 'dagit']
    assert dagit_ports == [f'{LOOPBACK}:4000:3000']

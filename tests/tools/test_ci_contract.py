from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_post_merge_changelog_workflow_removed() -> None:
    assert not (REPO_ROOT / '.github/workflows/pr_post_changelog.yml').exists()


def test_update_changelog_script_removed() -> None:
    assert not (REPO_ROOT / 'scripts/update_changelog.py').exists()

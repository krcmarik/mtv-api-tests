from __future__ import annotations

import stat
import subprocess

import pytest
from pytest_testconfig import config as py_config
from simple_logger.logger import get_logger

LOGGER = get_logger(name=__name__)


@pytest.fixture(scope="session")
def upgrade_script_path(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Clone the mtv-autodeploy repo and return the absolute path to the upgrade script.

    Args:
        tmp_path_factory (pytest.TempPathFactory): Pytest factory for session-scoped temporary directories.

    Returns:
        str: Absolute path to the executable upgrade script.

    Raises:
        ValueError: If required config values are missing or the script is not found after cloning.
        subprocess.CalledProcessError: If the git clone command fails.
    """
    repo_url: str = py_config["upgrade_repo_url"]
    repo_ref: str = py_config["upgrade_repo_ref"]
    script_relative_path: str = py_config["upgrade_script_path"]

    if not repo_url:
        raise ValueError("upgrade_repo_url must be provided via --tc=upgrade_repo_url:<url>")
    if not repo_ref:
        raise ValueError("upgrade_repo_ref must be provided via --tc=upgrade_repo_ref:<ref>")
    if not script_relative_path:
        raise ValueError("upgrade_script_path must be provided via --tc=upgrade_script_path:<relative-path>")

    base_dir = tmp_path_factory.mktemp("mtv-autodeploy")
    clone_dir = base_dir / "repo"

    LOGGER.info(f"Cloning {repo_url} (ref={repo_ref}) into {clone_dir}")
    subprocess.run(
        ["git", "clone", "--depth", "1", "--branch", repo_ref, repo_url, str(clone_dir)],
        check=True,
    )

    script_path = clone_dir / script_relative_path
    if not script_path.is_file():
        raise ValueError(f"Upgrade script not found at '{script_path}' after cloning {repo_url}")

    current_mode = script_path.stat().st_mode
    script_path.chmod(current_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    return str(script_path)

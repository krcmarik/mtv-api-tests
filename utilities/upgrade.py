"""
Upgrade utilities for MTV operator upgrade tests.

This module provides functions to run the MTV upgrade process.
"""

from __future__ import annotations

import os
import subprocess

from pytest_testconfig import config as py_config
from simple_logger.logger import get_logger

from exceptions.exceptions import MtvUpgradeError

LOGGER = get_logger(name=__name__)


def run_mtv_upgrade(
    script_path: str,
    mtv_version: str,
    mtv_source: str,
    image_index: str = "",
) -> None:
    """Run the MTV operator upgrade using the specified upgrade script.

    Args:
        script_path (str): Full path to the upgrade script.
        mtv_version (str): Target MTV version to upgrade to.
        mtv_source (str): MTV source identifier (e.g., "brew", "released").
        image_index (str): Optional image index override for the upgrade.

    Raises:
        ValueError: If required parameters are empty or script is not found.
        MtvUpgradeError: If the upgrade script exits with a non-zero status or times out.
    """
    if not script_path:
        raise ValueError("script_path must be provided via --tc=upgrade_script_path:<path>")
    if not os.path.isfile(script_path):
        raise ValueError(f"Upgrade script not found at path: {script_path}")
    if not mtv_version:
        raise ValueError("mtv_version must be provided via --tc=mtv_upgrade_to_version:<version>")
    if not mtv_source:
        raise ValueError("mtv_source must be provided via --tc=mtv_upgrade_to_source:<source>")

    env = os.environ.copy()
    env.update({
        "MTV_VERSION": mtv_version,
        "MTV_SOURCE": mtv_source.upper(),
        "IMAGE_INDEX": image_index,
        "CLUSTER_USERNAME": py_config["cluster_username"],
        "CLUSTER_PASSWORD": py_config["cluster_password"],
        "CLUSTER_API_URL": py_config["cluster_host"],
    })

    LOGGER.info(f"Running MTV upgrade: {script_path} (version={mtv_version}, source={mtv_source}, index={image_index})")

    try:
        subprocess.run(
            [script_path],
            env=env,
            cwd=os.path.dirname(script_path),
            check=True,
            timeout=3600,
        )
    except subprocess.TimeoutExpired as exc:
        raise MtvUpgradeError(f"MTV upgrade script timed out after {exc.timeout} seconds") from exc
    except subprocess.CalledProcessError as exc:
        raise MtvUpgradeError(f"MTV upgrade script failed with exit code {exc.returncode}") from exc

    LOGGER.info(f"MTV upgrade to version {mtv_version} completed successfully")

"""
Upgrade utilities for MTV operator upgrade tests.

This module provides functions to run the MTV upgrade process, wait for
forklift pods to become ready after upgrade, and verify that Plan CRs
remain in Ready condition post-upgrade.
"""

from __future__ import annotations

import os
import subprocess
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from kubernetes.dynamic.exceptions import NotFoundError
from ocp_resources.plan import Plan
from ocp_resources.pod import Pod
from pytest_testconfig import config as py_config
from simple_logger.logger import get_logger
from timeout_sampler import TimeoutSampler

from exceptions.exceptions import ForkliftPodsNotRunningError, MtvUpgradeError

if TYPE_CHECKING:
    from kubernetes.dynamic import DynamicClient

LOGGER = get_logger(name=__name__)


@contextmanager
def _duplicate_stdout_to_fd3() -> Generator[None, None, None]:
    """Duplicate stdout to fd 3"""
    os.dup2(1, 3)
    try:
        yield
    finally:
        os.close(3)


def run_mtv_upgrade(
    script_path: str,
    mtv_version: str,
    mtv_source: str,
    image_index: str = "",
) -> None:
    """Run the MTV operator upgrade using the specified upgrade script.

    Cluster credentials are read from py_config and passed as environment variables.

    Args:
        script_path (str): Full path to the upgrade script (e.g., "/path/to/mtv-autodeploy/mtv-upgrade.sh").
        mtv_version (str): Target MTV version to upgrade to.
        mtv_source (str): MTV source identifier (e.g., "brew", "released").
        image_index (str): Optional image index override for the upgrade.

    Raises:
        ValueError: If required parameters are empty.
        MtvUpgradeError: If the upgrade script exits with a non-zero status.
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
        with _duplicate_stdout_to_fd3():
            subprocess.run(
                [script_path],
                env=env,
                cwd=os.path.dirname(script_path),
                check=True,
                pass_fds=(3,),
            )
    except subprocess.CalledProcessError as exc:
        raise MtvUpgradeError(f"MTV upgrade script failed with exit code {exc.returncode}") from exc

    LOGGER.info(f"MTV upgrade to version {mtv_version} completed successfully")


def wait_for_forklift_pods_ready(
    admin_client: DynamicClient,
    timeout: int = 600,
) -> None:
    """Wait for all forklift pods to reach Running or Succeeded state.

    Polls the MTV namespace for forklift pods and verifies that the controller
    pod exists and all forklift pods are in a healthy state.

    Args:
        admin_client (DynamicClient): OpenShift admin client.
        timeout (int): Maximum time in seconds to wait for pods to be ready.

    Raises:
        ForkliftPodsNotRunningError: If pods do not become ready within the timeout.
    """
    mtv_namespace: str = py_config["mtv_namespace"]

    def _get_not_running_pods(_admin_client: DynamicClient) -> bool:
        controller_pod: Pod | None = None
        not_running_pods: list[str] = []

        for pod in Pod.get(client=_admin_client, namespace=mtv_namespace):
            if pod.name.startswith("forklift-"):
                if pod.name.startswith("forklift-controller"):
                    controller_pod = pod

                if pod.status not in (pod.Status.RUNNING, pod.Status.SUCCEEDED):
                    not_running_pods.append(pod.name)

        if not controller_pod:
            raise ForkliftPodsNotRunningError("Forklift controller pod not found")

        if not_running_pods:
            raise ForkliftPodsNotRunningError(f"Some of the forklift pods are not running: {not_running_pods}")

        return True

    for sample in TimeoutSampler(
        func=_get_not_running_pods,
        _admin_client=admin_client,
        sleep=5,
        wait_timeout=timeout,
        exceptions_dict={ForkliftPodsNotRunningError: [], NotFoundError: []},
    ):
        if sample:
            return


def verify_plan_ready_after_upgrade(
    plan: Plan,
    timeout: int = 300,
) -> None:
    """Verify that a Plan CR stays in Ready condition after an MTV upgrade.

    Polls the Plan resource until its status conditions include type=Ready
    with status=True, or until the timeout expires.

    Args:
        plan (Plan): The Plan CR to verify.
        timeout (int): Maximum time in seconds to wait for the Ready condition.

    Raises:
        MtvUpgradeError: If the Plan does not reach Ready state within the timeout.
    """

    def _is_plan_ready(_plan: Plan) -> bool:
        status: dict[str, Any] = _plan.instance.status or {}
        conditions: list[dict[str, Any]] = status.get("conditions", [])
        for condition in conditions:
            if condition.get("type") == "Ready" and condition.get("status") == "True":
                return True

        raise MtvUpgradeError(f"Plan '{_plan.name}' not in Ready state. Current conditions: {conditions}")

    for sample in TimeoutSampler(
        func=_is_plan_ready,
        _plan=plan,
        sleep=10,
        wait_timeout=timeout,
        exceptions_dict={MtvUpgradeError: []},
    ):
        if sample:
            LOGGER.info(f"Plan '{plan.name}' is in Ready state after upgrade")
            return

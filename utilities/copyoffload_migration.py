"""
Copy-offload migration utilities for MTV tests.

This module provides copy-offload specific functionality for VM migration tests,
specifically credential management for copy-offload configurations.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from kubernetes.dynamic import DynamicClient
from ocp_resources.secret import Secret
from rrmngmnt import Host, RootUser, User
from simple_logger.logger import get_logger
from timeout_sampler import TimeoutExpiredError, TimeoutSampler

from utilities.post_migration import get_ssh_credentials_from_provider_config

if TYPE_CHECKING:
    from libs.providers.vmware import VMWareProvider

LOGGER = get_logger(__name__)


def get_copyoffload_credential(
    credential_name: str,
    copyoffload_config: dict[str, Any],
) -> str | None:
    """
    Get a copyoffload credential from environment variable or config file.

    Environment variables take precedence over config file values.
    Environment variable names are constructed as COPYOFFLOAD_{credential_name.upper()}.

    Args:
        credential_name: Name of the credential (e.g., "storage_hostname", "ontap_svm",
                        "vantara_hostgroup_id_list")
        copyoffload_config: Copyoffload configuration dictionary

    Returns:
        str | None: Credential value from env var or config, or None if not found

    Examples:
        - "storage_hostname" → "COPYOFFLOAD_STORAGE_HOSTNAME"
        - "ontap_svm" → "COPYOFFLOAD_ONTAP_SVM"
        - "vantara_hostgroup_id_list" → "COPYOFFLOAD_VANTARA_HOSTGROUP_ID_LIST"
    """
    env_var_name = f"COPYOFFLOAD_{credential_name.upper()}"
    return os.getenv(env_var_name) or copyoffload_config.get(credential_name)


def wait_for_plan_secret(ocp_admin_client: DynamicClient, namespace: str, plan_name: str) -> None:
    """
    Wait for Forklift to create plan-specific secret for copy-offload.

    When a Plan is created with copy-offload configuration, ForkliftController
    should automatically create a plan-specific secret containing storage credentials.
    This function polls for that secret's existence.

    Args:
        ocp_admin_client: OpenShift dynamic client
        namespace: Namespace where the plan and secret exist
        plan_name: Name of the Plan (secret will be named {plan_name}-*)

    Note:
        Times out after 60 seconds but continues anyway (logs warning).
        The migration will fail with clearer error if secret is missing.
    """
    LOGGER.info("Copy-offload: waiting for Forklift to create plan-specific secret...")
    try:
        for _ in TimeoutSampler(
            wait_timeout=60,
            sleep=2,
            func=lambda: any(
                s.name.startswith(f"{plan_name}-") for s in Secret.get(client=ocp_admin_client, namespace=namespace)
            ),
        ):
            break
    except TimeoutExpiredError:
        LOGGER.warning(f"Timeout waiting for plan secret '{plan_name}-*' - continuing anyway")


def wait_for_vmware_cloud_init_all_vms(
    prepared_plan: dict[str, Any],
    source_provider: VMWareProvider,
    source_provider_data: dict[str, Any],
) -> None:
    """Wait for cloud-init to finish on all VMware VMs in the plan.

    Iterates over all VMs in the plan and waits for each to signal
    cloud-init completion via the presence of ``/cloud-init.finish``.

    Args:
        prepared_plan (dict[str, Any]): Processed plan config with VM data
        source_provider (VMWareProvider): Source VMware provider instance
        source_provider_data (dict[str, Any]): Source provider configuration data

    Returns:
        None

    Raises:
        TimeoutExpiredError: If cloud-init does not finish within timeout
        ValueError: If guest info or IP address is unavailable
    """
    for vm_data in prepared_plan["virtual_machines"]:
        vm_name = vm_data["name"]
        provider_vm_api = prepared_plan["source_vms_data"][vm_name]["provider_vm_api"]

        cloud_init_kwargs: dict[str, Any] = {
            "source_provider": source_provider,
            "source_provider_data": source_provider_data,
            "vm_name": vm_name,
            "provider_vm_api": provider_vm_api,
            "file_name": "/cloud-init.finish",
        }
        if "source_vm_power" in vm_data:
            cloud_init_kwargs["target_power_state"] = vm_data["source_vm_power"]

        wait_for_cloud_init(**cloud_init_kwargs)


def wait_for_cloud_init(
    source_provider: VMWareProvider,
    source_provider_data: dict[str, Any],
    vm_name: str,
    provider_vm_api: Any,
    file_name: str,
    timeout: int = 2000,
    target_power_state: str = "off",
) -> None:
    """
    Wait for cloud-init to finish by checking for a specific file.

    Args:
        source_provider: Source provider instance
        source_provider_data: Source provider configuration data
        vm_name: Name of the VM
        provider_vm_api: Provider VM object
        file_name: Full path to the file to check for (e.g., "/cloud-init.finish")
        timeout: Timeout in seconds (default: 2000)
        target_power_state: Desired power state after check ("on" or "off", default: "off")

    Returns:
        None

    Raises:
        TimeoutExpiredError: If cloud-init does not finish within timeout
        ValueError: If guest info or IP address is unavailable
    """
    LOGGER.info(f"Powering on VM {vm_name} to check cloud-init status")
    source_provider.start_vm(provider_vm_api)

    try:
        # Wait for IP
        if not source_provider.wait_for_vmware_guest_info(provider_vm_api, timeout=1000):
            raise ValueError(f"Guest info not available for VM '{vm_name}'")

        # Get IP with polling
        ip_address = None
        last_vm_info: dict[str, Any] = {}

        def _get_ip() -> str | None:
            nonlocal last_vm_info
            last_vm_info = source_provider.vm_dict(provider_vm_api=provider_vm_api)
            for nic in last_vm_info.get("network_interfaces", []):
                if nic.get("ip_addresses"):
                    return nic["ip_addresses"][0]["ip_address"]
            return None

        try:
            for ip in TimeoutSampler(wait_timeout=300, sleep=5, func=_get_ip):
                if ip:
                    ip_address = ip
                    break
        except TimeoutExpiredError:
            pass

        if not ip_address:
            raise ValueError(f"Could not find IP address for VM '{vm_name}'")

        LOGGER.info(f"VM {vm_name} has IP: {ip_address}")

        # Get credentials
        source_vm_info = {"win_os": last_vm_info.get("win_os", False)}
        username, password = get_ssh_credentials_from_provider_config(source_provider_data, source_vm_info)

        host = Host(ip_address)
        user = RootUser(password) if username == "root" else User(username, password)

        def _check_file() -> bool:
            try:
                rc, _, _ = host.executor(user=user).run_cmd(["ls", file_name])
                return rc == 0
            except Exception as e:
                LOGGER.warning(f"SSH check failed for {vm_name}: {type(e).__name__}: {e} - retrying...")
                return False

        LOGGER.info(f"Waiting for {file_name} on {ip_address}...")
        try:
            for sample in TimeoutSampler(wait_timeout=timeout, sleep=10, func=_check_file):
                if sample:
                    LOGGER.info(f"{file_name} found!")
                    break
        except TimeoutExpiredError:
            raise TimeoutExpiredError(f"Cloud-init did not finish (file {file_name} not found)") from None

    finally:
        if target_power_state == "off":
            LOGGER.info(f"Powering off VM - {vm_name}")
            try:
                source_provider.stop_vm(provider_vm_api)
            except Exception as e:
                LOGGER.warning(f"Failed to power off VM '{vm_name}': {type(e).__name__}: {e}")
        else:
            LOGGER.info(f"Leaving VM {vm_name} powered on")

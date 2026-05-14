"""
Upgrade migration tests for MTV.

This module implements tests that validate migration behavior across MTV
operator upgrades. The test flow creates migration resources (StorageMap,
NetworkMap, Plan) on the pre-upgrade version, upgrades the MTV operator,
verifies post-upgrade state, and then executes the migration.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from ocp_resources.network_map import NetworkMap
from ocp_resources.plan import Plan
from ocp_resources.storage_map import StorageMap
from pytest_testconfig import config as py_config
from packaging.version import InvalidVersion, Version
from simple_logger.logger import get_logger

from utilities.mtv_migration import (
    create_plan_resource,
    execute_migration,
    get_network_migration_map,
    get_storage_migration_map,
)
from utilities.post_migration import check_vms
from utilities.upgrade import (
    run_mtv_upgrade,
    verify_plan_ready_after_upgrade,
    wait_for_forklift_pods_ready,
)
from utilities.utils import get_mtv_version, populate_vm_ids

if TYPE_CHECKING:
    from kubernetes.dynamic import DynamicClient

    from libs.base_provider import BaseProvider
    from libs.forklift_inventory import ForkliftInventory
    from libs.providers.openshift import OCPProvider
    from utilities.ssh_utils import SSHConnectionManager

LOGGER = get_logger(name=__name__)


@pytest.mark.upgrade
@pytest.mark.vsphere
@pytest.mark.incremental
@pytest.mark.parametrize(
    "class_plan_config",
    [
        pytest.param(
            py_config["tests_params"]["test_upgrade_cold_migration"],
        )
    ],
    indirect=True,
    ids=["upgrade-cold"],
)
@pytest.mark.usefixtures("cleanup_migrated_vms")
class TestUpgradeColdMigration:
    """Cold migration with MTV operator upgrade between plan creation and execution."""

    storage_map: StorageMap
    network_map: NetworkMap
    plan_resource: Plan

    def test_verify_pre_upgrade_version(
        self,
        ocp_admin_client: DynamicClient,
    ) -> None:
        """Verify and log the pre-upgrade MTV version."""
        version = get_mtv_version(client=ocp_admin_client)
        LOGGER.info(f"Pre-upgrade MTV version: {version}")
        assert version, "Failed to retrieve pre-upgrade MTV version"

    def test_create_storagemap(
        self,
        prepared_plan: dict[str, Any],
        fixture_store: dict[str, Any],
        ocp_admin_client: DynamicClient,
        source_provider: BaseProvider,
        destination_provider: BaseProvider,
        source_provider_inventory: ForkliftInventory,
        target_namespace: str,
    ) -> None:
        """Create StorageMap resource for migration."""
        vms: list[str] = [vm["name"] for vm in prepared_plan["virtual_machines"]]
        self.__class__.storage_map = get_storage_migration_map(
            fixture_store=fixture_store,
            source_provider=source_provider,
            destination_provider=destination_provider,
            source_provider_inventory=source_provider_inventory,
            ocp_admin_client=ocp_admin_client,
            target_namespace=target_namespace,
            vms=vms,
        )
        assert self.storage_map, "StorageMap creation failed"

    def test_create_networkmap(
        self,
        prepared_plan: dict[str, Any],
        fixture_store: dict[str, Any],
        ocp_admin_client: DynamicClient,
        source_provider: BaseProvider,
        destination_provider: BaseProvider,
        source_provider_inventory: ForkliftInventory,
        target_namespace: str,
        multus_network_name: dict[str, str],
    ) -> None:
        """Create NetworkMap resource for migration."""
        vms: list[str] = [vm["name"] for vm in prepared_plan["virtual_machines"]]
        self.__class__.network_map = get_network_migration_map(
            fixture_store=fixture_store,
            source_provider=source_provider,
            destination_provider=destination_provider,
            source_provider_inventory=source_provider_inventory,
            ocp_admin_client=ocp_admin_client,
            target_namespace=target_namespace,
            multus_network_name=multus_network_name,
            vms=vms,
        )
        assert self.network_map, "NetworkMap creation failed"

    def test_create_plan(
        self,
        prepared_plan: dict[str, Any],
        fixture_store: dict[str, Any],
        ocp_admin_client: DynamicClient,
        source_provider: BaseProvider,
        destination_provider: OCPProvider,
        target_namespace: str,
        source_provider_inventory: ForkliftInventory,
    ) -> None:
        """Create MTV Plan CR resource."""
        populate_vm_ids(prepared_plan, source_provider_inventory)

        self.__class__.plan_resource = create_plan_resource(
            ocp_admin_client=ocp_admin_client,
            fixture_store=fixture_store,
            source_provider=source_provider,
            destination_provider=destination_provider,
            storage_map=self.storage_map,
            network_map=self.network_map,
            virtual_machines_list=prepared_plan["virtual_machines"],
            target_namespace=target_namespace,
            warm_migration=prepared_plan.get("warm_migration", False),
        )
        assert self.plan_resource, "Plan creation failed"

    def test_upgrade_mtv(self) -> None:
        """Upgrade the MTV operator to the target version."""
        run_mtv_upgrade(
            script_path=py_config["upgrade_script_path"],
            mtv_version=py_config["mtv_upgrade_to_version"],
            mtv_source=py_config["mtv_upgrade_to_source"],
            image_index=py_config["mtv_upgrade_image_index"],
        )

    def test_verify_post_upgrade(
        self,
        ocp_admin_client: DynamicClient,
    ) -> None:
        """Verify MTV version, pod readiness, and Plan CR state after upgrade."""
        version = get_mtv_version(client=ocp_admin_client)
        LOGGER.info(f"Post-upgrade MTV version: {version}")

        expected_version: str = py_config["mtv_upgrade_to_version"]
        try:
            actual = Version(version)
            expected = Version(expected_version)
        except InvalidVersion as exc:
            raise AssertionError(
                f"Could not parse MTV version strings: actual='{version}', expected='{expected_version}'"
            ) from exc
        assert (actual.major, actual.minor) == (expected.major, expected.minor), (
            f"Expected MTV major.minor version '{expected.major}.{expected.minor}', got '{version}'"
        )

        wait_for_forklift_pods_ready(admin_client=ocp_admin_client)
        verify_plan_ready_after_upgrade(plan=self.plan_resource)

    def test_migrate_vms(
        self,
        fixture_store: dict[str, Any],
        ocp_admin_client: DynamicClient,
        target_namespace: str,
    ) -> None:
        """Execute migration on the upgraded MTV operator."""
        execute_migration(
            ocp_admin_client=ocp_admin_client,
            fixture_store=fixture_store,
            plan=self.plan_resource,
            target_namespace=target_namespace,
        )

    def test_check_vms(
        self,
        prepared_plan: dict[str, Any],
        source_provider: BaseProvider,
        destination_provider: BaseProvider,
        source_provider_data: dict[str, Any],
        source_vms_namespace: str,
        source_provider_inventory: ForkliftInventory,
        vm_ssh_connections: SSHConnectionManager | None,
    ) -> None:
        """Validate migrated VMs after upgrade."""
        check_vms(
            plan=prepared_plan,
            source_provider=source_provider,
            destination_provider=destination_provider,
            network_map_resource=self.network_map,
            storage_map_resource=self.storage_map,
            source_provider_data=source_provider_data,
            source_vms_namespace=source_vms_namespace,
            source_provider_inventory=source_provider_inventory,
            vm_ssh_connections=vm_ssh_connections,
        )

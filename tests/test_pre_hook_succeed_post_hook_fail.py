from typing import TYPE_CHECKING, Any

import pytest
from ocp_resources.network_map import NetworkMap
from ocp_resources.plan import Plan
from ocp_resources.storage_map import StorageMap
from pytest_testconfig import config as py_config

from exceptions.exceptions import MigrationPlanExecError
from utilities.hooks import validate_hook_failure_and_check_vms
from utilities.mtv_migration import (
    create_plan_resource,
    execute_migration,
    get_network_migration_map,
    get_storage_migration_map,
)
from utilities.post_migration import check_vms
from utilities.utils import populate_vm_ids

if TYPE_CHECKING:
    from kubernetes.dynamic import DynamicClient
    from libs.ocp_provider import OCPProvider
    from libs.provider_inventory.ocp_inventory import ForkliftInventory
    from libs.ssh import SSHConnectionManager

    from libs.base_provider import BaseProvider


@pytest.mark.tier1
@pytest.mark.negative
@pytest.mark.incremental
@pytest.mark.parametrize(
    "class_plan_config",
    [pytest.param(py_config["tests_params"]["test_pre_hook_succeed_post_hook_fail"])],
    indirect=True,
    ids=["pre-hook-succeed-post-hook-fail"],
)
@pytest.mark.usefixtures("cleanup_migrated_vms")
class TestPreHookSucceedPostHookFail:
    """Test PreHook succeeds but PostHook fails - migration should fail at PostHook step."""

    storage_map: StorageMap | None = None
    network_map: NetworkMap | None = None
    plan_resource: Plan | None = None
    should_check_vms: bool = False

    def test_create_storagemap(
        self,
        prepared_plan: dict[str, Any],
        fixture_store: dict[str, Any],
        ocp_admin_client: "DynamicClient",
        source_provider: "BaseProvider",
        destination_provider: "OCPProvider",
        source_provider_inventory: "ForkliftInventory",
        target_namespace: str,
    ) -> None:
        """Create StorageMap resource for migration."""
        vms = [vm["name"] for vm in prepared_plan["virtual_machines"]]
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
        ocp_admin_client: "DynamicClient",
        source_provider: "BaseProvider",
        destination_provider: "OCPProvider",
        source_provider_inventory: "ForkliftInventory",
        target_namespace: str,
        multus_network_name: dict[str, str],
    ) -> None:
        """Create NetworkMap resource for migration."""
        vms = [vm["name"] for vm in prepared_plan["virtual_machines"]]
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
        ocp_admin_client: "DynamicClient",
        source_provider: "BaseProvider",
        destination_provider: "OCPProvider",
        target_namespace: str,
        source_provider_inventory: "ForkliftInventory",
    ) -> None:
        """Create MTV Plan CR resource with PreHook and PostHook."""
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
            warm_migration=prepared_plan["warm_migration"],
            pre_hook_name=prepared_plan.get("_pre_hook_name"),
            pre_hook_namespace=prepared_plan.get("_pre_hook_namespace"),
            after_hook_name=prepared_plan.get("_post_hook_name"),
            after_hook_namespace=prepared_plan.get("_post_hook_namespace"),
        )
        assert self.plan_resource, "Plan creation failed"

    def test_migrate_vms(
        self,
        prepared_plan: dict[str, Any],
        fixture_store: dict[str, Any],
        ocp_admin_client: "DynamicClient",
        target_namespace: str,
    ) -> None:
        """Execute migration - PreHook succeeds but PostHook fails."""
        expected_result = prepared_plan["expected_migration_result"]

        if expected_result == "fail":
            with pytest.raises(MigrationPlanExecError):
                execute_migration(
                    ocp_admin_client=ocp_admin_client,
                    fixture_store=fixture_store,
                    plan=self.plan_resource,
                    target_namespace=target_namespace,
                )
            self.__class__.should_check_vms = validate_hook_failure_and_check_vms(self.plan_resource, prepared_plan)
        else:
            execute_migration(
                ocp_admin_client=ocp_admin_client,
                fixture_store=fixture_store,
                plan=self.plan_resource,
                target_namespace=target_namespace,
            )
            self.__class__.should_check_vms = True

    def test_check_vms(
        self,
        prepared_plan: dict[str, Any],
        source_provider: "BaseProvider",
        destination_provider: "OCPProvider",
        source_provider_data: dict[str, Any],
        target_namespace: str,
        source_vms_namespace: str,
        source_provider_inventory: "ForkliftInventory",
        vm_ssh_connections: "SSHConnectionManager | None",
    ) -> None:
        """Validate migrated VMs - PostHook fails after migration, so VMs should exist."""
        # Runtime skip needed - decision based on previous test's migration execution result
        if not self.__class__.should_check_vms:
            pytest.skip("Skipping VM checks - PreHook failure means VMs were not migrated")

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

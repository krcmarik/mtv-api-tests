import pytest
from pytest_testconfig import config as py_config

from ocp_resources.plan import Plan
from ocp_resources.network_map import NetworkMap
from ocp_resources.storage_map import StorageMap
from utilities.mtv_migration import (
    create_plan_resource,
    execute_migration,
    get_network_migration_map,
    get_storage_migration_map,
)
from utilities.post_migration import check_vms


@pytest.mark.parametrize(
    "class_plan_config",
    [pytest.param(py_config["tests_params"]["test_custom_nad_vm_namespace"])],
    indirect=True,
    ids=["custom-namespaces"],
)
@pytest.mark.usefixtures("cleanup_migrated_vms")
@pytest.mark.incremental
@pytest.mark.tier1
class TestCustomNadVmNamespace:
    """Test migration with custom NAD and VM target namespaces.

    This test verifies that VMs can be migrated to a custom namespace
    and NetworkAttachmentDefinitions can be created in a separate custom namespace.
    """

    storage_map: StorageMap
    network_map: NetworkMap
    plan_resource: Plan

    def test_create_storagemap(
        self,
        prepared_plan,
        fixture_store,
        source_provider,
        destination_provider,
        ocp_admin_client,
        target_namespace,
        source_provider_inventory,
    ):
        """Create StorageMap resource."""
        vms = [vm["name"] for vm in prepared_plan["virtual_machines"]]
        self.__class__.storage_map = get_storage_migration_map(
            fixture_store=fixture_store,
            source_provider=source_provider,
            destination_provider=destination_provider,
            ocp_admin_client=ocp_admin_client,
            target_namespace=target_namespace,
            source_provider_inventory=source_provider_inventory,
            vms=vms,
        )
        assert self.storage_map

    def test_create_networkmap(
        self,
        prepared_plan,
        fixture_store,
        source_provider,
        destination_provider,
        ocp_admin_client,
        target_namespace,
        source_provider_inventory,
        multus_network_name,
    ):
        """Create NetworkMap resource with custom multus namespace."""
        vms = [vm["name"] for vm in prepared_plan["virtual_machines"]]
        self.__class__.network_map = get_network_migration_map(
            fixture_store=fixture_store,
            source_provider=source_provider,
            destination_provider=destination_provider,
            multus_network_name=multus_network_name,
            ocp_admin_client=ocp_admin_client,
            target_namespace=target_namespace,
            source_provider_inventory=source_provider_inventory,
            vms=vms,
        )
        assert self.network_map

    def test_create_plan(
        self,
        prepared_plan,
        fixture_store,
        source_provider,
        destination_provider,
        ocp_admin_client,
        target_namespace,
    ):
        """Create MTV Plan CR resource with custom VM target namespace."""
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
            preserve_static_ips=prepared_plan.get("preserve_static_ips", False),
            vm_target_namespace=prepared_plan["_vm_target_namespace"],
        )
        assert self.plan_resource

    def test_migrate_vms(self, fixture_store, ocp_admin_client, target_namespace):
        """Execute migration to custom VM namespace."""
        execute_migration(
            ocp_admin_client=ocp_admin_client,
            fixture_store=fixture_store,
            plan=self.plan_resource,
            target_namespace=target_namespace,
        )

    def test_check_vms(
        self,
        prepared_plan,
        source_provider,
        destination_provider,
        target_namespace,
        source_provider_data,
        source_vms_namespace,
        source_provider_inventory,
        vm_ssh_connections,
    ):
        """Validate migrated VMs in custom namespace."""
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

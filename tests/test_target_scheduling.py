import pytest
from ocp_resources.plan import Plan
from ocp_resources.network_map import NetworkMap
from ocp_resources.storage_map import StorageMap
from pytest_testconfig import config as py_config

from utilities.mtv_migration import (
    create_plan_resource,
    execute_migration,
    get_network_migration_map,
    get_storage_migration_map,
)
from utilities.post_migration import check_vms
from utilities.utils import populate_vm_ids


@pytest.mark.parametrize(
    "class_plan_config",
    [pytest.param(py_config["tests_params"]["test_target_scheduling_all_features"])],
    indirect=True,
    ids=["scheduling-all"],
)
@pytest.mark.usefixtures("cleanup_migrated_vms", "mtv_version_checker")
@pytest.mark.incremental
@pytest.mark.min_mtv_version("2.10.0")
@pytest.mark.tier1
class TestTargetSchedulingAllFeatures:
    """Test all target scheduling features: node selector, labels, and affinity."""

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
            source_provider_inventory=source_provider_inventory,
            ocp_admin_client=ocp_admin_client,
            target_namespace=target_namespace,
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
        """Create NetworkMap resource."""
        vms = [vm["name"] for vm in prepared_plan["virtual_machines"]]
        self.__class__.network_map = get_network_migration_map(
            fixture_store=fixture_store,
            source_provider=source_provider,
            destination_provider=destination_provider,
            source_provider_inventory=source_provider_inventory,
            ocp_admin_client=ocp_admin_client,
            multus_network_name=multus_network_name,
            target_namespace=target_namespace,
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
        source_provider_inventory,
        labeled_worker_node,
        target_vm_labels,
    ):
        """Create MTV Plan with target scheduling configuration."""
        populate_vm_ids(prepared_plan, source_provider_inventory)

        self.__class__.plan_resource = create_plan_resource(
            fixture_store=fixture_store,
            source_provider=source_provider,
            destination_provider=destination_provider,
            storage_map=self.storage_map,
            network_map=self.network_map,
            ocp_admin_client=ocp_admin_client,
            target_namespace=target_namespace,
            virtual_machines_list=prepared_plan["virtual_machines"],
            warm_migration=prepared_plan.get("warm_migration", False),
            target_node_selector={labeled_worker_node["label_key"]: labeled_worker_node["label_value"]},
            target_labels=target_vm_labels["vm_labels"],
            target_affinity=prepared_plan["target_affinity"],
        )
        assert self.plan_resource

    def test_migrate_vms(
        self,
        fixture_store,
        ocp_admin_client,
        target_namespace,
    ):
        """Execute migration with target scheduling."""
        execute_migration(
            fixture_store=fixture_store,
            ocp_admin_client=ocp_admin_client,
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
        labeled_worker_node,
        target_vm_labels,
    ):
        """Validate migrated VMs with target scheduling configuration."""
        check_vms(
            plan=prepared_plan,
            source_provider=source_provider,
            destination_provider=destination_provider,
            destination_namespace=target_namespace,
            source_provider_data=source_provider_data,
            network_map_resource=self.network_map,
            storage_map_resource=self.storage_map,
            source_vms_namespace=source_vms_namespace,
            source_provider_inventory=source_provider_inventory,
            labeled_worker_node=labeled_worker_node,
            target_vm_labels=target_vm_labels,
        )

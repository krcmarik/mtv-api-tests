# MTV Minimum Test Plan

> **Status**: Implementation Complete
> **Test Classes**: 3
> **Test Methods**: 15 (5 per class)
> **Features Covered**: 17 MTV IDs (14 explicit + 3 automatic)
> **VMs Required**: 3
> **Efficiency**: 85% fewer tests, 85% fewer VMs, 67% faster execution (74 min actual)

---

## Overview

Comprehensive MTV feature testing using class-based architecture that combines compatible features into single test classes.
Achieves complete coverage of 17 MTV IDs using 3 test classes (15 methods total) with 3 VMs.

**Class-Based Architecture** - Each test class follows a standardized 5-method pattern:

1. `test_create_storagemap` - Create StorageMap CR
2. `test_create_networkmap` - Create NetworkMap CR
3. `test_create_plan` - Create Plan CR with feature configuration
4. `test_migrate_vms` - Execute migration
5. `test_check_vms` - Validate all features automatically

**Key Benefits**: Configuration-driven validation, resource reuse within classes, parallel execution support via pytest-xdist.

---

## Test Classes

### Test 1: Cold Migration Comprehensive

**File**: `tests/test_cold_migration_comprehensive.py`
**Features**: 8 total (5 explicit + 3 automatic)
**Runtime**: ~21 minutes (measured: 1257.868s)
**Markers**: `@pytest.mark.tier1`, `@pytest.mark.incremental`

**Covers**:

- MTV-457: Static IP preservation (cold, multiple IPs on single NIC)
- MTV-479: PVC name template with .FileName (`{{.VmName}}-disk-{{.DiskIndex}}`)
- MTV-485: Target power state (OFF → ON) - Source OFF, target powers ON after migration
- MTV-516: Target VM scheduling (node selector + labels + affinity) - cold migration
- Cross-namespace NetworkAttachmentDefinition (NAD) access - NetworkMap references NAD from different namespace
- **Automatic**: MTV-483: Secure TLS connection to VMware
- **Automatic**: MTV-518: VMware serial number preservation
- **Automatic**: MTV-521: InsecureSkipVerify auto-set

**VM Requirements**:

- Name: `mtv-win2019-3disks`
- OS: Windows Server 2019
- **Network: 1 NIC with multiple static IPs (2+ IPs on single interface REQUIRED)** - validates MTV-457 static IP preservation
- **Storage: 3 SCSI disks (REQUIRED)** - validates `{{.DiskIndex}}` and `{{.VMName}}` template variables
- Guest Agent: Installed
- Source power state: OFF

**Configuration**:

```python
"test_cold_migration_comprehensive": {
    "virtual_machines": [
        {
            "name": "mtv-win2019-3disks",
            "source_vm_power": "off",
            "guest_agent": True,
        },
    ],
    "warm_migration": False,
    "target_power_state": "on",  # Plan-level: applies to all VMs (source OFF → target ON)
    "source_provider_insecure_skip_verify": "false",  # Enable TLS verification
    "preserve_static_ips": True,
    "pvc_name_template": "{{.VmName}}-disk-{{.DiskIndex}}",
    "pvc_name_template_use_generate_name": False,
    "target_node_selector": {"mtv-comprehensive-node": None},  # Auto-generated
    "target_labels": {"mtv-comprehensive-label": None, "test-type": "comprehensive"},
    "target_affinity": {...},  # Pod affinity rules
    "vm_target_namespace": "mtv-comprehensive-vms",
    "multus_namespace": "default",  # Cross-namespace NAD access
}
```

**Power State Scenario (OFF → ON)**: `target_power_state` is a **plan-level setting** that applies to ALL VMs in the migration plan.
Source VM is powered OFF, and all target VMs will be powered ON after migration completes (MTV-485).

---

### Test 2: Warm Migration Comprehensive

**File**: `tests/test_warm_migration_comprehensive.py`
**Features**: 7 total (5 explicit + 2 automatic)
**Runtime**: ~35 minutes (measured: 2116.609s)
**Markers**: `@pytest.mark.tier1`, `@pytest.mark.warm`, `@pytest.mark.incremental`

**Covers**:

- MTV-468: Static IP preservation (warm, multiple IPs on single NIC)
- MTV-469: Custom target namespace + cross-namespace NAD access
- MTV-486: Multiple static IPs on single NIC (warm migration)
- MTV-488: Target power state (ON → ON) - Source stays ON during warm migration to allow guest agent data collection, target powers ON
- MTV-489: PVC name template (warm) with 3 disks + generateName support
- MTV-517: Target VM scheduling (labels + affinity) - warm migration
- **Automatic**: MTV-518: VMware serial number preservation
- **Automatic**: MTV-521: InsecureSkipVerify auto-set

**VM Requirements**:

- Name: `mtv-win2022-ip-3disks`
- OS: Windows Server 2022
- **Network: 1 NIC with multiple static IPs (2+ IPs on single interface REQUIRED)** - validates MTV-468 and MTV-486 warm migration static IP preservation
- **Storage: 3 SATA disks (REQUIRED)** - validates `{{.DiskIndex}}` and `{{.FileName}}` across multiple disks
- Guest Agent: Installed
- Source power state: ON (running)

**Configuration**:

```python
"test_warm_migration_comprehensive": {
    "virtual_machines": [
        {
            "name": "mtv-win2022-ip-3disks",
            "source_vm_power": "on",
            "guest_agent": True,
        },
    ],
    "warm_migration": True,
    "target_power_state": "on",  # Plan-level: applies to all VMs (source ON → target ON)
    "source_provider_insecure_skip_verify": "false",  # Enable TLS verification (MTV-483)
    "preserve_static_ips": True,
    "vm_target_namespace": "custom-vm-namespace",
    "multus_namespace": "default",  # Cross-namespace NAD access
    "pvc_name_template": '{{ .FileName | trimSuffix \".vmdk\" | replace \"_\" \"-\" }}-{{.DiskIndex}}',
    "pvc_name_template_use_generate_name": True,
    "target_labels": {"mtv-comprehensive-test": None, "static-label": "static-value"},
    "target_affinity": {...},  # Pod affinity rules
}
```

**Power State Scenario (ON → ON)**: `target_power_state` is a **plan-level setting** that applies to ALL VMs in the migration plan.
Source VM stays powered ON during warm migration to allow guest agent data collection, and all target VMs will be powered ON after migration completes (MTV-488).
This ensures guest agent data (IP addresses, hostname, OS details) is available during the migration process.

**Special Fixtures**: Uses `precopy_interval_forkliftcontroller` for warm migration snapshot intervals

---

### Test 3: Hook Test with VM Retention

**File**: `tests/test_post_hook_retain_failed_vm.py`
**Features**: 6 total (3 explicit + 3 automatic)
**Runtime**: ~18 minutes (measured: 1069.636s)
**Markers**: `@pytest.mark.tier1`, `@pytest.mark.negative`, `@pytest.mark.incremental`

**Covers**:

- MTV-471: Pre-hook execution with success
- MTV-476: Post-hook VM retention on failure
- Power state ON → OFF: Part of general power state testing
- **Automatic**: MTV-483: Secure TLS connection to VMware
- **Automatic**: MTV-518: VMware serial number preservation
- **Automatic**: MTV-521: InsecureSkipVerify auto-set

**VM Requirements**:

- Name: `mtv-tests-rhel8`
- OS: RHEL 8
- Standard disk configuration
- Source power state: ON (running)

**Configuration**:

```python
"test_post_hook_retain_failed_vm": {
    "virtual_machines": [{"name": "mtv-tests-rhel8", "source_vm_power": "on"}],
    "warm_migration": False,
    "target_power_state": "off",  # Plan-level: applies to all VMs (source ON → target OFF)
    "source_provider_insecure_skip_verify": "false",  # Enable TLS verification
    "pre_hook": {"expected_result": "succeed"},
    "post_hook": {"expected_result": "fail"},
    "expected_migration_result": "fail",
}
```

**Power State Scenario (ON → OFF)**: Source VM is powered ON, and target VM will be powered OFF after migration completes
(before post-hook execution). This validates that target power state configuration (OFF) is applied correctly during hook
testing scenarios.

**Special Implementation**:

- `test_migrate_vms`: Expects `MigrationPlanExecError`, sets `should_check_vms` flag
- `test_check_vms`: Conditional validation - skips if hook failed before VM migration
- Validates: Pre-hook succeeds → Migration completes → Post-hook fails → VM retained

---

## VM Requirements

| VM Name                    | OS                  | Configuration                                                                          | Used By       |
|----------------------------|---------------------|----------------------------------------------------------------------------------------|---------------|
| **mtv-win2019-3disks**     | Windows Server 2019 | 1 NIC with multiple static IPs (2+ IPs REQUIRED), Guest Agent, 3 SCSI disks (REQUIRED) | Test 1 (Cold) |
| **mtv-win2022-ip-3disks**  | Windows Server 2022 | 1 NIC with multiple static IPs (2+ IPs REQUIRED), Guest Agent, 3 SATA disks (REQUIRED) | Test 2 (Warm) |
| **mtv-tests-rhel8**        | RHEL 8              | Standard disk configuration                                                            | Test 3 (Hook) |

### Multiple Static IPs Requirement

**CRITICAL for Test 1 & Test 2**: The Windows VMs (`mtv-win2019-3disks` for Test 1, `mtv-win2022-ip-3disks` for Test 2)
MUST have **2 or more static IP addresses configured on a single network interface**. This is a mandatory requirement for
testing static IP preservation features.

**Why Multiple IPs:**

- MTV-457 (Test 1): Validates that ALL static IPs are preserved during cold migration, not just the primary IP
- MTV-468 (Test 2): Validates that ALL static IPs are preserved during warm migration
- MTV-486 (Test 2): Specifically tests multiple static IPs on single NIC in warm migration scenarios

**Configuration Requirements:**

- Minimum 2 static IPs on a single network interface (more IPs provide better validation coverage)
- All IPs must be on the same subnet/interface
- Tests verify that ALL configured static IPs are preserved post-migration

### Disk Requirements

**CRITICAL for Test 1 & Test 2**: The Windows VMs (`mtv-win2019-3disks` for Test 1, `mtv-win2022-ip-3disks` for Test 2)
MUST have **3 disks** to validate PVC name template features (MTV-479, MTV-489). This validates `{{.DiskIndex}}` and
`{{.FileName}}` template variables across multiple disks.

**Disk Configuration:**

- Test 1 (`mtv-win2019-3disks`): **3 SCSI disks REQUIRED**
- Test 2 (`mtv-win2022-ip-3disks`): **3 SATA/SCSI disks REQUIRED**
- Test 3 (`mtv-tests-rhel8`): Standard disk configuration (no specific disk count requirement)

Fewer disks for Test 1 and Test 2 will result in incomplete PVC template validation.

---

## Configuration Structure

### Plan-Level vs Per-VM Settings

**Plan-Level Settings** (apply to ALL VMs in the migration plan):

- `warm_migration` - Migration type (warm/cold)
- `preserve_static_ips` - Static IP preservation
- `pvc_name_template` - PVC naming template
- `pvc_name_template_use_generate_name` - Use Kubernetes generateName
- `target_power_state` - Target VM power state (on/off) - **NOT configurable per-VM**
- `target_node_selector` - Node placement constraints
- `target_labels` - VM labels
- `target_affinity` - Pod affinity rules
- `vm_target_namespace` - Target namespace for VMs

**Per-VM Settings** (configured inside `virtual_machines` array):

- `name` - VM name in source provider
- `source_vm_power` - Source VM power state (on/off)
- `guest_agent` - Guest agent installed (True/False)

**Important**: Settings like `target_power_state` apply uniformly to all VMs in a migration plan. MTV does not support
different target power states for different VMs within the same plan.

---

## Feature Coverage

### Explicitly Configured Features

| MTV Issue | Feature | Test | Type | Config |
| --------- | ------- | ---- | ---- | ------ |
| MTV-457 | Static IP preservation (cold, multiple IPs) | Test 1 | Cold | `preserve_static_ips: True` |
| MTV-468 | Static IP preservation (warm, multiple IPs) | Test 2 | Warm | `preserve_static_ips: True` |
| MTV-469 | Custom target namespace + cross-namespace NAD | Test 2 | Warm | `vm_target_namespace: "custom-vm-namespace"`, `multus_namespace: "default"` |
| MTV-471 | Pre-hook success | Test 3 | Negative | `pre_hook: {"expected_result": "success"}` |
| MTV-476 | Post-hook VM retention | Test 3 | Negative | `post_hook: {"expected_result": "fail"}` |
| MTV-479 | PVC name template with .FileName | Test 1 | Cold | `pvc_name_template: "{{.VmName}}-disk-{{.DiskIndex}}"` |
| MTV-485 | Target power state (OFF → ON) | Test 1 | Cold | `target_power_state: "on"`, `source_vm_power: "off"` |
| MTV-486 | Multiple static IPs on single NIC (warm) | Test 2 | Warm | `preserve_static_ips: True` |
| MTV-488 | Target power state (ON → ON) | Test 2 | Warm | `target_power_state: "on"`, `source_vm_power: "on"` |
| MTV-489 | PVC name template (warm) + generateName | Test 2 | Warm | pvc_name_template with .FileName/trimSuffix/replace, use_generate_name: True |
| MTV-516 | Target VM scheduling (node selector + labels + affinity) cold | Test 1 | Cold | `target_node_selector: {...}`, `target_labels: {...}`, `target_affinity: {...}` |
| MTV-517 | Target VM scheduling (labels + affinity) warm | Test 2 | Warm | `target_labels: {...}`, `target_affinity: {...}` |
| (none) | Power state (ON → OFF) | Test 3 | Negative | `target_power_state: "off"`, `source_vm_power: "on"` |
| (none) | Cross-namespace NAD | Test 1 | Cold | `multus_namespace: "default"` |

### Automatically Validated Features

| MTV ID | Feature | Tests | Source |
| ------ | ------- | ----- | ------ |
| MTV-518 | **VMware Serial Number Preservation** | All 3 tests | Auto-validated in `check_vms()` for VMware |
| MTV-521 | **InsecureSkipVerify Auto-Set** | All 3 tests | Global `insecure_verify_skip` in config.py |
| MTV-483 | **Secure TLS Provider Connection** | Test 3 | Provider's `insecureSkipVerify` setting |

**Summary**: 17 total features (14 explicit + 3 automatic)

**Important Notes**:

- **MTV-516** covers node selector + labels + affinity together in Test 1 (cold migration)
- **MTV-517** covers labels + affinity together in Test 2 (warm migration)
- **MTV-469** covers custom namespace + cross-namespace NAD access together in Test 2
- **MTV-489** covers PVC name template (warm) + generateName support together in Test 2

---

## Execution

### Prerequisites

1. OpenShift cluster with MTV operator (`openshift-mtv` namespace)
2. VMware vSphere with test VMs:
   - `mtv-win2019-3disks` (Windows Server 2019) - **MUST have 2+ static IPs on single NIC + 3 SCSI disks**
   - `mtv-win2022-ip-3disks` (Windows Server 2022) - **MUST have 2+ static IPs on single NIC + 3 SATA disks**
   - `mtv-tests-rhel8` (RHEL 8) - Standard disk configuration
3. `.providers.json` with vSphere credentials
4. `tests/tests_config/config.py` configured
5. Dependencies: `uv sync`

### Run Individual Tests

```bash
# Cold migration comprehensive
uv run pytest tests/test_cold_migration_comprehensive.py -v

# Warm migration comprehensive
uv run pytest tests/test_warm_migration_comprehensive.py -v

# Hook test with VM retention
uv run pytest tests/test_post_hook_retain_failed_vm.py -v
```

### Run Full Suite

```bash
uv run pytest \
    tests/test_cold_migration_comprehensive.py \
    tests/test_warm_migration_comprehensive.py \
    tests/test_post_hook_retain_failed_vm.py \
    -v
```

### Run by Markers

```bash
# Tier 1 tests only
uv run pytest -m tier1 -v

# Warm migration tests
uv run pytest -m warm -v

# Negative tests
uv run pytest -m negative -v
```

### Parallel Execution

```bash
# 3 workers (one per test class)
uv run pytest \
    tests/test_cold_migration_comprehensive.py \
    tests/test_warm_migration_comprehensive.py \
    tests/test_post_hook_retain_failed_vm.py \
    -n 3
```

### Execution Time

| Test | Time | Breakdown |
| ---- | ---- | --------- |
| Test 1 (Cold) | ~21 min | StorageMap: 4.2 min, NetworkMap: 0.1 min, Plan: 0.3 min, Migration: 12.9 min, Check: 3.5 min |
| Test 2 (Warm) | ~35 min | StorageMap: 7.8 min, NetworkMap: 0.1 min, Plan: 0.1 min, Migration: 19.7 min, Check: 7.5 min |
| Test 3 (Hook) | ~18 min | StorageMap: 5.0 min, NetworkMap: 0.1 min, Plan: 0.2 min, Migration: 11.5 min, Check: 1.1 min |
| **Total** | **~74 min (1.2 hours)** | **Measured: 4444.225s** |

**Note**: Times measured from actual test execution on reference environment. Actual times vary based on cluster performance, network bandwidth, and VM disk sizes.

---

## Validation

### Configuration-Driven Validation

The `check_vms()` utility automatically executes validators based on test plan configuration:

```python
# In test_check_vms method
check_vms(plan=prepared_plan, network_map_resource=self.network_map, ...)
```

**Automatic Validator Selection** (no manual calls needed):

| Config Key | Validator |
| ---------- | --------- |
| `preserve_static_ips` | `check_static_ips()` |
| `pvc_name_template` | `check_pvc_names()` |
| `target_power_state` | `check_vm_power_state()` |
| `target_node_selector` | `check_vm_node_placement()` |
| `target_labels` | `check_vm_labels()` |
| `target_affinity` | `check_vm_affinity()` |
| `vm_target_namespace` | `check_vm_namespace()` |
| `warm_migration` | `check_warm_migration()` |

### Validation by Test

**Test 1 (Cold)**:

- MTV-457: Static IPs (multiple IPs on single NIC)
- MTV-479: PVC naming template with .FileName (3 disks)
- MTV-485: Power state scenario (OFF → ON)
- MTV-516: Target VM scheduling (node selector + labels + affinity - all three together)
- Cross-namespace NAD access
- Automatic: MTV-483 (TLS connection), MTV-518 (serial numbers), MTV-521 (InsecureSkipVerify)

**Test 2 (Warm)**:

- MTV-468: Static IPs (warm, multiple IPs on single NIC)
- MTV-469: Custom namespace + cross-namespace NAD access (both together)
- MTV-486: Multiple static IPs on single NIC (warm migration)
- MTV-488: Power state scenario (ON → ON, guest agent collection)
- MTV-489: PVC template (3 disks) + generateName support (both together)
- MTV-517: Target VM scheduling (labels + affinity - both together)
- Automatic: MTV-518 (serial numbers), MTV-521 (InsecureSkipVerify)

**Test 3 (Hook)**:

- MTV-471: Pre-hook success
- MTV-476: Post-hook failure with VM retention
- Power state scenario (ON → OFF)
- Automatic: MTV-483 (TLS connection), MTV-518 (serial numbers), MTV-521 (InsecureSkipVerify)

---

## Efficiency Metrics

### Time Savings

- **Individual approach**: 15 tests × 15 min = ~225 min (3.75 hours) - estimated if each feature tested separately
- **Comprehensive approach**: ~74 min (1.2 hours) - measured actual runtime
- **Savings**: ~151 min (**67% faster**)

### Maintenance Benefits

1. Single source of truth for configuration
2. Reusable 5-method pattern
3. Shared fixtures across tests
4. Less code to maintain and debug
5. Faster development cycles

---

## Summary

The MTV Minimum Test Plan achieves comprehensive feature coverage with maximum efficiency:

- ✅ **17 MTV IDs** validated (14 explicit + 3 automatic)
- ✅ **85% reduction** in test count (3 vs 20 classes)
- ✅ **85% reduction** in VM requirements (3 vs 20)
- ✅ **67% faster** execution (74 min actual vs ~225 min estimated individual approach)
- ✅ **Configuration-driven validation** eliminates manual validator calls
- ✅ **Class-based architecture** provides consistent structure
- ✅ **Parallel execution support** via pytest-xdist
- ✅ **Production-ready** with comprehensive error handling

**Critical Requirements**:

- Test 1 & Test 2 VMs must have **3 disks** for PVC template validation
- Test 1 & Test 2 VMs must have **2+ static IPs on single NIC** for static IP preservation testing

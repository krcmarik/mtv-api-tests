# MTV API Tests - Claude Instructions

## AI Workflow

1. **User Prompt** - User requests fix/new test/feature/enhancement
2. **Agent Selection** - Route to appropriate specialist agent
3. **Code Changes** - Specialist implements the changes
4. **Code Review** - Trigger `code-reviewer` after ANY code change
5. **Review Cycle** - Repeat steps 4-5 until no more changes needed
6. **Completion** - All changes reviewed and approved

### Agent Routing

| Operation                 | Agent                            | Allowed Direct        |
| ------------------------- | -------------------------------- | --------------------- |
| Python code (.py files)   | `python-expert`                  | Read, Grep, Glob only |
| Git operations            | `git-expert`                     | -                     |
| Documentation (.md files) | `technical-documentation-writer` | -                     |
| After ANY code change     | `code-reviewer`                  | -                     |

**Rules:**

- Never work on main branch - always create a feature branch first
- Run agents in PARALLEL when possible
- Never skip code-reviewer after code changes
- Update README.md when code changes affect usage/requirements/installation/configuration

## Code Quality Requirements

- **Type Annotations:** Use built-in Python typing (dict, list, tuple)
- **Package Management:** Use `uv` for all dependency management
- **Pre-commit:** Must pass before any commit - never use `--no-verify`
- **No Auto-Skip:** Never use `pytest.skip()` inside fixtures - use `@pytest.mark.skipif` at test level
- **Every OpenShift resource:** Must use `create_and_store_resource()` function

### OpenShift/Kubernetes Resource Interactions

All cluster interactions must use `openshift-python-wrapper`. Direct `kubernetes` package usage is forbidden at runtime.

```python
# Correct imports
from ocp_resources.namespace import Namespace
from ocp_resources.secret import Secret
from ocp_resources.virtual_machine import VirtualMachine
from ocp_utilities.infra import get_client

# Forbidden imports (runtime)
from kubernetes import client  # Never
from kubernetes.dynamic import DynamicClient  # Never instantiate directly
```

**DynamicClient Rules:**

| Usage                                      | Allowed                  |
| ------------------------------------------ | ------------------------ |
| Import inside `TYPE_CHECKING` block        | Yes                      |
| String annotation `"DynamicClient"`        | Yes                      |
| Instantiate `DynamicClient(...)` directly  | No - use `get_client()`  |
| Use in `isinstance()` or runtime checks    | No                       |
| Import other `kubernetes.*` modules        | No                       |

### Function Size

- Maximum ~50 lines per function
- Extract helpers with `_` prefix for sub-tasks
- Function names must clearly describe WHAT they do (e.g., `is_warm_migration_supported` not `is_supported`)

## Code Quality Rules

### Fail Fast - No Invalid States

Code must never result in `None` when `None` is not valid. Fail early with clear errors.

```python
# Wrong - allows None to propagate
def get_vm_firmware(template):
    return template.spec.domain.get("firmware")

# Correct - fail fast
def get_vm_firmware(template):
    firmware = template.spec.domain.get("firmware")
    if firmware is None:
        raise ValueError(f"Firmware not found in template '{template.name}'")
    return firmware
```

### Variables Must Have Consistent Types

```python
# Wrong
actual_affinity = vm.get("affinity")  # Could be dict, list, or None

# Correct
actual_affinity: dict[str, Any] = vm.get("affinity") or {}
```

### Trust Required Arguments

Don't check if required function arguments exist.

```python
# Wrong
def compare_labels(expected_labels: dict, actual_labels: dict) -> bool:
    if expected_labels and actual_labels:
        return expected_labels == actual_labels
    return False

# Correct
def compare_labels(expected_labels: dict, actual_labels: dict) -> bool:
    return expected_labels == actual_labels
```

### No Duplicate Code

Extract common logic into shared functions.

### No Unnecessary Variables

```python
# Wrong
@pytest.fixture
def my_fixture():
    result = create_resource()
    yield result

# Correct
@pytest.fixture
def my_fixture():
    yield create_resource()
```

### Exception Types - RuntimeError is Forbidden

| Instead of RuntimeError | Use                        |
| ----------------------- | -------------------------- |
| Invalid input/config    | `ValueError`               |
| Missing resource        | `ValueError`               |
| Type issues             | `TypeError`                |
| Key not found           | `KeyError` (let propagate) |
| Connection failures     | `ConnectionError`          |
| Domain-specific         | Custom exception class     |

### Use Empty Container Defaults

```python
# Wrong
firmware_spec = template.spec.domain.get("firmware")
if firmware_spec is not None:
    boot_order = firmware_spec.get("bootOrder")

# Correct
firmware_spec: dict[str, Any] = template.spec.domain.get("firmware", {})
boot_order: list = firmware_spec.get("bootOrder", [])
```

### Type Annotations in Docstrings

All functions must have complete type annotations with Args, Returns, and Raises sections.

```python
def process_vm(vm: VirtualMachine, options: dict[str, Any]) -> MigrationResult:
    """Process a VM for migration.

    Args:
        vm (VirtualMachine): The VM resource to process
        options (dict[str, Any]): Processing options

    Returns:
        MigrationResult: The result of the migration processing

    Raises:
        ValueError: If VM is in an invalid state
    """
```

### Validate at Source

Validation must happen in fixtures (where values originate), not in utility functions.

```python
# Wrong - validating in utility (too late)
def apply_node_label(labeled_worker_node, ...):
    if not labeled_worker_node:
        raise ValueError("No worker node provided")

# Correct - validate in fixture
@pytest.fixture
def labeled_worker_node(worker_nodes, target_node_selector):
    node = find_node_with_selector(worker_nodes, target_node_selector)
    if not node:
        raise ValueError(f"No node found matching selector {target_node_selector}")
    return node
```

### Fixture Rules

- **No autouse:** Only `autouse_fixtures` in conftest.py is allowed
- **Noun names:** Fixtures represent resources (`source_provider` not `setup_provider`)
- **No magic skip:** Use `@pytest.mark.skipif` at test level, not `pytest.skip()` in fixtures

### No Unnecessary Randomness

```python
# Wrong
selected_node = random.choice(available_nodes)

# Correct
selected_node = available_nodes[0]
```

### Use Context Managers for Cleanup

```python
# Wrong
editor = ResourceEditor(node)
editor.add_label(label)
# Caller must remember to cleanup

# Correct
with ResourceEditor(node) as editor:
    editor.add_label(label)
    yield
```

## Architecture Patterns

### Provider Abstraction

- Base class: `BaseProvider` in `libs/base_provider.py`
- Implementations: VMware, RHV, OpenStack, OVA, OpenShift providers
- Context manager support for provider connections

### Test Structure

- Session-scoped fixtures for resource management
- Provider-specific test parametrization
- Markers: tier0, warm, remote, copyoffload
- Automatic log/must-gather collection on failures

### Development Workflow

- Package Installation: `uv sync`
- Linting: `ruff check` and `ruff format`
- Type Checking: `mypy`
- Container Build: `podman build -f Dockerfile -t mtv-api-tests`

## Critical Constraints

### Test Execution Prohibition

AI must NEVER run tests directly (`pytest`, `uv run pytest`). Tests require live clusters, provider connections, and credentials.

AI can: Read/analyze/write/fix tests, suggest improvements, review structure
AI cannot: Execute tests, validate by running

### Deterministic Tests - No Defaults for Our Config

For configurations we control (`py_config`, `plan`, `tests_params`), never use defaults:

```python
# Wrong
storage_class = py_config.get("storage_class", "default-storage")

# Correct
storage_class = py_config["storage_class"]
```

For external/provider data, `.get()` with validation is acceptable:

```python
vm_id = provider_data.get("vm_id")
if not vm_id:
    raise ValueError(f"VM ID not found for VM '{vm_name}'")
```

**Exception - Optional feature flags:**

```python
if plan.get("warm_migration", False):  # OK
    setup_warm_migration()
```

### Resource Creation - create_and_store_resource()

Every OpenShift resource must use `utilities/resources.py:create_and_store_resource()`.

```python
@contextmanager
def create_and_store_resource(
    client: DynamicClient,
    fixture_store: dict[str, Any],
    resource: type[T],
    test_name: str | None = None,
    skip_teardown: bool | None = None,
    **kwargs: Any,
) -> Iterator[T]:
```

Features: auto-generates unique names, deploys and waits, stores in `fixture_store["teardown"]`, handles conflicts, truncates to 63 chars.

```python
# Correct
with create_and_store_resource(
    fixture_store=fixture_store,
    resource=Namespace,
    client=ocp_admin_client,
    name="my-namespace",
) as namespace:
    # Use namespace here
    print(f"Created namespace: {namespace.name}")

# Wrong - bypasses tracking
namespace = Namespace(client=ocp_admin_client, name="my-namespace")
namespace.deploy()
```

## Test Structure Pattern

All tests follow this structure:

```python
from pytest_testconfig import config as py_config
from utilities.mtv_migration import create_storagemap_and_networkmap, migrate_vms

@pytest.mark.parametrize(
    "plan",
    [pytest.param(py_config["tests_params"]["test_name_here"])],
    indirect=True,
    ids=["descriptive-test-id"],
)
@pytest.mark.tier0  # optional
@pytest.mark.warm   # optional: warm/remote/copyoffload
def test_name_here(
    request, fixture_store, ocp_admin_client, target_namespace,
    destination_provider, plan, source_provider, source_provider_data,
    multus_network_name, source_provider_inventory, source_vms_namespace,
):
    # 1. Create migration maps
    storage_migration_map, network_migration_map = create_storagemap_and_networkmap(
        fixture_store=fixture_store, source_provider=source_provider,
        destination_provider=destination_provider,
        source_provider_inventory=source_provider_inventory,
        ocp_admin_client=ocp_admin_client, multus_network_name=multus_network_name,
        target_namespace=target_namespace, plan=plan,
    )

    # 2. Execute migration
    migrate_vms(
        ocp_admin_client=ocp_admin_client, request=request,
        fixture_store=fixture_store, source_provider=source_provider,
        destination_provider=destination_provider, plan=plan,
        network_migration_map=network_migration_map,
        storage_migration_map=storage_migration_map,
        source_provider_data=source_provider_data,
        target_namespace=target_namespace, source_vms_namespace=source_vms_namespace,
        source_provider_inventory=source_provider_inventory,
    )
```

### Adding New Tests

1. Add configuration to `tests/tests_config/config.py`:

```python
tests_params: dict = {
    "test_my_new_test": {
        "virtual_machines": [{"name": "vm-name", "source_vm_power": "on", "guest_agent": True}],
        "warm_migration": False,
    },
}
```

1. Create test file `tests/test_<feature>_migration.py`
2. Add parametrize decorator with `indirect=True`
3. Add pytest markers (tier0, warm, remote, copyoffload)
4. Follow the two-step pattern: create maps, execute migration

**VM Configuration Options:**

| Option            | Required | Values                              |
| ----------------- | -------- | ----------------------------------- |
| `name`            | Yes      | VM name in source provider          |
| `source_vm_power` | No       | "on" or "off"                       |
| `guest_agent`     | No       | True if installed                   |
| `clone`           | No       | True to clone before migration      |
| `disk_type`       | No       | "thin", "thick-lazy", "thick-eager" |

## Fixture Patterns

### conftest.py Structure

Only pytest fixtures and hooks belong in conftest.py. Helper functions go to `utilities/`.

### Fixture Scopes

**Session-scoped** - shared across all tests:

```python
@pytest.fixture(scope="session")
def ocp_admin_client():
    return get_cluster_client()
```

Common: `ocp_admin_client`, `session_uuid`, `target_namespace`, `source_provider`, `destination_provider`, `fixture_store`

**Function-scoped** - per test:

```python
@pytest.fixture(scope="function")
def plan(request, fixture_store, source_provider):
    plan: dict[str, Any] = request.param
    # Clone VMs, update names
    yield plan
    # Track for cleanup
```

### fixture_store Structure

```python
{
    "session_uuid": "auto-abc123",
    "base_resource_name": "auto-abc123-vsphere-8-0",
    "teardown": {
        "Namespace": [{"name": "ns1", ...}],
        "VirtualMachine": [{"name": "vm1", ...}],
    },
}
```

## Test Markers

| Marker        | Purpose                          |
| ------------- | -------------------------------- |
| `tier0`       | Core functionality (smoke tests) |
| `warm`        | Warm migration tests             |
| `remote`      | Remote cluster tests             |
| `copyoffload` | Copy-offload (XCOPY) tests       |

```python
@pytest.mark.tier0
@pytest.mark.warm
@pytest.mark.skipif(not get_value_from_py_config("remote_ocp_cluster"), reason="No remote cluster")
def test_remote_warm_migration(...):
    pass
```

## Parallel Execution (pytest-xdist)

Tests are parallel-safe because:

- Unique namespaces per session via `session_uuid`
- Each worker has isolated `fixture_store`
- `create_and_store_resource()` generates unique names

Rules:

- Always use fixtures for namespaces (never hardcode)
- Always use `create_and_store_resource()` for unique names
- Never share mutable state between tests

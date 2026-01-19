from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import yaml
from kubernetes.dynamic import DynamicClient
from kubernetes.dynamic.exceptions import ConflictError
from ocp_resources.migration import Migration
from ocp_resources.plan import Plan
from ocp_resources.resource import Resource
from simple_logger.logger import get_logger

from utilities.naming import generate_name_with_uuid

LOGGER = get_logger(__name__)


@contextmanager
def create_and_store_resource(
    client: DynamicClient,
    fixture_store: dict[str, Any],
    resource: type[Resource],
    test_name: str | None = None,
    skip_teardown: bool = False,
    **kwargs: Any,
) -> Iterator[Resource]:
    """Context manager to create, track, and cleanup OpenShift resources.

    Creates a resource, tracks it in fixture_store, and automatically cleans it up
    when the context exits (unless skip_teardown=True).

    Args:
        client (DynamicClient): OpenShift DynamicClient
        fixture_store (dict[str, Any]): Session fixture store for tracking resources
        resource (type[Resource]): Resource class type to instantiate
        test_name (str | None): Optional test name for tracking
        skip_teardown (bool): If True, skip cleanup on context exit (default: False)
        **kwargs (Any): Additional arguments passed to resource constructor

    Yields:
        Resource: The deployed resource instance

    Raises:
        ValueError: If both yaml_file and kind_dict are specified

    Example:
        with create_and_store_resource(
            client=client,
            fixture_store=store,
            resource=Namespace,
            name="my-namespace"
        ) as namespace:
            # Use the namespace
            pass
        # Namespace is cleaned up here (unless skip_teardown=True)
    """
    kwargs["client"] = client

    _resource_name = kwargs.get("name")
    _resource_dict = kwargs.get("kind_dict", {})
    _resource_yaml = kwargs.get("yaml_file")

    if _resource_yaml and _resource_dict:
        raise ValueError("Cannot specify both yaml_file and kind_dict")

    if not _resource_name:
        if _resource_yaml:
            with open(_resource_yaml) as fd:
                _resource_dict = yaml.safe_load(fd)

        _resource_name = _resource_dict.get("metadata", {}).get("name")

    if not _resource_name:
        _resource_name = generate_name_with_uuid(name=fixture_store["base_resource_name"])

        if resource.kind in (Migration.kind, Plan.kind):
            _resource_name = f"{_resource_name}-{'warm' if kwargs.get('warm_migration') else 'cold'}"

    if len(_resource_name) > 63:
        LOGGER.warning(f"'{_resource_name=}' is too long ({len(_resource_name)} > 63). Truncating.")
        _resource_name = _resource_name[-63:]

    # IMPORTANT: Set teardown=False - we handle cleanup manually in finally block
    kwargs["teardown"] = False
    kwargs["name"] = _resource_name

    _resource = resource(**kwargs)

    try:
        _resource.deploy(wait=True)
    except ConflictError:
        LOGGER.warning(f"{_resource.kind} {_resource_name} already exists, reusing it.")
        _resource.wait()

    LOGGER.info(f"Storing {_resource.kind} {_resource.name} in fixture store")
    _resource_dict = {"name": _resource.name, "namespace": _resource.namespace, "module": _resource.__module__}

    if test_name:
        _resource_dict["test_name"] = test_name

    fixture_store["teardown"].setdefault(_resource.kind, []).append(_resource_dict)

    try:
        yield _resource
    finally:
        # Cleanup when context exits (unless skip_teardown=True)
        if not skip_teardown:
            LOGGER.info(f"Context exit: Cleaning up {_resource.kind} {_resource.name}")
            try:
                _resource.clean_up(wait=True)
                # Mark as cleaned but KEEP in tracking for session teardown verification
                _resource_dict["cleaned_by_context"] = True
                LOGGER.info(f"✓ Cleaned up {_resource.kind} {_resource.name} (marked in tracking)")
            except Exception as cleanup_exc:
                LOGGER.error(f"Failed to clean up {_resource.kind} {_resource.name}: {cleanup_exc}")
                _resource_dict["cleanup_failed"] = True
                _resource_dict["cleanup_error"] = str(cleanup_exc)
                # Don't re-raise - let session teardown handle failed cleanups
        else:
            LOGGER.info(f"Skipping cleanup for {_resource.kind} {_resource.name} (skip_teardown=True)")

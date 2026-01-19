from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, TypeVar

import yaml
from kubernetes.dynamic import DynamicClient
from kubernetes.dynamic.exceptions import ConflictError
from ocp_resources.migration import Migration
from ocp_resources.plan import Plan
from ocp_resources.resource import Resource
from simple_logger.logger import get_logger

from utilities.naming import generate_name_with_uuid

LOGGER = get_logger(__name__)

T = TypeVar("T", bound=Resource)


@contextmanager
def create_and_store_resource(
    client: DynamicClient,
    fixture_store: dict[str, Any],
    resource: type[T],
    test_name: str | None = None,
    skip_teardown: bool | None = None,
    **kwargs: Any,
) -> Iterator[T]:
    """Context manager to create, track, and cleanup OpenShift resources.

    Creates a resource, tracks it in fixture_store, and automatically cleans it up
    when the context exits (unless skip_teardown=True).

    Args:
        client (DynamicClient): OpenShift DynamicClient
        fixture_store (dict[str, Any]): Session fixture store for tracking resources
        resource (type[T]): Resource class type to instantiate (generic)
        test_name (str | None): Optional test name for tracking
        skip_teardown (bool | None): If True, skip cleanup on context exit.
            If None (default), use value from fixture_store["skip_teardown"].
            Explicit True/False overrides the global setting.
        **kwargs (Any): Additional arguments passed to resource constructor

    Yields:
        T: The deployed resource instance (specific type, not base Resource)

    Raises:
        ValueError: If both yaml_file and kind_dict are specified
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

    # Use explicit parameter if provided, else get from fixture_store
    if skip_teardown is None:
        skip_teardown = fixture_store.get("skip_teardown", False)

    # Use Resource's built-in teardown based on skip_teardown parameter
    kwargs["teardown"] = not skip_teardown
    kwargs["name"] = _resource_name

    _resource = resource(**kwargs)

    try:
        # Normal case: we create the resource, Resource.__exit__() will clean it up
        with _resource as deployed:
            LOGGER.info("Storing %s %s in fixture store", deployed.kind, deployed.name)
            _resource_dict = {"name": deployed.name, "namespace": deployed.namespace, "module": deployed.__module__}
            if test_name:
                _resource_dict["test_name"] = test_name
            fixture_store["teardown"].setdefault(deployed.kind, []).append(_resource_dict)
            yield deployed
            # After yield, context exits and resource is cleaned up

            # Resource was cleaned up by context manager __exit__
            # Remove from tracking if teardown was enabled (resource was deleted)
            if kwargs.get("teardown", True):  # teardown=True means resource was deleted
                try:
                    fixture_store["teardown"][deployed.kind].remove(_resource_dict)
                    LOGGER.debug(
                        "Removed %s %s from teardown tracking (cleaned by context)", deployed.kind, deployed.name
                    )
                except (KeyError, ValueError):
                    pass  # Already removed or not in list

    except ConflictError:
        # Resource already exists - reuse it WITHOUT cleanup or tracking
        LOGGER.warning("%s %s already exists, reusing it.", _resource.kind, _resource_name)
        _resource.wait()
        # Don't add to teardown - we didn't create it, don't clean it up
        yield _resource

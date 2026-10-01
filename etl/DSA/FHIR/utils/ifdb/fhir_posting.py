# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Shared FHIR posting utilities for IFDB resources.

This module provides simplified, high-performance posting logic for FHIR resources.
After benchmarking, we've standardized on:
- ID Resolution: canonical identifier lookup
- Posting: Direct DB via executemany() (4-7x faster than FHIR API)

Legacy multi-strategy code has been removed. For reference implementations of
other strategies, see etl/tests/performance/.
"""

from datetime import UTC, datetime
from typing import Any

import dagster as dg
from core.resources import FHIRDatabaseResource
from DSA.FHIR.utils.reference_resolver import (
    NestedReferenceFieldConfig,
    ReferenceFieldConfig,
    ResolveValuesFn,
    batch_resolve_references,
    resolve_and_apply_all_references,
)
from fhir.resources.R4B.resource import Resource


def post_resources_direct(
    context: dg.AssetExecutionContext,
    resources: list[Resource],
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    source_name: str = "ifdb_oracle",
    extracted_count: int | None = None,
    invalidate_cache: bool = True,
) -> dg.MaterializeResult:
    """
    Post FHIR resources directly to the database using executemany().

    This is the recommended posting method - 4-7x faster than FHIR API posting.
    Uses batch_upsert_resources() which internally uses psycopg's executemany().

    Uses strict mode: any posting failure raises an exception.

    Args:
        context: Dagster execution context
        resources: List of FHIR resources to post
        fhir_database: FHIR database resource for direct DB access
        resource_type: Type of resource (e.g., "Patient", "EpisodeOfCare")
        source_name: Name of the data source for metadata
        extracted_count: Number of records extracted (for logging)
        invalidate_cache: When False, skip per-call identifier cache
            invalidation. Chunked callers can then invalidate once after the
            whole partition.

    Returns:
        MaterializeResult with posting statistics

    Raises:
        dg.Failure: If any resource fails to post
    """
    logger = dg.get_dagster_logger()
    partition_key = context.partition_key if context.has_partition_key else "N/A"

    if not resources:
        ext_info = f"{extracted_count} ext -> " if extracted_count is not None else ""
        logger.info(f"Summary[{partition_key}]: {ext_info}0 trans -> 0 posted (0 failed)")
        return dg.MaterializeResult(
            metadata={
                "posted_count": dg.MetadataValue.int(0),
                "failed_count": dg.MetadataValue.int(0),
                "source": dg.MetadataValue.text(source_name),
                "partition_key": dg.MetadataValue.text(partition_key),
                "method": dg.MetadataValue.text("direct_postgres"),
            }
        )

    logger.info(f"Posting {len(resources)} {resource_type} resources via direct DB")

    when = datetime.now(UTC)

    # Use batch_upsert_resources which uses executemany() internally
    successes, failures, failed_ids = fhir_database.batch_upsert_resources(
        resources=resources,
        resource_type=resource_type,
        invalidate_cache=invalidate_cache,
    )

    # Strict mode - any failure raises
    total = len(resources)
    if failures > 0:
        raise dg.Failure(
            description=f"Failed to post {failures} of {total} {resource_type} resources. "
            f"Failed IDs: {failed_ids[:10]}"
        )

    ext_info = f"{extracted_count} ext -> " if extracted_count is not None else ""
    logger.info(
        f"Summary[{partition_key}]: {ext_info}{total} trans -> {successes} posted ({failures} failed)"
    )

    return dg.MaterializeResult(
        metadata={
            "posted_count": dg.MetadataValue.int(successes),
            "failed_count": dg.MetadataValue.int(failures),
            "failed_ids": dg.MetadataValue.json(failed_ids[:100]),
            "partition_key": dg.MetadataValue.text(partition_key),
            "source": dg.MetadataValue.text(source_name),
            "method": dg.MetadataValue.text("direct_postgres"),
            "decorated_at": dg.MetadataValue.text(when.isoformat()),
        }
    )


def post_resource_dicts_direct(
    context: dg.AssetExecutionContext,
    resource_dicts: list[tuple[str, dict]],
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    source_name: str = "ifdb_oracle",
    extracted_count: int | None = None,
    invalidate_cache: bool = True,
) -> dg.MaterializeResult:
    """Post pre-built FHIR resource dicts directly via ``batch_upsert_resource_dicts``.

    This is the dict-pipeline equivalent of :func:`post_resources_direct` —
    it bypasses all Pydantic serialisation and uses orjson internally.
    """
    logger = dg.get_dagster_logger()
    partition_key = context.partition_key if context.has_partition_key else "N/A"

    if not resource_dicts:
        ext_info = f"{extracted_count} ext -> " if extracted_count is not None else ""
        logger.info(f"Summary[{partition_key}]: {ext_info}0 trans -> 0 posted (0 failed)")
        return dg.MaterializeResult(
            metadata={
                "posted_count": dg.MetadataValue.int(0),
                "failed_count": dg.MetadataValue.int(0),
                "source": dg.MetadataValue.text(source_name),
                "partition_key": dg.MetadataValue.text(partition_key),
                "method": dg.MetadataValue.text("direct_postgres_dict"),
            }
        )

    logger.info(f"Posting {len(resource_dicts)} {resource_type} dicts via direct DB (dict path)")

    when = datetime.now(UTC)

    successes, failures, failed_ids = fhir_database.batch_upsert_resource_dicts(
        resource_dicts=resource_dicts,
        resource_type=resource_type,
        invalidate_cache=invalidate_cache,
    )

    if failures > 0:
        raise dg.Failure(
            description=(
                f"Failed to post {failures} of {len(resource_dicts)} {resource_type} dicts. "
                f"Failed IDs: {failed_ids[:10]}"
            )
        )

    total = len(resource_dicts)
    ext_info = f"{extracted_count} ext -> " if extracted_count is not None else ""
    logger.info(
        f"Summary[{partition_key}]: {ext_info}{total} trans -> {successes} posted ({failures} failed)"
    )

    return dg.MaterializeResult(
        metadata={
            "posted_count": dg.MetadataValue.int(successes),
            "failed_count": dg.MetadataValue.int(failures),
            "failed_ids": dg.MetadataValue.json(failed_ids[:100]),
            "partition_key": dg.MetadataValue.text(partition_key),
            "source": dg.MetadataValue.text(source_name),
            "method": dg.MetadataValue.text("direct_postgres_dict"),
            "decorated_at": dg.MetadataValue.text(when.isoformat()),
        }
    )


def resolve_references_for_resource(
    resources: list[Resource],
    reference_configs: list[dict[str, Any]],
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext | None = None,
) -> dict[str, dict[str, str]]:
    """
    Resolve references for a list of resources using memory cache.

    Args:
        resources: List of FHIR resources to resolve references for
        reference_configs: List of reference configuration dicts with keys:
            - field_path: Path to the reference field (e.g., "subject.identifier")
            - resource_type: Referenced resource type (e.g., "Patient")
            - identifier_system: Identifier system URI
            - required: Whether the reference is required (default True)
        fhir_database: Database resource for memory cache resolution
        context: Optional execution context for logging

    Returns:
        Dict mapping reference config field_path to dict of identifier->FHIR ID
    """
    resolved_refs: dict[str, dict[str, str]] = {}

    for config in reference_configs:
        field_path = config["field_path"]
        resource_type = config["resource_type"]
        identifier_system = config["identifier_system"]

        # Collect unique identifiers from all resources
        identifiers: dict[str, str] = {}
        for resource in resources:
            identifier_value = _get_identifier_from_path(resource, field_path)
            if identifier_value:
                identifiers[identifier_value] = ""  # Placeholder

        if identifiers:
            resolved = batch_resolve_references(
                identifiers=identifiers,
                resource_type=resource_type,
                identifier_system=identifier_system,
                fhir_database=fhir_database,
            )
            resolved_refs[field_path] = resolved

            if context:
                context.log.info(
                    f"Resolved {len(resolved)}/{len(identifiers)} {resource_type} references"
                )

    return resolved_refs


def _get_identifier_from_path(resource: Resource, field_path: str) -> str | None:
    """
    Get identifier value from a resource using a dot-separated path.

    Args:
        resource: FHIR resource
        field_path: Dot-separated path (e.g., "subject.identifier")

    Returns:
        Identifier value as string, or None if not found
    """
    parts = field_path.split(".")
    current = resource

    for part in parts[:-1]:  # Navigate to parent of identifier
        if current is None:
            return None
        if isinstance(current, list):
            current = current[0] if current else None
        else:
            current = getattr(current, part, None)

    # Get the identifier value
    if current is None:
        return None

    identifier = getattr(current, "identifier", None)
    if identifier is None:
        return None

    return str(identifier.value) if hasattr(identifier, "value") else None


def _resolve_via_preloaded_cache(
    resources: list[Resource],
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    preferred_id_systems: list[str],
    *,
    force_reload: bool,
) -> dict[int, str | None]:
    """Resolve resource IDs via the full preloaded identifier cache.

    Builds (or reuses) a SharedIdentifierCache / in-memory dict covering
    all identifiers for the given systems, then resolves from that cache.
    Preferred for non-indexed resource types (e.g. Observation) where
    amortising a single cache build across many batches beats repeated
    sequential-scan queries.
    """
    lookup = fhir_database.get_identifier_lookup(
        resource_type=resource_type,
        identifier_systems=preferred_id_systems,
        force_reload=force_reload,
    )

    index_to_id: dict[int, str | None] = {}
    for idx, resource in enumerate(resources):
        identifiers = getattr(resource, "identifier", None)
        if not identifiers:
            continue
        for preferred_system in preferred_id_systems:
            for ident in identifiers:
                system = getattr(ident, "system", None)
                value = getattr(ident, "value", None)
                if system == preferred_system and value:
                    fhir_id = lookup.get((str(system), str(value)))
                    if fhir_id:
                        index_to_id[idx] = fhir_id
                        break
            if idx in index_to_id:
                break

    return index_to_id


def resolve_resource_ids_for_upsert(
    resources: list[Resource],
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    preferred_id_systems: list[str],
    *,
    use_cache_resolution: bool = False,
    force_reload: bool = False,
) -> list[Resource]:
    """Resolve FHIR server IDs for resources before upsert.

    Two resolution strategies are available:

    - **Default** (``use_cache_resolution=False``): targeted SQL via
      ``fhir_database.resolve_resource_ids()``.  Uses UNIQUE partial
      expression indexes for indexed types; fast, batch-scoped.

    - **Cache** (``use_cache_resolution=True``): preloads the full
      identifier cache via ``get_identifier_lookup()`` and resolves
      from the ``SharedIdentifierCache`` (SQLite on disk).  Preferred
      for non-indexed resource types like Observation where amortising
      a single cache build avoids repeated sequential scans.

    Args:
        resources: List of resources to resolve IDs for.
        fhir_database: Database resource for direct Postgres access.
        resource_type: Type of FHIR resource.
        preferred_id_systems: Identifier systems to use for resolution.
        use_cache_resolution: When True, resolve via the full preloaded
            identifier cache instead of targeted SQL.
        force_reload: Force cache reload (passed through to resolve).

    Returns:
        List of resources with IDs attached where found.
    """
    if not resources:
        return resources

    logger = dg.get_dagster_logger()
    logger.info(f"Resolving IDs for {len(resources)} {resource_type} resources")

    if use_cache_resolution:
        index_to_id = _resolve_via_preloaded_cache(
            resources=resources,
            fhir_database=fhir_database,
            resource_type=resource_type,
            preferred_id_systems=preferred_id_systems,
            force_reload=force_reload,
        )
    else:
        index_to_id = fhir_database.resolve_resource_ids(
            resources=resources,
            resource_type=resource_type,
            preferred_systems=preferred_id_systems,
            force_reload=force_reload,
        )

    for idx, fhir_id in index_to_id.items():
        if idx < len(resources):
            try:
                resources[idx].id = fhir_id
            except Exception:
                payload = resources[idx].dict()
                payload["id"] = fhir_id
                resources[idx] = resources[idx].__class__(**payload)

    logger.info(f"Resolved {len(index_to_id)}/{len(resources)} {resource_type} IDs")
    return resources


def prepare_resources_for_upsert(
    *,
    resources: list[Resource],
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    preferred_id_systems: list[str],
    reference_configs: list[ReferenceFieldConfig] | None = None,
    nested_reference_configs: list[NestedReferenceFieldConfig] | None = None,
    reference_resolve_fn: ResolveValuesFn | None = None,
    reference_fallback_fn: ResolveValuesFn | None = None,
    use_cache_resolution: bool = False,
    force_reload: bool = False,
    logger: Any | None = None,
) -> list[Resource]:
    """Resolve configured references and attach existing FHIR IDs for upsert.

    This is the shared orchestration layer for posting assets. Resource-specific
    modules still own their declarative reference configs and preferred
    business identifier systems; the resolution mechanics stay here.

    Args:
        reference_resolve_fn: Optional resolver for configured references.
            Leave unset for the default DB path. Partition-scoped callers can
            pass a preloaded reference resolver without changing self-ID lookup.
        reference_fallback_fn: Optional secondary resolver tried only for
            references the primary lookup left unresolved (e.g. the patient-id →
            CPR bridge from ``build_patient_cpr_fallback``). Self-filtering, so
            it is safe to pass regardless of which references a resource has.
        use_cache_resolution: When True, resolve self-IDs via the full
            preloaded identifier cache (SharedIdentifierCache / SQLite)
            instead of targeted SQL.  See
            :func:`resolve_resource_ids_for_upsert` for details.
        force_reload: Force cache reload (passed through to resolve).
    """
    log = logger or dg.get_dagster_logger()

    if reference_configs or nested_reference_configs:
        result = resolve_and_apply_all_references(
            resources=resources,
            reference_configs=reference_configs or [],
            fhir_database=fhir_database,
            nested_reference_configs=nested_reference_configs,
            resolve_fn=reference_resolve_fn,
            force_reload=force_reload,
            fallback_resolve_fn=reference_fallback_fn,
        )

        log.info(
            f"Reference resolution: {result.total_resolved} resolved, "
            f"{result.total_unresolved} unresolved ({result.required_unresolved} required)"
        )

        if result.nested_results:
            for path, nested_result in result.nested_results.items():
                if nested_result.total_resolved > 0 or nested_result.total_unresolved > 0:
                    log.info(
                        f"  Nested '{path}': {nested_result.total_resolved} resolved, "
                        f"{nested_result.total_unresolved} unresolved"
                    )

    return resolve_resource_ids_for_upsert(
        resources=resources,
        fhir_database=fhir_database,
        resource_type=resource_type,
        preferred_id_systems=preferred_id_systems,
        use_cache_resolution=use_cache_resolution,
        force_reload=force_reload,
    )


def validate_references_exist(
    resources: list[Resource],
    resolved_refs: dict[str, dict[str, str]],
    reference_configs: list[dict[str, Any]],
    context: dg.AssetExecutionContext,
) -> list[Resource]:
    """
    Validate that required references exist and filter out resources with missing refs.

    Args:
        resources: List of FHIR resources
        resolved_refs: Dictionary of resolved references by field path
        reference_configs: Reference configuration list
        context: Execution context for logging

    Returns:
        List of resources with all required references resolved

    Raises:
        ValueError: If a required reference cannot be resolved
    """
    valid_resources = []

    for resource in resources:
        is_valid = True

        for config in reference_configs:
            field_path = config["field_path"]
            required = config.get("required", True)

            identifier_value = _get_identifier_from_path(resource, field_path)
            if identifier_value:
                resolved = resolved_refs.get(field_path, {})
                fhir_id = resolved.get(identifier_value)

                if not fhir_id:
                    if required:
                        raise ValueError(
                            f"Could not resolve required {config['resource_type']} "
                            f"reference with identifier {identifier_value}"
                        )
                    else:
                        context.log.warning(
                            f"Could not resolve optional {config['resource_type']} "
                            f"reference with identifier {identifier_value}"
                        )
                        is_valid = False

        if is_valid:
            valid_resources.append(resource)

    return valid_resources

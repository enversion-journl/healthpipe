# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Reference resolution utilities for FHIR resources.

This module provides batch resolution of references between FHIR resources
using the central identifier lookup facade with targeted SQL filtering.

Memory Optimization:
    The FHIR identifier lookup facade filters at the SQL level, loading only
    identifiers for the specified systems. This significantly reduces:
    - Data transferred over the network
    - Python objects to parse and filter
    - Memory usage (only stores relevant identifiers)

After benchmarking, direct database-backed identifier lookup is 18,000x+ faster
than resolving through the FHIR API.

Key Features:
- Supports top-level references (e.g., subject, author)
- Supports nested references (e.g., activity.detail.performer) via dot-notation paths
- Supports mixed-type reference lists (e.g., performer with both Organization and Practitioner)
  by using Reference.type to determine target resource type
- Extension-based references (e.g., extension.valueReference)

Main Entry Points:
- resolve_and_apply_all_references(): Resolve all configured references for a batch
- ReferenceFieldConfig: Configuration for top-level reference fields
- NestedReferenceFieldConfig: Configuration for nested/extension reference fields
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

import dagster as dg
from core.resources import FHIRDatabaseResource
from DSA.FHIR.utils.gdpr_masking import mask_identifier_for_logging
from fhir.resources.R4B.reference import Reference
from fhir.resources.R4B.resource import Resource

ResolveValuesFn = Callable[..., dict[str, str]]


# =============================================================================
# Result Dataclasses
# =============================================================================


@dataclass
class ReferenceResolutionResult:
    """Result of batch reference resolution."""

    resolved_count: int
    unresolved_count: int
    unresolved_identifiers: list[str]


@dataclass
class ResourceResolutionResult:
    """Result of resolving resource FHIR IDs for upsert."""

    existing_count: int  # Resources that already exist (will be updated)
    new_count: int  # Resources that are new (will be inserted)
    resolved_ids: dict[str, str] = field(default_factory=dict)  # identifier -> FHIR ID


@dataclass
class NestedReferenceResolutionResult:
    """Result of resolving nested references for a batch of resources."""

    # Results per resource type within the nested field
    type_results: dict[str, ReferenceResolutionResult] = field(default_factory=dict)

    # Summary counts across all types
    total_resolved: int = 0
    total_unresolved: int = 0


@dataclass
class AllReferencesResolutionResult:
    """Result of resolving all references for a batch of resources."""

    # Results per reference field (top-level references)
    field_results: dict[str, ReferenceResolutionResult] = field(default_factory=dict)

    # Results per nested reference field path
    nested_results: dict[str, NestedReferenceResolutionResult] = field(default_factory=dict)

    # Summary counts
    total_resolved: int = 0
    total_unresolved: int = 0
    required_unresolved: int = 0  # Critical - will cause failure


# =============================================================================
# Configuration Dataclasses
# =============================================================================


@dataclass
class ReferenceFieldConfig:
    """
    Configuration for a TOP-LEVEL reference field to resolve.

    Use this for simple, top-level reference fields like "subject", "patient", "author".

    For nested references (e.g., "activity.detail.performer"), use NestedReferenceFieldConfig.

    Examples:
        # Patient reference (required) - field name varies by resource type
        ReferenceFieldConfig(
            field_name="subject",  # or "patient" for EpisodeOfCare
            target_resource_type="Patient",
            identifier_system="urn:source:ifdb:patient_id",
            is_required=True,
        )

        # Organization reference (optional)
        ReferenceFieldConfig(
            field_name="author",
            target_resource_type="Organization",
            identifier_system="https://sundhedsdatastyrelsen.dk/sor",
            is_required=False,
        )
    """

    # Attribute name on the resource (e.g., "subject", "patient", "author")
    field_name: str

    # Target resource type for resolution
    target_resource_type: str  # "Patient", "Organization", etc.

    # Identifier system for the reference
    identifier_system: str

    # If True, fail if cannot resolve; if False, log warning and keep identifier
    is_required: bool = True

    # Optional description for logging
    description: str = ""


@dataclass
class NestedReferenceFieldConfig:
    """
    Configuration for a NESTED reference field to resolve.

    Use this for reference fields that are nested within the resource structure,
    such as "activity.detail.performer" in CarePlan or "extension.valueReference".

    Supports mixed-type reference lists where a single field (like performer)
    can contain references to different resource types (e.g., both Organization
    and Practitioner). The target type is determined by either:
    1. The Reference.type field (preferred - should be set in staging asset)
    2. The identifier_system_to_type mapping (fallback)

    Special handling for extensions:
    - Use field_path="extension.valueReference" for extension-based references
    - Optionally set extension_url to filter by specific extension URL

    Examples:
        # CarePlan.activity.detail.performer (mixed types)
        NestedReferenceFieldConfig(
            field_path="activity.detail.performer",
            identifier_system_to_type={
                "https://sundhedsdatastyrelsen.dk/sor": "Organization",
                "urn:source:ifdb:staff": "Practitioner",
            },
            is_required=False,
            description="Activity performer (Organization or Practitioner)",
        )

        # EpisodeOfCare extension reference
        from utils.fhir.fhir_extensions import EXTENSION_PREVIOUS_EPISODE_OF_CARE

        NestedReferenceFieldConfig(
            field_path="extension.valueReference",
            identifier_system_to_type={
                "urn:source:ifdb:episode_of_care_id": "EpisodeOfCare",
            },
            extension_url=EXTENSION_PREVIOUS_EPISODE_OF_CARE,
            is_required=False,
            description="Previous episode of care reference",
        )
    """

    # Dot-notation path to the reference field (e.g., "activity.detail.performer")
    field_path: str

    # Mapping from identifier system to target resource type
    # This allows mixed-type references where the same field can reference
    # different resource types (e.g., performer can be Organization OR Practitioner)
    identifier_system_to_type: dict[str, str] = field(default_factory=dict)

    # If True, fail if cannot resolve; if False, log warning and keep identifier
    is_required: bool = False

    # Optional description for logging
    description: str = ""

    # Optional: Filter extension references by URL (for extension.valueReference paths)
    extension_url: str | None = None

    def get_target_type_for_system(self, identifier_system: str) -> str | None:
        """Get the target resource type for a given identifier system."""
        return self.identifier_system_to_type.get(identifier_system)

    def get_all_target_types(self) -> set[str]:
        """Get all possible target resource types for this field."""
        return set(self.identifier_system_to_type.values())

    def get_systems_for_type(self, resource_type: str) -> list[str]:
        """Get all identifier systems that map to a given resource type."""
        return [
            system
            for system, rtype in self.identifier_system_to_type.items()
            if rtype == resource_type
        ]


# =============================================================================
# Core Resolution Functions (Memory Cache Only)
# =============================================================================


def batch_resolve_references(
    identifiers: dict[str, str],
    resource_type: str,
    identifier_system: str,
    fhir_database: FHIRDatabaseResource,
    force_reload: bool = False,
    resolve_fn: ResolveValuesFn | None = None,
) -> dict[str, str]:
    """
    Batch resolve identifier values to FHIR server IDs.

    Uses the targeted SQL filtering path by default. When ``resolve_fn`` is
    provided, it is used instead of ``fhir_database.resolve_identifier_values``.

    Args:
        identifiers: Dictionary mapping identifier values to be resolved (values are placeholders)
        resource_type: Type of FHIR resource (e.g., "Patient", "EpisodeOfCare")
        identifier_system: Identifier system URI
        fhir_database: FHIR database resource for the default backend
        force_reload: Force a fresh lookup after upstream mutations. Ignored
            when ``resolve_fn`` is supplied.
        resolve_fn: Optional callable matching ``resolve_identifier_values``.

    Returns:
        Dictionary mapping identifier values to resolved FHIR server IDs
    """
    if not identifiers:
        return {}

    logger = dg.get_dagster_logger()
    identifier_values = list(identifiers.keys())

    logger.info(f"Resolving {len(identifier_values)} {resource_type} references")

    if resolve_fn is not None:
        resolved_ids = resolve_fn(
            resource_type=resource_type,
            identifier_system=identifier_system,
            values=identifier_values,
        )
    else:
        resolved_ids = fhir_database.resolve_identifier_values(
            resource_type=resource_type,
            identifier_system=identifier_system,
            values=identifier_values,
            force_reload=force_reload,
        )

    resolved_count = len(resolved_ids)
    unresolved_count = len(identifier_values) - resolved_count

    logger.info(f"Resolved {resolved_count}/{len(identifier_values)} {resource_type} references")

    if unresolved_count > 0:
        unresolved = [v for v in identifier_values if v not in resolved_ids]
        # Mask sensitive identifiers in the unresolved list
        masked_unresolved = [
            mask_identifier_for_logging(v, identifier_system) for v in unresolved[:5]
        ]
        logger.warning(
            f"{unresolved_count} {resource_type} references could not be resolved: "
            f"{masked_unresolved}{('... (showing first 5, masked)' + f' +{len(unresolved) - 5} more' if len(unresolved) > 5 else ' (masked)')}"
        )

    return resolved_ids


def _apply_fallback_resolution(
    *,
    resolved_ids: dict[str, str],
    all_identifiers: Any,
    resource_type: str,
    identifier_system: str,
    fallback_resolve_fn: ResolveValuesFn | None,
) -> None:
    """Top up ``resolved_ids`` with a secondary resolver for unresolved values.

    Mutates ``resolved_ids`` in place. Tried only when a ``fallback_resolve_fn``
    is supplied and the primary lookup left identifiers unresolved — so it adds
    no overhead on the common all-resolved path. The fallback is responsible for
    self-filtering on ``(resource_type, identifier_system)`` and returning only
    the supplemental mappings it can recover (e.g. patient-id → CPR → FHIR id).
    """
    if fallback_resolve_fn is None:
        return

    unresolved = [v for v in all_identifiers if v not in resolved_ids]
    if not unresolved:
        return

    supplemental = fallback_resolve_fn(
        resource_type=resource_type,
        identifier_system=identifier_system,
        values=unresolved,
    )
    if supplemental:
        resolved_ids.update(supplemental)


# =============================================================================
# Reference Field Helpers
# =============================================================================


def _get_reference_from_resource(
    resource: Resource,
    field_name: str,
) -> Reference | list[Reference] | None:
    """
    Get a reference field from a resource by attribute name.

    Handles both single references and lists of references.

    Args:
        resource: FHIR resource
        field_name: Attribute name (e.g., "subject", "patient", "episodeOfCare")

    Returns:
        Reference, list of References, or None
    """
    return getattr(resource, field_name, None)


def _extract_identifier_from_reference(
    ref: Reference,
    identifier_system: str,
) -> str | None:
    """
    Extract identifier value from a Reference object.

    Args:
        ref: FHIR Reference object
        identifier_system: Expected identifier system

    Returns:
        Identifier value or None
    """
    if not ref or not hasattr(ref, "identifier") or not ref.identifier:
        return None

    # Check if system matches (if specified on the identifier)
    if ref.identifier.system and ref.identifier.system != identifier_system:
        return None

    return str(ref.identifier.value) if ref.identifier.value else None


def _apply_fhir_id_to_reference(
    ref: Reference,
    fhir_id: str,
    resource_type: str,
) -> None:
    """
    Apply a resolved FHIR ID to a Reference object.

    Transforms from identifier-based to direct reference:
    {identifier: {...}} → {id: "uuid", resourceType: "ResourceType"}

    Args:
        ref: Reference object to update
        fhir_id: Resolved FHIR UUID
        resource_type: Target resource type (e.g., "Patient")
    """
    ref.id = fhir_id
    ref.type = resource_type
    ref.identifier = None  # Clear identifier since we have direct reference


# =============================================================================
# Top-level Reference Resolution
# =============================================================================


def collect_reference_identifiers(
    resources: Sequence[Resource],
    config: ReferenceFieldConfig,
) -> dict[str, str]:
    """
    Collect all unique identifier values for a reference field across resources.

    Args:
        resources: List of FHIR resources
        config: Reference field configuration

    Returns:
        Dict mapping identifier values to empty strings (for batch resolution)
    """
    identifiers: dict[str, str] = {}

    for resource in resources:
        ref_field = _get_reference_from_resource(resource, config.field_name)

        if ref_field is None:
            continue

        # Handle single reference or list of references
        refs_to_check = ref_field if isinstance(ref_field, list) else [ref_field]

        for ref in refs_to_check:
            if isinstance(ref, Reference):
                identifier_value = _extract_identifier_from_reference(ref, config.identifier_system)
                if identifier_value:
                    identifiers[identifier_value] = ""
            # Handle EncounterLocation and similar nested structures
            elif hasattr(ref, "location") and isinstance(getattr(ref, "location", None), Reference):
                loc_ref = ref.location
                identifier_value = _extract_identifier_from_reference(
                    loc_ref, config.identifier_system
                )
                if identifier_value:
                    identifiers[identifier_value] = ""

    return identifiers


def apply_reference_fhir_ids(
    resources: Sequence[Resource],
    config: ReferenceFieldConfig,
    resolved_ids: dict[str, str],
) -> ReferenceResolutionResult:
    """
    Apply resolved FHIR IDs to a specific reference field across resources.

    Args:
        resources: List of FHIR resources
        config: Reference field configuration
        resolved_ids: Dict mapping identifier values to FHIR UUIDs

    Returns:
        ReferenceResolutionResult with statistics

    Raises:
        ValueError: If is_required=True and a reference cannot be resolved
    """
    resolved_count = 0
    unresolved_identifiers: list[str] = []

    for resource in resources:
        ref_field = _get_reference_from_resource(resource, config.field_name)

        if ref_field is None:
            continue

        # Handle single reference or list of references
        refs_to_check = ref_field if isinstance(ref_field, list) else [ref_field]

        for ref in refs_to_check:
            # Handle direct Reference
            if isinstance(ref, Reference):
                identifier_value = _extract_identifier_from_reference(ref, config.identifier_system)

                if not identifier_value:
                    continue

                fhir_id = resolved_ids.get(identifier_value)

                if fhir_id:
                    _apply_fhir_id_to_reference(ref, fhir_id, config.target_resource_type)
                    resolved_count += 1
                else:
                    if config.is_required:
                        raise ValueError(
                            f"{config.target_resource_type} not found for identifier {identifier_value} "
                            f"(field: {config.field_name}, required: True)"
                        )
                    else:
                        if identifier_value not in unresolved_identifiers:
                            unresolved_identifiers.append(identifier_value)

            # Handle EncounterLocation and similar nested structures
            elif hasattr(ref, "location") and isinstance(getattr(ref, "location", None), Reference):
                loc_ref = ref.location
                identifier_value = _extract_identifier_from_reference(
                    loc_ref, config.identifier_system
                )

                if not identifier_value:
                    continue

                fhir_id = resolved_ids.get(identifier_value)

                if fhir_id:
                    _apply_fhir_id_to_reference(loc_ref, fhir_id, config.target_resource_type)
                    resolved_count += 1
                else:
                    if config.is_required:
                        raise ValueError(
                            f"{config.target_resource_type} not found for identifier {identifier_value} "
                            f"(field: {config.field_name}, required: True)"
                        )
                    else:
                        if identifier_value not in unresolved_identifiers:
                            unresolved_identifiers.append(identifier_value)

    return ReferenceResolutionResult(
        resolved_count=resolved_count,
        unresolved_count=len(unresolved_identifiers),
        unresolved_identifiers=unresolved_identifiers,
    )


def resolve_and_apply_reference(
    resources: Sequence[Resource],
    config: ReferenceFieldConfig,
    fhir_database: FHIRDatabaseResource,
    resolve_fn: ResolveValuesFn | None = None,
    force_reload: bool = False,
    fallback_resolve_fn: ResolveValuesFn | None = None,
) -> ReferenceResolutionResult:
    """
    Resolve and apply a single reference field for a batch of resources.

    Args:
        resources: List of FHIR resources
        config: Reference field configuration
        fhir_database: FHIR database resource
        resolve_fn: Optional partition-scoped resolver; see
            :func:`batch_resolve_references`.
        force_reload: Force a fresh lookup after upstream mutations (e.g.
            dependency resolution persisted new identifiers via CPR-bridge).
        fallback_resolve_fn: Optional secondary resolver tried only for
            identifiers the primary lookup left unresolved (e.g. the
            patient-id → CPR bridge). It receives the unresolved values and
            returns any supplemental ``identifier -> FHIR id`` it can recover;
            see :func:`_apply_fallback_resolution`.

    Returns:
        ReferenceResolutionResult with statistics

    Raises:
        ValueError: If is_required=True and a reference cannot be resolved
    """
    logger = dg.get_dagster_logger()

    # Step 1: Collect identifiers
    identifiers = collect_reference_identifiers(resources, config)

    if not identifiers:
        logger.debug(f"No {config.field_name} references found")
        return ReferenceResolutionResult(
            resolved_count=0,
            unresolved_count=0,
            unresolved_identifiers=[],
        )

    logger.info(
        f"Resolving {len(identifiers)} {config.target_resource_type} references "
        f"(field: {config.field_name}, required: {config.is_required})"
    )

    # Step 2: Batch resolve
    resolved_ids = batch_resolve_references(
        identifiers=identifiers,
        resource_type=config.target_resource_type,
        identifier_system=config.identifier_system,
        fhir_database=fhir_database,
        resolve_fn=resolve_fn,
        force_reload=force_reload,
    )

    # Step 2b: Fallback for anything still unresolved (e.g. patient-id → CPR).
    _apply_fallback_resolution(
        resolved_ids=resolved_ids,
        all_identifiers=identifiers.keys(),
        resource_type=config.target_resource_type,
        identifier_system=config.identifier_system,
        fallback_resolve_fn=fallback_resolve_fn,
    )

    # Step 3: Apply resolved IDs
    return apply_reference_fhir_ids(resources, config, resolved_ids)


# =============================================================================
# Nested Reference Resolution (for fields like activity.detail.performer)
# =============================================================================


def _navigate_nested_path(
    obj: Any,
    path_parts: list[str],
    current_index: int = 0,
) -> list[tuple[Any, str, int | None]]:
    """
    Navigate a nested path through an object, yielding all leaf objects and their final attribute.

    Handles lists at any level in the path.

    Args:
        obj: Current object to navigate
        path_parts: List of attribute names (e.g., ["activity", "detail", "performer"])
        current_index: Current index in path_parts

    Returns:
        List of tuples: (parent_object, final_attribute_name, list_index_if_applicable)
        The list_index is only set when the final attribute itself is in a list.

    Example:
        For path "activity.detail.performer" on a CarePlan:
        - Navigates through activity (list) -> detail -> performer (list)
        - Returns all (detail_object, "performer", None) pairs
    """
    if current_index >= len(path_parts):
        return []

    current_attr = path_parts[current_index]
    is_last = current_index == len(path_parts) - 1

    # Get the attribute value
    if hasattr(obj, current_attr):
        attr_value = getattr(obj, current_attr)
    elif isinstance(obj, dict):
        attr_value = obj.get(current_attr)
    else:
        return []

    if attr_value is None:
        return []

    # If this is the last part of the path, return the parent and attribute name
    if is_last:
        return [(obj, current_attr, None)]

    # If the attribute is a list, recurse into each element
    if isinstance(attr_value, list):
        results = []
        for item in attr_value:
            results.extend(_navigate_nested_path(item, path_parts, current_index + 1))
        return results
    else:
        # Continue recursion with the single value
        return _navigate_nested_path(attr_value, path_parts, current_index + 1)


def _get_all_references_from_nested_path(
    resource: Resource,
    field_path: str,
    extension_url: str | None = None,
) -> list[Reference]:
    """
    Get all Reference objects from a nested path in a resource.

    Handles lists at any level in the path.

    Args:
        resource: FHIR resource
        field_path: Dot-notation path (e.g., "activity.detail.performer", "extension.valueReference")
        extension_url: Optional URL to filter extensions by

    Returns:
        List of all Reference objects found at the path
    """
    path_parts = field_path.split(".")
    results = []

    # Special handling for extension.valueReference pattern
    if path_parts[0] == "extension" and len(path_parts) >= 2:
        extensions = getattr(resource, "extension", None)
        if extensions:
            for ext in extensions:
                # Filter by extension URL if specified
                if extension_url and ext.url != extension_url:
                    continue

                # Navigate remaining path from extension
                if len(path_parts) == 2:
                    # Direct attribute on extension (e.g., valueReference)
                    attr_value = getattr(ext, path_parts[1], None)
                    if isinstance(attr_value, Reference):
                        results.append(attr_value)
                else:
                    # Deeper path - use navigation
                    sub_locations = _navigate_nested_path(ext, path_parts[1:])
                    for parent, attr_name, _ in sub_locations:
                        attr_value = (
                            getattr(parent, attr_name, None) if hasattr(parent, attr_name) else None
                        )
                        if isinstance(attr_value, Reference):
                            results.append(attr_value)
                        elif isinstance(attr_value, list):
                            for item in attr_value:
                                if isinstance(item, Reference):
                                    results.append(item)
        return results

    # Standard path navigation
    locations = _navigate_nested_path(resource, path_parts)

    for parent, attr_name, _ in locations:
        attr_value = getattr(parent, attr_name, None) if hasattr(parent, attr_name) else None

        if attr_value is None:
            continue

        if isinstance(attr_value, list):
            for item in attr_value:
                if isinstance(item, Reference):
                    results.append(item)
        elif isinstance(attr_value, Reference):
            results.append(attr_value)

    return results


def collect_nested_reference_identifiers(
    resources: Sequence[Resource],
    config: NestedReferenceFieldConfig,
) -> dict[str, dict[str, str]]:
    """
    Collect all unique identifier values for a nested reference field across resources.

    Groups identifiers by target resource type (determined by Reference.type or identifier system).

    Args:
        resources: List of FHIR resources
        config: Nested reference field configuration

    Returns:
        Dict mapping resource_type -> {identifier_value -> ""}
        Example: {"Organization": {"123": "", "456": ""}, "Practitioner": {"789": ""}}
    """
    # Group identifiers by target resource type
    identifiers_by_type: dict[str, dict[str, str]] = {}

    for resource in resources:
        refs = _get_all_references_from_nested_path(
            resource, config.field_path, config.extension_url
        )

        for ref in refs:
            if not ref or not hasattr(ref, "identifier") or not ref.identifier:
                continue

            identifier_value = str(ref.identifier.value) if ref.identifier.value else None
            if not identifier_value:
                continue

            # Determine target resource type from Reference.type (preferred) or identifier system
            target_type = None

            # First, try to get type from Reference.type
            if hasattr(ref, "type") and ref.type:
                target_type = ref.type

            # Fallback: determine type from identifier system
            if not target_type and ref.identifier.system:
                target_type = config.get_target_type_for_system(ref.identifier.system)

            if not target_type:
                # Cannot determine target type - skip this reference
                continue

            # Add to the correct type bucket
            if target_type not in identifiers_by_type:
                identifiers_by_type[target_type] = {}
            identifiers_by_type[target_type][identifier_value] = ""

    return identifiers_by_type


def apply_nested_reference_fhir_ids(
    resources: Sequence[Resource],
    config: NestedReferenceFieldConfig,
    resolved_ids_by_type: dict[str, dict[str, str]],
) -> NestedReferenceResolutionResult:
    """
    Apply resolved FHIR IDs to nested reference fields across resources.

    Args:
        resources: List of FHIR resources
        config: Nested reference field configuration
        resolved_ids_by_type: Dict mapping resource_type -> {identifier_value -> fhir_id}

    Returns:
        NestedReferenceResolutionResult with statistics per type

    Raises:
        ValueError: If is_required=True and a reference cannot be resolved
    """
    result = NestedReferenceResolutionResult()

    # Track counts per type
    resolved_per_type: dict[str, int] = {}
    unresolved_per_type: dict[str, list[str]] = {}

    for resource in resources:
        refs = _get_all_references_from_nested_path(
            resource, config.field_path, config.extension_url
        )

        for ref in refs:
            if not ref or not hasattr(ref, "identifier") or not ref.identifier:
                continue

            identifier_value = str(ref.identifier.value) if ref.identifier.value else None
            if not identifier_value:
                continue

            # Determine target resource type
            target_type = None

            if hasattr(ref, "type") and ref.type:
                target_type = ref.type
            elif ref.identifier.system:
                target_type = config.get_target_type_for_system(ref.identifier.system)

            if not target_type:
                continue

            # Try to resolve
            resolved_ids = resolved_ids_by_type.get(target_type, {})
            fhir_id = resolved_ids.get(identifier_value)

            if fhir_id:
                # Apply the resolved ID
                _apply_fhir_id_to_reference(ref, fhir_id, target_type)

                if target_type not in resolved_per_type:
                    resolved_per_type[target_type] = 0
                resolved_per_type[target_type] += 1
            else:
                # Could not resolve
                if target_type not in unresolved_per_type:
                    unresolved_per_type[target_type] = []
                if identifier_value not in unresolved_per_type[target_type]:
                    unresolved_per_type[target_type].append(identifier_value)

                if config.is_required:
                    raise ValueError(
                        f"{target_type} not found for identifier {identifier_value} "
                        f"(path: {config.field_path}, required: True)"
                    )

    # Build result
    for target_type in set(list(resolved_per_type.keys()) + list(unresolved_per_type.keys())):
        resolved_count = resolved_per_type.get(target_type, 0)
        unresolved_list = unresolved_per_type.get(target_type, [])

        result.type_results[target_type] = ReferenceResolutionResult(
            resolved_count=resolved_count,
            unresolved_count=len(unresolved_list),
            unresolved_identifiers=unresolved_list,
        )
        result.total_resolved += resolved_count
        result.total_unresolved += len(unresolved_list)

    return result


def resolve_and_apply_nested_references(
    resources: Sequence[Resource],
    config: NestedReferenceFieldConfig,
    fhir_database: FHIRDatabaseResource,
    resolve_fn: ResolveValuesFn | None = None,
    force_reload: bool = False,
    fallback_resolve_fn: ResolveValuesFn | None = None,
) -> NestedReferenceResolutionResult:
    """
    Resolve and apply nested references for a batch of resources.

    This handles nested paths (e.g., "activity.detail.performer") and mixed-type
    reference lists (e.g., performer containing both Organization and Practitioner).

    Process:
    1. Collect all identifiers from nested path, grouped by target type
    2. Batch resolve each type separately
    3. Apply resolved IDs to references

    Args:
        resources: List of FHIR resources
        config: Nested reference field configuration
        fhir_database: FHIR database resource
        resolve_fn: Optional partition-scoped resolver; see
            :func:`batch_resolve_references`.
        force_reload: Force a fresh lookup after upstream mutations.

    Returns:
        NestedReferenceResolutionResult with statistics per type

    Raises:
        ValueError: If is_required=True and a reference cannot be resolved
    """
    logger = dg.get_dagster_logger()

    # Step 1: Collect identifiers grouped by target type
    identifiers_by_type = collect_nested_reference_identifiers(resources, config)

    if not identifiers_by_type:
        logger.debug(f"No nested references found at path: {config.field_path}")
        return NestedReferenceResolutionResult()

    # Step 2: Batch resolve each type
    resolved_ids_by_type: dict[str, dict[str, str]] = {}

    for target_type, identifiers in identifiers_by_type.items():
        if not identifiers:
            continue

        # Get identifier systems for this type
        systems = config.get_systems_for_type(target_type)
        if not systems:
            logger.warning(f"No identifier system configured for type {target_type}")
            continue

        # Use the first system (should typically only be one per type)
        identifier_system = systems[0]

        logger.info(
            f"Resolving {len(identifiers)} {target_type} references "
            f"(path: {config.field_path}, required: {config.is_required})"
        )

        resolved_ids = batch_resolve_references(
            identifiers=identifiers,
            resource_type=target_type,
            identifier_system=identifier_system,
            fhir_database=fhir_database,
            resolve_fn=resolve_fn,
            force_reload=force_reload,
        )

        # Fallback for anything still unresolved (e.g. patient-id → CPR).
        _apply_fallback_resolution(
            resolved_ids=resolved_ids,
            all_identifiers=identifiers.keys(),
            resource_type=target_type,
            identifier_system=identifier_system,
            fallback_resolve_fn=fallback_resolve_fn,
        )

        resolved_ids_by_type[target_type] = resolved_ids

    # Step 3: Apply resolved IDs
    return apply_nested_reference_fhir_ids(resources, config, resolved_ids_by_type)


# =============================================================================
# Main Entry Point
# =============================================================================


def resolve_and_apply_all_references(
    resources: Sequence[Resource],
    reference_configs: list[ReferenceFieldConfig],
    fhir_database: FHIRDatabaseResource,
    nested_reference_configs: list[NestedReferenceFieldConfig] | None = None,
    resolve_fn: ResolveValuesFn | None = None,
    force_reload: bool = False,
    fallback_resolve_fn: ResolveValuesFn | None = None,
) -> AllReferencesResolutionResult:
    """
    Resolve and apply all configured references for a batch of resources.

    This is the main entry point for reference resolution in posting assets.

    Process:
    1. For each top-level reference config, collect identifiers from resources
    2. Batch resolve identifiers to FHIR IDs using memory cache
    3. Apply resolved IDs to references
    4. For each nested reference config, do the same with nested path navigation
    5. For required references, fail if any cannot be resolved
    6. For optional references, log warning and keep identifier reference

    Args:
        resources: List of FHIR resources
        reference_configs: List of top-level reference field configurations
        fhir_database: FHIR database resource
        nested_reference_configs: List of nested reference field configurations (optional)
        resolve_fn: Optional partition-scoped resolver; see
            :func:`batch_resolve_references`.
        force_reload: Force a fresh lookup after upstream mutations (e.g.
            dependency resolution persisted new identifiers via CPR-bridge).
            Propagated to all :func:`batch_resolve_references` calls.
        fallback_resolve_fn: Optional secondary resolver tried for identifiers
            the primary lookup left unresolved, before the required-reference
            check raises. Self-filters on ``(resource_type, identifier_system)``
            so a single instance (e.g. the patient-id → CPR bridge from
            ``build_patient_cpr_fallback``) can be passed for every config.

    Returns:
        AllReferencesResolutionResult with per-field and summary statistics

    Raises:
        ValueError: If any required reference cannot be resolved

    Example:
        ENCOUNTER_REFERENCE_CONFIGS = [
            ReferenceFieldConfig(
                field_name="subject",
                target_resource_type="Patient",
                identifier_system=SYSTEM_IFDB_PATIENT_ID,
                is_required=True,
            ),
            ReferenceFieldConfig(
                field_name="episodeOfCare",
                target_resource_type="EpisodeOfCare",
                identifier_system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                is_required=False,
            ),
        ]

        result = resolve_and_apply_all_references(
            resources=encounters,
            reference_configs=ENCOUNTER_REFERENCE_CONFIGS,
            fhir_database=fhir_database,
        )
    """
    logger = dg.get_dagster_logger()
    result = AllReferencesResolutionResult()

    # Process top-level references
    for config in reference_configs:
        field_result = resolve_and_apply_reference(
            resources=resources,
            config=config,
            fhir_database=fhir_database,
            resolve_fn=resolve_fn,
            force_reload=force_reload,
            fallback_resolve_fn=fallback_resolve_fn,
        )

        result.field_results[config.field_name] = field_result
        result.total_resolved += field_result.resolved_count
        result.total_unresolved += field_result.unresolved_count

        if config.is_required and field_result.unresolved_count > 0:
            result.required_unresolved += field_result.unresolved_count

    # Process nested references if provided
    if nested_reference_configs:
        for nested_config in nested_reference_configs:
            nested_result = resolve_and_apply_nested_references(
                resources=resources,
                config=nested_config,
                fhir_database=fhir_database,
                resolve_fn=resolve_fn,
                force_reload=force_reload,
                fallback_resolve_fn=fallback_resolve_fn,
            )

            result.nested_results[nested_config.field_path] = nested_result
            result.total_resolved += nested_result.total_resolved
            result.total_unresolved += nested_result.total_unresolved

            if nested_config.is_required and nested_result.total_unresolved > 0:
                result.required_unresolved += nested_result.total_unresolved

    logger.info(
        f"Reference resolution complete: {result.total_resolved} resolved, "
        f"{result.total_unresolved} unresolved ({result.required_unresolved} required)"
    )

    return result


# =============================================================================
# Resource ID Resolution (for upsert operations)
# =============================================================================


def resolve_resource_fhir_ids(
    resources: Sequence[Resource],
    resource_type: str,
    identifier_system: str,
    fhir_database: FHIRDatabaseResource,
    resolve_fn: ResolveValuesFn | None = None,
) -> dict[str, str]:
    """
    Resolve existing FHIR IDs for resources by their business identifiers.

    This is used before upserting to determine if a resource should be updated
    (existing FHIR ID) or created (new UUID).

    Args:
        resources: List of FHIR resources to check
        resource_type: Type of resource (e.g., "CarePlan", "EpisodeOfCare")
        identifier_system: The identifier system to look up
        fhir_database: FHIR database resource for direct queries
        resolve_fn: Optional partition-scoped resolver; see
            :func:`batch_resolve_references`.

    Returns:
        Dict mapping identifier values to existing FHIR UUIDs
    """
    logger = dg.get_dagster_logger()

    # Collect unique identifiers from resources
    identifiers_to_resolve: dict[str, str] = {}
    for resource in resources:
        if hasattr(resource, "identifier") and resource.identifier:
            for ident in resource.identifier:
                if ident.system == identifier_system and ident.value:
                    identifiers_to_resolve[str(ident.value)] = ""

    if not identifiers_to_resolve:
        logger.debug(f"No {resource_type} identifiers found for system {identifier_system}")
        return {}

    logger.info(f"Resolving {len(identifiers_to_resolve)} existing {resource_type} FHIR IDs")

    # Batch resolve
    resolved_ids = batch_resolve_references(
        identifiers=identifiers_to_resolve,
        resource_type=resource_type,
        identifier_system=identifier_system,
        fhir_database=fhir_database,
        resolve_fn=resolve_fn,
    )

    logger.info(f"  → Found {len(resolved_ids)} existing {resource_type} resources in database")

    return resolved_ids


def apply_resolved_fhir_ids(
    resources: Sequence[Resource],
    resolved_ids: dict[str, str],
    identifier_system: str,
) -> ResourceResolutionResult:
    """
    Apply resolved FHIR IDs to resources for upsert operations.

    If a resource already exists (has a resolved FHIR ID), we set the resource's
    `id` field so the database performs an UPDATE instead of INSERT.

    Args:
        resources: List of FHIR resources to update
        resolved_ids: Dict mapping identifier values to FHIR UUIDs
        identifier_system: The identifier system to match

    Returns:
        ResourceResolutionResult with counts and resolved IDs
    """
    existing_count = 0
    new_count = 0

    for resource in resources:
        resource_identifier = None

        # Find the resource identifier matching the system
        if hasattr(resource, "identifier") and resource.identifier:
            for ident in resource.identifier:
                if ident.system == identifier_system and ident.value:
                    resource_identifier = str(ident.value)
                    break

        if resource_identifier:
            fhir_id = resolved_ids.get(resource_identifier)
            if fhir_id:
                # Existing resource - set the FHIR ID for update
                resource.id = fhir_id
                existing_count += 1
            else:
                # New resource - will get a new UUID on insert
                new_count += 1
        else:
            new_count += 1

    return ResourceResolutionResult(
        existing_count=existing_count,
        new_count=new_count,
        resolved_ids=resolved_ids,
    )


def resolve_and_apply_resource_ids(
    resources: Sequence[Resource],
    resource_type: str,
    identifier_system: str,
    fhir_database: FHIRDatabaseResource,
    resolve_fn: ResolveValuesFn | None = None,
) -> ResourceResolutionResult:
    """
    Convenience function: Resolve and apply FHIR IDs for a batch of resources.

    Combines resolve_resource_fhir_ids() and apply_resolved_fhir_ids().

    Args:
        resources: List of FHIR resources
        resource_type: Type of resource (e.g., "CarePlan")
        identifier_system: The identifier system to look up
        fhir_database: FHIR database resource
        resolve_fn: Optional partition-scoped resolver; see
            :func:`batch_resolve_references`.

    Returns:
        ResourceResolutionResult with counts
    """
    resolved_ids = resolve_resource_fhir_ids(
        resources=resources,
        resource_type=resource_type,
        identifier_system=identifier_system,
        fhir_database=fhir_database,
        resolve_fn=resolve_fn,
    )

    return apply_resolved_fhir_ids(
        resources=resources,
        resolved_ids=resolved_ids,
        identifier_system=identifier_system,
    )


# =============================================================================
# Legacy Compatibility Functions (to be deprecated)
# =============================================================================


def apply_resolved_references_to_resources(
    resources: list[Any],
    resolved_ids: dict[str, str],
    resource_type: str,
) -> int:
    """
    Apply resolved FHIR IDs to all references of a specific type within resources.

    DEPRECATED: Use resolve_and_apply_all_references() with ReferenceFieldConfig instead.

    This function traverses FHIR resources and updates Reference objects to include
    the FHIR server's UUID, which enables navigation in the Aidbox FHIR UI.

    Args:
        resources: List of FHIR resources to update
        resolved_ids: Dict mapping identifier values to FHIR server UUIDs
        resource_type: The type of referenced resource (e.g., "Patient", "EpisodeOfCare")

    Returns:
        Count of references that were updated with FHIR IDs
    """
    updated_count = 0

    def update_reference(ref: Reference) -> bool:
        """Update a single reference with resolved FHIR ID."""
        nonlocal updated_count

        if not ref or not hasattr(ref, "identifier") or not ref.identifier:
            return False

        identifier_value = str(ref.identifier.value) if ref.identifier.value else None
        if not identifier_value:
            return False

        fhir_id = resolved_ids.get(identifier_value)
        if fhir_id:
            ref.id = fhir_id
            ref.type = resource_type
            ref.identifier = None
            updated_count += 1
            return True
        return False

    # Common reference field names for different resource types
    reference_fields = {
        "Patient": ["subject", "patient"],
        "EpisodeOfCare": ["episodeOfCare"],
        "Encounter": ["encounter"],
        "Location": ["location"],
        "Organization": ["managingOrganization", "organization"],
        "Practitioner": ["practitioner", "performer"],
    }

    fields_to_check = reference_fields.get(resource_type, [])

    for resource in resources:
        # Check direct reference fields
        for field_name in fields_to_check:
            ref = getattr(resource, field_name, None)
            if ref is None:
                continue

            if isinstance(ref, list):
                for item in ref:
                    if isinstance(item, Reference):
                        update_reference(item)
                    elif hasattr(item, "location") and isinstance(item.location, Reference):
                        update_reference(item.location)
            elif isinstance(ref, Reference):
                update_reference(ref)

        # Check extensions for valueReference
        extensions = getattr(resource, "extension", None)
        if extensions:
            for ext in extensions:
                value_ref = getattr(ext, "valueReference", None)
                if isinstance(value_ref, Reference):
                    if value_ref.identifier:
                        identifier_value = (
                            str(value_ref.identifier.value) if value_ref.identifier.value else None
                        )
                        if identifier_value and identifier_value in resolved_ids:
                            update_reference(value_ref)

    return updated_count

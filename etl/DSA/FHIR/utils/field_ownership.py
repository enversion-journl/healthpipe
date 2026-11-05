# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Field ownership for a FHIR resource that several sources write: which source has the say."""

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.domainresource import DomainResource
from fhir.resources.R4B.meta import Meta
from utils.fhir.identifier_systems import SYSTEM_SOURCE_TAG

# The code in meta.tag for EPJ (IFDB) on every resource it contributes to.
SOURCE_IFDB = "IFDB"

R = TypeVar("R", bound=DomainResource)


class UnknownField(ValueError):
    """A contribution contains a field or a source the table does not know."""


class NotOwner(ValueError):
    """A contribution supplies a field the source is not listed as owner of."""


class DuplicateContribution(ValueError):
    """Two contributions from the same source for the same resource."""


@dataclass(frozen=True)
class OwnershipTable(Generic[R]):
    """The owners per field of one resource type, the sources' canonical order and the builder."""

    resource_class: type[R]
    sources: tuple[str, ...]
    fields: Mapping[str, tuple[str, ...]]
    builder_uri: str
    choice_types: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    builder_owned: frozenset[str] = frozenset({"id", "meta"})

    def __post_init__(self) -> None:
        unknown = {s for owners in self.fields.values() for s in owners} - set(self.sources)
        if unknown:
            raise ValueError(f"owners {sorted(unknown)} are not in sources {self.sources}")


@dataclass(frozen=True)
class Contribution(Generic[R]):
    """One source's proposal: the source and a partial resource without id and meta."""

    source: str
    resource: R


def field_keys(table: OwnershipTable[R], resource: R) -> set[str]:
    """The table keys for everything set on a resource."""
    logical = {a: key for key, attributes in table.choice_types.items() for a in attributes}
    keys: set[str] = set()
    for name in resource.__fields__:
        if name in table.builder_owned or name == "resource_type":
            continue
        value = getattr(resource, name, None)
        if value is None or value == []:
            continue
        if name == "identifier":
            keys.update(f"identifier:{i.system}" for i in value)
        elif name == "extension":
            keys.update(f"extension:{e.url}" for e in value)
        else:
            keys.add(logical.get(name, name))
    return keys


def by_source(table: OwnershipTable[R], contributions: Sequence[Contribution[R]]) -> dict[str, R]:
    """The contributions per source; raises if one breaks the table."""
    if not contributions:
        raise ValueError("no contributions")
    resources: dict[str, R] = {}
    for c in contributions:
        if c.source not in table.sources:
            raise UnknownField(f"unknown source {c.source!r}; known sources are {table.sources}")
        if c.source in resources:
            raise DuplicateContribution(f"two contributions from {c.source}")
        if any(getattr(c.resource, name, None) is not None for name in table.builder_owned):
            raise NotOwner(f"{c.source} sets {sorted(table.builder_owned)}; the builder owns those")
        for key in field_keys(table, c.resource):
            if key not in table.fields:
                raise UnknownField(f"{c.source} supplies {key}, which no source owns")
            if c.source not in table.fields[key]:
                raise NotOwner(f"{c.source} supplies {key} but does not own it")
        resources[c.source] = c.resource
    return resources


def winner(
    table: OwnershipTable[R],
    key: str,
    resources: Mapping[str, R],
    has: Callable[[R], bool],
) -> str | None:
    """The first owner in the order whose contribution has the field."""
    for source in table.fields[key]:
        resource = resources.get(source)
        if resource is not None and has(resource):
            return source
    return None


def _assemble_list(
    table: OwnershipTable[R],
    name: str,
    group: Callable[[Any], str | None],
    resources: Mapping[str, R],
) -> list | None:
    """Merge a list (identifier/extension): winner per group, source order in the output."""
    prefix = f"{name}:"
    winners: dict[str | None, str] = {}
    for key in table.fields:
        if not key.startswith(prefix):
            continue
        group_key = key[len(prefix) :]

        def has_group(r: R, g: str = group_key) -> bool:
            return any(group(e) == g for e in (getattr(r, name, None) or []))

        source = winner(table, key, resources, has_group)
        if source is not None:
            winners[group_key] = source

    result = []
    for source in table.sources:
        resource = resources.get(source)
        if resource is None:
            continue
        for element in getattr(resource, name, None) or []:
            if winners.get(group(element)) == source:
                result.append(element)
    return result or None


def merged_fields(table: OwnershipTable[R], resources: Mapping[str, R]) -> dict:
    """The resource's fields by the ownership table, before they become a resource."""
    fields: dict = {}
    for key in table.fields:
        if ":" in key or key.startswith("meta."):
            continue
        attributes = table.choice_types.get(key, (key,))

        def has_field(r: R, a: tuple[str, ...] = attributes) -> bool:
            return any(getattr(r, x, None) not in (None, []) for x in a)

        source = winner(table, key, resources, has_field)
        if source is None:
            continue
        for attribute in attributes:
            value = getattr(resources[source], attribute, None)
            if value is not None:
                fields[attribute] = value

    identifiers = _assemble_list(table, "identifier", lambda i: i.system, resources)
    extensions = _assemble_list(table, "extension", lambda e: e.url, resources)
    if identifiers:
        fields["identifier"] = identifiers
    if extensions:
        fields["extension"] = extensions
    return fields


def assemble(table: OwnershipTable[R], contributions: Sequence[Contribution[R]]) -> R:
    """Merge the contributions into one resource by the ownership table. Without id and meta."""
    return table.resource_class(**merged_fields(table, by_source(table, contributions)))


def provenance(table: OwnershipTable[R], sources: Iterable[str]) -> Meta:
    """The builder as source, one tag per contributing source in canonical order."""
    contributing = set(sources)
    return Meta(
        source=table.builder_uri,
        tag=[Coding(system=SYSTEM_SOURCE_TAG, code=s) for s in table.sources if s in contributing],
    )


def apply_provenance(table: OwnershipTable[R], resource: R, sources: Iterable[str]) -> R:
    """Set meta to the builder's provenance for the contributing sources."""
    resource.meta = provenance(table, sources)
    return resource


__all__ = [
    "SOURCE_IFDB",
    "Contribution",
    "DuplicateContribution",
    "NotOwner",
    "OwnershipTable",
    "UnknownField",
    "apply_provenance",
    "assemble",
    "by_source",
    "field_keys",
    "merged_fields",
    "provenance",
    "winner",
]

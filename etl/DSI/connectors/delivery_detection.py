# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Delivery detection for file-based sources.

A file source cannot ask "what changed since my checkpoint?" the way a
queryable system can — the only thing it can do cheaply is decide whether the
newest file on the share is one it has already ingested. That decision needs
two things: a stable name for the unit being tracked, and a comparable
identity for the delivery itself.

This module is the CONVENTION for those two things. It deliberately adds
nothing to DFM: the state lives in ``meta.DFMState`` through the existing
``DfmManager.get_checkpoint`` / ``set_checkpoint``, with no new table, no new
column and no new API. That is a finding of the design, not an
oversight — the framework already does everything this needs, so the only
thing missing was an agreed key and value format, which belongs with the
extraction machinery that uses it rather than in the checkpoint framework::

    unit = delivery_unit("mysource", "T26", "M")
    stored = dfm.get_checkpoint(DELIVERY_SCHEMA, unit, target_type=DELIVERY_TARGET_TYPE)
    if is_new_delivery(stored, identity):
        ...                                   # load it
        dfm.set_checkpoint(
            DELIVERY_SCHEMA, unit, value=identity, target_type=DELIVERY_TARGET_TYPE
        )

The layout in ``meta.DFMState``:

    scope        = "staging"
    target_type  = "archive"                               <- what it GATES
    schema_name  = "dfm"                                   <- synthetic, no table
    table_name   = "<source>_leverance_<part>_<part>..."    <- the UNIT
    checkpoint_value = "<iso>[|<iso>...]"                   <- the IDENTITY

``target_type`` is "archive" rather than the "dfm" the schema would derive,
because the row is only advanced once the delivery has landed in ``arc.*``:
that is what the checkpoint promises, and it is what someone reading
``meta.DFMState`` needs to see. ``schema_name`` stays "dfm" — it marks the row
as synthetic, with no table of that name behind it.

The unit goes in the KEY rather than the value because ``checkpoint_value`` is
NVARCHAR(50): a map of several units could not fit, and widening the column
would touch every DFM consumer for one source's benefit. A synthetic
``table_name`` under ``schema_name = "dfm"`` is the established pattern here —
``ArchiveMergeConfig.flow_start_key`` uses ``("dfm", "<table>_flow_start")``
for SYDAX, MySupply and IFDB.

The identity is a "|"-joined list of ISO-8601 components, most significant
first. Because every component is rendered at a FIXED width (10 for a date,
19 for a second-precision datetime), lexicographic order equals chronological
order and the comparison is a plain string ">". That is what makes one format
readable across both sources:

    a packed archive   one component:  the filename's packaging timestamp
                       "2026-08-18T11:45:49"
    loose files        two components: delivery date, then file mtime
                       "2026-08-15|2026-08-16T07:12:33"

This module names NO source system, and must not start to. Each source builds
its own unit with its own validation and passes the parts to
:func:`delivery_unit`; what is shared is the key shape, the identity format
and the comparison — nothing that has to change when a source is added.
"""

import re
from datetime import date, datetime

# meta.DFMState: schema_name for synthetic (non-table) checkpoints.
DELIVERY_SCHEMA = "dfm"

# meta.DFMState: what the checkpoint gates. Advanced only after the delivery
# has been merged into arc.*, so a failed archive load leaves it unmoved and
# the next run retries the same file.
DELIVERY_TARGET_TYPE = "archive"

# meta.DFMState column limits — see infrastructure/ddl/tables/meta/02_dfm_state.sql
MAX_UNIT_LENGTH = 128
MAX_IDENTITY_LENGTH = 50

IDENTITY_SEPARATOR = "|"

_UNIT_PART_RE = re.compile(r"^[A-Za-z0-9_]+$")


def delivery_unit(source: str, *parts: str) -> str:
    """The DFMState ``table_name`` identifying one delivery unit.

    ``source`` names the feed; ``parts`` identify the unit within it. Every part must be a non-empty [A-Za-z0-9_] string — ``None``
    is rejected rather than defaulted, because a unit that cannot be named
    explicitly is a bug in the caller, not a case to paper over.
    """
    if not parts:
        raise ValueError(f"delivery_unit({source!r}) needs at least one unit part")
    for part in (source, *parts):
        if part is None:
            raise ValueError(
                f"delivery_unit({source!r}, ...) got a None part. Map it to an "
                f"explicit bucket at selection time — never here."
            )
        if not isinstance(part, str) or not _UNIT_PART_RE.match(part):
            raise ValueError(
                f"delivery_unit({source!r}, ...): {part!r} is not a valid unit "
                f"part (expected non-empty [A-Za-z0-9_])"
            )
    unit = f"{source}_leverance_" + "_".join(parts)
    if len(unit) > MAX_UNIT_LENGTH:
        raise ValueError(
            f"delivery unit {unit!r} is {len(unit)} chars — "
            f"meta.DFMState.table_name holds {MAX_UNIT_LENGTH}"
        )
    return unit


def _render(component: date | datetime) -> str:
    # datetime is a subclass of date — check it first or every datetime
    # renders as a bare date and loses the time component.
    if isinstance(component, datetime):
        return component.isoformat(timespec="seconds")
    if isinstance(component, date):
        return component.isoformat()
    raise TypeError(
        f"delivery identity components must be date or datetime, got "
        f"{type(component).__name__}. Fixed-width ISO rendering is what makes "
        f"string comparison equal chronological comparison."
    )


def delivery_identity(*components: date | datetime) -> str:
    """A sortable identity for one delivery.

    Components are given most significant first and rendered at fixed width,
    so ``a > b`` as strings means ``a`` is the newer delivery.
    """
    if not components:
        raise ValueError("delivery_identity() needs at least one component")
    identity = IDENTITY_SEPARATOR.join(_render(c) for c in components)
    if len(identity) > MAX_IDENTITY_LENGTH:
        raise ValueError(
            f"delivery identity {identity!r} is {len(identity)} chars — "
            f"meta.DFMState.checkpoint_value holds {MAX_IDENTITY_LENGTH}"
        )
    return identity


def is_new_delivery(checkpoint: str | None, identity: str) -> bool:
    """Should this delivery be ingested?

    True when no checkpoint exists (first run for the unit) or the identity is
    strictly newer. Equal identities are NOT new — that is the whole point:
    re-running against the same file must read nothing.
    """
    if not identity:
        raise ValueError("delivery identity must be non-empty")
    if checkpoint is None:
        return True
    if checkpoint.count(IDENTITY_SEPARATOR) != identity.count(IDENTITY_SEPARATOR):
        # Comparing identities of different shapes is meaningless, and silently
        # returning True would re-ingest every delivery forever.
        raise ValueError(
            f"delivery identity shape changed: stored {checkpoint!r} has "
            f"{checkpoint.count(IDENTITY_SEPARATOR) + 1} component(s), new "
            f"{identity!r} has {identity.count(IDENTITY_SEPARATOR) + 1}. Reset "
            f"the checkpoint deliberately if the format was migrated."
        )
    return identity > checkpoint


__all__ = [
    "DELIVERY_SCHEMA",
    "DELIVERY_TARGET_TYPE",
    "IDENTITY_SEPARATOR",
    "MAX_IDENTITY_LENGTH",
    "MAX_UNIT_LENGTH",
    "delivery_identity",
    "delivery_unit",
    "is_new_delivery",
]

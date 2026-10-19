# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Moving a FHIR builder's fhir_server checkpoint forward without skipping rows."""

from typing import Any


def advance_fhir_checkpoint(
    dfm: Any,
    run_ctx: Any,
    resource_name: str,
    config: Any,
    total_fetched: int,
    read_bound: str | None = None,
) -> str | None:
    """Advance the builder's fhir_server checkpoint to the latest safe point.

    The builder reads arc on ``dw_timestamp_update > checkpoint``. Moving the
    checkpoint to the time the builder *finished* would skip every archive row
    written while it ran. It is moved instead to the builder's read start,
    held back to the start of any archive MERGE still in flight
    (``DfmManager.safe_reader_checkpoint``). At worst a row is read twice,
    and posting is an idempotent upsert. ``read_bound`` replaces the read start
    for a builder that read a bounded delta slice.

    Returns the new checkpoint, or None when the run may not advance it.
    """
    if not (config.should_advance_checkpoint and total_fetched > 0):
        return None
    new_checkpoint = dfm.safe_reader_checkpoint(read_bound or getattr(run_ctx, "started_at", None))
    if new_checkpoint:
        dfm.set_checkpoint("fhir_server", resource_name, value=new_checkpoint, mode=config.mode)
    return new_checkpoint

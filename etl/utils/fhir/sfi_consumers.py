# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The switch for running the SFI chain without consumers, e.g. while the full load backfills history.

While it is on, the store's daily refresh is skipped, and the consumer indexes on the store's
source tables (QR and QR items on staging and DSO) stay disabled instead of being rebuilt after
every write. Indexes the pipeline itself reads keep their usual gate. The switch is a row in
``meta.DFMState`` (scope ``sfi_consumers``): the asset ``sfi_consumers_absent_start`` sets it, and
a run tag can override it for one run.
"""

from typing import Any

CONSUMERS_ABSENT_SCOPE = "sfi_consumers"
CONSUMERS_ABSENT_SCHEMA = "fhir"
CONSUMERS_ABSENT_TABLE = "SFI_datapoint_store"
CONSUMERS_ABSENT_TAG = "sfi_datapoint/consumers_absent"


def sfi_consumers_absent(context: Any = None, dfm: Any = None) -> bool:
    """Is the SFI chain running without consumers? The run tag wins; otherwise the DFM row."""
    tags = getattr(getattr(context, "run", None), "tags", None)
    tag = (dict(tags) if isinstance(tags, dict) else {}).get(CONSUMERS_ABSENT_TAG)
    if tag is not None:
        return tag.strip().lower() in ("true", "1", "on")
    if dfm is None:
        return False
    return (
        dfm.get_checkpoint(
            CONSUMERS_ABSENT_SCHEMA, CONSUMERS_ABSENT_TABLE, scope=CONSUMERS_ABSENT_SCOPE
        )
        is not None
    )

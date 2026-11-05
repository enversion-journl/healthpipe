# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Switches the SFI chain to running without consumers (see utils.fhir.sfi_consumers)."""

from datetime import UTC, datetime

import dagster as dg
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from utils.fhir.sfi_consumers import (
    CONSUMERS_ABSENT_SCHEMA,
    CONSUMERS_ABSENT_SCOPE,
    CONSUMERS_ABSENT_TABLE,
)


@dg.asset(
    name="sfi_consumers_absent_start",
    group_name="fhir_operations",
    compute_kind="mssql",
    description=(
        "Switch the SFI chain to running without consumers: from the next run the store's "
        "daily refresh is skipped, and the consumer indexes on fhir.QuestionnaireResponse and "
        "fhir.QuestionnaireResponse_item stay disabled instead of being rebuilt after every "
        "window. Run it before a full load while nobody reads SFI data. Idempotent: a second "
        "run keeps the time the switch was first set."
    ),
)
def sfi_consumers_absent_start(
    context: dg.AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    since = dfm.get_checkpoint(
        CONSUMERS_ABSENT_SCHEMA, CONSUMERS_ABSENT_TABLE, scope=CONSUMERS_ABSENT_SCOPE
    )
    if since is None:
        since = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
        dfm.set_checkpoint(
            CONSUMERS_ABSENT_SCHEMA,
            CONSUMERS_ABSENT_TABLE,
            value=since,
            mode="full_load",
            scope=CONSUMERS_ABSENT_SCOPE,
        )
        context.log.info(f"The SFI chain runs without consumers from now ({since} UTC).")
    else:
        context.log.info(f"The SFI chain has run without consumers since {since} UTC.")
    return dg.MaterializeResult(metadata={"consumers_absent_since": dg.MetadataValue.text(since)})


sfi_consumers_absent_start_job = dg.define_asset_job(
    name="sfi_consumers_absent_start_job",
    selection=dg.AssetSelection.assets("sfi_consumers_absent_start"),
    description="Switch the SFI chain to running without consumers (one click before a full load).",
)

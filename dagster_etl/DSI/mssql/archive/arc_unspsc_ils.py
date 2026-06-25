# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Archive: ext.unspsc_ils → arc.unspsc_ils

UPSERT archive with DFM checkpoint. Business logic mirrors arc_unspsc_ils.py.
"""

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource

UNSPSC_ILS_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="unspsc_ils",
    target_schema="arc",
    target_table="unspsc_ils",
    merge_keys=("prrowid",),
    non_key_columns=(
        "unspsc",
        "betegn",
        "un_name",
        "sysdato",
        "bruger",
        "seqnr",
        "aktiv",
        "version",
        "Pro2SrcPDB",
        "pro2created",
        "pro2modified",
    ),
    checkpoint_query="SELECT CAST(MAX([sysdato]) AS NVARCHAR(50)) FROM [arc].[unspsc_ils]",
    strategy="upsert",
)


@dg.asset(
    name="arc_unspsc_ils",
    group_name="extract_ils",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_unspsc_ils"])],
    tags={"domain": "erp", "layer": "archive", "source": "ils", "entity": "unspsc"},
    description="UPSERT ext.unspsc_ils → arc.unspsc_ils.",
)
def arc_unspsc_ils(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    result = execute_archive_merge(context, mssql_target_staging, UNSPSC_ILS_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.unspsc_ils; "
        f"mode={config.mode}, checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result, config, source_table="ext.unspsc_ils", target_table="arc.unspsc_ils"
        )
    )

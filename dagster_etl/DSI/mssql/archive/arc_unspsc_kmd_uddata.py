# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Archive: ext.unspsc_kmd_uddata → arc.unspsc_kmd_uddata

UPSERT archive with DFM checkpoint. Business logic mirrors arc_unspsc_kmd_uddata.py.
"""

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource

KMD_UNSPSC_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="unspsc_kmd_uddata",
    target_schema="arc",
    target_table="unspsc_kmd_uddata",
    merge_keys=("DW_SK_UNSPSC",),
    non_key_columns=(
        "DW_EK_UNSPSC",
        "UNSPSCClassID",
        "UNSPSCClassDescription",
        "UNSPSCLevel",
        "TenderCategoryKey",
        "UNSPSCClassLev1ID",
        "UNSPSCClassLev1Description",
        "UNSPSCClassLev1Label",
        "UNSPSCClassLev2ID",
        "UNSPSCClassLev2Description",
        "UNSPSCClassLev2Label",
        "UNSPSCClassLev3ID",
        "UNSPSCClassLev3Description",
        "UNSPSCClassLev3Label",
        "UNSPSCClassLev4ID",
        "UNSPSCClassLev4Description",
        "UNSPSCClassLev4Label",
        "DW_ArchiveID",
        "DW_ValidFrom",
        "DW_ValidTo",
        "DW_IsCurrent",
        "DW_Audit_Insert",
        "DW_Audit_Update",
        "DW_Audit",
    ),
    checkpoint_query=(
        "SELECT CAST(MAX([computed_max_audit]) AS NVARCHAR(50)) " "FROM [arc].[unspsc_kmd_uddata]"
    ),
    strategy="upsert",
)


@dg.asset(
    name="arc_unspsc_kmd_uddata",
    group_name="kmd_uddata_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_unspsc_kmd_uddata"])],
    tags={
        "domain": "kmd_uddata",
        "layer": "archive",
        "source": "kmd_uddata",
        "entity": "unspsc",
    },
    description="UPSERT ext → arc.unspsc_kmd_uddata.",
)
def arc_unspsc_kmd_uddata(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    result = execute_archive_merge(context, mssql_target_staging, KMD_UNSPSC_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.unspsc_kmd_uddata; "
        f"mode={config.mode}, checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.unspsc_kmd_uddata",
            target_table="arc.unspsc_kmd_uddata",
        )
    )

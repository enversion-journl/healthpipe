# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Archive: ext.afdlgrbeh_ils → arc.afdlgrbeh_ils

Insert-only snapshot archive. Business logic mirrors arc_afdlgrbeh_ils.py.
"""

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource

AFDLGRBEH_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="afdlgrbeh_ils",
    target_schema="arc",
    target_table="afdlgrbeh_ils",
    # insert_only appends snapshots — no MERGE/ON clause — so merge_keys is
    # empty and all copied columns (incl. the business key prrowid) live in
    # non_key_columns.
    merge_keys=(),
    non_key_columns=(
        "prrowid",
        "sgh",
        "lgr",
        "brik",
        "rnr",
        "vnr",
        "antal",
        "lnr",
        "batchnr",
        "udlobdto",
        "lotnr",
        "sysdato",
        "bruger",
        "seqnr",
        "Pro2SrcPDB",
        "pro2created",
        "pro2modified",
    ),
    strategy="insert_only",
)


@dg.asset(
    name="arc_afdlgrbeh_ils",
    group_name="extract_lagerbeholdning",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_afdlgrbeh_ils"])],
    tags={"domain": "erp", "layer": "archive", "source": "ils", "entity": "afdlgrbeh"},
    description="Insert-only snapshot ext → arc.afdlgrbeh_ils.",
)
def arc_afdlgrbeh_ils(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    result = execute_archive_merge(context, mssql_target_staging, AFDLGRBEH_ARCHIVE_CONFIG, config)

    context.log.info(f"Inserted {result.row_count} rows into arc.afdlgrbeh_ils")

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result, config, source_table="ext.afdlgrbeh_ils", target_table="arc.afdlgrbeh_ils"
        )
    )

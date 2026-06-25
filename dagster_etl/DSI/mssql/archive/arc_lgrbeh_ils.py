# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Archive: ext.lgrbeh_ils → arc.lgrbeh_ils

Insert-only snapshot archive. Business logic mirrors arc_lgrbeh_ils.py.
"""

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource

LGRBEH_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="lgrbeh_ils",
    target_schema="arc",
    target_table="lgrbeh_ils",
    # insert_only appends snapshots — no MERGE/ON clause — so merge_keys is
    # empty and all copied columns (incl. the business key prrowid) live in
    # non_key_columns.
    merge_keys=(),
    non_key_columns=(
        "prrowid",
        "sgh",
        "lgr",
        "vnr",
        "lgrlok",
        "antal",
        "batchnr",
        "lnr",
        "udlobdto",
        "fralok",
        "sw_egen_prod",
        "sysdato",
        "bruger",
        "seqnr",
        "lotnr",
        "udlevenh",
        "bem",
        "lvnr##1",
        "lvnr##2",
        "lvnr##3",
        "lvnr##4",
        "lgrbeh_prio",
        "sw_erstat",
        "Pro2SrcPDB",
        "pro2created",
        "pro2modified",
    ),
    strategy="insert_only",
)


@dg.asset(
    name="arc_lgrbeh_ils",
    group_name="extract_lagerbeholdning",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_lgrbeh_ils"])],
    tags={"domain": "erp", "layer": "archive", "source": "ils", "entity": "lgrbeh"},
    description="Insert-only snapshot ext → arc.lgrbeh_ils.",
)
def arc_lgrbeh_ils(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    result = execute_archive_merge(context, mssql_target_staging, LGRBEH_ARCHIVE_CONFIG, config)

    context.log.info(f"Inserted {result.row_count} rows into arc.lgrbeh_ils")

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result, config, source_table="ext.lgrbeh_ils", target_table="arc.lgrbeh_ils"
        )
    )

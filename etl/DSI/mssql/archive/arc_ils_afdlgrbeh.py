# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Archive: ext.ils_afdlgrbeh → arc.ils_afdlgrbeh

Insert-only snapshot archive of ILS department stock.
"""

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource

AFDLGRBEH_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ils_afdlgrbeh",
    target_schema="arc",
    target_table="ils_afdlgrbeh",
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
    name="arc_ils_afdlgrbeh",
    group_name="ils_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ils_afdlgrbeh"])],
    tags={"domain": "erp", "layer": "archive", "source": "ils", "entity": "afdlgrbeh"},
    description="Insert-only snapshot ext → arc.ils_afdlgrbeh.",
)
def arc_ils_afdlgrbeh(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    result = execute_archive_merge(context, mssql_target_staging, AFDLGRBEH_ARCHIVE_CONFIG, config)

    context.log.info(f"Inserted {result.row_count} rows into arc.ils_afdlgrbeh")

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result, config, source_table="ext.ils_afdlgrbeh", target_table="arc.ils_afdlgrbeh"
        )
    )

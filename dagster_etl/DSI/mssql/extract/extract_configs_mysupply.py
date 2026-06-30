# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DFM extract configs and factory-generated assets for MySupply source.

Runs in parallel with the legacy partition-based MySupply extract in
DSI/mssql/mysupply.py — the new DFM extracts target ext.*_mysupply_dfm so
they don't clash with ext.*_mysupply produced by the legacy flow.

FakturaDato is NVARCHAR(MAX) in source; TRY_CAST is used to skip rows whose
string is not a valid datetime instead of failing the extract. The 7-day
lookback covers backdated invoices.

Header filters directly on FakturaDato. The 4 dependent tables don't have
their own delta column; they filter via a correlated scalar subquery
through the FK chain to header. Each dependent reads its own checkpoint
from its arc table (independent advancement per DFM doctrine).

TODO: When the legacy partition-based MySupply extract is removed:
  1. Migrate ext.*_mysupply_dfm → ext.*_mysupply (see precedent migration
     infrastructure/ddl/migrations/20260528_dfm_erp_migration_rename_checkpoints.sql)
  2. Update each `target_table` below and the corresponding
     ArchiveMergeConfig.source_table in DSI/mssql/archive/arc_*_mysupply.py
     to drop the _dfm postfix.
"""

import dagster as dg
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource, MySupplyMSSQLResource
from DSI.connectors.extract_config import DfmExtractConfig
from DSI.connectors.factory import create_dfm_extraction_assets

MYSUPPLY_DFM_EXTRACT_CONFIGS = [
    DfmExtractConfig(
        source_schema="dbo",
        source_table="statistic_invoice_header",
        target_table="mysupply_statistic_invoice_header_dfm",
        delta_expression="TRY_CAST([FakturaDato] AS DATETIME2)",
        checkpoint_type="datetime",
        lookback_days=7,
        checkpoint_from=("arc", "mysupply_statistic_invoice_header"),
        description=(
            "MySupply invoice header (FakturaDato, 7-day tail). "
            "_dfm postfix during cutover from legacy partition extract."
        ),
    ),
    DfmExtractConfig(
        source_schema="dbo",
        source_table="metadata",
        target_table="mysupply_metadata_dfm",
        # Note: [dbo].[metadata].[id] must be explicitly qualified — both the
        # outer table (metadata) and the inner alias (h = statistic_invoice_header)
        # have a column named `id`, so an unqualified `[id]` would silently bind
        # to h.id (innermost scope), making the WHERE clause `h.metadata_id = h.id`
        # which is effectively never true → metadata extract would return 0 rows.
        delta_expression=(
            "TRY_CAST(("
            "SELECT TOP 1 h.[FakturaDato] "
            "FROM [dbo].[statistic_invoice_header] h "
            "WHERE h.[metadata_id] = [dbo].[metadata].[id]"
            ") AS DATETIME2)"
        ),
        checkpoint_type="datetime",
        lookback_days=7,
        checkpoint_from=("arc", "mysupply_metadata"),
        description=(
            "MySupply supplier metadata. Filtered via correlated subquery: "
            "a metadata row is in-window if any referencing header has "
            "FakturaDato within the lookback window."
        ),
    ),
    DfmExtractConfig(
        source_schema="dbo",
        source_table="statistic_invoice_line",
        target_table="mysupply_statistic_invoice_line_dfm",
        delta_expression=(
            "TRY_CAST(("
            "SELECT h.[FakturaDato] "
            "FROM [dbo].[statistic_invoice_header] h "
            "WHERE h.[id] = [statistic_invoice_header_id]"
            ") AS DATETIME2)"
        ),
        checkpoint_type="datetime",
        lookback_days=7,
        checkpoint_from=("arc", "mysupply_statistic_invoice_line"),
        description=(
            "MySupply invoice lines. Filtered via correlated subquery to "
            "the parent header's FakturaDato."
        ),
    ),
    DfmExtractConfig(
        source_schema="dbo",
        source_table="statistic_invoice_header_allowance_charge",
        target_table="mysupply_statistic_invoice_header_allowance_charge_dfm",
        delta_expression=(
            "TRY_CAST(("
            "SELECT h.[FakturaDato] "
            "FROM [dbo].[statistic_invoice_header] h "
            "WHERE h.[id] = [statistic_invoice_header_id]"
            ") AS DATETIME2)"
        ),
        checkpoint_type="datetime",
        lookback_days=7,
        checkpoint_from=("arc", "mysupply_statistic_invoice_header_allowance_charge"),
        description=(
            "MySupply header-level allowance/charge. Filtered via "
            "correlated subquery to the parent header's FakturaDato."
        ),
    ),
    DfmExtractConfig(
        source_schema="dbo",
        source_table="statistic_invoice_line_allowance_charge",
        target_table="mysupply_statistic_invoice_line_allowance_charge_dfm",
        delta_expression=(
            "TRY_CAST(("
            "SELECT h.[FakturaDato] "
            "FROM [dbo].[statistic_invoice_header] h "
            "INNER JOIN [dbo].[statistic_invoice_line] l "
            "    ON l.[statistic_invoice_header_id] = h.[id] "
            "WHERE l.[id] = [statistic_invoice_line_id]"
            ") AS DATETIME2)"
        ),
        checkpoint_type="datetime",
        lookback_days=7,
        checkpoint_from=("arc", "mysupply_statistic_invoice_line_allowance_charge"),
        description=(
            "MySupply line-level allowance/charge. Filtered via correlated "
            "subquery through line → header to FakturaDato."
        ),
    ),
]

# ---------------------------------------------------------------------------
# Flow-start assets (one per MySupply source table)
#
# Each asset captures SYSDATETIME() from the MySupply source DB before its
# matching extract reads source, and writes it to meta.dfm_state under
# (dfm, <arc_target>_flow_start). The corresponding arc.*_mysupply asset
# reads that row via ArchiveMergeConfig.flow_start_key, so the new archive
# checkpoint is the timestamp captured *before* extract started — closing
# the gap between extract-start and archive-merge-complete that the
# previous SYSDATETIME-fallback exposed.
# ---------------------------------------------------------------------------


def mysupply_flow_start_dfm_table(target_table: str) -> str:
    """Single source of truth for the dfm_state row name a flow_start asset writes to.

    The matching arc.*_mysupply config's `flow_start_key` must read from the same
    name. `tests/unit/test_mysupply_flow_start.py` asserts this round-trip — if
    this convention changes, the archive configs will mismatch and the test fails.

    target_table: ext target table name including the _dfm suffix.
    """
    return f"{target_table.removesuffix('_dfm')}_flow_start"


def _build_mysupply_flow_start_asset(target_table: str):
    """Build a dfm_<arc_target>_flow_start asset for a MySupply source table.

    target_table: ext target table name including the _dfm suffix
                  (e.g. "mysupply_statistic_invoice_header_dfm").
    """
    dfm_table = mysupply_flow_start_dfm_table(target_table)
    arc_target = target_table.removesuffix("_dfm")
    asset_name = f"dfm_{dfm_table}"

    @dg.asset(
        name=asset_name,
        group_name="mysupply_archive",
        compute_kind="mssql",
        deps=["staging_infra_dfm"],
        tags={"domain": "erp", "layer": "dfm", "source": "mysupply"},
        description=(
            f"Capture SYSDATETIME() from MySupply source as the flow-start "
            f"timestamp before ext.{target_table} extracts. "
            f"arc.{arc_target} uses this as its new checkpoint, ensuring "
            f"gap-free coverage."
        ),
    )
    def _flow_start_asset(
        context: dg.AssetExecutionContext,
        mysupply_mssql: MySupplyMSSQLResource,
        mssql_target_staging: MSSQLResource,
    ) -> dg.MaterializeResult:
        dfm = DfmManager(mssql_target_staging, context)
        run_ctx = dfm.start_dfm_run(
            "dfm",
            dfm_table,
            dagster_run_id=context.run_id,
            mode="flow_start",
        )
        try:
            with mysupply_mssql.get_connection() as src:
                cursor = src.cursor()
                try:
                    cursor.execute("SELECT CONVERT(NVARCHAR(50), SYSDATETIME(), 121)")
                    flow_ts = cursor.fetchone()[0]
                finally:
                    cursor.close()

            dfm.set_checkpoint("dfm", dfm_table, value=flow_ts, mode="flow_start")
            dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=flow_ts, rows_affected=0)
            return dg.MaterializeResult(
                metadata={"flow_start": flow_ts, "source": "mysupply_sysdatetime"}
            )
        except Exception as exc:
            dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
            raise

    return _flow_start_asset


mysupply_flow_start_assets = [
    _build_mysupply_flow_start_asset(cfg.target_table) for cfg in MYSUPPLY_DFM_EXTRACT_CONFIGS
]

mysupply_dfm_extract_assets: list = []
for _cfg in MYSUPPLY_DFM_EXTRACT_CONFIGS:
    _flow_start_asset_name = f"dfm_{mysupply_flow_start_dfm_table(_cfg.target_table)}"
    mysupply_dfm_extract_assets += create_dfm_extraction_assets(
        configs=[_cfg],
        source_resource_key="mysupply_mssql",
        source_resource_type=MySupplyMSSQLResource,
        source_system="MySupply",
        group_name="mysupply_archive",
        domain_tag="erp",
        dialect="mssql",
        deps=["staging_infra_erp", _flow_start_asset_name],
    )

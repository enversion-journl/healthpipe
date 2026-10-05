# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Kreditor (Dimension Layer, unified)

Single staging table with the DELTA set of clean_code_Kreditor across all
three sources (SydAX, ILS, MySupply) plus per-source presence flags.

The delta is extracted from the archives based on the ("dim", "Kreditor")
DFM checkpoint: only CVRs whose archive rows changed since the last
dim_Kreditor run are staged. No checkpoint (first run) or full_load mode
stages the complete archive picture.

Replaces the three per-source staging tables (stg.dim_Kreditor_sydax/ils/
mysupply): the map only needs the changed CVR key set + source flags, and the
descriptive attributes are enriched directly from the arc.* archives in
dim_Kreditor.

Grain: clean_code_Kreditor (CVR). Each source's CleanCode formula is kept
exactly as its downstream flows use it, so existing map keys keep matching.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from utils.sql.tax_id_sql import (
    sql_cvr_cleancode_from_column,
    sql_cvr_cleancode_from_juridisk_enhed,
    sql_cvr_cleancode_from_se_nr,
)


def _build_stg_sql(checkpoint: str | None) -> str:
    """Union of changed CVRs per source. checkpoint None => full archive picture."""

    def cp_filter(alias: str) -> str:
        if checkpoint is None:
            return ""
        return f"\n          AND {alias}.dw_timestamp_update > '{checkpoint}'"

    # valutakode/FakturaDato live on the invoice header, so header-only changes
    # must also flag the CVR as changed. Only relevant in delta mode — the full
    # picture already includes every metadata-derivable CVR.
    header_delta_branch = ""
    if checkpoint is not None:
        header_delta_branch = f"""

        UNION ALL

        -- MySupply: header-only changes (valutakode/FakturaDato live on the header)
        SELECT DISTINCT
            {sql_cvr_cleancode_from_column("m.LeverandorCVR")} AS clean_code_Kreditor,
            0, 0, 1
        FROM [arc].[mysupply_statistic_invoice_header] h
        JOIN [arc].[mysupply_metadata] m
            ON m.id = h.metadata_id
        WHERE m.LeverandorCVR IS NOT NULL
          AND LTRIM(RTRIM(m.LeverandorCVR)) <> ''
          AND h.dw_timestamp_update > '{checkpoint}'"""

    return f"""
    ;WITH sources AS (
        -- SydAX: CVR from JuridiskEnhed (DK prefix stripped, no space removal)
        SELECT DISTINCT
            {sql_cvr_cleancode_from_juridisk_enhed("k.JuridiskEnhed")} AS clean_code_Kreditor,
            1 AS dw_source_sydax,
            0 AS dw_source_ils,
            0 AS dw_source_mysupply
        FROM [arc].[sydax_kreditor] k
        WHERE k.JuridiskEnhed IS NOT NULL
          AND LTRIM(RTRIM(k.JuridiskEnhed)) <> ''{cp_filter("k")}

        UNION ALL

        -- ILS: CVR from lev.se_nr (spaces removed, DK prefix stripped)
        SELECT DISTINCT
            {sql_cvr_cleancode_from_se_nr("lev.se_nr")} AS clean_code_Kreditor,
            0, 1, 0
        FROM [arc].[ils_lev] lev
        WHERE lev.se_nr IS NOT NULL
          AND LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))) <> ''{cp_filter("lev")}

        UNION ALL

        -- MySupply: CVR from metadata.LeverandorCVR (shared helper formula)
        SELECT DISTINCT
            {sql_cvr_cleancode_from_column("m.LeverandorCVR")} AS clean_code_Kreditor,
            0, 0, 1
        FROM [arc].[mysupply_metadata] m
        WHERE m.LeverandorCVR IS NOT NULL
          AND LTRIM(RTRIM(m.LeverandorCVR)) <> ''{cp_filter("m")}{header_delta_branch}
    )
    -- clean_code_Kreditor is inferred from the union; the MySupply branch reads
    -- arc.mysupply_metadata.LeverandorCVR (NVARCHAR(MAX)), so the SELECT INTO
    -- column would inherit NVARCHAR(MAX) — invalid as an index key column. Bind
    -- it to NVARCHAR(255) (the map.Kreditor/dim.Kreditor column width) so the
    -- unique clustered index builds and the map MERGE gets no implicit convert.
    SELECT
        CAST(clean_code_Kreditor AS NVARCHAR(255)) AS clean_code_Kreditor,
        CAST(MAX(dw_source_sydax) AS BIT) AS dw_source_sydax,
        CAST(MAX(dw_source_ils) AS BIT) AS dw_source_ils,
        CAST(MAX(dw_source_mysupply) AS BIT) AS dw_source_mysupply
    INTO [stg].[dim_Kreditor]
    FROM sources
    WHERE clean_code_Kreditor IS NOT NULL
      AND clean_code_Kreditor <> ''
    GROUP BY CAST(clean_code_Kreditor AS NVARCHAR(255));
"""


@asset(
    key=AssetKey(["erp", "stg_dim_Kreditor"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["arc_sydax_kreditor"]),
        AssetKey(["arc_ils_lev"]),
        AssetKey(["arc_mysupply_metadata"]),
        AssetKey(["arc_mysupply_statistic_invoice_header"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "combined",
        "entity": "kreditor",
    },
    description=(
        "Unified Kreditor staging: DELTA of clean_code_Kreditor across SydAX, ILS and "
        "MySupply archives (dim.Kreditor checkpoint) with per-source presence flags. "
        "Feeds the map.Kreditor MERGE."
    ),
)
def stg_dim_Kreditor(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """
    Unified kreditor key staging (delta).

    One row per changed clean_code_Kreditor with dw_source_<system> flags
    (MAX over the union of changed archive rows). Filtered by the
    ("dim", "Kreditor") checkpoint — only CVRs whose archive rows changed
    since the last dim_Kreditor run are staged. Flags reflect the delta's
    sources only; stickiness (never 1 -> 0, union across runs) is applied in
    the map.Kreditor MERGE.
    """

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg", "dim_Kreditor", dagster_run_id=context.run_id, mode=config.mode
    )
    row_count = 0

    try:
        checkpoint = dfm.get_checkpoint("dim", "Kreditor")
        if config.mode == "full_load":
            checkpoint = None

        if checkpoint is not None:
            context.log.info(f"Delta mode: staging archive changes since {checkpoint}")
        else:
            context.log.info("No checkpoint — staging the full archive picture")

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Creating staging table stg.dim_Kreditor from arc layer...")

                cursor.execute("DROP TABLE IF EXISTS [stg].[dim_Kreditor];")

                cursor.execute(_build_stg_sql(checkpoint))

                row_count = cursor.rowcount
                context.log.info(f"Created stg.dim_Kreditor with {row_count} rows")

                cursor.execute(
                    """
                    CREATE UNIQUE CLUSTERED INDEX IX_dim_kreditor_CleanCode
                    ON [stg].[dim_Kreditor](clean_code_Kreditor);
                """
                )
                context.log.info("Created unique clustered index")

                conn.commit()

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        SUM(CAST(dw_source_sydax AS INT)) AS in_sydax,
                        SUM(CAST(dw_source_ils AS INT)) AS in_ils,
                        SUM(CAST(dw_source_mysupply AS INT)) AS in_mysupply
                    FROM [stg].[dim_Kreditor]
                """
                )
                stats = cursor.fetchone()
                context.log.info(
                    f"Coverage: {stats[0]} changed CVRs — "
                    f"sydax={stats[1]}, ils={stats[2]}, mysupply={stats[3]}"
                )
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=None, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.dim_Kreditor",
            "row_count": row_count,
            "source_tables": (
                "arc.sydax_kreditor + arc.ils_lev + arc.mysupply_metadata "
                "(+ arc.mysupply_statistic_invoice_header for header-only delta detection)"
            ),
            "grain": "clean_code_Kreditor (CVR)",
            "checkpoint_filter": checkpoint if checkpoint else "none (full archive picture)",
            "in_sydax": stats[1],
            "in_ils": stats[2],
            "in_mysupply": stats[3],
            "layer": "dim (Layer 1)",
            "mode": config.mode,
        }
    )

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Pre-Staging Asset: Vareudlevering from ILS

Pre-staging for ILS delivery data (single source table) with grain-level
deduplication.  Creates indexes for downstream map lookups.

Source: arc.ils_udlev — delivery lines from central warehouse to departments.
Grain: one row per delivery line, identified by clean_code_Vareudlevering (prrowid).

Dedup rationale: arc.ils_udlev is UPSERT-keyed on prrowid, so there is at most
one row per business key. The ROW_NUMBER dedup here is retained defensively so
both downstream consumers (stg_dim_Vareudlevering_ils and
stg_fct_Vareudlevering_ils) are protected from any grain violations.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    RetryPolicy,
    asset,
)
from DSA.SQL.common.ils_clean_code_shared import (
    sql_clean_code_lager,
    sql_clean_code_rekvirent,
)

# Both keys are raw, as the maps hold them.
# Module-level so the contract is testable without running the asset.
LAGER_CLEAN_CODE_SQL = sql_clean_code_lager()
REKVIRENT_CLEAN_CODE_SQL = sql_clean_code_rekvirent()


@asset(
    key=AssetKey(["erp", "pre_stg_Vareudlevering_ils"]),
    group_name="erp_pre_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[AssetKey(["arc_ils_udlev"])],
    tags={
        "domain": "erp",
        "layer": "pre_stg",
        "source": "ils",
        "entity": "vareudlevering",
    },
    description=(
        "Pre-staging for ILS delivery data from arc.ils_udlev with ROW_NUMBER "
        "dedup on clean_code_Vareudlevering. Generates CleanCodes inline, keeps "
        "latest version per grain, creates indexes."
    ),
)
def pre_stg_Vareudlevering_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """
    Pre-staging for ILS delivery data with grain-level deduplication.

    Single-source pre-staging (no joins needed - udlev is a single table).
    CleanCodes are generated inline in SQL from raw archive columns.

    Dedup strategy:
      PARTITION BY clean_code_Vareudlevering
      ORDER BY dw_timestamp_update DESC, prrowid ASC
    Latest version wins; prrowid is a deterministic tie-break.
    """

    dfm = DfmManager(mssql_target_staging, context)

    # Delta filter from downstream checkpoints (MIN of dim/fct owners).
    # full_load mode rebuilds everything; first run (no checkpoints) too.
    dim_cp = dfm.get_checkpoint("dim", "Vareudlevering")
    fct_cp = dfm.get_checkpoint("fct", "Vareudlevering")
    checkpoints = [cp for cp in [dim_cp, fct_cp] if cp is not None]
    min_checkpoint = min(checkpoints) if checkpoints else None
    if config.mode == "full_load":
        min_checkpoint = None  # full rebuild ignores checkpoint

    run_ctx = dfm.start_dfm_run(
        "pre_stg",
        "pre_Vareudlevering_ils",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    if min_checkpoint is not None:
        archive_filter = f"AND dw_timestamp_update > '{min_checkpoint}'"
        context.log.info(f"Checkpoint filter: dw_timestamp_update > {min_checkpoint}")
    else:
        archive_filter = ""
        context.log.info("No checkpoint filter — full reconstruction from archive")

    row_count = 0
    source_total = 0
    duplicates_removed = 0
    unique_lagre = 0
    unique_varer = 0
    null_prrowids = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:

                context.log.info("Creating pre-staging table stg.pre_Vareudlevering_ils...")

                cursor.execute(
                    f"""
                    SELECT
                        COUNT(*) AS total_source_rows,
                        COUNT(DISTINCT CASE WHEN prrowid IS NOT NULL THEN 'UDLEV_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(prrowid AS NVARCHAR(100)), ' ', '')))) END) AS distinct_cleancodes
                    FROM [arc].[ils_udlev]
                    WHERE prrowid IS NOT NULL {archive_filter}
                """
                )
                src_stats = cursor.fetchone()
                source_total = src_stats[0]
                source_distinct = src_stats[1]
                duplicates_removed = source_total - source_distinct
                context.log.info(
                    f"Source arc.ils_udlev: {source_total} rows, "
                    f"{source_distinct} distinct clean_code_Vareudlevering, "
                    f"{duplicates_removed} duplicates to be removed by dedup"
                )
                if duplicates_removed > 0:
                    context.log.warning(
                        f"Dedup will remove {duplicates_removed} duplicate rows "
                        "from arc.ils_udlev (same clean_code_Vareudlevering)"
                    )

                cursor.execute("DROP TABLE IF EXISTS [stg].[pre_Vareudlevering_ils];")

                cursor.execute(
                    f"""
                    SELECT
                        prrowid,
                        sgh,
                        lgr,
                        vnr,
                        lvnr_1,
                        rnr,
                        udlant,
                        udlbel,
                        indkbel,
                        rabatbel,
                        lagerantal,
                        udldto,
                        udlklk,
                        udlobdto,
                        postdto,
                        aflevdto,
                        aflevklk,
                        rvis,
                        id,
                        brik,
                        lnr,
                        sw_direkte,
                        sw_paksdl,
                        sw_flgsdl,
                        sw_fly,
                        sw_haster,
                        sw_erstat,
                        projnr,
                        bruger,
                        refnr,
                        reflinie,
                        salgsordre,
                        salgsordreint,
                        salgsfaknr,
                        rvisvareid,
                        plukvareid,
                        aflevoper,
                        aflevtxt,
                        aflevstreg,
                        vognnummer,
                        flgsednr,
                        ordre,
                        ordlinie,
                        kto,
                        udlevenh,
                        lgrlok,
                        udtraknr3,
                        clean_code_Vareudlevering,
                        clean_code_Lager,
                        clean_code_Rekvirent,
                        clean_code_Leverandoervare_vnr,
                        dw_timestamp_update

                    INTO [stg].[pre_Vareudlevering_ils]
                    FROM (
                        SELECT
                            prrowid,
                            sgh,
                            lgr,
                            vnr,
                            [lvnr##1] AS lvnr_1,
                            rnr,
                            udlant,
                            udlbel,
                            indkbel,
                            rabatbel,
                            lagerantal,
                            udldto,
                            udlklk,
                            udlobdto,
                            postdto,
                            aflevdto,
                            aflevklk,
                            rvis,
                            id,
                            brik,
                            lnr,
                            sw_direkte,
                            sw_paksdl,
                            sw_flgsdl,
                            sw_fly,
                            sw_haster,
                            sw_erstat,
                            projnr,
                            bruger,
                            refnr,
                            reflinie,
                            salgsordre,
                            salgsordreint,
                            salgsfaknr,
                            rvisvareid,
                            plukvareid,
                            aflevoper,
                            aflevtxt,
                            aflevstreg,
                            vognnummer,
                            flgsednr,
                            ordre,
                            ordlinie,
                            kto,
                            udlevenh,
                            lgrlok,
                            udtraknr3,
                            -- CleanCodes generated inline from raw archive columns
                            CASE WHEN prrowid IS NOT NULL
                                THEN 'UDLEV_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(prrowid AS NVARCHAR(100)), ' ', ''))))
                                ELSE NULL
                            END AS clean_code_Vareudlevering,
                            {LAGER_CLEAN_CODE_SQL} AS clean_code_Lager,
                            {REKVIRENT_CLEAN_CODE_SQL} AS clean_code_Rekvirent,
                            CASE WHEN vnr IS NOT NULL AND LTRIM(RTRIM(CAST(vnr AS NVARCHAR(100)))) <> ''
                                THEN 'VARE_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(vnr AS NVARCHAR(100)), ' ', ''))))
                                ELSE NULL
                            END AS clean_code_Leverandoervare_vnr,
                            dw_timestamp_update,
                            ROW_NUMBER() OVER (
                                PARTITION BY CASE WHEN prrowid IS NOT NULL
                                    THEN 'UDLEV_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(prrowid AS NVARCHAR(100)), ' ', ''))))
                                    ELSE NULL END
                                ORDER BY dw_timestamp_update DESC, prrowid ASC
                            ) AS rn
                        FROM [arc].[ils_udlev]
                        WHERE prrowid IS NOT NULL {archive_filter}
                    ) ranked
                    WHERE rn = 1;
                """
                )

                row_count = cursor.rowcount
                context.log.info(
                    f"Created stg.pre_Vareudlevering_ils with {row_count} rows (after dedup)"
                )

                # Create clustered index on delivery CleanCode
                cursor.execute(
                    """
                    CREATE CLUSTERED INDEX IX_pre_vareudlevering_ils_PK
                    ON [stg].[pre_Vareudlevering_ils](clean_code_Vareudlevering);
                """
                )

                # Create non-clustered indexes for map lookups
                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_pre_vareudlevering_ils_Lager
                    ON [stg].[pre_Vareudlevering_ils](clean_code_Lager);
                """
                )

                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_pre_vareudlevering_ils_Vare
                    ON [stg].[pre_Vareudlevering_ils](clean_code_Leverandoervare_vnr);
                """
                )

                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_pre_vareudlevering_ils_Rekvirent
                    ON [stg].[pre_Vareudlevering_ils](clean_code_Rekvirent);
                """
                )

                context.log.info("Created indexes for map lookups")

                conn.commit()

                # Diagnostic: Check coverage
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        COUNT(clean_code_Vareudlevering) AS has_vareudlev_clean,
                        COUNT(clean_code_Lager) AS has_lager_clean,
                        COUNT(clean_code_Leverandoervare_vnr) AS has_vare_clean,
                        COUNT(DISTINCT clean_code_Lager) AS unique_lagre,
                        COUNT(DISTINCT clean_code_Leverandoervare_vnr) AS unique_varer,
                        COALESCE(SUM(CASE WHEN prrowid IS NULL THEN 1 ELSE 0 END), 0) AS null_prrowids
                    FROM [stg].[pre_Vareudlevering_ils]
                """
                )
                stats = cursor.fetchone()
                total = stats[0] or 0
                has_vareudlev = stats[1] or 0
                has_lager = stats[2] or 0
                has_vare = stats[3] or 0
                unique_lagre = stats[4] or 0
                unique_varer = stats[5] or 0
                null_prrowids = stats[6] or 0
                context.log.info(
                    f"Coverage: {has_vareudlev}/{total} with clean_code_Vareudlevering, "
                    f"{has_lager}/{total} with clean_code_Lager, "
                    f"{has_vare}/{total} with clean_code_Leverandoervare_vnr. "
                    f"Unique lagre: {unique_lagre}, unique varer: {unique_varer}"
                )
                if null_prrowids > 0:
                    context.log.warning(
                        f"{null_prrowids} rows with NULL prrowid — these will not get a "
                        "clean_code_Vareudlevering and will be excluded from the fact table"
                    )
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.pre_Vareudlevering_ils",
            "row_count": row_count,
            "source_rows_before_dedup": source_total,
            "duplicates_removed": duplicates_removed,
            "source_tables": "arc.ils_udlev",
            "grain": "clean_code_Vareudlevering (one row per prrowid, deduped)",
            "unique_lagre": unique_lagre,
            "unique_varer": unique_varer,
            "null_prrowids": null_prrowids,
            "mode": config.mode,
            "checkpoint_filter": min_checkpoint if min_checkpoint else "none (full load)",
            "layer": "pre_stg",
        }
    )

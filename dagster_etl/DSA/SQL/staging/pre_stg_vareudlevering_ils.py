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
Grain: one row per delivery line, identified by CleanCode_Vareudlevering (prrowid).

Dedup rationale: arc.ils_udlev is UPSERT-keyed on prrowid, so there is at most
one row per business key. The ROW_NUMBER dedup here is retained defensively so
both downstream consumers (stg_dim_vareudlevering_ils and
stg_fct_vareudlevering_ils) are protected from any grain violations.
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


@asset(
    key=AssetKey(["erp", "pre_stg_vareudlevering_ils"]),
    group_name="pre_staging_vareudlevering",
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
        "dedup on CleanCode_Vareudlevering. Generates CleanCodes inline, keeps "
        "latest version per grain, creates indexes."
    ),
)
def pre_stg_vareudlevering_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """
    Pre-staging for ILS delivery data with grain-level deduplication.

    Single-source pre-staging (no joins needed - udlev is a single table).
    CleanCodes are generated inline in SQL from raw archive columns.

    Dedup strategy:
      PARTITION BY CleanCode_Vareudlevering
      ORDER BY DW_Timestamp_Update DESC, prrowid ASC
    Latest version wins; prrowid is a deterministic tie-break.
    """

    dfm = DfmManager(mssql_target_staging, context)

    dim_cp = dfm.get_checkpoint("dim", "VareudleveringEgenskaber")
    fct_cp = dfm.get_checkpoint("fct", "Vareudlevering")
    checkpoints = [cp for cp in [dim_cp, fct_cp] if cp is not None]
    min_checkpoint = min(checkpoints) if checkpoints else None

    run_ctx = dfm.start_dfm_run(
        "pre_stg",
        "pre_vareudlevering_ils",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    if min_checkpoint is not None:
        archive_filter = f"AND DW_Timestamp_Update > '{min_checkpoint}'"
        context.log.info(f"Checkpoint filter: DW_Timestamp_Update > {min_checkpoint}")
    else:
        archive_filter = ""
        context.log.info("No checkpoint found — loading all rows from archive")

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

                context.log.info("Creating pre-staging table stg.pre_vareudlevering_ils...")

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
                    f"{source_distinct} distinct CleanCode_Vareudlevering, "
                    f"{duplicates_removed} duplicates to be removed by dedup"
                )
                if duplicates_removed > 0:
                    context.log.warning(
                        f"Dedup will remove {duplicates_removed} duplicate rows "
                        "from arc.ils_udlev (same CleanCode_Vareudlevering)"
                    )

                cursor.execute("DROP TABLE IF EXISTS [stg].[pre_vareudlevering_ils];")

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
                        CleanCode_Vareudlevering,
                        Lager_CleanCode,
                        Rekvirent_CleanCode,
                        Vare_CleanCode,
                        DW_Timestamp_Update

                    INTO [stg].[pre_vareudlevering_ils]
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
                            -- CleanCodes generated inline from raw archive columns
                            CASE WHEN prrowid IS NOT NULL
                                THEN 'UDLEV_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(prrowid AS NVARCHAR(100)), ' ', ''))))
                                ELSE NULL
                            END AS CleanCode_Vareudlevering,
                            CASE
                                WHEN sgh IS NOT NULL AND lgr IS NOT NULL
                                THEN 'SGH_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(sgh AS NVARCHAR(50)), ' ', '')))) + '_LGR_' + RIGHT('0000' + UPPER(LTRIM(RTRIM(REPLACE(CAST(lgr AS NVARCHAR(50)), ' ', '')))), 4)
                                WHEN sgh IS NOT NULL
                                THEN 'SGH_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(sgh AS NVARCHAR(50)), ' ', ''))))
                                WHEN lgr IS NOT NULL
                                THEN 'LGR_' + RIGHT('0000' + UPPER(LTRIM(RTRIM(REPLACE(CAST(lgr AS NVARCHAR(50)), ' ', '')))), 4)
                                ELSE NULL
                            END AS Lager_CleanCode,
                            CASE
                                WHEN sgh IS NOT NULL AND rnr IS NOT NULL
                                THEN 'SGH_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(sgh AS NVARCHAR(50)), ' ', '')))) + '_RNR_' + RIGHT('0000' + UPPER(LTRIM(RTRIM(REPLACE(CAST(rnr AS NVARCHAR(50)), ' ', '')))), 4)
                                WHEN sgh IS NOT NULL
                                THEN 'SGH_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(sgh AS NVARCHAR(50)), ' ', ''))))
                                WHEN rnr IS NOT NULL
                                THEN 'RNR_' + RIGHT('0000' + UPPER(LTRIM(RTRIM(REPLACE(CAST(rnr AS NVARCHAR(50)), ' ', '')))), 4)
                                ELSE NULL
                            END AS Rekvirent_CleanCode,
                            CASE WHEN vnr IS NOT NULL AND LTRIM(RTRIM(CAST(vnr AS NVARCHAR(100)))) <> ''
                                THEN 'VARE_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(vnr AS NVARCHAR(100)), ' ', ''))))
                                ELSE NULL
                            END AS Vare_CleanCode,
                            DW_Timestamp_Update,
                            ROW_NUMBER() OVER (
                                PARTITION BY CASE WHEN prrowid IS NOT NULL
                                    THEN 'UDLEV_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(prrowid AS NVARCHAR(100)), ' ', ''))))
                                    ELSE NULL END
                                ORDER BY DW_Timestamp_Update DESC, prrowid ASC
                            ) AS rn
                        FROM [arc].[ils_udlev]
                        WHERE prrowid IS NOT NULL {archive_filter}
                    ) ranked
                    WHERE rn = 1;
                """
                )

                row_count = cursor.rowcount
                context.log.info(
                    f"Created stg.pre_vareudlevering_ils with {row_count} rows (after dedup)"
                )

                # Create clustered index on delivery CleanCode
                cursor.execute(
                    """
                    CREATE CLUSTERED INDEX IX_pre_vareudlevering_ils_PK
                    ON [stg].[pre_vareudlevering_ils](CleanCode_Vareudlevering);
                """
                )

                # Create non-clustered indexes for map lookups
                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_pre_vareudlevering_ils_Lager
                    ON [stg].[pre_vareudlevering_ils](Lager_CleanCode);
                """
                )

                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_pre_vareudlevering_ils_Vare
                    ON [stg].[pre_vareudlevering_ils](Vare_CleanCode);
                """
                )

                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_pre_vareudlevering_ils_Rekvirent
                    ON [stg].[pre_vareudlevering_ils](Rekvirent_CleanCode);
                """
                )

                context.log.info("Created indexes for map lookups")

                conn.commit()

                # Diagnostic: Check coverage
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        COUNT(CleanCode_Vareudlevering) AS has_vareudlev_clean,
                        COUNT(Lager_CleanCode) AS has_lager_clean,
                        COUNT(Vare_CleanCode) AS has_vare_clean,
                        COUNT(DISTINCT Lager_CleanCode) AS unique_lagre,
                        COUNT(DISTINCT Vare_CleanCode) AS unique_varer,
                        COALESCE(SUM(CASE WHEN prrowid IS NULL THEN 1 ELSE 0 END), 0) AS null_prrowids
                    FROM [stg].[pre_vareudlevering_ils]
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
                    f"Coverage: {has_vareudlev}/{total} with CleanCode_Vareudlevering, "
                    f"{has_lager}/{total} with Lager_CleanCode, "
                    f"{has_vare}/{total} with Vare_CleanCode. "
                    f"Unique lagre: {unique_lagre}, unique varer: {unique_varer}"
                )
                if null_prrowids > 0:
                    context.log.warning(
                        f"{null_prrowids} rows with NULL prrowid — these will not get a "
                        "CleanCode_Vareudlevering and will be excluded from the fact table"
                    )
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.pre_vareudlevering_ils",
            "row_count": row_count,
            "source_rows_before_dedup": source_total,
            "duplicates_removed": duplicates_removed,
            "source_tables": "arc.ils_udlev",
            "grain": "CleanCode_Vareudlevering (one row per prrowid, deduped)",
            "unique_lagre": unique_lagre,
            "unique_varer": unique_varer,
            "null_prrowids": null_prrowids,
            "mode": config.mode,
            "checkpoint_filter": min_checkpoint if min_checkpoint else "none (full load)",
            "layer": "pre_stg",
        }
    )

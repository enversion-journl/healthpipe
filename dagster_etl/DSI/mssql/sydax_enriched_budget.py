# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SydAX enriched extraction: ext.budgetpost_enriched

EXCEPTION to standard DSI 1:1 contract (documented in sql_etl_guide.md):
This asset performs multi-table JOINs directly against SydAX source tables and
writes the enriched result to ext.budgetpost_enriched on the target DW.
The exception exists because the enrichment requires cross-schema joins that
cannot be expressed via the standard TableConfig factory.

The entire enrichment query executes on the SydAX source database; the result
set is streamed into the target DW via MssqlExtractor.custom_stream_extract_and_insert
which uses chunked bulk-copy to avoid holding the full result set in memory.

Partition columns: Oprettelsestidspunkt OR SidstAendretTidspunkt (on Budgetpost)
Grain: ref_LedgerbudgetId (unique BIGINT from the source budget ledger)
"""

from datetime import datetime, timedelta

import dagster as dg
from core.partitions import (
    daily_partitions_erp,
    is_first_partition,
    monthly_partitions_erp,
)
from core.resources import MSSQLResource, SydaxMSSQLResource
from DSI.connectors.mssql.extractor import MssqlExtractor
from DSI.connectors.mssql.models import TableConfig

BUDGET_SYDAX_KILDESYSTEM = "SydAX"

ENRICHED_TARGET_TABLE = "budgetpost_enriched"

_ENRICHED_TABLE_CONFIG = TableConfig(
    output_name="ext_budgetpost_enriched_sydax",
    source_schema="Finans",
    source_table="Budgetpost",
    target_table=ENRICHED_TARGET_TABLE,
    description="Enriched Budgetpost with hierarchy lookups from SydAX",
)


def _build_partition_filter(
    partition_key: str,
    is_scheduled_catchup: bool = False,
    first_partition: bool = False,
) -> str:
    """Build WHERE clause fragment for dual-column partitioning (Oprettelsestidspunkt OR SidstAendretTidspunkt)."""
    is_monthly = len(partition_key) == 7 and partition_key.count("-") == 1

    if is_monthly:
        from calendar import monthrange

        year, month = map(int, partition_key.split("-"))
        _, last_day = monthrange(year, month)
        start_date = f"{year:04d}-{month:02d}-01"
        end_date = f"{year:04d}-{month:02d}-{last_day:02d}"
        if first_partition:
            date_filter = (
                f"(CAST(bp.[Oprettelsestidspunkt] AS DATE) <= '{end_date}' "
                f"OR CAST(bp.[SidstAendretTidspunkt] AS DATE) <= '{end_date}')"
            )
        else:
            date_filter = (
                f"(CAST(bp.[Oprettelsestidspunkt] AS DATE) >= '{start_date}' "
                f"AND CAST(bp.[Oprettelsestidspunkt] AS DATE) <= '{end_date}') "
                f"OR (CAST(bp.[SidstAendretTidspunkt] AS DATE) >= '{start_date}' "
                f"AND CAST(bp.[SidstAendretTidspunkt] AS DATE) <= '{end_date}')"
            )
    elif is_scheduled_catchup:
        today_date = datetime.strptime(partition_key, "%Y-%m-%d")
        yesterday_date = today_date - timedelta(days=1)
        start_ts = yesterday_date.strftime("%Y-%m-%d") + " 01:00:00"
        date_filter = (
            f"(bp.[Oprettelsestidspunkt] >= '{start_ts}' "
            f"OR bp.[SidstAendretTidspunkt] >= '{start_ts}')"
        )
    elif first_partition:
        date_filter = (
            f"(CAST(bp.[Oprettelsestidspunkt] AS DATE) <= '{partition_key}' "
            f"OR CAST(bp.[SidstAendretTidspunkt] AS DATE) <= '{partition_key}')"
        )
    else:
        date_filter = (
            f"(CAST(bp.[Oprettelsestidspunkt] AS DATE) = '{partition_key}' "
            f"OR CAST(bp.[SidstAendretTidspunkt] AS DATE) = '{partition_key}')"
        )

    return date_filter


def _detect_scheduled_catchup(context: dg.AssetExecutionContext) -> bool:
    return (
        context.has_tag("erp_extraction_mode")
        and context.run.tags.get("erp_extraction_mode") == "scheduled_catchup"
    )


def _build_enrichment_sql(partition_filter: str) -> str:
    """Build the enrichment SELECT that runs directly on the SydAX source.

    JOINs Budgetpost to Budgetmodel (for ref_BudgetmodelId), Finanskonto,
    FinanskontoOmkostningssted, OmkostningsstedHierarki, Hovedkonto,
    Gruppering, GrupperingAutoriseret, Art, Funktion, Dranst, and BudgetpostStatus.

    Dedup CTEs are only used for tables with proven duplicates on the join key
    (Hovedkonto, Gruppering, Art, Funktion). Tables confirmed unique by
    investigate_budget_dedup.py are joined directly without ROW_NUMBER overhead.
    ORDER BY uses EndDateTime DESC (temporal tables) or Modified DESC (Funktion)
    for deterministic tie-breaking, consistent with sydax_enriched_finanskladde.py.
    """
    return f"""
        ;WITH hk_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [ref_RECID] ORDER BY [EndDateTime] DESC
                   ) AS rn
            FROM [Finans].[Hovedkonto]
            WHERE [ref_RECID] IS NOT NULL
        ),
        grp_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [RECID] ORDER BY [EndDateTime] DESC
                   ) AS rn
            FROM [Finans].[Gruppering]
            WHERE [RECID] IS NOT NULL
        ),
        art_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [RECID] ORDER BY [EndDateTime] DESC
                   ) AS rn
            FROM [Finans].[Art]
            WHERE [RECID] IS NOT NULL
        ),
        art_a1_dedup AS (
            SELECT [A1], [Artstekst],
                   ROW_NUMBER() OVER (
                       PARTITION BY [A1] ORDER BY [EndDateTime] DESC
                   ) AS rn
            FROM [Finans].[Art]
            WHERE [A1] IS NOT NULL AND [A1] <> ''
              AND ([A2] IS NULL OR [A2] = '')
        ),
        art_a2_dedup AS (
            SELECT [A1], [A2], [Artstekst],
                   ROW_NUMBER() OVER (
                       PARTITION BY [A1], [A2] ORDER BY [EndDateTime] DESC
                   ) AS rn
            FROM [Finans].[Art]
            WHERE [A1] IS NOT NULL AND [A1] <> ''
              AND [A2] IS NOT NULL AND [A2] <> ''
        ),
        funk_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [F1], [F2], [F3] ORDER BY [Modified] DESC
                   ) AS rn
            FROM [Finans].[Funktion]
        )

        SELECT
            CAST(bp.[ref_LedgerbudgetId] AS BIGINT) AS ref_LedgerbudgetId,

            CASE
                WHEN bp.[ref_LedgerbudgetId] IS NOT NULL
                THEN 'BUDGET_' + UPPER(LTRIM(RTRIM(REPLACE(
                         CAST(bp.[ref_LedgerbudgetId] AS NVARCHAR(100)), ' ', '')
                     )))
                ELSE NULL
            END AS CleanCode_Budget,

            'BUDGETEGENSKABER'
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(REPLACE(
                    CAST(bm.[ref_BudgetmodelId] AS NVARCHAR(100)), ' ', '')))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(CAST(bm.[Budgetmodelnummer] AS NVARCHAR(100))))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(CAST(bm.[Budgetmodeltype] AS NVARCHAR(100))))), '')
                + '_' + ISNULL(CAST(bm.[ErLukket] AS NVARCHAR(10)), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(bp.[Valutakode]))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(bp.[DebetKredit]))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(bp.[Budgetkode]))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(bp.[Ændringstype]))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(bp.[ÆndringstypeBeskrivelse]))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(CAST(bp.[BeloebstypeId] AS NVARCHAR(50))))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(CAST(bps.[BudgetpostStatusId] AS NVARCHAR(50))))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(COALESCE(
                    CAST(omk_hier.[nummer20] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer19] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer18] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer17] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer16] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer15] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer14] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer13] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer12] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer11] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer10] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer9] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer8] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer7] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer6] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer5] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer4] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer3] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer2] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer1] AS NVARCHAR(50))
                )))), '')
            AS CleanCode_BudgetEgenskaber,
            CAST(bm.[ref_BudgetmodelId] AS NVARCHAR(100)) AS ref_BudgetmodelId,
            CAST(bm.[Budgetmodelnummer] AS NVARCHAR(100)) AS Budgetmodelnummer,
            CAST(bm.[Budgetmodeltekst] AS NVARCHAR(255)) AS Budgetmodeltekst,
            CAST(bm.[Budgetmodeltype] AS NVARCHAR(100)) AS Budgetmodeltype,
            bm.[ErLukket] AS ErLukket_Budgetmodel,

            'KONTOPLAN_' + UPPER(LTRIM(RTRIM(REPLACE(
                CAST(konto.[FinanskontoId] AS NVARCHAR(100)), ' ', '')
            ))) AS CleanCode_Kontoplan,

            CAST(bp.[FinanskontoId] AS BIGINT) AS FinanskontoId,
            CAST(bp.[FinanskontoId] AS NVARCHAR(100)) AS bk_FinanskontoId,
            konto.[Kontonummer],

            CAST(hk.[F1] AS NVARCHAR(50)) AS F1,
            CAST(hk.[F2] AS NVARCHAR(50)) AS F2,
            CAST(hk.[F3] AS NVARCHAR(50)) AS F3,
            CAST(hk.[D1] AS NVARCHAR(50)) AS D1,
            CAST(hk.[G1] AS NVARCHAR(50)) AS G1,

            CAST(omk_hier.[nummer1] AS NVARCHAR(50)) AS S1,
            CAST(omk_hier.[nummer2] AS NVARCHAR(50)) AS S2,
            CAST(omk_hier.[nummer3] AS NVARCHAR(50)) AS S3,
            CAST(omk_hier.[nummer4] AS NVARCHAR(50)) AS S4,
            CAST(omk_hier.[nummer5] AS NVARCHAR(50)) AS S5,
            CAST(omk_hier.[nummer6] AS NVARCHAR(50)) AS S6,
            CAST(omk_hier.[nummer7] AS NVARCHAR(50)) AS S7,
            CAST(omk_hier.[nummer8] AS NVARCHAR(50)) AS S8,
            CAST(omk_hier.[nummer9] AS NVARCHAR(50)) AS S9,
            CAST(omk_hier.[nummer10] AS NVARCHAR(50)) AS S10,
            CAST(omk_hier.[nummer11] AS NVARCHAR(50)) AS S11,
            CAST(omk_hier.[nummer12] AS NVARCHAR(50)) AS S12,
            CAST(omk_hier.[nummer13] AS NVARCHAR(50)) AS S13,
            CAST(omk_hier.[nummer14] AS NVARCHAR(50)) AS S14,
            CAST(omk_hier.[nummer15] AS NVARCHAR(50)) AS S15,
            CAST(omk_hier.[nummer16] AS NVARCHAR(50)) AS S16,
            CAST(omk_hier.[nummer17] AS NVARCHAR(50)) AS S17,
            CAST(omk_hier.[nummer18] AS NVARCHAR(50)) AS S18,
            CAST(omk_hier.[nummer19] AS NVARCHAR(50)) AS S19,
            CAST(omk_hier.[nummer20] AS NVARCHAR(50)) AS S20,

            CAST(grp.[Grupperingsnummer] AS NVARCHAR(50)) AS G2,
            CAST(art.[A1] AS NVARCHAR(50)) AS A1,
            CAST(art.[A2] AS NVARCHAR(50)) AS A2,

            CAST(omk_hier.[tekst1] AS NVARCHAR(255)) AS tekst_S1,
            CAST(omk_hier.[tekst2] AS NVARCHAR(255)) AS tekst_S2,
            CAST(omk_hier.[tekst3] AS NVARCHAR(255)) AS tekst_S3,
            CAST(omk_hier.[tekst4] AS NVARCHAR(255)) AS tekst_S4,
            CAST(omk_hier.[tekst5] AS NVARCHAR(255)) AS tekst_S5,
            CAST(omk_hier.[tekst6] AS NVARCHAR(255)) AS tekst_S6,
            CAST(omk_hier.[tekst7] AS NVARCHAR(255)) AS tekst_S7,
            CAST(omk_hier.[tekst8] AS NVARCHAR(255)) AS tekst_S8,
            CAST(omk_hier.[tekst9] AS NVARCHAR(255)) AS tekst_S9,
            CAST(omk_hier.[tekst10] AS NVARCHAR(255)) AS tekst_S10,
            CAST(omk_hier.[tekst11] AS NVARCHAR(255)) AS tekst_S11,
            CAST(omk_hier.[tekst12] AS NVARCHAR(255)) AS tekst_S12,
            CAST(omk_hier.[tekst13] AS NVARCHAR(255)) AS tekst_S13,
            CAST(omk_hier.[tekst14] AS NVARCHAR(255)) AS tekst_S14,
            CAST(omk_hier.[tekst15] AS NVARCHAR(255)) AS tekst_S15,
            CAST(omk_hier.[tekst16] AS NVARCHAR(255)) AS tekst_S16,
            CAST(omk_hier.[tekst17] AS NVARCHAR(255)) AS tekst_S17,
            CAST(omk_hier.[tekst18] AS NVARCHAR(255)) AS tekst_S18,
            CAST(omk_hier.[tekst19] AS NVARCHAR(255)) AS tekst_S19,
            CAST(omk_hier.[tekst20] AS NVARCHAR(255)) AS tekst_S20,

            COALESCE(
                CAST(grp_aut.[G1Tekst] AS NVARCHAR(255)),
                CAST(grp.[Grupperingstekst] AS NVARCHAR(255))
            ) AS tekst_G1,
            CAST(grp.[Grupperingstekst] AS NVARCHAR(255)) AS tekst_G2,
            CAST(art_a1.[Artstekst] AS NVARCHAR(255)) AS tekst_A1,
            CAST(art_a2.[Artstekst] AS NVARCHAR(255)) AS tekst_A2,
            CAST(funk_f1.[FunktionTekst] AS NVARCHAR(255)) AS tekst_F1,
            CAST(funk_f2.[FunktionTekst] AS NVARCHAR(255)) AS tekst_F2,
            CAST(funk_f3.[FunktionTekst] AS NVARCHAR(255)) AS tekst_F3,
            CAST(dr.[D1Tekst] AS NVARCHAR(255)) AS tekst_D1,

            CAST(omkost_link.[Omkostningsstedid] AS BIGINT) AS Omkostningsstedid,
            COALESCE(
                CAST(omk_hier.[nummer20] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer19] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer18] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer17] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer16] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer15] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer14] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer13] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer12] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer11] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer10] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer9] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer8] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer7] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer6] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer5] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer4] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer3] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer2] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer1] AS NVARCHAR(50))
            ) AS Omkostningsstednummer,

            TRY_CONVERT(DATE, bp.[Budgetdato]) AS Budgetdato,
            TRY_CONVERT(DATE, bp.[BudgetAdoptedDate]) AS BudgetAdoptedDate,
            TRY_CONVERT(DATETIME2, bp.[Oprettelsestidspunkt]) AS Oprettelsestidspunkt,
            TRY_CONVERT(DATETIME2, bp.[SidstAendretTidspunkt]) AS SidstAendretTidspunkt,

            TRY_CONVERT(DECIMAL(30,12), bp.[Budgetbeloeb]) AS Budgetbeloeb,
            TRY_CONVERT(DECIMAL(30,12), bp.[BudgetbeloebValuta]) AS BudgetbeloebValuta,
            bp.[Valutakode],
            bp.[DebetKredit],
            bp.[Budgetkode],
            bp.[Ændringstype] AS Aendringstype,
            bp.[ÆndringstypeBeskrivelse] AS AendringstypeBeskrivelse,
            CAST(bp.[BeloebstypeId] AS NVARCHAR(50)) AS BeloebstypeId,
            CAST(bbt.[BudgetbeløbTypetekst] AS NVARCHAR(1000)) AS BudgetbeloebTypetekst,
            CAST(bps.[BudgetpostStatusId] AS NVARCHAR(50)) AS BudgetpostStatusId,
            CAST(bps.[BudgetpostStatustekst] AS NVARCHAR(255)) AS BudgetpostStatusTekst

        FROM [Finans].[Budgetpost] bp

        LEFT JOIN [Finans].[Budgetmodel] bm
            ON bm.[BudgetmodelId] = bp.[BudgetmodelId]
            AND bm.[BudgetmodelId] IS NOT NULL
            AND bm.[Kildesystem] = '{BUDGET_SYDAX_KILDESYSTEM}'

        LEFT JOIN [Finans].[Finanskonto] konto
            ON konto.[FinanskontoId] = bp.[FinanskontoId]
            AND konto.[FinanskontoId] IS NOT NULL

        LEFT JOIN [Finans].[FinanskontoOmkostningssted] omkost_link
            ON omkost_link.[FinanskontoId] = bp.[FinanskontoId]

        LEFT JOIN [Finans].[OmkostningsstedHierarki] omk_hier
            ON CAST(CAST(omk_hier.[max_OmkostningsstedId] AS FLOAT) AS BIGINT) =
               CAST(omkost_link.[Omkostningsstedid] AS BIGINT)

        LEFT JOIN hk_dedup hk
            ON hk.[ref_RECID] = konto.[hk_RECID]
            AND hk.rn = 1

        LEFT JOIN grp_dedup grp
            ON grp.[RECID] = konto.[grp_RECID]
            AND grp.rn = 1

        LEFT JOIN [Finans].[GrupperingAutoriseret] grp_aut
            ON grp_aut.[ref_RECID] = konto.[grp_RECID]

        LEFT JOIN art_dedup art
            ON art.[RECID] = konto.[art_RECID]
            AND art.rn = 1

        LEFT JOIN art_a1_dedup art_a1
            ON art_a1.[A1] = art.[A1]
            AND art_a1.rn = 1

        LEFT JOIN art_a2_dedup art_a2
            ON art_a2.[A1] = art.[A1]
            AND art_a2.[A2] = art.[A2]
            AND art_a2.rn = 1

        LEFT JOIN funk_dedup funk_f1
            ON funk_f1.[F1] = hk.[F1]
            AND funk_f1.[F2] IS NULL
            AND funk_f1.[F3] IS NULL
            AND funk_f1.rn = 1

        LEFT JOIN funk_dedup funk_f2
            ON funk_f2.[F1] = hk.[F1]
            AND funk_f2.[F2] = hk.[F2]
            AND funk_f2.[F3] IS NULL
            AND funk_f2.rn = 1

        LEFT JOIN funk_dedup funk_f3
            ON funk_f3.[F1] = hk.[F1]
            AND funk_f3.[F2] = hk.[F2]
            AND funk_f3.[F3] = hk.[F3]
            AND funk_f3.rn = 1

        LEFT JOIN [Finans].[Dranst] dr
            ON dr.[ref_RECID] = TRY_CAST(konto.[d1_RECID] AS BIGINT)

        LEFT JOIN [Finans].[BudgetpostStatus] bps
            ON bps.[BudgetpostStatusId] = bp.[BudgetpostStatusId]

        LEFT JOIN [Finans].[BudgetbeløbType] bbt
            ON bbt.[BudgetbeløbTypeId] = bp.[BeloebstypeId]

        WHERE ({partition_filter})
          AND bp.[Kildesystem] = '{BUDGET_SYDAX_KILDESYSTEM}';
    """


def _make_enriched_asset(partition_type: str):
    """Build a daily or monthly enriched extraction asset."""
    suffix = partition_type
    partitions_def = daily_partitions_erp if partition_type == "daily" else monthly_partitions_erp

    @dg.asset(
        key=dg.AssetKey([f"ext_budgetpost_enriched_sydax_{suffix}"]),
        group_name=f"erp_extract_{suffix}",
        compute_kind="mssql",
        partitions_def=partitions_def,
        tags={
            "domain": "erp",
            "layer": "extract",
            "source": "sydax",
            "entity": "budgetpost_enriched",
        },
        retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
        description=(
            f"Enriched extraction from SydAX: joins Budgetpost with "
            f"Budgetmodel, finance hierarchy, and status tables — query runs "
            f"directly on SydAX source, result inserted into ext.{ENRICHED_TARGET_TABLE}. "
            f"Partitioned by Oprettelsestidspunkt OR SidstAendretTidspunkt ({suffix}). "
            f"Grain: ref_LedgerbudgetId."
        ),
    )
    def _extract_enriched(
        context: dg.AssetExecutionContext,
        sydax_mssql: SydaxMSSQLResource,
        mssql_target_staging: MSSQLResource,
    ) -> dg.MaterializeResult:
        partition_key = context.partition_key if context.has_partition_key else None
        scheduled_catchup = _detect_scheduled_catchup(context)
        first_part = bool(partition_key) and is_first_partition(partition_key)

        if partition_key:
            context.log.info(
                f"Running enriched SydAX budget extraction for {suffix} "
                f"partition: {partition_key}"
            )

        partition_filter = _build_partition_filter(
            partition_key or "2005-01-01",
            is_scheduled_catchup=scheduled_catchup,
            first_partition=first_part,
        )
        context.log.info(f"Partition filter: {partition_filter}")

        enrichment_sql = _build_enrichment_sql(partition_filter)

        with sydax_mssql.get_connection() as source_conn:
            with mssql_target_staging.get_connection() as target_conn:
                with mssql_target_staging.get_connection() as bulkcopy_conn:
                    extractor = MssqlExtractor(
                        source_conn=source_conn,
                        target_conn=target_conn,
                        context=context,
                        source_system="SydAX",
                        bulkcopy_conn=bulkcopy_conn,
                        mssql_target_resource=mssql_target_staging,
                    )

                    row_count = extractor.custom_stream_extract_and_insert(
                        enrichment_sql, _ENRICHED_TABLE_CONFIG, partition_key
                    )
                    context.log.info(f"Inserted {row_count} rows into ext.{ENRICHED_TARGET_TABLE}")

                target_cursor = target_conn.cursor()
                try:
                    target_cursor.execute(
                        f"""
                        IF NOT EXISTS (
                            SELECT 1 FROM sys.indexes
                            WHERE name = 'IX_{ENRICHED_TARGET_TABLE}_PK'
                              AND object_id = OBJECT_ID('[ext].[{ENRICHED_TARGET_TABLE}]')
                        )
                        CREATE CLUSTERED INDEX IX_{ENRICHED_TARGET_TABLE}_PK
                        ON [ext].[{ENRICHED_TARGET_TABLE}](ref_LedgerbudgetId);
                        """
                    )
                    target_cursor.execute(
                        f"""
                        IF NOT EXISTS (
                            SELECT 1 FROM sys.indexes
                            WHERE name = 'IX_{ENRICHED_TARGET_TABLE}_Kontoplan'
                              AND object_id = OBJECT_ID('[ext].[{ENRICHED_TARGET_TABLE}]')
                        )
                        CREATE NONCLUSTERED INDEX IX_{ENRICHED_TARGET_TABLE}_Kontoplan
                        ON [ext].[{ENRICHED_TARGET_TABLE}](CleanCode_Kontoplan);
                        """
                    )
                    target_conn.commit()

                    target_cursor.execute(
                        f"""
                        SELECT
                            COUNT(*) AS total,
                            COUNT(CleanCode_Kontoplan) AS has_cleancode_kontoplan,
                            COUNT(DISTINCT CleanCode_Kontoplan) AS unique_cleancode_kontoplan,
                            COUNT(CleanCode_Budget) AS has_cleancode_budget,
                            COUNT(DISTINCT CleanCode_Budget) AS unique_budget,
                            COUNT(CleanCode_BudgetEgenskaber) AS has_cleancode_BudgetEgenskaber,
                            COUNT(DISTINCT CleanCode_BudgetEgenskaber) AS unique_BudgetEgenskaber,
                            COUNT(Omkostningsstednummer) AS has_okosted,
                            COUNT(Budgetdato) AS has_budgetdato,
                            COUNT(Budgetbeloeb) AS has_budgetbeloeb
                        FROM [ext].[{ENRICHED_TARGET_TABLE}]
                        """
                    )
                    stats = target_cursor.fetchone()
                    context.log.info(
                        f"Coverage: {stats[1]}/{stats[0]} with CleanCode_Kontoplan, "
                        f"unique: {stats[2]}. "
                        f"CleanCode_Budget: {stats[3]}/{stats[0]}, unique: {stats[4]}. "
                        f"CleanCode_BudgetEgenskaber: {stats[5]}/{stats[0]}, unique: {stats[6]}. "
                        f"OKOSTED: {stats[7]}/{stats[0]}. "
                        f"Budgetdato: {stats[8]}/{stats[0]}. "
                        f"Budgetbeloeb: {stats[9]}/{stats[0]}."
                    )
                finally:
                    target_cursor.close()

        return dg.MaterializeResult(
            metadata={
                "table": f"ext.{ENRICHED_TARGET_TABLE}",
                "row_count": row_count,
                "partition_key": partition_key or "N/A",
                "partition_filter": partition_filter,
                "grain": "ref_LedgerbudgetId",
                "unique_kontoplan": stats[2],
                "unique_budget": stats[4],
                "unique_BudgetEgenskaber": stats[6],
                "has_okosted": stats[7],
                "has_budgetdato": stats[8],
                "has_budgetbeloeb": stats[9],
            }
        )

    _extract_enriched.__name__ = f"ext_budgetpost_enriched_sydax_{suffix}"
    _extract_enriched.__qualname__ = f"ext_budgetpost_enriched_sydax_{suffix}"
    return _extract_enriched


ext_budgetpost_enriched_sydax_daily = _make_enriched_asset("daily")
ext_budgetpost_enriched_sydax_monthly = _make_enriched_asset("monthly")


@dg.asset(
    key=dg.AssetKey(["ext_budgetpost_enriched_sydax_full_load"]),
    group_name="erp_extract_full_load",
    compute_kind="mssql",
    partitions_def=None,
    tags={
        "domain": "erp",
        "layer": "extract",
        "source": "sydax",
        "entity": "budgetpost_enriched",
        "partition": "full_load",
    },
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
    description=(
        "Full-load enriched extraction from SydAX: joins Budgetpost with "
        "Budgetmodel, finance hierarchy, and status tables — query runs "
        f"directly on SydAX source, result inserted into ext.{ENRICHED_TARGET_TABLE}. "
        "No partition filter (full reload). "
        "Grain: ref_LedgerbudgetId."
    ),
)
def ext_budgetpost_enriched_sydax_full_load(
    context: dg.AssetExecutionContext,
    sydax_mssql: SydaxMSSQLResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    context.log.info("Running full-load enriched SydAX budget extraction (no date filter)")

    partition_filter = "1=1"
    enrichment_sql = _build_enrichment_sql(partition_filter)

    with sydax_mssql.get_connection() as source_conn:
        with mssql_target_staging.get_connection() as target_conn:
            with mssql_target_staging.get_connection() as bulkcopy_conn:
                extractor = MssqlExtractor(
                    source_conn=source_conn,
                    target_conn=target_conn,
                    context=context,
                    source_system="SydAX",
                    bulkcopy_conn=bulkcopy_conn,
                    mssql_target_resource=mssql_target_staging,
                )

                row_count = extractor.custom_stream_extract_and_insert(
                    enrichment_sql, _ENRICHED_TABLE_CONFIG, None
                )
                context.log.info(f"Inserted {row_count} rows into ext.{ENRICHED_TARGET_TABLE}")

            target_cursor = target_conn.cursor()
            try:
                target_cursor.execute(
                    f"""
                    IF NOT EXISTS (
                        SELECT 1 FROM sys.indexes
                        WHERE name = 'IX_{ENRICHED_TARGET_TABLE}_PK'
                          AND object_id = OBJECT_ID('[ext].[{ENRICHED_TARGET_TABLE}]')
                    )
                    CREATE CLUSTERED INDEX IX_{ENRICHED_TARGET_TABLE}_PK
                    ON [ext].[{ENRICHED_TARGET_TABLE}](ref_LedgerbudgetId);
                    """
                )
                target_cursor.execute(
                    f"""
                    IF NOT EXISTS (
                        SELECT 1 FROM sys.indexes
                        WHERE name = 'IX_{ENRICHED_TARGET_TABLE}_Kontoplan'
                          AND object_id = OBJECT_ID('[ext].[{ENRICHED_TARGET_TABLE}]')
                    )
                    CREATE NONCLUSTERED INDEX IX_{ENRICHED_TARGET_TABLE}_Kontoplan
                    ON [ext].[{ENRICHED_TARGET_TABLE}](CleanCode_Kontoplan);
                    """
                )
                target_conn.commit()

                target_cursor.execute(
                    f"""
                    SELECT
                        COUNT(*) AS total,
                        COUNT(CleanCode_Kontoplan) AS has_cleancode_kontoplan,
                        COUNT(DISTINCT CleanCode_Kontoplan) AS unique_cleancode_kontoplan,
                        COUNT(CleanCode_Budget) AS has_cleancode_budget,
                        COUNT(DISTINCT CleanCode_Budget) AS unique_budget,
                        COUNT(CleanCode_BudgetEgenskaber) AS has_cleancode_BudgetEgenskaber,
                        COUNT(DISTINCT CleanCode_BudgetEgenskaber) AS unique_BudgetEgenskaber,
                        COUNT(Omkostningsstednummer) AS has_okosted,
                        COUNT(Budgetdato) AS has_budgetdato,
                        COUNT(Budgetbeloeb) AS has_budgetbeloeb
                    FROM [ext].[{ENRICHED_TARGET_TABLE}]
                    """
                )
                stats = target_cursor.fetchone()
                context.log.info(
                    f"Coverage: {stats[1]}/{stats[0]} with CleanCode_Kontoplan, "
                    f"unique: {stats[2]}. "
                    f"CleanCode_Budget: {stats[3]}/{stats[0]}, unique: {stats[4]}. "
                    f"CleanCode_BudgetEgenskaber: {stats[5]}/{stats[0]}, unique: {stats[6]}. "
                    f"OKOSTED: {stats[7]}/{stats[0]}. "
                    f"Budgetdato: {stats[8]}/{stats[0]}. "
                    f"Budgetbeloeb: {stats[9]}/{stats[0]}."
                )
            finally:
                target_cursor.close()

    return dg.MaterializeResult(
        metadata={
            "table": f"ext.{ENRICHED_TARGET_TABLE}",
            "row_count": row_count,
            "partition_key": "full_load",
            "partition_filter": partition_filter,
            "grain": "ref_LedgerbudgetId",
            "unique_kontoplan": stats[2],
            "unique_budget": stats[4],
            "unique_BudgetEgenskaber": stats[6],
            "has_okosted": stats[7],
            "has_budgetdato": stats[8],
            "has_budgetbeloeb": stats[9],
        }
    )


sydax_enriched_budget_assets = [
    ext_budgetpost_enriched_sydax_daily,
    ext_budgetpost_enriched_sydax_monthly,
    ext_budgetpost_enriched_sydax_full_load,
]

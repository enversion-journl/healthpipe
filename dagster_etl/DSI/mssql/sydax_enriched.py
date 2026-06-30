# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SydAX enriched extraction: ext.finanspostering_enriched

EXCEPTION to standard DSI 1:1 contract (documented in sql_etl_guide.md):
This asset performs multi-table JOINs directly against SydAX source tables and
writes the enriched result to ext.finanspostering_enriched on the target DW.
The exception exists because the enrichment requires cross-schema joins that
cannot be expressed via the standard TableConfig factory.

The entire enrichment query executes on the SydAX source database; the result
set is streamed into the target DW via MssqlExtractor.custom_stream_extract_and_insert
which uses chunked bulk-copy to avoid holding the full result set in memory.

The full-load variant uses parallel windowed extraction (8 workers) via
plan_keyset_windows + run_window_workers from DSI.connectors.mssql.parallel_extract.

Partition column: Oprettelsestidspunkt (on Finanspostering)
Grain: ref_LedgertransId (unique BIGINT from the source ledger)
Business key: bk_ref_LedgertransId
"""

from datetime import datetime, timedelta

import dagster as dg
from core.partitions import (
    daily_partitions_erp,
    is_first_partition,
    monthly_partitions_erp,
)
from core.resources import MSSQLResource, SydaxMSSQLResource
from DSI.connectors.mssql.bulk_copy import (
    build_bcp_truncation_limits,
    compute_streaming_chunk_size,
)
from DSI.connectors.mssql.extractor import MssqlExtractor, create_ext_table_ddl
from DSI.connectors.mssql.models import TableConfig
from DSI.connectors.mssql.parallel_extract import (
    _sql_literal,
    plan_keyset_windows,
    run_window_workers,
    source_budget,
)

POSTERING_SYDAX_KILDESYSTEM = "SydAX"

ENRICHED_TARGET_TABLE = "finanspostering_enriched"

_ENRICHED_PARALLEL_WORKERS = 8
_ENRICHED_ORDERING_KEY = "FinansposteringId"

_ENRICHED_TABLE_CONFIG = TableConfig(
    output_name="ext_finanspostering_enriched_sydax",
    source_schema="Finans",
    source_table="Finanspostering",
    target_table=ENRICHED_TARGET_TABLE,
    description="Enriched Finanspostering with hierarchy lookups from SydAX",
)


def _build_partition_filter(
    partition_key: str,
    is_scheduled_catchup: bool = False,
    first_partition: bool = False,
) -> str:
    """Build WHERE clause fragment for Oprettelsestidspunkt partitioning."""
    is_monthly = len(partition_key) == 7 and partition_key.count("-") == 1

    if is_monthly:
        from calendar import monthrange

        year, month = map(int, partition_key.split("-"))
        _, last_day = monthrange(year, month)
        start_date = f"{year:04d}-{month:02d}-01"
        end_date = f"{year:04d}-{month:02d}-{last_day:02d}"
        if first_partition:
            date_filter = f"CAST(post.[Oprettelsestidspunkt] AS DATE) <= '{end_date}'"
        else:
            date_filter = (
                f"CAST(post.[Oprettelsestidspunkt] AS DATE) >= '{start_date}' "
                f"AND CAST(post.[Oprettelsestidspunkt] AS DATE) <= '{end_date}'"
            )
    elif is_scheduled_catchup:
        today_date = datetime.strptime(partition_key, "%Y-%m-%d")
        yesterday_date = today_date - timedelta(days=1)
        start_ts = yesterday_date.strftime("%Y-%m-%d") + " 01:00:00"
        date_filter = f"post.[Oprettelsestidspunkt] >= '{start_ts}'"
    elif first_partition:
        date_filter = f"CAST(post.[Oprettelsestidspunkt] AS DATE) <= '{partition_key}'"
    else:
        date_filter = f"CAST(post.[Oprettelsestidspunkt] AS DATE) = '{partition_key}'"

    return date_filter


def _detect_scheduled_catchup(context: dg.AssetExecutionContext) -> bool:
    return (
        context.has_tag("erp_extraction_mode")
        and context.run.tags.get("erp_extraction_mode") == "scheduled_catchup"
    )


def _build_enrichment_sql(partition_filter: str) -> str:
    """Build the enrichment SELECT that runs directly on the SydAX source.

    All table references are schema-qualified source tables.  The result set is
    fetched into Python and inserted into the target DW by the caller.
    """
    return f"""
        ;WITH konto_dedup AS (
            SELECT *,
                   'KONTOPLAN_' + UPPER(LTRIM(RTRIM(REPLACE(
                       CAST([FinanskontoId] AS NVARCHAR(100)), ' ', '')
                   ))) AS Kontoplan_CleanCode,
                   ROW_NUMBER() OVER (
                       PARTITION BY [FinanskontoId] ORDER BY [FinanskontoId]
                   ) AS rn
            FROM [Finans].[Finanskonto]
            WHERE [FinanskontoId] IS NOT NULL
        ),
        omkost_link_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [FinanskontoId] ORDER BY [FinanskontoId]
                   ) AS rn
            FROM [Finans].[FinanskontoOmkostningssted]
            WHERE [FinanskontoId] IS NOT NULL
        ),
        omk_hier_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY CAST(CAST([max_OmkostningsstedId] AS FLOAT) AS BIGINT)
                       ORDER BY [max_OmkostningsstedId]
                   ) AS rn
            FROM [Finans].[OmkostningsstedHierarki]
            WHERE [max_OmkostningsstedId] IS NOT NULL
        ),
        hk_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [ref_RECID] ORDER BY [ref_RECID]
                   ) AS rn
            FROM [Finans].[Hovedkonto]
            WHERE [ref_RECID] IS NOT NULL
        ),
        grp_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [RECID] ORDER BY [RECID]
                   ) AS rn
            FROM [Finans].[Gruppering]
            WHERE [RECID] IS NOT NULL
        ),
        grp_aut_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [ref_RECID] ORDER BY [ref_RECID]
                   ) AS rn
            FROM [Finans].[GrupperingAutoriseret]
            WHERE [ref_RECID] IS NOT NULL
        ),
        art_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [RECID] ORDER BY [RECID]
                   ) AS rn
            FROM [Finans].[Art]
            WHERE [RECID] IS NOT NULL
        ),
        dranst_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [ref_RECID] ORDER BY [ref_RECID]
                   ) AS rn
            FROM [Finans].[Dranst]
            WHERE [ref_RECID] IS NOT NULL
        ),
        funk_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [F1], [F2], [F3] ORDER BY [F1], [F2], [F3]
                   ) AS rn
            FROM [Finans].[Funktion]
        )

        SELECT
            CAST(post.[ref_LedgertransId] AS BIGINT) AS ref_LedgertransId,
            CAST(post.[ref_LedgertransId] AS BIGINT) AS bk_ref_LedgertransId,

            CASE
                WHEN post.[Kildesystem] IS NOT NULL AND post.[ref_LedgertransId] IS NOT NULL
                THEN UPPER(LTRIM(RTRIM(REPLACE(
                         CAST(post.[Kildesystem] AS NVARCHAR(100)), ' ', '')
                     ))) + '_' +
                     CAST(post.[ref_LedgertransId] AS NVARCHAR(50))
                WHEN post.[ref_LedgertransId] IS NOT NULL
                THEN CAST(post.[ref_LedgertransId] AS NVARCHAR(50))
                ELSE NULL
            END AS CleanCode_Postering,

            CAST(trans.[ref_VendTransId] AS BIGINT) AS ref_VendTransId,

            CASE
                WHEN kred.[JuridiskEnhed] IS NOT NULL
                     AND LTRIM(RTRIM(kred.[JuridiskEnhed])) <> ''
                THEN 'CVR_' + CASE WHEN LTRIM(RTRIM(kred.[JuridiskEnhed])) LIKE 'DK[0-9]%'
                                   THEN SUBSTRING(LTRIM(RTRIM(kred.[JuridiskEnhed])), 3, LEN(LTRIM(RTRIM(kred.[JuridiskEnhed]))))
                                   ELSE LTRIM(RTRIM(kred.[JuridiskEnhed])) END
                ELSE NULL
            END AS CleanCode_Kreditor,
            konto.Kontoplan_CleanCode AS CleanCode_Kontoplan,
            CASE
                WHEN trans.[Fakturanummer] IS NOT NULL
                     AND LTRIM(RTRIM(trans.[Fakturanummer])) <> ''
                THEN 'FAKTURA_' + UPPER(LTRIM(RTRIM(REPLACE(
                         trans.[Fakturanummer], ' ', '')
                     )))
                ELSE NULL
            END AS CleanCode_Faktura,

            CAST(kred.[KreditorId] AS BIGINT) AS KreditorId,
            kred.[Kreditornummer],
            kred.[Kreditornavn],
            kred.[JuridiskEnhed],

            CAST(post.[FinanskontoId] AS BIGINT) AS FinanskontoId,
            post.[FinanskontoId] AS bk_FinanskontoId,
            konto.[Kontonummer],
            konto.[hk_RECID],
            konto.[grp_RECID],
            konto.[art_RECID],
            konto.[d1_RECID],

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
            CAST(art.[Artstekst] AS NVARCHAR(255)) AS tekst_A1,
            CAST(art.[Artstekst] AS NVARCHAR(255)) AS tekst_A2,
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
            CASE
                WHEN omk_hier.[nummer20] IS NOT NULL THEN 20
                WHEN omk_hier.[nummer19] IS NOT NULL THEN 19
                WHEN omk_hier.[nummer18] IS NOT NULL THEN 18
                WHEN omk_hier.[nummer17] IS NOT NULL THEN 17
                WHEN omk_hier.[nummer16] IS NOT NULL THEN 16
                WHEN omk_hier.[nummer15] IS NOT NULL THEN 15
                WHEN omk_hier.[nummer14] IS NOT NULL THEN 14
                WHEN omk_hier.[nummer13] IS NOT NULL THEN 13
                WHEN omk_hier.[nummer12] IS NOT NULL THEN 12
                WHEN omk_hier.[nummer11] IS NOT NULL THEN 11
                WHEN omk_hier.[nummer10] IS NOT NULL THEN 10
                WHEN omk_hier.[nummer9] IS NOT NULL THEN 9
                WHEN omk_hier.[nummer8] IS NOT NULL THEN 8
                WHEN omk_hier.[nummer7] IS NOT NULL THEN 7
                WHEN omk_hier.[nummer6] IS NOT NULL THEN 6
                WHEN omk_hier.[nummer5] IS NOT NULL THEN 5
                WHEN omk_hier.[nummer4] IS NOT NULL THEN 4
                WHEN omk_hier.[nummer3] IS NOT NULL THEN 3
                WHEN omk_hier.[nummer2] IS NOT NULL THEN 2
                WHEN omk_hier.[nummer1] IS NOT NULL THEN 1
                ELSE NULL
            END AS Omkostningssted_niveau,

            TRY_CONVERT(DATE, trans.[GodkendtDato]) AS GodkendtDato,
            TRY_CONVERT(DATE, trans.[LukketDato]) AS LukketDato,
            TRY_CONVERT(DATETIME2, trans.[Oprettelsestidspunkt]) AS Trans_Oprettelsestidspunkt,
            TRY_CONVERT(DATE, trans.[Forfaldsdato]) AS Forfaldsdato,
            TRY_CONVERT(DATE, trans.[Fakturadato]) AS Trans_Fakturadato,
            TRY_CONVERT(DATE, trans.[SidstUdlignetDato]) AS SidstUdlignetDato,
            TRY_CONVERT(DATE, trans.[Transaktionsdato]) AS Transaktionsdato,
            TRY_CONVERT(DATE, post.[Bogfoeringsdato]) AS Bogfoeringsdato,
            TRY_CONVERT(DATETIME2, post.[Oprettelsestidspunkt]) AS Post_Oprettelsestidspunkt,

            TRY_CONVERT(DECIMAL(30,12), trans.[TransaktionsbeloebValuta]) AS TransaktionsbeloebValuta,
            TRY_CONVERT(DECIMAL(30,12), trans.[Transaktionsbeloeb]) AS Transaktionsbeloeb,
            TRY_CONVERT(DECIMAL(30,12), trans.[UdlignetBeloebValuta]) AS UdlignetBeloebValuta,
            TRY_CONVERT(DECIMAL(30,12), trans.[UdlignetBeloeb]) AS UdlignetBeloeb,
            TRY_CONVERT(DECIMAL(30,12), post.[Posteringsbeloeb]) AS Posteringsbeloeb,

            trans.[Fakturanummer] AS FakturaNr,
            trans.[SidstUdlignetBilagsnummer],
            trans.[Bilagsnummer] AS Bilagsnummer_Trans,
            post.[Bilagsnummer] AS Bilagsnummer_Postering,
            TRY_CONVERT(BIGINT, trans.[DokumentId]) AS DokumentId,
            trans.[Valutakode] AS Valutakode_Trans,
            post.[Valutakode] AS Valutakode_Postering,
            trans.[Transaktionstekst],
            post.[Posteringstekst]

        FROM [Finans].[Finanspostering] post

        LEFT JOIN [Finans].[FinansposteringKreditorTransRelation] bridge
            ON bridge.[FinansposteringId] = post.[FinansposteringId]

        LEFT JOIN [Kreditor].[Kreditortransaktion] trans
            ON trans.[KreditortransaktionId] = bridge.[KreditortransaktionId]

        LEFT JOIN [Kreditor].[Kreditor] kred
            ON kred.[KreditorId] = trans.[KreditorId]

        LEFT JOIN konto_dedup konto
            ON konto.[FinanskontoId] = post.[FinanskontoId]
            AND konto.rn = 1

        LEFT JOIN omkost_link_dedup omkost_link
            ON omkost_link.[FinanskontoId] = post.[FinanskontoId]
            AND omkost_link.rn = 1

        LEFT JOIN omk_hier_dedup omk_hier
            ON CAST(CAST(omk_hier.[max_OmkostningsstedId] AS FLOAT) AS BIGINT) =
               CAST(omkost_link.[Omkostningsstedid] AS BIGINT)
            AND omk_hier.rn = 1

        LEFT JOIN hk_dedup hk
            ON hk.[ref_RECID] = konto.[hk_RECID]
            AND hk.rn = 1

        LEFT JOIN grp_dedup grp
            ON grp.[RECID] = konto.[grp_RECID]
            AND grp.rn = 1

        LEFT JOIN grp_aut_dedup grp_aut
            ON grp_aut.[ref_RECID] = konto.[grp_RECID]
            AND grp_aut.rn = 1

        LEFT JOIN art_dedup art
            ON art.[RECID] = konto.[art_RECID]
            AND art.rn = 1

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

        LEFT JOIN dranst_dedup dr
            ON dr.[ref_RECID] = TRY_CAST(konto.[d1_RECID] AS BIGINT)
            AND dr.rn = 1

        WHERE {partition_filter}
          AND post.[Kildesystem] = '{POSTERING_SYDAX_KILDESYSTEM}';
    """


def _build_windowed_enrichment_sql(keyset_range) -> str:
    """Build enrichment SQL with a keyset window predicate on post.[FinansposteringId].

    Used by the parallel full-load path to split the source table into N ranges.
    The Kildesystem filter is added by _build_enrichment_sql itself.
    """
    preds = []
    if keyset_range.lo is not None:
        preds.append(f"post.[{_ENRICHED_ORDERING_KEY}] >= {_sql_literal(keyset_range.lo)}")
    if keyset_range.hi is not None:
        op = "<=" if keyset_range.is_last else "<"
        preds.append(f"post.[{_ENRICHED_ORDERING_KEY}] {op} {_sql_literal(keyset_range.hi)}")
    if keyset_range.is_first and keyset_range.lo is not None:
        preds[0] = (
            f"(post.[{_ENRICHED_ORDERING_KEY}] >= {_sql_literal(keyset_range.lo)}"
            f" OR post.[{_ENRICHED_ORDERING_KEY}] IS NULL)"
        )
    partition_filter = " AND ".join(preds) if preds else "1=1"
    return _build_enrichment_sql(partition_filter)


def _make_enriched_asset(partition_type: str):
    """Build a daily or monthly enriched extraction asset."""
    suffix = partition_type
    partitions_def = daily_partitions_erp if partition_type == "daily" else monthly_partitions_erp

    @dg.asset(
        key=dg.AssetKey([f"ext_finanspostering_enriched_sydax_{suffix}"]),
        group_name=f"erp_extract_{suffix}",
        compute_kind="mssql",
        partitions_def=partitions_def,
        tags={
            "domain": "erp",
            "layer": "extract",
            "source": "sydax",
            "entity": "finanspostering_enriched",
        },
        retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
        description=(
            f"Enriched extraction from SydAX: joins Finanspostering with "
            f"Kreditortransaktion (via bridge), Kreditor, and finance hierarchy "
            f"tables — query runs directly on SydAX source, result inserted "
            f"into ext.{ENRICHED_TARGET_TABLE}. "
            f"Partitioned by Oprettelsestidspunkt ({suffix}). "
            f"Grain: ref_LedgertransId. Business key: bk_ref_LedgertransId."
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
                f"Running enriched SydAX extraction for {suffix} " f"partition: {partition_key}"
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
                        ON [ext].[{ENRICHED_TARGET_TABLE}](ref_LedgertransId);
                        """
                    )
                    target_cursor.execute(
                        f"""
                        IF NOT EXISTS (
                            SELECT 1 FROM sys.indexes
                            WHERE name = 'IX_{ENRICHED_TARGET_TABLE}_CleanCode'
                              AND object_id = OBJECT_ID('[ext].[{ENRICHED_TARGET_TABLE}]')
                        )
                        CREATE NONCLUSTERED INDEX IX_{ENRICHED_TARGET_TABLE}_CleanCode
                        ON [ext].[{ENRICHED_TARGET_TABLE}](CleanCode_Postering);
                        """
                    )
                    target_conn.commit()

                    target_cursor.execute(
                        f"""
                        SELECT
                            COUNT(*) AS total,
                            COUNT(CleanCode_Kreditor) AS has_cleancode_kreditor,
                            COUNT(CleanCode_Kontoplan) AS has_cleancode_kontoplan,
                            COUNT(DISTINCT CleanCode_Kreditor) AS unique_cleancode_kreditor,
                            COUNT(DISTINCT CleanCode_Kontoplan) AS unique_cleancode_kontoplan,
                            COUNT(ref_VendTransId) AS has_ref_vendtransid,
                            COUNT(Kreditornummer) AS has_kreditornummer,
                            COUNT(Kreditornavn) AS has_kreditornavn,
                            COUNT(FakturaNr) AS has_fakturanr,
                            COUNT(Bogfoeringsdato) AS has_bogfoeringsdato,
                            COUNT(Post_Oprettelsestidspunkt) AS has_post_oprettelsestidspunkt,
                            COUNT(GodkendtDato) AS has_godkendtdato,
                            COUNT(Transaktionsdato) AS has_transaktionsdato,
                            COUNT(Posteringsbeloeb) AS has_posteringsbeloeb,
                            COUNT(Transaktionsbeloeb) AS has_transaktionsbeloeb
                        FROM [ext].[{ENRICHED_TARGET_TABLE}]
                        """
                    )
                    stats = target_cursor.fetchone()
                    context.log.info(
                        f"Coverage: {stats[1]}/{stats[0]} with CleanCode_Kreditor, "
                        f"{stats[2]}/{stats[0]} with CleanCode_Kontoplan. "
                        f"Unique CleanCode_Kreditor: {stats[3]}, "
                        f"unique CleanCode_Kontoplan: {stats[4]}"
                    )
                    context.log.info(
                        f"Bridge/trans/kred coverage: "
                        f"ref_VendTransId={stats[5]}/{stats[0]}, "
                        f"Kreditornummer={stats[6]}/{stats[0]}, "
                        f"Kreditornavn={stats[7]}/{stats[0]}, "
                        f"FakturaNr={stats[8]}/{stats[0]}"
                    )
                    context.log.info(
                        f"Date coverage: "
                        f"Bogfoeringsdato={stats[9]}/{stats[0]}, "
                        f"Post_Oprettelsestidspunkt={stats[10]}/{stats[0]}, "
                        f"GodkendtDato={stats[11]}/{stats[0]}, "
                        f"Transaktionsdato={stats[12]}/{stats[0]}"
                    )
                    context.log.info(
                        f"Measure coverage: "
                        f"Posteringsbeloeb={stats[13]}/{stats[0]}, "
                        f"Transaktionsbeloeb={stats[14]}/{stats[0]}"
                    )
                finally:
                    target_cursor.close()

        return dg.MaterializeResult(
            metadata={
                "table": f"ext.{ENRICHED_TARGET_TABLE}",
                "row_count": row_count,
                "partition_key": partition_key or "N/A",
                "partition_filter": partition_filter,
                "grain": "ref_LedgertransId",
                "business_key": "bk_ref_LedgertransId",
                "unique_kreditor": stats[3],
                "unique_kontoplan": stats[4],
                "has_ref_vendtransid": stats[5],
                "has_kreditornummer": stats[6],
                "has_bogfoeringsdato": stats[9],
                "has_post_oprettelsestidspunkt": stats[10],
                "has_posteringsbeloeb": stats[13],
                "has_transaktionsbeloeb": stats[14],
            }
        )

    _extract_enriched.__name__ = f"ext_finanspostering_enriched_sydax_{suffix}"
    _extract_enriched.__qualname__ = f"ext_finanspostering_enriched_sydax_{suffix}"
    return _extract_enriched


ext_finanspostering_enriched_sydax_daily = _make_enriched_asset("daily")
ext_finanspostering_enriched_sydax_monthly = _make_enriched_asset("monthly")


@dg.asset(
    key=dg.AssetKey(["ext_finanspostering_enriched_sydax_full_load"]),
    group_name="erp_extract_full_load",
    compute_kind="mssql",
    partitions_def=None,
    tags={
        "domain": "erp",
        "layer": "extract",
        "source": "sydax",
        "entity": "finanspostering_enriched",
        "partition": "full_load",
    },
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
    description=(
        "Full-load parallel enriched extraction from SydAX (8 workers): joins "
        "Finanspostering with Kreditortransaktion (via bridge), Kreditor, and finance "
        f"hierarchy tables — query runs directly on SydAX source, result inserted "
        f"into ext.{ENRICHED_TARGET_TABLE}. "
        "No partition filter (full reload). "
        "Grain: ref_LedgertransId. Business key: bk_ref_LedgertransId."
    ),
)
def ext_finanspostering_enriched_sydax_full_load(
    context: dg.AssetExecutionContext,
    sydax_mssql: SydaxMSSQLResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    workers = _ENRICHED_PARALLEL_WORKERS
    source_system = "SydAX"
    context.log.info(
        f"Running parallel full-load enriched SydAX postering extraction "
        f"({workers} workers, ordering key: {_ENRICHED_ORDERING_KEY})"
    )

    _EXTRA_COLUMNS_DEF = [
        ("SourceSystem", "NVARCHAR(50)", False),
        ("DW_LoadTimestamp", "DATETIME2", False),
        ("DW_PartitionDate", "DATE", True),
    ]

    with sydax_mssql.get_connection() as source_conn:
        source_cursor = source_conn.cursor()
        try:
            probe_sql = _build_enrichment_sql("1=0")
            source_cursor.execute(probe_sql)
            columns_info = [
                MssqlExtractor._map_cursor_desc_to_columns_info(desc)
                for desc in (source_cursor.description or [])
            ]
            column_names = [desc[0] for desc in source_cursor.description or []]
        finally:
            source_cursor.close()

        keyset_ranges = plan_keyset_windows(
            source_conn,
            schema="Finans",
            table="Finanspostering",
            ordering_key=_ENRICHED_ORDERING_KEY,
            workers=workers,
            row_limit=None,
            base_where=f"[Kildesystem] = '{POSTERING_SYDAX_KILDESYSTEM}'",
        )
        context.log.info(f"Planned {len(keyset_ranges)} keyset windows for parallel extract")

    with mssql_target_staging.get_connection() as target_conn:
        target_cursor = target_conn.cursor()
        try:
            target_cursor.execute(f"DROP TABLE IF EXISTS [ext].[{ENRICHED_TARGET_TABLE}]")
            target_conn.commit()
            ddl = create_ext_table_ddl(
                ENRICHED_TARGET_TABLE,
                columns_info,
                _EXTRA_COLUMNS_DEF,
                [None] * len(columns_info),
                avoid_max_types=True,
            )
            target_cursor.execute(ddl)
            target_conn.commit()
            context.log.info(f"Created scratch heap ext.{ENRICHED_TARGET_TABLE}")
        finally:
            target_cursor.close()

    chunk_size = compute_streaming_chunk_size(columns_info)[0]
    bcp_limits = build_bcp_truncation_limits(columns_info)
    load_timestamp = datetime.utcnow().replace(microsecond=0)

    select_sqls = [_build_windowed_enrichment_sql(rng) for rng in keyset_ranges]

    try:
        results, peak_rss = run_window_workers(
            source_resource=sydax_mssql,
            target_resource=mssql_target_staging,
            select_sqls=select_sqls,
            workers=workers,
            target_table=ENRICHED_TARGET_TABLE,
            column_names=column_names,
            bcp_limits=bcp_limits,
            chunk_size=chunk_size,
            table_lock=True,
            load_timestamp=load_timestamp,
            source_system=source_system,
            source_key=source_system.lower(),
            source_capacity=source_budget(source_system),
            log=context.log,
        )
    except Exception:
        with mssql_target_staging.get_connection() as cleanup_conn:
            cleanup_cursor = cleanup_conn.cursor()
            try:
                cleanup_cursor.execute(f"DROP TABLE IF EXISTS [ext].[{ENRICHED_TARGET_TABLE}]")
                cleanup_conn.commit()
            finally:
                cleanup_cursor.close()
        raise

    row_count = sum(r.rows for r in results)
    context.log.info(
        f"[parallel] ext.{ENRICHED_TARGET_TABLE} w={workers}: "
        f"{row_count:,} rows, peak {peak_rss:.0f} MB"
    )

    with mssql_target_staging.get_connection() as target_conn:
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
                ON [ext].[{ENRICHED_TARGET_TABLE}](ref_LedgertransId);
                """
            )
            target_cursor.execute(
                f"""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.indexes
                    WHERE name = 'IX_{ENRICHED_TARGET_TABLE}_CleanCode'
                      AND object_id = OBJECT_ID('[ext].[{ENRICHED_TARGET_TABLE}]')
                )
                CREATE NONCLUSTERED INDEX IX_{ENRICHED_TARGET_TABLE}_CleanCode
                ON [ext].[{ENRICHED_TARGET_TABLE}](CleanCode_Postering);
                """
            )
            target_conn.commit()

            target_cursor.execute(
                f"""
                SELECT
                    COUNT(*) AS total,
                    COUNT(CleanCode_Kreditor) AS has_cleancode_kreditor,
                    COUNT(CleanCode_Kontoplan) AS has_cleancode_kontoplan,
                    COUNT(DISTINCT CleanCode_Kreditor) AS unique_cleancode_kreditor,
                    COUNT(DISTINCT CleanCode_Kontoplan) AS unique_cleancode_kontoplan,
                    COUNT(ref_VendTransId) AS has_ref_vendtransid,
                    COUNT(Kreditornummer) AS has_kreditornummer,
                    COUNT(Kreditornavn) AS has_kreditornavn,
                    COUNT(FakturaNr) AS has_fakturanr,
                    COUNT(Bogfoeringsdato) AS has_bogfoeringsdato,
                    COUNT(Post_Oprettelsestidspunkt) AS has_post_oprettelsestidspunkt,
                    COUNT(GodkendtDato) AS has_godkendtdato,
                    COUNT(Transaktionsdato) AS has_transaktionsdato,
                    COUNT(Posteringsbeloeb) AS has_posteringsbeloeb,
                    COUNT(Transaktionsbeloeb) AS has_transaktionsbeloeb
                FROM [ext].[{ENRICHED_TARGET_TABLE}]
                """
            )
            stats = target_cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with CleanCode_Kreditor, "
                f"{stats[2]}/{stats[0]} with CleanCode_Kontoplan. "
                f"Unique CleanCode_Kreditor: {stats[3]}, "
                f"unique CleanCode_Kontoplan: {stats[4]}"
            )
        finally:
            target_cursor.close()

    return dg.MaterializeResult(
        metadata={
            "table": f"ext.{ENRICHED_TARGET_TABLE}",
            "row_count": row_count,
            "partition_key": "full_load",
            "workers": workers,
            "windows": len(keyset_ranges),
            "peak_rss_mb": round(peak_rss, 1),
            "grain": "ref_LedgertransId",
            "business_key": "bk_ref_LedgertransId",
            "unique_kreditor": stats[3],
            "unique_kontoplan": stats[4],
            "has_ref_vendtransid": stats[5],
            "has_kreditornummer": stats[6],
            "has_bogfoeringsdato": stats[9],
            "has_post_oprettelsestidspunkt": stats[10],
            "has_posteringsbeloeb": stats[13],
            "has_transaktionsbeloeb": stats[14],
        }
    )


sydax_enriched_assets = [
    ext_finanspostering_enriched_sydax_daily,
    ext_finanspostering_enriched_sydax_monthly,
    ext_finanspostering_enriched_sydax_full_load,
]

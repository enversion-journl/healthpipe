# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SydAX enriched extraction: ext.finanskladdelinje_enriched

Multi-table JOIN that runs directly on the SydAX source database.  Joins
Finanskladdelinje with Finanskladde (header), Finanstransaktionstype, and the
full Finanskonto → hierarchy chain.  All joins MUST run on source because the
individual tables are loaded into ext independently (no FK integrity on target).

This asset outputs RAW joined columns only — no CleanCodes, no type casts, no
COALESCE for hierarchy texts.  All transform logic lives in the DSA pre_stg
layer (pre_stg_finanskladdelinje_sydax.py).

The result set is streamed into the target DW via
MssqlExtractor.custom_stream_extract_and_insert.

Partition columns: CreatedOn, Modified (on Finanskladdelinje — OR logic)
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
from DSI.connectors.mssql.extractor import MssqlExtractor
from DSI.connectors.mssql.models import TableConfig

FINANSKLADDE_SYDAX_KILDESYSTEM = "SydAX"

ENRICHED_TARGET_TABLE = "finanskladdelinje_enriched"

_ENRICHED_TABLE_CONFIG = TableConfig(
    output_name="ext_finanskladdelinje_enriched_sydax",
    source_schema="Finans",
    source_table="Finanskladdelinje",
    target_table=ENRICHED_TARGET_TABLE,
    description=(
        "Enriched Finanskladdelinje with Finanskladde header, Finanstransaktionstype, "
        "and finance hierarchy lookups from SydAX (raw joined columns, no transforms)"
    ),
)


def _build_single_col_filter(
    col: str,
    partition_key: str,
    is_monthly: bool,
    start_date: str,
    end_date: str,
    is_scheduled_catchup: bool,
    first_partition: bool,
) -> str:
    """Build a date filter for one column on the fkl alias."""
    if is_monthly:
        if first_partition:
            return f"CAST(fkl.[{col}] AS DATE) <= '{end_date}'"
        return (
            f"CAST(fkl.[{col}] AS DATE) >= '{start_date}' "
            f"AND CAST(fkl.[{col}] AS DATE) <= '{end_date}'"
        )
    if is_scheduled_catchup:
        today_date = datetime.strptime(partition_key, "%Y-%m-%d")
        yesterday_date = today_date - timedelta(days=1)
        start_ts = yesterday_date.strftime("%Y-%m-%d") + " 01:00:00"
        return f"fkl.[{col}] >= '{start_ts}'"
    if first_partition:
        return f"CAST(fkl.[{col}] AS DATE) <= '{partition_key}'"
    return f"CAST(fkl.[{col}] AS DATE) = '{partition_key}'"


_PARTITION_COLUMNS = ("CreatedOn", "Modified")


def _build_partition_filter(
    partition_key: str,
    is_scheduled_catchup: bool = False,
    first_partition: bool = False,
) -> str:
    """Build WHERE clause fragment for CreatedOn OR Modified partitioning.

    Mirrors the standard DSI partition_columns OR-logic from sql_builders.py:
    a row is selected when EITHER column falls within the partition window.
    """
    is_monthly = len(partition_key) == 7 and partition_key.count("-") == 1
    start_date = end_date = ""
    if is_monthly:
        from calendar import monthrange

        year, month = map(int, partition_key.split("-"))
        _, last_day = monthrange(year, month)
        start_date = f"{year:04d}-{month:02d}-01"
        end_date = f"{year:04d}-{month:02d}-{last_day:02d}"

    filters = []
    for col in _PARTITION_COLUMNS:
        f = _build_single_col_filter(
            col,
            partition_key,
            is_monthly,
            start_date,
            end_date,
            is_scheduled_catchup,
            first_partition,
        )
        filters.append(f"({f})")

    return " OR ".join(filters)


def _detect_scheduled_catchup(context: dg.AssetExecutionContext) -> bool:
    return (
        context.has_tag("erp_extraction_mode")
        and context.run.tags.get("erp_extraction_mode") == "scheduled_catchup"
    )


def _build_enrichment_sql(partition_filter: str) -> str:
    """Build the enrichment SELECT that runs directly on the SydAX source.

    Joins Finanskladdelinje with:
    - Finanskladde (header: resolve unstable FinanskladdeId -> ref_LedgerjournaltableId)
    - Finanstransaktionstype (transaction type lookup)
    - Finanskonto + hierarchy chain (Hovedkonto, Gruppering, GrupperingAutoriseret,
      Art, Funktion, Dranst, FinanskontoOmkostningssted, OmkostningsstedHierarki)

    Outputs raw joined values only.  CleanCodes, type casts, and COALESCE
    transforms are applied downstream in pre_stg_finanskladdelinje_sydax.
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
        funk_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [F1], [F2], [F3] ORDER BY [Modified] DESC
                   ) AS rn
            FROM [Finans].[Funktion]
        )

        SELECT
            -- Line grain
            fkl.[ref_LedgertransId],
            fkl.[ref_RecId],

            -- Header columns (denormalized from Finanskladde)
            fk.[ref_LedgerjournaltableId],
            fk.[Kladdenummer],
            fk.[Kladdetekst],
            fk.[Kladdenavn],
            fk.[ErLukket],
            fk.[DataAreaId],
            fk.[Maskinnummer] AS Maskinnummer_Kladde,

            -- Transaction type lookup
            tt.[Transaktionstypenummer],
            tt.[Transaktionstypetekst],

            -- Line descriptive columns
            fkl.[Linjenummer],
            fkl.[dato],
            fkl.[Beskrivelse],
            fkl.[Ekspeditionsnummer],
            fkl.[Maskinnummer],
            fkl.[BilagsnummerID],
            fkl.[DokumentId],
            fkl.[Debet],
            fkl.[Kredit],

            -- Account chain (raw FKs and values from Finanskonto)
            fkl.[FinanskontoId],
            konto.[Kontonummer],
            konto.[hk_RECID],
            konto.[grp_RECID],
            konto.[art_RECID],
            konto.[d1_RECID],

            -- Hierarchy raw values from Hovedkonto
            hk.[F1],
            hk.[F2],
            hk.[F3],
            hk.[D1],
            hk.[G1],

            -- Omkostningssted hierarchy raw values
            omk_hier.[nummer1],
            omk_hier.[nummer2],
            omk_hier.[nummer3],
            omk_hier.[nummer4],
            omk_hier.[nummer5],
            omk_hier.[nummer6],
            omk_hier.[nummer7],
            omk_hier.[nummer8],
            omk_hier.[nummer9],
            omk_hier.[nummer10],
            omk_hier.[nummer11],
            omk_hier.[nummer12],
            omk_hier.[nummer13],
            omk_hier.[nummer14],
            omk_hier.[nummer15],
            omk_hier.[nummer16],
            omk_hier.[nummer17],
            omk_hier.[nummer18],
            omk_hier.[nummer19],
            omk_hier.[nummer20],
            omk_hier.[tekst1],
            omk_hier.[tekst2],
            omk_hier.[tekst3],
            omk_hier.[tekst4],
            omk_hier.[tekst5],
            omk_hier.[tekst6],
            omk_hier.[tekst7],
            omk_hier.[tekst8],
            omk_hier.[tekst9],
            omk_hier.[tekst10],
            omk_hier.[tekst11],
            omk_hier.[tekst12],
            omk_hier.[tekst13],
            omk_hier.[tekst14],
            omk_hier.[tekst15],
            omk_hier.[tekst16],
            omk_hier.[tekst17],
            omk_hier.[tekst18],
            omk_hier.[tekst19],
            omk_hier.[tekst20],

            -- Gruppering raw values
            grp.[Grupperingsnummer],
            grp.[Grupperingstekst],

            -- GrupperingAutoriseret raw values
            grp_aut.[G1Tekst],

            -- Art raw values
            art.[A1],
            art.[A2],
            art.[Artstekst],

            -- Funktion raw tekst values
            funk_f1.[FunktionTekst] AS FunktionTekst_F1,
            funk_f2.[FunktionTekst] AS FunktionTekst_F2,
            funk_f3.[FunktionTekst] AS FunktionTekst_F3,

            -- Dranst raw value
            dr.[D1Tekst],

            -- Omkostningssted link
            omkost_link.[Omkostningsstedid],

            -- Dates from the line/header
            fkl.[CreatedOn] AS Linje_CreatedOn,
            fk.[CreatedOn] AS Kladde_CreatedOn,
            fk.[Modified] AS Kladde_Modified

        FROM [Finans].[Finanskladdelinje] fkl

        LEFT JOIN [Finans].[Finanskladde] fk
            ON fk.[FinanskladdeId] = fkl.[FinanskladdeId]

        LEFT JOIN [Finans].[Finanstransaktionstype] tt
            ON tt.[FinanstransaktionstypeId] = fkl.[FinanstransaktionstypeId]

        LEFT JOIN [Finans].[Finanskonto] konto
            ON konto.[FinanskontoId] = fkl.[FinanskontoId]
            AND konto.[FinanskontoId] IS NOT NULL

        LEFT JOIN [Finans].[FinanskontoOmkostningssted] omkost_link
            ON omkost_link.[FinanskontoId] = fkl.[FinanskontoId]

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

        WHERE ({partition_filter})
          AND fkl.[Kildesystem] = '{FINANSKLADDE_SYDAX_KILDESYSTEM}';
    """


def _make_enriched_asset(
    partition_type: str,
    *,
    bcp_max_chunk_size: int | None = None,
):
    """Build a daily or monthly enriched extraction asset.

    ``bcp_max_chunk_size`` is forwarded to ``MssqlExtractor`` to cap the
    bulk-copy chunk size when the default heuristic would produce chunks
    large enough to wedge mssql-python's ``cursor.bulkcopy()``.
    """
    suffix = partition_type
    partitions_def = daily_partitions_erp if partition_type == "daily" else monthly_partitions_erp

    @dg.asset(
        key=dg.AssetKey([f"ext_finanskladdelinje_enriched_sydax_{suffix}"]),
        group_name=f"erp_extract_{suffix}",
        compute_kind="mssql",
        partitions_def=partitions_def,
        deps=[
            # Partitioned master-data: daily/monthly variants exist
            dg.AssetKey([f"ext_finanskonto_sydax_{suffix}"]),
            dg.AssetKey([f"ext_finanskontoomkostningssted_sydax_{suffix}"]),
            dg.AssetKey([f"ext_omkostningsstedhierarki_sydax_{suffix}"]),
            # Unpartitioned master-data: single asset key, no suffix
            dg.AssetKey(["ext_hovedkonto_sydax"]),
            dg.AssetKey(["ext_gruppering_sydax"]),
            dg.AssetKey(["ext_grupperingautoriseret_sydax"]),
            dg.AssetKey(["ext_art_sydax"]),
            dg.AssetKey(["ext_funktion_sydax"]),
            dg.AssetKey(["ext_dranst_sydax"]),
        ],
        tags={
            "domain": "erp",
            "layer": "extract",
            "source": "sydax",
            "entity": "finanskladdelinje_enriched",
        },
        retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
        description=(
            f"Enriched extraction from SydAX: joins Finanskladdelinje with "
            f"Finanskladde (header), Finanstransaktionstype, and finance hierarchy "
            f"tables. Query runs on SydAX source, raw joined columns inserted "
            f"into ext.{ENRICHED_TARGET_TABLE}. Transforms (CleanCodes, casts) "
            f"applied in pre_stg. Partitioned by CreatedOn OR Modified "
            f"({suffix}). Grain: ref_LedgertransId. "
            f"Business key: bk_ref_LedgertransId."
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
                f"Running enriched Finanskladdelinje extraction for {suffix} "
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
                        bcp_max_chunk_size=bcp_max_chunk_size,
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
                            WHERE name = 'IX_{ENRICHED_TARGET_TABLE}_FinanskontoId'
                              AND object_id = OBJECT_ID('[ext].[{ENRICHED_TARGET_TABLE}]')
                        )
                        CREATE NONCLUSTERED INDEX IX_{ENRICHED_TARGET_TABLE}_FinanskontoId
                        ON [ext].[{ENRICHED_TARGET_TABLE}](FinanskontoId);
                        """
                    )
                    target_conn.commit()

                    target_cursor.execute(
                        f"""
                        SELECT
                            COUNT(*) AS total,
                            COUNT(ref_LedgerjournaltableId) AS has_header_ref,
                            COUNT(Kontonummer) AS has_kontonummer,
                            COUNT(Omkostningsstedid) AS has_omkostningssted,
                            COUNT(Transaktionstypenummer) AS has_transtype
                        FROM [ext].[{ENRICHED_TARGET_TABLE}]
                        """
                    )
                    stats = target_cursor.fetchone()
                    context.log.info(
                        f"Coverage: "
                        f"header_ref={stats[1]}/{stats[0]}, "
                        f"Kontonummer={stats[2]}/{stats[0]}, "
                        f"Omkostningssted={stats[3]}/{stats[0]}, "
                        f"Transaktionstype={stats[4]}/{stats[0]}"
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
                "has_header_ref": stats[1],
                "has_kontonummer": stats[2],
                "has_omkostningssted": stats[3],
                "has_transtype": stats[4],
            }
        )

    _extract_enriched.__name__ = f"ext_finanskladdelinje_enriched_sydax_{suffix}"
    _extract_enriched.__qualname__ = f"ext_finanskladdelinje_enriched_sydax_{suffix}"
    return _extract_enriched


ext_finanskladdelinje_enriched_sydax_daily = _make_enriched_asset("daily")
# Monthly volume crossed the threshold where mssql-python's
# cursor.bulkcopy() wedges on ~20k-row chunks (chunk 3 of
# finanskladdelinje_enriched 2023-03 monthly hung the full
# bulkcopy(timeout=600) deadline three times in a row, client-side, with
# no server-side activity). Capping the bulk-copy chunk size at 5,000
# rows side-steps the wedge with negligible overhead. Daily extractions
# never reach the threshold and keep the default.
ext_finanskladdelinje_enriched_sydax_monthly = _make_enriched_asset(
    "monthly",
    bcp_max_chunk_size=5000,
)


@dg.asset(
    key=dg.AssetKey(["ext_finanskladdelinje_enriched_sydax_full_load"]),
    group_name="erp_extract_full_load",
    compute_kind="mssql",
    partitions_def=None,
    deps=[
        dg.AssetKey(["ext_hovedkonto_sydax"]),
        dg.AssetKey(["ext_gruppering_sydax"]),
        dg.AssetKey(["ext_grupperingautoriseret_sydax"]),
        dg.AssetKey(["ext_art_sydax"]),
        dg.AssetKey(["ext_funktion_sydax"]),
        dg.AssetKey(["ext_dranst_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "extract",
        "source": "sydax",
        "entity": "finanskladdelinje_enriched",
        "partition": "full_load",
    },
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
    description=(
        "Full-load enriched extraction from SydAX: joins Finanskladdelinje with "
        "Finanskladde (header), Finanstransaktionstype, and finance hierarchy "
        f"tables. Query runs on SydAX source, raw joined columns inserted "
        f"into ext.{ENRICHED_TARGET_TABLE}. Transforms (CleanCodes, casts) "
        "applied in pre_stg. No partition filter (full reload). "
        "Grain: ref_LedgertransId. Business key: bk_ref_LedgertransId."
    ),
)
def ext_finanskladdelinje_enriched_sydax_full_load(
    context: dg.AssetExecutionContext,
    sydax_mssql: SydaxMSSQLResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    context.log.info("Running full-load enriched Finanskladdelinje extraction (no date filter)")

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
                    bcp_max_chunk_size=5000,
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
                    ON [ext].[{ENRICHED_TARGET_TABLE}](ref_LedgertransId);
                    """
                )
                target_cursor.execute(
                    f"""
                    IF NOT EXISTS (
                        SELECT 1 FROM sys.indexes
                        WHERE name = 'IX_{ENRICHED_TARGET_TABLE}_FinanskontoId'
                          AND object_id = OBJECT_ID('[ext].[{ENRICHED_TARGET_TABLE}]')
                    )
                    CREATE NONCLUSTERED INDEX IX_{ENRICHED_TARGET_TABLE}_FinanskontoId
                    ON [ext].[{ENRICHED_TARGET_TABLE}](FinanskontoId);
                    """
                )
                target_conn.commit()

                target_cursor.execute(
                    f"""
                    SELECT
                        COUNT(*) AS total,
                        COUNT(ref_LedgerjournaltableId) AS has_header_ref,
                        COUNT(Kontonummer) AS has_kontonummer,
                        COUNT(Omkostningsstedid) AS has_omkostningssted,
                        COUNT(Transaktionstypenummer) AS has_transtype
                    FROM [ext].[{ENRICHED_TARGET_TABLE}]
                    """
                )
                stats = target_cursor.fetchone()
                context.log.info(
                    f"Coverage: "
                    f"header_ref={stats[1]}/{stats[0]}, "
                    f"Kontonummer={stats[2]}/{stats[0]}, "
                    f"Omkostningssted={stats[3]}/{stats[0]}, "
                    f"Transaktionstype={stats[4]}/{stats[0]}"
                )
            finally:
                target_cursor.close()

    return dg.MaterializeResult(
        metadata={
            "table": f"ext.{ENRICHED_TARGET_TABLE}",
            "row_count": row_count,
            "partition_key": "full_load",
            "partition_filter": partition_filter,
            "grain": "ref_LedgertransId",
            "business_key": "bk_ref_LedgertransId",
            "has_header_ref": stats[1],
            "has_kontonummer": stats[2],
            "has_omkostningssted": stats[3],
            "has_transtype": stats[4],
        }
    )


sydax_enriched_finanskladde_assets = [
    ext_finanskladdelinje_enriched_sydax_daily,
    ext_finanskladdelinje_enriched_sydax_monthly,
    ext_finanskladdelinje_enriched_sydax_full_load,
]

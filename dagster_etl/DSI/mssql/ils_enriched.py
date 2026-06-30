# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ILS enriched extraction: ext.vare_enriched_ils

EXCEPTION to standard DSI 1:1 contract (documented in sql_etl_guide.md):
This asset performs multi-table JOINs directly against ILS source tables and
writes the enriched result to ext.vare_enriched_ils on the target DW.
The exception exists because the enrichment requires cross-schema joins
(vare + vareart + varegrp + varetype + moms + unspsc) that cannot be
expressed via the standard TableConfig factory.

The entire enrichment query executes on the ILS source database; the result
set is fetched row-by-row and inserted into the target DW.

Delta: vnr from dbo.vare, dbo.lgrbeh, dbo.afdlgrbeh (pro2modified in partition range)
Grain: vnr
Note: vnr values from lgrbeh/afdlgrbeh that don't exist in dbo.vare produce stub rows
(vnr only, all other columns NULL) so they get a map.IlsVare entry.
"""

from datetime import date, timedelta

import dagster as dg
from core.partitions import daily_partitions_erp, monthly_partitions_erp
from core.resources import ILSMSSQLResource, MSSQLResource
from DSA.SQL.staging.clean_codes import ilsvare_clean_code_ils
from DSI.connectors.mssql.models import RowView

ENRICHED_TARGET_TABLE = "vare_enriched_ils"

SOURCE_QUERY = """
    ;WITH delta_vnr AS (
        SELECT vnr FROM dbo.vare WHERE pro2modified >= %s AND pro2modified < %s
        UNION
        SELECT vnr FROM dbo.lgrbeh WHERE pro2modified >= %s AND pro2modified < %s
        UNION
        SELECT vnr FROM dbo.afdlgrbeh WHERE pro2modified >= %s AND pro2modified < %s
    )
    SELECT
        v.vnr,
        v.navn,
        v.navn2,
        v.varestr,
        v.basisenh,
        v.vareart,
        va.navn AS vareart_navn,
        v.varegrp,
        vg.navn AS varegrp_navn,
        v.varetype,
        vt.navn AS varetype_navn,
        v.momskd,
        mo.txt AS momskode_navn,
        mo.momspct,
        v.oprdto,
        v.oprtid,
        v.unspc,
        u1.betegn AS unspsc_navn,
        v.unspsc2,
        u2.betegn AS unspsc_intern_navn,
        v.aktiv,
        v.faelvnr,
        v.ean_vnr,
        v.billed,
        v.oprbruger,
        zz.bd_navn AS oprbruger_navn,
        v.billedli,
        v.ejlukkes,
        v.steril,
        v.pro2modified
    FROM dbo.vare v
    LEFT JOIN dbo.vareart va ON va.vareart = v.vareart
    LEFT JOIN dbo.varegrp vg ON vg.varegrp = v.varegrp
    LEFT JOIN dbo.varetype vt ON vt.varetype = v.varetype
    LEFT JOIN dbo.moms mo ON mo.momskd = v.momskd
    LEFT JOIN dbo.unspsc u1 ON u1.unspsc = v.unspc
    LEFT JOIN dbo.unspsc u2 ON u2.unspsc = v.unspsc2
    LEFT JOIN dbo.zz_oper zz ON zz.bd_init = v.oprbruger
    WHERE EXISTS (SELECT 1 FROM delta_vnr d WHERE d.vnr = v.vnr)

    UNION ALL

    SELECT
        d.vnr,
        NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL,
        NULL
    FROM delta_vnr d
    WHERE NOT EXISTS (SELECT 1 FROM dbo.vare v WHERE v.vnr = d.vnr)
"""

COLUMNS = [
    "vnr",
    "navn",
    "navn2",
    "varestr",
    "basisenh",
    "vareart",
    "vareart_navn",
    "varegrp",
    "varegrp_navn",
    "varetype",
    "varetype_navn",
    "momskd",
    "momskode_navn",
    "momspct",
    "oprdto",
    "oprtid",
    "unspc",
    "unspsc_navn",
    "unspsc2",
    "unspsc_intern_navn",
    "aktiv",
    "faelvnr",
    "ean_vnr",
    "billed",
    "oprbruger",
    "oprbruger_navn",
    "billedli",
    "ejlukkes",
    "steril",
    "pro2modified",
]

INSERT_COLUMNS = [
    "vnr",
    "navn",
    "navn2",
    "varestr",
    "basisenh",
    "vareart",
    "vareart_navn",
    "varegrp",
    "varegrp_navn",
    "varetype",
    "varetype_navn",
    "momskd",
    "momskode_navn",
    "momspct",
    "oprdto",
    "oprtid",
    "unspc",
    "unspsc_navn",
    "unspsc2",
    "unspsc_intern_navn",
    "aktiv",
    "faelvnr",
    "ean_vnr",
    "billed",
    "oprbruger",
    "oprbruger_navn",
    "billedli",
    "ejlukkes",
    "steril",
    "pro2modified",
    "CleanCode_IlsVare",
]

ENRICHED_TABLE_DDL = f"""
    IF NOT EXISTS (
        SELECT * FROM sys.objects
        WHERE object_id = OBJECT_ID(N'[ext].[{ENRICHED_TARGET_TABLE}]')
          AND type = 'U'
    )
    BEGIN
        CREATE TABLE [ext].[{ENRICHED_TARGET_TABLE}] (
            [vnr] NVARCHAR(100) NULL,
            [navn] NVARCHAR(MAX) NULL,
            [navn2] NVARCHAR(MAX) NULL,
            [varestr] NVARCHAR(255) NULL,
            [basisenh] NVARCHAR(50) NULL,
            [vareart] NVARCHAR(50) NULL,
            [vareart_navn] NVARCHAR(255) NULL,
            [varegrp] NVARCHAR(50) NULL,
            [varegrp_navn] NVARCHAR(255) NULL,
            [varetype] NVARCHAR(50) NULL,
            [varetype_navn] NVARCHAR(255) NULL,
            [momskd] NVARCHAR(50) NULL,
            [momskode_navn] NVARCHAR(255) NULL,
            [momspct] DECIMAL(18,4) NULL,
            [oprdto] DATETIME2 NULL,
            [oprtid] INT NULL,
            [unspc] NVARCHAR(50) NULL,
            [unspsc_navn] NVARCHAR(255) NULL,
            [unspsc2] NVARCHAR(50) NULL,
            [unspsc_intern_navn] NVARCHAR(255) NULL,
            [aktiv] BIT NULL,
            [faelvnr] NVARCHAR(100) NULL,
            [ean_vnr] NVARCHAR(100) NULL,
            [billed] NVARCHAR(MAX) NULL,
            [oprbruger] NVARCHAR(50) NULL,
            [oprbruger_navn] NVARCHAR(255) NULL,
            [billedli] NVARCHAR(MAX) NULL,
            [ejlukkes] NVARCHAR(50) NULL,
            [steril] NVARCHAR(50) NULL,
            [pro2modified] DATETIME2 NULL,
            [CleanCode_IlsVare] NVARCHAR(100) NULL,
            [DW_LoadTimestamp] DATETIME2 NOT NULL DEFAULT SYSDATETIME()
        ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
    END
"""


def _make_extract_vare_enriched_asset(partition_type: str):
    """Build a daily or monthly enriched vare extraction asset."""
    suffix = partition_type
    partitions_def = daily_partitions_erp if partition_type == "daily" else monthly_partitions_erp

    @dg.asset(
        key=dg.AssetKey([f"ext_vare_enriched_ils_{suffix}"]),
        group_name=f"erp_extract_{suffix}",
        compute_kind="mssql",
        partitions_def=partitions_def,
        deps=[
            dg.AssetKey(["ext_vare_ils"]),
            dg.AssetKey(["ext_vareart_ils"]),
            dg.AssetKey(["ext_varegrp_ils"]),
        ],
        tags={
            "domain": "erp",
            "layer": "extract",
            "source": "ils",
            "entity": "vare_enriched",
        },
        retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
        description=(
            f"Enriched extraction from ILS: joins dbo.vare with vareart, "
            f"varegrp, varetype, moms, unspsc — query runs directly on ILS "
            f"source, result inserted into ext.{ENRICHED_TARGET_TABLE}. "
            f"Partitioned by pro2modified ({suffix}). Grain: vnr."
        ),
    )
    def _extract_vare_enriched(
        context: dg.AssetExecutionContext,
        ils_mssql: ILSMSSQLResource,
        mssql_target_staging: MSSQLResource,
    ) -> dg.MaterializeResult:
        partition_key = context.partition_key

        parts = partition_key.split("-")
        if len(parts) == 3:
            start_date = date(int(parts[0]), int(parts[1]), int(parts[2]))
            end_date = start_date + timedelta(days=1)
        elif len(parts) == 2:
            year, month = int(parts[0]), int(parts[1])
            start_date = date(year, month, 1)
            if month == 12:
                end_date = date(year + 1, 1, 1)
            else:
                end_date = date(year, month + 1, 1)
        else:
            raise ValueError(f"Unexpected partition key format: {partition_key}")

        context.log.info(
            f"Extracting vare_enriched from ILS ({suffix}): {start_date} to {end_date}"
        )

        rows = []
        with ils_mssql.get_connection() as source_conn:
            source_cursor = source_conn.cursor()
            try:
                source_cursor.execute(
                    SOURCE_QUERY,
                    (
                        start_date.isoformat(),
                        end_date.isoformat(),
                        start_date.isoformat(),
                        end_date.isoformat(),
                        start_date.isoformat(),
                        end_date.isoformat(),
                    ),
                )
                for row in source_cursor:
                    row_dict = dict(zip(COLUMNS, row, strict=False))
                    row_view = RowView(row_dict)
                    row_dict["CleanCode_IlsVare"] = ilsvare_clean_code_ils(row_view)
                    rows.append(row_dict)
            finally:
                source_cursor.close()

        context.log.info(f"Extracted {len(rows)} rows from ILS")

        if not rows:
            return dg.MaterializeResult(
                metadata={
                    "table": f"ext.{ENRICHED_TARGET_TABLE}",
                    "row_count": 0,
                    "partition_key": partition_key,
                }
            )

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(ENRICHED_TABLE_DDL)

                cursor.execute(f"TRUNCATE TABLE [ext].[{ENRICHED_TARGET_TABLE}]")
                context.log.info(f"Truncated ext.{ENRICHED_TARGET_TABLE} before loading")

                col_list = ", ".join(INSERT_COLUMNS)
                placeholders = ", ".join(["?"] * len(INSERT_COLUMNS))
                insert_sql = (
                    f"INSERT INTO [ext].[{ENRICHED_TARGET_TABLE}] "
                    f"({col_list}) VALUES ({placeholders})"
                )
                for row_dict in rows:
                    cursor.execute(
                        insert_sql,
                        tuple(row_dict.get(c) for c in INSERT_COLUMNS),
                    )

                conn.commit()
            finally:
                cursor.close()

        return dg.MaterializeResult(
            metadata={
                "table": f"ext.{ENRICHED_TARGET_TABLE}",
                "row_count": len(rows),
                "partition_key": partition_key,
                "date_range": f"{start_date} to {end_date}",
                "source": "ILS dbo.vare + 5 reference tables",
            }
        )

    _extract_vare_enriched.__name__ = f"ext_vare_enriched_ils_{suffix}"
    _extract_vare_enriched.__qualname__ = f"ext_vare_enriched_ils_{suffix}"
    return _extract_vare_enriched


ext_vare_enriched_ils_daily = _make_extract_vare_enriched_asset("daily")
ext_vare_enriched_ils_monthly = _make_extract_vare_enriched_asset("monthly")

ils_enriched_assets = [
    ext_vare_enriched_ils_daily,
    ext_vare_enriched_ils_monthly,
]

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Vare

Sync dimension from map + add descriptive attributes.
Logic from: erp_dims_and_facts.yml (Type 1 SCD with composite natural key)

Uses the unified dimension staging table:
- stg.dim_vare: All attributes pre-joined (MySupply + ILS bridge + agreements)

Privacy (task 27191): This dimension uses ONLY MySupply/ILS sources.
KMD ItemDescription/ItemName must NEVER be added here — see guardrail test.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimensions",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_vare"]),
        AssetKey(["map_aftale"]),
        AssetKey(["erp", "stg_dim_vare"]),
        AssetKey(["map_ilsvare"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="Sync dim.Vare from map + unified stg.dim_vare attributes",
)
def dim_vare(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Dimension table maintenance: Sync from map + add attributes from unified staging."""

    afterburn_count = 0

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Vare dimension sync from unified stg.dim_vare...")

            cursor.execute("SELECT COUNT(*) FROM [map].[Vare]")
            map_count = cursor.fetchone()[0]
            context.log.info(f"Map has {map_count} Vare entries to sync")

            cursor.execute(
                """
            SELECT COUNT(*)
            FROM [stg].[dim_vare] v
            LEFT JOIN [map].[Vare] m
                ON m.CleanCode_Vare = v.CleanCode_Vare
            WHERE m.DW_EK_Vare IS NULL
            """
            )
            unmapped_rows = cursor.fetchone()[0]
            if unmapped_rows > 0:
                context.log.warning(f"Found {unmapped_rows} stg.dim_vare rows without map match")

            cursor.execute(
                """
            DROP TABLE IF EXISTS #Vare_Attributes;

            SELECT
                m.DW_EK_Vare,
                m.bk_Kreditor AS CVR,
                LEFT(m.bk_LeverandoerVarenummer, 255) AS VarenummerKreditor,

                -- MySupply attributes
                v.Final_Varenavn,
                v.Item_FakturaUdstederVareNavn AS VarenavnKort,

                -- ILS product attributes
                v.ILS_Varenavn,
                v.vnr AS ILS_Varenummer,
                v.faelvnr AS FaellesVarenummer,
                v.ean_vnr AS Stregkode,
                v.varestr AS StoerrelseTekst,
                v.navn_conc AS SammensatNavn,
                v.basisenh AS Basisenhed,
                v.vareart AS VareartKode,
                v.varegrp AS VaregruppeKode,
                v.varetype AS Varetype,

                -- Agreement/pricing attributes
                v.maerke AS Maerke,
                v.pakstr1 AS Pakkestoerrelse1,
                v.pakstr2 AS Pakkestoerrelse2,
                v.pakstr3 AS Pakkestoerrelse3,
                v.vareart_navn AS VareartNavn,
                v.varegrp_navn AS VaregruppeNavn,

                -- Agreement detail attributes
                v.aftale AS Aftale,
                v.afttype AS AftaleType,
                v.aftaletekst AS Aftaletekst,
                v.lvnr1 AS Kreditorvarenummer1,
                v.lvnr2 AS Kreditorvarenummer2,
                v.lvnr3 AS Kreditorvarenummer3,
                v.pris1 AS Pris1,
                v.pris2 AS Pris2,
                v.pris3 AS Pris3,
                v.ils_dsa_aftale_varenummer,
                v.kreditornummer AS Kreditornummer,
                v.momskd,
                v.afgiftkd,
                COALESCE(ma.DW_EK_Aftale, -1) AS DW_EK_Aftale

            INTO #Vare_Attributes
            FROM [stg].[dim_vare] v

            LEFT JOIN [map].[Vare] m
                ON m.CleanCode_Vare = v.CleanCode_Vare

            LEFT JOIN [map].[Aftale] ma
                ON ma.CleanCode_Aftale = v.CleanCode_Aftale
            WHERE m.DW_EK_Vare IS NOT NULL;
            """,
            )

            cursor.execute(
                """
            CREATE UNIQUE CLUSTERED INDEX IX_tmp_Vare_Attributes
            ON #Vare_Attributes (DW_EK_Vare)
            """
            )

            cursor.execute(
                """
            MERGE [dim].[Vare] AS tgt
            USING #Vare_Attributes AS src
            ON tgt.DW_EK_Vare = src.DW_EK_Vare

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.CVR = src.CVR,
                    tgt.VarenummerKreditor = src.VarenummerKreditor,
                    tgt.Varenavn = src.Final_Varenavn,
                    tgt.VarenavnKort = src.VarenavnKort,
                    tgt.Maerke = src.Maerke,
                    tgt.ILS_Varenummer = src.ILS_Varenummer,
                    tgt.FaellesVarenummer = src.FaellesVarenummer,
                    tgt.Stregkode = src.Stregkode,
                    tgt.StoerrelseTekst = src.StoerrelseTekst,
                    tgt.SammensatNavn = src.SammensatNavn,
                    tgt.Basisenhed = src.Basisenhed,
                    tgt.Pakkestoerrelse1 = src.Pakkestoerrelse1,
                    tgt.Pakkestoerrelse2 = src.Pakkestoerrelse2,
                    tgt.Pakkestoerrelse3 = src.Pakkestoerrelse3,
                    tgt.VareartKode = src.VareartKode,
                    tgt.VareartNavn = src.VareartNavn,
                    tgt.VaregruppeKode = src.VaregruppeKode,
                    tgt.VaregruppeNavn = src.VaregruppeNavn,
                    tgt.Varetype = src.Varetype,
                    tgt.Aftale = src.Aftale,
                    tgt.AftaleType = src.AftaleType,
                    tgt.Aftaletekst = src.Aftaletekst,
                    tgt.Kreditorvarenummer1 = src.Kreditorvarenummer1,
                    tgt.Kreditorvarenummer2 = src.Kreditorvarenummer2,
                    tgt.Kreditorvarenummer3 = src.Kreditorvarenummer3,
                    tgt.Pris1 = src.Pris1,
                    tgt.Pris2 = src.Pris2,
                    tgt.Pris3 = src.Pris3,
                    tgt.ils_dsa_aftale_varenummer = src.ils_dsa_aftale_varenummer,
                    tgt.Kreditornummer = src.Kreditornummer,
                    tgt.momskd = src.momskd,
                    tgt.afgiftkd = src.afgiftkd,
                    tgt.DW_EK_Aftale = src.DW_EK_Aftale,
                    tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    DW_EK_Vare, CVR, VarenummerKreditor, Varenavn, VarenavnKort,
                    Maerke, ILS_Varenummer, FaellesVarenummer, Stregkode, StoerrelseTekst,
                    SammensatNavn, Basisenhed, Pakkestoerrelse1, Pakkestoerrelse2, Pakkestoerrelse3,
                    VareartKode, VareartNavn, VaregruppeKode, VaregruppeNavn, Varetype,
                    Aftale, AftaleType, Aftaletekst, Kreditorvarenummer1, Kreditorvarenummer2,
                    Kreditorvarenummer3,
                    Pris1, Pris2, Pris3, ils_dsa_aftale_varenummer, Kreditornummer,
                    momskd, afgiftkd, DW_EK_Aftale,
                    DW_Timestamp_Insert
                )
                VALUES (
                    src.DW_EK_Vare, src.CVR,
                    src.VarenummerKreditor, src.Final_Varenavn, src.VarenavnKort,
                    src.Maerke, src.ILS_Varenummer, src.FaellesVarenummer, src.Stregkode,
                    src.StoerrelseTekst,
                    src.SammensatNavn, src.Basisenhed, src.Pakkestoerrelse1, src.Pakkestoerrelse2,
                    src.Pakkestoerrelse3,
                    src.VareartKode, src.VareartNavn, src.VaregruppeKode, src.VaregruppeNavn,
                    src.Varetype,
                    src.Aftale, src.AftaleType, src.Aftaletekst, src.Kreditorvarenummer1,
                    src.Kreditorvarenummer2, src.Kreditorvarenummer3,
                    src.Pris1, src.Pris2, src.Pris3, src.ils_dsa_aftale_varenummer,
                    src.Kreditornummer,
                    src.momskd, src.afgiftkd, src.DW_EK_Aftale,
                    CAST(SYSDATETIME() AS datetime2(0))
                );
                """
            )
            rows_affected = cursor.rowcount
            conn.commit()

            cursor.execute("DROP TABLE IF EXISTS #Vare_Attributes")
            cursor.execute("UPDATE STATISTICS [dim].[Vare]")

            # Afterburn: backfill DW_EK_IlsVare for rows where vnr is known
            cursor.execute(
                """
                UPDATE v
                SET v.DW_EK_IlsVare = m.DW_EK_IlsVare,
                    v.DW_Timestamp_Update = SYSDATETIME()
                FROM [dim].[Vare] v
                INNER JOIN [map].[IlsVare] m
                    ON m.CleanCode_IlsVare = CONCAT('ILSVARE_', UPPER(LTRIM(RTRIM(v.ILS_Varenummer))))
                WHERE v.DW_EK_IlsVare = -1
                AND m.DW_EK_IlsVare <> -1
                AND v.ILS_Varenummer IS NOT NULL
                AND LTRIM(RTRIM(v.ILS_Varenummer)) <> '';
            """
            )
            afterburn_count = cursor.rowcount
            conn.commit()
            if afterburn_count > 0:
                context.log.info(
                    f"Afterburn: updated {afterburn_count} dim.Vare rows with DW_EK_IlsVare"
                )

            context.log.info(f"Dimension sync complete: {rows_affected} rows merged")
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "table": "dim.Vare",
            "composite_natural_key": "CleanCode_Vare",
            "source_table": "stg.dim_vare (unified)",
            "unmapped_stg_rows": unmapped_rows,
            "scd_type": "Type 1 (update in place)",
            "afterburn_ilsvare_updated": afterburn_count,
        }
    )

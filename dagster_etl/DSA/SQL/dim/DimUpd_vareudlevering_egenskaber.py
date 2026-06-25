# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: VareudleveringEgenskaber

1:1 dimension with fct.Vareudlevering — same grain (one row per delivery line).
Enriches the fact table with descriptive attributes from the source.

Type 1 SCD — overwrite on change.

Uses:
- map.Vareudlevering: DW_EK_Vareudlevering assignment via IDENTITY
- stg.dim_vareudlevering_ils: Dimension-layer attributes (identifiers + descriptives)
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimensions",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_vareudlevering"]),
        AssetKey(["erp", "stg_dim_vareudlevering_ils"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="Sync dim.VareudleveringEgenskaber from map + add attributes from ILS dimension staging",
)
def dim_vareudlevering_egenskaber(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Dimension table maintenance: Sync from map + add attributes from ILS dimension staging."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting VareudleveringEgenskaber dimension sync...")

            cursor.execute("SELECT COUNT(*) FROM [map].[Vareudlevering]")
            map_count = cursor.fetchone()[0]
            context.log.info(f"Map has {map_count} Vareudlevering entries to sync")

            cursor.execute(
                """
            SELECT COUNT(*)
            FROM [stg].[dim_vareudlevering_ils] ds
            LEFT JOIN [map].[Vareudlevering] m
                ON m.CleanCode_Vareudlevering = ds.CleanCode_Vareudlevering
            WHERE m.DW_EK_Vareudlevering IS NULL
        """
            )
            unmapped_rows = cursor.fetchone()[0]
            if unmapped_rows > 0:
                raise ValueError(
                    f"dim_vareudlevering_egenskaber: {unmapped_rows} staging rows have no match in map.Vareudlevering. "
                    "Ensure map_vareudlevering runs before dim_vareudlevering_egenskaber and staging CleanCodes are valid."
                )

            cursor.execute(
                """
            DROP TABLE IF EXISTS #VareudleveringEgenskaber_Attributes;

            SELECT
                m.DW_EK_Vareudlevering AS DW_EK_VareudleveringEgenskaber,
                m.CleanCode_Vareudlevering AS CleanCode_VareudleveringEgenskaber,

                ds.Revisionsnummer,
                ds.Foelgeseddelnummer,
                ds.Brik,
                ds.Leverandoernummer,
                ds.Ordrenummer,
                ds.Ordrelinje,
                ds.sw_direkte_udlevering,
                ds.sw_pakkeseddel,
                ds.sw_foelgeseddel,
                ds.sw_fly,
                ds.sw_haster,
                ds.sw_erstat,
                ds.Projektnummer,
                ds.Kontonummer_kode,
                ds.Bruger,
                ds.bk_prrowid,
                ds.Referencenummer,
                ds.Referencelinje,
                ds.Salgsordre,
                ds.SalgsordreId,
                ds.Salgsfakturanummer,
                ds.RevisionsvarerId,
                ds.PlukvarerId,
                ds.Afleveringsoperatoer,
                ds.Afleveringstekst,
                ds.Afleveringsstregkode,
                ds.Vognnummer

            INTO #VareudleveringEgenskaber_Attributes
            FROM [stg].[dim_vareudlevering_ils] ds

            LEFT JOIN [map].[Vareudlevering] m
                ON m.CleanCode_Vareudlevering = ds.CleanCode_Vareudlevering
        """
            )

            context.log.info("Temp table #VareudleveringEgenskaber_Attributes created")

            # MERGE into dimension (Type 1 SCD)
            cursor.execute(
                """
            MERGE [dim].[VareudleveringEgenskaber] AS tgt
            USING #VareudleveringEgenskaber_Attributes AS src
            ON tgt.DW_EK_VareudleveringEgenskaber = src.DW_EK_VareudleveringEgenskaber

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.CleanCode_VareudleveringEgenskaber = src.CleanCode_VareudleveringEgenskaber,
                    tgt.Revisionsnummer = src.Revisionsnummer,
                    tgt.Foelgeseddelnummer = src.Foelgeseddelnummer,
                    tgt.Brik = src.Brik,
                    tgt.Leverandoernummer = src.Leverandoernummer,
                    tgt.Ordrenummer = src.Ordrenummer,
                    tgt.Ordrelinje = src.Ordrelinje,
                    tgt.sw_direkte_udlevering = src.sw_direkte_udlevering,
                    tgt.sw_pakkeseddel = src.sw_pakkeseddel,
                    tgt.sw_foelgeseddel = src.sw_foelgeseddel,
                    tgt.sw_fly = src.sw_fly,
                    tgt.sw_haster = src.sw_haster,
                    tgt.sw_erstat = src.sw_erstat,
                    tgt.Projektnummer = src.Projektnummer,
                    tgt.Kontonummer_kode = src.Kontonummer_kode,
                    tgt.Bruger = src.Bruger,
                    tgt.bk_prrowid = src.bk_prrowid,
                    tgt.Referencenummer = src.Referencenummer,
                    tgt.Referencelinje = src.Referencelinje,
                    tgt.Salgsordre = src.Salgsordre,
                    tgt.SalgsordreId = src.SalgsordreId,
                    tgt.Salgsfakturanummer = src.Salgsfakturanummer,
                    tgt.RevisionsvarerId = src.RevisionsvarerId,
                    tgt.PlukvarerId = src.PlukvarerId,
                    tgt.Afleveringsoperatoer = src.Afleveringsoperatoer,
                    tgt.Afleveringstekst = src.Afleveringstekst,
                    tgt.Afleveringsstregkode = src.Afleveringsstregkode,
                    tgt.Vognnummer = src.Vognnummer,
                    tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    DW_EK_VareudleveringEgenskaber,
                    CleanCode_VareudleveringEgenskaber,
                    Revisionsnummer,
                    Foelgeseddelnummer,
                    Brik,
                    Leverandoernummer,
                    Ordrenummer,
                    Ordrelinje,
                    sw_direkte_udlevering,
                    sw_pakkeseddel,
                    sw_foelgeseddel,
                    sw_fly,
                    sw_haster,
                    sw_erstat,
                    Projektnummer,
                    Kontonummer_kode,
                    Bruger,
                    bk_prrowid,
                    Referencenummer,
                    Referencelinje,
                    Salgsordre,
                    SalgsordreId,
                    Salgsfakturanummer,
                    RevisionsvarerId,
                    PlukvarerId,
                    Afleveringsoperatoer,
                    Afleveringstekst,
                    Afleveringsstregkode,
                    Vognnummer,
                    DW_Timestamp_Insert
                )
                VALUES (
                    src.DW_EK_VareudleveringEgenskaber,
                    src.CleanCode_VareudleveringEgenskaber,
                    src.Revisionsnummer,
                    src.Foelgeseddelnummer,
                    src.Brik,
                    src.Leverandoernummer,
                    src.Ordrenummer,
                    src.Ordrelinje,
                    src.sw_direkte_udlevering,
                    src.sw_pakkeseddel,
                    src.sw_foelgeseddel,
                    src.sw_fly,
                    src.sw_haster,
                    src.sw_erstat,
                    src.Projektnummer,
                    src.Kontonummer_kode,
                    src.Bruger,
                    src.bk_prrowid,
                    src.Referencenummer,
                    src.Referencelinje,
                    src.Salgsordre,
                    src.SalgsordreId,
                    src.Salgsfakturanummer,
                    src.RevisionsvarerId,
                    src.PlukvarerId,
                    src.Afleveringsoperatoer,
                    src.Afleveringstekst,
                    src.Afleveringsstregkode,
                    src.Vognnummer,
                    CAST(SYSDATETIME() AS datetime2(0))
                );
        """
            )

            rows_affected = cursor.rowcount
            conn.commit()

            # Clean up temp table
            cursor.execute("DROP TABLE IF EXISTS #VareudleveringEgenskaber_Attributes")

            cursor.execute("UPDATE STATISTICS [dim].[VareudleveringEgenskaber]")

            # DIAGNOSTIC: Get dimension statistics
            cursor.execute(
                """
            SELECT
                COUNT(*) AS total_dim_rows,
                SUM(CASE WHEN DW_Timestamp_Update IS NULL THEN 1 ELSE 0 END) AS inserts_only,
                SUM(CASE WHEN DW_Timestamp_Update IS NOT NULL THEN 1 ELSE 0 END) AS updated,
                COUNT(Revisionsnummer) AS has_revisionsnummer,
                COUNT(Foelgeseddelnummer) AS has_foelgeseddelnummer,
                COUNT(Brik) AS has_brik,
                COUNT(Leverandoernummer) AS has_leverandoernummer,
                COUNT(Ordrenummer) AS has_ordrenummer,
                COUNT(Ordrelinje) AS has_ordrelinje,
                COUNT(sw_direkte_udlevering) AS has_sw_direkte,
                COUNT(sw_pakkeseddel) AS has_sw_pakkeseddel,
                COUNT(sw_foelgeseddel) AS has_sw_foelgeseddel,
                COUNT(sw_fly) AS has_sw_fly,
                COUNT(sw_haster) AS has_sw_haster,
                COUNT(sw_erstat) AS has_sw_erstat,
                COUNT(Projektnummer) AS has_projektnummer,
                COUNT(Kontonummer_kode) AS has_kontonummer_kode,
                COUNT(Bruger) AS has_bruger,
                COUNT(bk_prrowid) AS has_bk_prrowid,
                COUNT(Referencenummer) AS has_referencenummer,
                COUNT(Referencelinje) AS has_referencelinje,
                COUNT(Salgsordre) AS has_salgsordre,
                COUNT(SalgsordreId) AS has_salgsordre_id,
                COUNT(Salgsfakturanummer) AS has_salgsfakturanummer,
                COUNT(RevisionsvarerId) AS has_revisionsvareid,
                COUNT(PlukvarerId) AS has_plukvareid,
                COUNT(Afleveringsoperatoer) AS has_afleveringsoperatoer,
                COUNT(Afleveringstekst) AS has_afleveringstekst,
                COUNT(Afleveringsstregkode) AS has_afleveringsstregkode,
                COUNT(Vognnummer) AS has_vognnummer
            FROM [dim].[VareudleveringEgenskaber]
            WHERE DW_EK_VareudleveringEgenskaber <> -1
        """
            )
            stats = cursor.fetchone()
            total_dim_rows = stats[0]
            inserts_only = stats[1]
            updated_count = stats[2]
            has_revisionsnummer = stats[3]
            has_foelgeseddelnummer = stats[4]
            has_brik = stats[5]
            has_leverandoernummer = stats[6]
            has_ordrenummer = stats[7]
            has_ordrelinje = stats[8]
            has_sw_direkte = stats[9]
            has_sw_pakkeseddel = stats[10]
            has_sw_foelgeseddel = stats[11]
            has_sw_fly = stats[12]
            has_sw_haster = stats[13]
            has_sw_erstat = stats[14]
            has_projektnummer = stats[15]
            has_kontonummer_kode = stats[16]
            has_bruger = stats[17]
            has_bk_prrowid = stats[18]
            has_referencenummer = stats[19]
            has_referencelinje = stats[20]
            has_salgsordre = stats[21]
            has_salgsordre_id = stats[22]
            has_salgsfakturanummer = stats[23]
            has_revisionsvareid = stats[24]
            has_plukvareid = stats[25]
            has_afleveringsoperatoer = stats[26]
            has_afleveringstekst = stats[27]
            has_afleveringsstregkode = stats[28]
            has_vognnummer = stats[29]

            context.log.info(
                f"Dimension sync complete: {total_dim_rows} total rows "
                f"({inserts_only} insert-only, {updated_count} updated)"
            )
            context.log.info(
                f"Coverage: {has_revisionsnummer}/{total_dim_rows} with Revisionsnummer, "
                f"{has_foelgeseddelnummer}/{total_dim_rows} with Foelgeseddelnummer, "
                f"{has_brik}/{total_dim_rows} with Brik, "
                f"{has_leverandoernummer}/{total_dim_rows} with Leverandoernummer, "
                f"{has_ordrenummer}/{total_dim_rows} with Ordrenummer, "
                f"{has_ordrelinje}/{total_dim_rows} with Ordrelinje, "
                f"{has_sw_direkte}/{total_dim_rows} with sw_direkte_udlevering, "
                f"{has_sw_pakkeseddel}/{total_dim_rows} with sw_pakkeseddel, "
                f"{has_sw_foelgeseddel}/{total_dim_rows} with sw_foelgeseddel, "
                f"{has_sw_fly}/{total_dim_rows} with sw_fly, "
                f"{has_sw_haster}/{total_dim_rows} with sw_haster, "
                f"{has_sw_erstat}/{total_dim_rows} with sw_erstat, "
                f"{has_projektnummer}/{total_dim_rows} with Projektnummer, "
                f"{has_kontonummer_kode}/{total_dim_rows} with Kontonummer_kode, "
                f"{has_bruger}/{total_dim_rows} with Bruger, "
                f"{has_bk_prrowid}/{total_dim_rows} with bk_prrowid, "
                f"{has_referencenummer}/{total_dim_rows} with Referencenummer, "
                f"{has_referencelinje}/{total_dim_rows} with Referencelinje, "
                f"{has_salgsordre}/{total_dim_rows} with Salgsordre, "
                f"{has_salgsordre_id}/{total_dim_rows} with SalgsordreId, "
                f"{has_salgsfakturanummer}/{total_dim_rows} with Salgsfakturanummer, "
                f"{has_revisionsvareid}/{total_dim_rows} with RevisionsvarerId, "
                f"{has_plukvareid}/{total_dim_rows} with PlukvarerId, "
                f"{has_afleveringsoperatoer}/{total_dim_rows} with Afleveringsoperatoer, "
                f"{has_afleveringstekst}/{total_dim_rows} with Afleveringstekst, "
                f"{has_afleveringsstregkode}/{total_dim_rows} with Afleveringsstregkode, "
                f"{has_vognnummer}/{total_dim_rows} with Vognnummer"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "total_dim_rows": total_dim_rows,
            "inserts_only": inserts_only,
            "updated": updated_count,
            "has_revisionsnummer": has_revisionsnummer,
            "has_foelgeseddelnummer": has_foelgeseddelnummer,
            "has_brik": has_brik,
            "has_leverandoernummer": has_leverandoernummer,
            "has_ordrenummer": has_ordrenummer,
            "has_ordrelinje": has_ordrelinje,
            "has_sw_direkte_udlevering": has_sw_direkte,
            "has_sw_pakkeseddel": has_sw_pakkeseddel,
            "has_sw_foelgeseddel": has_sw_foelgeseddel,
            "has_sw_fly": has_sw_fly,
            "has_sw_haster": has_sw_haster,
            "has_sw_erstat": has_sw_erstat,
            "has_projektnummer": has_projektnummer,
            "has_kontonummer_kode": has_kontonummer_kode,
            "has_bruger": has_bruger,
            "has_bk_prrowid": has_bk_prrowid,
            "has_referencenummer": has_referencenummer,
            "has_referencelinje": has_referencelinje,
            "has_salgsordre": has_salgsordre,
            "has_salgsordre_id": has_salgsordre_id,
            "has_salgsfakturanummer": has_salgsfakturanummer,
            "has_revisionsvareid": has_revisionsvareid,
            "has_plukvareid": has_plukvareid,
            "has_afleveringsoperatoer": has_afleveringsoperatoer,
            "has_afleveringstekst": has_afleveringstekst,
            "has_afleveringsstregkode": has_afleveringsstregkode,
            "has_vognnummer": has_vognnummer,
            "table": "dim.VareudleveringEgenskaber",
            "source_tables": "stg.dim_vareudlevering_ils + map.Vareudlevering",
            "scd_type": "Type 1 (overwrite)",
        }
    )

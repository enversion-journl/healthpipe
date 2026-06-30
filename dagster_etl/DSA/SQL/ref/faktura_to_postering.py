# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ref.Faktura_To_Postering — fct.Faktura ↔ fct.Postering bridge.

Full crosswalk of matched (DW_EK_Faktura, DW_EK_Postering) pairs with
"is_newest_*" flags for bidirectional FK resolution. Matched on conformed
(DW_EK_Kreditor, FakturaNr). Soft-delete model (DW_IsDeleted).

Rebuilt each run (no watermark). The apply-pass asset
(fct_apply_faktura_postering_relations) uses the "newest" rows to stamp
cross-FKs on fct.Faktura and fct.Postering.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="ref",
    compute_kind="mssql",
    deps=[
        AssetKey(["fct_faktura"]),
        AssetKey(["fct_postering"]),
        AssetKey(["dim_faktura"]),  # bridge reads header-identity cols from dim.Faktura
    ],
    tags={"layer": "ref", "module": "erp", "domain": "erp"},
    description=(
        "ref.Faktura_To_Postering — full crosswalk of fct.Faktura ↔ fct.Postering "
        "matched on (DW_EK_Kreditor, FakturaNr) with newest-selection flags."
    ),
)
def ref_faktura_to_postering(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """Build / refresh ref.Faktura_To_Postering from fct.Faktura × fct.Postering."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Building ref.Faktura_To_Postering bridge...")

            cursor.execute(
                """
                DROP TABLE IF EXISTS #ftp;
                WITH posting AS (
                    SELECT p.DW_EK_Postering, p.ref_LedgertransId, p.FakturaNr,
                           p.DW_EK_Kreditor, p.DW_haendelsestidspunkt,
                           -- Normalised invoice number — MUST mirror the Faktura_CleanCode
                           -- derivation in pre_stg_fakturalinje_mysupply / sydax_enriched
                           -- ('FAKTURA_' + UPPER(LTRIM(RTRIM(REPLACE(nr,' ',''))))). The raw
                           -- MySupply FakturaNr and SydAX Fakturanummer differ in case/spaces,
                           -- so matching on raw FakturaNr loses almost everything.
                           UPPER(LTRIM(RTRIM(REPLACE(p.FakturaNr, ' ', '')))) AS FakturaNr_norm
                    FROM [fct].[Postering] p
                    WHERE p.DW_EK_Postering <> -1 AND p.DW_EK_Kreditor <> -1
                      AND p.FakturaNr IS NOT NULL
                      AND LTRIM(RTRIM(REPLACE(p.FakturaNr, ' ', ''))) <> ''
                ),
                faktura AS (
                    -- Carry the MySupply header identity from the (persistent) dimension
                    -- so the "newest faktura per postering" ranking uses the real header
                    -- load order. dim.Faktura is 1:1 with fct.Faktura on DW_EK_Faktura;
                    -- LEFT JOIN keeps SydAX-only map rows (NULL header cols sort last).
                    -- NOTE: do NOT join stg.dim_faktura_mysupply here — it is a volatile
                    -- DROP/CREATE table and an INNER JOIN to it can silently empty the
                    -- crosswalk (see ref-table empty-bridge incident).
                    SELECT f.DW_EK_Faktura, f.DW_EK_Kreditor, f.FakturaNr,
                           d.statistic_invoice_header_id, d.Header_DW_LoadTimestamp,
                           UPPER(LTRIM(RTRIM(REPLACE(f.FakturaNr, ' ', '')))) AS FakturaNr_norm
                    FROM [fct].[Faktura] f
                    LEFT JOIN [dim].[Faktura] d ON d.DW_EK_Faktura = f.DW_EK_Faktura
                    WHERE f.DW_EK_Faktura <> -1 AND f.DW_EK_Kreditor <> -1
                      AND f.FakturaNr IS NOT NULL
                      AND LTRIM(RTRIM(REPLACE(f.FakturaNr, ' ', ''))) <> ''
                ),
                matched AS (
                    SELECT f.DW_EK_Faktura, pg.DW_EK_Postering,
                           f.DW_EK_Kreditor, f.FakturaNr,
                           pg.DW_haendelsestidspunkt, pg.ref_LedgertransId,
                           f.Header_DW_LoadTimestamp, f.statistic_invoice_header_id
                    FROM faktura f
                    JOIN posting pg
                      ON pg.DW_EK_Kreditor = f.DW_EK_Kreditor
                     AND pg.FakturaNr_norm = f.FakturaNr_norm   -- normalised, case/space-insensitive
                ),
                ranked AS (
                    SELECT *,
                        -- newest postering per faktura: source-system creation time (discriminating)
                        ROW_NUMBER() OVER (
                            PARTITION BY DW_EK_Faktura
                            ORDER BY DW_haendelsestidspunkt DESC,
                                     ref_LedgertransId DESC
                        ) AS rn_post,
                        -- newest faktura per postering: latest MySupply header load
                        -- (Header_DW_LoadTimestamp, statistic_invoice_header_id) — the agreed
                        -- ordering, NOT the insert-order DW_EK_Faktura surrogate.
                        ROW_NUMBER() OVER (
                            PARTITION BY DW_EK_Postering
                            -- statistic_invoice_header_id is globally unique and
                            -- non-null here (the faktura CTE draws only from
                            -- fct.Faktura, i.e. MySupply headers), so it is a
                            -- deterministic final tiebreaker — the winner is stable
                            -- run-to-run (no DSO-watermark churn via the apply-pass).
                            ORDER BY Header_DW_LoadTimestamp DESC,
                                     statistic_invoice_header_id DESC
                        ) AS rn_fak
                    FROM matched
                )
                SELECT DW_EK_Faktura, DW_EK_Postering, DW_EK_Kreditor, FakturaNr,
                       CASE WHEN rn_post = 1 THEN 1 ELSE 0 END
                           AS is_newest_postering_for_faktura,
                       CASE WHEN rn_fak  = 1 THEN 1 ELSE 0 END
                           AS is_newest_faktura_for_postering
                INTO #ftp FROM ranked;
            """
            )
            context.log.info("Bridge temp table #ftp built.")

            cursor.execute(
                """
                MERGE INTO [ref].[Faktura_To_Postering] AS tgt
                USING #ftp AS src
                ON tgt.DW_EK_Faktura = src.DW_EK_Faktura
                   AND tgt.DW_EK_Postering = src.DW_EK_Postering

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        DW_EK_Faktura, DW_EK_Postering, DW_EK_Kreditor, FakturaNr,
                        is_newest_postering_for_faktura,
                        is_newest_faktura_for_postering,
                        DW_IsDeleted, DW_Timestamp_Insert
                    ) VALUES (
                        src.DW_EK_Faktura, src.DW_EK_Postering,
                        src.DW_EK_Kreditor, src.FakturaNr,
                        src.is_newest_postering_for_faktura,
                        src.is_newest_faktura_for_postering,
                        0, CAST(SYSDATETIME() AS datetime2(0))
                    )

                WHEN MATCHED AND (
                       tgt.is_newest_postering_for_faktura
                           <> src.is_newest_postering_for_faktura
                    OR tgt.is_newest_faktura_for_postering
                           <> src.is_newest_faktura_for_postering
                    OR tgt.DW_IsDeleted = 1
                ) THEN
                    UPDATE SET
                        tgt.is_newest_postering_for_faktura
                            = src.is_newest_postering_for_faktura,
                        tgt.is_newest_faktura_for_postering
                            = src.is_newest_faktura_for_postering,
                        tgt.DW_EK_Kreditor = src.DW_EK_Kreditor,
                        tgt.FakturaNr      = src.FakturaNr,
                        tgt.DW_IsDeleted   = 0,
                        tgt.DW_Timestamp_Update
                            = CAST(SYSDATETIME() AS datetime2(0))

                WHEN NOT MATCHED BY SOURCE AND tgt.DW_IsDeleted = 0 THEN
                    UPDATE SET
                        tgt.DW_IsDeleted        = 1,
                        tgt.DW_Timestamp_Update
                            = CAST(SYSDATETIME() AS datetime2(0));
            """
            )
            merge_affected = cursor.rowcount

            cursor.execute(
                """
                SELECT COUNT(*) AS total_pairs,
                       COUNT(DISTINCT DW_EK_Faktura) AS distinct_faktura,
                       COUNT(DISTINCT DW_EK_Postering) AS distinct_postering
                FROM [ref].[Faktura_To_Postering]
                WHERE DW_IsDeleted = 0
            """
            )
            stats = cursor.fetchone()
            total_pairs = stats[0]
            distinct_faktura = stats[1]
            distinct_postering = stats[2]

            conn.commit()

            context.log.info(
                f"ref.Faktura_To_Postering: {merge_affected} rows touched by MERGE, "
                f"{total_pairs} active pairs "
                f"({distinct_faktura} faktura, {distinct_postering} postering)."
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "merge_affected": merge_affected,
            "total_pairs": total_pairs,
            "distinct_faktura_matched": distinct_faktura,
            "distinct_postering_matched": distinct_postering,
            "table": "ref.Faktura_To_Postering",
        }
    )

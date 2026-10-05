# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ref.FakturaToPostering — fct.Faktura ↔ fct.Postering bridge.

Full crosswalk of matched (dw_ek_Faktura, dw_ek_Postering) pairs with
"is_newest_*" flags for bidirectional FK resolution. Matched on conformed
(dw_ek_Kreditor, faktura_nr). Soft-delete model (dw_is_deleted).

Rebuilt each run (no watermark). The apply-pass asset
(fct_apply_faktura_postering_relations) uses the "newest" rows to stamp
cross-FKs on fct.Faktura and fct.Postering.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_ref",
    compute_kind="mssql",
    deps=[
        AssetKey(["fct_Faktura"]),
        AssetKey(["fct_Postering"]),
        AssetKey(["dim_Faktura"]),  # bridge reads header-identity cols from dim.Faktura
    ],
    tags={"layer": "ref", "module": "erp", "domain": "erp"},
    description=(
        "ref.FakturaToPostering — full crosswalk of fct.Faktura ↔ fct.Postering "
        "matched on (dw_ek_Kreditor, faktura_nr) with newest-selection flags."
    ),
)
def ref_faktura_to_postering(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """Build / refresh ref.FakturaToPostering from fct.Faktura × fct.Postering."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Building ref.FakturaToPostering bridge...")

            cursor.execute(
                """
                DROP TABLE IF EXISTS #ftp;
                WITH posting AS (
                    SELECT p.dw_ek_Postering, p.ref_ledgertrans_id, p.faktura_nr,
                           p.dw_ek_Kreditor, p.dw_haendelsestidspunkt,
                           -- Normalised invoice number — MUST mirror the clean_code_Faktura
                           -- derivation in pre_stg_Fakturalinje_mysupply / sydax_enriched
                           -- ('FAKTURA_' + UPPER(LTRIM(RTRIM(REPLACE(nr,' ',''))))). The raw
                           -- MySupply faktura_nr and SydAX Fakturanummer differ in case/spaces,
                           -- so matching on raw faktura_nr loses almost everything.
                           UPPER(LTRIM(RTRIM(REPLACE(p.faktura_nr, ' ', '')))) AS FakturaNr_norm
                    FROM [fct].[Postering] p
                    WHERE p.dw_ek_Postering <> -1 AND p.dw_ek_Kreditor <> -1
                      AND p.faktura_nr IS NOT NULL
                      AND LTRIM(RTRIM(REPLACE(p.faktura_nr, ' ', ''))) <> ''
                ),
                faktura AS (
                    -- Carry the MySupply header identity from the (persistent) dimension
                    -- so the "newest faktura per postering" ranking uses the real header
                    -- load order. dim.Faktura is 1:1 with fct.Faktura on dw_ek_Faktura;
                    -- LEFT JOIN keeps SydAX-only map rows (NULL header cols sort last).
                    -- NOTE: do NOT join stg.dim_Faktura_mysupply here — it is a volatile
                    -- DROP/CREATE table and an INNER JOIN to it can silently empty the
                    -- crosswalk (see ref-table empty-bridge incident).
                    SELECT f.dw_ek_Faktura, f.dw_ek_Kreditor, f.faktura_nr,
                           d.statistic_invoice_header_id, d.header_dw_timestamp_update,
                           UPPER(LTRIM(RTRIM(REPLACE(f.faktura_nr, ' ', '')))) AS FakturaNr_norm
                    FROM [fct].[Faktura] f
                    LEFT JOIN [dim].[Faktura] d ON d.dw_ek_Faktura = f.dw_ek_Faktura
                    WHERE f.dw_ek_Faktura <> -1 AND f.dw_ek_Kreditor <> -1
                      AND f.faktura_nr IS NOT NULL
                      AND LTRIM(RTRIM(REPLACE(f.faktura_nr, ' ', ''))) <> ''
                ),
                matched AS (
                    SELECT f.dw_ek_Faktura, pg.dw_ek_Postering,
                           f.dw_ek_Kreditor, f.faktura_nr,
                           pg.dw_haendelsestidspunkt, pg.ref_ledgertrans_id,
                           f.header_dw_timestamp_update, f.statistic_invoice_header_id
                    FROM faktura f
                    JOIN posting pg
                      ON pg.dw_ek_Kreditor = f.dw_ek_Kreditor
                     AND pg.FakturaNr_norm = f.FakturaNr_norm   -- normalised, case/space-insensitive
                ),
                ranked AS (
                    SELECT *,
                        -- newest postering per faktura: source-system creation time (discriminating)
                        ROW_NUMBER() OVER (
                            PARTITION BY dw_ek_Faktura
                            ORDER BY dw_haendelsestidspunkt DESC,
                                     ref_ledgertrans_id DESC
                        ) AS rn_post,
                        -- newest faktura per postering: latest MySupply header load
                        -- (header_dw_timestamp_update, statistic_invoice_header_id) — the agreed
                        -- ordering, NOT the insert-order dw_ek_Faktura surrogate.
                        ROW_NUMBER() OVER (
                            PARTITION BY dw_ek_Postering
                            -- statistic_invoice_header_id is globally unique and
                            -- non-null here (the faktura CTE draws only from
                            -- fct.Faktura, i.e. MySupply headers), so it is a
                            -- deterministic final tiebreaker — the winner is stable
                            -- run-to-run (no DSO-watermark churn via the apply-pass).
                            ORDER BY header_dw_timestamp_update DESC,
                                     statistic_invoice_header_id DESC
                        ) AS rn_fak
                    FROM matched
                )
                SELECT dw_ek_Faktura, dw_ek_Postering, dw_ek_Kreditor, faktura_nr,
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
                MERGE INTO [ref].[FakturaToPostering] AS tgt
                USING #ftp AS src
                ON tgt.dw_ek_Faktura = src.dw_ek_Faktura
                   AND tgt.dw_ek_Postering = src.dw_ek_Postering

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        dw_ek_Faktura, dw_ek_Postering, dw_ek_Kreditor, faktura_nr,
                        is_newest_postering_for_faktura,
                        is_newest_faktura_for_postering,
                        dw_is_deleted, dw_timestamp_insert, dw_timestamp_update
                    ) VALUES (
                        src.dw_ek_Faktura, src.dw_ek_Postering,
                        src.dw_ek_Kreditor, src.faktura_nr,
                        src.is_newest_postering_for_faktura,
                        src.is_newest_faktura_for_postering,
                        0,
                        CAST(SYSDATETIME() AS datetime2(0)),
                        CAST(SYSDATETIME() AS datetime2(0))
                    )

                WHEN MATCHED AND (
                       tgt.is_newest_postering_for_faktura
                           <> src.is_newest_postering_for_faktura
                    OR tgt.is_newest_faktura_for_postering
                           <> src.is_newest_faktura_for_postering
                    OR tgt.dw_is_deleted = 1
                ) THEN
                    UPDATE SET
                        tgt.is_newest_postering_for_faktura
                            = src.is_newest_postering_for_faktura,
                        tgt.is_newest_faktura_for_postering
                            = src.is_newest_faktura_for_postering,
                        tgt.dw_ek_Kreditor = src.dw_ek_Kreditor,
                        tgt.faktura_nr      = src.faktura_nr,
                        tgt.dw_is_deleted   = 0,
                        tgt.dw_timestamp_update
                            = CAST(SYSDATETIME() AS datetime2(0))

                WHEN NOT MATCHED BY SOURCE AND tgt.dw_is_deleted = 0 THEN
                    UPDATE SET
                        tgt.dw_is_deleted        = 1,
                        tgt.dw_timestamp_update
                            = CAST(SYSDATETIME() AS datetime2(0));
            """
            )
            merge_affected = cursor.rowcount

            cursor.execute(
                """
                SELECT COUNT(*) AS total_pairs,
                       COUNT(DISTINCT dw_ek_Faktura) AS distinct_faktura,
                       COUNT(DISTINCT dw_ek_Postering) AS distinct_postering
                FROM [ref].[FakturaToPostering]
                WHERE dw_is_deleted = 0
            """
            )
            stats = cursor.fetchone()
            total_pairs = stats[0]
            distinct_faktura = stats[1]
            distinct_postering = stats[2]

            conn.commit()

            context.log.info(
                f"ref.FakturaToPostering: {merge_affected} rows touched by MERGE, "
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
            "table": "ref.FakturaToPostering",
        }
    )

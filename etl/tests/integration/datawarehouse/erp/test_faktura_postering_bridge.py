# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration test: faktura <-> postering bridge (ref.Faktura_To_Postering).

Runs against the full session-materialized ERP pipeline (erp_materialized_session,
which materializes dim_Faktura, fct_Faktura, fct_Postering, ref_faktura_to_postering
and fct_apply_faktura_postering_relations), so the multi-header path is exercised
against real postering data.

Validates the as-built design:
- ref.Faktura_To_Postering is populated (self-contained on persistent tables).
- dim.Faktura persists the bridge tie-break columns (header_dw_timestamp_update,
  statistic_invoice_header_id) for MySupply-enriched rows.
- is_newest_postering_for_faktura ranks by dw_haendelsestidspunkt (source create time).
- is_newest_faktura_for_postering ranks by header load order (header_dw_timestamp_update,
  statistic_invoice_header_id) — NOT the insert-order dw_ek_Faktura surrogate.
- The apply-pass refreshed fct.Postering.dw_ek_Faktura to the bridge's chosen
  'newest' row. (fct.Faktura no longer stores a postering FK — finding 2.2.)
"""

import logging

import pytest

logger = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(5)
class TestFakturaPosteringBridge:
    """Full-session assertions on the faktura<->postering bridge + apply-pass."""

    def test_bridge_populated(self, real_mssql_target_resource, erp_materialized_session):
        """Bridge must have active pairs (proves the self-contained match works)."""
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total_pairs,
                        COUNT(DISTINCT dw_ek_Faktura) AS distinct_faktura,
                        COUNT(DISTINCT dw_ek_Postering) AS distinct_postering
                    FROM [ref].[Faktura_To_Postering]
                    WHERE dw_is_deleted = 0
                """
                )
                total, d_fak, d_post = cursor.fetchone()
                logger.info(
                    f"bridge active pairs={total} distinct_faktura={d_fak} "
                    f"distinct_postering={d_post}"
                )
                assert total > 0, (
                    "ref.Faktura_To_Postering is empty — the (dw_ek_Kreditor, faktura_nr) "
                    "match produced no pairs (the empty-bridge regression)"
                )
            finally:
                cursor.close()

    def test_dim_faktura_header_identity_persisted(
        self, real_mssql_target_resource, erp_materialized_session
    ):
        """dim.Faktura exposes + populates the tie-break columns for MySupply rows."""
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT COL_LENGTH('dim.Faktura', 'header_dw_timestamp_update'),
                           COL_LENGTH('dim.Faktura', 'statistic_invoice_header_id')
                """
                )
                row = cursor.fetchone()
                assert (
                    row[0] is not None and row[1] is not None
                ), "dim.Faktura missing header-identity columns"

                cursor.execute(
                    """
                    SELECT SUM(CASE WHEN header_dw_timestamp_update IS NULL THEN 1 ELSE 0 END),
                           COUNT(*)
                    FROM [dim].[Faktura]
                    WHERE dw_ek_Faktura <> -1 AND faktura_nr IS NOT NULL
                """
                )
                missing, total = cursor.fetchone()
                logger.info(f"dim.Faktura header-ts coverage: {total - (missing or 0)}/{total}")
                assert (
                    missing or 0
                ) == 0, f"{missing} MySupply dim.Faktura rows missing header_dw_timestamp_update"
            finally:
                cursor.close()

    def test_newest_faktura_ranking_is_header_load_order(
        self, real_mssql_target_resource, erp_materialized_session
    ):
        """is_newest_faktura_for_postering must land on the max header-load row."""
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    WITH flagged AS (
                        SELECT b.dw_ek_Postering, d.header_dw_timestamp_update AS ts,
                               d.statistic_invoice_header_id AS hid,
                               b.is_newest_faktura_for_postering AS is_newest
                        FROM [ref].[Faktura_To_Postering] b
                        JOIN [dim].[Faktura] d ON d.dw_ek_Faktura = b.dw_ek_Faktura
                        WHERE b.dw_is_deleted = 0
                    ),
                    chosen AS (
                        SELECT dw_ek_Postering, ts AS chosen_ts, hid AS chosen_hid
                        FROM flagged WHERE is_newest = 1
                    )
                    SELECT COUNT(*)
                    FROM flagged f
                    JOIN chosen c ON c.dw_ek_Postering = f.dw_ek_Postering
                    WHERE f.is_newest = 0
                      AND (
                            f.ts > c.chosen_ts
                         OR (f.ts = c.chosen_ts AND f.hid > c.chosen_hid)
                         OR (c.chosen_ts IS NULL AND f.ts IS NOT NULL)
                      )
                """
                )
                violations = cursor.fetchone()[0]
                assert (
                    violations == 0
                ), f"{violations} postering group(s) flag a non-newest header as newest"
            finally:
                cursor.close()

    def test_newest_postering_ranking_is_create_time(
        self, real_mssql_target_resource, erp_materialized_session
    ):
        """is_newest_postering_for_faktura must land on the max dw_haendelsestidspunkt row."""
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    WITH flagged AS (
                        SELECT b.dw_ek_Faktura, p.dw_haendelsestidspunkt AS ts,
                               p.ref_ledgertrans_id AS rid,
                               b.is_newest_postering_for_faktura AS is_newest
                        FROM [ref].[Faktura_To_Postering] b
                        JOIN [fct].[Postering] p ON p.dw_ek_Postering = b.dw_ek_Postering
                        WHERE b.dw_is_deleted = 0
                    ),
                    chosen AS (
                        SELECT dw_ek_Faktura, ts AS chosen_ts, rid AS chosen_rid
                        FROM flagged WHERE is_newest = 1
                    )
                    SELECT COUNT(*)
                    FROM flagged f
                    JOIN chosen c ON c.dw_ek_Faktura = f.dw_ek_Faktura
                    WHERE f.is_newest = 0
                      AND (
                            f.ts > c.chosen_ts
                         OR (f.ts = c.chosen_ts AND f.rid > c.chosen_rid)
                         OR (c.chosen_ts IS NULL AND f.ts IS NOT NULL)
                      )
                """
                )
                violations = cursor.fetchone()[0]
                assert (
                    violations == 0
                ), f"{violations} faktura group(s) flag a non-newest postering as newest"
            finally:
                cursor.close()

    def test_apply_pass_refreshed_cross_fks(
        self, real_mssql_target_resource, erp_materialized_session
    ):
        """fct cross-FKs must equal the bridge's chosen newest row (apply-pass ran)."""
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # fct.Faktura no longer stores dw_ek_Postering (finding 2.2): the
                # newest posting is exposed via vw_fct.Faktura, not a stored FK, so
                # there is no fct.Faktura cross-FK to assert here.
                # fct.Postering.dw_ek_Faktura must equal the newest-faktura bridge row.
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM [fct].[Postering] p
                    JOIN [ref].[Faktura_To_Postering] b
                      ON b.dw_ek_Postering = p.dw_ek_Postering
                     AND b.is_newest_faktura_for_postering = 1
                     AND b.dw_is_deleted = 0
                    WHERE p.dw_ek_Faktura <> b.dw_ek_Faktura
                """
                )
                p_unhealed = cursor.fetchone()[0]
                assert p_unhealed == 0, (
                    f"{p_unhealed} fct.Postering rows have dw_ek_Faktura != bridge newest "
                    f"(apply-pass did not refresh)"
                )
            finally:
                cursor.close()

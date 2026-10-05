# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Self-contained logic test for the faktura<->postering bridge + apply-pass.

Unlike test_faktura_postering_bridge.py (which needs the full ERP source data via
erp_materialized_session), this test seeds the minimal persistent rows
(dim.Faktura + fct.Faktura + fct.Postering) directly and invokes the REAL assets
(ref_faktura_to_postering, fct_apply_faktura_postering_relations). It therefore
runs fully locally against the test MSSQL container without any MySupply/SydAX
extract data.

Adversarial scenario — the lower dw_ek_Faktura surrogate carries the NEWER header
load time, so the "newest faktura per postering" tie-break can only land correctly
if it ranks by header_dw_timestamp_update (the fix) and NOT by the insert-order
dw_ek_Faktura surrogate (the regression):

    header 101 : header_dw_timestamp_update = 2026-02-01  (NEWER)  <- must win
    header 102 : header_dw_timestamp_update = 2026-01-01  (older)
    both share (dw_ek_Kreditor=7, faktura_nr='INV1')

    posting 201 : dw_haendelsestidspunkt = 2026-03-01
    posting 202 : dw_haendelsestidspunkt = 2026-04-01  (NEWER) <- must win
"""

import logging

import pytest
from dagster import build_asset_context
from DSA.SQL.fct.apply_faktura_postering_relations import (
    fct_apply_faktura_postering_relations,
)
from DSA.SQL.ref.faktura_to_postering import ref_faktura_to_postering

logger = logging.getLogger(__name__)

_TABLES = [
    "[ref].[Faktura_To_Postering]",
    "[fct].[Faktura]",
    "[fct].[Postering]",
    "[dim].[Faktura]",
]


def _wipe(cursor):
    for t in _TABLES:
        cursor.execute(f"DELETE FROM {t}")


def _seed(cursor):
    cursor.execute(
        "INSERT INTO [dim].[Faktura]"
        "(dw_ek_Faktura, faktura_nr, leverandoer_cvr, statistic_invoice_header_id, header_dw_timestamp_update) "
        "VALUES (101, 'INV1', 'CVR9', 5, '2026-02-01'), "
        "       (102, 'INV1', 'CVR9', 9, '2026-01-01')"
    )
    cursor.execute(
        "INSERT INTO [fct].[Faktura]"
        "(dw_ek_Faktura, dw_ek_Kreditor, faktura_nr) "
        "VALUES (101, 7, 'INV1'), (102, 7, 'INV1')"
    )
    cursor.execute(
        "INSERT INTO [fct].[Postering]"
        "(ref_ledgertrans_id, dw_ek_Postering, dw_ek_Kreditor, dw_ek_Faktura, faktura_nr, dw_haendelsestidspunkt) "
        "VALUES (1001, 201, 7, -1, 'INV1', '2026-03-01'), "
        "       (1002, 202, 7, -1, 'INV1', '2026-04-01')"
    )


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(6)
class TestFakturaPosteringBridgeLogic:
    """Run the real bridge + apply-pass assets against a seeded adversarial scenario."""

    @pytest.fixture
    def seeded(self, real_mssql_target_resource, setup_erp_infrastructure):
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                _wipe(cursor)
                _seed(cursor)
                conn.commit()
            finally:
                cursor.close()
        yield real_mssql_target_resource
        # teardown — leave the DB clean
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                _wipe(cursor)
                conn.commit()
            finally:
                cursor.close()

    def test_bridge_ranks_and_applies(self, seeded):
        res = seeded

        bridge_meta = ref_faktura_to_postering(
            build_asset_context(), mssql_target_staging=res
        ).metadata
        assert bridge_meta["total_pairs"] == 4, "expected full 2x2 crosswalk"
        assert bridge_meta["distinct_faktura_matched"] == 2
        assert bridge_meta["distinct_postering_matched"] == 2

        fct_apply_faktura_postering_relations(build_asset_context(), mssql_target_staging=res)

        with res.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # newest faktura per postering = header 101 (newer header_dw_timestamp_update,
                # despite the LOWER surrogate). The insert-order proxy would pick 102.
                cursor.execute(
                    """
                    SELECT dw_ek_Faktura
                    FROM [ref].[Faktura_To_Postering]
                    WHERE is_newest_faktura_for_postering = 1
                    GROUP BY dw_ek_Faktura
                """
                )
                newest_fakturas = {r[0] for r in cursor.fetchall()}
                assert newest_fakturas == {101}, (
                    f"newest-faktura must be header 101 (newer load ts); got {newest_fakturas} "
                    f"— ranking fell back to the insert-order surrogate"
                )

                # newest postering per faktura = posting 202 (max dw_haendelsestidspunkt)
                cursor.execute(
                    """
                    SELECT dw_ek_Postering
                    FROM [ref].[Faktura_To_Postering]
                    WHERE is_newest_postering_for_faktura = 1
                    GROUP BY dw_ek_Postering
                """
                )
                newest_posterings = {r[0] for r in cursor.fetchall()}
                assert newest_posterings == {
                    202
                }, f"newest-postering must be 202 (max create time); got {newest_posterings}"

                # cross-FK applied to fct.Postering. (The reverse link is no longer
                # stored on fct.Faktura — finding 2.2 — it's exposed via vw_fct.)
                cursor.execute("SELECT DISTINCT dw_ek_Faktura FROM [fct].[Postering]")
                assert {r[0] for r in cursor.fetchall()} == {101}
            finally:
                cursor.close()

    def test_apply_pass_is_idempotent(self, seeded):
        res = seeded
        ref_faktura_to_postering(build_asset_context(), mssql_target_staging=res)
        fct_apply_faktura_postering_relations(build_asset_context(), mssql_target_staging=res)
        # second apply must be a no-op (refresh/demote only-on-change guards)
        meta2 = fct_apply_faktura_postering_relations(
            build_asset_context(), mssql_target_staging=res
        ).metadata
        assert meta2["postering_rows_refreshed"] == 0
        assert meta2["postering_rows_demoted"] == 0

    def test_matches_across_case_and_space_differences(
        self, real_mssql_target_resource, setup_erp_infrastructure
    ):
        """faktura_nr differing only by case/spaces across systems must still match.

        MySupply and SydAX carry the raw invoice number with different casing/spacing;
        the bridge normalises (UPPER + strip spaces) to mirror the clean_code_Faktura key.
        """
        res = real_mssql_target_resource
        with res.get_connection() as conn:
            cursor = conn.cursor()
            try:
                _wipe(cursor)
                # faktura header carries 'iNv 1' (lower + space); postering carries 'INV1'
                cursor.execute(
                    "INSERT INTO [dim].[Faktura]"
                    "(dw_ek_Faktura, faktura_nr, statistic_invoice_header_id, header_dw_timestamp_update) "
                    "VALUES (201, 'iNv 1', 7, '2026-01-01')"
                )
                cursor.execute(
                    "INSERT INTO [fct].[Faktura](dw_ek_Faktura, dw_ek_Kreditor, faktura_nr) "
                    "VALUES (201, 7, 'iNv 1')"
                )
                cursor.execute(
                    "INSERT INTO [fct].[Postering]"
                    "(ref_ledgertrans_id, dw_ek_Postering, dw_ek_Kreditor, dw_ek_Faktura, faktura_nr, dw_haendelsestidspunkt) "
                    "VALUES (3001, 301, 7, -1, 'INV1', '2026-03-01')"
                )
                conn.commit()
            finally:
                cursor.close()
        try:
            meta = ref_faktura_to_postering(
                build_asset_context(), mssql_target_staging=res
            ).metadata
            assert (
                meta["total_pairs"] == 1
            ), "normalised faktura_nr ('iNv 1' vs 'INV1') should produce one matched pair"
        finally:
            with res.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    _wipe(cursor)
                    conn.commit()
                finally:
                    cursor.close()

    def test_stale_fk_demoted_when_match_disappears(self, seeded):
        """When a bridge pairing is retracted, the apply-pass resets the cross-FK to -1."""
        res = seeded
        ref_faktura_to_postering(build_asset_context(), mssql_target_staging=res)
        fct_apply_faktura_postering_relations(build_asset_context(), mssql_target_staging=res)

        # Simulate the match disappearing: soft-delete the whole bridge.
        with res.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("UPDATE [ref].[Faktura_To_Postering] SET dw_is_deleted = 1")
                conn.commit()
            finally:
                cursor.close()

        meta = fct_apply_faktura_postering_relations(
            build_asset_context(), mssql_target_staging=res
        ).metadata
        assert meta["postering_rows_demoted"] >= 1

        with res.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT DISTINCT dw_ek_Faktura FROM [fct].[Postering]")
                assert {r[0] for r in cursor.fetchall()} == {-1}, "stale faktura FK not demoted"
            finally:
                cursor.close()

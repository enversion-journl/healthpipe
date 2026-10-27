# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contract tests for the entity-delta hub.

The hub's correctness properties: (1) get_checkpoints never returns NULL —
a consumer checkpoint missing from DFM resolves to BEGINNING_OF_TIME so the
axis covers the full archive picture; (2) each axis is the chronological MIN
of its consumers' checkpoints; (3) checkpoint filters land on every source
branch of the axis; (4) the lev-change branch pulls ALL of a changed
supplier's entities; (5) the final entity list carries no dedup-winner logic.
"""

import pytest

pytestmark = pytest.mark.unit

from DSA.SQL.staging.pre_stg_Leverandoervare_ILSVare_delta_entiteter import (
    BEGINNING_OF_TIME,
    _build_hub_sql,
    get_checkpoints,
)
from utils.sql.leverandoervare_ilsvare_sql import LAFTDSTAM_ALT_LVNR_SLOTS


class _StubDfm:
    """Minimal DfmManager-stand-in: serverer checkpoints fra et dict."""

    def __init__(self, checkpoints):
        self._checkpoints = checkpoints

    def get_checkpoint(self, schema, table):
        return self._checkpoints.get((schema, table))


class _StubLog:
    def info(self, *args, **kwargs):
        pass


class _StubContext:
    log = _StubLog()


class TestGetCheckpoints:
    def test_all_present_takes_min_per_axis(self):
        dfm = _StubDfm(
            {
                ("dim", "Leverandoervare"): "2026-07-05",
                ("dim", "ILSVare"): "2026-07-03",
                ("bridge", "LeverandoervareILSVare"): "2026-07-04",
            }
        )
        checkpoint_ilsvare, checkpoint_vare = get_checkpoints(dfm, _StubContext())
        assert checkpoint_ilsvare == "2026-07-03"  # MIN(dim.ILSVare, bridge)
        assert checkpoint_vare == "2026-07-04"  # MIN(dim.Leverandoervare, bridge)

    def test_missing_dfm_checkpoint_resolves_to_beginning_of_time(self):
        dfm = _StubDfm(
            {
                ("dim", "Leverandoervare"): "2026-07-05",
                ("dim", "ILSVare"): None,
                ("bridge", "LeverandoervareILSVare"): "2026-07-04",
            }
        )
        checkpoint_ilsvare, checkpoint_vare = get_checkpoints(dfm, _StubContext())
        assert checkpoint_ilsvare == BEGINNING_OF_TIME
        assert checkpoint_vare == "2026-07-04"

    def test_never_returns_none(self):
        dfm = _StubDfm({})
        checkpoint_ilsvare, checkpoint_vare = get_checkpoints(dfm, _StubContext())
        assert checkpoint_ilsvare == BEGINNING_OF_TIME
        assert checkpoint_vare == BEGINNING_OF_TIME

    def test_missing_bridge_checkpoint_pulls_both_axes_to_beginning_of_time(self):
        dfm = _StubDfm(
            {
                ("dim", "Leverandoervare"): "2026-07-05",
                ("dim", "ILSVare"): "2026-07-03",
                ("bridge", "LeverandoervareILSVare"): None,
            }
        )
        checkpoint_ilsvare, checkpoint_vare = get_checkpoints(dfm, _StubContext())
        assert checkpoint_ilsvare == BEGINNING_OF_TIME
        assert checkpoint_vare == BEGINNING_OF_TIME


class TestHubSql:
    def test_axis_checkpoints_stay_on_their_own_branches(self):
        # Kontrakten er akse-separation, ikke et bestemt grenantal: Vare-cp
        # maa kun filtrere vare-grene, ILSVare-cp kun ILSVare-grene. (At ingen
        # gren mangler sit filter daekkes af integrationstestens delta-no-op.)
        sql = _build_hub_sql("2026-07-01", "2026-07-03")
        vare_section, ilsvare_section = sql.split("ilsvare_entities AS (")
        assert "> '2026-07-01'" in vare_section
        assert "> '2026-07-01'" not in ilsvare_section
        assert "> '2026-07-03'" in ilsvare_section
        assert "> '2026-07-03'" not in vare_section

    def test_item_master_changes_stay_on_ilsvare_axis(self):
        # dim.Leverandoervare baerer ingen kopi af ILS-varestamdata (navne bor paa
        # dim.ILSVare og naas via bridgen), saa ils_vare-aendringer maa IKKE
        # producere vare-entiteter — kun ILSVare-aksen laeser ils_vare.
        sql = _build_hub_sql("2026-07-01", BEGINNING_OF_TIME)
        assert "iv.vnr = p.raa_vnr" not in sql
        assert "raa_vnr" not in sql

    def test_filters_always_present(self):
        # Ingen NULL-semantik: begge akser filtrerer altid — BEGINNING_OF_TIME
        # giver det fulde arkivbillede.
        sql = _build_hub_sql(BEGINNING_OF_TIME, BEGINNING_OF_TIME)
        vare_section, ilsvare_section = sql.split("ilsvare_entities AS (")
        assert f"> '{BEGINNING_OF_TIME}'" in vare_section
        assert f"> '{BEGINNING_OF_TIME}'" in ilsvare_section

    def test_lev_change_restages_all_supplier_pairs(self):
        # Et leverandoer-skift (fx nyt CVR) skal restage ALLE leverandoerens
        # entiteter: ils_pairs baerer lev_dw_timestamp_update fra lev_current,
        # og vare-grenen OR-filtrerer paa den.
        sql = _build_hub_sql("2026-07-01", BEGINNING_OF_TIME)
        assert "OR lev_dw_timestamp_update > '2026-07-01'" in sql
        assert "lev.lev_dw_timestamp_update" in sql  # foert med i alle fem grene
        assert "INNER JOIN [arc].[ils_lev] lev ON lev.lnr = p.lnr" not in sql

    def test_output_table(self):
        sql = _build_hub_sql(BEGINNING_OF_TIME, BEGINNING_OF_TIME)
        assert "INTO [stg].[pre_Leverandoervare_ILSVare_delta_entiteter]" in sql
        # ROW_NUMBER er tilladt opstroems (lev_current: "hvilken CVR gaelder lige nu
        # for denne leverandoer" — samme etablerede moenster som det oevrige flow),
        # men den endelige entitetsliste (SELECT INTO ... FROM (...) u) maa ikke
        # indeholde dedup-vinder-logik.
        output_select = sql[sql.index("SELECT *\n") :]
        assert (
            "ROW_NUMBER" not in output_select
        ), "hub er en entitetsliste — ingen dedup-vinder-logik i output-selectet"


def test_hub_includes_laftdstam_alternate_lvnr_slots():
    """laftdstam lvnr##2-4 skal generere Vare-entiteter."""
    sql = _build_hub_sql("2026-01-01", "2026-01-01")
    for slot in LAFTDSTAM_ALT_LVNR_SLOTS:
        assert f"alt_s.[{slot}]" in sql, f"laftdstam-alternativ {slot} mangler i hub'en"


def test_hub_excludes_laftd_lvnr_columns():
    """laftds lvnr-kolonner er legacy og maa ikke seede Vare-aksen — vnr-aksen
    (ILSVare) beholder laftd."""
    sql = _build_hub_sql("2026-01-01", "2026-01-01")
    assert "ld.[lvnr##1]" not in sql
    assert "alt_d" not in sql
    assert "CAST(ld.vnr AS NVARCHAR(100))" in sql

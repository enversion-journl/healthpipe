# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""E2E for the DRG group-key layer: code dims + group interning + group bridges
+ the group FKs on fct.DRGKontakt.

Two somatik contacts with the SAME diagnosis/procedure set must share one group
key (the whole point of the redesign); a contact with no codes resolves to the
-1 empty group; group bridges are append-only; a contact that loses all its
codes has its fct group FK refreshed back to -1. Requires
USE_REAL_DB_FOR_TESTS=true and ENABLE_TEST_WRITES=true.
"""

import dagster as dg
import pytest
from core.dfm_config import RunModeConfig
from DSA.SQL.bridge.bridge_DRGDiagnoseGruppe import bridge_DRGDiagnoseGruppe
from DSA.SQL.bridge.bridge_DRGProcedureGruppe import bridge_DRGProcedureGruppe
from DSA.SQL.dim.dim_DRGDiagnose import dim_DRGDiagnose
from DSA.SQL.dim.dim_DRGProcedure import dim_DRGProcedure
from DSA.SQL.fct.fct_DRGKontakt import fct_DRGKontakt
from DSA.SQL.map.map_DRGDiagnose import map_DRGDiagnose
from DSA.SQL.map.map_DRGDiagnoseGruppe import map_DRGDiagnoseGruppe
from DSA.SQL.map.map_DRGKontakt import map_DRGKontakt
from DSA.SQL.map.map_DRGKontaktKlassifikation import map_DRGKontaktKlassifikation
from DSA.SQL.map.map_DRGProcedure import map_DRGProcedure
from DSA.SQL.map.map_DRGProcedureGruppe import map_DRGProcedureGruppe
from DSA.SQL.staging.stg_bridge_DRGKontaktDiagnose import stg_bridge_DRGKontaktDiagnose
from DSA.SQL.staging.stg_bridge_DRGKontaktProcedure import stg_bridge_DRGKontaktProcedure
from DSA.SQL.staging.stg_DRGDiagnoseGruppe import stg_DRGDiagnoseGruppe
from DSA.SQL.staging.stg_DRGProcedureGruppe import stg_DRGProcedureGruppe
from tests.integration.test_drg_staging_e2e import (  # noqa: F401
    SOR_KENDT,
    UUID_A,
    UUID_B,
    _koer_kaeden,
    require_real_environment,
    run_sql,
    scalar,
    staging_db,
)

pytestmark = pytest.mark.integration

_TABLES = (
    "[fct].[DRGKontakt]",
    "[bridge].[DRGDiagnoseGruppe]",
    "[bridge].[DRGProcedureGruppe]",
    "[map].[DRGDiagnoseGruppe]",
    "[map].[DRGProcedureGruppe]",
    "[dim].[DRGDiagnose]",
    "[dim].[DRGProcedure]",
    "[map].[DRGDiagnose]",
    "[map].[DRGProcedure]",
    "[arc].[drg_diagnose]",
    "[arc].[drg_diagnose_tillaeg]",
    "[arc].[drg_procedure]",
    "[arc].[drg_procedure_tillaeg]",
)


def _diagnose(db, uuid, kode, art, side):
    run_sql(
        db,
        "INSERT INTO [arc].[drg_diagnose] ([takstsystem],[kontakt_uuid],[DIAGNOSE],[ART],"
        "[SIDEANGIVELSE],[DIAGNOSE_VAERDI],[Aktivitetsaar],[leverance_dato],"
        "[dw_timestamp_insert],[dw_timestamp_update],[dw_is_deleted]) "
        "VALUES ('T26',?,?,?,?,'', '2025', '2026-08-24', SYSDATETIME(), SYSDATETIME(), 0)",
        uuid,
        kode,
        art,
        side,
    )


def _diagnose_tillaeg(db, uuid, kode, art, side, tillaeg):
    run_sql(
        db,
        "INSERT INTO [arc].[drg_diagnose_tillaeg] ([takstsystem],[kontakt_uuid],[DIAGNOSE],[ART],"
        "[SIDEANGIVELSE],[TILLAEGSKODE_DIAGNOSE],[Aktivitetsaar],[leverance_dato],"
        "[dw_timestamp_insert],[dw_timestamp_update],[dw_is_deleted]) "
        "VALUES ('T26',?,?,?,?,?, '2025', '2026-08-24', SYSDATETIME(), SYSDATETIME(), 0)",
        uuid,
        kode,
        art,
        side,
        tillaeg,
    )


def _procedure(db, uuid, pvaerdi, kode, sor):
    run_sql(
        db,
        "INSERT INTO [arc].[drg_procedure] ([takstsystem],[kontakt_uuid],[PROCEDURE],"
        "[SIDEANGIVELSE],[HANDLINGSSPEC],[KONTRAST],[PROCEDURE_STARTTIDSPUNKT],"
        "[PROCEDURE_SLUTTIDSPUNKT],[PRODUCERENDE_ENHED],[PRODUCERENDE_SYGEHUS],"
        "[PROCEDURE_BETALINGSAFTALE],[PROCEDURE_VAERDI],[Aktivitetsaar],[leverance_dato],"
        "[dw_timestamp_insert],[dw_timestamp_update],[dw_is_deleted]) "
        "VALUES ('T26',?,?,'','','','2025-03-01 10:00:00','2025-03-01 11:00:00',?,'', '', ?, '2025', "
        "'2026-08-24', SYSDATETIME(), SYSDATETIME(), 0)",
        uuid,
        kode,
        sor,
        pvaerdi,
    )


@pytest.fixture(scope="module", autouse=True)
def bridge_db(staging_db):  # noqa: F811
    """staging + fresh code/group/fct tables + arc children where A and B share
    an identical diagnosis and procedure set (so they share one group)."""
    for t in _TABLES:
        if t.startswith("[map]") or t.startswith("[dim]"):
            noegle = "dw_ek_" + t.split("[")[-1].rstrip("]")
            run_sql(staging_db, f"DELETE FROM {t} WHERE [{noegle}] <> -1")
        elif t.startswith("[bridge]"):
            noegle = "dw_ek_" + t.split("[")[-1].rstrip("]")
            run_sql(staging_db, f"DELETE FROM {t} WHERE [{noegle}] <> -1")
        else:
            run_sql(staging_db, f"DELETE FROM {t}")
    _koer_kaeden(staging_db)

    # A and B get the IDENTICAL diagnosis set {DZ01(A,''), DB02(B,'H')+tillaeg}
    for uuid in (UUID_A, UUID_B):
        _diagnose(staging_db, uuid, "DZ01", "A", "")
        _diagnose(staging_db, uuid, "DB02", "B", "H")
        _diagnose_tillaeg(staging_db, uuid, "DB02", "B", "H", "TUL2")
        _diagnose_tillaeg(staging_db, uuid, "DB02", "B", "H", "TUL1")  # unsorted
    # ...and the IDENTICAL procedure set {KAA10@SOR, KBB20@SOR}
    for uuid in (UUID_A, UUID_B):
        _procedure(staging_db, uuid, f"PV-{uuid[:4]}-1", "KAA10", SOR_KENDT)
        _procedure(staging_db, uuid, f"PV-{uuid[:4]}-2", "KBB20", SOR_KENDT)
    return staging_db


def _koer(res):
    cfg = RunModeConfig()
    for fn in (
        map_DRGKontakt,
        map_DRGKontaktKlassifikation,
        stg_bridge_DRGKontaktDiagnose,
        stg_bridge_DRGKontaktProcedure,
        map_DRGDiagnose,
        map_DRGProcedure,
        dim_DRGDiagnose,
        dim_DRGProcedure,
        stg_DRGDiagnoseGruppe,
        stg_DRGProcedureGruppe,
        map_DRGDiagnoseGruppe,
        map_DRGProcedureGruppe,
        bridge_DRGDiagnoseGruppe,
        bridge_DRGProcedureGruppe,
        fct_DRGKontakt,
    ):
        fn(dg.build_asset_context(), res, cfg)


def _gruppe(db, uuid, kind):
    """The contact's group FK on the fact (via clean_code_DRGKontakt LIKE uuid)."""
    return scalar(
        db,
        f"SELECT f.[dw_ek_{kind}Gruppe] FROM [fct].[DRGKontakt] f "
        "WHERE f.[clean_code_DRGKontakt] LIKE '%' + ?",
        uuid,
    )


def test_1_kode_dimension_dedupliceres(bridge_db):
    _koer(bridge_db)
    # DZ01 + DB02 = 2 distinkte diagnose-medlemmer (delt af A og B)
    assert (
        scalar(bridge_db, "SELECT COUNT(*) FROM [dim].[DRGDiagnose] WHERE dw_ek_DRGDiagnose<>-1")
        == 2
    )
    assert (
        scalar(bridge_db, "SELECT COUNT(*) FROM [dim].[DRGProcedure] WHERE dw_ek_DRGProcedure<>-1")
        == 2
    )


def test_2_ens_saet_deler_én_gruppe(bridge_db):
    # A og B har identisk diagnosesæt -> ét gruppemedlem (ud over seed)
    assert (
        scalar(
            bridge_db,
            "SELECT COUNT(*) FROM [map].[DRGDiagnoseGruppe] WHERE dw_ek_DRGDiagnoseGruppe<>-1",
        )
        == 1
    )
    assert (
        scalar(
            bridge_db,
            "SELECT COUNT(*) FROM [map].[DRGProcedureGruppe] WHERE dw_ek_DRGProcedureGruppe<>-1",
        )
        == 1
    )
    # og fct'ens gruppe-FK er den samme for A og B, og > -1
    ga, gb = _gruppe(bridge_db, UUID_A, "DRGDiagnose"), _gruppe(bridge_db, UUID_B, "DRGDiagnose")
    assert ga == gb and ga > 0
    pa, pb = _gruppe(bridge_db, UUID_A, "DRGProcedure"), _gruppe(bridge_db, UUID_B, "DRGProcedure")
    assert pa == pb and pa > 0


def test_3_gruppe_bridge_folder_ud_i_koder(bridge_db):
    # diagnose-gruppen har præcis 2 medlemmer (DZ01, DB02)
    medlemmer = scalar(
        bridge_db,
        "SELECT COUNT(*) FROM [bridge].[DRGDiagnoseGruppe] WHERE dw_ek_DRGDiagnoseGruppe<>-1",
    )
    assert medlemmer == 2
    # tillæg sorteret i kode-dimensionsnøglen
    tillaeg = scalar(
        bridge_db, "SELECT [tillaegskoder] FROM [dim].[DRGDiagnose] WHERE [diagnose_kode]='DB02'"
    )
    assert tillaeg == "TUL1;TUL2"
    # procedure-gruppens medlemmer har producerende enhed opløst (ikke -1)
    ukendt = scalar(
        bridge_db,
        "SELECT COUNT(*) FROM [bridge].[DRGProcedureGruppe] "
        "WHERE dw_ek_DRGProcedureGruppe<>-1 AND dw_ek_Organisation_producerende=-1",
    )
    assert ukendt == 0


def test_4_kontakt_uden_koder_faar_tom_gruppe(bridge_db):
    # PSY_A (psyk-kontakt uden diagnose/procedure-børn) -> -1 empty group
    from tests.integration.test_drg_staging_e2e import PSY_A

    assert _gruppe(bridge_db, PSY_A, "DRGDiagnose") == -1
    assert _gruppe(bridge_db, PSY_A, "DRGProcedure") == -1


def test_5_gruppe_bridge_er_append_only_ved_genkoersel(bridge_db):
    foer = scalar(bridge_db, "SELECT COUNT(*) FROM [bridge].[DRGDiagnoseGruppe]")
    _koer(bridge_db)  # genkør hele kæden
    assert scalar(bridge_db, "SELECT COUNT(*) FROM [bridge].[DRGDiagnoseGruppe]") == foer


def test_6_retraktion_saetter_fct_gruppe_til_tom(bridge_db):
    # fjern ALLE B's diagnoser -> B's fct diagnose-gruppe skal blive -1 (tom),
    # A's uændret. Delta-refresh via scope-tabellen.
    ga_foer = _gruppe(bridge_db, UUID_A, "DRGDiagnose")
    run_sql(
        bridge_db,
        "UPDATE [arc].[drg_diagnose] SET [dw_is_deleted]=1, [dw_timestamp_update]=SYSDATETIME() "
        "WHERE [kontakt_uuid]=?",
        UUID_B,
    )
    _koer(bridge_db)
    assert _gruppe(bridge_db, UUID_B, "DRGDiagnose") == -1
    assert _gruppe(bridge_db, UUID_A, "DRGDiagnose") == ga_foer  # A uændret


def test_7_kryds_type_delta_bevarer_diagnose_gruppe(bridge_db):
    # FINDING-lock: en delta hvor kun A's HEADER ændrer sig (A rammer fct-MERGE-
    # UPDATE) må IKKE klobbe A's diagnose-gruppe til -1, når A's diagnoser er
    # uden for diagnose-delta-scopen. Gruppe-FK'erne ejes af refreshen, ikke MERGE.
    diag_foer = _gruppe(bridge_db, UUID_A, "DRGDiagnose")
    assert diag_foer > 0
    run_sql(
        bridge_db,
        "UPDATE [arc].[drg_kontakt] SET [TOTALPRIS_DRG] = '4242', "
        "[dw_timestamp_update] = SYSDATETIME() WHERE [KONTAKT_VAERDI] = ?",
        UUID_A,
    )
    _koer_kaeden(bridge_db)  # genopbyg stg.fct med A's nye header
    _koer(bridge_db)  # gruppe-kæde (delta) + fct MERGE
    assert _gruppe(bridge_db, UUID_A, "DRGDiagnose") == diag_foer  # bevaret, ikke -1


def test_8_ingen_cpr_i_gruppe_bridge_eller_dim(bridge_db):
    n = scalar(
        bridge_db,
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA IN ('bridge','dim','map') "
        "AND TABLE_NAME IN ('DRGDiagnoseGruppe','DRGProcedureGruppe','DRGDiagnose','DRGProcedure') "
        "AND COLUMN_NAME LIKE '%cpr%'",
    )
    assert n == 0

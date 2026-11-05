# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""E2E-integrationstest for DRG-ingestion mod devcontainer-MSSQL.

Koerer hele kaeden med syntetiske tar.bz2-leverancer:

    DDL (tables/drg) -> extract (tar -> ext) -> arkiv (view -> MERGE -> arc)

og verificerer de tre semantikker, der baerer designet:

1. AARSSKIVE-SCOPED DELETE-DETECTION: en U-leverance daekker faerre
   aktivitetsaar end en M-leverance — aaret, der ikke er med, maa IKKE
   soft-slettes (pr. (takstsystem, aktivitetsaar), jf. beslutningsnotatet).
2. REVISION + SOFT-DELETE: prisaendringer logges til arc.drg_pris_revision
   FOER mergen; kontakter der udgaar af leverancen soft-slettes inden for
   scope; genkoersel af samme leverance er en no-op.
3. RETNINGSGUARD: en aeldre leverance maa ikke merges oven i en nyere.

Testene deler modul-scoped DB-state og koerer i definitionsraekkefoelge.
Kraever USE_REAL_DB_FOR_TESTS=true og ENABLE_TEST_WRITES=true (devcontainer).
"""

import csv
import io
import os
import tarfile

import dagster as dg
import pytest
from core.resources import FileShareResource
from DSI.csv_files import drg, drg_archive
from DSI.csv_files.drg_datasets import DATASETS, PSYK_DATASETS, SOMATIK_DATASETS

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def require_real_environment():
    use_real = os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() == "true"
    writes = os.getenv("ENABLE_TEST_WRITES", "false").lower() == "true"
    if not (use_real and writes):
        pytest.skip("kraever USE_REAL_DB_FOR_TESTS=true og ENABLE_TEST_WRITES=true")


DATASETS_BY_PREFIX = {d.source_prefix: d for d in SOMATIK_DATASETS}
KONTAKT = DATASETS_BY_PREFIX["DRGKONTAKT"]

UUID_A = "AAAAAAAA-0000-0000-0000-000000000001"
UUID_B = "BBBBBBBB-0000-0000-0000-000000000002"
UUID_C = "CCCCCCCC-0000-0000-0000-000000000003"
PV_1 = "EEEEEEEE-0000-0000-0000-000000000001"
PV_2 = "EEEEEEEE-0000-0000-0000-000000000002"


# ---------------------------------------------------------------------------
# Syntetiske leverancer
# ---------------------------------------------------------------------------


def _row(dataset, **values) -> list[str]:
    unknown = set(values) - set(dataset.source_columns)
    assert not unknown, f"{dataset.ext_table}: ukendte testkolonner {unknown}"
    return [values.get(column, "") for column in dataset.source_columns]


def _csv_bytes(dataset, rows: list[list[str]]) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=";", quoting=csv.QUOTE_ALL)
    writer.writerow(list(dataset.source_columns))
    writer.writerows(rows)
    return buffer.getvalue().encode("cp1252")


def kontakt_row(uuid: str, kontakt_id: str, forloeb_id: str, aar: str, pris: str) -> list[str]:
    return _row(
        KONTAKT,
        ID=kontakt_id,
        DRGSGHFORLOB_ID=forloeb_id,
        KONTAKT_VAERDI=uuid,
        FORLOB_VAERDI=f"HF-{kontakt_id}",
        AKTIVITETSAAR=aar,
        DRG="01MA01",
        MDC="01",
        PRIS_DRG=pris,
        TOTALPRIS_DRG=pris,
        TUNGESTE_DRGKONTAKT="1",
        UDFALDSGIVENDE_KONTAKT="1",
    )


def forloeb_row(forloeb_id: str, ind_id: str, ud_id: str, aar: str, pris: str) -> list[str]:
    return _row(
        DATASETS_BY_PREFIX["DRGSGHFORLOB"],
        ID=forloeb_id,
        DRGKONTAKT_ID_IND=ind_id,
        DRGKONTAKT_ID_UD=ud_id,
        DRG_KONTAKT_ID_DRGSGHF=ind_id,
        AKTIVITETSAAR=aar,
        ANTAL_I_FORLOB="1",
        PRIS_DRG_DRGSGHF=pris,
        TOTALPRIS_DRGSGHF=pris,
    )


def diagnose_row(diagnose_id: str, kontakt_id: str, kode: str, aar: str) -> list[str]:
    return _row(
        DATASETS_BY_PREFIX["DIAGNOSER"],
        ID=diagnose_id,
        DRGKONTAKT_ID=kontakt_id,
        DIAGNOSE=kode,
        ART="A",
        Aktivitetsaar=aar,
    )


def procedure_row(procedure_id: str, kontakt_id: str, uuid: str, aar: str) -> list[str]:
    return _row(
        DATASETS_BY_PREFIX["PROCEDURE"],
        ID=procedure_id,
        DRGKONTAKT_ID=kontakt_id,
        PROCEDURE="KPA00",
        PROCEDURE_VAERDI=uuid,
        Aktivitetsaar=aar,
    )


def build_delivery(
    tmp_path,
    filename: str,
    rows_by_prefix: dict[str, list[list[str]]],
    datasets_by_prefix: dict | None = None,
) -> None:
    """Skriv en tar.bz2 med data-medlemmer + SDP_ExportLog for alle datasaet."""
    members: dict[str, bytes] = {}
    log_lines = ["FilNavn;AntalRaekker"]
    for prefix, dataset in (datasets_by_prefix or DATASETS_BY_PREFIX).items():
        rows = rows_by_prefix.get(prefix, [])
        member_name = f"T26_{prefix}_1083_20260823.csv"
        members[member_name] = _csv_bytes(dataset, rows)
        log_lines.append(f"{member_name};{len(rows)}")
    members["SDP_ExportLog_20260823.csv"] = "\n".join(log_lines).encode("cp1252")

    with tarfile.open(tmp_path / filename, mode="w:bz2") as tar:
        for member_name in sorted(members):
            data = members[member_name]
            info = tarfile.TarInfo(name=f"T26/{member_name}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))


def build_delivery_a(tmp_path):
    """M-agtig leverance: aar 2024 (UUID-C) + aar 2025 (UUID-A, UUID-B)."""
    build_delivery(
        tmp_path,
        "Region_1083_DRG_T26_M_20260818114549.tar.bz2",
        {
            "DRGKONTAKT": [
                kontakt_row(UUID_A, "11", "901", "2025", "100"),
                kontakt_row(UUID_B, "12", "901", "2025", "200"),
                kontakt_row(UUID_C, "13", "902", "2024", "300"),
            ],
            "DRGSGHFORLOB": [
                forloeb_row("901", "11", "12", "2025", "300"),
                forloeb_row("902", "13", "13", "2024", "300"),
            ],
            "DIAGNOSER": [
                diagnose_row("1", "11", "DA01", "2025"),
                diagnose_row("2", "13", "DB02", "2024"),
            ],
            "DIAGNOSER_TILLAEG": [
                _row(
                    DATASETS_BY_PREFIX["DIAGNOSER_TILLAEG"],
                    ID="1",
                    DIAGNOSE_ID="1",
                    DRGKONTAKT_ID="11",
                    TILLAEGSKODE_DIAGNOSE="TUL1",
                    Aktivitetsaar="2025",
                )
            ],
            "PROCEDURE": [
                procedure_row("1", "11", PV_1, "2025"),
                procedure_row("2", "13", PV_2, "2024"),
            ],
            "PROCEDURE_TILLAEG": [
                _row(
                    DATASETS_BY_PREFIX["PROCEDURE_TILLAEG"],
                    ID="1",
                    PROCEDURE_ID="1",
                    DRGKONTAKT_ID="11",
                    TILLAEGSKODE_PROCEDURE="TPX1",
                    Aktivitetsaar="2025",
                )
            ],
            "INTERNE_KODER": [
                _row(
                    DATASETS_BY_PREFIX["INTERNE_KODER"],
                    ID="1",
                    DRGKONTAKT_ID="11",
                    INTERN_KODE="IK01",
                    INTERN_TYPE="A",
                    Aktivitetsaar="2025",
                )
            ],
            "DRGTAKSTBESKRIVELSE": [["01MA01", "Testgruppe"]],
            "LEVERANCEINFO": [["DRG", "2026-08-16"]],
        },
    )


def build_delivery_b(tmp_path):
    """U-agtig, nyere leverance: KUN aar 2025. UUID-A har ny pris (150);
    UUID-B er udgaaet. Kontaktens heltals-ID er om-nummereret (11 -> 21)."""
    build_delivery(
        tmp_path,
        "Region_1083_DRG_T26_U_20260824190546.tar.bz2",
        {
            "DRGKONTAKT": [kontakt_row(UUID_A, "21", "801", "2025", "150")],
            "DRGSGHFORLOB": [forloeb_row("801", "21", "21", "2025", "150")],
            "DIAGNOSER": [diagnose_row("1", "21", "DA01", "2025")],
            "DIAGNOSER_TILLAEG": [
                _row(
                    DATASETS_BY_PREFIX["DIAGNOSER_TILLAEG"],
                    ID="1",
                    DIAGNOSE_ID="1",
                    DRGKONTAKT_ID="21",
                    TILLAEGSKODE_DIAGNOSE="TUL1",
                    Aktivitetsaar="2025",
                )
            ],
            "PROCEDURE": [procedure_row("1", "21", PV_1, "2025")],
            "PROCEDURE_TILLAEG": [
                _row(
                    DATASETS_BY_PREFIX["PROCEDURE_TILLAEG"],
                    ID="1",
                    PROCEDURE_ID="1",
                    DRGKONTAKT_ID="21",
                    TILLAEGSKODE_PROCEDURE="TPX1",
                    Aktivitetsaar="2025",
                )
            ],
            "INTERNE_KODER": [
                _row(
                    DATASETS_BY_PREFIX["INTERNE_KODER"],
                    ID="1",
                    DRGKONTAKT_ID="21",
                    INTERN_KODE="IK01",
                    INTERN_TYPE="A",
                    Aktivitetsaar="2025",
                )
            ],
            "DRGTAKSTBESKRIVELSE": [["01MA01", "Testgruppe"]],
            "LEVERANCEINFO": [["DRG", "2026-08-23"]],
        },
    )


# ---------------------------------------------------------------------------
# DB-hjaelpere
# ---------------------------------------------------------------------------


def scalar(resource, sql: str, *params):
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, *params)
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            cursor.close()


@pytest.fixture(scope="module")
def drg_db(real_mssql_target_staging_resource):
    """Frisk DRG-skema i devcontainer-databasen: drop + koer DDL'en fra repoet.

    Only the migrations this test actually depends on are applied — the DFM
    table rename, so the merge engine finds meta.DFMState/DFMRunLog under
    their current names on a database that is behind. The full migration
    chain is deliberately NOT run: an unrelated migration that cannot apply
    to a given database state (e.g. an ALTER COLUMN blocked by a dependent
    index) would fail this fixture and mask the DRG behaviour under test.
    """
    from infrastructure.utils.ddl_loader import (
        execute_sql_file,
        execute_sql_files_from_directory,
    )

    resource = real_mssql_target_staging_resource
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for dataset in DATASETS:
                cursor.execute(f"DROP VIEW IF EXISTS [ext].[v_{dataset.ext_table}]")
                cursor.execute(f"DROP TABLE IF EXISTS [ext].[{dataset.ext_table}]")
                cursor.execute(f"DROP TABLE IF EXISTS [arc].[{dataset.ext_table}]")
            cursor.execute("DROP TABLE IF EXISTS [arc].[drg_pris_revision]")
            cursor.execute("DROP TABLE IF EXISTS [meta].[DRGLeverance]")
            conn.commit()
        finally:
            cursor.close()
    execute_sql_file(resource, "migrations/20260708_rename_00_meta.sql")
    execute_sql_file(resource, "tables/meta/02_dfm_state.sql")
    execute_sql_file(resource, "tables/meta/03_dfm_run_log.sql")
    execute_sql_file(resource, "migrations/20261005_01_dfm_run_log_source_columns.sql")
    execute_sql_files_from_directory(resource, "tables/drg")
    return resource


def run_extract(resource, base_dir, on_or_before: str | None = None):
    context = dg.build_asset_context()
    config = drg.DrgExtractConfig(
        delivery_on_or_before=on_or_before,
        max_delivery_age_days=36500,  # fixturens leverancedatoer er statiske
    )
    share = FileShareResource(base_dir=str(base_dir))
    return drg._run_extract(context, resource, share, SOMATIK_DATASETS, "DRG", config)


def run_archive_all(resource):
    for dataset in SOMATIK_DATASETS:
        context = dg.build_asset_context()
        drg_archive._run_archive(context, resource, dataset, drg_archive.DrgArchiveConfig())


# ---------------------------------------------------------------------------
# Testene
# ---------------------------------------------------------------------------


def test_1_foerste_leverance_lander_i_arc(drg_db, tmp_path):
    build_delivery_a(tmp_path)
    run_extract(drg_db, tmp_path)
    assert scalar(drg_db, "SELECT COUNT(*) FROM ext.drg_kontakt") == 3

    run_archive_all(drg_db)

    assert scalar(drg_db, "SELECT COUNT(*) FROM arc.drg_kontakt WHERE dw_is_deleted = 0") == 3
    assert scalar(drg_db, "SELECT COUNT(*) FROM arc.drg_sghforlob WHERE dw_is_deleted = 0") == 2
    # UUID-oploesning: diagnosen baerer kontaktens UUID, ikke dens heltals-ID
    assert (
        scalar(drg_db, "SELECT kontakt_uuid FROM arc.drg_diagnose WHERE DIAGNOSE = ?", "DB02")
        == UUID_C
    )
    # forloebsnoeglen er ind-kontaktens UUID
    assert (
        scalar(drg_db, "SELECT COUNT(*) FROM arc.drg_sghforlob WHERE ind_kontakt_uuid = ?", UUID_C)
        == 1
    )
    # tillaegskoderne har faaet noeglerne fra deres foraeldre-raekker
    assert (
        scalar(
            drg_db,
            "SELECT COUNT(*) FROM arc.drg_procedure_tillaeg "
            "WHERE procedure_uuid = ? AND kontakt_uuid = ?",
            PV_1,
            UUID_A,
        )
        == 1
    )
    # leveranceregister pr. takstsystem
    assert (
        str(
            scalar(
                drg_db,
                "SELECT MAX(leverance_dato) FROM meta.DRGLeverance WHERE kildetabel = ?",
                "drg_kontakt",
            )
        )
        == "2026-08-18"
    )
    # ingen volatile ID-kolonner i arc
    assert (
        scalar(
            drg_db,
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'drg_kontakt' "
            "AND COLUMN_NAME IN ('ID', 'DRGSGHFORLOB_ID', 'SYGDOMSFORLOB_ID')",
        )
        == 0
    )


def test_2_aarsskive_scope_revision_og_soft_delete(drg_db, tmp_path):
    """U-leverancen daekker kun 2025: 2024-aaret maa IKKE roeres; A's pris
    opdateres med revisionslog; B (udgaaet af 2025-skiven) soft-slettes."""
    build_delivery_b(tmp_path)
    run_extract(drg_db, tmp_path)
    assert scalar(drg_db, "SELECT COUNT(*) FROM ext.drg_kontakt") == 1

    run_archive_all(drg_db)

    # 2024-kontakten (UUID-C) overlever, selvom den ikke er i leverancen
    assert (
        scalar(drg_db, "SELECT dw_is_deleted FROM arc.drg_kontakt WHERE KONTAKT_VAERDI = ?", UUID_C)
        == 0
    )
    # UUID-B var i 2025-skiven men udgik -> soft-deleted
    assert (
        scalar(drg_db, "SELECT dw_is_deleted FROM arc.drg_kontakt WHERE KONTAKT_VAERDI = ?", UUID_B)
        == 1
    )
    # UUID-A: pris opdateret trods om-nummereret heltals-ID (11 -> 21)
    assert (
        scalar(drg_db, "SELECT TOTALPRIS_DRG FROM arc.drg_kontakt WHERE KONTAKT_VAERDI = ?", UUID_A)
        == "150"
    )
    assert (
        scalar(
            drg_db,
            "SELECT COUNT(*) FROM arc.drg_pris_revision "
            "WHERE kildetabel = 'drg_kontakt' AND noegle = ? "
            "AND kolonne = 'TOTALPRIS_DRG' AND vaerdi_foer = '100' AND vaerdi_efter = '150'",
            UUID_A,
        )
        == 1
    )
    # 2024-boernene (diagnose/procedure paa UUID-C) overlever ogsaa
    assert (
        scalar(drg_db, "SELECT dw_is_deleted FROM arc.drg_diagnose WHERE kontakt_uuid = ?", UUID_C)
        == 0
    )
    assert (
        scalar(
            drg_db, "SELECT dw_is_deleted FROM arc.drg_procedure WHERE PROCEDURE_VAERDI = ?", PV_2
        )
        == 0
    )


def test_3_genkoersel_er_noop(drg_db, tmp_path):
    build_delivery_b(tmp_path)
    run_extract(drg_db, tmp_path)
    before = scalar(
        drg_db,
        "SELECT MAX(dw_timestamp_update) FROM arc.drg_kontakt WHERE KONTAKT_VAERDI = ?",
        UUID_A,
    )
    revisions_before = scalar(drg_db, "SELECT COUNT(*) FROM arc.drg_pris_revision")

    run_archive_all(drg_db)

    after = scalar(
        drg_db,
        "SELECT MAX(dw_timestamp_update) FROM arc.drg_kontakt WHERE KONTAKT_VAERDI = ?",
        UUID_A,
    )
    assert str(before) == str(after), "genkoersel maa ikke roere uaendrede raekker"
    assert scalar(drg_db, "SELECT COUNT(*) FROM arc.drg_pris_revision") == revisions_before
    assert (
        scalar(drg_db, "SELECT dw_is_deleted FROM arc.drg_kontakt WHERE KONTAKT_VAERDI = ?", UUID_C)
        == 0
    )


def test_4_aeldre_leverance_springes_over_uden_at_roere_arkivet(drg_db, tmp_path):
    """En pinnet AELDRE leverance afvises ikke laengere haardt — den springes
    over pr. aarsskive, fordi dens datacap ikke er nyere end den, arkivet
    allerede afspejler. Det er den samme mekanisme, der goer en M efter en U
    korrekt, og den maa derfor ikke vaere en fejl."""
    build_delivery_a(tmp_path)
    build_delivery_b(tmp_path)
    # pin extract til den aeldre M-leverance — arkivet afspejler allerede B
    run_extract(drg_db, tmp_path, on_or_before="2026-08-18")

    before = scalar(
        drg_db, "SELECT TOTALPRIS_DRG FROM arc.drg_kontakt WHERE KONTAKT_VAERDI = ?", UUID_A
    )
    run_archive_all(drg_db)
    after = scalar(
        drg_db, "SELECT TOTALPRIS_DRG FROM arc.drg_kontakt WHERE KONTAKT_VAERDI = ?", UUID_A
    )

    assert str(before) == str(after), "den aeldre leverance maa ikke overskrive arkivet"


def test_7_paritetsguard_fanger_uoploeselig_reference(drg_db, tmp_path):
    """En diagnose der peger paa en kontakt uden for leverancen taber raekken
    LYDLOEST i INNER JOIN-viewet — paritetsguarden skal fejle koerslen, foer
    mergen soft-sletter dens arkivmodstykke."""
    build_delivery(
        tmp_path,
        "Region_1083_DRG_T26_U_20260825190546.tar.bz2",
        {
            "DRGKONTAKT": [kontakt_row(UUID_A, "31", "701", "2025", "150")],
            "DRGSGHFORLOB": [forloeb_row("701", "31", "31", "2025", "150")],
            "DIAGNOSER": [diagnose_row("1", "999", "DA01", "2025")],  # ukendt kontakt!
            "DIAGNOSER_TILLAEG": [
                _row(
                    DATASETS_BY_PREFIX["DIAGNOSER_TILLAEG"],
                    ID="1",
                    DIAGNOSE_ID="1",
                    DRGKONTAKT_ID="31",
                    TILLAEGSKODE_DIAGNOSE="TUL1",
                    Aktivitetsaar="2025",
                )
            ],
            "PROCEDURE": [procedure_row("1", "31", PV_1, "2025")],
            "PROCEDURE_TILLAEG": [
                _row(
                    DATASETS_BY_PREFIX["PROCEDURE_TILLAEG"],
                    ID="1",
                    PROCEDURE_ID="1",
                    DRGKONTAKT_ID="31",
                    TILLAEGSKODE_PROCEDURE="TPX1",
                    Aktivitetsaar="2025",
                )
            ],
            "INTERNE_KODER": [
                _row(
                    DATASETS_BY_PREFIX["INTERNE_KODER"],
                    ID="1",
                    DRGKONTAKT_ID="31",
                    INTERN_KODE="IK01",
                    INTERN_TYPE="A",
                    Aktivitetsaar="2025",
                )
            ],
            "DRGTAKSTBESKRIVELSE": [["01MA01", "Testgruppe"]],
            "LEVERANCEINFO": [["DRG", "2026-08-24"]],
        },
    )
    run_extract(drg_db, tmp_path)
    diagnose = DATASETS_BY_PREFIX["DIAGNOSER"]
    with pytest.raises(ValueError, match="tabt"):
        drg_archive._run_archive(
            dg.build_asset_context(), drg_db, diagnose, drg_archive.DrgArchiveConfig()
        )
    # og intet blev soft-slettet af den afbrudte koersel
    assert scalar(drg_db, "SELECT COUNT(*) FROM arc.drg_diagnose WHERE dw_is_deleted = 1") == 0


# ---------------------------------------------------------------------------
# Psyk-sporet — viewsene joiner anderledes (ID_INT / DW_EK_Kontakt) og har
# ingen aktivitetsaar-kolonne (delete-scope = takstsystem alene)
# ---------------------------------------------------------------------------

PSYK_BY_PREFIX = {d.source_prefix: d for d in PSYK_DATASETS}
PSY_A = "DDDDDDDD-0000-0000-0000-000000000001"
PSY_B = "DDDDDDDD-0000-0000-0000-000000000002"
PSY_PV = "DDDDDDDD-1111-0000-0000-000000000001"


def psyk_kontakt_row(uuid: str, id_int: str, dw_ek: str, forloeb_id: str) -> list[str]:
    return _row(
        PSYK_BY_PREFIX["Psyk_kontakt"],
        ID_INT=id_int,
        ID=id_int.zfill(32),
        PsykiatriForloeb_ID=forloeb_id,
        DW_EK_Kontakt=dw_ek,
        KONTAKT_VAERDI=uuid,
        KONTAKTTYPE="fysisk",
    )


def build_psyk_delivery(
    tmp_path, filename="Region_1083_DRGPSYK_T26_M_20260810120000.tar.bz2", **overrides
):
    """To kontakter i et forloeb + diagnose/procedure(+tillaeg) + betaling.

    `overrides` erstatter raekkerne for et enkelt datasaet — pre-flighten
    kraever >0 raekker i alle obligatoriske datasaet, saa resten skal med.
    """
    rows = {
        "Psyk_kontakt": [
            psyk_kontakt_row(PSY_A, "101", "501", "9001"),
            psyk_kontakt_row(PSY_B, "102", "502", "9001"),
        ],
        "Psyk_forloeb": [
            _row(
                PSYK_BY_PREFIX["Psyk_forloeb"],
                PsykiatriForloeb_ID="9001",
                DW_EK_Kontakt_foerste="501",
                DW_EK_Kontakt_sidste="502",
                ANTAL_I_FORLOEB="2",
                PRIS_PsykiatriForloeb="1000",
            )
        ],
        "Psyk_Diagnose": [
            _row(
                PSYK_BY_PREFIX["Psyk_Diagnose"],
                ID="1",
                Kontakt_ID_INT="101",
                Diagnose="DF20",
                ART="A",
            )
        ],
        "Psyk_Diagnose_tillaeg": [
            _row(
                PSYK_BY_PREFIX["Psyk_Diagnose_tillaeg"],
                ID="1",
                Kontakt_ID_INT="101",
                DIAGNOSE_ID="1",
                TILLAEGSKODE_DIAGNOSE="TUL9",
            )
        ],
        "Psyk_procedure": [
            _row(
                PSYK_BY_PREFIX["Psyk_procedure"],
                ID="1",
                Kontakt_ID_INT="101",
                PROCEDUREKODE="BRSP1",
                PROCEDURE_VAERDI=PSY_PV,
            )
        ],
        "Psyk_procedure_tillaeg": [
            _row(
                PSYK_BY_PREFIX["Psyk_procedure_tillaeg"],
                ID="1",
                PROCEDURE_ID="1",
                Kontakt_ID_INT="101",
                TILLAEGSKODE_PROCEDURE="TUL8",
            )
        ],
        "Psyk_Betalingsoplysninger": [
            _row(
                PSYK_BY_PREFIX["Psyk_Betalingsoplysninger"],
                ID="1",
                Kontakt_ID_INT="101",
                Bet_opl_starttidspunkt="2026-01-01 00:00:00",
                BETALINGSAFTALE="X1",
            )
        ],
    }
    rows.update(overrides)
    build_delivery(tmp_path, filename, rows, datasets_by_prefix=PSYK_BY_PREFIX)


def run_psyk_archive_all(resource):
    for dataset in PSYK_DATASETS:
        context = dg.build_asset_context()
        drg_archive._run_archive(context, resource, dataset, drg_archive.DrgArchiveConfig())


def test_8_psyk_sporet_e2e(drg_db, tmp_path):
    """Psyk-viewsene (ID_INT/DW_EK-joins) eksekveret mod rigtig SQL."""
    build_psyk_delivery(tmp_path)
    context = dg.build_asset_context()
    config = drg.DrgExtractConfig(max_delivery_age_days=36500)
    share = FileShareResource(base_dir=str(tmp_path))
    drg._run_extract(context, drg_db, share, PSYK_DATASETS, "DRGPSYK", config)
    assert scalar(drg_db, "SELECT COUNT(*) FROM ext.drgpsyk_kontakt") == 2

    run_psyk_archive_all(drg_db)

    # forloebsnoeglen (foerste kontakts UUID) er oploest via DW_EK-joinen
    assert (
        scalar(
            drg_db, "SELECT forloeb_noegle FROM arc.drgpsyk_kontakt WHERE KONTAKT_VAERDI = ?", PSY_B
        )
        == PSY_A
    )
    assert scalar(drg_db, "SELECT foerste_kontakt_uuid FROM arc.drgpsyk_forloeb") == PSY_A
    assert scalar(drg_db, "SELECT sidste_kontakt_uuid FROM arc.drgpsyk_forloeb") == PSY_B
    # boernene baerer kontaktens UUID, ikke ID_INT
    assert (
        scalar(drg_db, "SELECT kontakt_uuid FROM arc.drgpsyk_diagnose WHERE Diagnose = ?", "DF20")
        == PSY_A
    )
    assert scalar(drg_db, "SELECT kontakt_uuid FROM arc.drgpsyk_betalingsoplysninger") == PSY_A
    assert (
        scalar(
            drg_db,
            "SELECT COUNT(*) FROM arc.drgpsyk_procedure_tillaeg "
            "WHERE procedure_uuid = ? AND kontakt_uuid = ?",
            PSY_PV,
            PSY_A,
        )
        == 1
    )
    # genkoersel er no-op — intet soft-slettes (scope = takstsystem alene)
    run_psyk_archive_all(drg_db)
    assert scalar(drg_db, "SELECT COUNT(*) FROM arc.drgpsyk_kontakt WHERE dw_is_deleted = 0") == 2


# ---------------------------------------------------------------------------
# Kildedubletter — identiske dedupes, konfliktende fejler
# (SDS leverede ét byte-identisk diagnosepar i T24 psyk, maalt i dev 2026-08-27)
# ---------------------------------------------------------------------------


def test_9_byte_identiske_kildedubletter_dedupes(drg_db, tmp_path):
    """Samme diagnoseraekke leveret to gange: mergen skal lykkes med én raekke
    i arkivet — at fejle hele tabellen over en identisk kopi ville arkivere
    ingenting. Maalt i dev: SDS leverede ét saadant par i T24 psyk."""
    diagnose = PSYK_BY_PREFIX["Psyk_Diagnose"]
    identisk = _row(diagnose, ID="1", Kontakt_ID_INT="101", Diagnose="DZ032", ART="ALGA02")
    build_psyk_delivery(
        tmp_path,
        filename="Region_1083_DRGPSYK_T26_M_20260901120000.tar.bz2",
        Psyk_Diagnose=[identisk, list(identisk)],  # praecis samme raekke to gange
    )
    drg._run_extract(
        dg.build_asset_context(),
        drg_db,
        FileShareResource(base_dir=str(tmp_path)),
        PSYK_DATASETS,
        "DRGPSYK",
        drg.DrgExtractConfig(max_delivery_age_days=36500),
    )
    assert scalar(drg_db, "SELECT COUNT(*) FROM ext.drgpsyk_diagnose") == 2

    result = drg_archive._run_archive(
        dg.build_asset_context(),
        drg_db,
        PSYK_BY_PREFIX["Psyk_Diagnose"],
        drg_archive.DrgArchiveConfig(),
    )
    assert result.metadata["identiske_kildedubletter"] == 1
    # én raekke i arkivet, ikke to — og ingen PK-violation
    assert (
        scalar(drg_db, "SELECT COUNT(*) FROM arc.drgpsyk_diagnose WHERE Diagnose = ?", "DZ032") == 1
    )


def test_10_konfliktende_kildedubletter_fejler(drg_db, tmp_path):
    """Samme noegle, forskellige vaerdier: her ville en dedup afgoere hvad
    arkivet tror paa — koerslen skal fejle.

    Bruger kontakt-tabellen, ikke diagnose: psyk-diagnosens arc-kolonner ER
    merge-noeglen (alt andet er droppet), saa dens dubletter kan pr.
    konstruktion ikke konflikte. Kontakten har vaerdikolonner og kan.
    """
    kontakt = PSYK_BY_PREFIX["Psyk_kontakt"]
    a = _row(
        kontakt,
        ID_INT="101",
        ID="101".zfill(32),
        PsykiatriForloeb_ID="9001",
        DW_EK_Kontakt="501",
        KONTAKT_VAERDI=PSY_A,
        KONTAKTTYPE="fysisk",
    )
    b = _row(
        kontakt,
        ID_INT="102",
        ID="102".zfill(32),
        PsykiatriForloeb_ID="9001",
        DW_EK_Kontakt="502",
        KONTAKT_VAERDI=PSY_A,  # samme UUID
        KONTAKTTYPE="virtuel",  # men anden vaerdi -> konflikt
    )
    build_psyk_delivery(
        tmp_path,
        filename="Region_1083_DRGPSYK_T26_M_20260902120000.tar.bz2",
        Psyk_kontakt=[a, b],
    )
    drg._run_extract(
        dg.build_asset_context(),
        drg_db,
        FileShareResource(base_dir=str(tmp_path)),
        PSYK_DATASETS,
        "DRGPSYK",
        drg.DrgExtractConfig(max_delivery_age_days=36500),
    )
    with pytest.raises(ValueError, match="FORSKELLIGE vaerdier"):
        drg_archive._run_archive(
            dg.build_asset_context(),
            drg_db,
            PSYK_BY_PREFIX["Psyk_kontakt"],
            drg_archive.DrgArchiveConfig(),
        )


def test_11_registreringsid_dubletter_er_identiske(drg_db, tmp_path):
    """To diagnoseraekker der kun afviger paa kildens raekke-uuid er samme
    registrering gentaget — mergen skal lykkes med én raekke, deterministisk
    vinder."""
    diagnose = DATASETS_BY_PREFIX["DIAGNOSER"]
    kontakt_uuid = "EEEEEEEE-0000-0000-0000-000000000001"

    def dublet(diagnose_id: str, vaerdi: str) -> list[str]:
        return _row(
            diagnose,
            ID=diagnose_id,
            DRGKONTAKT_ID="61",
            DIAGNOSE="DZ032",
            ART="ALGA02",
            DIAGNOSE_VAERDI=vaerdi,
            Aktivitetsaar="2023",
        )

    build_delivery(
        tmp_path,
        "Region_1083_DRG_T26_M_20260903120000.tar.bz2",
        {
            "DRGKONTAKT": [kontakt_row(kontakt_uuid, "61", "71", "2023", "1000")],
            "DRGSGHFORLOB": [forloeb_row("71", "61", "61", "2023", "1000")],
            "DIAGNOSER": [
                dublet("1", "EEEEEEEE-1111-0000-0000-000000000001"),
                dublet("2", "EEEEEEEE-1111-0000-0000-000000000002"),
            ],
            # brugbarheds-reglen kraever raekker i alle oevrige datasaet
            "DIAGNOSER_TILLAEG": [
                _row(
                    DATASETS_BY_PREFIX["DIAGNOSER_TILLAEG"],
                    ID="1",
                    DIAGNOSE_ID="1",
                    DRGKONTAKT_ID="61",
                    TILLAEGSKODE_DIAGNOSE="TUL1",
                    Aktivitetsaar="2023",
                )
            ],
            "PROCEDURE": [procedure_row("1", "61", "EEEEEEEE-2222-0000-0000-000000000001", "2023")],
            "PROCEDURE_TILLAEG": [
                _row(
                    DATASETS_BY_PREFIX["PROCEDURE_TILLAEG"],
                    ID="1",
                    PROCEDURE_ID="1",
                    DRGKONTAKT_ID="61",
                    TILLAEGSKODE_PROCEDURE="TPX1",
                    Aktivitetsaar="2023",
                )
            ],
            "INTERNE_KODER": [
                _row(
                    DATASETS_BY_PREFIX["INTERNE_KODER"],
                    ID="1",
                    DRGKONTAKT_ID="61",
                    INTERN_KODE="IK01",
                    INTERN_TYPE="A",
                    Aktivitetsaar="2023",
                )
            ],
            "DRGTAKSTBESKRIVELSE": [["01MA01", "Testgruppe"]],
            "LEVERANCEINFO": [["DRG", "2026-09-03"]],
        },
    )
    run_extract(drg_db, tmp_path)
    assert scalar(drg_db, "SELECT COUNT(*) FROM ext.drg_diagnose") == 2

    result = drg_archive._run_archive(
        dg.build_asset_context(), drg_db, diagnose, drg_archive.DrgArchiveConfig()
    )
    assert result.metadata["identiske_kildedubletter"] == 1
    # én raekke i arkivet, og vinderen er deterministisk (hoejeste raekke-uuid)
    assert (
        scalar(
            drg_db,
            "SELECT COUNT(*) FROM arc.drg_diagnose WHERE kontakt_uuid = ?",
            kontakt_uuid,
        )
        == 1
    )
    assert (
        scalar(
            drg_db,
            "SELECT [DIAGNOSE_VAERDI] FROM arc.drg_diagnose WHERE kontakt_uuid = ?",
            kontakt_uuid,
        )
        == "EEEEEEEE-1111-0000-0000-000000000002"
    )

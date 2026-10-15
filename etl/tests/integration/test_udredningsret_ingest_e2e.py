# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""E2E integration test for udredningsret ingestion against the devcontainer MSSQL.

Runs the whole chain with synthetic tar.bz2 deliveries (synthetic CPRs):

    DDL (tables/udredningsret) -> extract (tar -> ext + manifest) -> archive
    (key table -> MERGE -> arc) -> checkpoint gate (meta.DFMState)

and verifies the semantics that carry the design (B3/B6 on feature 27631 and
the [internal ticket] delivery-detection pattern):

1. BUSINESS KEYS: helbredsforloeb, markoer and loebenummer are computed from
   the content; fully identical rows collapse into one with antal_kilderaekker;
   duplicates on (helbredsforloeb, code, date) get a loebenummer. The gate
   stamps the delivery checkpoint; the key table is dropped after the merge.
2. STABILITY DESPITE RENUMBERING: the next delivery renumbers F_ID and
   DW_EK_HelbredsForloeb and reissues OBJEKTIDs — including on ONE of two
   duplicate markers (review F13) — and the arc rows keep their keys, their
   loebenummer and their content.
3. DELIVERY DETECTION: a run without a new file reads nothing (empty manifest,
   ext untouched, archive and gate no-op). After a checkpoint reset the same
   file re-merges as 0/0/0.
4. RECOVERY GUARD: when the first marker changes for too many helbredsforloeb
   the run fails unless the limit is overridden deliberately.
5. DIRECTION GUARD + PIN: a pinned (older) delivery bypasses the checkpoint,
   must not be merged on top of a newer archive, and never advances the
   checkpoint.

The tests share module-scoped DB state and run in definition order.
Requires USE_REAL_DB_FOR_TESTS=true and ENABLE_TEST_WRITES=true (devcontainer).
"""

import csv
import io
import os
import tarfile

import dagster as dg
import pytest
from core.dfm_manager import DfmManager
from DSI.csv_files import udredningsret, udredningsret_archive
from DSI.csv_files.udredningsret_datasets import DATASETS, LEVERANCEINFO, UDREDNINGSRET_MONT

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def require_real_environment():
    use_real = os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() == "true"
    writes = os.getenv("ENABLE_TEST_WRITES", "false").lower() == "true"
    if not (use_real and writes):
        pytest.skip("requires USE_REAL_DB_FOR_TESTS=true and ENABLE_TEST_WRITES=true")


# Synthetic citizens (invalid CPR numbers) and LPR3 UUIDs
CPR_X, CPR_Y, CPR_Z = "0101011111", "0202022222", "0303033333"
ELEM_1, ELEM_2, ELEM_3 = (
    "e1e1e1e1-0000-0000-0000-000000000001",
    "e2e2e2e2-0000-0000-0000-000000000002",
    "e3e3e3e3-0000-0000-0000-000000000003",
)
OBJ = {k: f"{k * 8}-0000-0000-0000-{i:012d}" for i, k in enumerate("abcdefghij", start=1)}
# Reissued UUID for marker f that sorts BEFORE e's — the old OBJEKTID ordering
# would have swapped the two duplicate markers' loebenummer.
OBJ_F_REISSUED = "00000000-0000-0000-0000-000000000006"
SOR_1, SOR_2 = "239281000016009", "240441000016007"

UNIT = udredningsret.DELIVERY_UNIT


def _row(**values) -> list[str]:
    unknown = set(values) - set(UDREDNINGSRET_MONT.source_columns)
    assert not unknown, f"unknown test columns {unknown}"
    return [values.get(c, "") for c in UDREDNINGSRET_MONT.source_columns]


def markoer(dw_ek, elem, obj, pk, fid, cpr, kode, dato, sor=SOR_1, mrk_h="", h="", **extra):
    return _row(
        DW_EK_HelbredsForloeb=dw_ek,
        OBJEKTID_FORLOBELEMENT=elem,
        OBJEKTID_FORLOBSMARKOR=obj,
        PK=pk,
        F_ID=fid,
        CPR=cpr,
        C_SGHREGION="1083",
        ANSVARLIG_ENHED_FORLOB=sor,
        FORLOEBSLABEL="ALAL90",
        FORLOEBSMARKOER=kode,
        FORLOEBSMARKOER_DATO=dato,
        MRK_H=mrk_h,
        H=h,
        C_BOREGION="1083",
        **extra,
    )


def _csv(columns, rows) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=";", quoting=csv.QUOTE_ALL)
    writer.writerow(list(columns))
    writer.writerows(rows)
    return buffer.getvalue().encode("cp1252")


def build_delivery(
    tmp_path, filename: str, dato: str, lpr_dato: str, rows: list[list[str]]
) -> None:
    """tar.bz2 in SDS' member order: Metadata, SDP_ExportLog, data file, LEVERANCEINFO last."""
    data = f"UdredningsretMont_1083_{dato}.csv"
    info = f"UdredningsretMont_LEVERANCEINFO_1083_{dato}.csv"
    members = {
        f"Metadata_UdredningsretMont_1083_{dato}.csv": b'"column_ordinal";"column_name"\r\n',
        f"SDP_ExportLog_{dato}.csv": (
            "FilNavn;AntalRaekker;FilStoerrelseBytes;Oprettet\n"
            f"Metadata_UdredningsretMont_1083_{dato}.csv;28;1868;2026-09-07 19:29:18\n"
            f"{info};1;70;2026-09-07 19:29:18\n"
            f"{data};{len(rows)};1;2026-09-07 19:31:43\n"
        ).encode("cp1252"),
        data: _csv(UDREDNINGSRET_MONT.source_columns, rows),
        info: _csv(LEVERANCEINFO.source_columns, [["UdredningsretMont", lpr_dato]]),
    }
    with tarfile.open(tmp_path / filename, mode="w:bz2") as tar:
        for name in sorted(members):
            content = members[name]
            member = tarfile.TarInfo(name=f"1083/{name}")
            member.size = len(content)
            tar.addfile(member, io.BytesIO(content))


def rows_a() -> list[list[str]]:
    """Delivery A. HF1 (X): referred, start (delivered TWICE with another PK/OBJEKTID),
    finally assessed (MRK_H=1), two assessment plans on the same day with different
    content (loebenummer 1/2). HF2 (Y): one referral. HF3 (Z): referral + interrupted."""
    return [
        markoer(
            "100",
            ELEM_1,
            OBJ["a"],
            "5001_1",
            "5001",
            CPR_X,
            "AFV01A1",
            "2026-01-05",
            mrk_h="1",
            h="27",
        ),
        markoer("100", ELEM_1, OBJ["b"], "5001_2", "5001", CPR_X, "AFV01B", "2026-01-10"),
        markoer(
            "100", ELEM_1, OBJ["c"], "5001_3", "5001", CPR_X, "AFV01B", "2026-01-10"
        ),  # full duplicate of b
        markoer(
            "100",
            ELEM_1,
            OBJ["d"],
            "5001_4",
            "5001",
            CPR_X,
            "AFX01C",
            "2026-02-01",
            mrk_h="1",
            h="27",
        ),
        markoer(
            "100", ELEM_1, OBJ["e"], "5001_5", "5001", CPR_X, "AFX01D4", "2026-01-20", ALDER="50"
        ),
        markoer(
            "100", ELEM_1, OBJ["f"], "5001_6", "5001", CPR_X, "AFX01D4", "2026-01-20", ALDER="51"
        ),
        markoer(
            "200", ELEM_2, OBJ["g"], "5002_1", "5002", CPR_Y, "AFV01A1", "2026-03-01", sor=SOR_2
        ),
        markoer("300", ELEM_3, OBJ["h"], "5003_1", "5003", CPR_Z, "AFV01A1", "2026-04-01"),
        markoer("300", ELEM_3, OBJ["i"], "5003_2", "5003", CPR_Z, "AFV01X2", "2026-04-20"),
    ]


def rows_b() -> list[list[str]]:
    """Delivery B (newer): HF1's F_ID renumbered (5001 -> 9001), the end marker
    reissued with a new OBJEKTID (d -> j), ONE of the two duplicate assessment
    plans (f) reissued with an OBJEKTID that sorts first (review F13), a new
    assessment plan added; HF2's DW_EK renumbered (200 -> 250) with unchanged
    first marker; HF3's interruption (i) is gone."""
    return [
        markoer(
            "100",
            ELEM_1,
            OBJ["a"],
            "9001_1",
            "9001",
            CPR_X,
            "AFV01A1",
            "2026-01-05",
            mrk_h="1",
            h="27",
        ),
        markoer("100", ELEM_1, OBJ["b"], "9001_2", "9001", CPR_X, "AFV01B", "2026-01-10"),
        markoer("100", ELEM_1, OBJ["c"], "9001_3", "9001", CPR_X, "AFV01B", "2026-01-10"),
        markoer(
            "100",
            ELEM_1,
            OBJ["j"],
            "9001_4",
            "9001",
            CPR_X,
            "AFX01C",
            "2026-02-01",
            mrk_h="1",
            h="27",
        ),
        markoer(
            "100", ELEM_1, OBJ["e"], "9001_5", "9001", CPR_X, "AFX01D4", "2026-01-20", ALDER="50"
        ),
        markoer(
            "100",
            ELEM_1,
            OBJ_F_REISSUED,
            "9001_6",
            "9001",
            CPR_X,
            "AFX01D4",
            "2026-01-20",
            ALDER="51",
        ),
        markoer(
            "100", ELEM_1, OBJ["h"][:-1] + "9", "9001_7", "9001", CPR_X, "AFX01D1", "2026-01-25"
        ),
        markoer(
            "250", ELEM_2, OBJ["g"], "5002_1", "5002", CPR_Y, "AFV01A1", "2026-03-01", sor=SOR_2
        ),
        markoer("300", ELEM_3, OBJ["h"], "5003_1", "5003", CPR_Z, "AFV01A1", "2026-04-01"),
    ]


def rows_c() -> list[list[str]]:
    """Delivery C: HF2 gets a NEW, earlier first marker -> new helbredsforloeb key."""
    rows = rows_b()
    rows.append(
        markoer(
            "250", ELEM_2, OBJ["i"], "5002_0", "5002", CPR_Y, "AFV01A1", "2026-02-20", sor=SOR_2
        )
    )
    return rows


def scalar(resource, sql: str, *params):
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, *params)
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            cursor.close()


def rows(resource, sql: str, *params):
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, *params)
            return cursor.fetchall()
        finally:
            cursor.close()


def checkpoint(resource) -> str | None:
    return DfmManager(resource, dg.build_asset_context()).get_delivery_checkpoint(UNIT)


@pytest.fixture(scope="module")
def delivery_dir(tmp_path_factory):
    """Delivery folder shared by every test in the module — the tests build on each other's files."""
    return tmp_path_factory.mktemp("udredningsret_leverancer")


@pytest.fixture(scope="module")
def udred_db(real_mssql_target_staging_resource):
    """Fresh udredningsret schema in the devcontainer database: drop + run the repo's DDL."""
    from infrastructure.utils.ddl_loader import execute_sql_file, execute_sql_files_from_directory

    resource = real_mssql_target_staging_resource
    execute_sql_file(resource, "migrations/20260708_rename_00_meta.sql")
    execute_sql_file(resource, "tables/meta/02_dfm_state.sql")
    execute_sql_file(resource, "tables/meta/03_dfm_run_log.sql")
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for dataset in DATASETS:
                source = udredningsret_archive.source_table_name(dataset)
                cursor.execute(f"DROP TABLE IF EXISTS [ext].[{source}]")
                cursor.execute(f"DROP TABLE IF EXISTS [ext].[{dataset.ext_table}]")
                cursor.execute(f"DROP TABLE IF EXISTS [arc].[{dataset.ext_table}]")
            cursor.execute(f"DROP TABLE IF EXISTS [ext].[{udredningsret.MANIFEST_TABLE}]")
            cursor.execute("DROP TABLE IF EXISTS [meta].[UdredningsretLeverance]")
            cursor.execute(
                "DELETE FROM [meta].[DFMState] WHERE [schema_name] = 'dfm' AND [table_name] = ?",
                UNIT,
            )
            conn.commit()
        finally:
            cursor.close()
    execute_sql_files_from_directory(resource, "tables/udredningsret")
    return resource


def run_extract(resource, base_dir, monkeypatch, on_or_before: str | None = None):
    monkeypatch.setenv(udredningsret.BASE_DIR_ENV, str(base_dir))
    context = dg.build_asset_context()
    config = udredningsret.UdredningsretExtractConfig(delivery_on_or_before=on_or_before)
    return udredningsret._run_extract(context, resource, config)


def run_archive(resource, **config):
    context = dg.build_asset_context()
    return udredningsret_archive._run_archive(
        context,
        resource,
        UDREDNINGSRET_MONT,
        udredningsret_archive.UdredningsretArchiveConfig(**config),
    )


def run_gate(resource):
    return udredningsret_archive._run_checkpoint_gate(dg.build_asset_context(), resource)


A_FILE = "Region_1083_LPR_Udredningsret_U_20260831193918.tar.bz2"
B_FILE = "Region_1083_LPR_Udredningsret_U_20260907193544.tar.bz2"
C_FILE = "Region_1083_LPR_Udredningsret_M_20260912001030.tar.bz2"
A_ID, B_ID, C_ID = "2026-08-31T19:39:18", "2026-09-07T19:35:44", "2026-09-12T00:10:30"

KEY_TABLE_EXISTS = "SELECT COUNT(*) FROM sys.objects WHERE object_id = OBJECT_ID('[ext].[udredningsret_mont_noegler]')"
MANIFEST_ROWS = f"SELECT COUNT(*) FROM ext.{udredningsret.MANIFEST_TABLE}"


def test_1_foerste_leverance_faar_forretningsnoegler_og_checkpoint(
    udred_db, delivery_dir, monkeypatch
):
    assert checkpoint(udred_db) is None
    build_delivery(delivery_dir, A_FILE, "20260830", "2026-08-30", rows_a())
    result = run_extract(udred_db, delivery_dir, monkeypatch)
    assert result.metadata["besluttet"] == "indlaest"
    assert result.metadata["checkpoint_foer"] == "ingen (foerste indlaesning)"
    assert scalar(udred_db, "SELECT COUNT(*) FROM ext.udredningsret_mont") == 9
    assert (
        scalar(udred_db, "SELECT LPROPDATERINGSDATO FROM ext.udredningsret_leveranceinfo")
        == "2026-08-30"
    )
    assert result.metadata["leverance"]["lpropdateringsdato"] == "2026-08-30"
    assert scalar(udred_db, MANIFEST_ROWS) == 1
    assert (
        scalar(udred_db, f"SELECT checkpoint_identitet FROM ext.{udredningsret.MANIFEST_TABLE}")
        == A_ID
    )

    run_archive(udred_db)

    # 9 ext rows -> 8 arc rows (b+c are fully identical)
    assert (
        scalar(udred_db, "SELECT COUNT(*) FROM arc.udredningsret_mont WHERE dw_is_deleted = 0") == 8
    )
    assert (
        scalar(
            udred_db,
            "SELECT antal_kilderaekker FROM arc.udredningsret_mont WHERE FORLOEBSMARKOER = 'AFV01B'",
        )
        == 2
    )
    # three helbredsforloeb, each with its own key; HF1's key is shared by every HF1 marker
    assert (
        scalar(
            udred_db, "SELECT COUNT(DISTINCT helbredsforloeb_noegle) FROM arc.udredningsret_mont"
        )
        == 3
    )
    assert (
        scalar(
            udred_db,
            "SELECT COUNT(DISTINCT helbredsforloeb_noegle) FROM arc.udredningsret_mont WHERE DW_EK_HelbredsForloeb = '100'",
        )
        == 1
    )
    # duplicates on (helbredsforloeb, code, date) with different content -> loebenummer 1 and 2
    assert sorted(
        r[0]
        for r in rows(
            udred_db,
            "SELECT loebenummer FROM arc.udredningsret_mont WHERE FORLOEBSMARKOER = 'AFX01D4'",
        )
    ) == [1, 2]
    # CPR sits in its own column, not in the key (the key is a hex hash)
    assert scalar(udred_db, "SELECT COUNT(*) FROM arc.udredningsret_mont WHERE CPR = ?", CPR_X) == 5
    assert (
        scalar(
            udred_db,
            "SELECT COUNT(*) FROM arc.udredningsret_mont WHERE helbredsforloeb_noegle LIKE '%' + ? + '%'",
            CPR_X,
        )
        == 0
    )
    assert (
        scalar(
            udred_db,
            "SELECT LEN(markoer_noegle) FROM arc.udredningsret_mont WHERE PK = '5001_1'",
        )
        == 64
    )
    # delivery register
    assert (
        scalar(
            udred_db,
            "SELECT lpropdateringsdato FROM meta.UdredningsretLeverance WHERE kildetabel = 'udredningsret_mont'",
        )
        == "2026-08-30"
    )
    assert scalar(udred_db, "SELECT raekker_kilde FROM meta.UdredningsretLeverance") == 8
    # the key table is dropped after a successful merge (F18)
    assert scalar(udred_db, KEY_TABLE_EXISTS) == 0

    # the gate stamps the checkpoint with the manifest's identity
    assert checkpoint(udred_db) is None
    gate = run_gate(udred_db)
    assert gate.metadata["besluttet"] == "checkpoint rykket"
    assert checkpoint(udred_db) == A_ID


def test_2_omnummerering_og_genudstedelse_bevarer_noeglerne(udred_db, delivery_dir, monkeypatch):
    before = {
        r[0]: (r[1], r[2])
        for r in rows(
            udred_db,
            "SELECT PK, markoer_noegle, helbredsforloeb_noegle FROM arc.udredningsret_mont",
        )
    }
    # F13: which loebenummer carries which content for the two duplicate assessment plans
    plans_before = dict(
        rows(
            udred_db,
            "SELECT loebenummer, ALDER FROM arc.udredningsret_mont WHERE FORLOEBSMARKOER = 'AFX01D4'",
        )
    )
    assert sorted(plans_before.values()) == ["50", "51"]

    build_delivery(delivery_dir, B_FILE, "20260906", "2026-09-06", rows_b())
    extract = run_extract(udred_db, delivery_dir, monkeypatch)
    assert extract.metadata["checkpoint_foer"] == A_ID
    assert extract.metadata["kandidater"] == 1  # only B is newer than the checkpoint
    result = run_archive(udred_db)

    # HF1: F_ID 5001 -> 9001, but the same marker keys and the same helbredsforloeb key
    after = {
        r[0]: (r[1], r[2])
        for r in rows(
            udred_db,
            "SELECT OBJEKTID_FORLOBSMARKOR, markoer_noegle, helbredsforloeb_noegle FROM arc.udredningsret_mont WHERE dw_is_deleted = 0",
        )
    }
    assert after[OBJ["a"]] == before["5001_1"]
    # Pure renumbering of F_ID/PK is not a change: the row keeps A's values and
    # leverance_dato (SURROGATE_ID_COLUMNS outside change-detect).
    assert [
        tuple(r)
        for r in rows(
            udred_db,
            "SELECT F_ID, PK, CAST(leverance_dato AS NVARCHAR(10)) FROM arc.udredningsret_mont WHERE OBJEKTID_FORLOBSMARKOR = ?",
            OBJ["a"],
        )
    ] == [("5001", "5001_1", "2026-08-31")]
    # the end marker got a new OBJEKTID (d -> j) — same arc row, attribute updated,
    # and F_ID/PK are refreshed together with the real change
    assert after[OBJ["j"]] == before["5001_4"]
    assert [
        tuple(r)
        for r in rows(
            udred_db,
            "SELECT F_ID, PK, CAST(leverance_dato AS NVARCHAR(10)) FROM arc.udredningsret_mont WHERE OBJEKTID_FORLOBSMARKOR = ?",
            OBJ["j"],
        )
    ] == [("9001", "9001_4", "2026-09-07")]
    assert (
        scalar(
            udred_db,
            "SELECT COUNT(*) FROM arc.udredningsret_mont WHERE OBJEKTID_FORLOBSMARKOR = ?",
            OBJ["d"],
        )
        == 0
    )
    # F13: the reissued OBJEKTID on ONE duplicate marker did not swap the two rows —
    # each loebenummer still carries the same content, only f's OBJEKTID changed
    plans_after = dict(
        rows(
            udred_db,
            "SELECT loebenummer, ALDER FROM arc.udredningsret_mont WHERE FORLOEBSMARKOER = 'AFX01D4' AND dw_is_deleted = 0",
        )
    )
    assert plans_after == plans_before
    assert after[OBJ_F_REISSUED] == before["5001_6"]
    assert after[OBJ["e"]] == before["5001_5"]
    # HF2: DW_EK 200 -> 250, unchanged key
    assert after[OBJ["g"]] == before["5002_1"]
    assert (
        scalar(
            udred_db,
            "SELECT DW_EK_HelbredsForloeb FROM arc.udredningsret_mont WHERE OBJEKTID_FORLOBSMARKOR = ?",
            OBJ["g"],
        )
        == "250"
    )
    # new marker inserted, the interruption soft-deleted (the source is one full snapshot)
    assert (
        scalar(
            udred_db,
            "SELECT COUNT(*) FROM arc.udredningsret_mont WHERE FORLOEBSMARKOER = 'AFX01D1' AND dw_is_deleted = 0",
        )
        == 1
    )
    assert (
        scalar(
            udred_db,
            "SELECT dw_is_deleted FROM arc.udredningsret_mont WHERE OBJEKTID_FORLOBSMARKOR = ?",
            OBJ["i"],
        )
        == 1
    )
    assert result.metadata["rows_inserted"] == 1
    # only the three real changes (OBJEKTID d -> j, OBJEKTID f reissued, DW_EK 200 -> 250);
    # the F_ID renumbering of HF1's other markers does not count
    assert result.metadata["rows_updated"] == 3
    assert result.metadata["rows_soft_deleted"] == 1
    assert result.metadata["helbredsforloeb_genfindelse"]["andel"] == 1.0
    assert scalar(udred_db, "SELECT COUNT(*) FROM meta.UdredningsretLeverance") == 2
    run_gate(udred_db)
    assert checkpoint(udred_db) == B_ID


def test_3_koersel_uden_ny_fil_laeser_intet(udred_db, delivery_dir, monkeypatch):
    """Delivery detection: same files on the share, checkpoint at B -> extract is a declared
    no-op (empty manifest, ext untouched), archive and gate short-circuit."""
    snapshot_sql = (
        "SELECT COUNT(*), SUM(CAST(dw_is_deleted AS INT)), MAX(dw_timestamp_update), "
        "MAX(CAST(leverance_dato AS NVARCHAR(10))) FROM arc.udredningsret_mont"
    )
    before = rows(udred_db, snapshot_sql)[0]
    ext_before = rows(
        udred_db, "SELECT COUNT(*), MAX(dw_timestamp_insert) FROM ext.udredningsret_mont"
    )[0]

    extract = run_extract(udred_db, delivery_dir, monkeypatch)
    assert extract.metadata["besluttet"].startswith("ingen ny leverance")
    assert extract.metadata["kandidater"] == 0
    assert scalar(udred_db, MANIFEST_ROWS) == 0
    # ext still holds B, untouched (same rows, same insert timestamps)
    assert tuple(
        rows(udred_db, "SELECT COUNT(*), MAX(dw_timestamp_insert) FROM ext.udredningsret_mont")[0]
    ) == tuple(ext_before)

    result = run_archive(udred_db)
    assert result.metadata["besluttet"].startswith("no-op")
    assert result.metadata["rows_inserted"] == 0
    assert scalar(udred_db, KEY_TABLE_EXISTS) == 0  # never built
    assert tuple(rows(udred_db, snapshot_sql)[0]) == tuple(before)
    assert scalar(udred_db, "SELECT COUNT(*) FROM meta.UdredningsretLeverance") == 2

    gate = run_gate(udred_db)
    assert "uaendret" in gate.metadata["besluttet"]
    assert checkpoint(udred_db) == B_ID


def test_3b_genkoersel_efter_checkpoint_reset_er_idempotent(udred_db, delivery_dir, monkeypatch):
    """The same delivery again (checkpoint deliberately reset to A): nothing is touched in
    arc — verified in the table, not only in the counters."""
    DfmManager(udred_db, dg.build_asset_context()).set_delivery_checkpoint(UNIT, A_ID)
    snapshot_sql = (
        "SELECT COUNT(*), SUM(CAST(dw_is_deleted AS INT)), MAX(dw_timestamp_update), "
        "MAX(CAST(leverance_dato AS NVARCHAR(10))) FROM arc.udredningsret_mont"
    )
    before = rows(udred_db, snapshot_sql)[0]
    extract = run_extract(udred_db, delivery_dir, monkeypatch)
    assert extract.metadata["besluttet"] == "indlaest"
    assert extract.metadata["leverance"]["fil"] == B_FILE
    result = run_archive(udred_db)
    assert result.metadata["rows_inserted"] == 0
    assert result.metadata["rows_updated"] == 0
    assert result.metadata["rows_soft_deleted"] == 0
    assert tuple(rows(udred_db, snapshot_sql)[0]) == tuple(before)
    # the delivery register did not get a new row — the same (kildetabel, timestamp) is upserted
    assert scalar(udred_db, "SELECT COUNT(*) FROM meta.UdredningsretLeverance") == 2
    run_gate(udred_db)
    assert checkpoint(udred_db) == B_ID


def test_4_genfindelsesguard(udred_db, delivery_dir, monkeypatch):
    """HF2 gets a new, earlier first marker -> 2 of 3 keys recovered -> the guard fails."""
    build_delivery(delivery_dir, C_FILE, "20260911", "2026-09-11", rows_c())
    run_extract(udred_db, delivery_dir, monkeypatch)
    with pytest.raises(ValueError, match="recovered in the delivery"):
        run_archive(udred_db)
    # on failure the key table is KEPT for post mortem, and the checkpoint is not advanced
    assert scalar(udred_db, KEY_TABLE_EXISTS) == 1
    assert checkpoint(udred_db) == B_ID
    # deliberate override: HF2's old row is soft-deleted and recreated under a new key
    result = run_archive(udred_db, min_helbredsforloeb_genfindelse=0.5)
    assert result.metadata["rows_soft_deleted"] == 1
    assert result.metadata["rows_inserted"] == 2
    assert (
        scalar(
            udred_db,
            "SELECT COUNT(DISTINCT helbredsforloeb_noegle) FROM arc.udredningsret_mont WHERE dw_is_deleted = 0",
        )
        == 3
    )
    assert scalar(udred_db, KEY_TABLE_EXISTS) == 0
    run_gate(udred_db)
    assert checkpoint(udred_db) == C_ID


def test_5_retningsguard_og_pin(udred_db, delivery_dir, monkeypatch):
    """Delivery A (LPR 2026-08-30) must not be merged on top of the archive at 2026-09-11.
    The pin bypasses the checkpoint (A is older than C) and never advances it."""
    extract = run_extract(udred_db, delivery_dir, monkeypatch, on_or_before="2026-08-31")
    assert "pin" in extract.metadata["besluttet"]
    assert (
        scalar(udred_db, "SELECT LPROPDATERINGSDATO FROM ext.udredningsret_leveranceinfo")
        == "2026-08-30"
    )
    assert scalar(udred_db, f"SELECT ryk_checkpoint FROM ext.{udredningsret.MANIFEST_TABLE}") == 0
    live_before = scalar(
        udred_db, "SELECT COUNT(*) FROM arc.udredningsret_mont WHERE dw_is_deleted = 0"
    )
    with pytest.raises(ValueError, match="older delivery must not be merged"):
        run_archive(udred_db)
    # the archive is untouched: same live rows, no new delivery registered
    assert (
        scalar(udred_db, "SELECT COUNT(*) FROM arc.udredningsret_mont WHERE dw_is_deleted = 0")
        == live_before
    )
    assert scalar(udred_db, "SELECT COUNT(*) FROM meta.UdredningsretLeverance") == 3
    # even if the gate were reached, a pinned run never advances the checkpoint
    gate = run_gate(udred_db)
    assert gate.metadata["besluttet"].startswith("pin")
    assert checkpoint(udred_db) == C_ID

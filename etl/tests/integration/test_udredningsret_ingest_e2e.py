# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""E2E integration test for udredningsret ingestion (SDS LPR) against the devcontainer MSSQL.

Runs the whole chain with synthetic tar.bz2 deliveries (synthetic CPRs):

    DDL (tables/udredningsret) -> extract (tar -> ext + manifest) -> archive
    (key table -> MERGE -> arc) -> checkpoint gate (meta.DFMState)

and verifies the semantics that carry the design (B3/B6 on feature 27631 and
the delivery-detection pattern):

1. BUSINESS KEYS: helbredsforloeb, markoer and loebenummer are computed from
   the content; fully identical rows collapse into one with antal_kilderaekker;
   duplicates on (helbredsforloeb, code, date) get a loebenummer. The gate
   stamps the delivery checkpoint; the key table is dropped after the merge.
2. STABILITY DESPITE RENUMBERING: the next delivery renumbers F_ID and
   DW_EK_HelbredsForloeb and reissues OBJEKTIDs — including on ONE of two
   duplicate markers (review F13) — and the arc rows keep their keys, their
   loebenummer and their content. A pure PK/F_ID renumbering is not a merge
   update (leverance_dato and dw_timestamp_update stay), but the values ARE
   refreshed afterwards (decision U1) and counted in surrogat_id_opdateret.
3. DELIVERY DETECTION: a run without a new file reads nothing (empty manifest,
   ext untouched, archive and gate no-op). After a checkpoint reset the same
   file re-merges as 0/0/0.
4. RECOVERY: when the first marker changes for helbredsforloeb, the old rows
   are soft-deleted, the new rows are inserted and the recovery share is recorded.
5. DIRECTION GUARD + PIN: a pinned (older) delivery bypasses the checkpoint,
   must not be merged on top of a newer archive, and never advances the
   checkpoint.
6. REGROUPING (decision U15): a delivery in which SDS moves markers between
   F_ID groups inside one helbredsforloeb and changes NOTHING else — first a
   SPLIT, then a MERGE of the two groups back into one. The archive merge
   reports 0/0/0 both times (PK/F_ID are outside change-detect, U1), the new
   grouping lands in arc, and the detector bumps dw_timestamp_update on exactly
   that helbredsforloeb's live rows so staging re-reads it. A pure RELABEL of
   another helbredsforloeb in the same delivery bumps nothing — which is what
   proves U1 itself still holds.
7. THE BLANK BOUNDARY: markers leaving and re-entering forloeb membership
   (F_ID blank / non-blank). A WHOLE group going blank, and blank markers given
   an F_ID while another group stands, are bijections on labels — both of the
   detector's inequalities stay silent — but they add or remove a group in the
   archive and hence in the fact's control total. Only the third HAVING term
   sees them.

The tests share module-scoped DB state and run in definition order.
Requires USE_REAL_DB_FOR_TESTS=true and ENABLE_TEST_WRITES=true (devcontainer).
"""

import csv
import io
import os
import tarfile
from datetime import datetime

import dagster as dg
import pytest
from core.dfm_manager import DfmManager
from core.resources import FileShareResource
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


F_ID_IDX = UDREDNINGSRET_MONT.source_columns.index("F_ID")
PK_IDX = UDREDNINGSRET_MONT.source_columns.index("PK")
KODE_IDX = UDREDNINGSRET_MONT.source_columns.index("FORLOEBSMARKOER")
DATO_IDX = UDREDNINGSRET_MONT.source_columns.index("FORLOEBSMARKOER_DATO")
ALDER_IDX = UDREDNINGSRET_MONT.source_columns.index("ALDER")

# HF1's markers that move into a second F_ID group in delivery D, as (code, date). The
# referral and the two fully identical AFV01B rows stay behind in 9001 — both of those
# keep the SAME F_ID, or they would stop being identical and antal_kilderaekker would
# change, which the merge WOULD see.
HF1_UDSPALTET = {("AFX01C", "2026-02-01"), ("AFX01D4", "2026-01-20"), ("AFX01D1", "2026-01-25")}


def _relabel(row: list[str], fid: str) -> list[str]:
    """The same marker in another F_ID group. PK follows F_ID, as in the source
    (PK = F_ID_loebenr); nothing else is touched. Both are SURROGATE_ID_COLUMNS, so no
    change-detect column differs and the merge cannot see the difference (decision U1).

    fid = "" is the source's way of saying the marker belongs to NO udredningsforloeb
    (1.17 M markers, 14 %): F_ID is blank and PK keeps only its sequence number. The
    extract turns a blank field into NULL, so those markers land in arc with F_ID NULL."""
    out = list(row)
    out[F_ID_IDX] = fid
    out[PK_IDX] = f"{fid}_{row[PK_IDX].split('_', 1)[1]}"
    return out


def rows_d() -> list[list[str]]:
    """Delivery D: not one content byte differs from C — only F_ID and the PK that
    follows it. HF1's single group 9001 is SPLIT in two (9001 keeps the referral and the
    AFV01B pair, 9002 takes the other four rows) = a REGROUP. HF2 is relabelled
    5002 -> 8002 as a whole = a pure bijection. HF3 is untouched."""
    out = []
    for row in rows_c():
        group = row[F_ID_IDX]
        if group == "9001" and (row[KODE_IDX], row[DATO_IDX]) in HF1_UDSPALTET:
            out.append(_relabel(row, "9002"))
        elif group == "5002":
            out.append(_relabel(row, "8002"))
        else:
            out.append(list(row))
    return out


def rows_e() -> list[list[str]]:
    """Delivery E: again only F_ID/PK. HF1's TWO groups are MERGED back into one (7001) —
    the direction only the SECOND HAVING inequality catches, since |R| = |old| = 2 leaves
    the first one silent and only |new| = 1 differs. HF2 is relabelled once more,
    8002 -> 6002; HF3 is still untouched."""
    out = []
    for row in rows_d():
        group = row[F_ID_IDX]
        if group in ("9001", "9002"):
            out.append(_relabel(row, "7001"))
        elif group == "8002":
            out.append(_relabel(row, "6002"))
        else:
            out.append(list(row))
    return out


def rows_f() -> list[list[str]]:
    """Delivery F: HF1 is relabelled 7001 -> 4001 as a WHOLE (a bijection), and ONE of its
    markers ALSO has a real content change (ALDER on the AFX01D1 marker). The merge
    therefore writes that one row — including its new F_ID — while its five siblings still
    carry the old label at the moment the detector reads arc. Without the leverance_dato
    prune the detector would compare (4001 -> 4001) against (7001 -> 4001) and report a
    regrouping that never happened. HF2 and HF3 are untouched."""
    out = []
    for row in rows_e():
        if row[F_ID_IDX] != "7001":
            out.append(list(row))
            continue
        relabelled = _relabel(row, "4001")
        if (relabelled[KODE_IDX], relabelled[DATO_IDX]) == ("AFX01D1", "2026-01-25"):
            relabelled[ALDER_IDX] = "60"
        out.append(relabelled)
    return out


def rows_g() -> list[list[str]]:
    """Delivery G: again only F_ID/PK. HF1's four moved markers lose their F_ID while the
    referral and the AFV01B pair keep 4001 — a group going PARTLY blank, which the first
    inequality catches on its own. HF2's ONLY group goes WHOLLY blank: |R| = |old| =
    |new| = 1, so both inequalities stay silent and only the blank-boundary term sees
    that the archive lost a whole (helbredsforloeb, F_ID) group. HF3 is relabelled
    5003 -> 3003 (a pure bijection)."""
    out = []
    for row in rows_f():
        group = row[F_ID_IDX]
        if group == "4001" and (row[KODE_IDX], row[DATO_IDX]) in HF1_UDSPALTET:
            out.append(_relabel(row, ""))
        elif group == "6002":
            out.append(_relabel(row, ""))
        elif group == "5003":
            out.append(_relabel(row, "3003"))
        else:
            out.append(list(row))
    return out


def rows_h() -> list[list[str]]:
    """Delivery H: the blank markers are given an F_ID again, and this is the case the
    bijection argument cannot see AT ALL. HF1's four blank markers become 4002 while the
    two rows of 4001 stand: the pairs are (4001 -> 4001) and (#BLANK -> 4002), so
    |R| = |old| = |new| = 2 and both inequalities pass — yet the archive gains a group.
    HF2's two blank markers become 7002 (1/1/1, the same blind spot the other way round).
    HF3 is untouched."""
    out = []
    for row in rows_g():
        if row[F_ID_IDX]:
            out.append(list(row))
        elif (row[KODE_IDX], row[DATO_IDX]) in HF1_UDSPALTET:
            out.append(_relabel(row, "4002"))
        else:
            out.append(_relabel(row, "7002"))
    return out


def scalar(resource, sql: str, *params):
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, *params)
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            cursor.close()


def execute(resource, sql: str, *params) -> None:
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, *params)
            conn.commit()
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
    return DfmManager(resource, dg.build_asset_context()).get_checkpoint("dfm", UNIT)


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
    execute_sql_file(resource, "migrations/20261005_01_dfm_run_log_source_columns.sql")
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


def run_extract(resource, base_dir, on_or_before: str | None = None):
    context = dg.build_asset_context()
    config = udredningsret.UdredningsretExtractConfig(delivery_on_or_before=on_or_before)
    share = FileShareResource(base_dir=str(base_dir))
    return udredningsret._run_extract(context, resource, share, config)


def run_archive(resource):
    context = dg.build_asset_context()
    return udredningsret_archive._run_archive(
        context,
        resource,
        UDREDNINGSRET_MONT,
        udredningsret_archive.UdredningsretArchiveConfig(),
    )


def run_gate(resource):
    return udredningsret_archive._run_checkpoint_gate(dg.build_asset_context(), resource)


A_FILE = "Region_1083_LPR_Udredningsret_U_20260831193918.tar.bz2"
B_FILE = "Region_1083_LPR_Udredningsret_U_20260907193544.tar.bz2"
C_FILE = "Region_1083_LPR_Udredningsret_M_20260912001030.tar.bz2"
D_FILE = "Region_1083_LPR_Udredningsret_U_20260919120000.tar.bz2"
E_FILE = "Region_1083_LPR_Udredningsret_U_20260926120000.tar.bz2"
F_FILE = "Region_1083_LPR_Udredningsret_U_20261003120000.tar.bz2"
G_FILE = "Region_1083_LPR_Udredningsret_U_20261010120000.tar.bz2"
H_FILE = "Region_1083_LPR_Udredningsret_U_20261017120000.tar.bz2"
A_ID, B_ID, C_ID = "2026-08-31T19:39:18", "2026-09-07T19:35:44", "2026-09-12T00:10:30"
D_ID, E_ID = "2026-09-19T12:00:00", "2026-09-26T12:00:00"
# not F_ID: that is the SDS column this whole scenario is about
F_FILE_ID = "2026-10-03T12:00:00"
G_ID, H_ID = "2026-10-10T12:00:00", "2026-10-17T12:00:00"

# dw_timestamp_update is DATETIME2(0), and the bump writes SYSDATETIME() to the second:
# a bump within the same second as the previous value would be invisible. Every live row
# is therefore stamped with this baseline right before the delivery, so "bumped" and
# "not bumped" are exact statements about THIS delivery and not a race against the clock.
BASISSTEMPEL = datetime(2020, 1, 1)

KEY_TABLE_EXISTS = "SELECT COUNT(*) FROM sys.objects WHERE object_id = OBJECT_ID('[ext].[udredningsret_mont_noegler]')"
MANIFEST_ROWS = f"SELECT COUNT(*) FROM ext.{udredningsret.MANIFEST_TABLE}"


def test_1_foerste_leverance_faar_forretningsnoegler_og_checkpoint(udred_db, delivery_dir):
    assert checkpoint(udred_db) is None
    build_delivery(delivery_dir, A_FILE, "20260830", "2026-08-30", rows_a())
    result = run_extract(udred_db, delivery_dir)
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


def test_2_omnummerering_og_genudstedelse_bevarer_noeglerne(udred_db, delivery_dir):
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
    stamp_a_before = scalar(
        udred_db, "SELECT dw_timestamp_update FROM arc.udredningsret_mont WHERE PK = '5001_1'"
    )

    build_delivery(delivery_dir, B_FILE, "20260906", "2026-09-06", rows_b())
    extract = run_extract(udred_db, delivery_dir)
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
    # Pure renumbering of F_ID/PK is not a change: the row keeps A's leverance_dato
    # and dw_timestamp_update (SURROGATE_ID_COLUMNS outside change-detect) — but the
    # ids themselves are refreshed to B's values after the merge (decision U1).
    assert [
        tuple(r)
        for r in rows(
            udred_db,
            "SELECT F_ID, PK, CAST(leverance_dato AS NVARCHAR(10)) FROM arc.udredningsret_mont WHERE OBJEKTID_FORLOBSMARKOR = ?",
            OBJ["a"],
        )
    ] == [("9001", "9001_1", "2026-08-31")]
    assert (
        scalar(
            udred_db,
            "SELECT dw_timestamp_update FROM arc.udredningsret_mont WHERE OBJEKTID_FORLOBSMARKOR = ?",
            OBJ["a"],
        )
        == stamp_a_before
    )
    # no live HF1 row is left with A's F_ID
    assert (
        scalar(
            udred_db,
            "SELECT COUNT(*) FROM arc.udredningsret_mont WHERE F_ID = '5001' AND dw_is_deleted = 0",
        )
        == 0
    )
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
    # U1: the HF1 rows renumbered 5001 -> 9001 WITHOUT any other change got PK/F_ID
    # refreshed after the merge: a (AFV01A1), b+c (AFV01B, collapsed) and e (AFX01D4
    # plan with ALDER 50). d -> j and f were written by the merge itself, the new
    # AFX01D1 marker was inserted with 9001, HF2/HF3 keep F_ID 5002/5003.
    assert result.metadata["surrogat_id_opdateret"] == 3
    assert result.metadata["helbredsforloeb_genfindelse"]["andel"] == 1.0
    assert scalar(udred_db, "SELECT COUNT(*) FROM meta.UdredningsretLeverance") == 2
    run_gate(udred_db)
    assert checkpoint(udred_db) == B_ID


def test_3_koersel_uden_ny_fil_laeser_intet(udred_db, delivery_dir):
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

    extract = run_extract(udred_db, delivery_dir)
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


def test_3b_genkoersel_efter_checkpoint_reset_er_idempotent(udred_db, delivery_dir):
    """The same delivery again (checkpoint deliberately reset to A): nothing is touched in
    arc — verified in the table, not only in the counters."""
    DfmManager(udred_db, dg.build_asset_context()).set_checkpoint("dfm", UNIT, value=A_ID)
    snapshot_sql = (
        "SELECT COUNT(*), SUM(CAST(dw_is_deleted AS INT)), MAX(dw_timestamp_update), "
        "MAX(CAST(leverance_dato AS NVARCHAR(10))) FROM arc.udredningsret_mont"
    )
    before = rows(udred_db, snapshot_sql)[0]
    extract = run_extract(udred_db, delivery_dir)
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


def test_4_foerste_markoer_skifter(udred_db, delivery_dir):
    """HF2 gets a new, earlier first marker -> 2 of 3 keys recovered, the old row is soft-deleted."""
    build_delivery(delivery_dir, C_FILE, "20260911", "2026-09-11", rows_c())
    run_extract(udred_db, delivery_dir)
    result = run_archive(udred_db)
    assert result.metadata["helbredsforloeb_genfindelse"]["genfundet"] == 2
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


def test_5_retningsguard_og_pin(udred_db, delivery_dir):
    """Delivery A (LPR 2026-08-30) must not be merged on top of the archive at 2026-09-11.
    The pin bypasses the checkpoint (A is older than C) and never advances it."""
    extract = run_extract(udred_db, delivery_dir, on_or_before="2026-08-31")
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


# ---------------------------------------------------------------------------
# omgruppering uden indholdsaendring (decision U15)
# ---------------------------------------------------------------------------


def _hf_noegle(resource, objektid: str) -> str:
    return scalar(
        resource,
        "SELECT helbredsforloeb_noegle FROM arc.udredningsret_mont "
        "WHERE OBJEKTID_FORLOBSMARKOR = ? AND dw_is_deleted = 0",
        objektid,
    )


def _grupper(resource, hf: str) -> dict:
    """F_ID -> number of live arc rows in that group, for one helbredsforloeb."""
    return {
        r[0]: r[1]
        for r in rows(
            resource,
            "SELECT F_ID, COUNT(*) FROM arc.udredningsret_mont "
            "WHERE helbredsforloeb_noegle = ? AND dw_is_deleted = 0 GROUP BY F_ID",
            hf,
        )
    }


def _stempler(resource, hf: str) -> list:
    return [
        r[0]
        for r in rows(
            resource,
            "SELECT dw_timestamp_update FROM arc.udredningsret_mont "
            "WHERE helbredsforloeb_noegle = ? AND dw_is_deleted = 0",
            hf,
        )
    ]


def _nulstil_stempler(resource) -> None:
    execute(
        resource,
        "UPDATE arc.udredningsret_mont SET dw_timestamp_update = ? WHERE dw_is_deleted = 0",
        BASISSTEMPEL,
    )


def test_6_udspaltning_af_en_f_id_gruppe_bumper_kun_det_omgrupperede_helbredsforloeb(
    udred_db, delivery_dir
):
    """U15: SDS moves markers between F_ID groups inside ONE helbredsforloeb and changes
    nothing else. Every change-detect column is byte-identical, so the merge reports
    0/0/0 and U1's id refresh would otherwise write the new grouping onto rows whose
    dw_timestamp_update never moved: staging would keep the old grouping and the fact's
    control total (U13) would fail the whole nightly chain, run after run. The detector
    bumps exactly that helbredsforloeb; a pure RELABEL of another one in the SAME
    delivery bumps nothing — the assertion that proves U1 itself still holds."""
    hf1 = _hf_noegle(udred_db, OBJ["a"])
    hf2 = _hf_noegle(udred_db, OBJ["g"])
    hf3 = _hf_noegle(udred_db, OBJ["h"])
    assert len({hf1, hf2, hf3}) == 3
    assert _grupper(udred_db, hf1) == {"9001": 6}
    assert _grupper(udred_db, hf2) == {"5002": 2}
    assert _grupper(udred_db, hf3) == {"5003": 1}
    _nulstil_stempler(udred_db)

    build_delivery(delivery_dir, D_FILE, "20260918", "2026-09-18", rows_d())
    extract = run_extract(udred_db, delivery_dir)
    assert extract.metadata["kandidater"] == 1
    result = run_archive(udred_db)

    # (a) the merge saw nothing — PK/F_ID are outside change-detect (U1) ...
    assert result.metadata["rows_inserted"] == 0
    assert result.metadata["rows_updated"] == 0
    assert result.metadata["rows_soft_deleted"] == 0
    # ... and wrote nothing: not one row carries this delivery's date, so the detector's
    # own prune (dw_timestamp_update from before the merge started) removed nothing either
    assert (
        scalar(
            udred_db,
            "SELECT COUNT(*) FROM arc.udredningsret_mont WHERE leverance_dato = '2026-09-19'",
        )
        == 0
    )

    # (c) the new grouping IS in arc: one group became two over the same six rows
    assert _grupper(udred_db, hf1) == {"9001": 2, "9002": 4}
    assert _grupper(udred_db, hf2) == {"8002": 2}
    assert _grupper(udred_db, hf3) == {"5003": 1}
    # the two fully identical AFV01B rows stayed collapsed (both kept 9001)
    assert (
        scalar(
            udred_db,
            "SELECT antal_kilderaekker FROM arc.udredningsret_mont "
            "WHERE FORLOEBSMARKOER = 'AFV01B' AND dw_is_deleted = 0",
        )
        == 2
    )
    # 4 regrouped rows in HF1 + HF2's 2 relabelled rows
    assert result.metadata["surrogat_id_opdateret"] == 6

    # (b) every live row of the REGROUPED helbredsforloeb is back in staging's delta scope
    assert result.metadata["omgrupperinger"] == 1
    stempler_hf1 = _stempler(udred_db, hf1)
    assert len(stempler_hf1) == 6
    assert all(stempel > BASISSTEMPEL for stempel in stempler_hf1)
    # (d) the pure relabel bumped nothing: a renumbering is still not a change (U1),
    # and neither is a helbredsforloeb the delivery did not touch at all
    assert _stempler(udred_db, hf2) == [BASISSTEMPEL, BASISSTEMPEL]
    assert _stempler(udred_db, hf3) == [BASISSTEMPEL]

    run_gate(udred_db)
    assert checkpoint(udred_db) == D_ID


def test_7_sammenlaegning_af_to_f_id_grupper_fanges_ogsaa(udred_db, delivery_dir):
    """The other direction, and the one only the SECOND HAVING inequality catches: HF1's
    two groups are merged back into one. |R| = |old| = 2 leaves the first inequality
    silent; only |new| = 1 differs. Again nothing else changes, and again the relabelled
    and the untouched helbredsforloeb are left alone."""
    hf1 = _hf_noegle(udred_db, OBJ["a"])
    hf2 = _hf_noegle(udred_db, OBJ["g"])
    hf3 = _hf_noegle(udred_db, OBJ["h"])
    assert _grupper(udred_db, hf1) == {"9001": 2, "9002": 4}
    _nulstil_stempler(udred_db)

    build_delivery(delivery_dir, E_FILE, "20260925", "2026-09-25", rows_e())
    run_extract(udred_db, delivery_dir)
    result = run_archive(udred_db)

    assert result.metadata["rows_inserted"] == 0
    assert result.metadata["rows_updated"] == 0
    assert result.metadata["rows_soft_deleted"] == 0
    assert _grupper(udred_db, hf1) == {"7001": 6}
    assert _grupper(udred_db, hf2) == {"6002": 2}
    assert _grupper(udred_db, hf3) == {"5003": 1}
    assert result.metadata["omgrupperinger"] == 1
    assert result.metadata["surrogat_id_opdateret"] == 8  # HF1's 6 rows + HF2's 2

    assert all(stempel > BASISSTEMPEL for stempel in _stempler(udred_db, hf1))
    assert _stempler(udred_db, hf2) == [BASISSTEMPEL, BASISSTEMPEL]
    assert _stempler(udred_db, hf3) == [BASISSTEMPEL]

    run_gate(udred_db)
    assert checkpoint(udred_db) == E_ID


def test_8_delvist_opdateret_helbredsforloeb_er_en_ommaerkning_ikke_en_omgruppering(
    udred_db, delivery_dir
):
    """The prune, end to end — the detector's only false-positive class.
    HF1 is relabelled as a whole AND one of its markers has a real content change, so the
    merge writes that row (with its new F_ID) — and stamps it after the merge started,
    which is what takes it out of the comparison — while the five siblings still carry the
    old label. Compared without the prune, (4001 -> 4001) against (7001 -> 4001) looks
    non-bijective and the detector would report a regrouping that never happened. The
    helbredsforloeb is in staging's delta scope already through that one row — the scope
    is per helbredsforloeb (_scope_sql branch 1), not per row — so nothing must be bumped
    on top, and the metric must read 0: it counts 'helbredsforloeb needing a bump'."""
    hf1 = _hf_noegle(udred_db, OBJ["a"])
    hf2 = _hf_noegle(udred_db, OBJ["g"])
    assert _grupper(udred_db, hf1) == {"7001": 6}
    _nulstil_stempler(udred_db)

    build_delivery(delivery_dir, F_FILE, "20261002", "2026-10-02", rows_f())
    run_extract(udred_db, delivery_dir)
    result = run_archive(udred_db)

    # the merge saw the ALDER change on exactly one row and wrote it — with its new F_ID
    assert result.metadata["rows_updated"] == 1
    assert result.metadata["rows_inserted"] == 0
    assert result.metadata["rows_soft_deleted"] == 0
    aendret = "FROM arc.udredningsret_mont WHERE FORLOEBSMARKOER = 'AFX01D1' AND dw_is_deleted = 0"
    assert scalar(udred_db, f"SELECT ALDER {aendret}") == "60"
    assert (
        scalar(udred_db, f"SELECT CAST(leverance_dato AS NVARCHAR(10)) {aendret}") == "2026-10-03"
    )
    assert scalar(udred_db, f"SELECT dw_timestamp_update {aendret}") > BASISSTEMPEL

    # the whole helbredsforloeb carries the new label; the five rows the merge left alone
    # got theirs from the id refresh (decision U1)
    assert _grupper(udred_db, hf1) == {"4001": 6}
    assert result.metadata["surrogat_id_opdateret"] == 5

    # ... and that is a RELABEL, not a regrouping: nothing extra was bumped
    assert result.metadata["omgrupperinger"] == 0
    assert sorted(_stempler(udred_db, hf1)).count(BASISSTEMPEL) == 5
    assert _stempler(udred_db, hf2) == [BASISSTEMPEL, BASISSTEMPEL]

    run_gate(udred_db)
    assert checkpoint(udred_db) == F_FILE_ID


# ---------------------------------------------------------------------------
# markoerer ud og ind af forloebsmedlemskab (blank F_ID, decision U15)
# ---------------------------------------------------------------------------


def test_9_en_hel_gruppe_der_bliver_blank_er_en_omgruppering(udred_db, delivery_dir):
    """A whole F_ID group going blank is invisible to the bijection test. '#BLANK' is a
    label on both sides, but NOT a group downstream: stg.pre_Udredningsforloeb forms one
    forloeb per (helbredsforloeb, NON-BLANK F_ID) and the control total (U13) counts
    exactly those. HF2's only group goes blank, so |R| = |old| = |new| = 1 and BOTH
    inequalities stay silent — while the archive loses a group and arc_antal drops. Only
    the third HAVING term bumps it. HF1 goes partly blank (the first inequality already
    catches that), HF3 is a pure relabel and must stay untouched."""
    hf1 = _hf_noegle(udred_db, OBJ["a"])
    hf2 = _hf_noegle(udred_db, OBJ["g"])
    hf3 = _hf_noegle(udred_db, OBJ["h"])
    assert _grupper(udred_db, hf1) == {"4001": 6}
    assert _grupper(udred_db, hf2) == {"6002": 2}
    assert _grupper(udred_db, hf3) == {"5003": 1}
    _nulstil_stempler(udred_db)

    build_delivery(delivery_dir, G_FILE, "20261009", "2026-10-09", rows_g())
    run_extract(udred_db, delivery_dir)
    result = run_archive(udred_db)

    # not one change-detect column differs — the merge sees nothing (U1)
    assert result.metadata["rows_inserted"] == 0
    assert result.metadata["rows_updated"] == 0
    assert result.metadata["rows_soft_deleted"] == 0
    # a blank field is NULL in ext and hence in arc; the detector reads NULL and blank
    # alike (ISNULL(NULLIF(LTRIM(RTRIM(F_ID)), ''), '#BLANK'))
    assert _grupper(udred_db, hf1) == {"4001": 2, None: 4}
    assert _grupper(udred_db, hf2) == {None: 2}
    assert _grupper(udred_db, hf3) == {"3003": 1}
    assert result.metadata["surrogat_id_opdateret"] == 7  # 4 + 2 blanked, 1 relabelled

    # HF1 (partly blank) AND HF2 (wholly blank) — the second one is the one no count
    # comparison and neither inequality can see
    assert result.metadata["omgrupperinger"] == 2
    assert all(stempel > BASISSTEMPEL for stempel in _stempler(udred_db, hf1))
    assert all(stempel > BASISSTEMPEL for stempel in _stempler(udred_db, hf2))
    assert _stempler(udred_db, hf3) == [BASISSTEMPEL]

    run_gate(udred_db)
    assert checkpoint(udred_db) == G_ID


def test_10_blanke_markoerer_der_faar_f_id_igen_bumper_ogsaa(udred_db, delivery_dir):
    """The other direction, and the exact blind spot: HF1's four blank markers are given
    F_ID 4002 while the two rows of 4001 stand. The pairs are (4001 -> 4001) and
    (#BLANK -> 4002), so |R| = |old| = |new| = 2 — a bijection on labels, both
    inequalities silent — yet the archive GAINS a (helbredsforloeb, F_ID) group and the
    fact's control total would differ by one row, run after run, until someone ran
    full_load. HF2's blank pair likewise becomes 7002 (1/1/1). HF3, untouched, is the
    control."""
    hf1 = _hf_noegle(udred_db, OBJ["a"])
    hf2 = _hf_noegle(udred_db, OBJ["g"])
    hf3 = _hf_noegle(udred_db, OBJ["h"])
    assert _grupper(udred_db, hf1) == {"4001": 2, None: 4}
    assert _grupper(udred_db, hf2) == {None: 2}
    _nulstil_stempler(udred_db)

    build_delivery(delivery_dir, H_FILE, "20261016", "2026-10-16", rows_h())
    run_extract(udred_db, delivery_dir)
    result = run_archive(udred_db)

    assert result.metadata["rows_inserted"] == 0
    assert result.metadata["rows_updated"] == 0
    assert result.metadata["rows_soft_deleted"] == 0
    assert _grupper(udred_db, hf1) == {"4001": 2, "4002": 4}
    assert _grupper(udred_db, hf2) == {"7002": 2}
    assert _grupper(udred_db, hf3) == {"3003": 1}
    assert result.metadata["surrogat_id_opdateret"] == 6

    assert result.metadata["omgrupperinger"] == 2
    assert all(stempel > BASISSTEMPEL for stempel in _stempler(udred_db, hf1))
    assert all(stempel > BASISSTEMPEL for stempel in _stempler(udred_db, hf2))
    assert _stempler(udred_db, hf3) == [BASISSTEMPEL]

    run_gate(udred_db)
    assert checkpoint(udred_db) == H_ID

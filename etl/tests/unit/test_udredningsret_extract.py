# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the udredningsret extract (DSI/csv_files/udredningsret.py).

Synthetic tar.bz2 deliveries against a fake connection: filename parsing,
level-1 delivery detection (checkpoint + pin), pre-flight, staleness, schema
validation and — most importantly — the streaming loader's guards (export-log
control totals, field counts, mojibake, CPR format, ISO dates, missing
LEVERANCEINFO) plus the discard-on-error path and the manifest contract of
_run_extract. The database is faked; it is the stream logic that is tested.
Mirrors tests/unit/test_drg_extract.py.
"""

import csv
import io
import tarfile
from datetime import date, datetime

import pytest
from DSI.csv_files import udredningsret
from DSI.csv_files.udredningsret_datasets import (
    DATASETS,
    LEVERANCEINFO,
    UDREDNINGSRET_MONT,
)

pytestmark = pytest.mark.unit

DATO = "20260906"
DATA_FILE = f"UdredningsretMont_1083_{DATO}.csv"
INFO_FILE = f"UdredningsretMont_LEVERANCEINFO_1083_{DATO}.csv"
LOG_FILE = f"SDP_ExportLog_{DATO}.csv"
ARCHIVE_NAME = "Region_1083_LPR_Udredningsret_U_20260907193544.tar.bz2"
GYLDIG_CPR = "0101011111"  # synthetic, 10 digits


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


def _csv(columns, rows, encoding: str = "cp1252") -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=";", quoting=csv.QUOTE_ALL)
    writer.writerow(list(columns))
    writer.writerows(rows)
    return buffer.getvalue().encode(encoding)


def _tar(path, members: dict[str, bytes]) -> None:
    with tarfile.open(path, mode="w:bz2") as tar:
        for name in sorted(members):
            info = tarfile.TarInfo(name=f"1083/{name}")
            info.size = len(members[name])
            tar.addfile(info, io.BytesIO(members[name]))


def _data_rows(n: int, cpr: str = GYLDIG_CPR) -> list[list[str]]:
    row = {c: "1" for c in UDREDNINGSRET_MONT.source_columns}
    row["CPR"] = cpr
    row["FORLOEBSMARKOER_DATO"] = "2026-01-05"
    return [[row[c] for c in UDREDNINGSRET_MONT.source_columns] for _ in range(n)]


def export_log(counts: dict[str, int]) -> bytes:
    lines = ["FilNavn;AntalRaekker;FilStoerrelseBytes;Oprettet"]
    for filename, count in counts.items():
        lines.append(f"{filename};{count};0;2026-09-07 19:31:43")
    return "\n".join(lines).encode("cp1252")


def members(data_rows: int = 3, lpr: str = "2026-09-06") -> dict[str, bytes]:
    """Minimal valid delivery: data file + LEVERANCEINFO + export log."""
    return {
        DATA_FILE: _csv(UDREDNINGSRET_MONT.source_columns, _data_rows(data_rows)),
        INFO_FILE: _csv(LEVERANCEINFO.source_columns, [["UdredningsretMont", lpr]]),
        LOG_FILE: export_log({DATA_FILE: data_rows, INFO_FILE: 1}),
    }


def _delivery(tmp_path, filename: str, data_rows: int, with_log: bool = True) -> None:
    m = members(data_rows)
    if not with_log:
        del m[LOG_FILE]
    _tar(tmp_path / filename, m)


class FakeCursor:
    def __init__(self, store, fail_executemany: bool = False):
        self.store = store
        self.fail_executemany = fail_executemany

    def execute(self, sql, *params):
        self.store.setdefault("executed", []).append(" ".join(sql.split()))
        self.store.setdefault("params", []).append(params)

    def executemany(self, sql, rows):
        if self.fail_executemany:
            raise RuntimeError("String or binary data would be truncated")
        table = sql.split("[ext].[")[1].split("]")[0]
        self.store.setdefault(table, []).extend(rows)

    def fetchone(self):
        return None

    def close(self):
        pass


class FakeConn:
    def __init__(self, fail_executemany: bool = False):
        self.store: dict = {}
        self.fail_executemany = fail_executemany
        self.commits = 0

    def cursor(self):
        return FakeCursor(self.store, self.fail_executemany)

    def commit(self):
        self.commits += 1


class _Log:
    def __init__(self):
        self.warnings: list[str] = []
        self.infos: list[str] = []

    def info(self, message, *_):
        self.infos.append(message)

    def warning(self, message, *_):
        self.warnings.append(message)


class _Ctx:
    def __init__(self):
        self.log = _Log()


def _archive(tmp_path, name: str = ARCHIVE_NAME) -> udredningsret.UdredningsretArchiveFile:
    kadence, stamp = udredningsret.parse_archive_name(name)
    return udredningsret.UdredningsretArchiveFile(
        path=str(tmp_path / name), kadence=kadence, timestamp=stamp
    )


def _load(tmp_path, tar_members: dict[str, bytes], encoding: str = "cp1252"):
    _tar(tmp_path / ARCHIVE_NAME, tar_members)
    conn = FakeConn()
    context = _Ctx()
    result = udredningsret.load_delivery(conn, _archive(tmp_path), encoding, context)
    return conn.store, result, context


def _file(name: str) -> udredningsret.UdredningsretArchiveFile:
    kadence, stamp = udredningsret.parse_archive_name(name)
    return udredningsret.UdredningsretArchiveFile(
        path=f"/x/{name}", kadence=kadence, timestamp=stamp
    )


A = "Region_1083_LPR_Udredningsret_U_20260831193918.tar.bz2"
B = "Region_1083_LPR_Udredningsret_U_20260907193544.tar.bz2"
M = "Region_1083_LPR_Udredningsret_M_20260811234023.tar.bz2"


# ---------------------------------------------------------------------------
# filenames, delivery detection, pin, pre-flight, staleness
# ---------------------------------------------------------------------------


def test_parse_archive_name():
    assert udredningsret.parse_archive_name(ARCHIVE_NAME) == (
        "U",
        datetime(2026, 9, 7, 19, 35, 44),
    )
    assert udredningsret.parse_archive_name(M) == ("M", datetime(2026, 8, 11, 23, 40, 23))
    # the Arkiv/ formats and other sources must not match
    assert (
        udredningsret.parse_archive_name("Region_1083_Udredningsret_20220314145119.tar.bz2") is None
    )
    assert (
        udredningsret.parse_archive_name("Region_1083_LPR_Ventetider_U_20260817201602.tar.bz2")
        is None
    )


def test_identity_er_pakketidsstemplet_som_iso():
    assert _file(B).identity == "2026-09-07T19:35:44"
    assert udredningsret.DELIVERY_UNIT == "udredningsret_leverance_1083"


def test_select_candidates_uden_checkpoint_tager_alt():
    archives = [_file(M), _file(A), _file(B)]
    assert udredningsret.select_candidates(archives, None, None) == archives


def test_select_candidates_kun_strengt_nyere_end_checkpoint():
    """Level 1: a file already ingested (identity == checkpoint) is NOT a candidate."""
    archives = [_file(M), _file(A), _file(B)]
    assert udredningsret.select_candidates(archives, _file(A).identity, None) == [_file(B)]
    assert udredningsret.select_candidates(archives, _file(B).identity, None) == []


def test_select_candidates_pin_ignorerer_checkpoint():
    """A pin bypasses the checkpoint (it deliberately reloads an older state)."""
    archives = [_file(M), _file(A), _file(B)]
    pinned = udredningsret.select_candidates(archives, _file(B).identity, date(2026, 9, 1))
    assert pinned == [_file(M), _file(A)]
    assert udredningsret.select_candidates(archives, None, date(2026, 8, 1)) == []


def test_select_newest_vaelger_nyeste_brugbare_uanset_kadence(tmp_path):
    _delivery(tmp_path, M, 3)
    _delivery(tmp_path, A, 3)
    _delivery(tmp_path, B, 0)  # empty
    (tmp_path / "Arkiv").mkdir()
    archives = udredningsret.discover_archives(str(tmp_path))
    assert [a.kadence for a in archives] == ["M", "U", "U"]
    selected, notes = udredningsret.select_newest(archives, "cp1252")
    assert selected.filename == A
    assert len(notes) == 1 and "0 rows" in notes[0]


def test_is_usable_kraever_eksportlog(tmp_path):
    _delivery(tmp_path, ARCHIVE_NAME, 2, with_log=False)
    archive = udredningsret.discover_archives(str(tmp_path))[0]
    assert udredningsret.is_usable(archive, "cp1252") == "SDP_ExportLog is missing from the archive"


def test_check_delivery_age():
    archive = udredningsret.UdredningsretArchiveFile("/x/a", "U", datetime(2026, 9, 7, 19, 0))
    assert udredningsret.check_delivery_age(archive, date(2026, 9, 15)) == 8
    with pytest.raises(ValueError, match="days old"):
        udredningsret.check_delivery_age(archive, date(2026, 9, 20))
    assert udredningsret.check_delivery_age(archive, date(2026, 9, 20), max_age_days=30) == 13


def test_udredningsret_dir_fejler_haardt_uden_fallback(tmp_path, monkeypatch):
    monkeypatch.delenv(udredningsret.BASE_DIR_ENV, raising=False)
    with pytest.raises(ValueError, match="is unset"):
        udredningsret.udredningsret_dir()
    monkeypatch.setenv(udredningsret.BASE_DIR_ENV, str(tmp_path / "findes_ikke"))
    with pytest.raises(ValueError, match="not a directory"):
        udredningsret.udredningsret_dir()
    monkeypatch.setenv(udredningsret.BASE_DIR_ENV, str(tmp_path))
    assert udredningsret.udredningsret_dir() == str(tmp_path)


def test_base_dir_override_er_eksplicit_og_valideres(tmp_path, monkeypatch):
    """Run-config override wins over the variable, but is validated the same way —
    it is an explicit operator input, never a default."""
    monkeypatch.delenv(udredningsret.BASE_DIR_ENV, raising=False)
    assert udredningsret.udredningsret_dir(str(tmp_path)) == str(tmp_path)
    with pytest.raises(ValueError, match="not a directory"):
        udredningsret.udredningsret_dir(str(tmp_path / "findes_ikke"))
    with pytest.raises(ValueError, match="is unset"):
        udredningsret.udredningsret_dir("   ")  # blank override is no override
    monkeypatch.setenv(udredningsret.BASE_DIR_ENV, str(tmp_path / "findes_ikke"))
    assert udredningsret.udredningsret_dir(str(tmp_path)) == str(tmp_path)


# ---------------------------------------------------------------------------
# schema and ext DDL
# ---------------------------------------------------------------------------


def test_validate_member_schema():
    columns = list(UDREDNINGSRET_MONT.source_columns)
    udredningsret.validate_member_schema(UDREDNINGSRET_MONT, columns, "f.csv")
    with pytest.raises(ValueError, match="unknown columns"):
        udredningsret.validate_member_schema(UDREDNINGSRET_MONT, [*columns, "NY"], "f.csv")
    with pytest.raises(ValueError, match="missing"):
        udredningsret.validate_member_schema(UDREDNINGSRET_MONT, columns[:-1], "f.csv")


def test_ext_create_sql_har_leverancekolonner_og_bredde():
    sql = udredningsret._ext_create_sql(UDREDNINGSRET_MONT)
    assert sql.startswith("DROP TABLE IF EXISTS [ext].[udredningsret_mont];")
    assert "[kadence] NVARCHAR(1) NOT NULL" in sql
    assert "[leverance_tidsstempel] DATETIME2(0) NOT NULL" in sql
    assert "[CPR] NVARCHAR(500) NULL" in sql
    assert "[leverance_dato] DATE NOT NULL" in sql


def test_manifest_sql_matcher_write_manifest():
    """The manifest columns the archive reads must be the ones the extract writes."""
    sql = udredningsret.manifest_create_sql()
    assert sql.startswith("DROP TABLE IF EXISTS [ext].[udredningsret_leverance_manifest];")
    for column in (
        "filnavn",
        "kadence",
        "leverance_tidsstempel",
        "leverance_dato",
        "lpropdateringsdato",
        "checkpoint_enhed",
        "checkpoint_identitet",
        "ryk_checkpoint",
    ):
        assert f"[{column}]" in sql, column
    conn = FakeConn()
    udredningsret.write_manifest(conn, _file(B), "2026-09-06", advance_checkpoint=False)
    insert = conn.store["executed"][0]
    assert insert.startswith("INSERT INTO [ext].[udredningsret_leverance_manifest]")
    assert conn.store["params"][0] == (
        B,
        "U",
        "2026-09-07 19:35:44",
        "2026-09-07",
        "2026-09-06",
        "udredningsret_leverance_1083",
        "2026-09-07T19:35:44",
        0,
    )


def test_datasaet_prefix_skelner_data_og_leveranceinfo():
    assert udredningsret._dataset_prefix(DATA_FILE) == "UdredningsretMont"
    assert udredningsret._dataset_prefix(INFO_FILE) == "UdredningsretMont_LEVERANCEINFO"
    # a greedy regex would send Metadata_* into the data loader
    assert udredningsret._dataset_prefix(f"Metadata_{DATA_FILE}") is None


# ---------------------------------------------------------------------------
# the loader: stamping and control totals
# ---------------------------------------------------------------------------


def test_indlaesning_stempler_kadence_tidsstempel_og_leverancedato(tmp_path):
    store, result, _ = _load(tmp_path, members(3))
    rows = store["udredningsret_mont"]
    assert len(rows) == 3
    assert rows[0][0] == "U"  # kadence first
    assert rows[0][1] == "2026-09-07 19:35:44"  # the filename's timestamp
    assert rows[0][-1] == "2026-09-07"  # leverance_dato last
    assert len(rows[0]) == 2 + len(UDREDNINGSRET_MONT.source_columns) + 1
    assert store["udredningsret_leveranceinfo"][0][2:4] == ("UdredningsretMont", "2026-09-06")
    assert result["raekker"] == {DATA_FILE: 3, INFO_FILE: 1}
    assert result["lpropdateringsdato"] == "2026-09-06"
    assert result["cpr_ikke_10_cifre"] == 0
    assert result["blanke_datoer"] == {}
    # ext tables were recreated before the load
    assert store["executed"][0].startswith("DROP TABLE IF EXISTS [ext].[udredningsret_mont]")


def test_eksportlog_mismatch_fejler(tmp_path):
    m = members(3)
    m[LOG_FILE] = export_log({DATA_FILE: 999, INFO_FILE: 1})
    with pytest.raises(ValueError, match="deviate from SDP_ExportLog"):
        _load(tmp_path, m)


def test_manglende_eksportlog_fejler_i_loaderen(tmp_path):
    m = members(3)
    del m[LOG_FILE]
    with pytest.raises(ValueError, match="SDP_ExportLog is missing"):
        _load(tmp_path, m)


def test_metadata_medlem_springes_over_og_ukendt_medlem_advares(tmp_path):
    m = members(2)
    m[f"Metadata_{DATA_FILE}"] = b'"column_ordinal";"column_name"\r\n'
    m["Readme_1083.txt"] = b"ignore me"
    store, result, context = _load(tmp_path, m)
    assert len(store["udredningsret_mont"]) == 2
    assert any("unknown member Readme_1083.txt" in w for w in context.log.warnings)
    assert "Metadata" not in " ".join(result["raekker"])


def test_manglende_datafil_fejler(tmp_path):
    m = members(2)
    del m[DATA_FILE]
    m[LOG_FILE] = export_log({INFO_FILE: 1})
    with pytest.raises(ValueError, match="data file .* is missing"):
        _load(tmp_path, m)


def test_manglende_lpropdateringsdato_fejler(tmp_path):
    """Without LEVERANCEINFO the archive's direction guard cannot run."""
    m = members(2)
    del m[INFO_FILE]
    m[LOG_FILE] = export_log({DATA_FILE: 2})
    with pytest.raises(ValueError, match="LPROPDATERINGSDATO"):
        _load(tmp_path, m)


def test_forkert_feltantal_fejler(tmp_path):
    m = members(1)
    header = ";".join(f'"{c}"' for c in UDREDNINGSRET_MONT.source_columns)
    m[DATA_FILE] = f'{header}\n"kun";"to"\n'.encode("cp1252")
    with pytest.raises(ValueError, match="fields"):
        _load(tmp_path, m)


# ---------------------------------------------------------------------------
# the loader: ISO dates (review F14) — string order must be chronological
# ---------------------------------------------------------------------------


def test_lpropdateringsdato_der_ikke_er_iso_afvises(tmp_path):
    """'2026-09-07 00:00:00' would pass MAX() and fail later on NVARCHAR(10) truncation;
    'DD-MM-YYYY' would make the direction guard's string comparison meaningless."""
    for bad in ("2026-09-07 00:00:00", "07-09-2026", "20260907"):
        m = members(2, lpr=bad)
        with pytest.raises(ValueError, match="not ISO"):
            _load(tmp_path, m)


def test_forloebsmarkoer_dato_der_ikke_er_iso_afvises_uden_at_logge_vaerdien(tmp_path):
    """The helbredsforloeb key's 'earliest marker' is ordered as a string."""
    m = members(3)
    rows = _data_rows(3)
    bad = "05/01/2026"
    rows[1][UDREDNINGSRET_MONT.source_columns.index("FORLOEBSMARKOER_DATO")] = bad
    m[DATA_FILE] = _csv(UDREDNINGSRET_MONT.source_columns, rows)
    with pytest.raises(ValueError) as excinfo:
        _load(tmp_path, m)
    message = str(excinfo.value)
    assert "not ISO" in message and "FORLOEBSMARKOER_DATO" in message and "1" in message
    assert bad not in message


def test_blank_dato_taelles_men_fejler_ikke(tmp_path):
    m = members(3)
    rows = _data_rows(3)
    rows[2][UDREDNINGSRET_MONT.source_columns.index("FORLOEBSMARKOER_DATO")] = ""
    m[DATA_FILE] = _csv(UDREDNINGSRET_MONT.source_columns, rows)
    _, result, _ = _load(tmp_path, m)
    assert result["blanke_datoer"] == {"FORLOEBSMARKOER_DATO": 1}


# ---------------------------------------------------------------------------
# the loader: GDPR-safe quality guards (only column names and counts in the message)
# ---------------------------------------------------------------------------


def test_mojibake_fejler_uden_at_logge_vaerdien(tmp_path):
    m = members(2)
    rows = _data_rows(2)
    corrupt = "Odense ï¿½ Universitetshospital"
    label_index = UDREDNINGSRET_MONT.source_columns.index("FORLOEBSLABEL")
    rows[0][label_index] = corrupt
    m[DATA_FILE] = _csv(UDREDNINGSRET_MONT.source_columns, rows)
    with pytest.raises(ValueError) as excinfo:
        _load(tmp_path, m)
    message = str(excinfo.value)
    assert "encoding corruption" in message
    assert "FORLOEBSLABEL" in message
    assert corrupt not in message and "Odense" not in message


def test_u_fffd_fejler_ved_utf8_kilde(tmp_path):
    m = members(2)
    rows = _data_rows(2)
    rows[1][UDREDNINGSRET_MONT.source_columns.index("FORLOEBSLABEL")] = "tekst med �"
    m[DATA_FILE] = _csv(UDREDNINGSRET_MONT.source_columns, rows, encoding="utf-8")
    m[INFO_FILE] = _csv(
        LEVERANCEINFO.source_columns, [["UdredningsretMont", "2026-09-06"]], "utf-8"
    )
    m[LOG_FILE] = export_log({DATA_FILE: 2, INFO_FILE: 1})
    with pytest.raises(ValueError, match="encoding corruption"):
        _load(tmp_path, m, encoding="utf-8")


def test_cpr_format_afvigelse_over_graensen_fejler_uden_at_logge_vaerdien(tmp_path):
    """A shifted column yields many non-10-digit CPRs; the value must never be named."""
    m = members(4)
    skubbet = "ABC-IKKE-CPR"
    rows = _data_rows(4)
    cpr_index = UDREDNINGSRET_MONT.source_columns.index("CPR")
    rows[0][cpr_index] = skubbet
    rows[1][cpr_index] = skubbet
    m[DATA_FILE] = _csv(UDREDNINGSRET_MONT.source_columns, rows)
    with pytest.raises(ValueError) as excinfo:
        _load(tmp_path, m)
    message = str(excinfo.value)
    assert "not 10" in message
    assert "2 of 4" in message
    assert skubbet not in message and GYLDIG_CPR not in message


def test_cpr_format_afvigelse_under_graensen_taelles_kun(tmp_path):
    """A few replacement CPRs are normal — counted into metadata, no failure."""
    n = 200  # 1 of 200 = 0.5 % < MAX_CPR_FORMAT_DEVIATION (1 %)
    m = members(n)
    rows = _data_rows(n)
    rows[0][UDREDNINGSRET_MONT.source_columns.index("CPR")] = "erstatning"
    m[DATA_FILE] = _csv(UDREDNINGSRET_MONT.source_columns, rows)
    store, result, _ = _load(tmp_path, m)
    assert len(store["udredningsret_mont"]) == n
    assert result["cpr_ikke_10_cifre"] == 1


# ---------------------------------------------------------------------------
# the error path: buffers are discarded, the real cause surfaces (review F19)
# ---------------------------------------------------------------------------


def test_fejlsti_kasserer_buffere_og_bevarer_den_rigtige_aarsag(tmp_path):
    """Two rows sit in the buffer when the mojibake guard fires. Flushing them on the way
    out would run an INSERT that fails here — that SQL error must NOT replace the cause."""
    m = members(2)
    rows = _data_rows(2)
    rows[0][UDREDNINGSRET_MONT.source_columns.index("FORLOEBSLABEL")] = "x ï¿½ y"
    m[DATA_FILE] = _csv(UDREDNINGSRET_MONT.source_columns, rows)
    _tar(tmp_path / ARCHIVE_NAME, m)
    conn = FakeConn(fail_executemany=True)
    with pytest.raises(ValueError, match="encoding corruption"):
        udredningsret.load_delivery(conn, _archive(tmp_path), "cp1252", _Ctx())
    assert "udredningsret_mont" not in conn.store  # nothing was sent
    assert conn.commits == len(DATASETS)  # only the ext DDL was committed, never data


# ---------------------------------------------------------------------------
# _run_extract: delivery detection and the manifest contract
# ---------------------------------------------------------------------------


class _FakeResource:
    def __init__(self):
        self.conn = FakeConn()

    def get_connection(self):
        import contextlib

        @contextlib.contextmanager
        def _cm():
            yield self.conn

        return _cm()


class _FakeDfm:
    def __init__(self, checkpoint):
        self.checkpoint = checkpoint
        self.set_calls: list[tuple] = []

    def get_delivery_checkpoint(self, unit):
        assert unit == udredningsret.DELIVERY_UNIT
        return self.checkpoint

    def set_delivery_checkpoint(self, unit, identity):
        self.set_calls.append((unit, identity))


def _run(tmp_path, monkeypatch, checkpoint, on_or_before=None):
    monkeypatch.setenv(udredningsret.BASE_DIR_ENV, str(tmp_path))
    monkeypatch.setattr(udredningsret, "DfmManager", lambda mssql, context: _FakeDfm(checkpoint))
    resource = _FakeResource()
    result = udredningsret._run_extract(
        _Ctx(),
        resource,
        udredningsret.UdredningsretExtractConfig(delivery_on_or_before=on_or_before),
    )
    return resource.conn, result


def test_run_extract_uden_ny_fil_laeser_intet_og_skriver_tomt_manifest(tmp_path, monkeypatch):
    _delivery(tmp_path, A, 3)
    _delivery(tmp_path, B, 3)
    conn, result = _run(tmp_path, monkeypatch, checkpoint=_file(B).identity)
    executed = conn.store["executed"]
    # manifest recreated (empty), ext tables NOT touched, nothing inserted
    assert executed[0].startswith("DROP TABLE IF EXISTS [ext].[udredningsret_leverance_manifest]")
    assert not any("[ext].[udredningsret_mont]" in sql for sql in executed)
    assert not any(sql.startswith("INSERT") for sql in executed)
    assert "udredningsret_mont" not in conn.store
    assert result.metadata["besluttet"].startswith("ingen ny leverance")
    assert result.metadata["kandidater"] == 0
    assert result.metadata["nyeste_paa_sharet"] == "2026-09-07T19:35:44"


def test_run_extract_indlaeser_nyeste_nyere_end_checkpoint_og_skriver_manifest(
    tmp_path, monkeypatch
):
    _delivery(tmp_path, M, 3)
    _delivery(tmp_path, A, 3)
    _delivery(tmp_path, B, 3)
    conn, result = _run(tmp_path, monkeypatch, checkpoint=_file(M).identity)
    assert len(conn.store["udredningsret_mont"]) == 3
    assert conn.store["udredningsret_mont"][0][1] == "2026-09-07 19:35:44"  # B, not A
    manifest = [p for p in conn.store["params"] if p and p[0] == B]
    assert len(manifest) == 1 and manifest[0][-1] == 1  # ryk_checkpoint
    assert result.metadata["besluttet"] == "indlaest"
    assert result.metadata["kandidater"] == 2
    assert result.metadata["sprunget_over_mellemliggende"] == [A]
    assert conn.commits >= 2  # manifest DDL + data/manifest


def test_run_extract_pin_ignorerer_checkpoint_og_rykker_det_ikke(tmp_path, monkeypatch):
    _delivery(tmp_path, A, 3)
    _delivery(tmp_path, B, 3)
    conn, result = _run(
        tmp_path, monkeypatch, checkpoint=_file(B).identity, on_or_before="2026-09-01"
    )
    assert conn.store["udredningsret_mont"][0][1] == "2026-08-31 19:39:18"  # A
    manifest = [p for p in conn.store["params"] if p and p[0] == A]
    assert manifest[0][-1] == 0  # ryk_checkpoint = 0
    assert "pin" in result.metadata["besluttet"]


def test_run_extract_fejler_naar_alle_nye_kandidater_er_ubrugelige(tmp_path, monkeypatch):
    """A new file landed but is defective: NOT a silent no-op."""
    _delivery(tmp_path, A, 3)
    _delivery(tmp_path, B, 0)
    with pytest.raises(ValueError, match="unusable"):
        _run(tmp_path, monkeypatch, checkpoint=_file(A).identity)


def test_run_extract_fejler_uden_checkpoint_og_uden_brugbar_fil(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="No udredningsret delivery"):
        _run(tmp_path, monkeypatch, checkpoint=None)

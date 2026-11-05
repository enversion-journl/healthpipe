# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the shared SDS tar.bz2 machinery (DSI/csv_files/_sds_tar.py and
core/file_delivery.py).

The two modules hold what DRG, udredningsret and (next) LPR3 Grunddata genuinely
have in common. core/file_delivery.py holds the forward-only tar walk and the
non-seekable stream adapter; FileShareResource.root() checks the folder.
_sds_tar.py holds the chunked ext writer with its discard-on-error path, the
export-log parser, the ext DDL shape and the comparisons that feed each
source's own guards. The per-source messages are NOT here — they stay pinned by
tests/unit/test_drg_extract.py and tests/unit/test_udredningsret_extract.py.

The database is faked; it is the stream and buffer logic that is under test.
"""

import io
import random
import re
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest
from core import file_delivery
from core.resources import FileShareResource
from DSI.csv_files import _sds_tar

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# doubles
# ---------------------------------------------------------------------------


class FakeCursor:
    """Records what reached the driver. `fast_executemany` is set by the writer
    when present — a cursor WITHOUT it stands in for a non-pyodbc driver."""

    def __init__(self, store, fail_executemany: bool = False, with_fast: bool = True):
        self.store = store
        self.fail_executemany = fail_executemany
        if with_fast:
            self.fast_executemany = False

    def execute(self, sql, *params):
        self.store.setdefault("executed", []).append(sql)

    def executemany(self, sql, rows):
        self.store.setdefault("batches", []).append(list(rows))
        if self.fail_executemany:
            raise RuntimeError("String or binary data would be truncated")
        self.store.setdefault("rows", []).extend(rows)

    def close(self):
        self.store["closed"] = self.store.get("closed", 0) + 1


class FakeConn:
    def __init__(self, fail_executemany: bool = False, with_fast: bool = True):
        self.store: dict = {}
        self.commits = 0
        self._fail = fail_executemany
        self._with_fast = with_fast

    def cursor(self):
        return FakeCursor(self.store, self._fail, self._with_fast)

    def commit(self):
        self.commits += 1


class _Log:
    def __init__(self):
        self.infos: list[str] = []

    def info(self, message, *_):
        self.infos.append(message)


class _Ctx:
    def __init__(self):
        self.log = _Log()


def _writer(conn, table: str = "t", columns=("a", "b"), **kwargs) -> _sds_tar.ExtWriter:
    return _sds_tar.ExtWriter(conn, table, list(columns), _Ctx(), **kwargs)


class _StreamHandle:
    """A tarfile stream-mode file object: read only, seek/tell refuse."""

    def __init__(self, data: bytes):
        self._data = data
        self._pos = 0

    def read(self, size=-1):
        if size is None or size < 0:
            size = len(self._data) - self._pos
        chunk = self._data[self._pos : self._pos + size]
        self._pos += len(chunk)
        return chunk

    def seek(self, *_):
        raise io.UnsupportedOperation("seek")

    def tell(self):
        raise io.UnsupportedOperation("tell")

    def seekable(self):
        return False


# ---------------------------------------------------------------------------
# ExtWriter: chunking, insert shape, driver guard
# ---------------------------------------------------------------------------


def test_extwriter_sender_foerst_ved_chunk_graensen():
    """Rows must sit in the buffer until the chunk is full — a writer that sent
    per row would make a 16 GB delivery one round-trip per row."""
    conn = FakeConn()
    writer = _writer(conn, chunk_size=3)
    for i in range(2):
        writer.add((i, "x"))
    assert "batches" not in conn.store  # nothing sent yet
    assert writer.total == 0

    writer.add((2, "x"))
    assert [len(b) for b in conn.store["batches"]] == [3]
    assert writer.total == 3
    assert writer.buffer == []

    for i in range(3, 7):
        writer.add((i, "x"))
    assert [len(b) for b in conn.store["batches"]] == [3, 3]
    assert writer.total == 6

    writer.flush()  # the remainder
    assert [len(b) for b in conn.store["batches"]] == [3, 3, 1]
    assert writer.total == 7
    assert [row[0] for row in conn.store["rows"]] == list(range(7))


def test_extwriter_flush_uden_buffer_sender_ingenting():
    conn = FakeConn()
    writer = _writer(conn, chunk_size=2)
    writer.flush()
    writer.close()
    assert "batches" not in conn.store
    assert writer.total == 0


def test_extwriter_insert_sql_er_parametriseret_pr_kolonne():
    conn = FakeConn()
    writer = _writer(conn, table="drg_kontakt", columns=("takstsystem", "ID", "leverance_dato"))
    assert writer.insert_sql == (
        "INSERT INTO [ext].[drg_kontakt] ([takstsystem], [ID], [leverance_dato]) "
        "VALUES (?, ?, ?)"
    )


def test_extwriter_virker_uden_fast_executemany():
    """The attribute is pyodbc's; a driver without it must not crash the writer."""
    conn = FakeConn(with_fast=False)
    writer = _writer(conn, chunk_size=1)
    assert not hasattr(writer.cursor, "fast_executemany")
    writer.add((1, "x"))
    assert conn.store["rows"] == [(1, "x")]


def test_extwriter_saetter_fast_executemany_naar_driveren_har_den():
    conn = FakeConn()
    writer = _writer(conn)
    assert writer.cursor.fast_executemany is True


def test_extwriter_logger_fremdrift_paa_halvmillionen():
    """Progress is reported once per half million rows, in the SOURCE's own noun —
    the reason row_word is a parameter rather than a constant. Wound forward on
    the public counter instead of inserting half a million rows."""
    conn = FakeConn()
    context = _Ctx()
    writer = _sds_tar.ExtWriter(
        conn, "drg_kontakt", ["a"], context, chunk_size=1000, row_word="raekker"
    )
    writer.total = 499_000
    writer.buffer = [(i,) for i in range(1000)]
    writer.flush()
    assert context.log.infos == ["ext.drg_kontakt: 500,000 raekker"]

    writer.buffer = [(i,) for i in range(1000)]
    writer.flush()  # 501,000 — not a reporting point
    assert len(context.log.infos) == 1


# ---------------------------------------------------------------------------
# ExtWriter.discard + run_writers (review F19)
# ---------------------------------------------------------------------------


def test_discard_goer_efterfoelgende_close_til_en_no_op():
    conn = FakeConn()
    writer = _writer(conn, chunk_size=1000)
    writer.add((1, "x"))
    writer.add((2, "y"))
    writer.discard()
    writer.close()
    assert "batches" not in conn.store  # nothing was ever sent
    assert writer.total == 0
    assert conn.store["closed"] == 1


def test_close_uden_discard_sender_bufferen_og_kan_fejle():
    """The bug discard() exists to prevent: close() flushes, and a failing flush
    raises the DRIVER's error. This is what run_writers must keep from happening."""
    conn = FakeConn(fail_executemany=True)
    writer = _writer(conn, chunk_size=1000)
    writer.add((1, "x"))
    with pytest.raises(RuntimeError, match="truncated"):
        writer.close()


def test_run_writers_flusher_alle_og_returnerer_resultatet():
    conn = FakeConn()
    writers = {"a": _writer(conn, table="a", chunk_size=1000)}
    writers["b"] = _writer(conn, table="b", chunk_size=1000)
    writers["a"].add((1, "x"))
    writers["b"].add((2, "y"))

    result = _sds_tar.run_writers(writers, lambda: {"raekker": 2})

    assert result == {"raekker": 2}
    assert conn.store["rows"] == [(1, "x"), (2, "y")]
    assert writers["a"].total == 1 and writers["b"].total == 1
    assert conn.store["closed"] == 2


def test_run_writers_kasserer_bufferne_og_bevarer_den_rigtige_aarsag():
    """The designed (GDPR-safe) failure must be the exception that surfaces — not
    a driver error from the doomed final INSERT of the buffered rows."""
    conn = FakeConn(fail_executemany=True)
    writers = {"a": _writer(conn, table="a", chunk_size=1000)}

    def _load():
        writers["a"].add((1, "x"))
        writers["a"].add((2, "y"))
        raise ValueError("mulig encoding-korruption i kolonner: {'BESKRIVELSE': 1}")

    with pytest.raises(ValueError, match="encoding-korruption"):
        _sds_tar.run_writers(writers, _load)

    assert "batches" not in conn.store  # nothing was sent on the way out
    assert writers["a"].buffer == []
    assert conn.store["closed"] == 1  # closed anyway


def test_run_writers_kasserer_ogsaa_ved_baseexception():
    """A cancelled run (Ctrl-C, a Dagster termination) is a BaseException, not an
    Exception. Flushing its buffers on the way out would replace the cancellation
    with whatever the doomed INSERT says."""
    conn = FakeConn(fail_executemany=True)
    writers = {name: _writer(conn, table=name, chunk_size=1000) for name in ("a", "b", "c")}

    def _load():
        for writer in writers.values():
            writer.add((1, "x"))
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        _sds_tar.run_writers(writers, _load)
    assert "batches" not in conn.store  # nothing was sent
    assert conn.store["closed"] == 3  # every writer closed anyway


def test_run_writers_kasserer_ogsaa_naar_selve_flushen_fejler():
    """The explicit flush runs INSIDE the try, so a truncation error on one table
    still discards the other tables' buffers and still closes every writer — a
    flush that only happened in close() would leave the rest half-handled."""
    good = FakeConn()
    bad = FakeConn(fail_executemany=True)
    writers = {"bad": _writer(bad, table="bad", chunk_size=1000)}
    writers["good"] = _writer(good, table="good", chunk_size=1000)

    def _load():
        writers["bad"].add((1, "x"))
        writers["good"].add((2, "y"))
        return "loaded"

    with pytest.raises(RuntimeError, match="truncated"):
        _sds_tar.run_writers(writers, _load)

    assert "batches" not in good.store  # the healthy table was discarded, not sent
    assert bad.store["closed"] == 1 and good.store["closed"] == 1


def test_run_writers_lader_ikke_en_fejlende_close_erstatte_aarsagen():
    """A failed load leaves the connection in an error state, which is exactly
    when cursor.close() can raise — and an exception from a finally REPLACES the
    one in flight. That would reintroduce the masking this function removes, so
    a close error is swallowed and the remaining writers are still closed."""

    class _ClosesBadly(_sds_tar.ExtWriter):
        def close(self) -> None:
            raise RuntimeError("cursor is in an invalid state")

    good = FakeConn()
    writers = {
        "breaks": _ClosesBadly(FakeConn(), "breaks", ["a"], _Ctx(), chunk_size=1000),
        "good": _writer(good, table="good", chunk_size=1000),
    }

    def _load():
        raise ValueError("mulig encoding-korruption")

    with pytest.raises(ValueError, match="encoding-korruption"):
        _sds_tar.run_writers(writers, _load)

    assert good.store["closed"] == 1  # closing continued past the failing writer


# ---------------------------------------------------------------------------
# NonSeekableReader
# ---------------------------------------------------------------------------


def test_non_seekable_reader_returnerer_antal_laeste_bytes_og_nul_ved_eof():
    """readinto() must report the bytes actually read, and 0 at EOF.

    Reporting len(buffer) instead would never signal EOF: BufferedReader.read()
    would loop forever on garbage. That is a hung job rather than a red test, so
    the contract is asserted directly here and not only end to end below.
    """
    reader = file_delivery.NonSeekableReader(_StreamHandle(b"abc"))
    buffer = bytearray(10)

    assert reader.readinto(buffer) == 3
    assert bytes(buffer[:3]) == b"abc"
    assert reader.readinto(buffer) == 0


def test_non_seekable_reader_laeser_hele_stroemmen_gennem_textiowrapper():
    """tarfile's stream handles have no seekable(), which TextIOWrapper requires.
    The payload is deliberately larger than BufferedReader's buffer, so a
    readinto() that mis-reports its length would truncate or repeat."""
    text = "".join(f"linje {i};Æblegrød og ½ øl\n" for i in range(2000))
    handle = _StreamHandle(text.encode("cp1252"))
    with pytest.raises(io.UnsupportedOperation):
        handle.seek(0)

    wrapper = io.TextIOWrapper(
        io.BufferedReader(file_delivery.NonSeekableReader(handle)), encoding="cp1252", newline=""
    )
    # Bounded reads on purpose: an unbounded read() against a readinto() that
    # never reports EOF loops forever, which hangs CI instead of failing it.
    assert wrapper.read(len(text)) == text
    assert wrapper.read(1) == ""


def test_open_member_csv_deler_paa_semikolon_og_dekoder():
    data = '"DRG";"BESKRIVELSE"\r\n"01MA01";"Æblegrød"\r\n'.encode("cp1252")
    reader = file_delivery.open_member_csv(_StreamHandle(data), "cp1252")
    assert list(reader) == [["DRG", "BESKRIVELSE"], ["01MA01", "Æblegrød"]]


def test_open_member_csv_taeller_linjer_til_fejlbeskeder():
    """The sources quote reader.line_num in their field-count messages."""
    data = b'"A";"B"\n"1";"2"\n"3";"4"\n'
    reader = file_delivery.open_member_csv(_StreamHandle(data), "cp1252")
    next(reader)
    next(reader)
    assert reader.line_num == 2


# ---------------------------------------------------------------------------
# iter_members
# ---------------------------------------------------------------------------


def _tar(
    path,
    members: list[tuple[str, bytes]],
    directories: tuple[str, ...] = (),
    symlinks: tuple[tuple[str, str], ...] = (),
) -> None:
    with tarfile.open(path, mode="w:bz2") as tar:
        for name in directories:
            info = tarfile.TarInfo(name=name)
            info.type = tarfile.DIRTYPE
            tar.addfile(info)
        for name, data in members:
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        for name, target in symlinks:
            info = tarfile.TarInfo(name=name)
            info.type = tarfile.SYMTYPE
            info.linkname = target
            tar.addfile(info)


def test_iter_members_giver_kun_filer_med_basenavn_i_tar_orden(tmp_path):
    path = tmp_path / "delivery.tar.bz2"
    _tar(
        path,
        [
            ("1083/Metadata_UdredningsretMont_1083_20260906.csv", b"column_ordinal;column_name\n"),
            ("1083/SDP_ExportLog_20260906.csv", b"FilNavn;AntalRaekker\n"),
            ("1083/UdredningsretMont_1083_20260906.csv", b'"CPR"\n"0101011111"\n'),
        ],
        directories=("1083/",),
        # tarfile REFUSES to extract a (sym)link in stream mode — isfile() is what
        # keeps a stray link from aborting the whole delivery.
        symlinks=(("1083/latest.csv", "UdredningsretMont_1083_20260906.csv"),),
    )

    seen = [(name, handle.read()) for name, handle in file_delivery.iter_members(str(path))]

    assert [name for name, _ in seen] == [
        "Metadata_UdredningsretMont_1083_20260906.csv",
        "SDP_ExportLog_20260906.csv",
        "UdredningsretMont_1083_20260906.csv",
    ]
    assert seen[2][1] == b'"CPR"\n"0101011111"\n'


def test_iter_members_lukker_arkivet_naar_forbrugeren_stopper(tmp_path, monkeypatch):
    """A caller that breaks out (an error mid-archive) must not leak the tar.

    Both sources raise out of the member loop on a defective delivery, so the
    generator is abandoned mid-stream. Without the `with` block that leaks an
    open bz2 decompressor and file handle per failed delivery.
    """
    path = tmp_path / "delivery.tar.bz2"
    _tar(path, [("1083/a.csv", b"a\n"), ("1083/b.csv", b"b\n")])

    opened = []
    real_open = tarfile.open
    monkeypatch.setattr(
        tarfile, "open", lambda *a, **k: opened.append(real_open(*a, **k)) or opened[-1]
    )

    members = file_delivery.iter_members(str(path))
    next(members)
    assert opened and opened[0].closed is False
    members.close()

    assert opened[0].closed is True


def test_iter_members_haenger_ikke_naar_forbrugeren_stopper_midt_i_arkivet(tmp_path):
    """LPR3's preflight reads one member of a 22 GB delivery and closes the stream.

    The close must return mid-archive, also with a threaded decoder that reads
    ahead. The loop runs in a child process: a deadlocked close cannot be interrupted.
    """
    path = tmp_path / "delivery.tar.bz2"
    rng = random.Random(2)
    data = "".join(f"{rng.random()};{rng.randint(0, 10**9)}\n" for _ in range(400_000)).encode()
    with tarfile.open(path, "w:bz2") as tar:
        for index in range(4):
            info = tarfile.TarInfo(f"1083/{index}.csv")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    script = (
        "import sys\n"
        "from core.file_delivery import iter_members\n"
        "for _ in range(300):\n"
        "    members = iter_members(sys.argv[1])\n"
        "    next(members)\n"
        "    members.close()\n"
    )

    subprocess.run(
        [sys.executable, "-c", script, str(path)],
        cwd=Path(_sds_tar.__file__).parents[2],
        timeout=60,
        check=True,
    )


def test_iter_members_handle_doer_naar_loekken_gaar_videre(tmp_path):
    """The forward-only contract: r|bz2 decompresses once and never seeks back.

    Reading a handle after the loop advanced must fail loudly. If this module
    ever moved to `r:bz2` (random access) the stale read would silently succeed,
    and every delivery would be re-decompressed once per member — the exact cost
    the single-pass design exists to avoid, with no test going red.
    """
    path = tmp_path / "delivery.tar.bz2"
    _tar(path, [("1083/a.csv", b"a\n"), ("1083/b.csv", b"b\n")])

    members = file_delivery.iter_members(str(path))
    _, first = next(members)
    next(members)

    with pytest.raises(tarfile.StreamError):
        first.read()


def test_iter_members_tillader_at_springe_et_medlem_over_uden_at_laese_det(tmp_path):
    """Skipping without reading is what the routing in every source does with
    Metadata_* members — and what a source that loads a subset of a large
    delivery depends on. tarfile winds forward through the decompressor itself."""
    path = tmp_path / "delivery.tar.bz2"
    _tar(
        path,
        [
            ("1083/Metadata_Takst_1083_20260906.csv", b"x" * 5000),
            ("1083/T26_TAKSTBESKRIVELSE_1083_20260906.csv", b'"DRG"\n"01MA01"\n'),
        ],
    )

    seen = []
    for name, handle in file_delivery.iter_members(str(path)):
        if name.startswith("Metadata_"):
            continue  # deliberately never touches the handle
        seen.append((name, handle.read()))

    assert seen == [("T26_TAKSTBESKRIVELSE_1083_20260906.csv", b'"DRG"\n"01MA01"\n')]


def test_iter_members_lader_arkivfejl_slippe_igennem(tmp_path):
    """Both sources' pre-flight catches tarfile errors themselves. Swallowing one
    here would turn a corrupt delivery into an empty iteration — indistinguishable
    from an empty archive, and the exact failure the pre-flight exists to catch."""
    broken = tmp_path / "broken.tar.bz2"
    broken.write_bytes(b"not a bz2 archive at all")
    with pytest.raises(tarfile.TarError):
        list(file_delivery.iter_members(str(broken)))


# ---------------------------------------------------------------------------
# export log and control totals
# ---------------------------------------------------------------------------


def test_parse_export_log_fire_kolonner():
    text = (
        "FilNavn;AntalRaekker;FilStoerrelseBytes;Oprettet\n"
        "T26_DRGKONTAKT_1083_20260823.csv;1234;99;2026-08-24 18:43:47\n"
        "Metadata_T26_DRGKONTAKT_1083_20260823.csv;57;12;2026-08-24 18:43:47\n"
    )
    assert _sds_tar.parse_export_log(text) == {
        "T26_DRGKONTAKT_1083_20260823.csv": 1234,
        "Metadata_T26_DRGKONTAKT_1083_20260823.csv": 57,
    }


def test_parse_export_log_to_kolonner_og_alternativ_delimiter():
    assert _sds_tar.parse_export_log("FilNavn;AntalRaekker\na.csv;0\n") == {"a.csv": 0}
    assert _sds_tar.parse_export_log("FilNavn,AntalRaekker\na.csv,7\n", delimiter=",") == {
        "a.csv": 7
    }
    # the default really is ';' — a comma log parsed with it has no FilNavn column
    with pytest.raises(KeyError):
        _sds_tar.parse_export_log("FilNavn,AntalRaekker\na.csv,7\n")


def test_export_log_deviations_finder_afvigelser_og_springer_metadata_over():
    export_log = {
        "data.csv": 10,
        "info.csv": 1,
        "Metadata_data.csv": 57,  # schemas carry no data rows
    }
    actual = {"data.csv": 9, "info.csv": 1}
    assert _sds_tar.export_log_deviations(export_log, actual) == {
        "data.csv": {"eksportlog": 10, "indlaest": 9}
    }


def test_export_log_deviations_rapporterer_helt_manglende_fil_som_none():
    deviations = _sds_tar.export_log_deviations({"data.csv": 10}, {})
    assert deviations == {"data.csv": {"eksportlog": 10, "indlaest": None}}


def test_export_log_deviations_er_tom_naar_alt_stemmer():
    assert _sds_tar.export_log_deviations({"a.csv": 3}, {"a.csv": 3, "b.csv": 9}) == {}


# ---------------------------------------------------------------------------
# schema drift and encoding sanity
# ---------------------------------------------------------------------------


def test_schema_drift_finder_ukendte_og_manglende_kolonner():
    unknown, missing = _sds_tar.schema_drift(["A", "NY", "B"], ["A", "B", "C"])
    assert unknown == ["NY"]  # in the header's order
    assert missing == ["C"]  # in the spec's order


def test_schema_drift_accepterer_kolonner_der_lovligt_maa_mangle():
    unknown, missing = _sds_tar.schema_drift(["A"], ["A", "B", "C"], optional={"B"})
    assert unknown == []
    assert missing == ["C"]  # an optional column is allowed away, C is not


def test_schema_drift_paa_identisk_skema_er_tom():
    assert _sds_tar.schema_drift(["A", "B"], ["A", "B"]) == ([], [])


def test_has_mojibake_genkender_begge_markoerer():
    assert _sds_tar.has_mojibake("tekst med �") is True  # sender already replaced
    assert _sds_tar.has_mojibake("Odense ï¿½ Universitetshospital") is True  # UTF-8 as cp1252
    assert _sds_tar.has_mojibake("Ærø Sygehus") is False  # Danish letters are not mojibake
    assert _sds_tar.has_mojibake("") is False


# ---------------------------------------------------------------------------
# ext DDL
# ---------------------------------------------------------------------------


def test_ext_create_sql_er_ordret():
    sql = _sds_tar.ext_create_sql(
        "drg_takstbeskrivelse",
        ["[takstsystem] NVARCHAR(3) NOT NULL"],
        [("DRG", 6, True), ("BESKRIVELSE", 500, False)],
    )
    assert sql == (
        "DROP TABLE IF EXISTS [ext].[drg_takstbeskrivelse];\n"
        "CREATE TABLE [ext].[drg_takstbeskrivelse] (\n"
        "    [takstsystem] NVARCHAR(3) NOT NULL,\n"
        "    [DRG] NVARCHAR(6) NOT NULL,\n"
        "    [BESKRIVELSE] NVARCHAR(500) NULL,\n"
        "    [leverance_dato] DATE NOT NULL,\n"
        "    [dw_timestamp_insert] DATETIME2(0) NOT NULL DEFAULT GETDATE(),\n"
        "    [dw_timestamp_update] DATETIME2(0) NOT NULL DEFAULT GETDATE()\n"
        ");"
    )


def test_ext_create_sql_tager_flere_indledende_linjer():
    sql = _sds_tar.ext_create_sql(
        "udredningsret_mont",
        ["[kadence] NVARCHAR(1) NOT NULL", "[leverance_tidsstempel] DATETIME2(0) NOT NULL"],
        [("CPR", 500, False)],
    )
    assert "    [kadence] NVARCHAR(1) NOT NULL,\n    [leverance_tidsstempel]" in sql
    assert sql.endswith("DEFAULT GETDATE()\n);")


def test_execute_statements_koerer_hvert_udsagn_og_committer():
    conn = FakeConn()
    sql = _sds_tar.ext_create_sql("t", [], [("A", 10, False)])
    _sds_tar.execute_statements(conn, sql)
    executed = conn.store["executed"]
    assert len(executed) == 2
    assert executed[0] == "DROP TABLE IF EXISTS [ext].[t]"
    assert executed[1].startswith("CREATE TABLE [ext].[t]")
    assert conn.commits == 1
    assert conn.store["closed"] == 1


def test_execute_statements_kan_udskyde_committen():
    conn = FakeConn()
    _sds_tar.execute_statements(conn, "DROP TABLE IF EXISTS [ext].[t];\n", commit=False)
    assert conn.store["executed"] == ["DROP TABLE IF EXISTS [ext].[t]"]
    assert conn.commits == 0


# ---------------------------------------------------------------------------
# environment and filenames
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sub_dir", "require_non_empty", "fejl"),
    [
        (None, False, "is unset or empty"),
        ("findes_ikke", False, "is not a directory"),
        ("", True, "empty directory"),
    ],
)
def test_share_root_fejler_haardt_uden_fallback(tmp_path, sub_dir, require_non_empty, fejl):
    base_dir = "   " if sub_dir is None else str(tmp_path / sub_dir)
    with pytest.raises(ValueError, match=fejl):
        FileShareResource(base_dir=base_dir, require_non_empty=require_non_empty).root()


def test_share_root_accepterer_en_monteret_mappe(tmp_path):
    assert FileShareResource(base_dir=f"  {tmp_path}  ").root() == str(tmp_path)
    (tmp_path / "20260730").mkdir()
    assert FileShareResource(base_dir=str(tmp_path), require_non_empty=True).root() == str(tmp_path)


def test_source_encoding_falder_tilbage_paa_default(monkeypatch):
    monkeypatch.delenv("SDS_TAR_TEST_ENCODING", raising=False)
    assert file_delivery.source_encoding("SDS_TAR_TEST_ENCODING") == "cp1252"
    monkeypatch.setenv("SDS_TAR_TEST_ENCODING", "  ")
    assert file_delivery.source_encoding("SDS_TAR_TEST_ENCODING") == "cp1252"
    monkeypatch.setenv("SDS_TAR_TEST_ENCODING", " utf-8 ")
    assert file_delivery.source_encoding("SDS_TAR_TEST_ENCODING") == "utf-8"
    assert file_delivery.source_encoding("SDS_TAR_TEST_ENCODING_MISSING", "latin-1") == "latin-1"


def test_parse_archive_timestamp():
    """Impossible calendar values and non-numeric text are None, so the caller
    can reject the file instead of crashing on it. The digit COUNT is the
    caller's regex to enforce — strptime accepts unpadded fields."""
    stamp = _sds_tar.parse_archive_timestamp("20260824190546")
    assert (stamp.year, stamp.month, stamp.day, stamp.hour, stamp.minute, stamp.second) == (
        2026,
        8,
        24,
        19,
        5,
        46,
    )
    assert _sds_tar.parse_archive_timestamp("20261332190546") is None  # month 13
    assert _sds_tar.parse_archive_timestamp("20260230190546") is None  # 30 February
    assert _sds_tar.parse_archive_timestamp("2026-08-24T19:05:46") is None
    assert _sds_tar.parse_archive_timestamp("") is None


def test_dataset_prefix_bruger_kildens_eget_moenster():
    member_re = re.compile(r"^T\d{2}_(?P<prefix>.+)_1083_\d{8}\.csv$")
    assert _sds_tar.dataset_prefix("T26_DRGKONTAKT_1083_20260823.csv", member_re) == "DRGKONTAKT"
    assert _sds_tar.dataset_prefix("Metadata_T26_DRGKONTAKT_1083_20260823.csv", member_re) is None
    assert _sds_tar.dataset_prefix("Readme.txt", member_re) is None


def test_upsert_register_opdaterer_ikke_insert_only_kolonner():
    """A rerun of a CPR delivery must keep the forrige_produktionsdato it was first stored with."""
    store: dict = {}
    _sds_tar.upsert_register(
        FakeCursor(store),
        "CPRLeverance",
        ("kildetabel",),
        {"kildetabel": "t", "forrige_produktionsdato": "20260101", "filnavn": "f"},
        insert_only=("forrige_produktionsdato",),
    )
    sql = store["executed"][0]
    update_set = re.search(r"UPDATE SET (.*?) WHEN NOT MATCHED", sql).group(1)
    insert = re.search(r"INSERT \((.*?)\)", sql).group(1)
    assert "[filnavn]" in update_set
    assert "[forrige_produktionsdato]" not in update_set
    assert "[forrige_produktionsdato]" in insert

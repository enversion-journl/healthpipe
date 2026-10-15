# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the udredningsret archive (DSI/csv_files/udredningsret_archive.py).

The key SQL's content, the merge config's change-detect rules, the manifest
contract, guard behaviour against a fake connection (incl. the SQL the guards
actually issue), _run_archive's wiring (no-op on empty manifest, soft-delete cap
BEFORE the merge, key table dropped after) and the checkpoint gate. The extract
layer is tested in test_udredningsret_extract.py.
"""

import contextlib
from datetime import date
from types import SimpleNamespace

import dagster as dg
import pytest
from DSI.csv_files import udredningsret_archive
from DSI.csv_files.udredningsret_datasets import UDREDNINGSRET_MONT

pytestmark = pytest.mark.unit

D = UDREDNINGSRET_MONT
ARC = "[arc].[udredningsret_mont]"
EXT = "[ext].[udredningsret_mont]"
KILDE = "[ext].[udredningsret_mont_noegler]"
MANIFEST = "[ext].[udredningsret_leverance_manifest]"
MANIFEST_ROW = (
    "Region_1083_LPR_Udredningsret_U_20260907193544.tar.bz2",
    "2026-09-07 19:35:44",
    "U",
    "2026-09-07",
    "2026-09-06",
    "udredningsret_leverance_1083",
    "2026-09-07T19:35:44",
    True,
)


class FakeCursor:
    """Replays prepared results keyed on an SQL fragment (first match wins)."""

    def __init__(self, responses):
        self.responses = responses
        self.executed: list[str] = []
        self.params: list[tuple] = []
        self._current = None
        self.rowcount = 0

    def execute(self, sql, *params):
        self.executed.append(" ".join(sql.split()))
        self.params.append(params)
        self._current = None
        for fragment, rows in self.responses.items():
            if fragment in self.executed[-1]:
                self._current = rows
                return

    def fetchone(self):
        return (self._current or [(0,)])[0]

    def fetchall(self):
        return list(self._current or [])

    def close(self):
        pass


class FakeConn:
    def __init__(self, responses):
        self._responses = responses
        self.cursors: list[FakeCursor] = []

    def cursor(self):
        cursor = FakeCursor(self._responses)
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        pass

    @property
    def executed(self) -> list[str]:
        return [sql for c in self.cursors for sql in c.executed]


class FakeResource:
    def __init__(self, responses):
        self.conn = FakeConn(responses)

    @contextlib.contextmanager
    def get_connection(self):
        yield self.conn


def _manifest_responses(rows):
    return {"FROM sys.objects WHERE object_id": [(1,)], f"FROM {MANIFEST}": rows}


# ---------------------------------------------------------------------------
# key SQL and merge config
# ---------------------------------------------------------------------------


def test_source_table_sql_bygger_noeglerne():
    statements = udredningsret_archive.build_source_table_sql()
    assert statements == [f"DROP TABLE IF EXISTS {KILDE};", statements[1]]
    build = statements[1]
    assert f"INTO {KILDE}" in build
    # indhold, loebenummer_hash, helbredsforloeb, markoer
    assert build.count("HASHBYTES('SHA2_256'") == 4
    assert "PARTITION BY [DW_EK_HelbredsForloeb]" in build
    assert (
        "ORDER BY [FORLOEBSMARKOER_DATO], [FORLOEBSMARKOER], [ANSVARLIG_ENHED_FORLOB], [OBJEKTID_FORLOBELEMENT]"
        in build
    )
    assert "LOWER([OBJEKTID_FORLOBELEMENT])" in build
    for column in D.source_columns:
        assert f"n.[{column}]" in build, column
    # PK/OBJEKTID are not part of the content hash, but CPR is (identical rows = same citizen)
    indhold = build.split("AS indhold_hash")[0]
    assert "e.[PK]" not in indhold and "e.[OBJEKTID_FORLOBSMARKOR]" not in indhold
    assert "e.[CPR]" in indhold and "e.[F_ID]" in indhold
    # no clean_code_* in the arc layer (DSA name, formed in pre_stg)
    assert "clean_code" not in build


def test_loebenummer_rangeres_paa_stabil_indholdshash_ikke_paa_ustabile_ider():
    """Review F13: OBJEKTID is reissued and PK/F_ID/DW_EK renumbered between deliveries,
    so ranking on them lets two duplicate markers swap places. The rank must be a hash
    of the content WITHOUT those ids; the dedup partition may still use them."""
    build = udredningsret_archive.build_source_table_sql()[1]
    stable = build.split("AS indhold_hash")[1].split("AS loebenummer_hash")[0]
    for excluded in ("PK", "OBJEKTID_FORLOBSMARKOR", "F_ID", "DW_EK_HelbredsForloeb"):
        assert f"e.[{excluded}]" not in stable, excluded
    assert "e.[FORLOEBSMARKOER]" in stable and "e.[ALDER]" in stable
    assert (
        "PARTITION BY n.[markoer_noegle] ORDER BY n.[loebenummer_hash], "
        "n.[OBJEKTID_FORLOBSMARKOR], n.[PK]) AS INT) AS loebenummer"
    ) in " ".join(build.split())
    assert "PARTITION BY i.[indhold_hash] ORDER BY [OBJEKTID_FORLOBSMARKOR], [PK]" in build


def test_source_index_og_drop_sql():
    sql = udredningsret_archive.build_source_index_sql()
    assert sql.startswith("CREATE UNIQUE CLUSTERED INDEX [UX_ext_udredningsret_mont_noegler]")
    assert sql.endswith(f"ON {KILDE} ([markoer_noegle], [loebenummer]);")
    assert udredningsret_archive.drop_source_table_sql() == f"DROP TABLE IF EXISTS {KILDE};"


def test_merge_config_change_detect_udelader_leverancedato_og_surrogat_id():
    cfg = udredningsret_archive.MERGE_CONFIGS["udredningsret_mont"]
    # leverance_dato is always written but never counts as a change
    assert "leverance_dato" in cfg.non_key_columns
    assert "leverance_dato" not in cfg.change_detect_columns
    # SDS' renumbered sequence numbers stay but trigger no update; the marker's
    # LPR3 UUID is kept current.
    for surrogate in ("PK", "F_ID"):
        assert surrogate in cfg.non_key_columns
        assert surrogate not in cfg.change_detect_columns
    for tracked in ("OBJEKTID_FORLOBSMARKOR", "DW_EK_HelbredsForloeb", "MRK_H", "ALDER"):
        assert tracked in cfg.change_detect_columns
    # every arc column is written — an omitted column would NULL the archive on merge
    assert set(cfg.merge_keys + cfg.non_key_columns) == set(D.arc_columns) | {"leverance_dato"}


# ---------------------------------------------------------------------------
# manifest
# ---------------------------------------------------------------------------


def test_read_manifest_kraever_tabellen():
    conn = FakeConn({"FROM sys.objects WHERE object_id": [(0,)]})
    with pytest.raises(ValueError, match="does not exist"):
        udredningsret_archive.read_manifest(conn)


def test_read_manifest_tom_er_noop_og_een_raekke_er_leverancen():
    assert udredningsret_archive.read_manifest(FakeConn(_manifest_responses([]))) is None
    delivery = udredningsret_archive.read_manifest(FakeConn(_manifest_responses([MANIFEST_ROW])))
    assert delivery == udredningsret_archive.ExtDelivery(
        filnavn=MANIFEST_ROW[0],
        filnavn_stempel="2026-09-07 19:35:44",
        kadence="U",
        leverance_dato="2026-09-07",
        lpropdateringsdato="2026-09-06",
        checkpoint_enhed="udredningsret_leverance_1083",
        checkpoint_identitet="2026-09-07T19:35:44",
        ryk_checkpoint=True,
    )
    with pytest.raises(ValueError, match="2 rows"):
        udredningsret_archive.read_manifest(
            FakeConn(_manifest_responses([MANIFEST_ROW, MANIFEST_ROW]))
        )


def test_guard_ext_not_empty():
    with pytest.raises(ValueError, match="is empty although the manifest"):
        udredningsret_archive.guard_ext_not_empty(FakeConn({f"FROM {EXT}": [(0,)]}), D)
    udredningsret_archive.guard_ext_not_empty(FakeConn({f"FROM {EXT}": [(5,)]}), D)


# ---------------------------------------------------------------------------
# guards
# ---------------------------------------------------------------------------


def _delivery_obj(lpr="2026-09-06", advance=True):
    return udredningsret_archive.ExtDelivery(
        filnavn=MANIFEST_ROW[0],
        filnavn_stempel="2026-09-07 19:35:44",
        kadence="U",
        leverance_dato="2026-09-07",
        lpropdateringsdato=lpr,
        checkpoint_enhed="udredningsret_leverance_1083",
        checkpoint_identitet="2026-09-07T19:35:44",
        ryk_checkpoint=advance,
    )


def test_direction_guard():
    meta = "FROM [meta].[UdredningsretLeverance]"
    conn = FakeConn({meta: [("2026-08-30", date(2026, 8, 31))]})
    assert udredningsret_archive.guard_delivery_direction(conn, D, _delivery_obj(), False) == (
        "2026-08-30"
    )
    conn = FakeConn({meta: [("2026-09-13", date(2026, 9, 14))]})
    with pytest.raises(ValueError, match="older delivery must not be merged"):
        udredningsret_archive.guard_delivery_direction(conn, D, _delivery_obj(), False)
    # backfill requires the opposite
    assert udredningsret_archive.guard_delivery_direction(conn, D, _delivery_obj(), True) == (
        "2026-09-13"
    )
    conn = FakeConn({meta: [("2026-08-30", date(2026, 8, 31))]})
    with pytest.raises(ValueError, match="backfill requires"):
        udredningsret_archive.guard_delivery_direction(conn, D, _delivery_obj(), True)
    # empty archive -> no guard
    conn = FakeConn({meta: [(None, None)]})
    assert udredningsret_archive.guard_delivery_direction(conn, D, _delivery_obj(), False) is None


def test_direction_guard_tillader_genkoersel_af_samme_leverance():
    conn = FakeConn({"FROM [meta].[UdredningsretLeverance]": [("2026-09-06", date(2026, 9, 7))]})
    assert udredningsret_archive.guard_delivery_direction(conn, D, _delivery_obj(), False) == (
        "2026-09-06"
    )


def test_direction_guard_falder_tilbage_til_leverancedato_uden_lpr():
    """When LPROPDATERINGSDATO is missing from the archive's history, judge on leverance_dato."""
    conn = FakeConn({"FROM [meta].[UdredningsretLeverance]": [(None, date(2026, 9, 14))]})
    with pytest.raises(ValueError, match="leverance_dato 2026-09-07"):
        udredningsret_archive.guard_delivery_direction(conn, D, _delivery_obj(), False)
    conn = FakeConn({"FROM [meta].[UdredningsretLeverance]": [(None, date(2026, 8, 31))]})
    assert udredningsret_archive.guard_delivery_direction(conn, D, _delivery_obj(), False) == str(
        date(2026, 8, 31)
    )


def test_row_parity_guard():
    ok = FakeConn({f"FROM {EXT}": [(10,)], f"FROM {KILDE}": [(8, 10)]})
    assert udredningsret_archive.guard_row_parity(ok, D) == {
        "ext_raekker": 10,
        "kilderaekker_efter_dubletreduktion": 8,
        "fuldt_identiske_dubletter": 2,
    }
    bad = FakeConn({f"FROM {EXT}": [(10,)], f"FROM {KILDE}": [(8, 9)]})
    with pytest.raises(ValueError, match="lost or doubled"):
        udredningsret_archive.guard_row_parity(bad, D)


def test_key_uniqueness_guard_grupperer_paa_begge_merge_noegler():
    conn = FakeConn({"HAVING COUNT(*) > 1": [(0,)]})
    udredningsret_archive.guard_key_uniqueness(conn, D)
    assert "GROUP BY [markoer_noegle], [loebenummer] HAVING COUNT(*) > 1" in conn.executed[0]
    with pytest.raises(ValueError, match="Programming error"):
        udredningsret_archive.guard_key_uniqueness(FakeConn({"HAVING COUNT(*) > 1": [(3,)]}), D)


def test_source_unique_index_guard():
    conn = FakeConn({"sys.indexes": [(1,)]})
    udredningsret_archive.guard_source_unique_index(conn, D)
    assert "is_unique = 1 AND type = 1" in conn.executed[0]
    assert conn.cursors[0].params[0] == (KILDE, "UX_ext_udredningsret_mont_noegler")
    with pytest.raises(ValueError, match="is missing"):
        udredningsret_archive.guard_source_unique_index(FakeConn({"sys.indexes": [(0,)]}), D)


def test_helbredsforloeb_recovery_guard():
    live = "COUNT(DISTINCT [helbredsforloeb_noegle])"
    found = f"AND EXISTS (SELECT 1 FROM {KILDE}"
    responses = {live: [(1000,)], found: [(999,)]}
    conn = FakeConn(responses)
    assert udredningsret_archive.guard_helbredsforloeb_recovery(conn, D, 0.999)["andel"] == 0.999
    # both counts are restricted to live arc rows
    assert all("[dw_is_deleted] = 0" in sql for sql in conn.executed)
    responses[found] = [(990,)]
    with pytest.raises(ValueError, match="recovered in the delivery"):
        udredningsret_archive.guard_helbredsforloeb_recovery(FakeConn(responses), D, 0.999)
    # override for this run
    assert (
        udredningsret_archive.guard_helbredsforloeb_recovery(FakeConn(responses), D, 0.98)[
            "genfundet"
        ]
        == 990
    )
    # empty archive (first load) is skipped
    empty = FakeConn({live: [(0,)]})
    assert udredningsret_archive.guard_helbredsforloeb_recovery(empty, D, 0.999)["andel"] == 1.0


def test_count_live_taeller_kun_levende_raekker():
    conn = FakeConn({f"FROM {ARC} WHERE [dw_is_deleted] = 0": [(42,)]})
    assert udredningsret_archive.count_live(conn, D) == 42
    assert conn.executed == [f"SELECT COUNT(*) FROM {ARC} WHERE [dw_is_deleted] = 0"]


def test_expected_soft_deletes_sql_bruger_merge_noeglerne():
    conn = FakeConn({"NOT EXISTS": [(7,)]})
    assert udredningsret_archive.count_expected_soft_deletes(conn, D) == 7
    sql = conn.executed[0]
    assert "src.[markoer_noegle] = tgt.[markoer_noegle]" in sql
    assert "src.[loebenummer] = tgt.[loebenummer]" in sql
    assert "tgt.[dw_is_deleted] = 0" in sql


def test_record_delivery_upserter_paa_kildetabel_og_tidsstempel():
    conn = FakeConn({})
    result = SimpleNamespace(rows_inserted=5, rows_updated=6, rows_soft_deleted=7)
    parity = {"ext_raekker": 10, "kilderaekker_efter_dubletreduktion": 8}
    udredningsret_archive.record_delivery(
        conn, D, _delivery_obj(), result, parity, {"andel": 0.9995}, backfill=False
    )
    sql = conn.executed[0]
    assert "MERGE [meta].[UdredningsretLeverance] AS tgt" in sql
    assert (
        "ON tgt.[kildetabel] = src.kildetabel AND tgt.[leverance_tidsstempel] = src.leverance_tidsstempel"
        in sql
    )
    params = conn.cursors[0].params[0]
    assert params[:2] == ("udredningsret_mont", "2026-09-07 19:35:44")
    # 10 values for UPDATE and the same 10 for INSERT, in the same order
    assert len(params) == 22
    assert params[2:12] == params[12:22]
    assert params[2:6] == ("U", "2026-09-07", "2026-09-06", "merge")
    assert params[6:12] == (10, 8, 5, 6, 7, 0.9995)


# ---------------------------------------------------------------------------
# _run_archive: no-op, cap BEFORE the merge, key table dropped AFTER
# ---------------------------------------------------------------------------


def _wired_run(monkeypatch, live: int, expected_deletes: int, manifest_rows=(MANIFEST_ROW,)):
    responses = {
        **_manifest_responses(list(manifest_rows)),
        "FROM [meta].[UdredningsretLeverance]": [(None, None)],
        f"SELECT COUNT(*) FROM {EXT}": [(10,)],
        "ISNULL(SUM([antal_kilderaekker]), 0)": [(10, 10)],
        "HAVING COUNT(*) > 1": [(0,)],
        "sys.indexes": [(1,)],
        "COUNT(DISTINCT [helbredsforloeb_noegle])": [(live,)],
        f"AND EXISTS (SELECT 1 FROM {KILDE}": [(live,)],
        f"FROM {ARC} WHERE [dw_is_deleted] = 0": [(live,)],
        "NOT EXISTS": [(expected_deletes,)],
    }
    resource = FakeResource(responses)
    calls: list[str] = []

    def fake_merge(context, mssql, merge_config, run_config):
        calls.append("merge")
        return SimpleNamespace(rows_inserted=0, rows_updated=0, rows_soft_deleted=expected_deletes)

    monkeypatch.setattr(udredningsret_archive, "execute_archive_merge", fake_merge)
    monkeypatch.setattr(udredningsret_archive, "build_archive_metadata", lambda *a, **k: {})
    monkeypatch.setattr(
        udredningsret_archive, "record_delivery", lambda *a, **k: calls.append("record")
    )
    context = dg.build_asset_context()
    config = udredningsret_archive.UdredningsretArchiveConfig()
    return resource, calls, lambda: udredningsret_archive._run_archive(context, resource, D, config)


def test_tomt_manifest_er_noop_foer_noegletabel_guards_og_merge(monkeypatch):
    resource, calls, run = _wired_run(monkeypatch, live=5000, expected_deletes=0, manifest_rows=())
    result = run()
    assert calls == []
    assert result.metadata["besluttet"].startswith("no-op")
    assert result.metadata["rows_inserted"] == 0
    # only the manifest was read — nothing built, guarded or dropped
    assert all("manifest" in sql or "sys.objects" in sql for sql in resource.conn.executed)


def test_soft_delete_loft_fyrer_foer_mergen(monkeypatch):
    """12 % expected soft-deletes of 5,000 live > the 1 % ratio: nothing is merged."""
    resource, calls, run = _wired_run(monkeypatch, live=5000, expected_deletes=600)
    with pytest.raises(dg.Failure) as excinfo:
        run()
    assert "marked-vs-live" in excinfo.value.description
    assert calls == []  # neither merge nor delivery registration was reached
    executed = resource.conn.executed
    # the key table was built and indexed before the cap was judged — and is KEPT for post mortem
    i_index = next(i for i, s in enumerate(executed) if s.startswith("CREATE UNIQUE CLUSTERED"))
    assert not any(sql == f"DROP TABLE IF EXISTS {KILDE};" for sql in executed[i_index:])


def test_soft_delete_under_loftet_merger_registrerer_og_dropper_noegletabellen(monkeypatch):
    resource, calls, run = _wired_run(monkeypatch, live=5000, expected_deletes=30)
    result = run()
    assert calls == ["merge", "record"]
    assert result.metadata["levende_raekker_foer"] == 5000
    assert result.metadata["helbredsforloeb_genfindelse"]["andel"] == 1.0
    assert result.metadata["leverance"]["checkpoint_identitet"] == "2026-09-07T19:35:44"
    executed = resource.conn.executed
    # index is created after the uniqueness guard and verified before the merge
    i_guard = next(i for i, s in enumerate(executed) if "HAVING COUNT(*) > 1" in s)
    i_index = next(i for i, s in enumerate(executed) if s.startswith("CREATE UNIQUE CLUSTERED"))
    i_check = next(i for i, s in enumerate(executed) if "sys.indexes" in s)
    assert i_guard < i_index < i_check
    # the key table is dropped as the LAST statement, after the merge succeeded (F18)
    assert executed[-1] == f"DROP TABLE IF EXISTS {KILDE};"
    assert len(executed) - 1 > i_check


# ---------------------------------------------------------------------------
# the checkpoint gate
# ---------------------------------------------------------------------------


class _FakeDfm:
    def __init__(self, checkpoint):
        self.checkpoint = checkpoint
        self.set_calls: list[tuple] = []

    def get_delivery_checkpoint(self, unit):
        return self.checkpoint

    def set_delivery_checkpoint(self, unit, identity):
        self.set_calls.append((unit, identity))


def _gate(monkeypatch, manifest_rows, checkpoint="2026-08-31T19:39:18"):
    dfm = _FakeDfm(checkpoint)
    monkeypatch.setattr(udredningsret_archive, "DfmManager", lambda mssql, context: dfm)
    resource = FakeResource(_manifest_responses(list(manifest_rows)))
    result = udredningsret_archive._run_checkpoint_gate(dg.build_asset_context(), resource)
    return dfm, result


def test_gate_rykker_checkpointet_til_manifestets_identitet(monkeypatch):
    dfm, result = _gate(monkeypatch, [MANIFEST_ROW])
    assert dfm.set_calls == [("udredningsret_leverance_1083", "2026-09-07T19:35:44")]
    assert result.metadata["checkpoint_foer"] == "2026-08-31T19:39:18"
    assert result.metadata["checkpoint_efter"] == "2026-09-07T19:35:44"


def test_gate_er_noop_paa_tomt_manifest(monkeypatch):
    dfm, result = _gate(monkeypatch, [])
    assert dfm.set_calls == []
    assert "uaendret" in result.metadata["besluttet"]


def test_gate_rykker_ikke_ved_pin(monkeypatch):
    pinned = (*MANIFEST_ROW[:-1], False)
    dfm, result = _gate(monkeypatch, [pinned])
    assert dfm.set_calls == []
    assert result.metadata["besluttet"].startswith("pin")


def test_gate_afviser_fremmed_enhed(monkeypatch):
    foreign = (*MANIFEST_ROW[:5], "drg_leverance_DRG_T26_M", *MANIFEST_ROW[6:])
    with pytest.raises(ValueError, match="foreign unit"):
        _gate(monkeypatch, [foreign])

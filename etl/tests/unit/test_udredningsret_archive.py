# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the udredningsret archive (DSI/csv_files/udredningsret_archive.py).

The key SQL's content, the merge config's change-detect rules, the PK/F_ID
refresh SQL (decision U1), the regrouping detector that guards U1's blind spot
(decision U15: build_regroup_detection_sql / build_regroup_bump_sql and their
order and single commit), the manifest contract, guard behaviour
against a fake connection (incl. the SQL the guards actually issue),
_run_archive's wiring (no-op on empty manifest, PK/F_ID refresh after the merge
and before the key table is dropped, skipped on backfill) and the checkpoint gate. The extract layer is tested in
test_udredningsret_extract.py.
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
TMP = udredningsret_archive.REGROUP_TABLE  # "#omgruppering"
COUNT_TMP = f"SELECT COUNT(*) FROM {TMP}"
REFRESH_IDS = "UPDATE tgt SET tgt.[PK]"
CLOCK = "CAST(SYSDATETIME() AS datetime2(0)), 126"  # read_merge_start's SELECT
MERGE_START = "2026-09-07T19:40:00"
BUMP = "UPDATE tgt SET tgt.[dw_timestamp_update]"
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
                # an UPDATE's prepared "result" is its rowcount
                if self.executed[-1].startswith("UPDATE"):
                    self.rowcount = rows[0][0] if rows else 0
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
        self.commits = 0
        # number of statements executed at each commit — proves WHAT was committed
        self.executed_at_commit: list[int] = []

    def cursor(self):
        cursor = FakeCursor(self._responses)
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        self.commits += 1
        self.executed_at_commit.append(len(self.executed))

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


def test_refresh_surrogate_ids_sql_opdaterer_kun_pk_og_f_id_paa_levende_raekker():
    """Decision U1: after the merge PK/F_ID are copied from the key table onto the live
    arc rows where they differ — and NOTHING else, so a renumbering stays a non-change
    (no dw_timestamp_update, no leverance_dato)."""
    sql = " ".join(udredningsret_archive.build_refresh_surrogate_ids_sql(D).split())
    assert sql.startswith("UPDATE tgt SET tgt.[PK] = src.[PK], tgt.[F_ID] = src.[F_ID] FROM ")
    assert f"FROM {ARC} AS tgt JOIN {KILDE} AS src" in sql
    assert (
        "ON src.[markoer_noegle] = tgt.[markoer_noegle] AND src.[loebenummer] = tgt.[loebenummer]"
        in sql
    )
    assert "WHERE tgt.[dw_is_deleted] = 0 AND (" in sql
    for surrogate in ("PK", "F_ID"):
        assert f"ISNULL(tgt.[{surrogate}], N'') <> ISNULL(src.[{surrogate}], N'')" in sql
    # the SET list is exactly the surrogate columns — no timestamp, no delivery date
    set_list = sql.split("UPDATE tgt SET ", 1)[1].split(" FROM ", 1)[0]
    assert set_list == "tgt.[PK] = src.[PK], tgt.[F_ID] = src.[F_ID]"
    assert "dw_timestamp_update" not in sql and "leverance_dato" not in sql
    assert "dw_is_deleted = 1" not in sql


def test_regroup_detection_sql_finder_de_helbredsforloeb_hvor_grupperingen_ikke_er_en_bijektion():
    """Decision U15. A pure RELABEL of F_ID is a non-event (U1); a REGROUP is a real
    change the merge cannot see, because every change-detect column is byte-identical.
    par is the DISTINCT set of (old group, new group) pairs of one helbredsforloeb, so
    COUNT(*) per hf is |R|, and |R| = |old| = |new| IS 'the relabel is a bijection on
    groups'. Both inequalities must be there: the first alone misses a MERGE of two
    groups into one (|R| = |old| = 2, |new| = 1), the second alone misses a SPLIT. And
    both TOGETHER miss the blank boundary, which is why the third term exists: '#BLANK'
    is a label, but not a group downstream (pre_Udredningsforloeb and the control total
    count non-blank F_ID only), so a whole group going blank — or a blank marker given
    an F_ID — stays a bijection on labels while arc gains or loses a group."""
    statements = udredningsret_archive.build_regroup_detection_sql(D)
    assert len(statements) == 2
    scaffold, populate = statements
    # The work table is CREATED unparameterised and only FILLED by the parameterised
    # statement: the driver (mssql_python) runs a parameterised statement as a prepared
    # batch, and a #temp created inside such a batch dies with it — measured: the bump
    # then fails with "Invalid object name '#omgruppering'". Same split as
    # DSA/CodeSystem/utils.py.
    assert "?" not in scaffold
    assert populate.count("?") == 1
    assert scaffold.startswith(f"DROP TABLE IF EXISTS {TMP};")
    assert f"CREATE TABLE {TMP} ([hf] CHAR(64) NOT NULL);" in scaffold
    assert f"CREATE UNIQUE CLUSTERED INDEX [IX_omgruppering] ON {TMP}([hf]);" in scaffold
    assert f"INSERT INTO {TMP} ([hf])" in populate
    # the ONLY mention of the work table is the INSERT target: a SELECT ... INTO #tmp
    # would recreate the table inside the prepared batch and lose it again
    assert populate.count(TMP) == 1

    sql = " ".join(" ".join(statements).split())
    # arc against the key table on the merge keys, live rows only
    assert f"FROM {ARC} AS tgt JOIN {KILDE} AS src" in sql
    assert (
        "ON src.[markoer_noegle] = tgt.[markoer_noegle] AND src.[loebenummer] = tgt.[loebenummer]"
        in sql
    )
    assert "WHERE tgt.[dw_is_deleted] = 0" in sql
    # blank is labelled '#BLANK' on BOTH sides — 14 % of the markers belong to no
    # forloeb, so it must be comparable at all
    for alias, side in (("tgt", "gammel"), ("src", "ny")):
        assert f"ISNULL(NULLIF(LTRIM(RTRIM({alias}.[F_ID])), N''), N'#BLANK') AS [{side}]" in sql
    # the prune: rows THIS RUN's merge already changed are out of the comparison (their
    # helbredsforloeb is already in scope, and their untouched siblings would otherwise
    # make a half-updated helbredsforloeb look non-bijective). The threshold is the
    # server clock read before the merge, NOT the delivery date: two deliveries can be
    # packed on the same date, and a date prune would then also drop rows an EARLIER run
    # changed — and with them the evidence of a regrouping that would never be bumped.
    assert "AND ISNULL(tgt.[dw_timestamp_update], '1900-01-01') < CAST(? AS datetime2(0))" in sql
    # DISTINCT is what makes COUNT(*) equal |R| instead of a row count over 8.2 M rows
    assert "SELECT DISTINCT" in sql
    assert "GROUP BY p.[hf]" in sql
    assert (
        "HAVING COUNT(*) <> COUNT(DISTINCT p.[gammel]) OR COUNT(*) <> COUNT(DISTINCT p.[ny]) "
        "OR MAX(CASE WHEN (CASE WHEN p.[gammel] = N'#BLANK' THEN 1 ELSE 0 END) "
        "<> (CASE WHEN p.[ny] = N'#BLANK' THEN 1 ELSE 0 END) THEN 1 ELSE 0 END) = 1;" in sql
    )
    # a detector writes nothing: no SET at all, so dw_timestamp_update is mentioned
    # exactly once — in the prune — and leverance_dato not at all
    assert " SET " not in sql and "UPDATE" not in sql
    assert sql.count("dw_timestamp_update") == 1
    assert "leverance_dato" not in sql


def test_regroup_bump_sql_saetter_kun_tidsstemplet_paa_de_fundne_helbredsforloeb():
    """The bump is what puts a regrouped helbredsforloeb back into staging's delta scope.
    ONLY dw_timestamp_update: leverance_dato means 'the delivery in which this row last
    changed' (review F16) and writing it would also blind the detector's own prune on the
    next delivery."""
    sql = " ".join(udredningsret_archive.build_regroup_bump_sql(D).split())
    set_list = sql.split("UPDATE tgt SET ", 1)[1].split(" FROM ", 1)[0]
    assert set_list == "tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))"
    assert f"FROM {ARC} AS tgt JOIN {TMP} AS o ON o.[hf] = tgt.[helbredsforloeb_noegle]" in sql
    assert "WHERE tgt.[dw_is_deleted] = 0" in sql
    assert "leverance_dato" not in sql
    assert "[PK]" not in sql and "[F_ID]" not in sql
    # takes no parameter — the work table must stay visible to it — and leaves the
    # connection as it found it
    assert "?" not in sql
    assert sql.endswith(f"DROP TABLE IF EXISTS {TMP};")


def test_read_merge_start_laeser_serverens_ur_i_sekundoploesning():
    """The detector's prune threshold must come from the SERVER and be comparable with
    the stamps the merge writes (CAST(SYSDATETIME() AS datetime2(0))) — not from Python's
    clock, and not from the delivery date: two deliveries can be packed on the same date,
    and a date prune would also drop rows an EARLIER run changed. ISO 8601 (style 126)
    parses back to datetime2 whatever the server's language setting is."""
    conn = FakeConn({CLOCK: [(MERGE_START,)]})
    assert udredningsret_archive.read_merge_start(conn) == MERGE_START
    (sql,) = conn.executed
    assert sql == ("SELECT CONVERT(NVARCHAR(19), CAST(SYSDATETIME() AS datetime2(0)), 126)")
    assert conn.commits == 0


def test_refresh_surrogate_ids_udfoerer_committer_og_returnerer_rowcount():
    """Detection -> id refresh -> bump, on ONE cursor with EXACTLY ONE commit, in that
    order. Detection must read arc BEFORE the ids are overwritten: afterwards arc.F_ID
    already equals the delivery's, and the key table it compares against is dropped a
    moment later."""
    conn = FakeConn({REFRESH_IDS: [(7,)], COUNT_TMP: [(2,)]})
    assert udredningsret_archive.refresh_surrogate_ids(conn, D, MERGE_START) == (7, 2)
    assert len(conn.cursors) == 1
    i_scaffold = next(i for i, s in enumerate(conn.executed) if s.startswith("DROP TABLE IF"))
    i_detect = next(i for i, s in enumerate(conn.executed) if s.startswith("WITH par AS"))
    i_count = next(i for i, s in enumerate(conn.executed) if s.startswith(COUNT_TMP))
    i_refresh = next(i for i, s in enumerate(conn.executed) if s.startswith(REFRESH_IDS))
    i_bump = next(i for i, s in enumerate(conn.executed) if s.startswith(BUMP))
    assert i_scaffold < i_detect < i_count < i_refresh < i_bump
    # the prune threshold is bound to the populate statement, and to nothing else
    (params,) = [p for p in conn.cursors[0].params if p]
    assert params == (MERGE_START,)
    assert conn.cursors[0].params[i_detect] == (MERGE_START,)
    # committed exactly once, and AFTER all three — an uncommitted refresh would
    # otherwise ride on record_delivery's commit (or be rolled back with its failure),
    # and a commit between the UPDATE and the bump would let a crash lose the evidence
    assert conn.commits == 1 and conn.executed_at_commit == [len(conn.executed)]


def test_refresh_surrogate_ids_rapporterer_aldrig_negativt_rowcount():
    """Driveren giver -1 naar serveren ikke sender et antal (NOCOUNT/trigger): metadata
    og log skal vise 0, ikke '-1 live rows got PK/F_ID refreshed'."""
    conn = FakeConn({REFRESH_IDS: [(-1,)]})
    assert udredningsret_archive.refresh_surrogate_ids(conn, D, MERGE_START) == (0, 0)
    assert conn.commits == 1


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


def test_helbredsforloeb_recovery_maaler_andelen_af_levende_noegler_i_leverancen():
    live = "COUNT(DISTINCT [helbredsforloeb_noegle])"
    found = f"AND EXISTS (SELECT 1 FROM {KILDE}"
    conn = FakeConn({live: [(1000,)], found: [(990,)]})
    assert udredningsret_archive.measure_helbredsforloeb_recovery(conn, D) == {
        "levende_helbredsforloeb": 1000,
        "genfundet": 990,
        "andel": 0.99,
    }
    # both counts are restricted to live arc rows
    assert all("[dw_is_deleted] = 0" in sql for sql in conn.executed)
    # empty archive (first load)
    empty = FakeConn({live: [(0,)]})
    assert udredningsret_archive.measure_helbredsforloeb_recovery(empty, D)["andel"] == 1.0


def test_count_live_taeller_kun_levende_raekker():
    conn = FakeConn({f"FROM {ARC} WHERE [dw_is_deleted] = 0": [(42,)]})
    assert udredningsret_archive.count_live(conn, D) == 42
    assert conn.executed == [f"SELECT COUNT(*) FROM {ARC} WHERE [dw_is_deleted] = 0"]


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
# _run_archive: no-op, PK/F_ID refresh and key table drop AFTER
# ---------------------------------------------------------------------------

REFRESH = "UPDATE tgt SET tgt.[PK] = src.[PK], tgt.[F_ID] = src.[F_ID]"


def _wired_run(
    monkeypatch,
    live: int,
    manifest_rows=(MANIFEST_ROW,),
    refreshed: int = 3,
    regrouped: int = 0,
    **config_overrides,
):
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
        COUNT_TMP: [(regrouped,)],
        CLOCK: [(MERGE_START,)],
        REFRESH: [(refreshed,)],
    }
    resource = FakeResource(responses)
    # calls: the merge and the delivery registration, each with the number of SQL
    # statements the fake connection had seen at that moment (to place the refresh)
    calls: list[str] = []
    seen_at: dict[str, int] = {}

    def fake_merge(context, mssql, merge_config, run_config):
        calls.append("merge")
        seen_at["merge"] = len(resource.conn.executed)
        return SimpleNamespace(rows_inserted=0, rows_updated=0, rows_soft_deleted=0)

    def fake_record(*a, **k):
        calls.append("record")
        seen_at["record"] = len(resource.conn.executed)

    monkeypatch.setattr(udredningsret_archive, "execute_archive_merge", fake_merge)
    monkeypatch.setattr(udredningsret_archive, "build_archive_metadata", lambda *a, **k: {})
    monkeypatch.setattr(udredningsret_archive, "record_delivery", fake_record)
    context = dg.build_asset_context()
    config = udredningsret_archive.UdredningsretArchiveConfig(**config_overrides)
    resource.seen_at = seen_at
    return resource, calls, lambda: udredningsret_archive._run_archive(context, resource, D, config)


def test_tomt_manifest_er_noop_foer_noegletabel_guards_og_merge(monkeypatch):
    resource, calls, run = _wired_run(monkeypatch, live=5000, manifest_rows=())
    result = run()
    assert calls == []
    assert result.metadata["besluttet"].startswith("no-op")
    assert result.metadata["rows_inserted"] == 0
    # only the manifest was read — nothing built, guarded or dropped
    assert all("manifest" in sql or "sys.objects" in sql for sql in resource.conn.executed)


def test_merger_registrerer_og_dropper_noegletabellen(monkeypatch):
    resource, calls, run = _wired_run(monkeypatch, live=5000)
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
    # decision U1: the PK/F_ID refresh runs exactly once, AFTER the merge, BEFORE the
    # delivery registration and BEFORE the key table it reads is dropped
    refreshes = [i for i, s in enumerate(executed) if s.startswith(REFRESH)]
    assert len(refreshes) == 1
    (i_refresh,) = refreshes
    assert resource.seen_at["merge"] <= i_refresh < resource.seen_at["record"]
    assert i_refresh < len(executed) - 1
    assert result.metadata["surrogat_id_opdateret"] == 3


def test_asset_metadata_baerer_omgrupperinger_og_detektionen_koerer_foer_id_opdateringen(
    monkeypatch,
):
    """U15: the detector runs inside the asset, in the window where BOTH labellings
    exist — after the merge, before the id refresh, and before the key table it reads
    is dropped — and the count reaches the asset's metadata next to
    surrogat_id_opdateret, so a regrouping is visible in the Dagster run."""
    resource, calls, run = _wired_run(monkeypatch, live=5000, refreshed=3, regrouped=2)
    result = run()
    assert calls == ["merge", "record"]
    assert result.metadata["omgrupperinger"] == 2
    assert result.metadata["surrogat_id_opdateret"] == 3
    executed = resource.conn.executed
    i_detect = next(i for i, s in enumerate(executed) if s.startswith("WITH par AS"))
    i_refresh = next(i for i, s in enumerate(executed) if s.startswith(REFRESH))
    i_bump = next(i for i, s in enumerate(executed) if s.startswith(BUMP))
    assert resource.seen_at["merge"] <= i_detect < i_refresh < i_bump
    assert i_bump < resource.seen_at["record"]
    # the prune threshold is the server clock read BEFORE the merge — afterwards it
    # would be later than the stamps the merge wrote and prune the whole archive away
    i_clock = next(i for i, s in enumerate(executed) if CLOCK in s)
    assert i_clock < resource.seen_at["merge"]
    bound = [
        params
        for cursor in resource.conn.cursors
        for sql, params in zip(cursor.executed, cursor.params, strict=True)
        if sql.startswith("WITH par AS")
    ]
    assert bound == [(MERGE_START,)]
    # the key table both statements read is dropped only afterwards (F18)
    assert executed[-1] == f"DROP TABLE IF EXISTS {KILDE};"
    assert i_bump < len(executed) - 1


def test_backfill_springer_surrogat_id_opdatering_og_omgrupperingsdetektion_over(monkeypatch):
    """An older delivery must not push its older PK/F_ID over the archive's newer ones —
    and its grouping is not the one the archive should follow either, so the detector must
    not run and must not bump anything."""
    resource, calls, run = _wired_run(monkeypatch, live=5000, regrouped=9, backfill_historical=True)
    result = run()
    assert calls == ["merge", "record"]
    assert not any(s.startswith("UPDATE") for s in resource.conn.executed)
    assert not any(TMP in s for s in resource.conn.executed)
    assert result.metadata["surrogat_id_opdateret"] == 0
    assert result.metadata["omgrupperinger"] == 0
    assert resource.conn.executed[-1] == f"DROP TABLE IF EXISTS {KILDE};"


# ---------------------------------------------------------------------------
# the checkpoint gate
# ---------------------------------------------------------------------------


class _FakeDfm:
    def __init__(self, checkpoint):
        self.checkpoint = checkpoint
        self.set_calls: list[tuple] = []

    def get_checkpoint(self, schema, unit):
        assert schema == "dfm"
        return self.checkpoint

    def set_checkpoint(self, schema, unit, *, value):
        assert schema == "dfm"
        self.set_calls.append((unit, value))


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

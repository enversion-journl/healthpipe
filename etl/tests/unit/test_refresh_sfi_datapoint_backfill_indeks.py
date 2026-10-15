# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Backfill-tilstand for fhir.SFI_datapoint_store: brede indeks fra under store vinduer.

Fejl disse tests fanger:
- den daglige delta udløser frakobling (aftagerne mister søgestier uden grund)
- det unikke indeks slås fra (entydigheden håndhæves ikke under indlæsning)
- indeks genopbygges ikke efter løkken, eller ikke efter en fejl
- indeks efterladt slået fra af en dræbt kørsel bliver ikke repareret
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from DSO.load import refresh_sfi_datapoint as m

pytestmark = pytest.mark.unit


class _Cursor:
    def __init__(self, indeks: list[tuple[str, int]]) -> None:
        self.indeks = indeks
        self.sql: list[str] = []
        self.fail_on: str | None = None

    def execute(self, sql, *_a):
        self.sql.append(" ".join(sql.split()))
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("simuleret fejl")

    def fetchall(self):
        return list(self.indeks)


class _Conn:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _ctx():
    return SimpleNamespace(
        log=SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None)
    )


# Storens ikke-unikke B-træer (columnstoren og det unikke indeks er ikke kandidater).
_STORE_INDEKS = [
    ("IX_FHIR_SFI_datapoint_store_PatientOccurredDt", 0),
]


def test_kandidat_sql_udelader_pk_og_unikke_indeks():
    sql = " ".join(m._backfill_candidate_indexes_sql().split())
    assert "i.type = 2" in sql
    assert "i.is_primary_key = 0" in sql and "i.is_unique = 0" in sql
    assert "SFI_datapoint_store" in sql


def test_ingen_beholdte_indeks_ud_over_de_unikke():
    assert m._BACKFILL_KEEP_INDEXES == frozenset()
    cur = _Cursor(_STORE_INDEKS)
    navne = [n for n, _ in m._list_backfill_candidate_indexes(cur)]
    assert navne == ["IX_FHIR_SFI_datapoint_store_PatientOccurredDt"]


def test_graense_og_tilstand(monkeypatch):
    monkeypatch.delenv(m._BACKFILL_INDEX_MODE_ENV, raising=False)
    monkeypatch.delenv(m._BACKFILL_MIN_RESOURCES_ENV, raising=False)
    assert m._should_disable_indexes_for_backfill(250_000) is False, "daglig delta"
    assert m._should_disable_indexes_for_backfill(1_000_000) is True, "kvartalsvindue"
    monkeypatch.setenv(m._BACKFILL_MIN_RESOURCES_ENV, "100")
    assert m._should_disable_indexes_for_backfill(100) is True
    monkeypatch.setenv(m._BACKFILL_INDEX_MODE_ENV, "never")
    assert m._should_disable_indexes_for_backfill(10**9) is False
    monkeypatch.setenv(m._BACKFILL_INDEX_MODE_ENV, "always")
    assert m._should_disable_indexes_for_backfill(1) is True
    monkeypatch.setenv(m._BACKFILL_INDEX_MODE_ENV, "vrøvl")
    assert m._backfill_index_mode() == "auto"


def test_daglig_delta_roerer_ingen_indeks(monkeypatch):
    monkeypatch.delenv(m._BACKFILL_INDEX_MODE_ENV, raising=False)
    cur, conn = _Cursor(_STORE_INDEKS), _Conn()
    with m._BackfillIndexScope(cur, conn, _ctx(), changed_resources=250_000) as scope:
        pass
    assert scope.active is False and scope.disabled == []
    assert not [s for s in cur.sql if "ALTER INDEX" in s]


def test_backfill_slaar_brede_indeks_fra_og_genopbygger_efter(monkeypatch):
    monkeypatch.delenv(m._BACKFILL_INDEX_MODE_ENV, raising=False)
    cur, conn = _Cursor(_STORE_INDEKS), _Conn()
    with m._BackfillIndexScope(cur, conn, _ctx(), changed_resources=5_000_000) as scope:
        assert scope.active is True
        disabled = [s for s in cur.sql if "DISABLE" in s]
        assert len(disabled) == 1 and "PatientOccurredDt" in disabled[0]
        assert conn.commits == 1, "frakoblingen committes før løkken"
    rebuilt = [s for s in cur.sql if "REBUILD" in s]
    assert len(rebuilt) == 1
    assert set(scope.rebuilt_ms) == set(scope.disabled)
    assert conn.commits == 2, "ét commit pr. genopbygget indeks"


def test_fejl_i_loekken_genopbygger_stadig_og_propagerer(monkeypatch):
    monkeypatch.delenv(m._BACKFILL_INDEX_MODE_ENV, raising=False)
    cur, conn = _Cursor(_STORE_INDEKS), _Conn()
    with pytest.raises(RuntimeError, match="portion fejlede"):
        with m._BackfillIndexScope(cur, conn, _ctx(), changed_resources=5_000_000):
            raise RuntimeError("portion fejlede")
    assert len([s for s in cur.sql if "REBUILD" in s]) == 1
    assert conn.rollbacks == 1


def test_efterladte_deaktiverede_indeks_repareres_ved_daglig_koersel(monkeypatch):
    monkeypatch.delenv(m._BACKFILL_INDEX_MODE_ENV, raising=False)
    indeks = [(n, 1) for n, _ in _STORE_INDEKS]
    cur, conn = _Cursor(indeks), _Conn()
    with m._BackfillIndexScope(cur, conn, _ctx(), changed_resources=1000) as scope:
        pass
    rebuilt = [s for s in cur.sql if "REBUILD" in s]
    assert len(rebuilt) == 1 and "PatientOccurredDt" in rebuilt[0]
    assert scope.active is False and scope.disabled == []


def test_allerede_deaktiverede_indeks_i_backfill_slaas_ikke_fra_igen_men_genopbygges(monkeypatch):
    monkeypatch.delenv(m._BACKFILL_INDEX_MODE_ENV, raising=False)
    indeks = [(n, 1) for n, _ in _STORE_INDEKS]
    cur, conn = _Cursor(indeks), _Conn()
    with m._BackfillIndexScope(cur, conn, _ctx(), changed_resources=5_000_000) as scope:
        assert len([s for s in cur.sql if "DISABLE" in s]) == 0
        assert len(scope.disabled) == 1
    assert len([s for s in cur.sql if "REBUILD" in s]) == 1


def test_run_tags_overstyrer_miljoeet(monkeypatch):
    monkeypatch.delenv(m._BACKFILL_INDEX_MODE_ENV, raising=False)
    monkeypatch.delenv(m._BACKFILL_MIN_RESOURCES_ENV, raising=False)
    ctx = SimpleNamespace(
        run=SimpleNamespace(tags={m._BACKFILL_MIN_RESOURCES_TAG: "100000"}),
        log=SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None),
    )
    assert m._backfill_min_resources(ctx) == 100_000
    assert m._should_disable_indexes_for_backfill(150_000, ctx) is True
    assert (
        m._should_disable_indexes_for_backfill(150_000) is False
    ), "uden tag gælder default 1 mio."
    ctx.run.tags[m._BACKFILL_INDEX_MODE_TAG] = "never"
    assert m._should_disable_indexes_for_backfill(10**9, ctx) is False
    # attrap uden run-attribut: tom tag-dict, ingen fejl
    assert m._run_tags(_ctx()) == {}


def test_watermark_reset_skriver_parametriseret_update_og_kraever_raekke():
    class _Cur:
        def __init__(self, row):
            self.row = row
            self.sql = []

        def execute(self, sql, params=None):
            self.sql.append((" ".join(sql.split()), params))

        def fetchone(self):
            return self.row

        def close(self):
            pass

    class _Conn:
        def __init__(self, cur):
            self.cur = cur
            self.commits = 0

        def cursor(self):
            return self.cur

        def commit(self):
            self.commits += 1

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Res:
        def __init__(self, conn):
            self.conn = conn

        def get_connection(self):
            return self.conn

    cur = _Cur(("2026-09-07 06:29:00", "success"))
    conn = _Conn(cur)
    ctx = SimpleNamespace(log=SimpleNamespace(info=lambda *a, **k: None))
    cfg = m.SfiDatapointWatermarkResetConfig(
        new_value="2026-09-06 06:00:00", reason="måling: replay af to dagsdeltaer"
    )
    res = m._reset_sfi_datapoint_watermark(ctx.log, cfg, _Res(conn))
    upd = [s for s, p in cur.sql if s.startswith("UPDATE")]
    assert len(upd) == 1 and "last_sync_timestamp = ?" in upd[0] and conn.commits == 1
    assert cur.sql[-1][1][2:] == ["sfi_datapoint", "refresh_daily"]
    assert res.metadata["old_value"].value == "2026-09-07 06:29:00"

    cur2 = _Cur(None)
    with pytest.raises(RuntimeError, match="Ingen watermark-række"):
        m._reset_sfi_datapoint_watermark(ctx.log, cfg, _Res(_Conn(cur2)))

    # create_if_missing: rækken oprettes med det angivne tidspunkt (efter en migration,
    # der har slettet watermarks), i stedet for at næste refresh genopbygger alt.
    cur3, conn3 = _Cur(None), None
    conn3 = _Conn(cur3)
    cfg3 = m.SfiDatapointWatermarkResetConfig(
        new_value="2026-09-09 10:20:00", reason="seed efter migration", create_if_missing=True
    )
    res3 = m._reset_sfi_datapoint_watermark(ctx.log, cfg3, _Res(conn3))
    inserts = [(s, p) for s, p in cur3.sql if "INSERT INTO [meta].[SyncWatermarks]" in s]
    assert len(inserts) == 1 and "IF NOT EXISTS" in inserts[0][0] and conn3.commits == 1
    assert inserts[0][1][:2] == ["sfi_datapoint", "refresh_daily"]
    assert inserts[0][1][5] == "reset: seed efter migration"
    assert res3.metadata["old_value"].value == "None"


def test_fuld_afstemning_saar_manglende_watermarks_til_kandidatsaettets_tidspunkt():
    class _Cur:
        def __init__(self):
            self.sql = []
            self.rowcount = 1

        def execute(self, sql, params=None):
            self.sql.append((" ".join(sql.split()), params))

    class _Conn:
        commits = 0

        def commit(self):
            self.commits += 1

    cur, conn = _Cur(), _Conn()
    log = SimpleNamespace(info=lambda *a, **k: None)
    seeded = m._seed_missing_watermarks(
        cur, conn, log, captured_at="2026-09-09 10:35:18", exclude=m._WATERMARK_TABLE_RECONCILIATION
    )
    assert seeded == ["refresh_daily", "refresh_monthly"]
    assert all(
        "IF NOT EXISTS" in s and "INSERT INTO [meta].[SyncWatermarks]" in s for s, _ in cur.sql
    )
    assert [p[1] for _, p in cur.sql] == ["refresh_daily", "refresh_monthly"]
    assert all(
        p[4] == "2026-09-09 10:35:18" and p[5] == "seeded_by_reconciliation" for _, p in cur.sql
    )
    assert conn.commits == 1
    # setup-SQL'en leverer tidspunktet sammen med tællingen
    assert "AS candidates_captured_at" in m._build_reconciliation_setup_sql()

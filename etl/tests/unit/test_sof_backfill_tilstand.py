# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Backfill-tilstand for SOF-MERGE'n på staging: portioneret MERGE og indeks fra.

Fejl disse tests fanger:
- det daglige delta (lille) rammer porten, eller en tabel uden tilvalg gør
- portionerne deler dublerede nøgler, så dedup pr. nøgle bryder (PK-brud)
- MERGE'ns tekst ændres for kaldere uden portionering (den daglige sti)
- indeks slås fra og genopbygges ikke efter løkken eller efter en fejl
- portioner committes ikke hver for sig
"""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from DSA.FHIR.utils.sql_on_fhir import sof_utils as su
from DSA.FHIR.utils.sql_on_fhir.sof_utils import (
    SOF_RESOURCE_CONFIGS,
    SofBackfillPlan,
    build_fhir_merge_sql,
    extract_and_merge_sof_view,
    resolve_sof_backfill_plan,
)

pytestmark = pytest.mark.unit


def _norm(text: str) -> str:
    return " ".join(text.split())


class _Cursor:
    """Svar pr. SQL-delstreng; husker alt der udføres."""

    def __init__(self, responses=None, rowcount=7):
        self.responses = list(responses or [])
        self.sql: list[str] = []
        self.rowcount = rowcount
        self._rows: list = []
        self.description = None

    def execute(self, sql, params=None):
        text = _norm(sql)
        self.sql.append(text)
        self._rows = []
        for key, rows in self.responses:
            if key in text:
                self._rows = list(rows)
                break

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def executemany(self, sql, rows):
        self.sql.append("EXECUTEMANY " + _norm(sql))

    def close(self):
        pass

    def executed(self, fragment):
        return [s for s in self.sql if fragment in s]


class _Conn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1
        self._cursor.sql.append("--COMMIT--")

    def rollback(self):
        self.rollbacks += 1


def _ctx(tags=None):
    return SimpleNamespace(
        run=SimpleNamespace(tags=tags or {}),
        log=SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None),
    )


def _clear_env(monkeypatch):
    for var in (
        su.SOF_BACKFILL_MODE_ENV,
        su.SOF_MERGE_BATCH_ROWS_ENV,
        su.SOF_INDEX_DISABLE_ENV,
        su.SOF_BACKFILL_MIN_ROWS_ENV,
        su.SOF_BACKFILL_MIN_RATIO_ENV,
    ):
        monkeypatch.delenv(var, raising=False)


# ---------------------------------------------------------------------------
# Plan / port
# ---------------------------------------------------------------------------


def test_qr_tables_opt_in_and_others_do_not():
    assert SOF_RESOURCE_CONFIGS["questionnaire_response_view"]["backfill_mode"] is True
    assert SOF_RESOURCE_CONFIGS["questionnaire_response_item_view"]["backfill_mode"] is True
    assert not SOF_RESOURCE_CONFIGS["patient_view"].get("backfill_mode")


def test_table_without_opt_in_never_enters_backfill_and_runs_no_sql(monkeypatch):
    _clear_env(monkeypatch)
    cur = _Cursor()
    plan = resolve_sof_backfill_plan(
        cur, "Patient", SOF_RESOURCE_CONFIGS["patient_view"], 10**8, _ctx()
    )
    assert plan.active is False and cur.sql == []


def test_daily_delta_stays_below_the_gate(monkeypatch):
    _clear_env(monkeypatch)
    cfg = SOF_RESOURCE_CONFIGS["questionnaire_response_view"]
    cur = _Cursor([("sys.partitions", [(37_000_000,)])])
    plan = resolve_sof_backfill_plan(cur, "QuestionnaireResponse", cfg, 90_000, _ctx())
    assert plan.active is False and "min_rows" in plan.reason
    assert cur.sql == [], "under min_rows spørges målet ikke"
    plan = resolve_sof_backfill_plan(cur, "QuestionnaireResponse", cfg, 2_000_000, _ctx())
    assert (
        plan.active is False and "ratio" in plan.reason
    ), "2 mio. ind i 37 mio. er 5 % — under 10 %"


def test_backfill_window_opens_the_gate_with_defaults(monkeypatch):
    _clear_env(monkeypatch)
    cfg = SOF_RESOURCE_CONFIGS["questionnaire_response_view"]
    cur = _Cursor([("sys.partitions", [(37_000_000,)])])
    plan = resolve_sof_backfill_plan(cur, "QuestionnaireResponse", cfg, 6_000_000, _ctx())
    assert plan.active is True
    assert plan.batch_rows == su.SOF_DEFAULT_MERGE_BATCH_ROWS and plan.disable_indexes is True
    assert plan.incoming_rows == 6_000_000 and plan.target_rows == 37_000_000
    assert "active" in plan.describe()


def test_tags_steer_mode_batch_size_and_index_disable(monkeypatch):
    _clear_env(monkeypatch)
    cfg = SOF_RESOURCE_CONFIGS["questionnaire_response_item_view"]
    never = resolve_sof_backfill_plan(
        _Cursor(),
        "QuestionnaireResponse_item",
        cfg,
        10**8,
        _ctx({su.SOF_BACKFILL_MODE_TAG: "never"}),
    )
    assert never.active is False
    always = resolve_sof_backfill_plan(
        _Cursor(),
        "QuestionnaireResponse_item",
        cfg,
        10,
        _ctx(
            {
                su.SOF_BACKFILL_MODE_TAG: "always",
                su.SOF_MERGE_BATCH_ROWS_TAG: "0",
                su.SOF_INDEX_DISABLE_TAG: "off",
            }
        ),
    )
    assert always.active is True and always.batch_rows == 0 and always.disable_indexes is False
    monkeypatch.setenv(su.SOF_MERGE_BATCH_ROWS_ENV, "100000")
    env_plan = resolve_sof_backfill_plan(
        _Cursor(), "QuestionnaireResponse_item", cfg, 10, _ctx({su.SOF_BACKFILL_MODE_TAG: "always"})
    )
    assert env_plan.batch_rows == 100_000, "miljøet gælder, når tagget mangler"
    monkeypatch.setenv(su.SOF_MERGE_BATCH_ROWS_ENV, "vrøvl")
    assert (
        resolve_sof_backfill_plan(
            _Cursor(),
            "QuestionnaireResponse_item",
            cfg,
            10,
            _ctx({su.SOF_BACKFILL_MODE_TAG: "always"}),
        ).batch_rows
        == su.SOF_DEFAULT_MERGE_BATCH_ROWS
    )


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------


def test_batch_assignment_uses_dense_rank_over_the_full_key():
    sql = _norm(su._build_stage_batch_assignment_sql("##S", ["qr_id", "link_id"], 250_000))
    assert "DENSE_RANK() OVER (ORDER BY [qr_id], [link_id]) AS __sof_rank" in sql
    assert "UPDATE ranked SET [__sof_batch] = (__sof_rank - 1) / 250000" in sql


def test_merge_without_filter_is_unchanged_and_filter_lands_in_both_source_shapes():
    cols = ["qr_id", "status", "last_updated"]
    plain = build_fhir_merge_sql("QuestionnaireResponse", cols, ["qr_id"], source_table="##S")
    assert "USING ##S AS src" in plain and "__sof_batch" not in plain
    filtered = _norm(
        build_fhir_merge_sql(
            "QuestionnaireResponse",
            cols,
            ["qr_id"],
            source_table="##S",
            source_filter="[__sof_batch] = 3",
        )
    )
    assert (
        "USING (SELECT [qr_id], [status], [last_updated] FROM ##S WHERE [__sof_batch] = 3) AS src"
        in filtered
    )
    dedup = _norm(
        build_fhir_merge_sql(
            "Patient_Address",
            ["patient_id", "address_period_start", "line", "last_updated"],
            ["patient_id", "address_period_start"],
            source_table="##S",
            deduplicate_source_by_pk=True,
            source_filter="[__sof_batch] = 3",
        )
    )
    assert (
        "FROM ##S AS src_base WHERE [__sof_batch] = 3 ) AS dedup WHERE __sof_dedup_rn = 1" in dedup
    )
    assert "NOT MATCHED BY SOURCE" not in dedup, "portionering forudsætter ingen kilde-negativ gren"


def test_merge_in_batches_numbers_reindexes_and_commits_per_batch():
    cur = _Cursor([("SELECT MAX([__sof_batch])", [(2,)])], rowcount=10)
    conn = _Conn(cur)
    total, batches = su._merge_in_batches(
        cur,
        conn,
        _ctx(),
        stage_table="##S",
        table_name="QuestionnaireResponse",
        columns=["qr_id", "status", "last_updated"],
        primary_keys=["qr_id"],
        batch_rows=250_000,
        dedup_enabled=False,
        dedup_tiebreak_columns=None,
        resurrect_soft_deleted=True,
        computed_columns=None,
    )
    assert (total, batches) == (30, 3)
    assert cur.executed("ALTER TABLE ##S ADD [__sof_batch] INT NULL")
    assert cur.executed("DENSE_RANK() OVER (ORDER BY [qr_id])")
    assert cur.executed(
        "CREATE CLUSTERED INDEX IX_SofTemp_PK ON ##S ([__sof_batch], [qr_id]) WITH (DROP_EXISTING = ON)"
    )
    merges = cur.executed("MERGE [fhir].[QuestionnaireResponse] AS tgt")
    assert len(merges) == 3
    assert [f"WHERE [__sof_batch] = {b}" in m for b, m in enumerate(merges)] == [True, True, True]
    assert conn.commits == 4, "ét commit efter nummereringen og ét pr. portion"
    # rækkefølge: hver MERGE efterfølges af et commit
    idx = [i for i, s in enumerate(cur.sql) if s.startswith("MERGE")]
    assert all(cur.sql[i + 1] == "--COMMIT--" for i in idx)


def test_disable_lists_all_nonclustered_but_only_disables_enabled_ones():
    cur = _Cursor([("FROM sys.indexes i", [("IX_a", 0), ("IX_b", 1)])])
    names = su._disable_target_indexes(cur, "QuestionnaireResponse")
    assert names == ["IX_a", "IX_b"]
    disables = cur.executed("DISABLE")
    assert disables == ["ALTER INDEX [IX_a] ON [fhir].[QuestionnaireResponse] DISABLE"]
    conn = _Conn(cur)
    timings = su._rebuild_target_indexes(cur, conn, "QuestionnaireResponse", names, _ctx())
    assert set(timings) == {"IX_a", "IX_b"} and conn.commits == 2


@pytest.mark.parametrize(
    ("disable_all", "disabled", "rebuilt"),
    [(True, ["IX_a", "IX_c"], ["IX_a", "IX_b"]), (False, ["IX_c"], ["IX_b"])],
)
def test_held_consumer_index_stays_off_and_a_leftover_is_healed(disable_all, disabled, rebuilt):
    cur = _Cursor([("FROM sys.indexes i", [("IX_a", 0), ("IX_b", 1), ("IX_c", 0)])])
    names = su._disable_target_indexes(
        cur, "QuestionnaireResponse", disable_all=disable_all, hold=frozenset({"IX_c"})
    )
    assert names == rebuilt
    assert cur.executed("DISABLE") == [
        f"ALTER INDEX [{n}] ON [fhir].[QuestionnaireResponse] DISABLE" for n in disabled
    ]


def test_staging_keeps_the_indexes_the_dso_sync_reads_the_delta_with():
    for view in ("questionnaire_response_view", "questionnaire_response_item_view"):
        held = SOF_RESOURCE_CONFIGS[view]["consumer_indexes"]
        assert held and not [n for n in held if "DWEffective" in n or "Watermark" in n], held


# ---------------------------------------------------------------------------
# Hele stien med attrapper
# ---------------------------------------------------------------------------


class _PgCursor:
    def __init__(self, chunks, columns):
        self._chunks = list(chunks)
        self.description = [(c,) for c in columns]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, *_a):
        return None

    def fetchone(self):
        return ("1.0",)

    def fetchall(self):
        return []

    def fetchmany(self, _n):
        return self._chunks.pop(0) if self._chunks else []


class _PgResource:
    def __init__(self, cursor):
        self._cursor = cursor

    @contextmanager
    def get_connection(self):
        yield SimpleNamespace(cursor=lambda name=None, **k: self._cursor)


class _MssqlResource:
    host = "h"
    port = 0
    database = "d"

    def __init__(self, conn):
        self._conn = conn

    @contextmanager
    def get_connection(self, *a, **k):
        yield self._conn


def _run_qr_extract(plan, consumers_absent_since=None):
    columns = ["qr_id", "questionnaire", "status", "dw_is_deleted", "last_updated", "created_at"]
    pg = _PgCursor([[("q1", "Q|1", "completed", None, "t", "t")]], columns)
    cur = _Cursor(
        [
            (
                "INFORMATION_SCHEMA.COLUMNS",
                [(c, "nvarchar", 255, None, None) for c in columns],
            ),
            ("FROM sys.indexes i", [("IX_FHIR_QR_LastUpdated", 0), ("IX_FHIR_QR_DWEffective", 0)]),
            ("SELECT MAX([__sof_batch])", [(1,)]),
            ("[meta].[DFMState]", [(consumers_absent_since,)] if consumers_absent_since else []),
        ],
        rowcount=1,
    )
    conn = _Conn(cur)
    timings: dict = {}
    rows, affected = extract_and_merge_sof_view(
        fhir_database=_PgResource(pg),
        mssql_target=_MssqlResource(conn),
        view_name="questionnaire_response_view",
        resource_table="questionnaireresponse",
        id_column="qr_id",
        table_name="QuestionnaireResponse",
        primary_keys=["qr_id"],
        context=_ctx(),
        backfill_override=plan,
        timings=timings,
    )
    return rows, affected, cur, conn, timings


def test_backfill_plan_drives_disable_batched_merge_and_rebuild():
    plan = SofBackfillPlan(True, 250_000, True, "test")
    rows, affected, cur, conn, timings = _run_qr_extract(plan)
    assert rows == 1 and affected == 2, "to portioner à rowcount 1"
    disables = cur.executed("] ON [fhir].[QuestionnaireResponse] DISABLE")
    rebuilds = cur.executed("] ON [fhir].[QuestionnaireResponse] REBUILD")
    merges = cur.executed("MERGE [fhir].[QuestionnaireResponse] AS tgt")
    assert len(disables) == 2 and len(rebuilds) == 2 and len(merges) == 2
    first_merge = next(i for i, s in enumerate(cur.sql) if s.startswith("MERGE"))
    assert all(cur.sql.index(d) < first_merge for d in disables), "indeks fra før MERGE"
    assert all(cur.sql.index(r) > cur.sql.index(merges[-1]) for r in rebuilds), "genopbygget efter"
    assert timings["merge_batches"] == 2 and timings["disabled_indexes"] == [
        "IX_FHIR_QR_LastUpdated",
        "IX_FHIR_QR_DWEffective",
    ]
    assert timings["plan"].startswith("active")


def test_the_dfm_switch_holds_the_consumer_index_off_through_a_backfill_load():
    plan = SofBackfillPlan(True, 250_000, True, "test")
    *_, cur, _conn, timings = _run_qr_extract(plan, consumers_absent_since="2026-10-27 09:00:00")
    assert len(cur.executed("] ON [fhir].[QuestionnaireResponse] DISABLE")) == 2
    assert cur.executed("] ON [fhir].[QuestionnaireResponse] REBUILD") == [
        "ALTER INDEX [IX_FHIR_QR_DWEffective] ON [fhir].[QuestionnaireResponse] REBUILD"
    ]
    assert timings["disabled_indexes"] == ["IX_FHIR_QR_DWEffective"]


def test_inactive_plan_keeps_the_single_statement_path():
    plan = SofBackfillPlan(False, 0, False, "test")
    rows, affected, cur, conn, timings = _run_qr_extract(plan)
    merges = cur.executed("MERGE [fhir].[QuestionnaireResponse] AS tgt")
    assert len(merges) == 1 and "__sof_batch" not in merges[0]
    assert not cur.executed("DISABLE") and not cur.executed("REBUILD")
    assert timings["merge_batches"] == 0 and timings["disabled_indexes"] == []


def test_failed_merge_still_rebuilds_indexes_and_propagates():
    plan = SofBackfillPlan(True, 0, True, "test")
    columns = ["qr_id", "questionnaire", "status", "dw_is_deleted", "last_updated", "created_at"]
    pg = _PgCursor([[("q1", "Q|1", "completed", None, "t", "t")]], columns)

    class _Failing(_Cursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if sql.lstrip().startswith("MERGE"):
                raise RuntimeError("MERGE faldt")

    cur = _Failing(
        [
            ("INFORMATION_SCHEMA.COLUMNS", [(c, "nvarchar", 255, None, None) for c in columns]),
            ("FROM sys.indexes i", [("IX_FHIR_QR_LastUpdated", 0)]),
        ]
    )
    conn = _Conn(cur)
    with pytest.raises(RuntimeError, match="MERGE faldt"):
        extract_and_merge_sof_view(
            fhir_database=_PgResource(pg),
            mssql_target=_MssqlResource(conn),
            view_name="questionnaire_response_view",
            resource_table="questionnaireresponse",
            id_column="qr_id",
            table_name="QuestionnaireResponse",
            primary_keys=["qr_id"],
            context=_ctx(),
            backfill_override=plan,
        )
    assert cur.executed(
        "ALTER INDEX [IX_FHIR_QR_LastUpdated] ON [fhir].[QuestionnaireResponse] REBUILD"
    )
    assert conn.rollbacks == 1

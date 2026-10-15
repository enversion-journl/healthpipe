# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Månedspartitionering af fhir.SFI_datapoint_store og SWITCH-indlæsning.

Fejl disse tests fanger:
- DDL'ens partitionsgrænser eller indeks afviger fra Python-definitionen, så arbejds-
  tabellens indeks ikke er identiske med storens (SWITCH fejler først på serveren)
- arbejdstabellen har andre kolonner end storen
- den daglige sti ændres (routed=False skal give præcis dagens SQL-form)
- strategi-valget vælger SWITCH på en upartitioneret store eller på en genkørsel
- indeks slået fra af en dræbt kørsel repareres ikke, eller arbejdstabellens rester tømmes ikke
- SWITCH-fejl på én partition vælter kørslen i stedet for at falde tilbage til INSERT
- columnstoren eller et B-træ mangler på arbejdstabellen før skiftet
"""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from DSO.load import refresh_sfi_datapoint as m
from DSO.load import sfi_store_partitioning as p

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]
STORE_DDL = REPO_ROOT / "infrastructure" / "ddl" / "tables" / "fhir" / "02_sfi_datapoint_store.sql"
MIGRATION = (
    REPO_ROOT / "infrastructure" / "ddl" / "migrations" / "20260908_sfi_store_columnstore.sql"
)


def _norm(text: str) -> str:
    return " ".join(text.split())


# ---------------------------------------------------------------------------
# Attrapper
# ---------------------------------------------------------------------------


class FakeCursor:
    """Svar pr. SQL-delstreng (første match vinder). ``fail`` = delstreng -> antal fejl
    (eller -1 for altid) og fejltekst."""

    def __init__(self, responses=None, fail=None):
        self.responses = list(responses or [])
        self.fail = dict(fail or {})
        self.sql: list[str] = []
        self.params: list = []
        self._rows: list = []
        self.description = None

    def execute(self, sql, params=None):
        text = _norm(sql)
        self.sql.append(text)
        self.params.append(params)
        for key, spec in list(self.fail.items()):
            if key in text:
                count, message = spec
                if count == -1 or count > 0:
                    if count > 0:
                        self.fail[key] = (count - 1, message)
                    raise RuntimeError(message)
        self._rows = []
        self.description = None
        for key, rows in self.responses:
            if key in text:
                self._rows = list(rows)
                self.description = [("c",)]
                break

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def nextset(self):
        return False

    def executed(self, fragment: str) -> list[str]:
        return [s for s in self.sql if fragment in s]


class FakeConn:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class FakeLog:
    def __init__(self):
        self.infos: list[str] = []
        self.warnings: list[str] = []

    def info(self, msg, *args):
        self.infos.append(msg % args if args else msg)

    def warning(self, msg, *args):
        self.warnings.append(msg % args if args else msg)


def _ctx(tags=None):
    log = FakeLog()
    return SimpleNamespace(log=log, run=SimpleNamespace(tags=tags or {})), log


# ---------------------------------------------------------------------------
# Grænser og indeksdefinitioner
# ---------------------------------------------------------------------------


def test_monthly_boundaries_are_contiguous_first_of_month():
    b = p.monthly_boundaries()
    assert b[0].isoformat() == "2021-10-01" and b[-1].isoformat() == "2035-12-01"
    assert len(b) == 171
    for prev, nxt in zip(b, b[1:], strict=False):
        assert nxt.day == 1
        assert (nxt.year, nxt.month) == (
            (prev.year + 1, 1) if prev.month == 12 else (prev.year, prev.month + 1)
        )
    with pytest.raises(ValueError):
        p.monthly_boundaries(first=b[0].replace(day=2))


def test_boundary_literals_are_iso_datetime2_strings():
    lits = p.boundary_literals()
    assert lits.startswith("N'2021-10-01T00:00:00', N'2021-11-01T00:00:00'")
    assert lits.endswith("N'2035-12-01T00:00:00'")
    assert lits.count("N'") == 171


def test_index_definitions_are_two_aligned_btrees_on_a_columnstore():
    names = [ix.name for ix in p.STORE_INDEXES]
    assert names == [
        "UX_FHIR_SFI_datapoint_store_DatapointKey",
        "IX_FHIR_SFI_datapoint_store_PatientOccurredDt",
    ]
    assert p.NON_ALIGNED_INDEXES == (), "på en columnstore skal alle B-træer være alignede"
    for ix in p.STORE_INDEXES:
        assert ix.key_columns[-1] == p.PARTITION_COLUMN, f"{ix.name}: partitionskolonnen sidst"
        assert ix.include_columns == (), "smalle B-træer: kolonnerne læses fra columnstoren"
        sql = ix.create_sql("[x].[y]")
        assert "STATISTICS_INCREMENTAL = ON" in sql and "DATA_COMPRESSION = PAGE" in sql
        assert f"ON [{p.PARTITION_SCHEME}]([{p.PARTITION_COLUMN}])" in sql
    assert p.STORE_INDEXES[0].unique and "CREATE UNIQUE NONCLUSTERED" in p.STORE_INDEXES[
        0
    ].create_sql("[x].[y]")


def test_columnstore_sql_names_and_reorganize():
    assert p.columnstore_index_name() == "CCI_FHIR_SFI_datapoint_store" == p.COLUMNSTORE_INDEX
    assert p.columnstore_index_name(p.WORK_TABLE) == "CCI_FHIR_SFI_datapoint_store_backfill"
    sql = _norm(p.columnstore_index_sql())
    assert sql == (
        "CREATE CLUSTERED COLUMNSTORE INDEX [CCI_FHIR_SFI_datapoint_store] "
        "ON [fhir].[SFI_datapoint_store] ON [ps_SFI_datapoint_store_Maaned]([occurred_at_dt]);"
    )
    assert "WITH (DROP_EXISTING = ON)" in p.columnstore_index_sql("[fhir].[x]", drop_existing=True)
    assert p.reorganize_sql() == (
        "ALTER INDEX [CCI_FHIR_SFI_datapoint_store] ON [fhir].[SFI_datapoint_store] "
        "REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);"
    )
    assert "i.type = 5" in p.columnstore_exists_sql()


# ---------------------------------------------------------------------------
# DDL-kontrakter
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def store_ddl() -> str:
    return STORE_DDL.read_text(encoding="utf-8")


def _create_table_block(ddl: str, table: str) -> str:
    match = re.search(rf"CREATE TABLE \[fhir\]\.\[{table}\]\s*\(", ddl)
    assert match, f"CREATE TABLE {table} mangler"
    index, depth = match.end() - 1, 0
    while index < len(ddl):
        if ddl[index] == "(":
            depth += 1
        elif ddl[index] == ")":
            depth -= 1
            if depth == 0:
                break
        index += 1
    return ddl[match.end() : index]


def _column_lines(block: str) -> list[str]:
    lines = []
    for raw in block.splitlines():
        line = raw.strip()
        if not line.startswith("[") or line.startswith("[sfi_datapoint_id]"):
            continue
        lines.append(_norm(line.rstrip(",")))
    return lines


def test_ddl_boundaries_match_python(store_ddl):
    match = re.search(
        r"CREATE PARTITION FUNCTION \[pf_SFI_datapoint_store_Maaned\] \(DATETIME2\(0\)\)\s*"
        r"AS RANGE RIGHT FOR VALUES \((.*?)\);",
        store_ddl,
        re.DOTALL,
    )
    assert match, "partitionsfunktionen mangler i DDL'en"
    assert _norm(match.group(1)) == p.boundary_literals()
    # Målebænken opretter samme objekter før infrastrukturjobbet — samme tekst.
    assert p.boundary_literals() in _norm(p.partition_function_sql())
    assert "ALL TO ([PRIMARY])" in store_ddl and "ALL TO ([PRIMARY])" in p.partition_scheme_sql()


def test_ddl_index_statements_match_python_definitions(store_ddl):
    normalized = _norm(store_ddl)
    assert _norm(p.columnstore_index_sql()) in normalized, "DDL'ens columnstore afviger"
    for ix in p.STORE_INDEXES:
        statement = _norm(ix.create_sql(p.STORE_TABLE))
        assert statement in normalized, f"DDL'ens {ix.name} afviger fra STORE_INDEXES"
    # Kun de to B-træer og columnstoren — ingen brede indeks ved siden af columnstoren
    # (kommentarlinjer tælles ikke med).
    code = _norm(
        "\n".join(line for line in store_ddl.splitlines() if not line.strip().startswith("--"))
    )
    assert code.count("CREATE UNIQUE NONCLUSTERED INDEX") == 1
    assert code.count("CREATE NONCLUSTERED INDEX") == 1
    assert code.count("CREATE CLUSTERED COLUMNSTORE INDEX") == 1


def test_work_table_has_identical_columns_and_key(store_ddl):
    store = _create_table_block(store_ddl, "SFI_datapoint_store")
    work = _create_table_block(store_ddl, "SFI_datapoint_store_backfill")
    leftover = _create_table_block(store_ddl, "SFI_datapoint_store_efterslaeb")
    assert _column_lines(store) == _column_lines(work) == _column_lines(leftover)
    # holdetabellen er en heap på PRIMARY (ikke partitioneret; få rækker, aldrig SWITCH)
    tail = store_ddl.split("CREATE TABLE [fhir].[SFI_datapoint_store_efterslaeb]")[1]
    assert ") ON [PRIMARY];" in tail and "ps_SFI_datapoint_store_Maaned" not in tail
    for block in (store, work, leftover):
        assert "CLUSTERED" not in block.upper(), "heap i CREATE TABLE; columnstoren bygges bagefter"
        assert "NEXT VALUE FOR [fhir].[seq_SFI_datapoint_store]" in block
        assert "IDENTITY" not in block.upper()
    # Arbejdstabellen får hverken columnstore eller B-træer i DDL'en — de bygges lige
    # før skiftet, og den er heap under indlæsningen.
    assert (
        "ON [fhir].[SFI_datapoint_store_backfill]"
        not in store_ddl.split("CREATE TABLE [fhir].[SFI_datapoint_store_backfill]")[1]
    )
    assert store_ddl.count("ON [ps_SFI_datapoint_store_Maaned]([occurred_at_dt])") == 5


def test_ddl_has_no_fulltext_and_no_textimage(store_ddl):
    assert "FULLTEXT" not in store_ddl.upper()
    assert "TEXTIMAGE_ON" not in store_ddl.upper()


def test_migration_drops_the_unpartitioned_store_first_and_resets_watermarks():
    sql = MIGRATION.read_text(encoding="utf-8")
    assert "sys.partition_schemes" in sql, "vogtet på at storen IKKE er partitioneret"
    assert re.search(r"DROP TABLE\s+\[fhir\]\.\[SFI_datapoint_store\];", sql)
    assert "sp_rename" not in sql and "_gammel" not in sql
    assert "DROP FULLTEXT INDEX ON [fhir].[SFI_datapoint_store]" in sql
    assert "DELETE FROM [meta].[SyncWatermarks]" in sql and "N'sfi_datapoint'" in sql
    # Watermarks slettes inde i samme vogtede blok som DROP TABLE — aldrig alene.
    drop_at = sql.index("DROP TABLE [fhir].[SFI_datapoint_store];")
    delete_at = sql.index("DELETE FROM [meta].[SyncWatermarks]")
    assert drop_at < delete_at < sql.index("\nGO", drop_at)
    # Filen sorterer efter alle eksisterende migrationer.
    others = sorted(f.name for f in MIGRATION.parent.glob("*.sql") if f.name != MIGRATION.name)
    assert MIGRATION.name > others[-1]


# ---------------------------------------------------------------------------
# SQL-byggere
# ---------------------------------------------------------------------------


def test_probe_and_delete_window_share_the_source_date_expression():
    expr = p.source_date_expr()
    assert "COALESCE(qr.[authored], o.[effective_date])" in expr and expr.count("TRY_CONVERT") == 3
    assert expr in p.probe_empty_share_sql("#changed_numbered", 100_000)
    window = _norm(p.batch_date_window_sql("#batch_resources"))
    assert "DECLARE @dt_lo DATETIME2(0), @dt_hi DATETIME2(0), @dt_null_count INT;" in window
    assert "FROM #batch_resources AS br" in window and _norm(expr) in window
    assert "LEFT JOIN [fhir].[QuestionnaireResponse] AS qr" in window
    assert "LEFT JOIN [fhir].[Observation] AS o" in window


def test_scoped_delete_limits_to_the_window_and_handles_null_dates():
    sql = _norm(p.scoped_delete_sql(p.STORE_TABLE, "#batch_resources"))
    assert sql.count("DELETE tgt FROM [fhir].[SFI_datapoint_store] AS tgt") == 2
    assert "WHERE tgt.[occurred_at_dt] >= @dt_lo AND tgt.[occurred_at_dt] <= @dt_hi;" in sql
    assert "IF @dt_null_count > 0" in sql and "WHERE tgt.[occurred_at_dt] IS NULL;" in sql
    assert "DECLARE @deleted_rows BIGINT = 0;" in sql
    assert "SET @deleted_rows = @@ROWCOUNT;" in sql and "SET @deleted_rows += @@ROWCOUNT;" in sql
    # den daglige sti bruger intervallet; afstemningen sletter uden dato (opsamling)
    daily = _norm(m._build_inline_batch_apply_sql(1, 1000))
    assert "@dt_lo" in daily and "tgt.[occurred_at_dt] >= @dt_lo" in daily
    reconcile = _norm(m._build_reconciliation_batch_sql(1, 1000))
    assert "@dt_lo" not in reconcile
    assert (
        "DELETE tgt FROM [fhir].[SFI_datapoint_store] AS tgt INNER JOIN #batch_resources"
        in reconcile
    )
    assert "DECLARE @deleted_rows BIGINT = @@ROWCOUNT;" in reconcile


def test_routed_insert_sql_splits_rows_and_returns_four_counters():
    sql = p.routed_insert_sql(insert_columns="[a], [b]", batch_select_sql="SELECT 1 AS a, 2 AS b")
    n = _norm(sql)
    assert "INTO #batch_rows FROM ( SELECT 1 AS a, 2 AS b ) AS rows_" in n
    assert "INSERT INTO [fhir].[SFI_datapoint_store_backfill] ( [a], [b] )" in n
    assert "INSERT INTO [fhir].[SFI_datapoint_store] ( [a], [b] )" in n
    assert n.count("$PARTITION.pf_SFI_datapoint_store_Maaned(r.[occurred_at_dt])") == 2
    assert "WHERE EXISTS ( SELECT 1 FROM #empty_partitions" in n
    assert "WHERE NOT EXISTS ( SELECT 1 FROM #empty_partitions" in n
    assert n.endswith(
        "SELECT @deleted_rows AS deleted_rows, @work_rows + @store_rows AS inserted_rows, "
        "@work_rows AS work_rows, @store_rows AS store_rows;"
    )


def test_probe_sql_uses_stride_to_bound_sample():
    small = _norm(p.probe_empty_share_sql("#changed_numbered", 5_000))
    big = _norm(p.probe_empty_share_sql("#reconcile_resources", 17_000_000))
    assert "c.[row_id] % 1 = 0" in small
    assert "c.[row_id] % 1700 = 0" in big
    assert "[fhir].[QuestionnaireResponse]" in big and "[fhir].[Observation]" in big
    assert "TRY_CONVERT(DATETIME2(0), COALESCE(qr.[authored], o.[effective_date]), 126)" in big
    assert "FROM #reconcile_resources AS c" in big


def test_switch_fallback_and_statistics_sql():
    assert p.switch_partition_sql(13) == (
        "ALTER TABLE [fhir].[SFI_datapoint_store_backfill] SWITCH PARTITION 13 "
        "TO [fhir].[SFI_datapoint_store] PARTITION 13;"
    )
    fb = _norm(p.fallback_insert_sql(13, insert_columns="[a]"))
    assert "WHERE $PARTITION.pf_SFI_datapoint_store_Maaned([occurred_at_dt]) = 13" in fb
    assert "TRUNCATE TABLE [fhir].[SFI_datapoint_store_backfill] WITH (PARTITIONS (13))" in fb
    stats = p.update_statistics_sql([14, 13, 13])
    assert stats.count("WITH RESAMPLE ON PARTITIONS (13, 14);") == len(p.ALIGNED_INDEXES)
    for ix in p.ALIGNED_INDEXES:
        assert f"UPDATE STATISTICS [fhir].[SFI_datapoint_store] ([{ix.name}])" in stats
    assert p.columnstore_index_name() not in stats, "columnstoren kan ikke være inkrementel"
    assert p.is_lock_timeout(RuntimeError("Lock request time out period exceeded. (1222)"))
    assert not p.is_lock_timeout(RuntimeError("Cannot switch: target partition not empty"))


def test_empty_partitions_sql_reads_partition_stats_of_clustered_index():
    sql = _norm(p.empty_partitions_temp_sql())
    assert (
        "sys.dm_db_partition_stats" in sql
        and "ps.index_id IN (0, 1)" in sql
        and f"ps.row_count <= {p.NEARLY_EMPTY_MAX_ROWS}" in sql
    )
    assert "INTO #empty_partitions" in sql


# ---------------------------------------------------------------------------
# SwitchLoadScope
# ---------------------------------------------------------------------------


def test_leftover_sql_moves_by_partition_and_restores_only_unrebuilt_resources():
    move = _norm(p.move_leftovers_sql(14, insert_columns="[a], [b]"))
    assert (
        "INSERT INTO [fhir].[SFI_datapoint_store_efterslaeb] ( [sfi_datapoint_id], [a], [b] ) "
        "SELECT [sfi_datapoint_id], [a], [b] FROM [fhir].[SFI_datapoint_store] "
        "WHERE $PARTITION.pf_SFI_datapoint_store_Maaned([occurred_at_dt]) = 14;" in move
    )
    assert "TRUNCATE TABLE [fhir].[SFI_datapoint_store] WITH (PARTITIONS (14));" in move
    restore = _norm(
        p.restore_leftovers_sql(insert_columns="[a], [b]", numbered_table="#changed_numbered")
    )
    assert "INSERT INTO [fhir].[SFI_datapoint_store] ( [sfi_datapoint_id], [a], [b] )" in restore
    assert (
        "FROM [fhir].[SFI_datapoint_store_efterslaeb] AS l WHERE NOT EXISTS ( SELECT 1 FROM #changed_numbered AS c"
        in restore
    )
    assert "SELECT @@ROWCOUNT AS restored_rows;" in restore
    assert "TRUNCATE TABLE [fhir].[SFI_datapoint_store_efterslaeb];" in restore
    everything = _norm(p.restore_leftovers_sql(insert_columns="[a]", numbered_table=None))
    assert (
        "NOT EXISTS" not in everything
        and "TRUNCATE TABLE [fhir].[SFI_datapoint_store_efterslaeb];" in everything
    )
    assert (
        "ps.row_count > 0" in p.leftover_partitions_sql()
        and "#empty_partitions" in p.leftover_partitions_sql()
    )


_WORK_INDEXES = [(ix.name, 2, 0) for ix in p.STORE_INDEXES] + [
    ("CCI_FHIR_SFI_datapoint_store_backfill", 5, 0)
]


def _scope_responses(populated, work_indexes=None, leftovers=None):
    return [
        (
            "sys.dm_db_partition_stats AS ps WHERE ps.object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store_backfill]') AND ps.index_id IN (0, 1) AND ps.row_count > 0",
            populated,
        ),
        ("INTO #empty_partitions", [(5,)]),
        (
            "INNER JOIN #empty_partitions AS e ON e.[partition_number] = ps.partition_number",
            leftovers or [],
        ),
        ("SELECT @@ROWCOUNT AS restored_rows", [(3,)]),
        (
            "FROM sys.indexes i WHERE i.object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store_backfill]')",
            work_indexes or [],
        ),
    ]


def test_enter_prepares_work_table_and_materialises_empty_partitions():
    cur, conn, log = FakeCursor(_scope_responses([])), FakeConn(), FakeLog()
    scope = p.SwitchLoadScope(cur, conn, log, insert_columns="[a]").__enter__()
    assert scope.report.empty_partitions == 5
    assert cur.executed("TRUNCATE TABLE [fhir].[SFI_datapoint_store_backfill]")
    assert cur.executed("INTO #empty_partitions")
    assert conn.commits >= 2


def test_finalize_builds_columnstore_and_btrees_then_switches():
    cur = FakeCursor(_scope_responses([(13, 1000), (14, 2000)], _WORK_INDEXES))
    conn, log = FakeConn(), FakeLog()
    with p.SwitchLoadScope(cur, conn, log, insert_columns="[a]") as scope:
        scope.note_batch(3000, 7)
    r = scope.report
    assert r.finalized and r.switched_partitions == [13, 14] and r.fallback_partitions == []
    assert r.work_rows == 3000 and r.store_rows == 7
    creates = [s for s in cur.sql if s.startswith("CREATE ")]
    # columnstoren først, så det unikke B-træ, så patientindekset — alle på arbejdstabellen
    assert [c.split("[")[1].split("]")[0] for c in creates] == [
        "CCI_FHIR_SFI_datapoint_store_backfill",
        "UX_FHIR_SFI_datapoint_store_DatapointKey",
        "IX_FHIR_SFI_datapoint_store_PatientOccurredDt",
    ]
    assert all("[fhir].[SFI_datapoint_store_backfill]" in c for c in creates)
    assert set(r.index_build_ms) == {
        "CCI_FHIR_SFI_datapoint_store_backfill",
        "UX_FHIR_SFI_datapoint_store_DatapointKey",
        "IX_FHIR_SFI_datapoint_store_PatientOccurredDt",
    }
    assert not cur.executed("DISABLE") and not cur.executed(" REBUILD"), "alt er alignet"
    switches = cur.executed("SWITCH PARTITION")
    assert switches == [
        _norm(p.switch_partition_sql(13)),
        _norm(p.switch_partition_sql(14)),
    ]
    assert cur.executed("WITH RESAMPLE ON PARTITIONS (13, 14)")
    idx = {
        k: next(i for i, s in enumerate(cur.sql) if k in s)
        for k in ("CREATE CLUSTERED COLUMNSTORE", "SWITCH PARTITION 14", "UPDATE STATISTICS")
    }
    assert (
        idx["CREATE CLUSTERED COLUMNSTORE"] < idx["SWITCH PARTITION 14"] < idx["UPDATE STATISTICS"]
    )
    # oprydning: arbejdstabellen tømmes, og alle dens indeks (B-træer og columnstore) droppes
    assert len(cur.executed("TRUNCATE TABLE [fhir].[SFI_datapoint_store_backfill]")) == 2
    assert len(cur.executed("DROP INDEX")) == 2 * len(_WORK_INDEXES)
    assert cur.executed("SET LOCK_TIMEOUT 120000") and cur.executed("SET LOCK_TIMEOUT -1")
    assert r.as_metadata()["switch_partitions"] == 2
    assert "switch_non_aligned_rebuild_ms" not in r.as_metadata()


def test_nearly_empty_partition_is_emptied_before_switch_and_leftovers_restored_after():
    cur = FakeCursor(_scope_responses([(14, 7_000_000)], _WORK_INDEXES, leftovers=[(14, 204)]))
    conn, log = FakeConn(), FakeLog()
    with p.SwitchLoadScope(
        cur, conn, log, insert_columns="[a]", numbered_table="#changed_numbered"
    ) as scope:
        # efterslæberne er flyttet FØR løkken
        assert scope.report.leftover_partitions == [14] and scope.report.leftover_rows == 204
        assert cur.executed("TRUNCATE TABLE [fhir].[SFI_datapoint_store] WITH (PARTITIONS (14))")
        assert cur.executed("INSERT INTO [fhir].[SFI_datapoint_store_efterslaeb]")
    r = scope.report
    assert r.switched_partitions == [14] and r.restored_rows == 3
    restore = cur.executed("FROM [fhir].[SFI_datapoint_store_efterslaeb] AS l WHERE NOT EXISTS")
    assert len(restore) == 1 and "#changed_numbered" in restore[0]
    idx = {
        k: next(i for i, s in enumerate(cur.sql) if k in s)
        for k in ("SWITCH PARTITION 14", "AS l WHERE NOT EXISTS", "UPDATE STATISTICS")
    }
    assert idx["SWITCH PARTITION 14"] < idx["AS l WHERE NOT EXISTS"] < idx["UPDATE STATISTICS"]
    assert any("efterslæbere" in m for m in log.infos)
    meta = r.as_metadata()
    assert meta["switch_leftover_partitions"] == 1 and meta["switch_leftover_rows"] == 204
    assert meta["switch_restored_rows"] == 3


def test_loop_failure_restores_all_leftovers():
    cur = FakeCursor(_scope_responses([(14, 10)], leftovers=[(14, 204)]))
    conn, log = FakeConn(), FakeLog()
    with pytest.raises(RuntimeError, match="portion fejlede"):
        with p.SwitchLoadScope(cur, conn, log, insert_columns="[a]", numbered_table="#x"):
            raise RuntimeError("portion fejlede")
    restore = cur.executed("FROM [fhir].[SFI_datapoint_store_efterslaeb] AS l")
    assert len(restore) == 1 and "NOT EXISTS" not in restore[0], "alt lægges tilbage ved fejl"
    assert not cur.executed("SWITCH PARTITION")


def test_repair_restores_leftovers_from_a_killed_run():
    cur = FakeCursor(
        [
            (
                "OBJECT_ID(N'[fhir].[SFI_datapoint_store_efterslaeb]') AND ps.index_id IN (0, 1) AND ps.row_count > 0",
                [(14, 204)],
            ),
            ("SELECT @@ROWCOUNT AS restored_rows", [(204,)]),
        ]
    )
    conn, log = FakeConn(), FakeLog()
    assert p.restore_all_leftovers(cur, conn, log, insert_columns="[a]") == 204
    assert cur.executed("INSERT INTO [fhir].[SFI_datapoint_store] ( [sfi_datapoint_id], [a] )")
    assert conn.commits == 1 and any("afbrudt kørsel" in w for w in log.warnings)


def test_switch_failure_falls_back_to_insert_for_that_partition_only():
    cur = FakeCursor(
        _scope_responses([(13, 1000), (14, 2000)]),
        fail={"SWITCH PARTITION 14": (-1, "target partition is not empty")},
    )
    conn, log = FakeConn(), FakeLog()
    with p.SwitchLoadScope(cur, conn, log, insert_columns="[a]") as scope:
        pass
    r = scope.report
    assert r.switched_partitions == [13] and r.fallback_partitions == [14]
    fallback = cur.executed("$PARTITION.pf_SFI_datapoint_store_Maaned([occurred_at_dt]) = 14")
    assert len(fallback) == 1 and "INSERT INTO [fhir].[SFI_datapoint_store]" in fallback[0]
    assert (
        "TRUNCATE TABLE [fhir].[SFI_datapoint_store_backfill] WITH (PARTITIONS (14))" in fallback[0]
    )
    assert conn.rollbacks == 1
    assert any("kunne ikke gennemføres" in w for w in log.warnings)
    assert cur.executed("WITH RESAMPLE ON PARTITIONS (13)")


def test_lock_timeout_is_retried_then_succeeds():
    cur = FakeCursor(
        _scope_responses([(13, 10)]),
        fail={"SWITCH PARTITION 13": (2, "Lock request time out period exceeded. 1222")},
    )
    conn, log = FakeConn(), FakeLog()
    with p.SwitchLoadScope(cur, conn, log, insert_columns="[a]") as scope:
        pass
    assert scope.report.switched_partitions == [13] and scope.report.fallback_partitions == []
    assert len(cur.executed("SWITCH PARTITION 13")) == 3
    assert sum("ventede forgæves" in w for w in log.warnings) == 2


def test_loop_failure_leaves_work_rows_and_switches_nothing():
    cur = FakeCursor(_scope_responses([(13, 10)]))
    conn, log = FakeConn(), FakeLog()
    with pytest.raises(RuntimeError, match="portion fejlede"):
        with p.SwitchLoadScope(cur, conn, log, insert_columns="[a]") as scope:
            scope.note_batch(10, 0)
            raise RuntimeError("portion fejlede")
    assert not cur.executed("SWITCH PARTITION")
    assert not scope.report.finalized and conn.rollbacks == 1
    assert any("skiftes IKKE ind" in w for w in log.warnings)


def test_statistics_failure_is_not_fatal():
    cur = FakeCursor(
        _scope_responses([(13, 10)]),
        fail={
            "WITH RESAMPLE ON PARTITIONS": (
                -1,
                "incremental stats unavailable",
            )
        },
    )
    conn, log = FakeConn(), FakeLog()
    with p.SwitchLoadScope(cur, conn, log, insert_columns="[a]") as scope:
        pass
    assert scope.report.finalized
    assert (
        cur.executed("UPDATE STATISTICS [fhir].[SFI_datapoint_store]")[-1]
        == "UPDATE STATISTICS [fhir].[SFI_datapoint_store]"
    )


# ---------------------------------------------------------------------------
# Refresh-integration: strategi, reparation, løkke
# ---------------------------------------------------------------------------


def _clear_env(monkeypatch):
    for var in (
        m._BACKFILL_INDEX_MODE_ENV,
        m._BACKFILL_MIN_RESOURCES_ENV,
        m._BACKFILL_STRATEGY_ENV,
    ):
        monkeypatch.delenv(var, raising=False)


def _strategy_cursor(*, partitioned: bool, work_exists: bool = True, probe=(10_000, 9_500)):
    return FakeCursor(
        [
            ("sys.partition_schemes ps ON ps.data_space_id", [(1 if partitioned else 0,)]),
            (
                "OBJECT_ID(N'[fhir].[SFI_datapoint_store_backfill]', N'U') IS NULL",
                [(1 if work_exists else 0,)],
            ),
            ("OBJECT_ID(N'[fhir].[SFI_datapoint_store_efterslaeb]', N'U') IS NULL", [(1,)]),
            ("INTO #empty_partitions", [(9,)]),
            ("COUNT_BIG(*) AS sampled", [probe]),
        ]
    )


def test_small_delta_gets_no_backfill_handling(monkeypatch):
    _clear_env(monkeypatch)
    ctx, _ = _ctx()
    cur = _strategy_cursor(partitioned=True)
    strategy, probe = m._resolve_backfill_strategy(
        cur, ctx, total_resources=200_000, numbered_table="#changed_numbered"
    )
    assert strategy == "none" and cur.sql == []


def test_unpartitioned_store_gets_index_disable_even_when_switch_requested(monkeypatch):
    _clear_env(monkeypatch)
    ctx, log = _ctx(tags={m._BACKFILL_STRATEGY_TAG: "switch"})
    cur = _strategy_cursor(partitioned=False)
    strategy, _ = m._resolve_backfill_strategy(
        cur, ctx, total_resources=5_000_000, numbered_table="#changed_numbered"
    )
    assert strategy == "disable_indexes"
    assert any("ikke partitioneret" in w for w in log.warnings)


def test_partitioned_store_with_fresh_window_gets_switch(monkeypatch):
    _clear_env(monkeypatch)
    ctx, _ = _ctx()
    cur = _strategy_cursor(partitioned=True, probe=(10_000, 9_800))
    strategy, probe = m._resolve_backfill_strategy(
        cur, ctx, total_resources=5_000_000, numbered_table="#changed_numbered"
    )
    assert strategy == "switch"
    assert probe["empty_partitions"] == 9 and probe["probe_share"] == 0.98
    assert cur.executed("FROM #changed_numbered AS c")


def test_replay_of_loaded_window_gets_index_disable(monkeypatch):
    _clear_env(monkeypatch)
    ctx, _ = _ctx()
    cur = _strategy_cursor(partitioned=True, probe=(10_000, 120))
    strategy, probe = m._resolve_backfill_strategy(
        cur, ctx, total_resources=5_000_000, numbered_table="#reconcile_resources"
    )
    assert strategy == "disable_indexes" and probe["probe_share"] == 0.012


def test_requested_strategies_are_honoured(monkeypatch):
    _clear_env(monkeypatch)
    ctx, _ = _ctx(tags={m._BACKFILL_STRATEGY_TAG: "none"})
    strategy, _ = m._resolve_backfill_strategy(
        _strategy_cursor(partitioned=True), ctx, total_resources=10**8, numbered_table="#x"
    )
    assert strategy == "none"
    ctx, _ = _ctx(tags={m._BACKFILL_STRATEGY_TAG: "disable_indexes"})
    cur = _strategy_cursor(partitioned=True)
    strategy, _ = m._resolve_backfill_strategy(cur, ctx, total_resources=10**8, numbered_table="#x")
    assert strategy == "disable_indexes" and not cur.executed("INTO #empty_partitions")
    ctx, _ = _ctx(tags={m._BACKFILL_STRATEGY_TAG: "switch"})
    cur = _strategy_cursor(partitioned=True, probe=(10_000, 0))
    strategy, _ = m._resolve_backfill_strategy(cur, ctx, total_resources=10**8, numbered_table="#x")
    assert strategy == "switch" and not cur.executed(
        "COUNT_BIG(*) AS sampled"
    ), "forlangt switch springer stikprøven over"
    monkeypatch.setenv(m._BACKFILL_STRATEGY_ENV, "vrøvl")
    assert m._backfill_strategy() == "auto"
    ctx, _ = _ctx(tags={m._BACKFILL_INDEX_MODE_TAG: "never"})
    strategy, _ = m._resolve_backfill_strategy(
        _strategy_cursor(partitioned=True), ctx, total_resources=10**8, numbered_table="#x"
    )
    assert strategy == "none", "backfill_index_mode=never lukker porten for alle strategier"


def test_missing_work_table_falls_back_to_index_disable(monkeypatch):
    _clear_env(monkeypatch)
    ctx, log = _ctx()
    cur = _strategy_cursor(partitioned=True, work_exists=False)
    strategy, _ = m._resolve_backfill_strategy(
        cur, ctx, total_resources=5_000_000, numbered_table="#x"
    )
    assert strategy == "disable_indexes" and any("mangler" in w for w in log.warnings)


def test_required_disabled_indexes_are_rebuilt_before_the_loop():
    indeks = [
        ("IX_FHIR_SFI_datapoint_store_PatientOccurredDt", 1),
        ("UX_FHIR_SFI_datapoint_store_DatapointKey", 1),
    ]
    cur = FakeCursor([("FROM sys.indexes i WHERE i.object_id", indeks)])
    assert m._list_disabled_required_indexes(cur) == [
        "UX_FHIR_SFI_datapoint_store_DatapointKey",
    ]
    ctx, log = _ctx()
    conn = FakeConn()
    with m._BackfillIndexScope(cur, conn, ctx, changed_resources=5_000_000, active=True) as scope:
        rebuilt_first = [s for s in cur.sql if "REBUILD" in s]
        assert len(rebuilt_first) == 1 and "DatapointKey" in rebuilt_first[0]
        # PatientOccurredDt var allerede slået fra og bliver ikke slået fra igen
        assert not cur.executed("PatientOccurredDt] ON [fhir].[SFI_datapoint_store] DISABLE")
        assert scope.active is True
    assert any("påkrævede indeks" in w for w in log.warnings)


def test_repair_leftovers_truncates_work_rows():
    cur = FakeCursor(
        [
            ("OBJECT_ID(N'[fhir].[SFI_datapoint_store_backfill]', N'U') IS NULL", [(1,)]),
            ("OBJECT_ID(N'[fhir].[SFI_datapoint_store_efterslaeb]', N'U') IS NULL", [(1,)]),
            (
                "OBJECT_ID(N'[fhir].[SFI_datapoint_store_efterslaeb]') AND ps.index_id IN (0, 1) AND ps.row_count > 0",
                [],
            ),
            (
                "OBJECT_ID(N'[fhir].[SFI_datapoint_store_backfill]') AND ps.index_id IN (0, 1) AND ps.row_count > 0",
                [(13, 4200)],
            ),
            (
                "FROM sys.indexes i WHERE i.object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store_backfill]')",
                [("UX_FHIR_SFI_datapoint_store_DatapointKey", 2, 0)],
            ),
        ]
    )
    ctx, log = _ctx()
    m._repair_leftovers(cur, FakeConn(), ctx)
    assert cur.executed("TRUNCATE TABLE [fhir].[SFI_datapoint_store_backfill]")
    assert cur.executed(
        "DROP INDEX [UX_FHIR_SFI_datapoint_store_DatapointKey] ON [fhir].[SFI_datapoint_store_backfill]"
    )
    assert any("4200 rækker fra en afbrudt kørsel" in w for w in log.warnings)


def test_apply_loop_in_switch_mode_accumulates_four_counters_and_finalizes(monkeypatch):
    _clear_env(monkeypatch)
    responses = [
        ("BATCH_MARKER", [(5, 100, 95, 5)]),
        ("sys.partition_schemes ps ON ps.data_space_id", [(1,)]),
        ("OBJECT_ID(N'[fhir].[SFI_datapoint_store_backfill]', N'U') IS NULL", [(1,)]),
        ("OBJECT_ID(N'[fhir].[SFI_datapoint_store_efterslaeb]', N'U') IS NULL", [(1,)]),
        ("COUNT_BIG(*) AS sampled", [(10_000, 9_900)]),
    ] + _scope_responses([(13, 190)])
    cur, conn = FakeCursor(responses), FakeConn()
    ctx, _ = _ctx()

    def build(start, end, *, routed):
        assert routed is True
        return f"BATCH_MARKER {start} {end}"

    totals, meta = m._apply_with_backfill_handling(
        cur,
        conn,
        ctx,
        total_resources=2_000_000,
        batch_size=1_000_000,
        build_batch_sql=build,
        numbered_table="#changed_numbered",
    )
    assert (totals.batches, totals.deleted, totals.inserted) == (2, 10, 200)
    assert (totals.work_rows, totals.store_rows) == (190, 10)
    assert meta["backfill_strategy"] == "switch" and meta["switch_partitions"] == 1
    assert meta["backfill_work_rows"] == 190 and meta["backfill_index_mode"] is False
    assert cur.executed("SWITCH PARTITION 13")


def test_apply_loop_in_plain_mode_uses_two_counters(monkeypatch):
    _clear_env(monkeypatch)
    responses = [
        ("BATCH_MARKER", [(1, 20)]),
        ("OBJECT_ID(N'[fhir].[SFI_datapoint_store_backfill]', N'U') IS NULL", [(0,)]),
        ("FROM sys.indexes i WHERE i.object_id", []),
    ]
    cur, conn = FakeCursor(responses), FakeConn()
    ctx, _ = _ctx()

    def build(start, end, *, routed):
        assert routed is False
        return f"BATCH_MARKER {start} {end}"

    totals, meta = m._apply_with_backfill_handling(
        cur,
        conn,
        ctx,
        total_resources=150,
        batch_size=100,
        build_batch_sql=build,
        numbered_table="#changed_numbered",
    )
    assert (totals.batches, totals.deleted, totals.inserted, totals.work_rows) == (2, 2, 40, 0)
    assert meta["backfill_strategy"] == "none" and meta["backfill_index_mode"] is False
    assert not cur.executed("SWITCH") and not cur.executed("DISABLE")
    assert conn.commits == 2, "ét commit pr. portion"


def test_backfill_metadata_values_are_typed():
    values = m._backfill_metadata_values({"a": True, "b": 3, "c": 0.5, "d": "x"})
    assert {k: type(v).__name__ for k, v in values.items()} == {
        "a": "BoolMetadataValue",
        "b": "IntMetadataValue",
        "c": "FloatMetadataValue",
        "d": "TextMetadataValue",
    }


def test_switch_mode_rebuilds_disabled_store_indexes_before_the_loop(monkeypatch):
    """SWITCH kræver identiske, aktive indeks på begge sider: et bredt indeks efterladt
    slået fra af en dræbt frakoblingskørsel genopbygges før løkken, ikke efter."""
    _clear_env(monkeypatch)
    responses = [
        ("BATCH_MARKER", [(0, 10, 10, 0)]),
        ("sys.partition_schemes ps ON ps.data_space_id", [(1,)]),
        ("OBJECT_ID(N'[fhir].[SFI_datapoint_store_backfill]', N'U') IS NULL", [(1,)]),
        ("OBJECT_ID(N'[fhir].[SFI_datapoint_store_efterslaeb]', N'U') IS NULL", [(1,)]),
        ("COUNT_BIG(*) AS sampled", [(10_000, 10_000)]),
        ("AND i.type = 5", [(1,)]),
        (
            "FROM sys.indexes i WHERE i.object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')",
            [
                ("IX_FHIR_SFI_datapoint_store_PatientOccurredDt", 1),
                ("UX_FHIR_SFI_datapoint_store_DatapointKey", 0),
            ],
        ),
    ] + _scope_responses([(13, 10)])
    cur, conn = FakeCursor(responses), FakeConn()
    ctx, log = _ctx()

    def build(start, end, *, routed):
        return f"BATCH_MARKER {start} {end}"

    totals, meta = m._apply_with_backfill_handling(
        cur,
        conn,
        ctx,
        total_resources=1_000_000,
        batch_size=1_000_000,
        build_batch_sql=build,
        numbered_table="#x",
    )
    rebuilds = cur.executed("REBUILD")
    assert (
        rebuilds[0]
        == "ALTER INDEX [IX_FHIR_SFI_datapoint_store_PatientOccurredDt] ON [fhir].[SFI_datapoint_store] REBUILD"
    )
    marker = next(i for i, s in enumerate(cur.sql) if "BATCH_MARKER" in s)
    assert cur.sql.index(rebuilds[0]) < marker, "genopbygningen sker før portionsløkken"
    assert meta["backfill_strategy"] == "switch" and "backfill_index_rebuild_before_ms" in meta
    assert any("før SWITCH-tilstand" in w for w in log.warnings)
    # kørslen slutter med REORGANIZE af columnstoren, efter skiftet
    reorganize = cur.executed("REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON)")
    assert reorganize == [_norm(p.reorganize_sql())]
    assert cur.sql.index(reorganize[0]) > cur.sql.index(_norm(p.switch_partition_sql(13)))
    assert "columnstore_reorganize_ms" in meta

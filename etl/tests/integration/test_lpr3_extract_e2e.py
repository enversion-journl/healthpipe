# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""All 47 LPR3 extracts against local MSSQL, including failed/concurrent runs.

The fixture explicitly requires the disposable devcontainer SQL host. Real SDS
validation is a separate read-only probe; these rows are entirely synthetic.
"""

import dagster as dg
import pytest
from DSI.csv_files import lpr3
from DSI.csv_files._lpr3_run import claim_run, owned_landing
from DSI.csv_files._sds_tar import execute_statements
from DSI.csv_files.lpr3_datasets import SOURCE_DATASETS, SOURCES_BY_PREFIX
from tests.fixtures.lpr3 import build_delivery, context, extract, prepare, source_row

pytestmark = pytest.mark.integration


def scalar(resource, sql, params=()):
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        result = cursor.fetchone()[0]
        cursor.close()
        return result


def test_all_47_and_next_snapshot_replace_ext_preserving_exact_values(lpr3_db, tmp_path):
    address = source_row(
        SOURCES_BY_PREFIX["DimAdresse"], Type="", Dør="", Vejnavn='Test; "æøå"\nlinje'
    )
    extract(lpr3_db, build_delivery(tmp_path, rows={"DimAdresse": [address]}), "snapshot-a")
    assert scalar(lpr3_db, f"SELECT COUNT(*) FROM ext.{lpr3.MANIFEST_TABLE}") == 47
    with lpr3_db.get_connection() as conn:
        cursor = conn.cursor()
        for dataset in SOURCE_DATASETS:
            cursor.execute(
                "SELECT data_compression_desc FROM sys.partitions "
                "WHERE object_id=OBJECT_ID(?) AND index_id=0",
                (f"ext.{dataset.ext_table}",),
            )
            assert cursor.fetchone()[0] == "PAGE"
            cursor.execute(f"SELECT * FROM [ext].[{dataset.ext_table}]")
            assert [column[0] for column in cursor.description][:3] == [
                "kadence",
                "leverance_tidsstempel",
                "leverance_dato",
            ]
            row = cursor.fetchone()
            expected = address if dataset.source_prefix == "DimAdresse" else source_row(dataset)
            assert tuple(row[3 : 3 + len(dataset.columns)]) == tuple(expected)
            assert cursor.fetchone() is None
        cursor.close()
    # A missing input removes its stale ext table and its manifest entry. Two
    # new contacts replace the old snapshot; values are not appended to it.
    contacts = SOURCES_BY_PREFIX["DimKontakt"]
    path = build_delivery(
        tmp_path,
        stamp="20260921190000",
        cutoff="2026-09-20",
        cadence="U",
        omit=("DimAdresse",),
        rows={"DimKontakt": [source_row(contacts, 2), source_row(contacts, 3)]},
    )
    extract(lpr3_db, path, "snapshot-b")
    assert scalar(lpr3_db, f"SELECT COUNT(*) FROM ext.{lpr3.MANIFEST_TABLE}") == 46
    assert scalar(lpr3_db, "SELECT OBJECT_ID(N'ext.lpr3_dimadresse',N'U')") is None
    assert scalar(lpr3_db, "SELECT COUNT(*) FROM ext.lpr3_dimkontakt") == 2
    assert scalar(lpr3_db, "SELECT COUNT(*) FROM ext.lpr3_dimkontakt WHERE DW_SK_Kontakt='1'") == 0


def test_count_failure_never_publishes_manifest(lpr3_db, tmp_path):
    path = build_delivery(tmp_path, row_count_delta={"DimKontakt": 1})
    plan, ctx = prepare(lpr3_db, path, "failed-extract")
    with lpr3_db.get_connection() as conn:
        with owned_landing(conn, ctx.run_id):
            with pytest.raises(ValueError, match="row counts differ"):
                lpr3.load_group(conn, ctx, plan, ("DimKontakt",))
            conn.rollback()
    assert scalar(lpr3_db, f"SELECT COUNT(*) FROM ext.{lpr3.MANIFEST_TABLE}") == 0


def test_active_unknown_and_orphan_runs_cannot_be_displaced(lpr3_db):
    with lpr3_db.get_connection() as conn:
        with claim_run(conn, context("owner")):
            pass
    # Between worker steps, persisted ownership protects an unfinished run.
    with lpr3_db.get_connection() as conn:
        with pytest.raises(ValueError, match="unfinished or unknown"):
            with claim_run(conn, context("intruder", previous_finished=False)):
                pytest.fail("claimed active landing")
        conn.rollback()
    unknown = context("intruder")
    unknown.instance.get_run_by_id.return_value = None
    with lpr3_db.get_connection() as conn:
        with pytest.raises(ValueError, match="unfinished or unknown"):
            with claim_run(conn, unknown):
                pytest.fail("claimed unknown landing")
        conn.rollback()
    # Two workers share access; even a terminal owner's orphaned process keeps
    # the next run from overwriting its tables until its SQL session releases.
    with lpr3_db.get_connection() as worker1, lpr3_db.get_connection() as worker2:
        with owned_landing(worker1, "owner"), owned_landing(worker2, "owner"):
            with lpr3_db.get_connection() as contender:
                with pytest.raises(ValueError, match="in use"):
                    with claim_run(contender, context("replacement")):
                        pytest.fail("displaced running SQL workers")
        worker1.commit()
        worker2.commit()
    with lpr3_db.get_connection() as conn:
        with claim_run(conn, context("replacement")):
            pass
        with pytest.raises(ValueError, match="different Dagster run"):
            with owned_landing(conn, "owner"):
                pytest.fail("accepted stale plan")


def test_dagster_chain_and_checkpoint_noop_leave_ext_untouched(lpr3_db, tmp_path, monkeypatch):
    build_delivery(tmp_path)
    checkpoint = {"value": None}
    monkeypatch.setattr(
        lpr3.DfmManager, "get_checkpoint", lambda *args, **kwargs: checkpoint["value"]
    )
    assets = [a for a in lpr3.lpr3_extract_assets if a.key != dg.AssetKey("lpr3_leverance_alder")]
    config = {"ops": {"lpr3_leverance_plan": {"config": {"base_dir": str(tmp_path)}}}}
    with dg.DagsterInstance.ephemeral() as instance:
        first = dg.materialize(
            assets,
            instance=instance,
            resources={"mssql_target_staging": lpr3_db},
            run_config=config,
        )
        assert first.success
        assert scalar(lpr3_db, f"SELECT COUNT(*) FROM ext.{lpr3.MANIFEST_TABLE}") == 47
        # An observable stamp proves the no-op did not recreate/reinsert ext.
        with lpr3_db.get_connection() as conn:
            execute_statements(
                conn, "UPDATE ext.lpr3_dimkontakt SET dw_timestamp_insert='2020-01-01'"
            )
        checkpoint["value"] = "2026-09-15T19:56:03"
        second = dg.materialize(
            assets,
            instance=instance,
            resources={"mssql_target_staging": lpr3_db},
            run_config=config,
        )
        assert second.success
        assert scalar(lpr3_db, f"SELECT COUNT(*) FROM ext.{lpr3.MANIFEST_TABLE}") == 0
        assert scalar(lpr3_db, "SELECT YEAR(dw_timestamp_insert) FROM ext.lpr3_dimkontakt") == 2020


def test_checkpoint_advancing_during_preflight_prevents_stale_load(lpr3_db, tmp_path, monkeypatch):
    build_delivery(tmp_path)
    checkpoints = iter((None, "2026-09-15T19:56:03"))
    monkeypatch.setattr(
        lpr3.DfmManager, "get_checkpoint", lambda *args, **kwargs: next(checkpoints)
    )
    assets = [a for a in lpr3.lpr3_extract_assets if a.key != dg.AssetKey("lpr3_leverance_alder")]
    with dg.DagsterInstance.ephemeral() as instance:
        result = dg.materialize(
            assets,
            instance=instance,
            resources={"mssql_target_staging": lpr3_db},
            run_config={"ops": {"lpr3_leverance_plan": {"config": {"base_dir": str(tmp_path)}}}},
        )
    assert result.success
    assert scalar(lpr3_db, f"SELECT COUNT(*) FROM ext.{lpr3.MANIFEST_TABLE}") == 0
    assert scalar(lpr3_db, "SELECT OBJECT_ID(N'ext.lpr3_dimkontakt',N'U')") is None


def test_meta_ddl_is_idempotent_and_only_archived_cutoff_is_used(lpr3_db):
    from infrastructure.utils.ddl_loader import execute_sql_file

    with lpr3_db.get_connection() as conn:
        execute_statements(conn, "DROP TABLE IF EXISTS meta.LPR3Leverance")
    try:
        for _ in range(2):
            execute_sql_file(lpr3_db, "tables/lpr3/01_meta.sql")
        with lpr3_db.get_connection() as conn:
            assert lpr3.read_last_cutoff(conn) is None
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO meta.LPR3Leverance (kildetabel,leverance_tidsstempel,run_id,kadence,leverance_dato,arc_tabel,indlaesningstype,status,raekker_ext,raekker_kilde,bytes_kilde,audit_insert_distinct,genindlaesning_alarm) VALUES ('DimKontakt','2026-09-15','test','M','2026-09-10','lpr3_kontakt','full','extracted',32564135,32564135,7460000000,370,0)"
            )
            conn.commit()
            assert lpr3.read_last_cutoff(conn) is None
            cursor.execute("UPDATE meta.LPR3Leverance SET status='archived'")
            conn.commit()
            assert str(lpr3.read_last_cutoff(conn)) == "2026-09-10"
            cursor.close()
    finally:
        with lpr3_db.get_connection() as conn:
            execute_statements(conn, "DROP TABLE IF EXISTS meta.LPR3Leverance")

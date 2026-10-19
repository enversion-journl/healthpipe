# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The register's producer: who is rebuilt, and where the checkpoint lands."""

from types import SimpleNamespace

import dagster as dg
import pytest
from core.dfm_config import RunModeConfig
from DSA.FHIR.Patient import Patient_CPR_dfm as modul
from DSA.FHIR.Patient.patient_cpr_contribution import REGISTER_TABLES
from DSA.FHIR.Patient.patient_service import RegisterWriteResult

pytestmark = pytest.mark.unit


def test_full_load_is_the_registers_live_persons():
    q = modul._changed_persons_query(None, RunModeConfig(mode="full_load"))
    assert "[arc].[cpr_001_person]" in q and "[dw_is_deleted] = 0" in q
    assert "UNION" not in q


def test_delta_is_the_union_of_all_tables_including_022_on_pnrgaeld():
    q = modul._changed_persons_query("2026-09-16 22:00:00", RunModeConfig(mode="delta"))
    for table, _ in REGISTER_TABLES.values():
        schema, name = table.split(".")
        assert f"[{schema}].[{name}]" in q, table
    assert "[arc].[cpr_022_historisk_personnummer]" in q
    assert "SELECT DISTINCT [pnrgaeld] AS [pnr]" in q
    assert q.count(" UNION ") == len(REGISTER_TABLES)  # one per register table, and 022 last
    assert "[dw_is_deleted] = 0" not in q  # deleted rows are changes too
    assert "> '2026-09-16 22:00:00'" in q


def test_period_mode_limits_on_dates():
    q = modul._changed_persons_query(
        None, RunModeConfig(mode="period", date_from="2026-09-01", date_to="2026-09-02")
    )
    assert ">= '2026-09-01'" in q and "DATEADD(day, 1, CAST('2026-09-02' AS DATE))" in q


class _Cursor:
    def __init__(self, numbers):
        self._numbers = list(numbers)
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append(sql)

    def fetchone(self):
        return ("2026-09-16 21:59:59",)

    def fetchmany(self, n):
        chunk, self._numbers = self._numbers[:n], self._numbers[n:]
        return [(x,) for x in chunk]

    def close(self):
        pass


class _Conn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class _Mssql:
    def __init__(self, numbers):
        self.cursor = _Cursor(numbers)

    def get_connection(self):
        return _Conn(self.cursor)


class _Dfm:
    checkpoints = []

    def __init__(self, *_, **__):
        pass

    def start_dfm_run(self, *_, **__):
        return SimpleNamespace(
            log_id=1, checkpoint="2026-09-15 22:00:00", started_at="2026-09-16 21:59:59"
        )

    def safe_reader_checkpoint(self, started_at):
        return started_at

    def set_checkpoint(self, schema, table, *, value, mode):
        _Dfm.checkpoints.append((schema, table, value, mode))

    def complete_dfm_run(self, *_, **__):
        pass

    def fail_dfm_run(self, *_, **__):
        pass


def _run(monkeypatch, numbers, **config):
    _Dfm.checkpoints = []
    calls = []

    def _write(context, mssql, fhir_database, cprs, *, scope, source_name):
        calls.append((list(cprs), scope, source_name))
        return RegisterWriteResult(written=len(cprs), rebuilt=len(cprs))

    monkeypatch.setattr(modul, "DfmManager", _Dfm)
    monkeypatch.setattr(modul, "write_register_persons", _write)
    monkeypatch.setattr(modul, "BATCH_SIZE", 2)
    result = modul.fhir_server_patient_cpr_dfm(
        context=dg.build_asset_context(),
        mssql_target_staging=_Mssql(numbers),
        fhir_database=object(),
        config=modul.PatientRegisterConfig(**config),
    )
    return result, calls


def test_numbers_go_in_batches_through_the_write_point_and_checkpoint_is_start_time(monkeypatch):
    result, calls = _run(monkeypatch, ["1", "2", "3"], mode="delta")
    assert [c[0] for c in calls] == [["1", "2"], ["3"]]
    assert all(c[1] == "known" and c[2] == "cpr_register_dfm" for c in calls)
    assert result.metadata["written"] == 3 and result.metadata["batches"] == 2
    assert _Dfm.checkpoints == [("fhir_server", "Patient_CPR", "2026-09-16 21:59:59", "delta")]


def test_advance_checkpoint_false_moves_nothing(monkeypatch):
    _run(monkeypatch, ["1"], mode="full_load", scope="register", advance_checkpoint=False)
    assert _Dfm.checkpoints == []


def test_scope_is_passed_on_and_validated(monkeypatch):
    _, calls = _run(monkeypatch, ["1"], mode="full_load", scope="register")
    assert calls[0][1] == "register"
    with pytest.raises(ValueError):
        _run(monkeypatch, ["1"], scope="alle")


def test_without_changes_no_write_and_no_checkpoint(monkeypatch):
    result, calls = _run(monkeypatch, [], mode="delta")
    assert calls == [] and result.metadata["fetched"] == 0 and _Dfm.checkpoints == []


def test_the_asset_is_in_the_cpr_job():
    from jobs.cpr_jobs import cpr_ingest_job

    assert "fhir_server_patient_cpr_dfm" in str(cpr_ingest_job.selection)

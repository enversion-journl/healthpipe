# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Population failure boundaries, safe output and integration contracts."""

import importlib
from pathlib import Path
from unittest.mock import MagicMock

import dagster as dg
import pytest
from core.dfm_config import RunModeConfig
from DSA.SQL.common.borgerdemografi import borger_key_sql, key_sql
from DSA.SQL.dim.dim_BorgerDemografi import dim_BorgerDemografi

mod = importlib.import_module("DSA.SQL.dim.dim_BorgerDemografi")
DDL = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl"
pytestmark = pytest.mark.unit


def test_success_commits_before_recording_completion(monkeypatch):
    calls = MagicMock()
    monkeypatch.setattr(mod, "DfmManager", lambda *args: calls.dfm)
    resource = MagicMock()
    resource.get_connection.return_value.__enter__.return_value = calls.conn
    cursor = calls.conn.cursor.return_value
    cursor.fetchone.return_value = (3,)
    cursor.rowcount = 2
    result = dim_BorgerDemografi.op.compute_fn.decorated_fn(
        MagicMock(), resource, RunModeConfig(mode="full_load")
    )
    order = [
        name for name, *_ in calls.mock_calls if name in ("conn.commit", "dfm.complete_dfm_run")
    ]
    assert order == ["conn.commit", "dfm.complete_dfm_run"]
    calls.conn.rollback.assert_not_called()
    calls.dfm.set_checkpoint.assert_not_called()
    assert result.metadata["new_combinations"] == 2


def test_failure_rolls_back_without_exposing_values(monkeypatch):
    dfm = MagicMock()
    monkeypatch.setattr(mod, "DfmManager", lambda *args: dfm)
    resource = MagicMock()
    conn = resource.get_connection.return_value.__enter__.return_value
    conn.cursor.return_value.execute.side_effect = RuntimeError("sensitive database value")
    with pytest.raises(dg.Failure) as error:
        dim_BorgerDemografi.op.compute_fn.decorated_fn(MagicMock(), resource, RunModeConfig())
    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    dfm.complete_dfm_run.assert_not_called()
    assert "sensitive" not in str(error.value)
    assert dfm.fail_dfm_run.call_args.kwargs["error_message"] == "RuntimeError"


def test_unknown_birth_cohort_keeps_the_existing_hash_contract():
    old = (DDL / "migrations/20261028_borgerdemografi_hash.sql").read_text()
    legacy = old.split("ADD [dw_ek_BorgerDemografi] AS (", 1)[1].split(") PERSISTED;", 1)[0]
    assert f"THEN {legacy} ELSE" in key_sql()


def test_persisted_key_on_borger_matches_the_shared_expression():
    expression = f"AS ({borger_key_sql()}) PERSISTED"
    for path in (
        "tables/shared_dimensions/03_borgerdemografi.sql",
        "migrations/20261028b_borgerdemografi_foedselsperiode.sql",
    ):
        sql = (DDL / path).read_text()
        assert expression in sql or expression in sql.replace("''", "'")


def test_birth_date_day_cannot_participate_in_the_key():
    sql = borger_key_sql()
    assert "YEAR([foedselsdato])" in sql
    assert "MONTH([foedselsdato])" in sql
    remainder = sql.replace("YEAR([foedselsdato])", "").replace("MONTH([foedselsdato])", "")
    assert "foedselsdato" not in remainder

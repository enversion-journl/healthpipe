# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The backlog sensor: one more run when a delivery is waiting — otherwise nothing."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import dagster as dg
import pytest
from sensors import cpr_backlog_sensor as modul

pytestmark = pytest.mark.unit

_SHARE = SimpleNamespace(root=lambda: "/tmp/share")


def _context(active=False):
    context = MagicMock()
    context.instance.get_run_records.return_value = [object()] if active else []
    return context


def test_an_active_run_gives_skip_without_reading_the_backlog(monkeypatch):
    monkeypatch.setattr(modul, "sidst_indlaeste_leverance", MagicMock(side_effect=AssertionError))
    context = _context(active=True)
    result = modul.evaluate(
        context, job_name="cpr_ingest_job_feature_x", mssql=object(), share=_SHARE
    )
    assert isinstance(result, dg.SkipReason) and "still active" in result.skip_message
    run_filter = context.instance.get_run_records.call_args.kwargs["filters"]
    assert run_filter.job_name == "cpr_ingest_job_feature_x"
    assert dg.DagsterRunStatus.STARTED in run_filter.statuses


def test_no_waiting_delivery_gives_skip(monkeypatch):
    monkeypatch.setattr(modul, "sidst_indlaeste_leverance", lambda mssql: ("20260915", False))
    monkeypatch.setattr(modul, "naeste_leverance", lambda rod, sidst: None)
    result = modul.evaluate(_context(), job_name="cpr_ingest_job", mssql=object(), share=_SHARE)
    assert isinstance(result, dg.SkipReason) and "20260915" in result.skip_message


def test_waiting_delivery_orders_exactly_one_run_keyed_by_file_name(monkeypatch):
    monkeypatch.setattr(modul, "sidst_indlaeste_leverance", lambda mssql: ("20260915", False))
    monkeypatch.setattr(modul, "naeste_leverance", lambda rod, sidst: "/tmp/share/d260916.l208109")
    result = modul.evaluate(
        _context(), job_name="cpr_ingest_job_feature_x", mssql=object(), share=_SHARE
    )
    assert isinstance(result, dg.RunRequest)
    assert result.job_name == "cpr_ingest_job_feature_x"
    assert result.run_key == "cpr_backlog:d260916.l208109"
    assert result.tags["dagster/triggered_by"] == modul.SENSOR_NAME


def test_lookup_failure_does_not_fail_the_tick(monkeypatch):
    def _fejl(mssql):
        raise OSError("share")

    monkeypatch.setattr(modul, "sidst_indlaeste_leverance", _fejl)
    result = modul.evaluate(_context(), job_name="cpr_ingest_job", mssql=object(), share=_SHARE)
    assert isinstance(result, dg.SkipReason) and "OSError" in result.skip_message
    assert "share" not in result.skip_message


def test_a_programming_error_does_not_become_a_skip(monkeypatch):
    def _fejl(mssql):
        raise TypeError("wrong call")

    monkeypatch.setattr(modul, "sidst_indlaeste_leverance", _fejl)
    with pytest.raises(TypeError):
        modul.evaluate(_context(), job_name="cpr_ingest_job", mssql=object(), share=_SHARE)


def test_the_sensor_is_built_on_the_given_job():
    job = dg.define_asset_job(name="cpr_ingest_job_feature_x", selection="ext_cpr")
    sensor = modul.build_cpr_backlog_sensor(job)
    assert [j.name for j in sensor.jobs] == ["cpr_ingest_job_feature_x"]
    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED  # manual start per environment

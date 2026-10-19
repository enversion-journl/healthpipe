# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Every IFDB delta is bounded on one clock: the flow start stores both ceilings, and each
extract reads the ceiling on its delta column's clock."""

from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from core.dfm_config import FlowStartConfig
from DSI.oracle import ifdb_extract
from DSI.oracle.ifdb_extract import IFDB_EPJ_CEILING, IFDB_TRANSFER_CEILING

pytestmark = pytest.mark.unit

_CUT = datetime(2026, 10, 5, 13, 0, 8)
_ORACLE_NOW = datetime(2026, 10, 5, 13, 34, 23)
_COMPLETE = [
    ("Patientoplysninger til datawarehouse", _CUT, 0),
    ("Epikriser", datetime(2026, 10, 5, 13, 0, 16), 0),
]
_RUNNING = [
    ("Patientoplysninger til datawarehouse", _CUT, 1),
    ("Epikriser", datetime(2026, 10, 5, 13, 0, 16), 0),
]


class _FakeDfm:
    """DFMState as a dict keyed by (schema, table)."""

    def __init__(self, state=None):
        self.state = dict(state or {})

    def start_dfm_run(self, *args, **kwargs):
        return SimpleNamespace(log_id=1)

    def get_checkpoint(self, schema, table):
        return self.state.get((schema, table))

    def set_checkpoint(self, schema, table, *, value=None, mode="delta"):
        self.state[(schema, table)] = value if value is not None else "2026-10-05 11:34:23"

    def complete_dfm_run(self, *args, **kwargs):
        pass

    def fail_dfm_run(self, *args, **kwargs):
        pass


def _flow_start(rows, state=None, override=None):
    dfm = _FakeDfm(state)
    with (
        patch.object(ifdb_extract, "DfmManager", return_value=dfm),
        patch.object(ifdb_extract, "_read_ifdb_cycle_rows", return_value=(rows, _ORACLE_NOW)),
    ):
        result = ifdb_extract.dfm_ifdb_flow_start(
            context=dg.build_asset_context(),
            ifdb_oracle=MagicMock(),
            mssql_target_staging=MagicMock(),
            config=FlowStartConfig(flow_start_override=override),
        )
    return dfm.state, result


def test_complete_cycle_stores_oracle_time_as_transfer_ceiling_and_cut_as_epj_ceiling():
    """IFDB stamps a cycle's rows after the SLUTTID cut, so SLUTTID cannot bound them."""
    state, result = _flow_start(_COMPLETE)

    assert state[IFDB_TRANSFER_CEILING] == "2026-10-05 13:34:23"
    assert state[IFDB_EPJ_CEILING] == "2026-10-05 13:00:08"
    assert result.metadata["transfer_ceiling"] == "2026-10-05 13:34:23"


def test_incomplete_cycle_keeps_both_previous_ceilings():
    """A transfer ceiling past uncommitted rows skips them; an EPJ ceiling alone moving on
    would put the EPJ-clock tables a cycle ahead of the rest."""
    state, _ = _flow_start(
        _RUNNING,
        state={
            IFDB_TRANSFER_CEILING: "2026-10-05 09:20:00",
            IFDB_EPJ_CEILING: "2026-10-05 09:00:08",
        },
    )

    assert state[IFDB_TRANSFER_CEILING] == "2026-10-05 09:20:00"
    assert state[IFDB_EPJ_CEILING] == "2026-10-05 09:00:08"


def test_incomplete_cycle_without_an_earlier_transfer_ceiling_fails():
    with pytest.raises(dg.Failure, match="not complete"):
        _flow_start(_RUNNING)


def test_override_sets_both_ceilings():
    state, _ = _flow_start(_RUNNING, override="2025-06-30 23:59:59")

    assert state[IFDB_TRANSFER_CEILING] == "2025-06-30 23:59:59"
    assert state[IFDB_EPJ_CEILING] == "2025-06-30 23:59:59"


def test_cycle_read_takes_oracle_time_before_the_job_status():
    """Read after the job status, the time can pass rows a job started in between still holds."""
    executed: list[str] = []
    cursor = MagicMock()
    cursor.execute.side_effect = lambda sql, *args: executed.append(sql)
    cursor.fetchone.return_value = (_ORACLE_NOW,)
    cursor.fetchall.return_value = _COMPLETE

    @contextmanager
    def get_connection():
        yield SimpleNamespace(cursor=lambda: cursor)

    rows, now = ifdb_extract._read_ifdb_cycle_rows(SimpleNamespace(get_connection=get_connection))

    assert "SYSDATE" in executed[0]
    assert "JOB_STATUS" in executed[1]
    assert (rows, now) == (_COMPLETE, _ORACLE_NOW)


# Columns IFDB stamps when it transfers the row, measured against the 13:00 cycle on
# 2026-10-05; MODTIDSPUNKT and REFERRAL_DATE follow the EPJ clock.
_TRANSFER_CLOCK_COLUMNS = {"IFDB_MODIFICATION_DATETIME", "OPDATERET_TID", "UPDATE_TIME"}
_EPJ_CLOCK_COLUMNS = {"MODTIDSPUNKT", "REFERRAL_DATE"}


def _ifdb_delta_extracts():
    return [
        c
        for c in ifdb_extract.IFDB_EXTRACT_CONFIGS
        if c.source_schema == "IFDB" and not c.is_full_load and c.flow_start_from
    ]


@pytest.mark.parametrize("extract", _ifdb_delta_extracts(), ids=lambda c: c.target_table)
def test_ifdb_delta_extract_bounds_both_ends_on_its_delta_column(extract):
    """A ceiling on another column runs on another clock and splits or repeats IFDB cycles."""
    assert (extract.upper_bound_column or extract.effective_delta_col) == (
        extract.effective_delta_col
    )


@pytest.mark.parametrize("extract", _ifdb_delta_extracts(), ids=lambda c: c.target_table)
def test_ifdb_delta_extract_reads_the_ceiling_on_its_delta_column_clock(extract):
    """The EPJ cut lies before its own cycle's transfer stamps; the wrong clock cuts cycles."""
    column = extract.effective_delta_col.split(".")[-1]
    assert column in _TRANSFER_CLOCK_COLUMNS | _EPJ_CLOCK_COLUMNS, f"unmeasured clock: {column}"
    expected = IFDB_TRANSFER_CEILING if column in _TRANSFER_CLOCK_COLUMNS else IFDB_EPJ_CEILING
    assert extract.flow_start_from == expected

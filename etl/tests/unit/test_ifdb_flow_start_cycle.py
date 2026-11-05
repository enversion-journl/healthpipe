# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The IFDB cycle wait holds the flow start until every active job type has finished a cycle."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from core.dfm_config import IFDBCycleWaitConfig
from definitions import defs
from DSI.oracle import ifdb_extract
from DSI.oracle.ifdb_extract import ifdb_cycle_state, wait_for_complete_ifdb_cycle

pytestmark = pytest.mark.unit

_09 = datetime(2026, 9, 23, 9, 0, 0)
_09_SECONDS_LATER = datetime(2026, 9, 23, 9, 0, 8)
_05 = datetime(2026, 9, 23, 5, 0, 15)
_13 = datetime(2026, 9, 23, 13, 0, 8)
_NOW = datetime(2026, 9, 23, 13, 0, 30)

_OLD_CYCLE_COMPLETE = [
    ("Patientoplysninger til datawarehouse", _09, 0),
    ("Medicinordinationer", _09, 0),
]
_NEW_CYCLE_STARTED = [
    ("Patientoplysninger til datawarehouse", _09, 1),
    ("Medicinordinationer", _09, 0),
]
_NEW_CYCLE_COMPLETE = [
    ("Patientoplysninger til datawarehouse", _13, 0),
    ("Medicinordinationer", _13, 0),
]


def _wait(rows, timeout_minutes=180):
    """Run the wait against scripted reads; return the state and how many reads it made."""
    with (
        patch.object(
            ifdb_extract, "_read_ifdb_cycle_rows", side_effect=[(r, _NOW) for r in rows]
        ) as reader,
        patch.object(ifdb_extract.time, "sleep"),
    ):
        state = wait_for_complete_ifdb_cycle(
            MagicMock(), MagicMock(), timeout_minutes=timeout_minutes
        )
    return state, reader.call_count


def test_complete_cycle_with_seconds_spread_is_complete():
    """A finished cycle can spread SLUTTID over a few seconds."""
    state = ifdb_cycle_state([("PAS kontakter", _09, 0), ("Epikriser", _09_SECONDS_LATER, 0)], _NOW)

    assert state.is_complete
    assert state.flow_start == _09


def test_job_type_from_previous_cycle_is_lagging():
    state = ifdb_cycle_state(
        [("Patientoplysninger til datawarehouse", _09, 0), ("Medicinordinationer", _05, 0)], _NOW
    )

    assert not state.is_complete
    assert state.lagging_job_types == ("Medicinordinationer",)
    assert state.flow_start == _05


def test_running_job_type_blocks_before_its_cycle_is_logged():
    """JOB_LOG gets a row only on completion, so only JOB_STATUS.IGANG shows a running job type."""
    state = ifdb_cycle_state(
        [("Patientoplysninger til datawarehouse", _05, 1), ("PAS kontakter", _05, 0)], _NOW
    )

    assert not state.is_complete
    assert state.running_job_types == ("Patientoplysninger til datawarehouse",)


def test_no_rows_gives_no_flow_start():
    assert ifdb_cycle_state([], _NOW).flow_start is None


@pytest.mark.parametrize(
    ("medicin_running", "now", "failed"),
    [
        (0, datetime(2026, 9, 23, 9, 20), ("Medicinordinationer",)),
        (0, datetime(2026, 9, 23, 9, 10), ()),
        (1, datetime(2026, 9, 23, 9, 20), ()),
    ],
    ids=["idle-after-15-min", "idle-within-15-min", "running"],
)
def test_lagging_job_type_counts_as_failed_only_when_idle_after_15_minutes(
    medicin_running, now, failed
):
    rows = [
        ("Patientoplysninger til datawarehouse", _09, 0),
        ("Medicinordinationer", _05, medicin_running),
    ]

    assert ifdb_cycle_state(rows, now).failed_job_types == failed


def test_failed_job_type_fails_the_wait_at_once():
    """A job type that failed in IFDB would otherwise hold the run until the timeout."""
    lagging = [("Patientoplysninger til datawarehouse", _09, 0), ("Medicinordinationer", _05, 0)]
    with (
        patch.object(
            ifdb_extract,
            "_read_ifdb_cycle_rows",
            return_value=(lagging, datetime(2026, 9, 23, 9, 20)),
        ),
        patch.object(ifdb_extract.time, "sleep") as sleep,
        pytest.raises(dg.Failure, match=r"failed in IFDB.*Medicinordinationer"),
    ):
        wait_for_complete_ifdb_cycle(MagicMock(), MagicMock(), timeout_minutes=180)

    sleep.assert_not_called()


def test_wait_warns_about_active_job_types_it_cannot_see():
    rows = [("Patientoplysninger til datawarehouse", _09, None)]
    context = MagicMock()
    with (
        patch.object(
            ifdb_extract,
            "IFDB_ACTIVE_JOB_TYPES",
            ("Patientoplysninger til datawarehouse", "Epikriser"),
        ),
        patch.object(ifdb_extract, "_read_ifdb_cycle_rows", return_value=(rows, _NOW)),
        patch.object(ifdb_extract.time, "sleep"),
    ):
        wait_for_complete_ifdb_cycle(context, MagicMock(), timeout_minutes=180)

    warning = context.log.warning.call_args.args[0]
    assert "Epikriser" in warning
    assert "Patientoplysninger til datawarehouse" in warning


def test_complete_cycle_is_confirmed_by_a_second_read():
    state, reads = _wait([_OLD_CYCLE_COMPLETE, _OLD_CYCLE_COMPLETE])

    assert state.flow_start == _09
    assert reads == 2


def test_cycle_that_starts_during_the_confirmation_is_waited_for():
    """The patient job sets IGANG under a second after hh:00, so one read can miss it."""
    state, reads = _wait(
        [_OLD_CYCLE_COMPLETE, _NEW_CYCLE_STARTED, _NEW_CYCLE_COMPLETE, _NEW_CYCLE_COMPLETE]
    )

    assert state.flow_start == _13
    assert reads == 4


def test_new_flow_start_after_the_confirmation_is_confirmed_again():
    state, reads = _wait([_OLD_CYCLE_COMPLETE, _NEW_CYCLE_COMPLETE, _NEW_CYCLE_COMPLETE])

    assert state.flow_start == _13
    assert reads == 3


def test_zero_timeout_does_not_read_ifdb():
    """A run with an override must work when Oracle is not reachable."""
    with patch.object(ifdb_extract, "_read_ifdb_cycle_rows") as reader:
        result = ifdb_extract.dfm_ifdb_cycle_wait(
            context=dg.build_asset_context(),
            ifdb_oracle=MagicMock(),
            config=IFDBCycleWaitConfig(cycle_wait_timeout_minutes=0),
        )

    reader.assert_not_called()
    assert result.metadata["complete"] is None


def test_stuck_job_type_fails_once_the_timeout_in_minutes_has_passed():
    clock = [0.0]

    def sleep(seconds):
        clock[0] += seconds
        if clock[0] > 2 * 180 * 60:
            raise AssertionError("the wait never timed out")

    with (
        patch.object(
            ifdb_extract, "_read_ifdb_cycle_rows", return_value=(_NEW_CYCLE_STARTED, _NOW)
        ),
        patch.object(ifdb_extract.time, "sleep", side_effect=sleep),
        patch.object(ifdb_extract.time, "monotonic", side_effect=lambda: clock[0]),
        pytest.raises(dg.Failure, match="Patientoplysninger til datawarehouse"),
    ):
        wait_for_complete_ifdb_cycle(MagicMock(), MagicMock(), timeout_minutes=180)

    assert 180 * 60 <= clock[0] < 180 * 60 + ifdb_extract.IFDB_CYCLE_POLL_SECONDS


def test_cycle_wait_without_job_log_rows_is_not_reported_complete():
    """No rows means the flow start falls back to SYSDATETIME, which is not a complete cycle."""
    with patch.object(ifdb_extract, "_read_ifdb_cycle_rows", return_value=([], _NOW)):
        result = ifdb_extract.dfm_ifdb_cycle_wait(
            context=dg.build_asset_context(),
            ifdb_oracle=MagicMock(),
            config=IFDBCycleWaitConfig(),
        )

    assert result.metadata["complete"] is False
    assert result.metadata["flow_start"] == ""


def test_flow_start_depends_on_the_cycle_wait_in_every_ifdb_job():
    """Without this edge the flow start runs at once and the wait silently does nothing."""
    graph = defs.resolve_asset_graph()
    keys = {k.path[-1]: k for k in graph.get_all_asset_keys()}
    wait = next(k for name, k in keys.items() if name.startswith("dfm_ifdb_cycle_wait"))
    flow_start = next(k for name, k in keys.items() if name.startswith("dfm_ifdb_flow_start"))

    assert wait in graph.get(flow_start).parent_keys
    for job_name in ("ifdb_archive_job", "ifdb_dfm_daily_job"):
        selected = defs.get_job_def(job_name).asset_layer.selected_asset_keys
        assert flow_start in selected and wait in selected, job_name

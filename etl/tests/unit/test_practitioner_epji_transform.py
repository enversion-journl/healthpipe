# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the EPJI Practitioner transform and query."""

import re
from types import SimpleNamespace
from unittest.mock import MagicMock

import dagster as dg
import DSA.FHIR.Practitioner.Practitioner_EPJI_dfm as practitioner_epji
import pytest
from core.dfm_config import RunModeConfig
from DSA.FHIR.Practitioner.Practitioner_EPJI_dfm import (
    build_practitioner_epji_query,
    resolve_staff_code_owners,
    transform_practitioner_epji_rows,
)
from utils.fhir.identifier_systems import (
    SYSTEM_EPJI_SOURCE_TABLE,
    SYSTEM_IFDB_STAFF,
    SYSTEM_REGION_REGIONSID,
)
from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED

pytestmark = pytest.mark.unit

CHECKPOINT = "2026-10-19 06:00:00"


def _account_row(**overrides) -> dict:
    row = {
        "PK": "1001",
        "EXTERNAL_ID": "pr-abc12",
        "STATE": "1",
        "NAME": "Testæøå Mellem Testson",
        "GIVEN_NAME": "Testæøå Mellem",
        "FAMILY_NAME": "Testson",
        "dw_is_deleted": False,
        "_regionsid": "abc12",
    }
    row.update(overrides)
    return row


def _one(**overrides):
    (resource,) = transform_practitioner_epji_rows([_account_row(**overrides)])
    return resource


def _is_deleted(resource) -> int:
    (ext,) = [e for e in resource.extension if e.url == EXTENSION_FHIR_IS_DELETED]
    return ext.valueInteger


@pytest.mark.parametrize(("state", "expected"), [("1", True), ("0", False), (" 0 ", False)])
def test_state_maps_to_active(state, expected):
    assert _one(STATE=state).active is expected


@pytest.mark.parametrize("state", ["2", "", None])
def test_unknown_state_leaves_active_out_and_is_logged(state):
    logger = MagicMock()
    (resource,) = transform_practitioner_epji_rows([_account_row(STATE=state)], logger)
    assert resource.active is None
    assert any("STATE" in call.args[0] for call in logger.warning.call_args_list)


def test_inactive_account_is_not_soft_deleted():
    """IFDB rows written under an inactive account must still find it."""
    assert _is_deleted(_one(STATE="0", dw_is_deleted=False)) == 0


def test_account_gone_from_epj_is_soft_deleted_whatever_its_state():
    assert _is_deleted(_one(STATE="1", dw_is_deleted=True)) == 1


def test_identifiers_are_staff_code_and_regionsid_only():
    resource = _one()
    assert [(i.system, i.value) for i in resource.identifier] == [
        (SYSTEM_IFDB_STAFF, "pr-abc12"),
        (SYSTEM_REGION_REGIONSID, "abc12"),
    ]


@pytest.mark.parametrize("regionsid", [None, "", "  "])
def test_account_without_regionsid_keeps_its_staff_code(regionsid):
    resource = _one(_regionsid=regionsid)
    assert [i.system for i in resource.identifier] == [SYSTEM_IFDB_STAFF]


def test_staff_code_case_variants_stay_two_resources():
    resources = transform_practitioner_epji_rows(
        [_account_row(EXTERNAL_ID="pr-ABC12"), _account_row(EXTERNAL_ID="pr-abc12")]
    )
    assert [r.identifier[0].value for r in resources] == ["pr-ABC12", "pr-abc12"]


def test_staff_code_is_trimmed_not_normalised():
    assert _one(EXTERNAL_ID="  pr-v-Abc12-2 ").identifier[0].value == "pr-v-Abc12-2"


def test_staff_code_owners_are_resolved_on_the_transform_key():
    rows = [
        ("1", "pr-abc12", 0),
        ("2", " pr-abc12\t", 1),
        ("3", "pr-ABC12", 0),
        ("4", None, 0),
        ("5", "", 1),
        ("6", "   ", 1),
    ]
    assert resolve_staff_code_owners(rows) == ({"2": "pr-abc12"}, {})


_WINDOW_COLUMNS = list(_account_row())


class _ArchiveCursor:
    """Answers the asset's reads: unknown systems, staff-code owners, and the run window."""

    def __init__(self, archive: list[tuple], window: list[dict]) -> None:
        self._archive = archive
        self._window = [tuple(row[c] for c in _WINDOW_COLUMNS) for row in window]
        self._result: list[tuple] = []
        self.description = [(c,) for c in _WINDOW_COLUMNS]

    def execute(self, sql: str, _params=()) -> None:
        if "NOT IN" in sql:
            self._result = []
        elif "_regionsid" in sql:
            self._result = list(self._window)
        else:
            self._result = list(self._archive)

    def fetchall(self) -> list[tuple]:
        return self._result

    def fetchmany(self, size: int) -> list[tuple]:
        batch, self._result = self._result[:size], self._result[size:]
        return batch

    def close(self) -> None:
        pass


def _run_asset(monkeypatch, archive: list[tuple], window_pks: list[str]) -> list:
    """Run the asset once in delta and return the Practitioners it posts."""
    accounts = {
        pk: _account_row(PK=pk, EXTERNAL_ID=code, NAME=f"konto-{pk}", dw_is_deleted=bool(deleted))
        for pk, code, deleted in archive
    }
    cursor = _ArchiveCursor(archive, [accounts[pk] for pk in window_pks])
    connection = MagicMock()
    connection.__enter__.return_value.cursor.return_value = cursor
    mssql = SimpleNamespace(get_connection=lambda: connection)

    posted: list = []

    def upsert(resources, resource_type):
        posted.extend(resources)
        return len(resources), 0, []

    dfm = MagicMock()
    dfm.start_dfm_run.return_value = SimpleNamespace(
        checkpoint=CHECKPOINT, log_id=1, started_at=CHECKPOINT
    )
    monkeypatch.setattr(practitioner_epji, "DfmManager", lambda *_a, **_k: dfm)
    monkeypatch.setattr(
        practitioner_epji, "prepare_resources_for_upsert", lambda resources, **_k: resources
    )
    monkeypatch.setattr(practitioner_epji, "advance_fhir_checkpoint", lambda *_a, **_k: None)

    compute = practitioner_epji.fhir_server_practitioner_epji_dfm.op.compute_fn
    asset_fn = getattr(compute, "decorated_fn", compute)
    asset_fn(
        SimpleNamespace(run_id="test-run", log=MagicMock()),
        mssql,
        SimpleNamespace(batch_upsert_resources=upsert),
        RunModeConfig(mode="delta"),
    )
    return posted


def _posted(resources) -> list[tuple[str, int]]:
    return [(r.name[0].text, _is_deleted(r)) for r in resources]


def test_deleted_loser_alone_in_the_window_posts_nothing(monkeypatch):
    archive = [("999", "pr-abc12", 1), ("1000", "pr-abc12", 0)]
    assert _run_asset(monkeypatch, archive, window_pks=["999"]) == []


def test_live_row_owns_a_code_a_deleted_row_also_holds(monkeypatch):
    archive = [("999", "pr-abc12", 0), ("1000", "pr-abc12", 1)]
    posted = _run_asset(monkeypatch, archive, window_pks=["999", "1000"])
    assert _posted(posted) == [("konto-999", 0)]


def test_two_deleted_rows_highest_pk_owns_the_code(monkeypatch):
    archive = [("999", "pr-abc12", 1), ("1000", "pr-abc12", 1)]
    posted = _run_asset(monkeypatch, archive, window_pks=["999", "1000"])
    assert _posted(posted) == [("konto-1000", 1)]


def test_two_live_rows_with_one_code_fail_before_posting(monkeypatch):
    archive = [("999", "pr-abc12", 0), ("1000", " pr-abc12", 0)]
    with pytest.raises(dg.Failure, match="pr-abc12"):
        _run_asset(monkeypatch, archive, window_pks=["999", "1000"])


@pytest.mark.parametrize("external_id", [None, "", "   "])
def test_account_without_staff_code_is_skipped_and_logged(external_id):
    logger = MagicMock()
    rows = [_account_row(EXTERNAL_ID=external_id), _account_row(EXTERNAL_ID="pr-x1")]
    resources = transform_practitioner_epji_rows(rows, logger)
    assert [r.identifier[0].value for r in resources] == ["pr-x1"]
    logger.warning.assert_called_once()


def test_name_parts_come_from_their_own_columns():
    (name,) = _one().name
    assert name.text == "Testæøå Mellem Testson"
    assert name.family == "Testson"
    assert name.given == ["Testæøå", "Mellem"]


def test_empty_name_part_is_left_out_not_derived_from_text():
    (name,) = _one(FAMILY_NAME="  ").name
    assert name.family is None
    assert name.text == "Testæøå Mellem Testson"


def test_source_table_tag():
    (tag,) = _one().meta.tag
    assert (tag.system, tag.code) == (SYSTEM_EPJI_SOURCE_TABLE, "epji_practitioner")


def _window(query: str) -> str:
    """The inner archive query, between the derived table's parentheses."""
    return query[query.index("FROM (") + len("FROM (") : query.index(") p")]


def test_delta_also_picks_up_accounts_whose_identifiers_changed():
    window = _window(build_practitioner_epji_query(CHECKPOINT, RunModeConfig(mode="delta")))
    assert "[arc].[epji_practitioner_identifier]" in window
    assert f"[dw_timestamp_update] > '{CHECKPOINT}'" in window
    assert "[dw_is_deleted]" not in window


def test_delta_lookback_also_widens_the_identifier_window():
    config = RunModeConfig(mode="delta", lookback_days=3)
    window = _window(build_practitioner_epji_query(CHECKPOINT, config))
    identifier_subquery = window[window.index("[arc].[epji_practitioner_identifier]") :]
    assert re.search(
        r"\[dw_timestamp_update\]\s*>\s*DATEADD\(day,\s*-3,", identifier_subquery, re.IGNORECASE
    )


def test_delta_column_override_applies_to_the_identifier_window_too():
    config = RunModeConfig(mode="delta", delta_column="dw_timestamp_insert")
    window = _window(build_practitioner_epji_query(CHECKPOINT, config))
    identifier_subquery = window[window.index("[arc].[epji_practitioner_identifier]") :]
    assert re.search(r"\[dw_timestamp_insert\]\s*>", identifier_subquery, re.IGNORECASE)
    assert "dw_timestamp_update" not in window


def test_regionsid_join_is_restricted_to_live_rows_of_one_system():
    query = build_practitioner_epji_query(None, RunModeConfig(mode="full_load"))
    join = query[query.index("LEFT JOIN") :]
    assert "r.[SYSTEM] = ?" in join
    assert "r.[dw_is_deleted] = 0" in join

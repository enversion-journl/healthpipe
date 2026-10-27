# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""dw-keys on the flat fhir.* tables: run scope, lookup contract and wiring."""

import re
from unittest.mock import MagicMock

import pytest
from dagster import AssetKey
from definitions import defs
from DSA.FHIR.SqlOnFhir.DWKeys_dfm import DW_KEYS_CONFIG
from DSA.FHIR.utils.sql_on_fhir.dw_keys import (
    DWKeysConfig,
    lookup_sql,
    organisation_read_bound,
    resolve_scope,
    run_dw_keys,
)

EPISODE_OF_CARE = DW_KEYS_CONFIG["episode_of_care"]
CHECKPOINTS = {
    EPISODE_OF_CARE.checkpoint_fhir: "2026-10-12 06:00:00.0000000",
    EPISODE_OF_CARE.checkpoint_borger: "2026-10-12 06:00:00.000",
    EPISODE_OF_CARE.checkpoint_organisation: "2026-10-12 06:00:00.000",
}


@pytest.mark.unit
class TestResolveScope:
    def test_delta_with_every_checkpoint_reads_candidates_only(self) -> None:
        assert resolve_scope("delta", CHECKPOINTS) == (False, None)

    def test_a_missing_checkpoint_forces_a_full_pass(self) -> None:
        """Without the borger watermark, borger changes before the first run would be missed."""
        checkpoints = {**CHECKPOINTS, EPISODE_OF_CARE.checkpoint_borger: None}
        full, reason = resolve_scope("delta", checkpoints)
        assert full
        assert EPISODE_OF_CARE.checkpoint_borger in reason

    def test_full_load_ignores_checkpoints(self) -> None:
        assert resolve_scope("full_load", CHECKPOINTS)[0]

    @pytest.mark.parametrize("mode", ["period", "ids"])
    def test_other_modes_are_refused(self, mode: str) -> None:
        with pytest.raises(ValueError, match=mode):
            resolve_scope(mode, CHECKPOINTS)

    @pytest.mark.parametrize(
        ("started_at", "bound"),
        [("2026-10-14 06:00:01.0000000", "2026-10-14 05:00:01"), (None, None)],
    )
    def test_organisation_bound_reaches_back_before_the_run_start(self, started_at, bound) -> None:
        """A dim_Organisation commit after the read must fall after the bound, or it is lost."""
        assert organisation_read_bound(started_at) == bound


@pytest.mark.unit
class TestLookupContract:
    def test_entity_key_is_found_for_a_soft_deleted_borger_but_the_version_is_not(self) -> None:
        """dw_ek_Borger is the borger's identity; dw_sk_Borger only comes from a live version."""
        _, applies = lookup_sql(EPISODE_OF_CARE)
        entity = applies.split(") AS borger_entity")[0]
        version = applies.split(") AS borger_entity")[1].split(") AS borger_version")[0]
        live_only = re.compile(r"dw_is_deleted\s*=\s*0", re.IGNORECASE)
        assert not live_only.search(entity)
        assert live_only.search(version)

    def test_patient_column_without_time_column_is_refused(self) -> None:
        with pytest.raises(ValueError, match="together"):
            DWKeysConfig(
                asset_suffix="x",
                table="X",
                primary_key="x_id",
                organisation_column="organization_id",
                patient_column="patient_id",
            )


class _RecordingCursor:
    """Answers the candidate count, an empty batch range and the given key distribution."""

    def __init__(self, candidates: int = 0, distribution: dict[str, int] | None = None) -> None:
        self.calls: list[tuple[str, object]] = []
        self.description: list[tuple[str]] = []
        self._candidates = candidates
        self._distribution = distribution or {}
        self._row: tuple | None = None

    def execute(self, sql: str, params: object = None) -> None:
        self.calls.append((sql, params))
        if "COUNT_BIG" in sql:
            self._row = (self._candidates,)
        elif "_minus1]" in sql:
            self.description = [(name,) for name in self._distribution]
            self._row = tuple(self._distribution.values())
        elif "SELECT TOP" in sql:
            self._row = (None,)
        else:
            self._row = ("2026-10-13 06:00:00",)

    def fetchone(self) -> tuple | None:
        return self._row


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("episode_of_care", ["fhir", "borger", "organisation"]),
        ("location", ["fhir", "organisation"]),
    ],
)
def test_delta_binds_one_checkpoint_per_candidate_source_in_query_order(
    name: str, expected: list[str]
) -> None:
    """A wrong count or order puts one source's watermark on another and skips changes."""
    cfg = DW_KEYS_CONFIG[name]
    checkpoints = {
        cfg.checkpoint_fhir: "fhir",
        cfg.checkpoint_borger: "borger",
        cfg.checkpoint_organisation: "organisation",
    }
    cursor = _RecordingCursor()
    run_dw_keys(cursor, MagicMock(), cfg, full=False, checkpoints=checkpoints, log=MagicMock())
    sql, params = next(call for call in cursor.calls if "INSERT INTO #candidates" in call[0])
    assert list(params) == expected
    assert sql.count("?") == len(expected)
    assert sql.find("[dim].[Borger]") < sql.index("[dim].[Organisation]")


@pytest.mark.unit
@pytest.mark.parametrize(("minus1", "warned"), [(0, False), (3, True)])
def test_a_reference_the_dimensions_lack_is_logged(minus1: int, warned: bool) -> None:
    """A -1 is a source value the dimensions do not have; unlogged it is silent drift."""
    cfg = DW_KEYS_CONFIG["location"]
    checkpoints = {cfg.checkpoint_fhir: "fhir", cfg.checkpoint_organisation: "organisation"}
    distribution = {
        "dw_ek_Organisation_found": 10,
        "dw_ek_Organisation_minus1": minus1,
    }
    log = MagicMock()
    result = run_dw_keys(
        _RecordingCursor(candidates=13, distribution=distribution),
        MagicMock(),
        cfg,
        full=False,
        checkpoints=checkpoints,
        log=log,
    )
    assert result["distribution"] == distribution
    assert log.warning.called is warned
    if warned:
        assert "dw_ek_Organisation=3" in log.warning.call_args.args[-1]


@pytest.mark.unit
@pytest.mark.parametrize("sof_name", sorted(DW_KEYS_CONFIG))
def test_dso_sync_waits_for_the_dw_keys_and_index_maintenance_for_the_sync(sof_name: str) -> None:
    """Without these edges the sync carries keys still at -1, or a rebuild locks a table mid-sync."""
    graph = defs.resolve_asset_graph()
    sync = AssetKey.from_user_string(f"dso_sync_fhir_{sof_name}_dfm")
    keys = AssetKey.from_user_string(DW_KEYS_CONFIG[sof_name].asset_name)
    maintenance = AssetKey.from_user_string("fhir_dw_keys_index_maintenance")
    assert keys in graph.get(sync).parent_keys
    assert sync in graph.get(maintenance).parent_keys

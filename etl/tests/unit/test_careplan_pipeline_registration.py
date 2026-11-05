# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for CarePlan asset wiring on the DFM path."""

import pytest
from core.dfm_config import RunModeConfig
from dagster import AssetKey
from DSA.FHIR.CarePlan.CarePlan_IFDB_dfm import ARCHIVE_TABLE, fhir_server_careplan_dfm
from DSA.FHIR.SqlOnFhir.CarePlan_dfm import sof_careplan_activity_dfm, sof_careplan_dfm
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.sql_on_fhir.sof_utils import SOF_RESOURCE_CONFIGS
from DSI.oracle.ifdb_archive import EVENTMARKER_ARCHIVE_CONFIG
from tests.utils.asset_graph_helpers import (
    DFM_PRODUCER_PREFIXES,
    SOF_PREFIXES,
    assert_deps,
)


def _single_asset_group(asset_def, asset_name: str) -> str:
    return asset_def.group_names_by_key[AssetKey.from_user_string(asset_name)]


@pytest.mark.unit
def test_careplan_transform_runs_in_the_dfm_group() -> None:
    assert (
        _single_asset_group(fhir_server_careplan_dfm, "fhir_server_careplan_dfm") == "fhir_ifdb_dfm"
    )


@pytest.mark.unit
def test_careplan_reads_the_archive_and_waits_for_patient_and_eoc() -> None:
    """The transform reads arc.ifdb_eventmarker, not IFDB Oracle, and its
    subject/EpisodeOfCare references must already exist on the FHIR server."""

    assert_deps(
        fhir_server_careplan_dfm,
        "fhir_server_careplan_dfm",
        allowed_prefixes=DFM_PRODUCER_PREFIXES,
        required={
            "arc_eventmarker_ifdb",
            "fhir_server_patient_dfm",
            "fhir_server_eoc_dfm",
        },
    )


@pytest.mark.unit
def test_sof_careplan_assets_follow_the_transform() -> None:

    assert_deps(
        sof_careplan_dfm,
        "sof_careplan_dfm",
        allowed_prefixes=SOF_PREFIXES,
        required={"fhir_sof_infrastructure", "fhir_server_careplan_dfm"},
    )
    assert_deps(
        sof_careplan_activity_dfm,
        "sof_careplan_activity_dfm",
        allowed_prefixes=SOF_PREFIXES,
        required={"fhir_sof_infrastructure", "sof_careplan_dfm"},
    )
    assert (
        _single_asset_group(sof_careplan_activity_dfm, "sof_careplan_activity_dfm")
        == "fhir_sof_dfm"
    )


@pytest.mark.unit
def test_careplan_activity_reconciles_deletes_at_child_grain() -> None:
    """Without this the MERGE only ever adds and updates, so an activity removed
    from a still-live CarePlan would stay live in fhir.CarePlan_Activity."""
    cfg = SOF_RESOURCE_CONFIGS["careplan_activity_view"]

    assert cfg["child_grain_reconcile"] is True
    # The reconcile anti-joins on the PARENT id, so the view's id_column must be
    # the careplan, not the activity key it is MERGEd on.
    assert cfg["id_column"] == "careplan_id"
    assert cfg["primary_keys"] == ["ifdb_eventmarker_id"]
    # The mark-then-MERGE only works if the MERGE writes dw_is_deleted back to 0.
    assert cfg["resurrect_soft_deleted"] is True


@pytest.mark.unit
def test_archive_maps_the_source_delete_flag_onto_dw_is_deleted() -> None:
    """The whole delete chain hangs off this: the merge derives dw_is_deleted
    from EVENTMARKER.DELETED, which is why a retirement is visible in the
    archive delta at all."""
    assert EVENTMARKER_ARCHIVE_CONFIG.source_delete_column == "DELETED"
    # ext holds only the delta window, so NOT-MATCHED-BY-SOURCE would retire the
    # whole archive on every run.
    assert EVENTMARKER_ARCHIVE_CONFIG.detect_deletes is False


@pytest.mark.unit
def test_delta_patient_scope_query_does_not_filter_out_retired_markers() -> None:
    """A retired marker's only trace in the window IS the row that just went
    dw_is_deleted = 1. Filtering on dw_is_deleted = 0 here would drop the patient
    from the scope, and the removal would never reach the FHIR server —
    silently, because every other row of that patient still looks fine."""
    query = build_fhir_archive_query(
        ARCHIVE_TABLE,
        "2025-06-01 00:00:00",
        RunModeConfig(mode="delta"),
        include_deleted=True,
        select_clause="SELECT DISTINCT [PATIENT_ID]",
    )

    assert "[dw_is_deleted] = 0" not in query
    assert "SELECT DISTINCT [PATIENT_ID]" in query
    assert "[dw_timestamp_update] > '2025-06-01 00:00:00'" in query


@pytest.mark.unit
def test_the_nightly_job_selects_the_whole_careplan_chain() -> None:
    """En dep på et asset uden for jobbet er en ekstern dep i Dagster: ordnings-
    kanten forsvinder stille. Hele kæden — arkiv, transform, de to SOF-loads og
    DSO-syncen — skal derfor vælges af det samme natjob."""
    from definitions import defs

    nodes = {node.name for node in defs.resolve_job_def("ifdb_dfm_daily_job").nodes}

    for navn in (
        "arc_eventmarker_ifdb",
        "fhir_server_careplan_dfm",
        "sof_careplan_dfm",
        "sof_careplan_activity_dfm",
        "dso_sync_fhir_careplan_dfm",
        "dso_sync_fhir_careplan_activity_dfm",
    ):
        assert navn in nodes, navn

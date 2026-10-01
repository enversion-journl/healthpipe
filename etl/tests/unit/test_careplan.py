# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for _build_alle_markoerer_careplan_dict.

Tests the pure dict-building function that constructs CarePlan data
for the "Alle Markoerer" (All Markers) patient rights area.
"""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import dagster as dg
import DSA.FHIR.CarePlan.CarePlan_IFDB as careplan_module
import pytest
from core.parallel import ChunkedPipelineStage
from DSA.FHIR.CarePlan.CarePlan_IFDB import (
    _build_alle_markoerer_careplan_dict,
    _post_careplans_to_fhir,
    _post_careplans_to_fhir_batch,
    _run_careplan_delta_pipeline,
)
from fhir.resources.R4B.careplan import CarePlan
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.reference import Reference
from utils.fhir.identifier_systems import SYSTEM_CAREPLAN_ALLE_MARKOERER


def _make_marker(
    object_id: str = "OBJ1",
    marker_code: str = "AFV01A1",
    marker_code_full: str = "SKS.a.adm.AFV01A1",
    marker_type: str = "start",
    event_text: str = "Henvist til udredning",
    timestamp: datetime | None = None,
    sor_code: str = "SOR123",
    staff_id: str = "STAFF1",
    episode_of_care_id: str = "EOC1",
) -> dict:
    """Helper to build a marker dict with sensible defaults."""
    return {
        "OBJECT_ID": object_id,
        "MARKER_CODE": marker_code,
        "MARKER_CODE_FULL": marker_code_full,
        "MARKER_TYPE": marker_type,
        "EVENT_TEXT": event_text,
        "MarkoerDatotid": timestamp or datetime(2025, 1, 15, 10, 0, 0),
        "ADMINISTRATIVE_UNIT_SORCODE": sor_code,
        "RESPONSIBLE_STAFF_PERSID": staff_id,
        "EPISODE_OF_CARE_ID": episode_of_care_id,
    }


class _FakeFhirDatabase:
    def __init__(self) -> None:
        self.suspend_calls = 0
        self.finalize_calls: list[tuple[set[str], str]] = []

    def suspend_identifier_caches(self) -> None:
        self.suspend_calls += 1

    def finalize_identifier_caches(
        self,
        resource_types: set[str],
        *,
        reason: str,
    ) -> None:
        self.finalize_calls.append((resource_types, reason))


def _fake_context() -> SimpleNamespace:
    return SimpleNamespace(
        has_partition_key=True,
        partition_key="2026-04",
        log=SimpleNamespace(
            info=lambda *args, **kwargs: None,
            warning=lambda *args, **kwargs: None,
        ),
    )


@pytest.mark.unit
def test_careplan_chunk_stage_default_limits_reference_fanout():
    # Default deliberately raised 1_000 -> 5_000 ("Stabilize CarePlan monthly
    # execution"); keep in sync with CAREPLAN_CHUNK_STAGE in CarePlan_IFDB.py.
    assert careplan_module.CAREPLAN_CHUNK_STAGE.chunk_size == 5_000


@pytest.mark.unit
def test_delta_pipeline_chunks_staged_careplans_with_core_runner(monkeypatch):
    staged_careplans = [
        {"identifier": "cp-1", "activities": [{"object_id": "m-1"}]},
        {"identifier": "cp-2", "activities": [{"object_id": "m-2"}]},
        {"identifier": "cp-3", "activities": [{"object_id": "m-3"}]},
    ]
    transform_calls: list[list[str]] = []
    post_calls: list[list[str]] = []
    fhir_database = _FakeFhirDatabase()

    monkeypatch.setattr(
        careplan_module,
        "CAREPLAN_CHUNK_STAGE",
        ChunkedPipelineStage(
            stage_id="test.careplan",
            chunk_size=2,
            cleanup_resource_types=frozenset({"CarePlan"}),
        ),
    )
    monkeypatch.setattr(
        careplan_module,
        "_stage_careplans",
        lambda context, extracted_data: staged_careplans,
    )

    def fake_transform(chunk, context):
        transform_calls.append([cp["identifier"] for cp in chunk])
        return chunk

    def fake_post(context, careplans, fhir_database, mssql=None, reference_fallback_fn=None):
        post_calls.append([cp["identifier"] for cp in careplans])
        return dg.MaterializeResult(
            metadata={
                "posted_count": dg.MetadataValue.int(len(careplans)),
                "failed_count": dg.MetadataValue.int(0),
                "source": dg.MetadataValue.text("ifdb_alle_markoerer"),
                "method": dg.MetadataValue.text("direct_postgres"),
            }
        )

    monkeypatch.setattr(careplan_module, "_transform_careplans_to_fhir", fake_transform)
    monkeypatch.setattr(careplan_module, "_post_careplans_to_fhir", fake_post)

    result = _run_careplan_delta_pipeline(
        context=_fake_context(),
        extracted_data={"markers": [{"PATIENT_ID": "p-1"}]},
        fhir_database=fhir_database,
        mssql=SimpleNamespace(),
    )

    assert transform_calls == [["cp-1", "cp-2"], ["cp-3"]]
    assert post_calls == [["cp-1", "cp-2"], ["cp-3"]]
    assert fhir_database.suspend_calls == 2
    assert fhir_database.finalize_calls == [({"CarePlan"}, "test.careplan post-partition cleanup")]
    assert result.metadata["posted_count"].value == 3
    assert result.metadata["chunk_count"].value == 2
    assert result.metadata["chunk_size"].value == 2


def _make_careplan(identifier_value: str) -> CarePlan:
    return CarePlan(
        status="active",
        intent="plan",
        subject=Reference(reference="Patient/p1"),
        identifier=[
            Identifier(
                system=SYSTEM_CAREPLAN_ALLE_MARKOERER,
                value=identifier_value,
            )
        ],
    )


# ─── Basic happy path ────────────────────────────────────────────────


@pytest.mark.unit
def test_single_marker():
    """Single marker produces correct structure with all fields mapped."""
    ts = datetime(2025, 3, 1, 9, 30, 0)
    marker = _make_marker(
        object_id="OBJ42",
        marker_code="AFV01F",
        marker_code_full="SKS.a.adm.AFV01F",
        marker_type="end",
        event_text="Behandling start",
        timestamp=ts,
        sor_code="SOR999",
        staff_id="STAFF7",
        episode_of_care_id="EOC55",
    )

    result = _build_alle_markoerer_careplan_dict(patient_id="P100", markers=[marker])

    assert result["identifier"] == "P100_alle_markoerer"
    assert result["patient_id"] == "P100"
    assert result["period_start"] == ts
    assert result["period_end"] == ts  # Single marker → start == end
    assert result["status"] == "active"
    assert result["author_sor"] == "SOR999"
    assert result["patient_rights_type"] == "alle_markoerer"
    assert result["marker_count"] == 1

    activity = result["activities"][0]
    assert activity["object_id"] == "OBJ42"
    assert activity["marker_code"] == "AFV01F"
    assert activity["marker_code_full"] == "SKS.a.adm.AFV01F"
    assert activity["marker_type"] == "end"
    assert activity["event_text"] == "Behandling start"
    assert activity["timestamp"] == ts
    assert activity["status"] == "completed"
    assert activity["sor_code"] == "SOR999"
    assert activity["staff_id"] == "STAFF7"
    assert activity["episode_of_care_id"] == "EOC55"


@pytest.mark.unit
def test_multiple_markers_sorted_by_timestamp():
    """Markers are sorted chronologically; period spans first to last."""
    ts_early = datetime(2025, 1, 1, 8, 0, 0)
    ts_mid = datetime(2025, 2, 1, 10, 0, 0)
    ts_late = datetime(2025, 3, 1, 12, 0, 0)

    markers = [
        _make_marker(object_id="LATE", timestamp=ts_late),
        _make_marker(object_id="EARLY", timestamp=ts_early),
        _make_marker(object_id="MID", timestamp=ts_mid),
    ]

    result = _build_alle_markoerer_careplan_dict(patient_id="P200", markers=markers)

    assert result["period_start"] == ts_early
    assert result["period_end"] == ts_late
    assert result["marker_count"] == 3
    assert result["author_sor"] == markers[1]["ADMINISTRATIVE_UNIT_SORCODE"]  # From earliest

    # Activities should be in chronological order
    activity_ids = [a["object_id"] for a in result["activities"]]
    assert activity_ids == ["EARLY", "MID", "LATE"]


# ─── Edge cases ──────────────────────────────────────────────────────


@pytest.mark.unit
def test_empty_markers():
    """Empty marker list produces empty activities and None fields."""
    result = _build_alle_markoerer_careplan_dict(patient_id="P300", markers=[])

    assert result["identifier"] == "P300_alle_markoerer"
    assert result["patient_id"] == "P300"
    assert result["period_start"] is None
    assert result["period_end"] is None
    assert result["status"] == "active"
    assert result["author_sor"] is None
    assert result["marker_count"] == 0
    assert result["activities"] == []


@pytest.mark.unit
def test_marker_with_none_timestamp_sorted_first():
    """Markers with None timestamp sort before those with real timestamps."""
    ts = datetime(2025, 6, 15, 14, 0, 0)
    markers = [
        _make_marker(object_id="WITH_TS", timestamp=ts),
        _make_marker(object_id="NO_TS", timestamp=None),
    ]
    # The helper sets a default timestamp; override it to None explicitly
    markers[1]["MarkoerDatotid"] = None

    result = _build_alle_markoerer_careplan_dict(patient_id="P400", markers=markers)

    # None timestamp sorts to datetime.min, so it comes first
    assert result["activities"][0]["object_id"] == "NO_TS"
    assert result["activities"][1]["object_id"] == "WITH_TS"
    # period_start comes from first sorted marker (None timestamp)
    assert result["period_start"] is None
    assert result["period_end"] == ts


@pytest.mark.unit
def test_marker_with_missing_optional_fields():
    """Markers with missing keys produce None values in activities."""
    sparse_marker = {
        "OBJECT_ID": "SPARSE1",
        "MarkoerDatotid": datetime(2025, 4, 1),
        # All other fields intentionally missing
    }

    result = _build_alle_markoerer_careplan_dict(patient_id="P500", markers=[sparse_marker])

    assert result["marker_count"] == 1
    activity = result["activities"][0]
    assert activity["object_id"] == "SPARSE1"
    assert activity["marker_code"] is None
    assert activity["marker_code_full"] is None
    assert activity["marker_type"] is None
    assert activity["event_text"] is None
    assert activity["staff_id"] is None
    assert activity["episode_of_care_id"] is None
    assert activity["sor_code"] is None


# ─── Identifier and static fields ────────────────────────────────────


@pytest.mark.unit
@pytest.mark.parametrize(
    "patient_id, expected_identifier",
    [
        ("12345", "12345_alle_markoerer"),
        (99999, "99999_alle_markoerer"),
        ("ABC-DEF", "ABC-DEF_alle_markoerer"),
    ],
)
def test_identifier_format(patient_id, expected_identifier):
    """Identifier is always PATIENT_ID_alle_markoerer."""
    result = _build_alle_markoerer_careplan_dict(
        patient_id=patient_id,
        markers=[_make_marker()],
    )
    assert result["identifier"] == expected_identifier


@pytest.mark.unit
def test_status_always_active():
    """Status is always 'active' regardless of marker content."""
    result = _build_alle_markoerer_careplan_dict(
        patient_id="P600",
        markers=[_make_marker(), _make_marker()],
    )
    assert result["status"] == "active"


@pytest.mark.unit
def test_all_activities_have_completed_status():
    """Every activity has status 'completed'."""
    markers = [
        _make_marker(object_id=f"OBJ{i}", timestamp=datetime(2025, 1, i + 1)) for i in range(5)
    ]

    result = _build_alle_markoerer_careplan_dict(patient_id="P700", markers=markers)

    assert all(a["status"] == "completed" for a in result["activities"])


# ─── Author SOR ──────────────────────────────────────────────────────


@pytest.mark.unit
def test_author_sor_from_earliest_marker():
    """Author SOR comes from the chronologically first marker, not the input order."""
    markers = [
        _make_marker(object_id="LATER", timestamp=datetime(2025, 6, 1), sor_code="SOR_LATER"),
        _make_marker(object_id="EARLIER", timestamp=datetime(2025, 1, 1), sor_code="SOR_EARLIER"),
    ]

    result = _build_alle_markoerer_careplan_dict(patient_id="P800", markers=markers)

    assert result["author_sor"] == "SOR_EARLIER"


# ─── CarePlan ID resolution ──────────────────────────────────────────


@pytest.mark.unit
def test_delta_post_uses_targeted_self_id_resolver():
    """Daily/monthly post path uses a clean targeted self-ID context."""
    careplans = [_make_careplan("P1_alle_markoerer")]
    context = MagicMock()
    context.log = MagicMock()
    fhir_database = MagicMock()
    post_result = MagicMock()

    with (
        patch(
            "DSA.FHIR.CarePlan.CarePlan_IFDB.prepare_resources_for_upsert",
            return_value=careplans,
        ) as resolve_all,
        patch(
            "DSA.FHIR.CarePlan.CarePlan_IFDB.post_resources_direct",
            return_value=post_result,
        ),
    ):
        result = _post_careplans_to_fhir(
            context,
            careplans,
            fhir_database,
            resolve_missing_dependencies=False,
        )

    assert result is post_result
    kwargs = resolve_all.call_args.kwargs
    assert kwargs["resources"] is careplans
    assert kwargs["fhir_database"] is fhir_database
    assert kwargs["resource_type"] == "CarePlan"
    assert kwargs["preferred_id_systems"] == [SYSTEM_CAREPLAN_ALLE_MARKOERER]
    assert kwargs["force_reload"] is True
    assert kwargs["logger"] is context.log


@pytest.mark.unit
def test_batch_post_uses_default_self_id_resolver():
    """Full-load batches keep the preloaded shared-cache resolver path."""
    careplans = [_make_careplan("P1_alle_markoerer")]
    context = MagicMock()
    context.log = MagicMock()
    fhir_database = MagicMock()
    post_result = MagicMock(metadata={"posted_count": 1, "failed_count": 0})

    with (
        patch(
            "DSA.FHIR.CarePlan.CarePlan_IFDB.prepare_resources_for_upsert",
            return_value=careplans,
        ) as resolve_all,
        patch(
            "DSA.FHIR.CarePlan.CarePlan_IFDB.post_resources_direct",
            return_value=post_result,
        ),
    ):
        result = _post_careplans_to_fhir_batch(
            context,
            careplans,
            fhir_database,
            resolve_missing_dependencies=False,
        )

    assert result == {"posted": 1, "failed": 0}
    kwargs = resolve_all.call_args.kwargs
    assert kwargs["resources"] is careplans
    assert kwargs["fhir_database"] is fhir_database
    assert kwargs["resource_type"] == "CarePlan"
    assert kwargs["preferred_id_systems"] == [SYSTEM_CAREPLAN_ALLE_MARKOERER]
    assert kwargs["logger"] is context.log

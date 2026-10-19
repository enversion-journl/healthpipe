# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for the Alle Markoerer CarePlan transform.

The staging functions read ``arc.ifdb_eventmarker`` rows directly, so the tests
feed archive-shaped rows (EVENT_CODE, TIME as the archived string, DELETED) and
pin what the transform derives from them.
"""

import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import dagster as dg
import DSA.FHIR.CarePlan.CarePlan_IFDB_dfm as careplan_module
import pytest
from core.dfm_config import RunModeConfig
from DSA.FHIR.CarePlan.CarePlan_IFDB_dfm import (
    _build_alle_markoerer_careplan_dict,
    _key_careplans_on_fhir_patient,
    _post_careplans_to_fhir,
    _refuse_old_scheme_resources,
    _stage_careplans,
    _transform_careplan_to_fhir,
)
from DSA.FHIR.utils.sql_on_fhir.sof_utils import load_view_definitions
from fhir.resources.R4B.careplan import CarePlan, CarePlanActivity
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.reference import Reference
from utils.fhir.identifier_systems import (
    SYSTEM_CAREPLAN_ALLE_MARKOERER,
    SYSTEM_IFDB_PATIENT_ID,
)


def _make_marker(
    object_id: str = "OBJ1",
    patient_id: str = "P1",
    event_code: str = "SKS.a.admAFV01A1",
    event_text: str = "Henvist til udredning",
    timestamp: str | None = "2025-01-15 10:00:00",
    sor_code: str = "SOR123",
    staff_id: str = "STAFF1",
    episode_of_care_id: str = "EOC1",
    deleted: str = "0",
) -> dict:
    """An arc.ifdb_eventmarker row as the batch fetch returns it."""
    return {
        "OBJECT_ID": object_id,
        "PATIENT_ID": patient_id,
        "EPISODE_OF_CARE_ID": episode_of_care_id,
        "EVENT_CODE": event_code,
        "EVENT_TEXT": event_text,
        "TIME": timestamp,
        "ADMINISTRATIVE_UNIT_SORCODE": sor_code,
        "RESPONSIBLE_STAFF_PERSID": staff_id,
        "DELETED": deleted,
    }


def _logger() -> SimpleNamespace:
    return SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None)


@pytest.mark.unit
@pytest.mark.parametrize("staff_id,expected", [("STAFF1", ["STAFF1"]), (None, [])])
def test_staff_projection_on_actual_builder_output(staff_id, expected):
    """Viewets practitioner_id vælger på identifier-system, ikke på type.

    Byggeren sætter ikke Reference.type på medarbejderen, så projektionen skal
    finde den på urn:source:ifdb:staff. Stien læses fra ViewDefinitionen, og
    udvælgelsen efterlignes i Python uden en FHIRPath-motor.
    """
    data = _build_alle_markoerer_careplan_dict(
        patient_id="syntetisk-patient", markers=[_make_marker(staff_id=staff_id)]
    )
    resource = json.loads(_transform_careplan_to_fhir(data).json())
    view = next(v for v in load_view_definitions() if v["name"] == "careplan_activity_view")
    selection = next(s for s in view["select"] if s.get("forEach") == "activity")
    path = next(c["path"] for c in selection["column"] if c["name"] == "practitioner_id")
    assert path == (
        "detail.performer.where(identifier.system = 'urn:source:ifdb:staff')"
        ".identifier.value.first()"
    )
    performers = resource["activity"][0].get("detail", {}).get("performer", [])
    for performer in performers:
        assert "type" not in performer, "byggeren sætter ikke type; stien må ikke afhænge af den"
    values = [
        p["identifier"]["value"]
        for p in performers
        if p.get("identifier", {}).get("system") == "urn:source:ifdb:staff"
    ]
    assert values[:1] == expected


# ─── Basic happy path ────────────────────────────────────────────────


@pytest.mark.unit
def test_single_marker():
    """Single marker produces correct structure with all fields mapped."""
    ts = datetime(2025, 3, 1, 9, 30, 0)
    marker = _make_marker(
        object_id="OBJ42",
        event_code="SKS.a.admAFV01F",
        event_text="Behandling start",
        timestamp="2025-03-01 09:30:00",
        sor_code="SOR999",
        staff_id="STAFF7",
        episode_of_care_id="EOC55",
    )

    result = _build_alle_markoerer_careplan_dict(patient_id="P100", markers=[marker])

    assert result["patient_id"] == "P100"
    assert result["period_start"] == ts
    assert result["period_end"] == ts  # Single marker → start == end
    assert result["status"] == "active"
    assert result["author_sor"] == "SOR999"
    assert result["patient_rights_type"] == "alle_markoerer"
    assert result["marker_count"] == 1

    activity = result["activities"][0]
    assert activity["object_id"] == "OBJ42"
    # EVENT_CODE is parsed here now — the extract no longer pre-enriches it.
    assert activity["marker_code"] == "AFV01F"
    assert activity["marker_code_full"] == "AFV01F"
    assert activity["marker_type"] == "AFV"
    assert activity["event_text"] == "Behandling start"
    assert activity["timestamp"] == ts
    assert activity["status"] == "completed"
    assert activity["sor_code"] == "SOR999"
    assert activity["staff_id"] == "STAFF7"
    assert activity["episode_of_care_id"] == "EOC55"


@pytest.mark.unit
def test_multiple_markers_sorted_by_timestamp():
    """Markers are sorted chronologically; period spans first to last."""
    markers = [
        _make_marker(object_id="LATE", timestamp="2025-03-01 12:00:00", sor_code="SOR_LATE"),
        _make_marker(object_id="EARLY", timestamp="2025-01-01 08:00:00", sor_code="SOR_EARLY"),
        _make_marker(object_id="MID", timestamp="2025-02-01 10:00:00", sor_code="SOR_MID"),
    ]

    result = _build_alle_markoerer_careplan_dict(patient_id="P200", markers=markers)

    assert result["period_start"] == datetime(2025, 1, 1, 8, 0, 0)
    assert result["period_end"] == datetime(2025, 3, 1, 12, 0, 0)
    assert result["marker_count"] == 3
    assert result["author_sor"] == "SOR_EARLY"  # From the chronologically first
    assert [a["object_id"] for a in result["activities"]] == ["EARLY", "MID", "LATE"]


# ─── Retired markers (the delete-marking contract) ────────────────────


@pytest.mark.unit
def test_retired_markers_are_dropped():
    """DELETED=1 markers never reach the resource."""
    markers = [
        _make_marker(object_id="LIVE", timestamp="2025-01-01 08:00:00"),
        _make_marker(object_id="GONE", timestamp="2025-02-01 08:00:00", deleted="1"),
    ]

    result = _build_alle_markoerer_careplan_dict(patient_id="P250", markers=markers)

    assert [a["object_id"] for a in result["activities"]] == ["LIVE"]
    assert result["marker_count"] == 1
    # The retired marker must not stretch the period either.
    assert result["period_end"] == datetime(2025, 1, 1, 8, 0, 0)


@pytest.mark.unit
def test_deleted_flag_survives_oracle_float_stringification():
    """Oracle NUMBER(1) reaches the archive as NVARCHAR — "1.0" still means deleted."""
    markers = [_make_marker(object_id="GONE", deleted="1.0")]

    result = _build_alle_markoerer_careplan_dict(patient_id="P260", markers=markers)

    assert result["activities"] == []


@pytest.mark.unit
def test_patient_whose_markers_were_all_retired_still_yields_a_careplan():
    """The empty post is what retires the patient's CarePlan_Activity rows.

    Dropping the patient from the batch instead would leave the resource
    untouched in Aidbox, so its ``ts`` would not advance, so the SOF child-grain
    reconcile would never see it — and the stale activity rows would stay live.
    """
    careplans = _stage_careplans(
        ["P310"],
        [_make_marker(object_id="GONE", patient_id="P310", deleted="1")],
        _logger(),
    )

    assert len(careplans) == 1
    assert careplans[0]["activities"] == []

    resource = _transform_careplan_to_fhir(careplans[0])
    assert resource.activity is None
    assert resource.status == "active"


@pytest.mark.unit
def test_stage_emits_one_careplan_per_scoped_patient():
    """Grouping is driven by the scoped patient ids, not by the marker rows."""
    markers = [
        _make_marker(object_id="A", patient_id="P1"),
        _make_marker(object_id="B", patient_id="P1"),
        _make_marker(object_id="C", patient_id="P2"),
    ]

    careplans = _stage_careplans(["P1", "P2", "P3"], markers, _logger())

    assert [cp["patient_id"] for cp in careplans] == ["P1", "P2", "P3"]
    assert [cp["marker_count"] for cp in careplans] == [2, 1, 0]


@pytest.mark.unit
def test_a_reissued_patients_two_ids_stage_as_one_careplan():
    """The bug this grouping exists for.

    IFDB retires a PATIENT_ID and issues a new one for the same CPR. The retired
    id keeps its marker rows — soft-deleted — and the scope query runs with
    include_deleted=True, so BOTH ids land in the batch. One CarePlan per id
    would be two resources racing for the same identifier.
    """
    markers = [_make_marker(object_id="A", patient_id="P_NEW")]

    careplans = _stage_careplans(
        ["P_OLD", "P_NEW"],
        markers,
        _logger(),
        _persons({"P_OLD": ["P_OLD", "P_NEW"]}),
    )

    assert len(careplans) == 1
    assert careplans[0]["marker_count"] == 1
    # Subject is the id that still carries live markers, not the retired one.
    assert careplans[0]["patient_id"] == "P_NEW"


@pytest.mark.unit
def test_siblings_in_separate_batches_stage_the_same_careplan():
    """The silent half of the same bug.

    Two ids for one person need not share a batch. Before the grouping, each
    batch staged its own resource for the person and the later post overwrote
    the earlier — and the retired id's plan is empty, so it could wipe the live
    one's activities. Both batches must now produce the same resource.
    """
    persons = _persons({"P_OLD": ["P_OLD", "P_NEW"]})
    markers = [_make_marker(object_id="A", patient_id="P_NEW")]

    # The retired id alone still widens to its sibling, so the fetch returns the
    # live markers and both batches build the same resource.
    assert set(persons.expand(["P_OLD"])) == {"P_OLD", "P_NEW"}
    retired_batch = _stage_careplans(["P_OLD"], markers, _logger(), persons)
    live_batch = _stage_careplans(["P_NEW"], markers, _logger(), persons)

    assert retired_batch[0]["patient_id"] == live_batch[0]["patient_id"] == "P_NEW"
    assert retired_batch[0]["marker_count"] == live_batch[0]["marker_count"] == 1


@pytest.mark.unit
def test_person_with_every_marker_retired_still_yields_one_empty_careplan():
    """The observed QA case: both ids present, every row deleted.

    The empty post is still what retires the activity rows, so it must survive
    the grouping — but there must be exactly one of it, not one per id.
    """
    careplans = _stage_careplans(
        ["3322139228", "119805505220"],
        [],
        _logger(),
        _persons({"119805505220": ["119805505220", "3322139228"]}),
    )

    assert len(careplans) == 1
    assert careplans[0]["activities"] == []
    assert careplans[0]["patient_id"] == "119805505220"


@pytest.mark.unit
def test_subject_prefers_the_id_that_still_has_markers():
    """Digit strings must order numerically, not lexicographically."""
    assert careplan_module._subject_patient_id("P_KEY", []) == "P_KEY"

    markers = [
        _make_marker(object_id="A", patient_id="9"),
        _make_marker(object_id="B", patient_id="10"),
    ]
    assert careplan_module._subject_patient_id("P_KEY", markers) == "10"


# ─── Edge cases ──────────────────────────────────────────────────────


@pytest.mark.unit
def test_empty_markers():
    """Empty marker list produces empty activities and None fields."""
    result = _build_alle_markoerer_careplan_dict(patient_id="P300", markers=[])

    assert result["patient_id"] == "P300"
    assert result["period_start"] is None
    assert result["period_end"] is None
    assert result["status"] == "active"
    assert result["author_sor"] is None
    assert result["marker_count"] == 0
    assert result["activities"] == []


@pytest.mark.unit
def test_marker_with_none_timestamp_sorted_first():
    """Markers with no TIME sort before those with a real timestamp."""
    markers = [
        _make_marker(object_id="WITH_TS", timestamp="2025-06-15 14:00:00"),
        _make_marker(object_id="NO_TS", timestamp=None),
    ]

    result = _build_alle_markoerer_careplan_dict(patient_id="P400", markers=markers)

    # A missing timestamp sorts as datetime.min, so it comes first
    assert result["activities"][0]["object_id"] == "NO_TS"
    assert result["activities"][1]["object_id"] == "WITH_TS"
    # period_start comes from the first sorted marker (no timestamp)
    assert result["period_start"] is None
    assert result["period_end"] == datetime(2025, 6, 15, 14, 0, 0)


@pytest.mark.unit
def test_marker_with_unparsable_clob_timestamp_is_treated_as_undated():
    """One skewed TIME value must not take the run down.

    ``parse_clob_datetime`` raises on a value that LOOKS like the CLOB format but
    will not parse. Left uncaught, the run dies, the checkpoint never advances,
    and the same row kills the run again every night after.
    """
    markers = [
        _make_marker(object_id="OK", timestamp="2025-06-15 14:00:00"),
        _make_marker(object_id="SKEW", timestamp="32MAY2025:99:99:99"),
    ]

    result = _build_alle_markoerer_careplan_dict(patient_id="P401", markers=markers)

    assert [a["object_id"] for a in result["activities"]] == ["SKEW", "OK"]
    assert result["activities"][0]["timestamp"] is None


@pytest.mark.unit
def test_marker_with_missing_optional_fields():
    """Markers with missing keys produce None values in activities."""
    sparse_marker = {
        "OBJECT_ID": "SPARSE1",
        "PATIENT_ID": "P500",
        "TIME": "2025-04-01 00:00:00",
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


@pytest.mark.unit
def test_unparsable_event_code_falls_back_to_the_raw_value():
    """A code without the SKS.a.adm prefix keeps its raw form as marker_code_full."""
    result = _build_alle_markoerer_careplan_dict(
        patient_id="P550",
        markers=[_make_marker(event_code="NOT_A_SKS_CODE")],
    )

    activity = result["activities"][0]
    assert activity["marker_code"] is None
    assert activity["marker_code_full"] == "NOT_A_SKS_CODE"


# ─── Identifier and static fields ────────────────────────────────────


@pytest.mark.unit
def test_staging_mints_no_identifier():
    """Identity is the person's, and the FHIR patient id is not known yet."""
    result = _build_alle_markoerer_careplan_dict(
        patient_id="12345",
        markers=[_make_marker()],
    )
    assert "identifier" not in result

    resource = _transform_careplan_to_fhir(result)
    assert resource.identifier is None


@pytest.mark.unit
def test_identity_is_keyed_on_the_resolved_fhir_patient():
    """Keyed on the FHIR patient id, not on EVENTMARKER.PATIENT_ID.

    A re-issued IFDB PATIENT_ID resolves to the patient that already exists, so
    the CarePlan keeps its identifier and is updated instead of duplicated.
    """
    careplans = [_make_careplan("P_NEW")]

    kept = _key_careplans_on_fhir_patient(careplans, {"P_NEW": "fhir-abc"}, _logger())

    assert len(kept) == 1
    assert [i.value for i in kept[0].identifier] == ["fhir-abc_alle_markoerer"]
    assert [i.system for i in kept[0].identifier] == [SYSTEM_CAREPLAN_ALLE_MARKOERER]


@pytest.mark.unit
def test_two_ifdb_patients_for_one_person_keep_the_fuller_plan():
    """The residual case: same FHIR patient, no shared CPR to group them on.

    Staging normally merges a re-issued patient's ids into one person, so this
    only fires when arc.ifdb_patient disagrees with what FHIR already bridged.
    Posting both would have them overwrite each other's activity list run after
    run, and the loser is the retired id's empty plan — so keep the fuller one
    rather than fail the whole night over one person. Order must not matter.
    """
    patient_map = {"P_OLD": "fhir-abc", "P_NEW": "fhir-abc"}

    for order in ("retired_first", "live_first"):
        retired = _make_careplan("P_OLD", activity_count=0)
        live = _make_careplan("P_NEW", activity_count=3)
        batch = [retired, live] if order == "retired_first" else [live, retired]

        kept = _key_careplans_on_fhir_patient(batch, patient_map, _logger())

        assert len(kept) == 1, order
        assert len(kept[0].activity or []) == 3, order
        assert [i.value for i in kept[0].identifier] == ["fhir-abc_alle_markoerer"], order


@pytest.mark.unit
def test_careplan_without_resolvable_patient_is_dropped():
    """No subject, nothing to hang the plan on."""
    kept = _key_careplans_on_fhir_patient([_make_careplan("P_GONE")], {}, _logger())
    assert kept == []


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
        _make_marker(object_id=f"OBJ{i}", timestamp=f"2025-01-{i + 1:02d} 00:00:00")
        for i in range(5)
    ]

    result = _build_alle_markoerer_careplan_dict(patient_id="P700", markers=markers)

    assert all(a["status"] == "completed" for a in result["activities"])


@pytest.mark.unit
def test_staff_reference_is_carried_but_not_resolved_to_practitioner():
    """RESPONSIBLE_STAFF_PERSID stays on the resource; nothing tries to resolve it.

    The Practitioner resources being built come from a different source system
    with a different identifier system, so an IFDB staff id can only be linked
    through a crosswalk — not through a reference config. Mapping the system
    would buy a per-batch lookup that always misses; leaving it unmapped makes
    the resolver skip the reference and leave the identifier intact.
    """
    performer_cfg = next(
        c
        for c in careplan_module.CAREPLAN_NESTED_REFERENCE_CONFIGS
        if c.field_path == "activity.detail.performer"
    )
    assert performer_cfg.get_all_target_types() == {"Organization"}
    assert performer_cfg.get_target_type_for_system(careplan_module.SYSTEM_IFDB_STAFF) is None

    careplan = _transform_careplan_to_fhir(
        _build_alle_markoerer_careplan_dict(
            patient_id="P500", markers=[_make_marker(staff_id="STAFF9")]
        )
    )
    systems = [
        p.identifier.system
        for p in careplan.activity[0].detail.performer
        if p.identifier is not None
    ]
    assert careplan_module.SYSTEM_IFDB_STAFF in systems


# ─── Deploy-rækkefølgen ──────────────────────────────────────────────


def _staging_with_stale_rows(stale: int):
    cursor = MagicMock()
    cursor.fetchone.return_value = (stale,)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=conn)
    cm.__exit__ = MagicMock(return_value=False)
    resource = MagicMock()
    resource.get_connection.return_value = cm
    return resource, cursor


@pytest.mark.unit
def test_preflight_passes_once_the_reload_has_run():
    mssql, _ = _staging_with_stale_rows(0)
    _refuse_old_scheme_resources(mssql, _logger())  # no raise


@pytest.mark.unit
def test_preflight_refuses_while_old_scheme_resources_remain():
    """Forgetting the reload fails silently otherwise.

    The lookup would miss every time, so every patient in the window gets a
    SECOND CarePlan while the first is left behind — and a delta window never
    contains both, so nothing complains until the first wide window.
    """
    mssql, _ = _staging_with_stale_rows(1_270_000)
    with pytest.raises(dg.Failure) as excinfo:
        _refuse_old_scheme_resources(mssql, _logger())
    assert "careplan_genkoersel_nulstilling" in str(excinfo.value)


@pytest.mark.unit
def test_preflight_compares_identifier_against_patient_id_on_live_rows():
    """The invariant is exact, not heuristic: under the new identity every live
    row has identifier_value == patient_id + '_alle_markoerer'."""
    mssql, cursor = _staging_with_stale_rows(0)
    _refuse_old_scheme_resources(mssql, _logger())

    sql = " ".join(str(cursor.execute.call_args.args[0]).split())
    assert "[dw_is_deleted] = 0" in sql
    assert "[identifier_value]" in sql and "[patient_id]" in sql
    assert "'_alle_markoerer'" in sql


@pytest.mark.unit
def test_a_refusal_is_logged_as_a_failed_dfm_run():
    """start_dfm_run has already opened a DFMRunLog row when the guard runs.

    The guard is built to raise, so a refusal outside the try would be the
    ordinary path to a row left at *running* until the stale sweep abandons it.
    """
    resource = MagicMock()

    with (
        patch.object(careplan_module, "DfmManager") as dfm_cls,
        patch.object(
            careplan_module,
            "_refuse_old_scheme_resources",
            side_effect=dg.Failure(description="gamle ressourcer tilbage"),
        ),
    ):
        dfm = dfm_cls.return_value
        dfm.start_dfm_run.return_value = SimpleNamespace(log_id=42, checkpoint=None)
        context = dg.build_asset_context(
            resources={"mssql_target_staging": resource, "fhir_database": resource}
        )
        with pytest.raises(dg.Failure):
            careplan_module.fhir_server_careplan_dfm(context, config=RunModeConfig())

    dfm.fail_dfm_run.assert_called_once()
    assert dfm.fail_dfm_run.call_args.args[0] == 42


# ─── Person-gruppering ───────────────────────────────────────────────


def _persons(ids_by_key):
    """PersonGrouping from {person_key: [patient_ids]}."""
    return careplan_module.PersonGrouping(
        key_by_pid={pid: key for key, ids in ids_by_key.items() for pid in ids},
        ids_by_key=ids_by_key,
    )


def _patient_archive(rows):
    """MSSQL fake returning (PATIENT_ID, PERSON_KEY) rows from the patient archive."""
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    conn = MagicMock()
    conn.cursor.return_value = cursor
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=conn)
    cm.__exit__ = MagicMock(return_value=False)
    resource = MagicMock()
    resource.get_connection.return_value = cm
    return resource, cursor


@pytest.mark.unit
def test_person_keys_group_ids_sharing_a_cpr():
    mssql, cursor = _patient_archive(
        [("119805505220", "119805505220"), ("3322139228", "119805505220")]
    )

    persons = careplan_module._person_keys_by_patient_id(mssql, _logger())

    assert persons.key_by_pid == {
        "119805505220": "119805505220",
        "3322139228": "119805505220",
    }
    assert persons.ids_by_key == {"119805505220": ["119805505220", "3322139228"]}
    # The widening the marker fetch runs on: either id pulls in the other.
    # Order is irrelevant — it becomes an IN clause.
    assert set(persons.expand(["3322139228"])) == {"3322139228", "119805505220"}
    sql = " ".join(str(cursor.execute.call_args.args[0]).split())
    # Only multi-id CPRs are read back — the whole archive would be a pointless
    # scan, and the key must be MIN over the GROUP, not over a batch.
    assert "HAVING COUNT(DISTINCT [PATIENT_ID]) > 1" in sql
    assert "MIN(s.[PATIENT_ID]) OVER (PARTITION BY s.[CPR])" in sql


@pytest.mark.unit
def test_a_sentinel_cpr_does_not_fuse_unrelated_patients():
    """A CPR shared by dozens of ids is a placeholder, not a person.

    Merging them would put unrelated patients in one CarePlan — worse than the
    duplicate this map exists to prevent — so the group is dropped and logged.
    """
    oversized = [(str(i), "0") for i in range(careplan_module._MAX_IDS_PER_PERSON + 1)]
    mssql, _ = _patient_archive(oversized)

    assert careplan_module._person_keys_by_patient_id(mssql, _logger()).key_by_pid == {}

    ok = [(str(i), "0") for i in range(careplan_module._MAX_IDS_PER_PERSON)]
    mssql, _ = _patient_archive(ok)
    grouped = careplan_module._person_keys_by_patient_id(mssql, _logger())
    assert len(grouped.key_by_pid) == len(ok)


# ─── Chunking ────────────────────────────────────────────────────────


@pytest.mark.unit
def test_careplan_chunk_stage_default_limits_reference_fanout():
    # Default deliberately raised 1_000 -> 5_000 ("Stabilize CarePlan monthly
    # execution"). On the DFM path the unit is patients per batch, which is the
    # same thing: one CarePlan per patient.
    assert careplan_module.CAREPLAN_CHUNK_STAGE.chunk_size == 5_000
    assert careplan_module.CAREPLAN_CHUNK_STAGE.cleanup_resource_types == frozenset({"CarePlan"})


# ─── CarePlan ID resolution ──────────────────────────────────────────


def _make_careplan(ifdb_patient_id: str, activity_count: int = 1) -> CarePlan:
    """A CarePlan as the transform leaves it: subject carries the IFDB
    PATIENT_ID and the business identifier is not stamped yet."""
    return CarePlan(
        status="active",
        intent="plan",
        subject=Reference(
            identifier=Identifier(
                system=SYSTEM_IFDB_PATIENT_ID,
                value=ifdb_patient_id,
            )
        ),
        activity=[CarePlanActivity() for _ in range(activity_count)] or None,
    )


@pytest.mark.unit
def test_post_upserts_on_the_alle_markoerer_identifier_only():
    """Self-ID lookup is keyed on this source's system, so it can never match
    (and overwrite) a CarePlan posted by another source."""
    careplans = [_make_careplan("P1")]
    context = MagicMock()
    context.log = MagicMock()
    fhir_database = MagicMock()
    post_result = MagicMock()

    with (
        patch(
            "DSA.FHIR.CarePlan.CarePlan_IFDB_dfm.prepare_resources_for_upsert",
            return_value=careplans,
        ) as resolve_all,
        patch(
            "DSA.FHIR.CarePlan.CarePlan_IFDB_dfm.post_resources_direct",
            return_value=post_result,
        ),
        patch(
            "DSA.FHIR.utils.ifdb.dependency_resolver.resolve_careplan_dependencies",
            return_value={"P1": "fhir-1"},
        ) as resolve_deps,
    ):
        result = _post_careplans_to_fhir(
            context,
            careplans,
            fhir_database,
            mssql=MagicMock(),
        )

    assert result is post_result
    resolve_deps.assert_called_once()
    kwargs = resolve_all.call_args.kwargs
    # The batch handed on is keyed on the resolved FHIR patient, not on the
    # IFDB PATIENT_ID the subject arrived with.
    assert [i.value for i in kwargs["resources"][0].identifier] == ["fhir-1_alle_markoerer"]
    assert kwargs["fhir_database"] is fhir_database
    assert kwargs["resource_type"] == "CarePlan"
    assert kwargs["preferred_id_systems"] == [SYSTEM_CAREPLAN_ALLE_MARKOERER]
    assert kwargs["logger"] is context.log
    # The identifier cache is preloaded once per run by the asset, so batches
    # must NOT force a reload — that would refetch it for every batch.
    assert "force_reload" not in kwargs

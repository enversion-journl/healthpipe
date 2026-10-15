# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR CarePlan assets for Alle Markoerer (All Markers), using DFM.

Single-asset pattern (mirrors ``ServiceRequest_IFDB_RehabilitationPlan_dfm``):
read ``arc.ifdb_eventmarker``, transform to FHIR CarePlan, post to the FHIR
server. Replaces the partitioned two-asset pipeline that read IFDB Oracle
directly.

Grain: one CarePlan per PERSON holding ALL that person's markers, no filtering
or grouping by marker type. Because the resource spans every marker a patient
has, the delta cannot be "the changed rows" — it is "every marker for the
patients whose markers changed":

1. scope query — DISTINCT PATIENT_ID from the archive delta window;
2. per patient batch — fetch ALL of that patient's live markers, not just the
   ones that changed;
3. group by patient, transform, post.

Identity
--------
The business identifier is ``<FHIR patient id>_alle_markoerer``, stamped by
``_key_careplans_on_fhir_patient`` once the dependency resolver has resolved the
subject — not built from ``EVENTMARKER.PATIENT_ID`` at staging time.

That distinction is the whole point. IFDB retires a PATIENT_ID and issues a new
one for the same CPR; keyed on the IFDB id, the person got a second CarePlan and
the first became unreachable to the pipeline, leaving two live resources whose
activity rows collide in the SOF child MERGE on any full load. Keyed on the FHIR
patient id — which the resolver's CPR bridge makes the same for both IFDB ids —
the re-issue is an update of the one resource, and the UNIQUE partial index on
``extract_identifier_value(resource, SYSTEM_CAREPLAN_ALLE_MARKOERER)`` makes the
duplicate unwritable rather than merely unlikely.

Two IFDB patients for one person DO both reach the scope query. A retired
PATIENT_ID keeps its marker rows in the archive — soft-deleted, but the scope
runs with ``include_deleted=True`` (see below), so the old id comes back with
the new one. The batch unit is therefore the person, not the id: ids sharing a
CPR in ``arc.ifdb_patient`` are grouped onto one key before the markers are
fetched, so one person yields one CarePlan whichever batch each id lands in.
Keyed on the id instead, the two resources would race for the same identifier,
and the retired id's plan — empty, because all its rows are deleted — would
wipe the live one whenever it posted last.

Retired markers
---------------
The archive merge maps ``EVENTMARKER.DELETED`` onto ``dw_is_deleted``
(``source_delete_column`` on ``EVENTMARKER_ARCHIVE_CONFIG``), so a marker retired
upstream sits in the archive with ``dw_is_deleted = 1``.

The scope query therefore runs with ``include_deleted=True``, and that is
load-bearing rather than incidental: a retirement's only trace in the window IS
the row that just went ``dw_is_deleted = 1``, so filtering it out would leave the
patient outside the scope and the removal would never be published at all.

The per-patient fetch then keeps only live rows, so the posted resource holds
only live markers — and a patient whose last marker was retired still gets a
CarePlan posted, with an empty activity list. That empty post is load-bearing
too: it advances the resource's Aidbox ``ts``, which is what puts the patient
into the SOF child-grain reconcile window so its stale ``fhir.CarePlan_Activity``
rows are marked deleted (see ``careplan_activity_view`` in ``sof_utils``).
"""

from collections import defaultdict
from datetime import datetime
from typing import Any, NamedTuple

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.parallel import ChunkedPipelineStage
from core.parallel.chunked_pipeline import _read_int_metadata
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.DocumentReference.document_reference_common import advance_fhir_checkpoint
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.fhir_utils import fhir_datetime
from DSA.FHIR.utils.ifdb.fhir_posting import (
    post_resources_direct,
    prepare_resources_for_upsert,
)
from DSA.FHIR.utils.ifdb.patient_cpr_fallback import build_patient_cpr_fallback
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.ifdb.transforms import (
    is_true_flag,
    normalize_oracle_id,
    parse_clob_datetime,
)
from DSA.FHIR.utils.reference_resolver import (
    NestedReferenceFieldConfig,
    ReferenceFieldConfig,
    ResolveValuesFn,
)
from fhir.resources.R4B.careplan import (
    CarePlan,
    CarePlanActivity,
    CarePlanActivityDetail,
)
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference
from utils.eventmarker_utils import parse_event_code
from utils.fhir.fhir_extensions import (
    EXTENSION_ACTIVITY_EPISODE_OF_CARE,
    EXTENSION_ACTIVITY_IDENTIFIER,
)
from utils.fhir.identifier_systems import (
    SYSTEM_CAREPLAN_ALLE_MARKOERER,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_EVENTMARKER,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_STAFF,
    SYSTEM_PATIENT_RIGHTS_TYPE,
    SYSTEM_SKS,
    SYSTEM_SOR,
)

ARCHIVE_TABLE = "arc.ifdb_eventmarker"

# One CarePlan holds all of a patient's markers, so a patient IS the chunk unit:
# row-level chunking would split the semantic grain and post several CarePlans
# for the same patient. chunk_size therefore counts patients per batch, which is
# also the number of CarePlans transformed and posted per batch.
CAREPLAN_CHUNK_STAGE = ChunkedPipelineStage.from_env(
    stage_id="ifdb.careplan.alle_markoerer.transform_post",
    env_prefix="IFDB_CAREPLAN_PIPELINE",
    default_chunk_size=5_000,
    cleanup_resource_types={"CarePlan"},
)

# MSSQL caps a statement at 2100 parameters; 999 leaves room and matches the
# chunking used by the other archive child-row fetches.
_IN_CLAUSE_CHUNK = 999

# The patient archive, read to group re-issued PATIENT_IDs onto one person.
_PATIENT_ARCHIVE_SCHEMA = "arc"
_PATIENT_ARCHIVE_TABLE = "ifdb_patient"

# Above this, a shared CPR is a sentinel value rather than a person.
_MAX_IDS_PER_PERSON = 10

# Only the columns the transform reads. The archive holds all 17 source columns,
# but the covering index on PATIENT_ID includes exactly this set, so widening
# the projection would turn every batch fetch into a clustered-index lookup.
_MARKER_COLUMNS = (
    "OBJECT_ID",
    "PATIENT_ID",
    "EPISODE_OF_CARE_ID",
    "EVENT_CODE",
    "EVENT_TEXT",
    "TIME",
    "ADMINISTRATIVE_UNIT_SORCODE",
    "RESPONSIBLE_STAFF_PERSID",
    "DELETED",
)


# =============================================================================
# Archive reads
# =============================================================================


class PersonGrouping(NamedTuple):
    """Re-issued PATIENT_IDs seen as people.

    ``key_by_pid`` maps each id of a re-issued patient onto one key;
    ``ids_by_key`` is its inverse, used to widen a batch from the ids that were
    scoped to every id the person has. Ids absent from both are their own
    person, so a plain ``.get(pid, pid)`` is always the right lookup.
    """

    key_by_pid: dict[str, str]
    ids_by_key: dict[str, list[str]]

    def person_of(self, patient_id: str) -> str:
        return self.key_by_pid.get(patient_id, patient_id)

    def expand(self, patient_ids: list[str]) -> list[str]:
        """Every PATIENT_ID belonging to the people this batch scoped.

        The marker fetch has to run on this, not on the scoped ids. A batch that
        caught only the retired id would otherwise find no live markers for it
        and post an empty CarePlan for the person — wiping the live sibling's
        activities, because both write the same identifier.
        """
        widened: list[str] = []
        seen: set[str] = set()
        for pid in patient_ids:
            for sibling in self.ids_by_key.get(self.person_of(pid), [pid]):
                if sibling not in seen:
                    seen.add(sibling)
                    widened.append(sibling)
        return widened


def _person_keys_by_patient_id(
    mssql: MSSQLResource,
    logger: dg.DagsterLogManager,
) -> PersonGrouping:
    """Map every re-issued ``PATIENT_ID`` to one stable key for its person.

    IFDB retires a PATIENT_ID and issues a new one for the same CPR. The retired
    id KEEPS its marker rows in the archive — soft-deleted, but the scope query
    runs with ``include_deleted=True``, so those rows still put the old id in a
    batch. Both ids then name the same person, and one CarePlan per id means two
    resources racing for the same identifier: whichever posts last wins, and the
    retired id's plan is empty, so it can wipe the live one's activities.

    The key is the lowest PATIENT_ID in the CPR group, computed over the WHOLE
    group rather than per batch, so two siblings that land in different batches
    still agree on it and stage the same resource. Ids that share a CPR with
    nobody are absent from the map and stay their own person.

    Read once per run: only CPRs carrying more than one PATIENT_ID are returned,
    which is a small set next to the patient archive, and a per-batch lookup
    would re-scan that archive for every batch.
    """
    sql = f"""
        SELECT s.[PATIENT_ID],
               MIN(s.[PATIENT_ID]) OVER (PARTITION BY s.[CPR]) AS [PERSON_KEY]
        FROM [{_PATIENT_ARCHIVE_SCHEMA}].[{_PATIENT_ARCHIVE_TABLE}] s
        WHERE s.[CPR] IN (
            SELECT [CPR]
            FROM [{_PATIENT_ARCHIVE_SCHEMA}].[{_PATIENT_ARCHIVE_TABLE}]
            WHERE [CPR] IS NOT NULL AND [CPR] <> ''
            GROUP BY [CPR]
            HAVING COUNT(DISTINCT [PATIENT_ID]) > 1
        )
    """
    groups: dict[str, list[str]] = defaultdict(list)
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            for patient_id, person_key in cursor.fetchall():
                pid = normalize_oracle_id(patient_id)
                key = normalize_oracle_id(person_key)
                if pid and key and pid not in groups[key]:
                    groups[key].append(pid)
        finally:
            cursor.close()

    # A CPR shared by dozens of PATIENT_IDs is a sentinel value, not a person.
    # Merging those would fuse unrelated patients into one CarePlan, which is a
    # worse failure than the one this map exists to prevent — leave them alone
    # and say so.
    oversized = {key: ids for key, ids in groups.items() if len(ids) > _MAX_IDS_PER_PERSON}
    for key, ids in oversized.items():
        logger.warning(
            f"CPR group {key} holds {len(ids)} PATIENT_IDs — treating them as separate "
            "people. A CPR that wide is a sentinel value in arc.ifdb_patient, not a "
            "person, and merging them would fuse unrelated patients into one CarePlan."
        )
        del groups[key]

    key_by_pid = {pid: key for key, ids in groups.items() for pid in ids}
    if key_by_pid:
        logger.info(
            f"{len(key_by_pid):,} PATIENT_IDs across {len(groups):,} re-issued people "
            "will be staged per person, not per id"
        )
    return PersonGrouping(key_by_pid=key_by_pid, ids_by_key=dict(groups))


def _subject_patient_id(person_key: str, markers: list[dict[str, Any]]) -> str:
    """The PATIENT_ID a person's CarePlan hangs its subject reference on.

    Prefer an id that still carries live markers: that is the one IFDB writes to
    today, and the one most likely to exist as a Patient resource. With every
    marker retired there is nothing to prefer, so the person key does the job —
    it is itself a PATIENT_ID and it is stable across batches.
    """
    live_ids = {
        pid for m in markers if (pid := normalize_oracle_id(m.get("PATIENT_ID"))) is not None
    }
    if not live_ids:
        return person_key
    # Digit strings: order by length first so 9 does not sort above 10.
    return max(live_ids, key=lambda pid: (len(pid), pid))


def _fetch_markers_for_patients(
    mssql: MSSQLResource,
    patient_ids: list[str],
) -> list[dict[str, Any]]:
    """Fetch the live archived markers for a batch of patients.

    ``dw_is_deleted`` is the archive's retirement flag — the merge derives it from
    ``EVENTMARKER.DELETED`` — so this is where retired markers are dropped.
    ``DELETED`` is still projected and re-checked when the activities are built:
    the two disagree only if a row reached the archive without that mapping (a
    bulk cold-load), and a stale activity is worse than a missing one.
    """
    if not patient_ids:
        return []

    col_sql = ", ".join(f"[{c}]" for c in _MARKER_COLUMNS)
    rows: list[dict[str, Any]] = []

    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for start in range(0, len(patient_ids), _IN_CLAUSE_CHUNK):
                chunk = patient_ids[start : start + _IN_CLAUSE_CHUNK]
                placeholders = ", ".join("?" for _ in chunk)
                cursor.execute(
                    f"SELECT {col_sql} FROM [arc].[ifdb_eventmarker] "
                    f"WHERE [dw_is_deleted] = 0 AND [PATIENT_ID] IN ({placeholders})",
                    chunk,
                )
                columns = [desc[0] for desc in cursor.description]
                rows.extend(dict(zip(columns, row, strict=False)) for row in cursor.fetchall())
        finally:
            cursor.close()

    return rows


# =============================================================================
# CarePlan Building Functions
# =============================================================================


def _marker_timestamp(raw: Any) -> datetime | None:
    """Parse an archived EVENTMARKER.TIME value to a datetime.

    The archive stores the Oracle DATE as its stringified form, so this is
    normally ``datetime.fromisoformat``; the CLOB branch covers values that were
    archived through a text path. Sorting must not fall back to string order —
    the two formats do not sort alike.
    """
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    text = str(raw).strip()
    if not text:
        return None
    try:
        parsed = parse_clob_datetime(text)
    except ValueError:
        # parse_clob_datetime raises when a value LOOKS like the CLOB format but
        # will not parse. One such row must not take the whole run down: the
        # checkpoint would then never advance and the same row would fail again
        # every night. Sorting treats it as undated instead.
        return None
    if parsed:
        return parsed
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _build_activities(markers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Enrich and sort the live markers of one patient into activity dicts.

    The ``DELETED`` check is the second of the two retirement guards described in
    ``_fetch_markers_for_patients`` — the archive's ``dw_is_deleted`` filter has
    normally already removed these rows.
    """
    live: list[tuple[datetime, dict[str, Any]]] = []
    for marker in markers:
        if is_true_flag(marker.get("DELETED")):
            continue
        timestamp = _marker_timestamp(marker.get("TIME"))
        parsed = parse_event_code(marker.get("EVENT_CODE"))
        live.append(
            (
                timestamp or datetime.min,
                {
                    "object_id": normalize_oracle_id(marker.get("OBJECT_ID")),
                    "marker_code": parsed["marker_code"],
                    "marker_code_full": parsed["marker_code_full"] or marker.get("EVENT_CODE"),
                    "marker_type": parsed["marker_type"],
                    "event_text": marker.get("EVENT_TEXT"),
                    "timestamp": timestamp,
                    "status": "completed",
                    "sor_code": marker.get("ADMINISTRATIVE_UNIT_SORCODE"),
                    "staff_id": marker.get("RESPONSIBLE_STAFF_PERSID"),
                    "episode_of_care_id": normalize_oracle_id(marker.get("EPISODE_OF_CARE_ID")),
                },
            )
        )

    live.sort(key=lambda pair: pair[0])
    return [activity for _, activity in live]


def _build_alle_markoerer_careplan_dict(
    patient_id: Any,
    markers: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build an Alle Markoerer CarePlan dict for a single patient.

    ``markers`` may contain only retired rows (or none at all) — the result is
    then a CarePlan with no activities, which is what retires the patient's
    activity rows downstream.
    """
    activities = _build_activities(markers)

    period_start = activities[0]["timestamp"] if activities else None
    period_end = activities[-1]["timestamp"] if activities else None
    author_sor = activities[0].get("sor_code") if activities else None

    return {
        # No identifier here on purpose: the resource is keyed on the *person*,
        # and the FHIR patient id is not known until the dependency resolver has
        # run. _key_careplans_on_fhir_patient() stamps it before the upsert.
        "patient_id": patient_id,
        "period_start": period_start,
        "period_end": period_end,
        # Alle markoerer is always "active" — it is a living document, and it
        # keeps existing for the patient even once every marker is retired.
        "status": "active",
        "author_sor": author_sor,
        "patient_rights_type": "alle_markoerer",
        "marker_count": len(activities),
        "activities": activities,
    }


def _stage_careplans(
    patient_ids: list[str],
    markers: list[dict[str, Any]],
    logger: dg.DagsterLogManager,
    persons: PersonGrouping | None = None,
) -> list[dict[str, Any]]:
    """Group markers by PERSON — one CarePlan per person in the batch.

    The grain is the person, not ``EVENTMARKER.PATIENT_ID``. A re-issued patient
    has two ids in the archive, both reach the scope query (the retired one keeps
    its soft-deleted rows), and one CarePlan per id would mean two resources
    overwriting each other on the same identifier. ``persons`` maps the ids
    of such a person onto one key — see :func:`_person_keys_by_patient_id`;
    ids absent from it are their own person.

    Driven by ``patient_ids``, not by the marker rows: a person whose markers
    were all retired has no rows left to group, and would silently drop out of
    the batch if the grouping keyed on the markers alone.
    """
    persons = persons or PersonGrouping({}, {})
    person_of = persons.person_of

    by_person: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for marker in markers:
        patient_id = normalize_oracle_id(marker.get("PATIENT_ID"))
        if patient_id:
            by_person[person_of(patient_id)].append(marker)

    # First-seen order, so the batch log reads in scope order and a person whose
    # two ids are both in the batch is staged once.
    person_keys: list[str] = []
    seen: set[str] = set()
    for patient_id in patient_ids:
        key = person_of(patient_id)
        if key not in seen:
            seen.add(key)
            person_keys.append(key)

    careplans = [
        _build_alle_markoerer_careplan_dict(
            _subject_patient_id(key, by_person.get(key, [])),
            by_person.get(key, []),
        )
        for key in person_keys
    ]

    total_activities = sum(cp["marker_count"] for cp in careplans)
    emptied = sum(1 for cp in careplans if cp["marker_count"] == 0)
    merged = len(patient_ids) - len(person_keys)
    merged_note = f", {merged} id(s) merged into a sibling" if merged else ""
    logger.info(
        f"Staged {len(careplans)} CarePlans with {total_activities} activities "
        f"({emptied} with no live markers left{merged_note})"
    )
    return careplans


# =============================================================================
# FHIR Transformation
# =============================================================================


def _build_activity(activity: dict[str, Any]) -> CarePlanActivity:
    """Build a FHIR CarePlanActivity from activity dict."""
    marker_code = activity.get("marker_code")
    marker_type = activity.get("marker_type")
    timestamp = activity.get("timestamp")
    activity_status = activity.get("status", "completed")

    # Extensions for activity identification
    extensions: list[Extension] = []

    # Add source identifier (OBJECT_ID)
    object_id = activity.get("object_id")
    if object_id:
        extensions.append(
            Extension(
                url=EXTENSION_ACTIVITY_IDENTIFIER,
                valueIdentifier=Identifier(
                    system=SYSTEM_IFDB_EVENTMARKER,
                    value=str(object_id),
                ),
            )
        )

    # Add EpisodeOfCare reference
    episode_of_care_id = activity.get("episode_of_care_id")
    if episode_of_care_id:
        extensions.append(
            Extension(
                url=EXTENSION_ACTIVITY_EPISODE_OF_CARE,
                valueReference=Reference(
                    identifier=Identifier(
                        system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                        value=str(episode_of_care_id),
                    )
                ),
            )
        )

    # Outcome code (marker code with type)
    outcome_code = None
    if marker_code:
        display = f"{marker_code}"
        if marker_type:
            display = f"{marker_type}: {marker_code}"
        outcome_code = CodeableConcept(
            coding=[
                Coding(
                    system=SYSTEM_SKS,
                    code=marker_code,
                    display=display,
                )
            ]
        )

    # Scheduled period (timestamp)
    scheduled_period = None
    if timestamp:
        scheduled_period = Period(
            start=fhir_datetime(timestamp),
            end=fhir_datetime(timestamp),
        )

    # Performers (Organization from SOR, Practitioner from staff)
    performers: list[Reference] = []
    sor_code = activity.get("sor_code")
    if sor_code:
        performers.append(
            Reference(
                identifier=Identifier(
                    system=SYSTEM_SOR,
                    value=str(sor_code),
                )
            )
        )
    staff_id = activity.get("staff_id")
    if staff_id:
        performers.append(
            Reference(
                identifier=Identifier(
                    system=SYSTEM_IFDB_STAFF,
                    value=str(staff_id),
                )
            )
        )

    # Event text
    event_text = activity.get("event_text")

    # Build activity detail
    detail = CarePlanActivityDetail(
        status=activity_status,
        scheduledPeriod=scheduled_period,
        performer=performers if performers else None,
        description=event_text,
    )

    return CarePlanActivity(
        extension=extensions if extensions else None,
        outcomeCodeableConcept=[outcome_code] if outcome_code else None,
        detail=detail,
    )


def _transform_careplan_to_fhir(careplan_data: dict[str, Any]) -> CarePlan:
    """Transform a staged CarePlan dict to a FHIR CarePlan resource.

    The business identifier is deliberately left unset — see
    :func:`_key_careplans_on_fhir_patient`, which stamps it once the subject's
    FHIR patient id is known.
    """
    # Subject (Patient reference via identifier - will be resolved later)
    patient_id = careplan_data.get("patient_id")
    subject = Reference(
        identifier=Identifier(
            system=SYSTEM_IFDB_PATIENT_ID,
            value=str(patient_id),
        )
    )

    # Status and Intent
    status = careplan_data.get("status", "active")
    intent = "plan"

    # Category - patient rights type
    patient_rights_type = careplan_data.get("patient_rights_type", "alle_markoerer")

    categories = [
        CodeableConcept(
            coding=[
                Coding(
                    system=SYSTEM_PATIENT_RIGHTS_TYPE,
                    code=patient_rights_type,
                    display="Alle markoerer",
                )
            ]
        )
    ]

    # Period
    period_start = careplan_data.get("period_start")
    period_end = careplan_data.get("period_end")
    period = None
    if period_start or period_end:
        period = Period(
            start=fhir_datetime(period_start) if period_start else None,
            end=fhir_datetime(period_end) if period_end else None,
        )

    # Author (Organization via SOR)
    author_sor = careplan_data.get("author_sor")
    author = None
    if author_sor:
        author = Reference(
            identifier=Identifier(
                system=SYSTEM_SOR,
                value=str(author_sor),
            )
        )

    # Activities. A patient whose markers were all retired posts with
    # activity absent — see the module docstring on why that post still happens.
    activities = [_build_activity(a) for a in careplan_data.get("activities", [])]

    # Build CarePlan
    return CarePlan(
        status=status,
        intent=intent,
        category=categories,
        subject=subject,
        period=period,
        author=author,
        activity=activities if activities else None,
    )


def _transform_careplans_to_fhir(
    careplans_data: list[dict[str, Any]],
    context: dg.AssetExecutionContext,
) -> list[CarePlan]:
    """Transform list of staged CarePlan dicts to FHIR CarePlan resources."""
    careplans = []

    for cp_data in careplans_data:
        try:
            careplan = _transform_careplan_to_fhir(cp_data)
            careplans.append(careplan)
        except Exception as e:
            context.log.warning(
                f"Failed to transform CarePlan for patient {cp_data.get('patient_id')}: {e}"
            )

    return careplans


# =============================================================================
# Reference Resolution Configuration
# =============================================================================

# Top-level reference fields
CAREPLAN_REFERENCE_CONFIGS = [
    # Patient reference (REQUIRED) - field is "subject" for CarePlan
    ReferenceFieldConfig(
        field_name="subject",
        target_resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        is_required=True,
        description="Patient reference from IFDB PATIENT_ID",
    ),
    # Author (Organization via SOR) - OPTIONAL
    ReferenceFieldConfig(
        field_name="author",
        target_resource_type="Organization",
        identifier_system=SYSTEM_SOR,
        is_required=False,
        description="Author organization from SOR code",
    ),
]

# Nested reference fields
CAREPLAN_NESTED_REFERENCE_CONFIGS = [
    # Activity performer (Organization via SOR) - OPTIONAL
    #
    # SYSTEM_IFDB_STAFF is deliberately NOT mapped to Practitioner. The staff
    # reference still rides along on the resource as a bare
    # Identifier(system=urn:source:ifdb:staff) — an unmapped system is skipped by
    # the resolver, not dropped — so RESPONSIBLE_STAFF_PERSID is preserved for
    # whoever can use it later. Mapping it would only buy a lookup that always
    # misses: the Practitioner resources being built come from a different source
    # system and carry a different identifier system, so linking them to an IFDB
    # staff id needs a crosswalk, which a reference config cannot express.
    NestedReferenceFieldConfig(
        field_path="activity.detail.performer",
        identifier_system_to_type={
            SYSTEM_SOR: "Organization",
        },
        is_required=False,
        description="Activity performer (Organization from SOR)",
    ),
    # Activity EpisodeOfCare extension reference - OPTIONAL
    NestedReferenceFieldConfig(
        field_path="activity.extension.valueReference",
        identifier_system_to_type={
            SYSTEM_IFDB_EPISODE_OF_CARE_ID: "EpisodeOfCare",
        },
        is_required=False,
        description="Activity EpisodeOfCare reference from EVENTMARKER.EPISODE_OF_CARE_ID",
    ),
]


# =============================================================================
# Post Functions
# =============================================================================


# Under den nye identitet gælder identifier_value == patient_id +
# '_alle_markoerer' for hver levende række. Gamle ressourcer bærer
# '<IFDB PATIENT_ID>_alle_markoerer' og bryder ligheden, så én tælling afgør om
# nulstillingen er kørt. Ingen heuristik, og efter genindlæsningen altid nul.
_OLD_SCHEME_ROWS_SQL = """
SELECT COUNT(*) FROM [fhir].[CarePlan]
WHERE [dw_is_deleted] = 0
  AND COALESCE([identifier_value], '') <> COALESCE([patient_id], '') + '_alle_markoerer'
"""


def _refuse_old_scheme_resources(mssql: MSSQLResource, logger: dg.DagsterLogManager) -> None:
    """Stop before posting when the identity reload has not been run.

    The producer now looks resources up on '<FHIR patient id>_alle_markoerer'.
    Against a server still holding the IFDB-keyed ones, that lookup misses every
    time and every patient in the window gets a SECOND CarePlan while the first
    is left behind — silently, because a delta window never contains both and
    the child MERGE just repoints its row. The damage only surfaces later, as a
    duplicate per patient and 8672 on the first wide window.

    So this refuses rather than warns: the operator has to run
    careplan_genkoersel_nulstilling and the full load first. The guard is
    self-neutralising — once the reload is done the count is zero forever.
    """
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(_OLD_SCHEME_ROWS_SQL)
            stale = cursor.fetchone()[0]
        finally:
            cursor.close()

    if not stale:
        return

    raise dg.Failure(
        description=(
            f"{stale:,} live rows in fhir.CarePlan still carry the old "
            "IFDB-keyed identifier. The CarePlan identity moved to the FHIR "
            "patient id, so posting now would create a second resource for "
            "every patient in the window instead of updating the existing one. "
            "Run careplan_genkoersel_nulstilling (dry_run first), then "
            "fhir_server_careplan_dfm -> sof_careplan_dfm -> "
            "sof_careplan_activity_dfm -> the DSO sync in full_load."
        )
    )


def _subject_ifdb_patient_id(careplan: CarePlan) -> str | None:
    """Read the IFDB PATIENT_ID off a CarePlan's still-unresolved subject."""
    subject = getattr(careplan, "subject", None)
    identifier = getattr(subject, "identifier", None) if subject else None
    if identifier and getattr(identifier, "system", None) == SYSTEM_IFDB_PATIENT_ID:
        value = getattr(identifier, "value", None)
        return str(value) if value else None
    return None


def _key_careplans_on_fhir_patient(
    careplans: list[CarePlan],
    patient_map: dict[str, str],
    logger: dg.DagsterLogManager,
) -> list[CarePlan]:
    """Give each CarePlan the identity of its *person*, not of its IFDB record.

    Alle Markoerer is one living document per person, so keying it on
    EVENTMARKER's PATIENT_ID was a grain mismatch: IFDB retires a PATIENT_ID and
    issues a new one for the same CPR, and the old key then names a resource
    that nothing in the pipeline will ever touch again. Both PATIENT_IDs resolve
    to the same FHIR patient — the dependency resolver's CPR bridge appends the
    new id to the patient that already exists — so keying on the FHIR patient id
    turns a re-issue into an update of the one CarePlan.

    That also moves the invariant into the database. The UNIQUE partial index on
    ``extract_identifier_value(resource, SYSTEM_CAREPLAN_ALLE_MARKOERER)`` now
    indexes the patient id, so a second live CarePlan for one person cannot be
    written at all — including by a full load, which is where the previous
    scheme failed the child MERGE with two rows per marker.

    A CarePlan whose patient does not resolve is dropped: without a subject it
    has nothing to hang on.
    """
    kept: dict[str, CarePlan] = {}
    unresolved = 0

    for careplan in careplans:
        ifdb_patient_id = _subject_ifdb_patient_id(careplan)
        fhir_patient_id = patient_map.get(ifdb_patient_id) if ifdb_patient_id else None
        if not fhir_patient_id:
            unresolved += 1
            continue

        careplan.identifier = [
            Identifier(
                system=SYSTEM_CAREPLAN_ALLE_MARKOERER,
                value=f"{fhir_patient_id}_alle_markoerer",
            )
        ]

        existing = kept.get(fhir_patient_id)
        if existing is not None:
            # Staging groups a re-issued patient's ids onto one person before we
            # get here (see _person_keys_by_patient_id), so this is the residual
            # case only: two PATIENT_IDs that resolve to one FHIR patient WITHOUT
            # sharing a CPR in arc.ifdb_patient — a CPR corrected in the archive
            # after the FHIR patient had already absorbed both ids.
            #
            # Keep the fuller plan rather than refuse. Posting both would have
            # them overwrite each other's activity list run after run, and
            # failing the nightly run over one person is disproportionate now
            # that re-issues are known to reach the scope query. The loser is
            # almost always the retired id's empty plan; the warning is there for
            # the case where it is not.
            loser, winner = sorted((existing, careplan), key=lambda cp: len(cp.activity or []))
            logger.warning(
                f"IFDB patients {_subject_ifdb_patient_id(existing)!r} and "
                f"{ifdb_patient_id!r} both resolve to FHIR patient "
                f"{fhir_patient_id} without sharing a CPR in arc.ifdb_patient. "
                f"Keeping the plan with {len(winner.activity or [])} activities and "
                f"dropping the one with {len(loser.activity or [])}. Check the CPRs "
                "on those two ids — the person grouping should have caught this."
            )
            kept[fhir_patient_id] = winner
            continue

        kept[fhir_patient_id] = careplan

    if unresolved:
        logger.warning(
            f"Dropped {unresolved} CarePlans whose patient could not be resolved in FHIR"
        )

    return list(kept.values())


def _post_careplans_to_fhir(
    context: dg.AssetExecutionContext,
    careplans: list[CarePlan],
    fhir_database: FHIRDatabaseResource,
    mssql: MSSQLResource,
    reference_fallback_fn: ResolveValuesFn | None = None,
) -> dg.MaterializeResult:
    """Post one batch of CarePlan resources with reference resolution.

    Process:
    1. Resolve missing dependencies (Patient, EpisodeOfCare) from the archive
    2. Key each CarePlan on its resolved FHIR patient id
    3. Resolve all configured references (Patient, Organization, etc.)
    4. Resolve existing CarePlan FHIR IDs (for upsert - prevents duplicates)
    5. Post to FHIR database using direct DB insertion
    """
    if careplans:
        from DSA.FHIR.utils.ifdb.dependency_resolver import resolve_careplan_dependencies

        patient_map = resolve_careplan_dependencies(
            careplans=careplans,
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
        )
        careplans = _key_careplans_on_fhir_patient(careplans, patient_map, context.log)

    # The CarePlan identifier cache is preloaded once per run (see the asset), so
    # batches reuse it rather than reloading per batch.
    careplans = prepare_resources_for_upsert(
        resources=careplans,
        fhir_database=fhir_database,
        resource_type="CarePlan",
        preferred_id_systems=[SYSTEM_CAREPLAN_ALLE_MARKOERER],
        reference_configs=CAREPLAN_REFERENCE_CONFIGS,
        nested_reference_configs=CAREPLAN_NESTED_REFERENCE_CONFIGS,
        reference_fallback_fn=reference_fallback_fn,
        logger=context.log,
    )

    return post_resources_direct(
        context=context,
        resources=careplans,
        fhir_database=fhir_database,
        resource_type="CarePlan",
        source_name="ifdb_alle_markoerer",
    )


# =============================================================================
# Asset Definition
# =============================================================================


@dg.asset(
    name="fhir_server_careplan_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=[
        "arc_eventmarker_ifdb",
        "fhir_server_patient_dfm",  # Patients must exist
        "fhir_server_eoc_dfm",  # EpisodeOfCare references must exist
    ],
    tags={
        TAG_CHUNK_SIZE: "400",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Rebuild the Alle Markoerer CarePlan for every patient whose markers moved "
        "in arc.ifdb_eventmarker: one CarePlan per patient holding all their live "
        "markers, posted to the FHIR server. Checkpoint owner for fhir_server.CarePlan."
    ),
)
def fhir_server_careplan_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Scope to delta patients, rebuild each patient's CarePlan, post (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "CarePlan",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    batch_size = CAREPLAN_CHUNK_STAGE.chunk_size
    total_patients = 0
    total_markers = 0
    total_posted = 0
    batch_num = 0

    # Everything after start_dfm_run() belongs inside the try: the DFMRunLog row
    # is already open, so anything that raises outside it leaves the run stuck
    # at *running* until the stale sweep abandons it. The guard below is built
    # to raise, which makes that the realistic path rather than a corner case.
    try:
        # Before anything is posted: the identity reload is a hard precondition
        # for this code, and forgetting it fails silently rather than loudly.
        _refuse_old_scheme_resources(mssql_target_staging, context.log)

        # Patient-id -> CPR fallback: resolves subject references to
        # soft-deleted PATIENT_IDs (e.g. a deleted CPR-duplicate) to the active
        # patient posted under the same CPR. Built once, reused for every batch.
        patient_cpr_fallback = build_patient_cpr_fallback(mssql_target_staging, fhir_database)

        # Re-issued PATIENT_IDs grouped onto one person. Read once for the whole
        # run, not per batch: the key has to be the same in every batch, or two
        # siblings scoped into different batches would stage different resources.
        persons = _person_keys_by_patient_id(mssql_target_staging, context.log)

        # include_deleted=True is required, not defensive — see the module
        # docstring: a retired marker's only trace in the window is the row that
        # just went dw_is_deleted = 1, so filtering it out would drop the patient
        # from the scope and the removal would never reach the FHIR server.
        query = build_fhir_archive_query(
            ARCHIVE_TABLE,
            run_ctx.checkpoint,
            config,
            include_deleted=True,
            select_clause="SELECT DISTINCT [PATIENT_ID]",
        )
        context.log.info(
            f"Scoping delta patients from {ARCHIVE_TABLE} "
            f"(mode={config.mode}, batch_size={batch_size:,})"
        )

        # The scope cursor stays open across the whole run; the per-batch marker
        # fetch opens its own connection so it never competes with it.
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)

                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break

                    batch_num += 1
                    patient_ids = [
                        normalize_oracle_id(row[0]) for row in rows if row[0] is not None
                    ]
                    if not patient_ids:
                        continue

                    total_patients += len(patient_ids)
                    context.log.info(
                        f"Batch {batch_num}: {len(patient_ids):,} patients "
                        f"(total so far: {total_patients:,})"
                    )

                    # Widened to every id the scoped people hold: a batch that
                    # caught only a retired id must still see its live sibling's
                    # markers, or it posts an empty plan over a live one.
                    markers = _fetch_markers_for_patients(
                        mssql_target_staging, persons.expand(patient_ids)
                    )
                    total_markers += len(markers)

                    careplans_data = _stage_careplans(patient_ids, markers, context.log, persons)
                    del markers

                    careplans = _transform_careplans_to_fhir(careplans_data, context)
                    del careplans_data

                    if careplans:
                        result = _post_careplans_to_fhir(
                            context,
                            careplans,
                            fhir_database,
                            mssql=mssql_target_staging,
                            reference_fallback_fn=patient_cpr_fallback,
                        )
                        total_posted += _read_int_metadata(result, "posted_count", len(careplans))
                    del careplans
            finally:
                cursor.close()

        context.log.info(
            f"Done: {total_patients:,} patients, {total_markers:,} markers, "
            f"{total_posted:,} CarePlans posted in {batch_num} batches"
        )

        new_checkpoint = advance_fhir_checkpoint(
            dfm,
            mssql_target_staging,
            "CarePlan",
            config,
            total_patients,
        )
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=new_checkpoint, rows_affected=total_posted
        )

        return dg.MaterializeResult(
            metadata={
                "posted_count": total_posted,
                "patient_count": total_patients,
                "marker_count": total_markers,
                "batch_count": batch_num,
                "batch_size": batch_size,
                "mode": config.mode,
            }
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

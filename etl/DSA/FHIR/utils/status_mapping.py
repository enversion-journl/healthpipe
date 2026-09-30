# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from datetime import UTC, datetime


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _as_aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        # Assume UTC if naive
        return dt.replace(tzinfo=UTC)
    return dt


def map_episode_of_care_status(
    source_status: str | None,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
    *,
    is_deleted: bool = False,
) -> str:
    """
    Maps source EpisodeOfCare status and context to FHIR EpisodeOfCare.status.
    Allowed: planned | waitlist | active | onhold | finished | cancelled | entered-in-error

    When IFDB marks the row deleted (DELETED), use entered-in-error per FHIR semantics.
    """
    if is_deleted:
        return "entered-in-error"

    if source_status:
        s = source_status.strip().lower()
        mapping = {
            "planned": "planned",
            "waitlist": "waitlist",
            "active": "active",
            "onhold": "onhold",
            "finished": "finished",
            "cancelled": "cancelled",
            "entered-in-error": "entered-in-error",
        }
        if s in mapping:
            return mapping[s]

    # Fallback: derive from period
    now = _now_utc()
    start = _as_aware(period_start)
    end = _as_aware(period_end)

    if start and start > now:
        return "planned"
    if start and start <= now and (end is None or end > now):
        return "active"
    if end and end <= now:
        return "finished"

    return "active"


def map_encounter_status(source: str | None, *, is_deleted: bool = False) -> str:
    """
    Maps source Encounter status text/code to FHIR Encounter.status.
    Allowed: planned | arrived | triaged | in-progress | onleave | finished | cancelled | entered-in-error | unknown

    When IFDB marks the row deleted (DELETED), use entered-in-error per FHIR semantics.
    """
    if is_deleted:
        return "entered-in-error"

    if not source:
        return "unknown"
    s = source.strip().lower()
    allowed = {
        "planned",
        "arrived",
        "triaged",
        "in-progress",
        "onleave",
        "finished",
        "cancelled",
        "entered-in-error",
        "unknown",
    }
    return s if s in allowed else "unknown"


# BOM_IFDB BOOKINGSTATUS (and AFTALEBOOKINGHISTORIK.STATUS — verified identical encoding)
# documented on AFTALE.BOOKINGSTATUS: 1 Booket, 2 Afviklet, 3 Slettet, 4 Huskeliste,
# 5 Genbooking, 10 Aflyst, 11 Anmodet, 13 Webanmodet.
_APPOINTMENT_STATUS_MAP: dict[int, str] = {
    1: "booked",
    2: "fulfilled",
    3: "entered-in-error",
    4: "proposed",
    5: "pending",
    10: "cancelled",
    11: "proposed",
    13: "proposed",
}

# AFVIKLINGSSTATUS.STATUSTYPE — workflow resolution refinement.
# When an AFVIKLINGSSTATUS is assigned, BOOKINGSTATUS may still be 1 (booked)
# even though the appointment was completed or the patient didn't show up.
# STATUSTYPE encodes the semantic class of the resolution status per OPGAVETYPE.
# Known values: 1 = Afviklet (completed), 2 = Ikke-fremmødt (no-show).
_AFVIKLINGSSTATUS_TYPE_OVERRIDE: dict[int, str] = {
    1: "fulfilled",
    2: "noshow",
}


def map_appointment_status(
    source_status: int | str | None,
    *,
    is_deleted: bool = False,
    afviklingsstatus_type: int | str | None = None,
) -> str:
    """
    Map BOM_IFDB AFTALE/AFTALEBOOKINGHISTORIK numeric status to FHIR R4B Appointment.status.

    Allowed FHIR values: proposed | pending | booked | arrived | fulfilled | cancelled
                       | noshow | entered-in-error | checked-in | waitlist

    When AFTALE.DATOSLETTET is set (soft delete), override to entered-in-error.
    When AFVIKLINGSSTATUS.STATUSTYPE is present and the base status is "booked",
    refine to "fulfilled" (afviklet) or "noshow" (ikke-fremmødt).
    Unknown / NULL source values fall back to "proposed" (least committal).
    """
    if is_deleted:
        return "entered-in-error"

    if source_status is None:
        return "proposed"

    try:
        code = int(source_status)
    except (TypeError, ValueError):
        return "proposed"

    base = _APPOINTMENT_STATUS_MAP.get(code, "proposed")

    if base == "booked" and afviklingsstatus_type is not None:
        try:
            afs_type = int(afviklingsstatus_type)
        except (TypeError, ValueError):
            return base
        return _AFVIKLINGSSTATUS_TYPE_OVERRIDE.get(afs_type, base)

    return base


_PARTICIPANT_STATUS_MAP: dict[str, str] = {
    "proposed": "needs-action",
    "pending": "needs-action",
    "booked": "accepted",
    "arrived": "accepted",
    "fulfilled": "accepted",
    "cancelled": "declined",
    "noshow": "accepted",
    "entered-in-error": "accepted",
    "checked-in": "accepted",
    "waitlist": "needs-action",
}


def map_participant_status(appointment_status: str) -> str:
    """
    Derive FHIR Appointment.participant.status from the resolved Appointment.status.

    FHIR R4B ParticipationStatus value set:
        accepted | declined | tentative | needs-action

    Logic:
    - proposed/pending/waitlist -> needs-action (not yet confirmed)
    - booked/arrived/fulfilled/checked-in/noshow/entered-in-error -> accepted
    - cancelled -> declined (appointment was called off)
    - unknown values -> needs-action (safe default)
    """
    return _PARTICIPANT_STATUS_MAP.get(appointment_status, "needs-action")


def map_encounter_location_status(
    encounter_status_text: str | None = None,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> str:
    """
    Maps source data to FHIR Encounter.location.status.

    FHIR EncounterLocationStatus is a Required binding with allowed values:
    planned | active | reserved | completed

    Mapping logic:
    1. First try to derive from encounter STATUS_TEXT patterns:
       - *Finished* -> completed
       - *Inprogress* -> active
       - *Planned* -> planned
    2. Fallback to period-based derivation:
       - Period in future -> planned
       - Period active (start <= now, no end or end > now) -> active
       - Period ended -> completed
    3. Default: active

    Args:
        encounter_status_text: The encounter's STATUS_TEXT field (e.g., "EncounterState.Status.StatusParent.Finished")
        period_start: Start of the location period
        period_end: End of the location period

    Returns:
        One of: "planned", "active", "reserved", "completed"
    """
    # Try to derive from encounter status text patterns
    if encounter_status_text:
        status_lower = encounter_status_text.lower()

        # Check for Finished pattern
        if "finished" in status_lower:
            return "completed"

        # Check for Inprogress pattern
        if (
            "inprogress" in status_lower
            or "in-progress" in status_lower
            or "in progress" in status_lower
        ):
            return "active"

        # Check for Planned pattern
        if "planned" in status_lower:
            return "planned"

    # Fallback: derive from period
    now = _now_utc()
    start = _as_aware(period_start)
    end = _as_aware(period_end)

    # If start is in the future, status is planned
    if start and start > now:
        return "planned"

    # If period has ended, status is completed
    if end and end <= now:
        return "completed"

    # If start is in the past and no end or end is in the future, status is active
    if start and start <= now:
        return "active"

    # Default to active
    return "active"

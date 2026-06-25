# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Appointment assets for BOM_IFDB data.

Transforms extracted BOM_IFDB.AFTALE appointment data into FHIR Appointment
resources and posts them to the FHIR server using direct DB insertion.

Source tables (pre-joined in the extract config):
    BOM_IFDB.AFTALE                  – core appointment
    BOM_IFDB.AFTALEBOOKINGHISTORIK   – latest historik row (status, reason, dates)
    BOM_IFDB.PATIENTINFO             – CPR number (PATIENT_CPR)
    BOM_IFDB.ORGENHED + SORENHED     – performing org SOR code (UDFOERENDE_SORKODE)
    BOM_IFDB.OPGAVE + OPGAVETYPE     – appointment type / SKS code
    BOM_IFDB.AFVIKLINGSSTATUS        – workflow status refinement (fulfilled/noshow)

Inline lookup (one-to-many, not in extract):
    BOM_IFDB.AFTALERESSOURCEDELTAGER – resource participants (~1.1 per appointment)

Reference Resolution:
    Uses declarative configuration with NestedReferenceFieldConfig for:
    - Patient  (participant.actor via CPR)       – required
    - Location (participant.actor via SOR)       – optional (SOR-keyed Location feeder)

Note on Encounter linkage:
    Per FHIR R4B, the Encounter→Appointment reference lives on Encounter
    (Encounter.appointment), NOT on Appointment. We do NOT store an Encounter
    reference on Appointment.

Note on dependency resolution:
    BOM_IFDB uses CPR as the patient identifier (SYSTEM_CPR), not IFDB patient IDs.
    Since resolve_missing_patients() expects IFDB patient IDs, we use the CPR-first
    resolver (resolve_missing_patients_by_cpr) with IFDB→BOM_IFDB fallback chain.
    The Patient asset for the same partition MUST be a dep.
"""

from collections import defaultdict
from datetime import timedelta
from typing import Any

import dagster as dg
from core.parallel import env_int
from core.partitions import (
    daily_partitions,
    full_load_partitions,
    monthly_partitions,
)
from core.resources import (
    FHIRDatabaseResource,
    IFDBOracleResource,
)
from DSA.FHIR.utils.fhir_utils import fhir_datetime
from DSA.FHIR.utils.ifdb.fhir_posting import (
    post_resources_direct,
    prepare_resources_for_upsert,
)
from DSA.FHIR.utils.ifdb.posting_config import (
    TAG_CHUNK_SIZE,
    TAG_MAX_WORKERS,
)
from DSA.FHIR.utils.reference_resolver import (
    NestedReferenceFieldConfig,
    ReferenceFieldConfig,
)
from DSA.FHIR.utils.status_mapping import map_appointment_status, map_participant_status
from DSI.connectors.oracle.models import fetch_rows_as_dicts
from fhir.resources.R4B.appointment import Appointment, AppointmentParticipant
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.meta import Meta
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference
from utils.fhir.identifier_systems import (
    SYSTEM_BOM_IFDB_AFTALE_OID,
    SYSTEM_BOM_IFDB_CANCELLATION_REASON,
    SYSTEM_BOM_IFDB_KONTAKTTYPE,
    SYSTEM_BOM_IFDB_REKVISITION_OID,
    SYSTEM_COLUMNA_BOOKING_ACTIVITY_ID,
    SYSTEM_CPR,
    SYSTEM_SKS,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

_APPOINTMENT_CHUNK_SIZE = max(1, env_int("IFDB_APPOINTMENT_CHUNK_SIZE", 500_000, minimum=1))

# =============================================================================
# Reference Resolution Configuration
# =============================================================================

APPOINTMENT_REFERENCE_CONFIGS: list[ReferenceFieldConfig] = []

APPOINTMENT_NESTED_REFERENCE_CONFIGS = [
    NestedReferenceFieldConfig(
        field_path="participant.actor",
        identifier_system_to_type={
            SYSTEM_CPR: "Patient",
            SYSTEM_SOR: "Location",
        },
        is_required=False,
        description="Participant actors: Patient (CPR) and Location (SOR)",
    ),
]

KONTAKTTYPE_DISPLAY: dict[int, str] = {
    0: "Overføres - ingen kontakt",
    1: "Overføres - Ambulant",
    3: "Overføres - Indlæggelse",
    4: "Overføres - Virtuel",
    5: "Overføres - Ude",
    6: "Overføres - Død",
}


# =============================================================================
# Inline Lookup Functions
# =============================================================================


def _fetch_participants(
    aftale_oids: list[int],
    ifdb_oracle: IFDBOracleResource,
    context: dg.AssetExecutionContext,
) -> dict[int, list[dict[str, Any]]]:
    """
    Fetch AFTALERESSOURCEDELTAGER rows for a set of AFTALE OIDs.

    Returns mapping of AFTALEOID -> list of participant dicts.
    Average ~1.1 participants per appointment.
    """
    participants_by_aftale: dict[int, list[dict[str, Any]]] = defaultdict(list)
    if not aftale_oids:
        return participants_by_aftale

    chunk_size = 999
    id_chunks = [aftale_oids[i : i + chunk_size] for i in range(0, len(aftale_oids), chunk_size)]

    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for chunk in id_chunks:
                placeholders = ", ".join([f":p{i}" for i in range(len(chunk))])
                binds = {f"p{i}": oid for i, oid in enumerate(chunk)}
                cursor.execute(
                    f"SELECT AFTALEOID, KRAV_PERSONALE_ROLLEKODE, "
                    f"TRAEK_RESSOURCEOID, TRAEK_START_DATOTID, TRAEK_SLUT_DATOTID "
                    f"FROM BOM_IFDB.AFTALERESSOURCEDELTAGER "
                    f"WHERE AFTALEOID IN ({placeholders})",
                    binds,
                )
                rows = fetch_rows_as_dicts(cursor, context)
                for row in rows:
                    aftale_oid = row.get("AFTALEOID")
                    if aftale_oid is not None:
                        participants_by_aftale[int(aftale_oid)].append(row)
        finally:
            cursor.close()

    total_participants = sum(len(v) for v in participants_by_aftale.values())
    context.log.info(
        f"[INLINE LOOKUP] Fetched {total_participants} participants "
        f"for {len(participants_by_aftale)} appointments"
    )
    return participants_by_aftale


# =============================================================================
# Transform Helpers
# =============================================================================


def _strip_or_none(value: Any) -> str | None:
    """Return stripped string or None for empty/NULL values."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _build_cancelation_reason(row: dict[str, Any]) -> CodeableConcept | None:
    """Build cancelationReason from AFTALEBOOKINGHISTORIK AARSAG columns."""
    aarsag_type = row.get("HIST_AARSAG_TYPE")
    aarsag_kode_tekst = row.get("HIST_AARSAG_KODE_TEKST")
    aarsag_egentekst = row.get("HIST_AARSAG_EGENTEKST")

    if not any((aarsag_type, aarsag_kode_tekst, aarsag_egentekst)):
        return None

    codings = []
    if aarsag_type is not None:
        codings.append(
            Coding(
                system=SYSTEM_BOM_IFDB_CANCELLATION_REASON,
                code=str(aarsag_type).strip(),
                display=_strip_or_none(aarsag_kode_tekst),
            )
        )

    return CodeableConcept(
        coding=codings if codings else None,
        text=_strip_or_none(aarsag_egentekst) or _strip_or_none(aarsag_kode_tekst),
    )


# =============================================================================
# Transform Function
# =============================================================================


def _transform_appointments_to_fhir(
    aftale_rows: list[dict[str, Any]],
    participants_by_aftale: dict[int, list[dict[str, Any]]],
    context: dg.AssetExecutionContext,
) -> list[Appointment]:
    """
    Transform BOM_IFDB.AFTALE rows to FHIR Appointment resources.

    Uses pre-joined columns from the extract config (PATIENT_CPR, UDFOERENDE_SORKODE,
    HIST_*, OPGAVE/OPGAVETYPE fields, AFVIKLINGSSTATUS) to avoid redundant
    Oracle round-trips.

    Builds:
    - identifier: primary (OID), secondary (Columna booking activity ID)
    - status: HIST_STATUS → BOOKINGSTATUS fallback → AFVIKLINGSSTATUS_TYPE refinement
    - meta.security: v3-Confidentiality R when SKAERMNING_SKAERMET is set
    - appointmentType: from OPGAVETYPE (SKS code)
    - cancelationReason: from HIST_AARSAG_* when cancelled/noshow
    - serviceType: from KONTAKTTYPE
    - basedOn: logical ServiceRequest reference from REKVISITIONOID
    - participant: patient (CPR, required from PATIENTDELTAGER) +
      AFTALERESSOURCEDELTAGER + location (SOR)
    - start/end, minutesDuration, description, patientInstruction, comment, created
    - requestedPeriod: from SOEGEPERIODE_FRADATO / TILDATO
    - priority: from SOEGEPERIODE_PRIORITET
    - extensions: isDeleted
    """
    # Oracle session returns naive datetimes in Europe/Copenhagen local time.
    # fhir_datetime() appends the configured TZ offset (+01:00 / +02:00 DST).
    appointments: list[Appointment] = []
    skipped_no_pk = 0
    skipped_no_cpr = 0

    for row in aftale_rows:
        aftale_oid = row.get("OID")
        if aftale_oid is None:
            skipped_no_pk += 1
            continue

        aftale_oid_int = int(aftale_oid)

        # Patient CPR from pre-joined PATIENTINFO column
        cpr = _strip_or_none(row.get("PATIENT_CPR"))
        if not cpr:
            skipped_no_cpr += 1
            continue

        # -- Identifiers: OID (primary) + Columna (secondary, when present) -----
        identifiers = [
            Identifier(system=SYSTEM_BOM_IFDB_AFTALE_OID, value=str(aftale_oid)),
        ]
        columna_id = _strip_or_none(row.get("COLUMNA_AFTALEIDENTIFIKATION"))
        if columna_id:
            identifiers.append(
                Identifier(system=SYSTEM_COLUMNA_BOOKING_ACTIVITY_ID, value=columna_id)
            )

        # -- Status: HIST_STATUS with BOOKINGSTATUS fallback, DATOSLETTET override,
        # AFVIKLINGSSTATUS_TYPE refinement for completed/noshow appointments.
        raw_deleted = row.get("DATOSLETTET")
        is_deleted = raw_deleted is not None
        deleted_value = 1 if is_deleted else 0
        status_code = row.get("HIST_STATUS") or row.get("BOOKINGSTATUS")
        status = map_appointment_status(
            status_code,
            is_deleted=is_deleted,
            afviklingsstatus_type=row.get("AFVIKLINGSSTATUS_TYPE"),
        )

        # -- Start: prefer historik, fallback to AFTALE ------------------------
        moede_dt = row.get("HIST_PATIENT_MOEDEDATOTID") or row.get("PATIENT_MOEDEDATOTID")
        start_fhir = fhir_datetime(moede_dt) if moede_dt is not None else None

        # -- Duration: validate once, use for both end and minutesDuration ------
        end_fhir = None
        minutes_duration = None
        restraek_dt = row.get("RESTRAEK_STARTDATOTID")
        duration_minutes = row.get("RESTRAEKTOTALMINUTTER")
        duration_minutes_int = None
        if duration_minutes is not None:
            try:
                val = int(duration_minutes)
                duration_minutes_int = val if val > 0 else None
            except (TypeError, ValueError):
                pass

        if restraek_dt is not None and duration_minutes_int is not None:
            end_fhir = fhir_datetime(restraek_dt + timedelta(minutes=duration_minutes_int))

        minutes_duration = duration_minutes_int

        # -- Text fields -------------------------------------------------------
        description = _strip_or_none(row.get("ANGAAENDE"))
        patient_instruction = _strip_or_none(row.get("BORGERTITEL"))
        comment = _strip_or_none(row.get("BREVTEKST"))

        # -- Created -----------------------------------------------------------
        created_fhir = None
        oprettet_dt = row.get("OPRETTETDATOTID")
        if oprettet_dt is not None:
            created_fhir = fhir_datetime(oprettet_dt)

        # -- appointmentType from OPGAVETYPE (SKS code) ------------------------
        appointment_type = None
        opgavetype_sks = _strip_or_none(row.get("OPGAVETYPE_SKSKODE"))
        if opgavetype_sks:
            appointment_type = CodeableConcept(
                coding=[
                    Coding(
                        system=SYSTEM_SKS,
                        code=opgavetype_sks,
                        display=_strip_or_none(row.get("OPGAVETYPE_NAVN")),
                    )
                ],
                text=(
                    _strip_or_none(row.get("OPGAVE_TITEL"))
                    or _strip_or_none(row.get("OPGAVE_NAVN"))
                ),
            )

        # -- cancelationReason from HIST_AARSAG_* (cancelled/noshow only) ------
        cancelation_reason = None
        if status in ("cancelled", "noshow"):
            cancelation_reason = _build_cancelation_reason(row)

        # -- serviceType from KONTAKTTYPE --------------------------------------
        service_type = None
        kontakttype = row.get("KONTAKTTYPE")
        if kontakttype is not None:
            try:
                kt_int = int(kontakttype)
                service_type = [
                    CodeableConcept(
                        coding=[
                            Coding(
                                system=SYSTEM_BOM_IFDB_KONTAKTTYPE,
                                code=str(kt_int),
                                display=KONTAKTTYPE_DISPLAY.get(kt_int),
                            )
                        ]
                    )
                ]
            except (TypeError, ValueError):
                pass

        # -- meta.security from SKAERMNING (screened/confidential appointments) -
        resource_meta = None
        if row.get("SKAERMNING_SKAERMET") == 1:
            resource_meta = Meta(
                security=[
                    Coding(
                        system="http://terminology.hl7.org/CodeSystem/v3-Confidentiality",
                        code="R",
                        display="restricted",
                    )
                ]
            )

        # -- basedOn → ServiceRequest from REKVISITIONOID ----------------------
        based_on = None
        rekvisition_oid = row.get("REKVISITIONOID")
        if rekvisition_oid is not None:
            based_on = [
                Reference(
                    type="ServiceRequest",
                    identifier=Identifier(
                        system=SYSTEM_BOM_IFDB_REKVISITION_OID,
                        value=str(int(rekvisition_oid)),
                    ),
                )
            ]

        # -- requestedPeriod from SOEGEPERIODE ---------------------------------
        requested_period = None
        soege_fra = row.get("SOEGEPERIODE_FRADATO")
        soege_til = row.get("SOEGEPERIODE_TILDATO")
        if soege_fra is not None or soege_til is not None:
            requested_period = [
                Period(
                    start=fhir_datetime(soege_fra) if soege_fra is not None else None,
                    end=fhir_datetime(soege_til) if soege_til is not None else None,
                )
            ]

        # -- priority from PRIORITET.NR (via SOEGEPERIODE_PRIORITETOID join) ---
        priority = None
        raw_priority = row.get("SOEGEPERIODE_PRIORITET_NR")
        if raw_priority is not None:
            try:
                prio_int = int(raw_priority)
                if prio_int >= 0:
                    priority = prio_int
            except (TypeError, ValueError):
                pass

        # -- Participants ------------------------------------------------------
        fhir_participants = []
        participant_status = map_participant_status(status)

        # Patient (participant.required derived from PATIENTDELTAGER)
        patient_required = "required"
        if row.get("PATIENTDELTAGER") == 0:
            patient_required = "information-only"

        fhir_participants.append(
            AppointmentParticipant(
                type=[
                    CodeableConcept(
                        coding=[
                            Coding(
                                system="http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                                code="SBJ",
                                display="subject",
                            )
                        ]
                    )
                ],
                actor=Reference(
                    type="Patient",
                    identifier=Identifier(system=SYSTEM_CPR, value=cpr),
                ),
                required=patient_required,
                status=participant_status,
            )
        )

        # Resource participants from AFTALERESSOURCEDELTAGER
        for part_row in participants_by_aftale.get(aftale_oid_int, []):
            rolle_code = part_row.get("KRAV_PERSONALE_ROLLEKODE")
            if rolle_code:
                fhir_participants.append(
                    AppointmentParticipant(
                        type=[
                            CodeableConcept(
                                coding=[
                                    Coding(
                                        system="http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                                        code="ATND",
                                        display=str(rolle_code),
                                    )
                                ]
                            )
                        ],
                        status=participant_status,
                    )
                )

        # Performing Location via pre-joined SOR code
        sor_code = _strip_or_none(row.get("UDFOERENDE_SORKODE"))
        if sor_code:
            fhir_participants.append(
                AppointmentParticipant(
                    actor=Reference(
                        type="Location",
                        identifier=Identifier(system=SYSTEM_SOR, value=sor_code),
                    ),
                    type=[
                        CodeableConcept(
                            coding=[
                                Coding(
                                    system="http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                                    code="LOC",
                                    display="location",
                                )
                            ]
                        )
                    ],
                    status=participant_status,
                )
            )

        # -- Extensions --------------------------------------------------------
        extensions = [build_is_deleted_extension(deleted_value)]

        # -- Build the Appointment resource ------------------------------------
        appointment = Appointment(
            meta=resource_meta,
            identifier=identifiers,
            status=status,
            appointmentType=appointment_type,
            cancelationReason=cancelation_reason,
            serviceType=service_type,
            basedOn=based_on,
            start=start_fhir,
            end=end_fhir,
            minutesDuration=minutes_duration,
            description=description,
            patientInstruction=patient_instruction,
            comment=comment,
            created=created_fhir,
            participant=fhir_participants,
            requestedPeriod=requested_period,
            priority=priority,
            extension=extensions,
        )

        appointments.append(appointment)

    if skipped_no_pk:
        context.log.warning(f"Skipped {skipped_no_pk} rows with NULL OID")
    if skipped_no_cpr:
        context.log.warning(
            f"Skipped {skipped_no_cpr} rows where PATIENTINFO CPR could not be resolved"
        )

    context.log.info(f"Transformed {len(appointments)} FHIR Appointment resources")
    return appointments


# =============================================================================
# Post Function
# =============================================================================


def _post_appointments_to_fhir(
    context: dg.AssetExecutionContext,
    appointments: list[Appointment],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    ifdb_oracle: IFDBOracleResource | None = None,
    resolve_missing_dependencies: bool = True,
) -> dg.MaterializeResult:
    """Post Appointment resources to FHIR database with reference resolution.

    Uses resolve_missing_patients_by_cpr() to ensure referenced patients exist
    in the FHIR server before resolving references.
    """
    logger = dg.get_dagster_logger()

    if not appointments:
        return dg.MaterializeResult(
            metadata={
                "extracted_count": extracted_count,
                "transformed_count": 0,
                "posted_count": 0,
            }
        )

    # Resolve missing Patient dependencies via CPR fallback before reference resolution
    if resolve_missing_dependencies and ifdb_oracle and appointments:
        from DSA.FHIR.utils.ifdb.dependency_resolver import (
            resolve_missing_patients_by_cpr,
        )

        cprs: set[str] = set()
        for appt in appointments:
            for participant in appt.participant or []:
                actor = participant.actor
                if actor and actor.identifier and actor.identifier.system == SYSTEM_CPR:
                    if actor.identifier.value:
                        cprs.add(actor.identifier.value)

        if cprs:
            resolve_missing_patients_by_cpr(
                cprs=cprs,
                ifdb_oracle=ifdb_oracle,
                fhir_database=fhir_database,
                context=context,
            )

    # Resolve all references and FHIR IDs for upsert using declarative configuration
    appointments = prepare_resources_for_upsert(
        resources=appointments,
        fhir_database=fhir_database,
        resource_type="Appointment",
        preferred_id_systems=[SYSTEM_BOM_IFDB_AFTALE_OID],
        reference_configs=APPOINTMENT_REFERENCE_CONFIGS,
        nested_reference_configs=APPOINTMENT_NESTED_REFERENCE_CONFIGS,
        force_reload=True,
        logger=logger,
    )

    # Post resources using direct DB insertion
    return post_resources_direct(
        context=context,
        resources=appointments,
        fhir_database=fhir_database,
        resource_type="Appointment",
        source_name="bom_ifdb",
        extracted_count=extracted_count,
    )


# =============================================================================
# Shared Asset Logic
# =============================================================================


def _run_appointment_pipeline(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Shared pipeline logic for small partitions (daily).

    Processes all extracted data in a single pass — appropriate when the partition
    size fits comfortably in memory (typically < 500k rows for daily).
    """
    if not extracted_data:
        context.log.info("No AFTALE rows extracted — nothing to transform.")
        return dg.MaterializeResult(
            metadata={"extracted_count": 0, "transformed_count": 0, "posted_count": 0}
        )

    # Pre-filter to rows that will actually produce Appointments (have OID + CPR),
    # avoiding wasted Oracle round-trips for participants of rows that get skipped.
    valid_rows = [
        r
        for r in extracted_data
        if r.get("OID") is not None and _strip_or_none(r.get("PATIENT_CPR"))
    ]
    skipped_prefilter = len(extracted_data) - len(valid_rows)
    if skipped_prefilter:
        context.log.info(f"Pre-filtered {skipped_prefilter} rows missing OID or PATIENT_CPR")

    # Inline lookup for AFTALERESSOURCEDELTAGER (one-to-many, not in extract)
    aftale_oids = [int(r["OID"]) for r in valid_rows]
    participants_by_aftale = _fetch_participants(aftale_oids, ifdb_oracle, context)

    # Transform using pre-joined columns from extract
    appointments = _transform_appointments_to_fhir(valid_rows, participants_by_aftale, context)

    # Post
    return _post_appointments_to_fhir(
        context=context,
        appointments=appointments,
        fhir_database=fhir_database,
        extracted_count=len(extracted_data),
        ifdb_oracle=ifdb_oracle,
        resolve_missing_dependencies=True,
    )


def _run_appointment_pipeline_chunked(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Chunked pipeline for large partitions (monthly / full_load).

    Memory semantics:
      - Iterates over extracted_data in chunks of _APPOINTMENT_CHUNK_SIZE.
      - Each chunk independently fetches its participants, transforms, and posts.
      - Chunk-local state is freed between iterations, bounding peak memory to
        the chunk size rather than the full partition.

    If extracted_data supports iter_batches() (BatchedExtractRows from the
    streaming IO manager), it is consumed lazily without loading the full
    partition into memory.
    """
    total_rows = len(extracted_data)
    if total_rows == 0:
        context.log.info("No AFTALE rows extracted — nothing to transform.")
        return dg.MaterializeResult(
            metadata={"extracted_count": 0, "transformed_count": 0, "posted_count": 0}
        )

    chunk_size = _APPOINTMENT_CHUNK_SIZE
    iter_batches = getattr(extracted_data, "iter_batches", None)

    if total_rows <= chunk_size:
        if callable(iter_batches):
            chunk = next(iter_batches(chunk_size), [])
        else:
            chunk = list(extracted_data)
        return _process_appointment_chunk(
            context=context,
            chunk=chunk,
            ifdb_oracle=ifdb_oracle,
            fhir_database=fhir_database,
            extracted_count=total_rows,
        )

    n_chunks = (total_rows + chunk_size - 1) // chunk_size
    context.log.info(
        "Appointment chunked transform+post: %d rows in %d chunks of %d",
        total_rows,
        n_chunks,
        chunk_size,
    )

    total_posted = 0
    total_transformed = 0
    total_skipped = 0

    if callable(iter_batches):
        chunks_iter = iter_batches(chunk_size)
    else:
        chunks_iter = (
            extracted_data[start : start + chunk_size] for start in range(0, total_rows, chunk_size)
        )

    for chunk_idx, chunk in enumerate(chunks_iter, start=1):
        context.log.info("Chunk %d/%d: processing %d rows", chunk_idx, n_chunks, len(chunk))

        valid_rows = [
            r for r in chunk if r.get("OID") is not None and _strip_or_none(r.get("PATIENT_CPR"))
        ]
        skipped = len(chunk) - len(valid_rows)
        total_skipped += skipped
        if skipped:
            context.log.info(
                "Chunk %d: pre-filtered %d rows missing OID or PATIENT_CPR",
                chunk_idx,
                skipped,
            )

        if not valid_rows:
            context.log.info("Chunk %d: no valid rows after filtering, skipping", chunk_idx)
            continue

        aftale_oids = [int(r["OID"]) for r in valid_rows]
        participants_by_aftale = _fetch_participants(aftale_oids, ifdb_oracle, context)

        appointments = _transform_appointments_to_fhir(valid_rows, participants_by_aftale, context)
        total_transformed += len(appointments)

        if appointments:
            _post_appointments_to_fhir(
                context=context,
                appointments=appointments,
                fhir_database=fhir_database,
                extracted_count=len(chunk),
                ifdb_oracle=ifdb_oracle,
                resolve_missing_dependencies=True,
            )
            total_posted += len(appointments)

        del chunk, valid_rows, aftale_oids, participants_by_aftale, appointments

    context.log.info(
        "Appointment chunked pipeline complete: %d extracted, %d transformed, "
        "%d posted, %d skipped (no OID/CPR)",
        total_rows,
        total_transformed,
        total_posted,
        total_skipped,
    )

    return dg.MaterializeResult(
        metadata={
            "extracted_count": total_rows,
            "transformed_count": total_transformed,
            "posted_count": total_posted,
            "skipped_count": total_skipped,
            "chunk_count": n_chunks,
            "chunk_size": chunk_size,
        }
    )


def _process_appointment_chunk(
    context: dg.AssetExecutionContext,
    chunk: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
) -> dg.MaterializeResult:
    """Process a single chunk end-to-end (filter → participants → transform → post)."""
    valid_rows = [
        r for r in chunk if r.get("OID") is not None and _strip_or_none(r.get("PATIENT_CPR"))
    ]
    skipped_prefilter = len(chunk) - len(valid_rows)
    if skipped_prefilter:
        context.log.info(f"Pre-filtered {skipped_prefilter} rows missing OID or PATIENT_CPR")

    aftale_oids = [int(r["OID"]) for r in valid_rows]
    participants_by_aftale = _fetch_participants(aftale_oids, ifdb_oracle, context)
    appointments = _transform_appointments_to_fhir(valid_rows, participants_by_aftale, context)

    return _post_appointments_to_fhir(
        context=context,
        appointments=appointments,
        fhir_database=fhir_database,
        extracted_count=extracted_count,
        ifdb_oracle=ifdb_oracle,
        resolve_missing_dependencies=True,
    )


# =============================================================================
# Daily Partition Asset
# =============================================================================


@dg.asset(
    name="fhir_appointment_daily_ifdb",
    ins={
        "extracted_data": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "appointment_daily"])),
    },
    # Location/Patient deps ensure intra-partition ordering. Cross-partition references
    # resolve at post-time via batch_resolve_references (partition-agnostic).
    deps=[
        "fhir_server_patient_dfm",
        "fhir_location_sor_daily_ifdb",
    ],
    group_name="fhir_ifdb_daily",
    partitions_def=daily_partitions,
    tags={
        TAG_CHUNK_SIZE: "400",
        TAG_MAX_WORKERS: "8",
    },
)
def fhir_appointment_daily_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post Appointment resources for daily partition."""
    return _run_appointment_pipeline(context, extracted_data, ifdb_oracle, fhir_database)


# =============================================================================
# Monthly Partition Asset
# =============================================================================


@dg.asset(
    name="fhir_appointment_monthly_ifdb",
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "appointment_monthly"]),
            input_manager_key="streaming_batched_extract_io_manager",
        ),
    },
    deps=[
        "fhir_server_patient_dfm",
        "fhir_location_sor_monthly_ifdb",
    ],
    group_name="fhir_ifdb_monthly",
    partitions_def=monthly_partitions,
    tags={
        TAG_CHUNK_SIZE: "1000",
        TAG_MAX_WORKERS: "8",
    },
)
def fhir_appointment_monthly_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post Appointment resources for monthly partition."""
    return _run_appointment_pipeline_chunked(context, extracted_data, ifdb_oracle, fhir_database)


# =============================================================================
# Full Load Partition Asset
# =============================================================================


@dg.asset(
    name="fhir_appointment_full_load_ifdb",
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "appointment_full_load"]),
            input_manager_key="streaming_batched_extract_io_manager",
        ),
    },
    deps=[
        "fhir_server_patient_dfm",
        "fhir_location_sor_full_load_ifdb",
    ],
    group_name="fhir_ifdb_full_load",
    partitions_def=full_load_partitions,
    tags={
        TAG_CHUNK_SIZE: "1000",
        TAG_MAX_WORKERS: "16",
    },
)
def fhir_appointment_full_load_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post Appointment resources for full load."""
    return _run_appointment_pipeline_chunked(context, extracted_data, ifdb_oracle, fhir_database)

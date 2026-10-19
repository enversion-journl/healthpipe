# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Appointment asset using DFM (Data Flow Management).

Single-asset pattern: fetches BOM_IFDB.AFTALE rows from arc.bom_ifdb_aftale
in batches, joins child rows from arc.bom_ifdb_aftaleressourcedeltager per
batch, transforms to FHIR Appointment resources, and posts to the FHIR server.

Soft-deleted rows (DATOSLETTET IS NOT NULL) are included so they can be
tomb-stoned as ``entered-in-error`` rather than left active in Aidbox.

The CPR-based dependency resolver still uses Oracle for the final
BOM_IFDB.PATIENTINFO fallback; ``ifdb_oracle`` is passed through to
``_post_appointments_to_fhir`` for that purpose.
"""

from datetime import datetime, timedelta
from typing import Any

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, IFDBOracleResource, MSSQLResource
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from DSA.FHIR.utils.fhir_utils import fhir_datetime
from DSA.FHIR.utils.ifdb import arc_lookup
from DSA.FHIR.utils.ifdb.fhir_posting import (
    post_resources_direct,
    prepare_resources_for_upsert,
)
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.ifdb.transforms import is_true_flag, normalize_oracle_id
from DSA.FHIR.utils.reference_resolver import (
    NestedReferenceFieldConfig,
    ReferenceFieldConfig,
)
from DSA.FHIR.utils.status_mapping import map_appointment_status, map_participant_status
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

BATCH_SIZE = 10_000

_V3_CONFIDENTIALITY_SYSTEM = "http://terminology.hl7.org/CodeSystem/v3-Confidentiality"
_V3_PARTICIPATION_TYPE_SYSTEM = "http://terminology.hl7.org/CodeSystem/v3-ParticipationType"

_PARTICIPANT_COLUMNS = [
    "OID",
    "AFTALEOID",
    "KRAV_PERSONALE_ROLLEKODE",
    "TRAEK_RESSOURCEOID",
    "TRAEK_START_DATOTID",
    "TRAEK_SLUT_DATOTID",
]

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


def _to_datetime(value: Any) -> datetime | None:
    """Coerce an arc NVARCHAR timestamp ("2026-02-01 09:00:00[.ffffff]") back
    to a datetime. The archive stores every BOM_IFDB DATE column as text, so
    the transform must parse before (a) fhir_datetime() — which only attaches
    the timezone to datetime instances — and (b) RESTRAEK + timedelta
    arithmetic, which raises TypeError on str."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _transform_appointments_to_fhir(
    aftale_rows: list[dict[str, Any]],
    participants_by_aftale: dict[int, list[dict[str, Any]]],
    context: dg.AssetExecutionContext,
) -> list[Appointment]:
    """Transform BOM_IFDB.AFTALE rows to FHIR Appointment resources."""
    # The archive delivers naive local timestamps as NVARCHAR; _to_datetime()
    # restores datetimes so fhir_datetime() appends the TZ offset as it did
    # for the Oracle-native values.
    appointments: list[Appointment] = []
    skipped_no_pk = 0
    skipped_bad_oid = 0
    skipped_no_cpr = 0

    for row in aftale_rows:
        # The archive stores OID as NVARCHAR; normalize before casting so a
        # float-formatted value ("123.0") neither crashes the batch nor leaks
        # into the identifier (the upsert match is exact-string).
        aftale_oid = normalize_oracle_id(row.get("OID"))
        if aftale_oid is None:
            skipped_no_pk += 1
            continue
        try:
            aftale_oid_int = int(aftale_oid)
        except ValueError:
            skipped_bad_oid += 1
            continue

        cpr = _strip_or_none(row.get("PATIENT_CPR"))
        if not cpr:
            skipped_no_cpr += 1
            continue

        identifiers = [
            Identifier(system=SYSTEM_BOM_IFDB_AFTALE_OID, value=aftale_oid),
        ]
        columna_id = _strip_or_none(row.get("COLUMNA_AFTALEIDENTIFIKATION"))
        if columna_id:
            identifiers.append(
                Identifier(system=SYSTEM_COLUMNA_BOOKING_ACTIVITY_ID, value=columna_id)
            )

        raw_deleted = row.get("DATOSLETTET")
        is_deleted = raw_deleted is not None
        deleted_value = 1 if is_deleted else 0
        status_code = row.get("HIST_STATUS") or row.get("BOOKINGSTATUS")
        status = map_appointment_status(
            status_code,
            is_deleted=is_deleted,
            afviklingsstatus_type=row.get("AFVIKLINGSSTATUS_TYPE"),
        )

        moede_dt = _to_datetime(
            row.get("HIST_PATIENT_MOEDEDATOTID") or row.get("PATIENT_MOEDEDATOTID")
        )
        start_fhir = fhir_datetime(moede_dt) if moede_dt is not None else None

        end_fhir = None
        minutes_duration = None
        restraek_dt = _to_datetime(row.get("RESTRAEK_STARTDATOTID"))
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

        description = _strip_or_none(row.get("ANGAAENDE"))
        patient_instruction = _strip_or_none(row.get("BORGERTITEL"))
        comment = _strip_or_none(row.get("BREVTEKST"))

        created_fhir = None
        oprettet_dt = _to_datetime(row.get("OPRETTETDATOTID"))
        if oprettet_dt is not None:
            created_fhir = fhir_datetime(oprettet_dt)

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

        cancelation_reason = None
        if status in ("cancelled", "noshow"):
            cancelation_reason = _build_cancelation_reason(row)

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

        resource_meta = None
        # NUMBER(1) arrives as NVARCHAR from arc ("1"/"0") — is_true_flag
        # covers int, str and float-string forms. A bare == 1 would be
        # permanently False and silently drop the confidentiality marking.
        if is_true_flag(row.get("SKAERMNING_SKAERMET")):
            resource_meta = Meta(
                security=[
                    Coding(
                        system=_V3_CONFIDENTIALITY_SYSTEM,
                        code="R",
                        display="restricted",
                    )
                ]
            )

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

        requested_period = None
        soege_fra = _to_datetime(row.get("SOEGEPERIODE_FRADATO"))
        soege_til = _to_datetime(row.get("SOEGEPERIODE_TILDATO"))
        if soege_fra is not None or soege_til is not None:
            requested_period = [
                Period(
                    start=fhir_datetime(soege_fra) if soege_fra is not None else None,
                    end=fhir_datetime(soege_til) if soege_til is not None else None,
                )
            ]

        priority = None
        raw_priority = row.get("SOEGEPERIODE_PRIORITET_NR")
        if raw_priority is not None:
            try:
                prio_int = int(raw_priority)
                if prio_int >= 0:
                    priority = prio_int
            except (TypeError, ValueError):
                pass

        fhir_participants = []
        participant_status = map_participant_status(status)

        patient_required = "required"
        # NUMBER(1) arrives as NVARCHAR from arc — compare normalized text,
        # not == 0 (permanently False against "0").
        patientdeltager = row.get("PATIENTDELTAGER")
        if patientdeltager is not None and str(patientdeltager).strip() in ("0", "0.0"):
            patient_required = "information-only"

        fhir_participants.append(
            AppointmentParticipant(
                type=[
                    CodeableConcept(
                        coding=[
                            Coding(
                                system=_V3_PARTICIPATION_TYPE_SYSTEM,
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

        for part_row in participants_by_aftale.get(aftale_oid_int, []):
            rolle_code = part_row.get("KRAV_PERSONALE_ROLLEKODE")
            if rolle_code:
                fhir_participants.append(
                    AppointmentParticipant(
                        type=[
                            CodeableConcept(
                                coding=[
                                    Coding(
                                        system=_V3_PARTICIPATION_TYPE_SYSTEM,
                                        code="ATND",
                                        display=str(rolle_code),
                                    )
                                ]
                            )
                        ],
                        status=participant_status,
                    )
                )

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
                                    system=_V3_PARTICIPATION_TYPE_SYSTEM,
                                    code="LOC",
                                    display="location",
                                )
                            ]
                        )
                    ],
                    status=participant_status,
                )
            )

        extensions = [build_is_deleted_extension(deleted_value)]

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
    if skipped_bad_oid:
        context.log.warning(f"Skipped {skipped_bad_oid} rows with non-numeric OID")
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
    mssql: MSSQLResource | None = None,
    resolve_missing_dependencies: bool = True,
) -> dg.MaterializeResult:
    """Post Appointment resources to FHIR database with reference resolution."""
    logger = dg.get_dagster_logger()

    if not appointments:
        return dg.MaterializeResult(
            metadata={
                "extracted_count": extracted_count,
                "transformed_count": 0,
                "posted_count": 0,
            }
        )

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
                mssql=mssql,
            )

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

    return post_resources_direct(
        context=context,
        resources=appointments,
        fhir_database=fhir_database,
        resource_type="Appointment",
        source_name="bom_ifdb",
        extracted_count=extracted_count,
    )


# =============================================================================
# DFM Asset
# =============================================================================


@dg.asset(
    name="fhir_server_appointment_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=[
        "arc_bom_ifdb_aftale",
        "arc_bom_ifdb_aftaleressourcedeltager",
        "arc_bom_ifdb_patientinfo",
        "fhir_server_patient_dfm",
        "fhir_server_location_sor_dfm",
    ],
    tags={
        TAG_CHUNK_SIZE: "400",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Fetch appointments from arc.bom_ifdb_aftale in batches, join participants "
        "from arc.bom_ifdb_aftaleressourcedeltager, transform to FHIR Appointment, "
        "and post to FHIR server. Checkpoint owner for fhir_server.Appointment."
    ),
)
def fhir_server_appointment_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    ifdb_oracle: IFDBOracleResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch -> transform -> post for Appointment resources (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "Appointment",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        query = build_fhir_archive_query(
            "arc.bom_ifdb_aftale",
            run_ctx.checkpoint,
            config,
            include_deleted=True,
        )
        context.log.info(f"Querying arc.bom_ifdb_aftale (mode={config.mode})")

        total_fetched = 0
        total_posted = 0
        batch_num = 0

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]

                while True:
                    raw_rows = cursor.fetchmany(BATCH_SIZE)
                    if not raw_rows:
                        break

                    batch_num += 1
                    batch: list[dict[str, Any]] = [
                        dict(zip(columns, row, strict=False)) for row in raw_rows
                    ]
                    total_fetched += len(batch)

                    context.log.info(
                        f"Batch {batch_num}: processing {len(batch)} aftale rows "
                        f"(total so far: {total_fetched})"
                    )

                    aftale_oids = [
                        oid
                        for oid in (normalize_oracle_id(r.get("OID")) for r in batch)
                        if oid is not None
                    ]

                    child_rows_by_key = arc_lookup.chunked_in_fetch_grouped(
                        aftale_oids,
                        table="arc.bom_ifdb_aftaleressourcedeltager",
                        columns=list(_PARTICIPANT_COLUMNS),
                        where_column="AFTALEOID",
                        mssql=mssql_target_staging,
                        key_fn=lambda row: normalize_oracle_id(row.get("AFTALEOID")),
                        sort_key=lambda row: str(row.get("OID") or ""),
                    )

                    # _transform_appointments_to_fhir expects int-keyed dict
                    participants_by_aftale: dict[int, list[dict]] = {}
                    for str_key, rows in child_rows_by_key.items():
                        try:
                            participants_by_aftale[int(str_key)] = rows
                        except (ValueError, TypeError):
                            pass

                    appointments = _transform_appointments_to_fhir(
                        batch, participants_by_aftale, context
                    )

                    if appointments:
                        _post_appointments_to_fhir(
                            context=context,
                            appointments=appointments,
                            fhir_database=fhir_database,
                            extracted_count=len(batch),
                            ifdb_oracle=ifdb_oracle,
                            mssql=mssql_target_staging,
                            resolve_missing_dependencies=True,
                        )
                        total_posted += len(appointments)

            finally:
                cursor.close()

        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted in {batch_num} batches"
        )

        new_checkpoint = advance_fhir_checkpoint(dfm, run_ctx, "Appointment", config, total_fetched)

        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=new_checkpoint,
            rows_affected=total_posted,
        )

        return dg.MaterializeResult(
            metadata={
                "posted_count": total_posted,
                "fetched_count": total_fetched,
                "batch_count": batch_num,
                "mode": config.mode,
            }
        )

    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

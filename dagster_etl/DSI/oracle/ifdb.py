# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
IFDB source system extraction configs and assets.

Extracts data from IFDB Oracle for daily, monthly, and full load partitions.

Entities:
- Eventmarker Alle Markoerer: All event markers with delta + full-load metadata strategy
- Appointment: Appointment data from IFDB
- Orgenhed SOR: BOM_IFDB ORGENHED + SORENHED join for SOR-keyed Locations

Patient, Encounter, EpisodeOfCare, and Location extraction moved to the DFM
pipeline (DSI/oracle/ifdb_extract.py). Their TableExtractionConfig definitions
remain here because the dependency resolver, deleted-resource quality checks,
and tests use them for on-demand Oracle lookups.
"""

from calendar import monthrange
from collections.abc import Callable
from typing import Any

import dagster as dg
from core.extract import ExtractBatchingPolicy
from core.resources import IFDBOracleResource
from DSI.connectors.oracle.asset_factory import create_extraction_assets
from DSI.connectors.oracle.models import TableExtractionConfig
from utils.eventmarker_utils import (
    EVENTMARKER_BASE_COLUMNS,
    extract_eventmarker_delta,
    parse_event_code,
)

# ---------------------------------------------------------------------------
# Patient
# ---------------------------------------------------------------------------

PATIENT_CONFIG = TableExtractionConfig(
    table="IFDB.PATIENT_OPLYSNINGER_COLUMNA",
    columns=[
        "PATIENT_ID",
        "CPR",
        "FORNAVNE",
        "EFTERNAVN",
        "VEJ",
        "POSTNR",
        "STEDNAVN",
        "KOMMUNEKODE",
        "KOEN_KODE",
        "KOEN_KODETEKST",
        "FOEDSELSDATO",
        "LAEGENAVN",
        "LAEGENUMMER",
        "PERSON_STATUSKODE",
        "PERSON_STATUSKODETEKST",
        "HAR_ADRESSEBESKYTTELSE",
        "OENSKER_DISKRETION",
        "BEMAERKNINGER",
        "PERSON_DISTRIKTKODE",
        "PERSON_DISTRIKTKODETEKST",
        "PERSON_SOCIALDISTRIKTKODE",
        "PERSON_SOCIALDISTRIKTKODETEKST",
        "OPDATERET_TID",
        "SLETTET",
        "MODTIDSPUNKT",
    ],
    partition_column="MODTIDSPUNKT",
    description="Patient demographic data from IFDB EPJ system.",
)

# ---------------------------------------------------------------------------
# Encounter
# ---------------------------------------------------------------------------

ENCOUNTER_CONFIG = TableExtractionConfig(
    table="IFDB.ENCOUNTER",
    columns=[
        "OBJECT_ID",
        "PATIENT_ID",
        "EPISODE_OF_CARE_ID",
        "STATUS_CODE",
        "STATUS_TEXT",
        "CLASS_CODE",
        "CLASS_TEXT",
        "PRIORITY_CODE",
        "PRIORITY_TEXT",
        "CLINICAL_SPECIALITY_CODE",
        "CLINICAL_SPECIALITY_TEXT",
        "PERIOD_START",
        "PERIOD_END",
        "SERVICE_PROVIDER_CODE",
        "SERVICE_PROVIDER_NAME",
        "SERVICE_PROVIDER_ORGCODE",
        "SERVICE_PROVIDER_SORCODE",
        "WARD_CODE",
        "WARD_NAME",
        "WARD_ORGCODE",
        "WARD_SORCODE",
        "BED",
        "ROOM",
        "TEAM",
        "EXPECTED_DISCHARGE",
        "TREATMENT_START",
        "REFERRAL_ID",
        "MUNICIPALITY_CARE",
        "BROUGHT_IN_BY",
        "DELETED",
        "EPJI_CREATED_DATETIME",
        "IFDB_MODIFICATION_DATETIME",
    ],
    partition_column="IFDB_MODIFICATION_DATETIME",
    description="Encounter data from IFDB EPJ system.",
)

# ---------------------------------------------------------------------------
# Episode of Care
# ---------------------------------------------------------------------------

EPISODE_OF_CARE_CONFIG = TableExtractionConfig(
    table="IFDB.EPISODE_OF_CARE",
    columns=[
        "OBJECT_ID",
        "PATIENT_ID",
        "STATUS",
        "PERIOD_START",
        "PERIOD_END",
        "MANAGING_ORGANIZATION_CODE",
        "MANAGING_ORGANIZATION_NAME",
        "MANAGING_ORGANIZATION_SORCODE",
        "CARE_MANAGER_ID",
        "CARE_MANAGER_NAME",
        "PREVIOUS_EOC_ID",
        "PREVIOUS_EOC_REF_TYPE_CODE",
        "HEALTHISSUE_ID",
        "DISCHARGE_DISPOSITION_CODE",
        "DELETED",
        "IFDB_MODIFICATION_DATETIME",
    ],
    partition_column="IFDB_MODIFICATION_DATETIME",
    description="Episode of Care (sygedomsforløb) data from IFDB.",
)

# ---------------------------------------------------------------------------
# Location (custom DISTINCT query — SOR code only, no name)
# ---------------------------------------------------------------------------

LOCATION_CONFIG = TableExtractionConfig(
    table="IFDB.LOCATION",
    columns=[
        "OBJECT_ID",
        "LOCATION_CODE",
        "LOCATION_SORCODE",
    ],
    partition_column=None,
    description="Location master data from IFDB (static, no partition column).",
)

# ---------------------------------------------------------------------------
# Eventmarker Alle Markoerer
# ---------------------------------------------------------------------------

EVENTMARKER_CONFIG = TableExtractionConfig(
    table="EVENTMARKER",
    columns=EVENTMARKER_BASE_COLUMNS.replace("em.", "").split(","),
    partition_column=None,
    description="ALL EVENTMARKER data for Alle Markoerer CarePlans.",
)

FULL_LOAD_BATCH_SIZE = 5000


def _enrich_marker_row(row_dict: dict[str, Any]) -> dict[str, Any]:
    """Enrich a marker row with derived fields. Includes ALL markers without filtering."""
    event_code = row_dict.get("EVENT_CODE")

    parsed = parse_event_code(event_code)
    row_dict["MARKER_TYPE"] = parsed["marker_type"]
    row_dict["MARKER_CODE"] = parsed["marker_code"]
    row_dict["MARKER_CODE_FULL"] = parsed["marker_code_full"] or event_code

    row_dict["MARKER_CLASSIFICATION"] = "eventmarker"

    if "TIME" in row_dict:
        row_dict["MarkoerDatotid"] = row_dict.pop("TIME")

    return row_dict


def _extract_full_load_metadata(
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource,
) -> dict[str, Any]:
    """Extract ONLY metadata for full load (counts, not actual patient IDs)."""
    context.log.info("Full-load extraction - fetching COUNTS ONLY (no patient IDs)")

    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*) as total_count
                FROM EVENTMARKER em
                WHERE em.DELETED = 0
            """
            )
            total_marker_count = cursor.fetchone()[0]
            context.log.info(f"Total EVENTMARKER rows (DELETED=0): {total_marker_count:,}")

            cursor.execute(
                """
                SELECT COUNT(DISTINCT PATIENT_ID) as patient_count
                FROM EVENTMARKER
                WHERE DELETED = 0
            """
            )
            total_patient_count = cursor.fetchone()[0]
            context.log.info(f"Distinct patients: {total_patient_count:,}")
        finally:
            cursor.close()

    expected_batches = (total_patient_count + FULL_LOAD_BATCH_SIZE - 1) // FULL_LOAD_BATCH_SIZE
    avg_markers_per_patient = (
        total_marker_count / total_patient_count if total_patient_count > 0 else 0
    )
    context.log.info(f"Expected batches with batch_size={FULL_LOAD_BATCH_SIZE}: {expected_batches}")
    context.log.info(f"Average markers per patient: {avg_markers_per_patient:.1f}")

    return {
        "total_patient_count": total_patient_count,
        "total_marker_count": total_marker_count,
        "full_load_mode": True,
        "batch_size": FULL_LOAD_BATCH_SIZE,
    }


def _get_patient_ids_batch(
    ifdb_oracle: IFDBOracleResource,
    offset: int,
    batch_size: int,
) -> list[Any]:
    """Get a batch of patient IDs using database-level pagination (Oracle ROWNUM)."""
    query = """
        SELECT PATIENT_ID FROM (
            SELECT PATIENT_ID, ROWNUM as rn FROM (
                SELECT DISTINCT PATIENT_ID
                FROM EVENTMARKER
                WHERE DELETED = 0
                ORDER BY PATIENT_ID
            )
            WHERE ROWNUM <= :end_row
        )
        WHERE rn > :start_row
    """

    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                query,
                {
                    "start_row": offset,
                    "end_row": offset + batch_size,
                },
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()


def _extract_markers_for_patient_batch(
    ifdb_oracle: IFDBOracleResource,
    patient_ids: list[Any],
    logger: dg.DagsterLogManager,
) -> list[dict[str, Any]]:
    """Extract ALL markers for a specific batch of patients."""
    if not patient_ids:
        return []

    all_rows: list[tuple] = []
    columns: list[str] = []

    in_clause_batch_size = 1000

    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for i in range(0, len(patient_ids), in_clause_batch_size):
                sub_batch = patient_ids[i : i + in_clause_batch_size]

                placeholders = ", ".join([f":p{j}" for j in range(len(sub_batch))])
                bind_vars = {f"p{j}": pid for j, pid in enumerate(sub_batch)}

                query = f"""
                    SELECT
                        {EVENTMARKER_BASE_COLUMNS}
                    FROM EVENTMARKER em
                    WHERE em.DELETED = 0
                      AND em.PATIENT_ID IN ({placeholders})
                """

                cursor.execute(query, bind_vars)

                if not columns:
                    columns = [desc[0] for desc in cursor.description]

                all_rows.extend(cursor.fetchall())
        finally:
            cursor.close()

    result_rows: list[dict[str, Any]] = []
    for row in all_rows:
        row_dict = dict(zip(columns, row, strict=True))
        enriched = _enrich_marker_row(row_dict)
        result_rows.append(enriched)

    return result_rows


def _eventmarker_builder(asset_kwargs: dict[str, Any], partition_type: str) -> Callable:
    @dg.asset(**asset_kwargs)
    def _asset_fn(
        context: dg.AssetExecutionContext,
        ifdb_oracle: IFDBOracleResource,
    ) -> dict[str, Any]:
        if partition_type == "full_load":
            context.log.info(f"Full-load extraction for partition: {context.partition_key}")
            return _extract_full_load_metadata(context, ifdb_oracle)

        partition_key = context.partition_key
        context.log.info(
            f"{partition_type.title()} delta extraction for partition: {partition_key}"
        )

        if partition_type == "daily":
            start_date = f"{partition_key} 00:00:00"
            end_date = f"{partition_key} 23:59:59"
        else:
            year, month = map(int, partition_key.split("-"))
            _, last_day = monthrange(year, month)
            start_date = f"{partition_key}-01 00:00:00"
            end_date = f"{partition_key}-{last_day:02d} 23:59:59"

        markers = extract_eventmarker_delta(
            context,
            ifdb_oracle,
            start_date,
            end_date,
            eventmarker_columns=EVENTMARKER_BASE_COLUMNS,
            enrich_fn=_enrich_marker_row,
            domain_name="alle_markoerer",
        )

        return {"markers": markers}

    return _asset_fn


# ---------------------------------------------------------------------------
# BOM_IFDB.AFTALE — FHIR Appointment source
# ---------------------------------------------------------------------------
#
# AFTALE is the core appointment table in the BOM_IFDB (Bookplan) schema. We
# join the latest AFTALEBOOKINGHISTORIK row (SENESTE_INDGANG=1) for status and
# cancellation reason, plus PATIENTINFO for the CPR, ORGENHED→SORENHED for the
# performing-organisation SOR code, and OPGAVE/OPGAVETYPE/AFVIKLINGSSTATUS for
# denormalised descriptive fields used by the FHIR transform.
#
# Schema-qualified table name uses an alias so JOIN clauses can reference it.

APPOINTMENT_CONFIG = TableExtractionConfig(
    table="BOM_IFDB.AFTALE a",
    columns=[
        # AFTALE core
        "a.OID",
        "a.GUID",
        "a.UPDATEID",
        "a.PATIENTINFOOID",
        "a.LASTMODIFIED",
        "a.OPRETTETDATOTID",
        "a.OPRETTET_AF_BRUGEROID",
        "a.OPRETTET_AF_AFDELINGOID",
        "a.BOOKINGSTATUS",
        "a.AFVIKLINGSSTATUSOID",
        "a.PATIENT_MOEDEDATOTID",
        "a.PATIENT_SLUT_DATOTID",
        "a.RESTRAEK_STARTDATOTID",
        "a.RESTRAEKTOTALMINUTTER",
        "a.HENVISNINGSDATO",
        "a.PATIENTDELTAGER",
        "a.KONTAKTTYPE",
        "a.OPGAVEOID",
        "a.OPGAVE_AFDELINGOID",
        "a.MOEDESTEDOID",
        "a.YDELSEOID",
        "a.BESTILT_BOOKINGSDATO",
        "a.TYPEOPERATION",
        "a.RELEVANSDATO",
        "a.DATOSLETTET",
        "a.AFLYSTPAADAGENDATOTID",
        "a.TYPEHOLD",
        "a.COLUMNA_AFTALEIDENTIFIKATION",
        "a.SKAERMNING_SKAERMET",
        "a.SKAERMNING_BEGRUNDELSE",
        "a.REKVISITIONOID",
        "a.SOEGEPERIODE_FRADATO",
        "a.SOEGEPERIODE_TILDATO",
        "a.SOEGEPERIODE_PRIORITETOID",
        # PRIORITET (search priority for the appointment)
        "pr.NR AS SOEGEPERIODE_PRIORITET_NR",
        "pr.NAVN AS SOEGEPERIODE_PRIORITET_NAVN",
        "a.ANGAAENDE",
        "a.BORGERTITEL",
        "a.BREVTEKST",
        "a.UDFOERENDE_ORGENHEDOID",
        # AFTALEBOOKINGHISTORIK (latest row per AFTALE, ~99.93% coverage)
        "h.STATUS AS HIST_STATUS",
        "h.PATIENT_MOEDEDATOTID AS HIST_PATIENT_MOEDEDATOTID",
        "h.AFTALE_START_DATOTID AS HIST_AFTALE_START_DATOTID",
        "h.REGISTRERET_DATOTID AS HIST_REGISTRERET_DATOTID",
        "h.AARSAG_AARSAGSTYPE AS HIST_AARSAG_TYPE",
        "h.AARSAG_KODE_KODEKLASSIFIKATIONOID AS HIST_AARSAG_KODE_OID",
        "h.AARSAG_KODE_TEKST_VED_REGISTRERING AS HIST_AARSAG_KODE_TEKST",
        "h.AARSAG_EGENTEKST AS HIST_AARSAG_EGENTEKST",
        # PATIENTINFO (for CPR-based Patient reference)
        "pi.CPRNR AS PATIENT_CPR",
        # UDFOERENDE_ORGENHED → SORENHED chain (for Appointment.participant→Location)
        "se.SORKODE AS UDFOERENDE_SORKODE",
        # OPGAVE / OPGAVETYPE (for appointmentType, serviceType, denormalised labels)
        "op.NAVN AS OPGAVE_NAVN",
        "op.KORTNAVN AS OPGAVE_KORTNAVN",
        "op.TITEL AS OPGAVE_TITEL",
        "op.PRIMAERSKSKODE AS OPGAVE_PRIMAERSKSKODE",
        "opt.NAVN AS OPGAVETYPE_NAVN",
        "opt.KORTNAVN AS OPGAVETYPE_KORTNAVN",
        "opt.SKSKODE AS OPGAVETYPE_SKSKODE",
        "opt.KONTAKTTYPE AS OPGAVETYPE_KONTAKTTYPE",
        # AFVIKLINGSSTATUS (workflow status refinement; denormalised display)
        "afs.NAVN AS AFVIKLINGSSTATUS_NAVN",
        "afs.KORTNAVN AS AFVIKLINGSSTATUS_KORTNAVN",
        "afs.STATUSTYPE AS AFVIKLINGSSTATUS_TYPE",
    ],
    partition_column="a.LASTMODIFIED",
    description="BOM_IFDB Appointment data (AFTALE + joins).",
    additional_joins=(
        "LEFT JOIN BOM_IFDB.AFTALEBOOKINGHISTORIK h "
        "ON h.AFTALEOID = a.OID AND h.SENESTE_INDGANG = 1 "
        "LEFT JOIN BOM_IFDB.PATIENTINFO pi "
        "ON pi.PATIENTINFOOID = a.PATIENTINFOOID "
        "LEFT JOIN BOM_IFDB.ORGENHED oe "
        "ON oe.OID = a.UDFOERENDE_ORGENHEDOID "
        "LEFT JOIN BOM_IFDB.SORENHED se "
        "ON se.OID = oe.SORENHEDOID "
        "LEFT JOIN BOM_IFDB.OPGAVE op "
        "ON op.OPGAVEOID = a.OPGAVEOID "
        "LEFT JOIN BOM_IFDB.OPGAVETYPE opt "
        "ON opt.OPGAVETYPEOID = op.OPGAVETYPEOID "
        "LEFT JOIN BOM_IFDB.AFVIKLINGSSTATUS afs "
        "ON afs.OID = a.AFVIKLINGSSTATUSOID "
        "LEFT JOIN BOM_IFDB.PRIORITET pr "
        "ON pr.PRIORITETOID = a.SOEGEPERIODE_PRIORITETOID"
    ),
    batching=ExtractBatchingPolicy(
        enabled=True,
        fetch_size=100_000,
        env_prefix="IFDB_EXTRACT_APPOINTMENT",
        write_batch_sidecars=True,
    ),
)

# ---------------------------------------------------------------------------
# BOM_IFDB.ORGENHED + SORENHED — FHIR Location (SOR-keyed feeder)
# ---------------------------------------------------------------------------
#
# Produces one Location resource per ORGENHED row where SORENHEDOID is set
# (~95.5% coverage). Keyed by SORENHED.SORKODE so it dovetails with the
# existing SOR-keyed Organization pipeline. Sibling to Location_IFDB_dfm.py
# which is keyed by IFDB.LOCATION SOR codes.

ORGENHED_SOR_CONFIG = TableExtractionConfig(
    table="BOM_IFDB.ORGENHED oe",
    columns=[
        "oe.OID",
        "oe.GUID",
        "oe.ORG_TYPE",
        "oe.AKTIV",
        "oe.GYLDIG_FRA_DATOTID",
        "oe.GYLDIG_TIL_DATOTID",
        "oe.SORENHEDOID",
        "oe.PARENT_ORGENHEDOID",
        "oe.LASTMODIFIED",
        "se.SORKODE",
        "se.SHAKKODE",
        "se.OID AS SORENHED_OID",
    ],
    partition_column="oe.LASTMODIFIED",
    description="BOM_IFDB ORGENHED + SORENHED join for SOR-keyed Locations.",
    additional_joins=("INNER JOIN BOM_IFDB.SORENHED se ON se.OID = oe.SORENHEDOID"),
)

# ---------------------------------------------------------------------------
# Generate assets
# ---------------------------------------------------------------------------

(
    extract_eventmarker_alle_markoerer_daily,
    extract_eventmarker_alle_markoerer_monthly,
    extract_eventmarker_alle_markoerer_full_load,
) = create_extraction_assets(
    entity_name="eventmarker_alle_markoerer",
    config=EVENTMARKER_CONFIG,
    custom_builder=_eventmarker_builder,
)

(
    extract_appointment_daily,
    extract_appointment_monthly,
    extract_appointment_full_load,
) = create_extraction_assets(
    entity_name="appointment",
    config=APPOINTMENT_CONFIG,
    source_deps=[],
)

(
    extract_orgenhed_sor_daily,
    extract_orgenhed_sor_monthly,
    extract_orgenhed_sor_full_load,
) = create_extraction_assets(
    entity_name="orgenhed_sor",
    config=ORGENHED_SOR_CONFIG,
    source_deps=[],
)

# ---------------------------------------------------------------------------
# Asset lists
# ---------------------------------------------------------------------------

ifdb_daily_assets = [
    extract_eventmarker_alle_markoerer_daily,
    extract_appointment_daily,
    extract_orgenhed_sor_daily,
]

ifdb_monthly_assets = [
    extract_eventmarker_alle_markoerer_monthly,
    extract_appointment_monthly,
    extract_orgenhed_sor_monthly,
]

ifdb_full_load_assets = [
    extract_eventmarker_alle_markoerer_full_load,
    extract_appointment_full_load,
    extract_orgenhed_sor_full_load,
]

ifdb_all_assets = ifdb_daily_assets + ifdb_monthly_assets + ifdb_full_load_assets

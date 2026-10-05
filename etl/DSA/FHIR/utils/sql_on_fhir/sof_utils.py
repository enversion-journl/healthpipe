# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR Utilities

Shared logic for:
- Managing ViewDefinitions on Aidbox (create, materialize)
- Extracting flat view data from PostgreSQL sof schema
- Staging into MSSQL temp tables via bulkcopy (with executemany fallback) and MERGE
"""

import json
import time
import uuid
from pathlib import Path
from typing import Any

import dagster as dg
from core.etl_thresholds import check_soft_delete_threshold
from core.resources import (
    FHIRDatabaseResource,
    FHIRServerResource,
    MSSQLResource,
)
from DSA.FHIR.utils.sql_on_fhir.period_keys import (
    PERIOD_START_SENTINEL,
    is_unparseable,
    normalize_fhir_datetime,
    parse_fhir_datetime,
)

# Directory containing ViewDefinition JSON files
VIEW_DEFINITIONS_DIR = (
    Path(__file__).resolve().parent.parent.parent / "SqlOnFhir" / "view_definitions"
)

# One chunk size feeds BOTH the PG server-side fetchmany() and the MSSQL
# bulkcopy(). compute_streaming_chunk_size (DSI/connectors/mssql/bulk_copy.py)
# caps at 10_000 and mssql-python's bulkcopy() is known to wedge on ~15-20k
# rows, so 10_000 is the safe ceiling for both ends.
SOF_STREAM_FETCH_CHUNK_SIZE = 10_000
SOF_BULKCOPY_TIMEOUT_SECONDS = 600

# PostgreSQL plans DECLARE CURSOR with `cursor_tuple_fraction` (default 0.1,
# "optimize for fetching the first 10%"). For SoF views, the bare SELECT picks
# a Hash Join (build once, stream cheaply) but the cursor variant often flips
# to a Nested Loop because the first 10% is "cheap" to start — then each
# fetchmany() blocks for minutes on a join that should take seconds. Pin the
# cursor planner to the same plan the bare SELECT gets by treating the full
# result as the target (fraction = 1.0). Scoped to the cursor's session and
# reset afterwards because the PG connection comes from a pool.
SOF_CURSOR_TUPLE_FRACTION = 1.0

# ODBC/DDBC drivers have a hard limit of 2100 parameters per statement.
# Use 2000 to leave headroom for edge cases.
MAX_DRIVER_PARAMS = 2000

# Retry policy applied to SOF asset decorators (see DSA/FHIR/SqlOnFhir/*).
# Transient TCP/login timeouts to the MSSQL target during the daily job
# are handled by re-executing the whole asset rather than reconnecting
# inline mid-extraction.
SOF_ASSET_RETRY_POLICY = dg.RetryPolicy(max_retries=2, delay=120)

# =============================================================================
# Resource Configuration Registry
# =============================================================================

# Maps each ViewDefinition name to metadata needed for extraction and loading.
#   - view_name: Name of the sof view (matches ViewDefinition.name)
#   - resource_table: Aidbox raw resource table (lowercase FHIR resource type)
#   - id_column: Column in the sof view that matches r.id in the raw table
#   - mssql_table: Target table in MSSQL fhir schema
#   - primary_keys: Primary key column(s) for the MSSQL MERGE
SOF_RESOURCE_CONFIGS: dict[str, dict[str, Any]] = {
    "patient_view": {
        "view_name": "patient_view",
        "resource_table": "patient",
        "id_column": "patient_id",
        "mssql_table": "Patient",
        "primary_keys": ["patient_id"],
        # An absent extension must land 0, not NULL. The MERGE writes the view
        # value on both INSERT and UPDATE, so for dw_is_deleted — NOT NULL on
        # fhir.Patient — a NULL fails the statement outright; for
        # address_protection a `= 0` filter would silently drop the row.
        "boolean_false_default_columns": ["address_protection", "dw_is_deleted"],
    },
    "patient_address_view": {
        "view_name": "patient_address_view",
        "resource_table": "patient",
        "id_column": "patient_id",
        "mssql_table": "Patient_Address",
        # Period columns are DATETIME2(0), normalised at load.
        "primary_keys": ["patient_id", "address_period_start"],
        "normalized_datetime_columns": ["address_period_start", "address_period_end"],
        # Absent period.start → 1900-01-01. period.end NULL = still current.
        "null_sentinel_columns": ["address_period_start"],
        "validation_trigger_column": "address_period_start",
        "deduplicate_by_pk": True,
        "dedup_tiebreak_columns": ["last_updated", "address_period_end"],
        # dw_is_deleted is set by child-grain reconcile, not the resource.
        "resurrect_soft_deleted": True,
        "child_grain_reconcile": True,
    },
    "encounter_view": {
        "view_name": "encounter_view",
        "resource_table": "encounter",
        "id_column": "encounter_id",
        "mssql_table": "Encounter",
        "primary_keys": ["encounter_id"],
        # Absent isDeleted extension must land 0, not NULL -- see patient_view.
        "boolean_false_default_columns": ["dw_is_deleted"],
    },
    "encounter_location_view": {
        "view_name": "encounter_location_view",
        "resource_table": "encounter",
        "id_column": "encounter_id",
        "mssql_table": "Encounter_Location",
        "primary_keys": ["encounter_id", "location_id", "location_period_start"],
        "null_sentinel_columns": ["location_id", "location_period_start"],
        "validation_trigger_column": "location_id",
        "required_columns_when_trigger_present": ["location_period_start"],
        "deduplicate_by_pk": True,
        "dedup_tiebreak_columns": ["last_updated", "created_at", "location_period_end"],
        # dw_is_deleted is not projected by this view: dqa_fhir_server_cleanup
        # sets it to 1, and this MERGE is the only thing that writes 0 back.
        # Without it a retired row stays invisible even when the resource
        # returns.
        "resurrect_soft_deleted": True,
    },
    "observation_view": {
        "view_name": "observation_view",
        "resource_table": "observation",
        "id_column": "observation_id",
        "mssql_table": "Observation",
        "primary_keys": ["observation_id"],
        # Absent isDeleted extension must land 0, not NULL -- see patient_view.
        "boolean_false_default_columns": ["dw_is_deleted"],
    },
    "documentreference_view": {
        "view_name": "documentreference_view",
        "resource_table": "documentreference",
        "id_column": "documentreference_id",
        "mssql_table": "DocumentReference",
        "primary_keys": ["documentreference_id"],
        # Absent isDeleted extension must land 0, not NULL -- see patient_view.
        "boolean_false_default_columns": ["dw_is_deleted"],
    },
    "documentreference_recipient_view": {
        "view_name": "documentreference_recipient_view",
        "resource_table": "documentreference",
        "id_column": "documentreference_id",
        "mssql_table": "DocumentReference_Recipient",
        "primary_keys": ["documentreference_id", "recipient_key"],
        "null_sentinel_columns": ["recipient_key"],
        # dw_is_deleted is not projected by this view: dqa_fhir_server_cleanup
        # sets it to 1, and this MERGE is the only thing that writes 0 back.
        # Without it a retired row stays invisible even when the resource
        # returns.
        "resurrect_soft_deleted": True,
    },
    "episode_of_care_view": {
        "view_name": "episode_of_care_view",
        "resource_table": "episodeofcare",
        "id_column": "episode_of_care_id",
        "mssql_table": "EpisodeOfCare",
        "primary_keys": ["episode_of_care_id"],
        # Absent isDeleted extension must land 0, not NULL -- see patient_view.
        "boolean_false_default_columns": ["dw_is_deleted"],
    },
    "location_view": {
        "view_name": "location_view",
        "resource_table": "location",
        "id_column": "location_id",
        "mssql_table": "Location",
        "primary_keys": ["location_id"],
        # dw_is_deleted is not projected by this view: dqa_fhir_server_cleanup
        # sets it to 1, and this MERGE is the only thing that writes 0 back.
        # Without it a retired row stays invisible even when the resource
        # returns.
        "resurrect_soft_deleted": True,
    },
    "organization_view": {
        "view_name": "organization_view",
        "resource_table": "organization",
        "id_column": "organization_id",
        "mssql_table": "Organization",
        "primary_keys": ["organization_id"],
        # Warehouse-managed flag: Aidbox has no isDeleted on Organization.
        # Full extract marks UUIDs missing from the server as deleted, then
        # MERGE resurrects rows still present. Mark-before-MERGE is required
        # so the live-SOR unique index can accept a new UUID for the same code.
        "resurrect_soft_deleted": True,
        "resource_grain_reconcile": True,
    },
    "careplan_view": {
        "view_name": "careplan_view",
        "resource_table": "careplan",
        "id_column": "careplan_id",
        "mssql_table": "CarePlan",
        "primary_keys": ["careplan_id"],
        # dw_is_deleted is not projected by this view: dqa_fhir_server_cleanup
        # sets it to 1, and this MERGE is the only thing that writes 0 back.
        # Without it a retired row stays invisible even when the resource
        # returns.
        "resurrect_soft_deleted": True,
    },
    "careplan_activity_view": {
        "view_name": "careplan_activity_view",
        "resource_table": "careplan",
        "id_column": "careplan_id",
        "mssql_table": "CarePlan_Activity",
        "primary_keys": ["ifdb_eventmarker_id"],
        # dw_is_deleted is not projected by this view: dqa_fhir_server_cleanup
        # sets it to 1, and this MERGE is the only thing that writes 0 back.
        # Without it a retired row stays invisible even when the resource
        # returns.
        "resurrect_soft_deleted": True,
    },
    "procedure_view": {
        "view_name": "procedure_view",
        "resource_table": "procedure",
        "id_column": "procedure_id",
        "mssql_table": "Procedure",
        "primary_keys": ["procedure_id"],
        # Absent isDeleted extension must land 0, not NULL -- see patient_view.
        "boolean_false_default_columns": ["dw_is_deleted"],
    },
    "procedure_tillaegskode_view": {
        "view_name": "procedure_tillaegskode_view",
        "resource_table": "procedure",
        "id_column": "procedure_id",
        "mssql_table": "Procedure_tillaegskode",
        "primary_keys": ["procedure_id", "tillaegskode_kode", "tillaegskode_system"],
        "null_sentinel_columns": ["tillaegskode_kode", "tillaegskode_system"],
        # dw_is_deleted is not projected by this view: dqa_fhir_server_cleanup
        # sets it to 1, and this MERGE is the only thing that writes 0 back.
        # Without it a retired row stays invisible even when the resource
        # returns.
        "resurrect_soft_deleted": True,
    },
    "servicerequest_view": {
        "view_name": "servicerequest_view",
        "resource_table": "servicerequest",
        "id_column": "service_request_id",
        "mssql_table": "ServiceRequest",
        "primary_keys": ["service_request_id"],
        # Absent isDeleted extension must land 0, not NULL -- see patient_view.
        "boolean_false_default_columns": ["dw_is_deleted"],
    },
    "servicerequest_tillaegskode_view": {
        "view_name": "servicerequest_tillaegskode_view",
        "resource_table": "servicerequest",
        "id_column": "service_request_id",
        "mssql_table": "ServiceRequest_tillaegskode",
        "primary_keys": ["service_request_id", "tillaegskode_kode", "tillaegskode_system"],
        "null_sentinel_columns": ["tillaegskode_kode", "tillaegskode_system"],
        "deduplicate_by_pk": True,
        "dedup_tiebreak_columns": ["last_updated", "created_at"],
        # dw_is_deleted is not projected by this view: dqa_fhir_server_cleanup
        # sets it to 1, and this MERGE is the only thing that writes 0 back.
        # Without it a retired row stays invisible even when the resource
        # returns.
        "resurrect_soft_deleted": True,
    },
    # Rehabilitation_Plan-receivers. forEach over the receivers extension; only
    # source_table='REHABILITATION_PLAN' ServiceRequests carry it, so HENVISNING/
    # RIS rows yield 0 child rows (plain forEach = inner-join semantics).
    "servicerequest_recipient_view": {
        "view_name": "servicerequest_recipient_view",
        "resource_table": "servicerequest",
        "id_column": "service_request_id",
        "mssql_table": "ServiceRequest_Recipient",
        "primary_keys": ["service_request_id", "recipient_key"],
        "null_sentinel_columns": ["recipient_key"],
        # dw_is_deleted is not projected by this view: dqa_fhir_server_cleanup
        # sets it to 1, and this MERGE is the only thing that writes 0 back.
        # Without it a retired row stays invisible even when the resource
        # returns.
        "resurrect_soft_deleted": True,
    },
    # One row per Condition resource (PK: condition_id). code/code_system/
    # code_display carry the PRIMARY diagnosis coding, selected via
    # code.coding.where(userSelected = true) — supplementary tillaegskoder
    # (incl. ATC medication codes) are NOT projected here.
    "condition_view": {
        "view_name": "condition_view",
        "resource_table": "condition",
        "id_column": "condition_id",
        "mssql_table": "Condition",
        "primary_keys": ["condition_id"],
        # Absent isDeleted extension must land 0, not NULL -- see patient_view.
        "boolean_false_default_columns": ["dw_is_deleted"],
    },
    "condition_tillaegskode_view": {
        "view_name": "condition_tillaegskode_view",
        "resource_table": "condition",
        "id_column": "condition_id",
        "mssql_table": "Condition_tillaegskode",
        "primary_keys": ["condition_id", "tillaegskode_kode", "tillaegskode_system"],
        "null_sentinel_columns": ["tillaegskode_kode", "tillaegskode_system"],
        # dw_is_deleted is not projected by this view: dqa_fhir_server_cleanup
        # sets it to 1, and this MERGE is the only thing that writes 0 back.
        # Without it a retired row stays invisible even when the resource
        # returns.
        "resurrect_soft_deleted": True,
    },
    "appointment_view": {
        "view_name": "appointment_view",
        "resource_table": "appointment",
        "id_column": "appointment_id",
        "mssql_table": "Appointment",
        "primary_keys": ["appointment_id"],
        # Absent isDeleted extension must land 0, not NULL -- see patient_view.
        "boolean_false_default_columns": ["dw_is_deleted"],
    },
    "questionnaire_view": {
        "view_name": "questionnaire_view",
        "resource_table": "questionnaire",
        "id_column": "questionnaire_id",
        "mssql_table": "Questionnaire",
        "primary_keys": ["questionnaire_id"],
        # dw_is_deleted is not projected by this view: dqa_fhir_server_cleanup
        # sets it to 1, and this MERGE is the only thing that writes 0 back.
        # Without it a retired row stays invisible even when the resource
        # returns.
        "resurrect_soft_deleted": True,
    },
    "questionnaire_item_view": {
        "view_name": "questionnaire_item_view",
        "resource_table": "questionnaire",
        "id_column": "questionnaire_id",
        "mssql_table": "Questionnaire_item",
        "primary_keys": ["questionnaire_id", "link_id", "item_key"],
        "null_sentinel_columns": ["link_id", "item_key"],
        # dw_is_deleted is not projected by this view: dqa_fhir_server_cleanup
        # sets it to 1, and this MERGE is the only thing that writes 0 back.
        # Without it a retired row stays invisible even when the resource
        # returns.
        "resurrect_soft_deleted": True,
    },
    "questionnaire_response_view": {
        "view_name": "questionnaire_response_view",
        "resource_table": "questionnaireresponse",
        "id_column": "qr_id",
        "mssql_table": "QuestionnaireResponse",
        "primary_keys": ["qr_id"],
        # dw_is_deleted is not projected by this view: dqa_fhir_server_cleanup
        # sets it to 1, and this MERGE is the only thing that writes 0 back.
        # Without it a retired row stays invisible even when the resource
        # returns.
        "resurrect_soft_deleted": True,
    },
    "questionnaire_response_item_view": {
        "view_name": "questionnaire_response_item_view",
        "resource_table": "questionnaireresponse",
        "id_column": "qr_id",
        "mssql_table": "QuestionnaireResponse_item",
        "primary_keys": ["qr_id", "link_id", "item_key", "answer_index"],
        "null_sentinel_columns": ["link_id", "item_key", "answer_index"],
        # dw_is_deleted is not projected by this view: dqa_fhir_server_cleanup
        # sets it to 1, and this MERGE is the only thing that writes 0 back.
        # Without it a retired row stays invisible even when the resource
        # returns.
        "resurrect_soft_deleted": True,
    },
}


def _questionnaire_canonical_expressions(alias: str) -> dict[str, str]:
    """Column -> SQL expression deriving canonical/version from [questionnaire].

    Single source of truth shared by the MERGE (which normalizes rows as they
    are inserted/updated) and the post-merge safety-net UPDATE (which heals
    any historical rows and guarantees the invariant).
    """
    q = f"{alias}.[questionnaire]"
    return {
        "questionnaire_canonical": (
            f"CASE WHEN {q} IS NULL THEN NULL "
            f"WHEN CHARINDEX(N'|', {q}) > 0 "
            f"THEN LEFT({q}, CHARINDEX(N'|', {q}) - 1) "
            f"ELSE {q} END"
        ),
        "questionnaire_version": (
            f"CASE WHEN {q} IS NULL THEN NULL "
            f"WHEN CHARINDEX(N'|', {q}) > 0 "
            f"THEN SUBSTRING({q}, CHARINDEX(N'|', {q}) + 1, LEN({q})) "
            f"ELSE NULL END"
        ),
    }


def _build_questionnaire_response_key_normalization_sql(source_table: str) -> str:
    """Build scoped normalization SQL for QuestionnaireResponse canonical/version.

    Since the MERGE started computing canonical/version inline this is a
    safety net: it matches only rows whose stored values drifted (e.g. rows
    from before the inline computation existed) and normally updates 0 rows.
    """
    exprs = _questionnaire_canonical_expressions("qr")
    return f"""
        UPDATE qr
        SET
            [questionnaire_canonical] = {exprs["questionnaire_canonical"]},
            [questionnaire_version] = {exprs["questionnaire_version"]},
            [dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
        FROM [fhir].[QuestionnaireResponse] AS qr
        INNER JOIN {source_table} AS src
            ON src.[qr_id] = qr.[qr_id]
        WHERE
            qr.[questionnaire] IS NOT NULL
            AND (
                qr.[questionnaire_canonical] IS NULL
                OR qr.[questionnaire_canonical] <> {exprs["questionnaire_canonical"]}
                OR ISNULL(qr.[questionnaire_version], N'')
                    <> ISNULL({exprs["questionnaire_version"]}, N'')
            );
    """


# =============================================================================
# ViewDefinition Management
# =============================================================================


def load_view_definitions() -> list[dict[str, Any]]:
    """
    Load all ViewDefinition JSON files from the view_definitions directory.

    Returns:
        List of ViewDefinition resource dicts.
    """
    view_defs = []
    for json_file in sorted(VIEW_DEFINITIONS_DIR.glob("*.json")):
        with open(json_file) as f:
            view_defs.append(json.load(f))
    return view_defs


def existing_sof_view_names(fhir_database) -> set[str]:
    """Names of the SQL views currently present in the ``sof`` schema."""
    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT viewname FROM pg_views WHERE schemaname = 'sof'")
            return {row[0] for row in cur.fetchall()}


def server_view_definition_matches(
    fhir_server: FHIRServerResource,
    view_def: dict[str, Any],
    context: dg.AssetExecutionContext | None = None,
) -> bool:
    """True when the server's stored ViewDefinition equals *view_def*.

    Compares only the keys we author (the server adds ``id``/``meta`` etc.).
    Any error (404, network) counts as "does not match" so the caller falls
    back to the full PUT + $materialize path.
    """
    name = view_def["name"]
    url = f"{fhir_server.base_url.rstrip('/')}/ViewDefinition/{name}"
    try:
        response = fhir_server.session.get(url, headers={"Accept": "application/json"}, timeout=30)
        if not response.ok:
            return False
        server_def = response.json()
    except Exception as exc:
        if context:
            context.log.warning(f"GET ViewDefinition/{name} failed ({exc}); will re-PUT.")
        return False
    return all(server_def.get(key) == value for key, value in view_def.items())


def ensure_view_definition(
    fhir_server: FHIRServerResource,
    view_def: dict[str, Any],
    context: dg.AssetExecutionContext | None = None,
) -> None:
    """
    Ensure a ViewDefinition exists and is materialized in Aidbox.

    1. PUT /ViewDefinition/{name} to create or update the resource
    2. POST /fhir/ViewDefinition/{name}/$materialize to materialize the SQL view

    Args:
        fhir_server: The FHIR server resource (provides HTTP session + base_url).
        view_def: The ViewDefinition resource dict (must include "name").
        context: Optional Dagster context for logging.
    """
    name = view_def["name"]
    base_url = fhir_server.base_url.rstrip("/")
    session = fhir_server.session

    # Step 1: Create/update the ViewDefinition resource
    url = f"{base_url}/ViewDefinition/{name}"
    if context:
        context.log.info(f"PUT ViewDefinition at {url}")
    response = session.put(
        url,
        json=view_def,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    if not response.ok and context:
        context.log.error(
            f"PUT ViewDefinition/{name} failed ({response.status_code}): {response.text[:2000]}"
        )
    response.raise_for_status()

    # Step 2: Materialize the SQL view in the sof schema
    materialize_url = f"{base_url}/fhir/ViewDefinition/{name}/$materialize"
    body = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "type", "valueCode": "view"},
        ],
    }
    if context:
        context.log.info(f"POST $materialize at {materialize_url}")
    mat_response = session.post(
        materialize_url,
        json=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    if not mat_response.ok and context:
        context.log.error(
            f"$materialize ViewDefinition/{name} failed "
            f"({mat_response.status_code}): {mat_response.text[:2000]}"
        )
    mat_response.raise_for_status()
    if context:
        context.log.info(f"ViewDefinition '{name}' materialized successfully")


def _validate_sof_columns(
    view_name: str,
    columns: list[str],
    required_columns: list[str],
) -> None:
    """Fail early when Aidbox returns an older materialized view shape."""
    missing = [column for column in required_columns if column not in columns]
    if not missing:
        return

    available = ", ".join(columns) if columns else "<none>"
    raise RuntimeError(
        f"sof.{view_name} did not return required column(s): {', '.join(missing)}. "
        "The Aidbox ViewDefinition is likely stale; run fhir_sof_infrastructure "
        "to PUT and materialize the latest ViewDefinitions. "
        f"Returned columns: {available}"
    )


# =============================================================================
# Index Validation
# =============================================================================


def check_sof_required_indexes(
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext | None = None,
) -> list[str]:
    """
    Verify that every ``sof.*`` view's underlying resource table has the
    ``(ts) INCLUDE (cts)`` index the SoF extract path relies on.

    Views themselves cannot be indexed in PostgreSQL — only materialised views
    can. The SoF views are regular views over the raw resource tables, so the
    relevant index lives on the *underlying* table. This function:

    1. Enumerates all views in the ``sof`` schema via ``pg_views``.
    2. Resolves each view's directly-referenced base table(s) via
       ``pg_depend``/``pg_rewrite``.
    3. Checks ``pg_indexes`` for a btree on ``(ts)`` covering ``cts`` on each
       base table.
    4. Logs a warning per missing index and returns the list of warnings.

    Returns the list of warning messages (empty if all indexes are present).
    """
    log = context.log if context else dg.get_dagster_logger()

    # View → distinct base tables it reads from (one row per dependency).
    view_deps_sql = """
        SELECT DISTINCT
            v.viewname              AS view_name,
            base_ns.nspname         AS base_schema,
            base.relname            AS base_table
        FROM pg_views v
        JOIN pg_class vc
          ON vc.relname = v.viewname
         AND vc.relnamespace = (
             SELECT oid FROM pg_namespace WHERE nspname = v.schemaname
         )
        JOIN pg_rewrite rw      ON rw.ev_class = vc.oid
        JOIN pg_depend d        ON d.objid = rw.oid
                               AND d.classid = 'pg_rewrite'::regclass
        JOIN pg_class base      ON base.oid = d.refobjid
                               AND base.relkind = 'r'
        JOIN pg_namespace base_ns ON base_ns.oid = base.relnamespace
        WHERE v.schemaname = 'sof'
          AND base.oid <> vc.oid
        ORDER BY v.viewname, base.relname
    """

    # An index qualifies if it is a btree on (ts) and INCLUDEs cts.
    # We approximate this by the naming convention idx_<table>_ts_cts
    # and by inspecting indexdef.
    index_exists_sql = """
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = %s
          AND tablename  = %s
          AND indexdef ILIKE '%%(ts)%%'
          AND indexdef ILIKE '%%INCLUDE%%cts%%'
        LIMIT 1
    """

    warnings: list[str] = []
    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(view_deps_sql)
            deps = cur.fetchall()

            if not deps:
                log.info("No sof.* views found; nothing to validate")
                return warnings

            # Group by base table — one table backs several views,
            # but we only need to validate the index once per table.
            tables_seen: dict[tuple[str, str], list[str]] = {}
            for view_name, base_schema, base_table in deps:
                tables_seen.setdefault((base_schema, base_table), []).append(view_name)

            for (base_schema, base_table), view_names in sorted(tables_seen.items()):
                cur.execute(index_exists_sql, (base_schema, base_table))
                if cur.fetchone():
                    log.debug(
                        f"[SOF Index Check] OK {base_schema}.{base_table} "
                        f"(serves: {', '.join(view_names)})"
                    )
                    continue

                msg = (
                    f"Missing (ts) INCLUDE (cts) btree on "
                    f"{base_schema}.{base_table} — required by sof views: "
                    f"{', '.join(view_names)}. "
                    f"Run the full_infrastructure_setup job or create manually: "
                    f"CREATE INDEX CONCURRENTLY idx_{base_table}_ts_cts "
                    f"ON {base_schema}.{base_table} (ts) INCLUDE (cts);"
                )
                log.warning(f"[SOF Index Check] {msg}")
                warnings.append(msg)

    if not warnings and tables_seen:
        log.info(
            f"[SOF Index Check] All {len(tables_seen)} sof base tables "
            f"have the required (ts) INCLUDE (cts) index"
        )

    return warnings


# =============================================================================
# PostgreSQL Extraction
# =============================================================================


def _build_sof_extract_query(
    view_name: str,
    resource_table: str,
    id_column: str,
    partition_start: str | None = None,
    partition_end: str | None = None,
) -> tuple[str, list[Any]]:
    """Build SQL and params for extracting rows from a materialized SoF view."""
    sql = f"""
        SELECT v.*, r.ts AS last_updated, r.cts AS created_at
        FROM sof.{view_name} v
        JOIN {resource_table} r ON r.id = v.{id_column}
    """

    params: list[Any] = []
    if partition_start and partition_end:
        sql += " WHERE r.ts >= %s AND r.ts < %s"
        params = [partition_start, partition_end]
    elif partition_start:
        sql += " WHERE r.ts >= %s"
        params = [partition_start]

    # No ORDER BY: rows are staged into #SofTemp (insertion order is irrelevant),
    # then a CLUSTERED INDEX on primary keys is built, then MERGE runs set-based
    # against fhir.<table>. Sorting in PostgreSQL would force the planner to
    # materialise the full result before the first FETCH could return — the
    # measured cost on a 60K-row partition was ~15 minutes for the first chunk
    # and ~0.3s for all remaining chunks combined. With ORDER BY removed the
    # server-side cursor can stream rows directly from the index scan.
    return sql, params


def _build_changed_parent_ids_query(
    resource_table: str,
    partition_start: str | None = None,
    partition_end: str | None = None,
) -> tuple[str, list[Any]]:
    """Parent resource ids in the same ts window as the child-grain extract."""
    sql = f"SELECT r.id FROM {resource_table} r"
    params: list[Any] = []
    if partition_start and partition_end:
        sql += " WHERE r.ts >= %s AND r.ts < %s"
        params = [partition_start, partition_end]
    elif partition_start:
        sql += " WHERE r.ts >= %s"
        params = [partition_start]
    return sql, params


def build_child_grain_mark_deleted_sql(
    table_name: str,
    parent_id_column: str,
    *,
    full_load: bool,
    ids_table: str = "#ReconcileParents",
) -> str:
    """Soft-delete live child rows for changed parents (or the whole table)."""
    target = f"[fhir].[{table_name}]"
    timestamp = "CAST(SYSDATETIME() AS datetime2(0))"
    if full_load:
        return (
            f"UPDATE {target} SET [dw_is_deleted] = 1, "
            f"[dw_timestamp_update] = {timestamp} "
            f"WHERE [dw_is_deleted] = 0"
        )
    return (
        f"UPDATE tgt SET tgt.[dw_is_deleted] = 1, "
        f"tgt.[dw_timestamp_update] = {timestamp} "
        f"FROM {target} AS tgt "
        f"INNER JOIN {ids_table} AS ids ON ids.[id] = tgt.[{parent_id_column}] "
        f"WHERE tgt.[dw_is_deleted] = 0"
    )


def build_resource_grain_mark_deleted_sql(
    table_name: str,
    id_column: str,
    *,
    source_table: str = "#SofTemp",
) -> str:
    """Soft-delete live resource rows absent from the staged extract.

    The anti-join is on ``id_column`` alone, so this is only a PK-grain reconcile
    for resources whose MERGE key IS that single column. The caller enforces
    ``primary_keys == [id_column]`` before reaching here — a composite-key resource
    would match on a prefix and retire rows that are still present.
    """
    target = f"[fhir].[{table_name}]"
    timestamp = "CAST(SYSDATETIME() AS datetime2(0))"
    return (
        f"UPDATE tgt SET tgt.[dw_is_deleted] = 1, "
        f"tgt.[dw_timestamp_update] = {timestamp} "
        f"FROM {target} AS tgt "
        f"WHERE tgt.[dw_is_deleted] = 0 "
        f"AND NOT EXISTS ("
        f"SELECT 1 FROM {source_table} AS s "
        f"WHERE s.[{id_column}] = tgt.[{id_column}]"
        f")"
    )


def _fetch_changed_parent_ids(
    pg_conn: Any,
    resource_table: str,
    partition_start: str | None,
    partition_end: str | None,
) -> list[Any]:
    """Load parent ids before the named extract cursor occupies the connection."""
    sql, params = _build_changed_parent_ids_query(resource_table, partition_start, partition_end)
    with pg_conn.cursor() as cursor:
        cursor.execute(sql, params or None)
        return [row[0] for row in cursor.fetchall() if row and row[0] is not None]


def _reconcile_parents_create_sql(ids_table: str = "#ReconcileParents") -> str:
    """CREATE for the delta parent-id temp table, collated to the staging DB."""
    return f"CREATE TABLE {ids_table} ([id] NVARCHAR(255){_TEMP_COLLATE} NOT NULL PRIMARY KEY)"


def _mark_child_rows_deleted(
    mssql_cursor: Any,
    table_name: str,
    parent_id_column: str,
    *,
    full_load: bool,
    parent_ids: list[Any],
) -> int:
    """Stage parent ids and flag live child rows deleted. Returns rows updated."""
    if not full_load and not parent_ids:
        return 0
    ids_table = "#ReconcileParents"
    if full_load:
        mssql_cursor.execute(
            build_child_grain_mark_deleted_sql(table_name, parent_id_column, full_load=True)
        )
        return mssql_cursor.rowcount
    mssql_cursor.execute(f"DROP TABLE IF EXISTS {ids_table}")
    mssql_cursor.execute(_reconcile_parents_create_sql(ids_table))
    insert_sql = f"INSERT INTO {ids_table} ([id]) VALUES (?)"
    batch_size = 1000
    for offset in range(0, len(parent_ids), batch_size):
        batch = [(pid,) for pid in parent_ids[offset : offset + batch_size]]
        mssql_cursor.executemany(insert_sql, batch)
    mssql_cursor.execute(
        build_child_grain_mark_deleted_sql(
            table_name, parent_id_column, full_load=False, ids_table=ids_table
        )
    )
    return mssql_cursor.rowcount


def extract_sof_view(
    fhir_database: FHIRDatabaseResource,
    view_name: str,
    resource_table: str,
    id_column: str,
    partition_start: str | None = None,
    partition_end: str | None = None,
    context: dg.AssetExecutionContext | None = None,
) -> list[dict[str, Any]]:
    """
    Extract flat data from a SQL on FHIR view, joining with the raw resource
    table to get last_updated (r.ts) and created_at (r.cts) timestamps.

    SQL pattern:
        SELECT v.*, r.ts AS last_updated, r.cts AS created_at
        FROM sof.{view_name} v
        JOIN {resource_table} r ON r.id = v.{id_column}
        [WHERE r.ts >= partition_start AND r.ts < partition_end]

    Args:
        fhir_database: PostgreSQL database resource for direct access.
        view_name: Name of the sof view (e.g., "patient_view").
        resource_table: Raw resource table name (e.g., "patient").
        id_column: Column in the view that matches r.id (e.g., "patient_id").
        partition_start: Optional partition start timestamp (ISO format).
        partition_end: Optional partition end timestamp (ISO format).
        context: Optional Dagster context for logging.

    Returns:
        List of row dictionaries.
    """
    sql, params = _build_sof_extract_query(
        view_name=view_name,
        resource_table=resource_table,
        id_column=id_column,
        partition_start=partition_start,
        partition_end=partition_end,
    )

    if context:
        context.log.info(
            f"Querying sof.{view_name} (partition: {partition_start} -> {partition_end})"
        )

    with fhir_database.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params or None)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

    result = [dict(zip(columns, row, strict=False)) for row in rows]

    if context:
        context.log.info(f"Extracted {len(result)} rows from sof.{view_name}")

    return result


def extract_and_merge_sof_view(
    fhir_database: FHIRDatabaseResource,
    mssql_target: MSSQLResource,
    view_name: str,
    resource_table: str,
    id_column: str,
    table_name: str,
    primary_keys: list[str],
    partition_start: str | None = None,
    partition_end: str | None = None,
    context: dg.AssetExecutionContext | None = None,
    null_sentinel_columns: list[str] | None = None,
    normalized_datetime_columns: list[str] | None = None,
    fetch_chunk_size: int = SOF_STREAM_FETCH_CHUNK_SIZE,
    soft_delete_threshold: int | None = None,
) -> tuple[int, int]:
    """Stream SoF rows directly into MSSQL temp table and MERGE.

    This path avoids loading the full extract into Python memory. It mirrors the
    chunked approach used by the MSSQL extractor: stream chunks, stage them, then
    do one set-based MERGE.

    Args:
        null_sentinel_columns: Columns where an absent value must become a
            sentinel rather than NULL, because they participate in a composite
            primary key. For a column also listed in
            ``normalized_datetime_columns`` the sentinel is
            ``PERIOD_START_SENTINEL``; otherwise it is the empty string.
        normalized_datetime_columns: Columns holding a raw FHIR dateTime that the
            target stores as DATETIME2(0). Rewritten in place via
            ``parse_fhir_datetime`` before staging, so the DWH never keys on the
            string form -- see ``period_keys`` for why that mapping is
            many-to-one and therefore unusable as a key.

    Returns:
        Tuple: (source_row_count, rows_affected_by_merge)
    """
    sql, params = _build_sof_extract_query(
        view_name=view_name,
        resource_table=resource_table,
        id_column=id_column,
        partition_start=partition_start,
        partition_end=partition_end,
    )

    if context:
        context.log.info(
            f"Streaming sof.{view_name} (partition: {partition_start} -> {partition_end})"
        )

    source_row_count = 0
    rows_affected = 0
    table_cfg = SOF_RESOURCE_CONFIGS.get(view_name)
    child_grain_reconcile = bool(table_cfg and table_cfg.get("child_grain_reconcile"))
    resource_grain_reconcile = bool(table_cfg and table_cfg.get("resource_grain_reconcile"))
    if resource_grain_reconcile and primary_keys != [id_column]:
        # The reconcile anti-joins on id_column alone. On a composite key that
        # matches a prefix, so rows still present in the extract would be retired.
        raise dg.Failure(
            description=(
                f"resource_grain_reconcile for fhir.{table_name} requires the MERGE key "
                f"to be the single id_column: primary_keys={primary_keys}, "
                f"id_column={id_column!r}. Use child_grain_reconcile, or widen the "
                "reconcile anti-join to the full key, before enabling this flag."
            ),
        )
    reconcile_deletes = child_grain_reconcile or resource_grain_reconcile
    full_load = partition_start is None
    parent_ids: list[Any] = []

    with fhir_database.get_connection() as pg_conn:
        # Pin the cursor planner to the bare-SELECT plan. The PG connection may
        # be pooled, so this is session-scoped and reset in a finally below.
        _ctf_original: str | None = None
        try:
            with pg_conn.cursor() as _ctf_cur:
                _ctf_cur.execute("SHOW cursor_tuple_fraction")
                _ctf_original = _ctf_cur.fetchone()[0]
                _ctf_cur.execute(f"SET cursor_tuple_fraction = {SOF_CURSOR_TUPLE_FRACTION}")
        except Exception as _ctf_exc:
            if context:
                context.log.warning(f"[SOF_DIAG] could not pin cursor_tuple_fraction: {_ctf_exc}")

        # Diagnostic: log the (now pinned) cursor planner setting and the
        # bare-SELECT EXPLAIN top so any future divergence is visible.
        if context:
            try:
                with pg_conn.cursor() as _diag_cur:
                    _diag_cur.execute("SHOW cursor_tuple_fraction")
                    _ctf = _diag_cur.fetchone()[0]
                    _diag_cur.execute(f"EXPLAIN {sql}", params or None)
                    _plan_rows = [r[0] for r in _diag_cur.fetchall()]
                context.log.info(
                    f"[SOF_DIAG] cursor_tuple_fraction={_ctf} "
                    f"(was {_ctf_original}) "
                    f"bare_select_plan_top={_plan_rows[0] if _plan_rows else 'n/a'}"
                )
            except Exception as _diag_exc:
                context.log.info(f"[SOF_DIAG] plan probe failed: {_diag_exc}")

        # Parent ids must be loaded before DECLARE CURSOR occupies the
        # connection. The view extract misses patients who now have zero
        # address rows; those still need mark-all.
        if child_grain_reconcile and not full_load:
            parent_ids = _fetch_changed_parent_ids(
                pg_conn, resource_table, partition_start, partition_end
            )
            if context:
                context.log.info(
                    f"Child-grain reconcile: {len(parent_ids)} changed "
                    f"{resource_table} id(s) since {partition_start}"
                )

        cursor_name = f"sof_stream_{uuid.uuid4().hex}"
        with pg_conn.cursor(name=cursor_name) as pg_cursor:
            # TIMING: server-side DECLARE CURSOR — should return in seconds
            # since PostgreSQL doesn't materialise the result up front.
            _t_declare_start = time.perf_counter()
            pg_cursor.execute(sql, params or None)
            _t_declare = time.perf_counter() - _t_declare_start
            columns = [desc[0] for desc in (pg_cursor.description or [])]
            if context:
                context.log.info(
                    f"[SOF_TIMING] declare_cursor elapsed={_t_declare:.2f}s "
                    f"cursor={cursor_name} columns={len(columns)}"
                )
            if not columns:
                if context:
                    context.log.info(f"No columns returned from sof.{view_name}; skipping merge")
                return 0, 0
            _validate_sof_columns(view_name, columns, primary_keys)

            normalized_names = set(normalized_datetime_columns or [])
            sentinel_names = set(null_sentinel_columns or [])
            false_default_names = set((table_cfg or {}).get("boolean_false_default_columns") or [])
            # Rewritten in place, so the temp table DDL, INSERT and bulkcopy
            # mappings — all driven off `columns` — need no adjustment.
            normalize_indexes = [idx for idx, col in enumerate(columns) if col in normalized_names]
            # An absent value in a normalised column becomes the datetime
            # sentinel; the empty-string sentinel only applies to columns that
            # stay character-typed.
            datetime_sentinel_indexes = {
                idx
                for idx, col in enumerate(columns)
                if col in normalized_names and col in sentinel_names
            }
            sentinel_indexes = [
                idx
                for idx, col in enumerate(columns)
                if col in sentinel_names and col not in normalized_names
            ]
            false_default_indexes = [
                idx for idx, col in enumerate(columns) if col in false_default_names
            ]

            unparseable_count = 0
            unparseable_samples: list[str] = []

            _t_mssql_conn_start = time.perf_counter()
            with mssql_target.get_connection() as mssql_conn:
                _t_mssql_conn = time.perf_counter() - _t_mssql_conn_start
                mssql_cursor = mssql_conn.cursor()
                used_bulkcopy = hasattr(mssql_cursor, "bulkcopy")
                bulkcopy_disabled = False
                chunk_num = 0
                if context:
                    _server = getattr(mssql_target, "server", None) or getattr(
                        mssql_target, "host", "?"
                    )
                    _db = getattr(mssql_target, "database", "?")
                    context.log.info(
                        f"[SOF_TIMING] mssql_connect elapsed={_t_mssql_conn:.2f}s "
                        f"target={_server}/{_db} bulkcopy_available={used_bulkcopy}"
                    )
                # TIMING accumulators (one number per phase summed across chunks)
                _t_fetch_total = 0.0
                _t_prep_total = 0.0
                _t_stage_total = 0.0
                # mssql-python's cursor.bulkcopy() opens its own PyCoreConnection
                # under the hood, so a local temp table (#SofTemp, session-scoped)
                # is invisible to it — the driver fails with "Invalid object name
                # '#SofTemp'" when looking up destination metadata. Use a global
                # temp table with a uuid suffix so it is visible across sessions
                # while still being unique per run.
                stage_table = f"##SofTemp_{uuid.uuid4().hex}"
                try:
                    _t_colmap_start = time.perf_counter()
                    col_type_map = _get_target_column_types(
                        mssql_cursor, table_name, placeholder="?"
                    )
                    _t_colmap = time.perf_counter() - _t_colmap_start
                    col_defs = _temp_table_col_defs(columns, col_type_map)
                    _t_create_start = time.perf_counter()
                    mssql_cursor.execute(f"CREATE TABLE {stage_table} ({col_defs})")
                    # bulkcopy() opens a second internal connection for the TDS
                    # bulk insert; it must read the staging table's metadata, so
                    # the CREATE TABLE schema lock has to be released (committed)
                    # before the first bulkcopy() call or that second connection
                    # blocks indefinitely.
                    mssql_conn.commit()
                    _t_create = time.perf_counter() - _t_create_start
                    if context:
                        context.log.info(
                            f"[SOF_TIMING] stage_table_setup col_type_map={_t_colmap:.2f}s "
                            f"create_table={_t_create:.2f}s table={stage_table}"
                        )

                    col_list = ", ".join(f"[{c}]" for c in columns)
                    placeholders = ", ".join(["?"] * len(columns))
                    temp_insert_sql = (
                        f"INSERT INTO {stage_table} ({col_list}) VALUES ({placeholders})"
                    )

                    if context:
                        strategy = "bulkcopy" if used_bulkcopy else "executemany"
                        context.log.info(
                            f"Staging into {stage_table} via {strategy} "
                            f"(fetch_chunk_size={fetch_chunk_size:,}, columns={len(columns)})"
                        )

                    while True:
                        # TIMING: FETCH from PostgreSQL — actual server roundtrip
                        _t_fetch_start = time.perf_counter()
                        chunk = pg_cursor.fetchmany(fetch_chunk_size)
                        _t_fetch = time.perf_counter() - _t_fetch_start
                        _t_fetch_total += _t_fetch
                        if not chunk:
                            break

                        # TIMING: row preparation (sentinel substitution + tuple build)
                        _t_prep_start = time.perf_counter()
                        prepared_rows: list[tuple[Any, ...]] = []
                        if sentinel_indexes or normalize_indexes or false_default_indexes:
                            for row in chunk:
                                row_values = list(row)
                                for sentinel_idx in sentinel_indexes:
                                    if row_values[sentinel_idx] is None:
                                        row_values[sentinel_idx] = ""
                                for idx in normalize_indexes:
                                    raw = row_values[idx]
                                    if is_unparseable(raw):
                                        unparseable_count += 1
                                        if len(unparseable_samples) < 5:
                                            unparseable_samples.append(str(raw))
                                    if idx in datetime_sentinel_indexes:
                                        row_values[idx] = normalize_fhir_datetime(raw)
                                    else:
                                        row_values[idx] = parse_fhir_datetime(raw)
                                for idx in false_default_indexes:
                                    if row_values[idx] is None:
                                        row_values[idx] = 0
                                prepared_rows.append(tuple(row_values))
                        else:
                            prepared_rows = [tuple(row) for row in chunk]
                        _t_prep = time.perf_counter() - _t_prep_start
                        _t_prep_total += _t_prep

                        # TIMING: MSSQL staging (bulkcopy or executemany)
                        _t_stage_start = time.perf_counter()
                        if prepared_rows:
                            if used_bulkcopy and not bulkcopy_disabled:
                                try:
                                    result = mssql_cursor.bulkcopy(
                                        stage_table,
                                        prepared_rows,
                                        batch_size=0,
                                        timeout=SOF_BULKCOPY_TIMEOUT_SECONDS,
                                        table_lock=True,
                                        column_mappings=columns,
                                    )
                                    inserted = (
                                        result.get("rows_copied", len(prepared_rows))
                                        if isinstance(result, dict)
                                        else len(prepared_rows)
                                    )
                                except Exception as exc:
                                    bulkcopy_disabled = True
                                    if context:
                                        context.log.warning(
                                            f"bulkcopy into {stage_table} failed ({exc}); "
                                            "falling back to executemany"
                                        )
                                    mssql_cursor.executemany(temp_insert_sql, prepared_rows)
                                    inserted = len(prepared_rows)
                            else:
                                mssql_cursor.executemany(temp_insert_sql, prepared_rows)
                                inserted = len(prepared_rows)

                            source_row_count += inserted
                            chunk_num += 1
                            _t_stage = time.perf_counter() - _t_stage_start
                            _t_stage_total += _t_stage
                            if context:
                                context.log.info(
                                    f"[SOF_TIMING] chunk={chunk_num} "
                                    f"rows={inserted} "
                                    f"fetch={_t_fetch:.2f}s "
                                    f"prep={_t_prep:.2f}s "
                                    f"stage={_t_stage:.2f}s "
                                    f"cum_rows={source_row_count:,} "
                                    f"cum_fetch={_t_fetch_total:.2f}s "
                                    f"cum_stage={_t_stage_total:.2f}s"
                                )
                        else:
                            _t_stage_total += time.perf_counter() - _t_stage_start

                    if context:
                        context.log.info(
                            f"[SOF_TIMING] stage_loop_done chunks={chunk_num} "
                            f"rows={source_row_count:,} "
                            f"fetch_total={_t_fetch_total:.2f}s "
                            f"prep_total={_t_prep_total:.2f}s "
                            f"stage_total={_t_stage_total:.2f}s"
                        )
                        if unparseable_count:
                            context.log.warning(
                                f"{unparseable_count:,} row(s) had an unparseable value in "
                                f"{sorted(normalized_names)} and fell back to the "
                                f"{PERIOD_START_SENTINEL:%Y-%m-%d} sentinel or NULL. Samples: "
                                f"{unparseable_samples}. Multiple unparseable values for the same "
                                f"parent collapse onto one key and are deduplicated — investigate "
                                f"before they silently drop a real period."
                            )

                    # TIMING: index + MERGE phase (set-based DSA upsert)
                    _t_merge_total = 0.0
                    _t_idx = 0.0
                    reconcile_open = False
                    try:
                        if reconcile_deletes and (full_load or parent_ids):
                            if source_row_count == 0:
                                raise dg.Failure(
                                    description=(
                                        f"Delete reconcile for fhir.{table_name}: "
                                        f"extract returned 0 rows (full_load={full_load}, "
                                        f"changed_parents={len(parent_ids)}). Refusing to "
                                        "soft-delete live rows — an empty view would "
                                        "retire every active row. Investigate the SoF view "
                                        "and Aidbox connection before retrying."
                                    ),
                                )
                            if child_grain_reconcile:
                                marked = _mark_child_rows_deleted(
                                    mssql_cursor,
                                    table_name,
                                    id_column,
                                    full_load=full_load,
                                    parent_ids=parent_ids,
                                )
                                reconcile_open = True
                                if context:
                                    context.log.info(
                                        f"Delete reconcile: marked {marked} live "
                                        f"fhir.{table_name} row(s) deleted "
                                        f"(full_load={full_load})"
                                    )

                        if source_row_count > 0:
                            pk_cols = ", ".join(f"[{pk}]" for pk in primary_keys)
                            if context:
                                context.log.info(
                                    f"[SOF_TIMING] create_index_start table={stage_table} "
                                    f"pk=({pk_cols}) rows={source_row_count:,}"
                                )
                            _t_idx_start = time.perf_counter()
                            mssql_cursor.execute(
                                f"CREATE CLUSTERED INDEX IX_SofTemp_PK ON {stage_table} ({pk_cols})"
                            )
                            _t_idx = time.perf_counter() - _t_idx_start
                            if context:
                                context.log.info(
                                    f"[SOF_TIMING] create_index_done elapsed={_t_idx:.2f}s"
                                )
                            if resource_grain_reconcile and full_load:
                                mssql_cursor.execute(
                                    f"SELECT COUNT(*) FROM [fhir].[{table_name}] "
                                    f"WHERE [dw_is_deleted] = 0"
                                )
                                live_before = mssql_cursor.fetchone()[0]
                                mssql_cursor.execute(
                                    build_resource_grain_mark_deleted_sql(
                                        table_name,
                                        id_column,
                                        source_table=stage_table,
                                    )
                                )
                                marked = mssql_cursor.rowcount
                                check_soft_delete_threshold(
                                    f"fhir.{table_name}",
                                    marked,
                                    context,
                                    override=soft_delete_threshold,
                                    live_row_count=live_before,
                                )
                                reconcile_open = True
                                if context:
                                    context.log.info(
                                        f"Delete reconcile: marked {marked} live "
                                        f"fhir.{table_name} row(s) absent from extract "
                                        f"(full_load={full_load})"
                                    )
                            dedup_enabled = bool(table_cfg and table_cfg.get("deduplicate_by_pk"))
                            dedup_tiebreak_columns = (
                                table_cfg.get("dedup_tiebreak_columns") if table_cfg else None
                            )
                            resurrect_soft_deleted = bool(
                                table_cfg and table_cfg.get("resurrect_soft_deleted")
                            )
                            if dedup_enabled:
                                mssql_cursor.execute(
                                    _build_sof_duplicate_stats_sql(stage_table, primary_keys)
                                )
                                (
                                    duplicate_group_count,
                                    duplicate_extra_row_count,
                                    duplicate_source_row_count,
                                ) = mssql_cursor.fetchone()

                                if duplicate_group_count:
                                    if context:
                                        context.log.warning(
                                            f"[SOF_DEDUP] target=fhir.{table_name} "
                                            f"duplicate_groups={duplicate_group_count:,} "
                                            f"duplicate_extra_rows={duplicate_extra_row_count:,} "
                                            f"duplicate_source_rows={duplicate_source_row_count:,} "
                                            f"pk={primary_keys}"
                                        )

                                    conflict_sql = _build_sof_duplicate_conflict_count_sql(
                                        stage_table,
                                        columns,
                                        primary_keys,
                                        dedup_tiebreak_columns,
                                    )
                                    if conflict_sql:
                                        mssql_cursor.execute(conflict_sql)
                                        conflict_group_count = mssql_cursor.fetchone()[0]
                                        if conflict_group_count:
                                            sample_sql = _build_sof_duplicate_conflict_sample_sql(
                                                stage_table,
                                                columns,
                                                primary_keys,
                                                dedup_tiebreak_columns,
                                            )
                                            mssql_cursor.execute(sample_sql)
                                            sample_keys = [
                                                tuple(row) for row in mssql_cursor.fetchall()
                                            ]
                                            raise ValueError(
                                                f"Semantic duplicate conflict in "
                                                f"[fhir].[{table_name}]: "
                                                f"{conflict_group_count} duplicate PK group(s) "
                                                f"have different non-audit values. "
                                                f"Sample keys: {sample_keys}. "
                                                "Refusing to deduplicate because this could "
                                                "hide extra downstream data."
                                            )
                            merge_sql = build_fhir_merge_sql(
                                table_name,
                                columns,
                                primary_keys,
                                source_table=stage_table,
                                deduplicate_source_by_pk=dedup_enabled,
                                dedup_tiebreak_columns=dedup_tiebreak_columns,
                                resurrect_soft_deleted=resurrect_soft_deleted,
                                computed_columns=(
                                    _questionnaire_canonical_expressions("src")
                                    if table_name == "QuestionnaireResponse"
                                    else None
                                ),
                            )
                            if context and dedup_enabled:
                                context.log.info(
                                    f"[SOF_TIMING] merge_source_dedup enabled "
                                    f"target=fhir.{table_name} pk={primary_keys}"
                                )
                            if context:
                                context.log.info(
                                    f"[SOF_TIMING] merge_start target=fhir.{table_name} "
                                    f"source={stage_table} cols={len(columns)} "
                                    f"stage_rows={source_row_count:,}"
                                )
                            _t_merge_start = time.perf_counter()
                            mssql_cursor.execute(merge_sql)
                            _t_merge_total = time.perf_counter() - _t_merge_start
                            rows_affected = mssql_cursor.rowcount
                            if context:
                                context.log.info(
                                    f"[SOF_TIMING] merge_done elapsed={_t_merge_total:.2f}s "
                                    f"rows_affected={rows_affected:,}"
                                )
                            if table_name == "QuestionnaireResponse":
                                _t_normalize_start = time.perf_counter()
                                mssql_cursor.execute(
                                    _build_questionnaire_response_key_normalization_sql(stage_table)
                                )
                                _t_normalize = time.perf_counter() - _t_normalize_start
                                if context:
                                    context.log.info(
                                        "[SOF_TIMING] questionnaire_response_key_normalized "
                                        f"elapsed={_t_normalize:.2f}s "
                                        f"rows={mssql_cursor.rowcount:,}"
                                    )

                        if reconcile_open or source_row_count > 0:
                            mssql_conn.commit()
                            reconcile_open = False
                    except Exception:
                        mssql_conn.rollback()
                        raise

                    # TIMING: temp drop + commit
                    if context:
                        context.log.info(f"[SOF_TIMING] drop_commit_start table={stage_table}")
                    _t_drop_start = time.perf_counter()
                    mssql_cursor.execute(f"DROP TABLE {stage_table}")
                    _t_drop_only = time.perf_counter() - _t_drop_start
                    _t_commit_start = time.perf_counter()
                    mssql_conn.commit()
                    _t_commit = time.perf_counter() - _t_commit_start
                    _t_drop = _t_drop_only + _t_commit
                    if context:
                        context.log.info(
                            f"[SOF_TIMING] drop_commit_done "
                            f"drop={_t_drop_only:.2f}s commit={_t_commit:.2f}s"
                        )

                    if context:
                        _t_total = (
                            _t_declare
                            + _t_fetch_total
                            + _t_prep_total
                            + _t_stage_total
                            + _t_idx
                            + _t_merge_total
                            + _t_drop
                        )
                        context.log.info(
                            f"[SOF_TIMING] SUMMARY view=sof.{view_name} "
                            f"rows={source_row_count:,} chunks={chunk_num} "
                            f"declare={_t_declare:.2f}s "
                            f"fetch_total={_t_fetch_total:.2f}s "
                            f"prep_total={_t_prep_total:.2f}s "
                            f"stage_total={_t_stage_total:.2f}s "
                            f"create_index={_t_idx:.2f}s "
                            f"merge={_t_merge_total:.2f}s "
                            f"drop={_t_drop:.2f}s "
                            f"timed_sum={_t_total:.2f}s"
                        )
                finally:
                    mssql_cursor.close()

        # Restore cursor_tuple_fraction so the pooled PG connection doesn't
        # carry the override into unrelated callers.
        if _ctf_original is not None:
            try:
                with pg_conn.cursor() as _ctf_reset:
                    _ctf_reset.execute("RESET cursor_tuple_fraction")
            except Exception as _ctf_reset_exc:
                if context:
                    context.log.warning(
                        f"[SOF_DIAG] could not reset cursor_tuple_fraction: {_ctf_reset_exc}"
                    )

    if context:
        context.log.info(
            f"Streaming merge complete for fhir.{table_name}: "
            f"source_rows={source_row_count:,}, rows_affected={rows_affected:,}"
        )

    return source_row_count, rows_affected


# =============================================================================
# MSSQL Loading
# =============================================================================

_SOF_DIFF_EXCLUDED_COLUMNS = {
    "dw_timestamp_insert",
    "dw_timestamp_update",
    "last_updated",
    "created_at",
}


def _build_payload_diff_predicate(columns: list[str], primary_keys: list[str]) -> str:
    """Return a NULL-safe predicate for meaningful source/target differences."""
    pk_names = {pk.lower() for pk in primary_keys}
    diff_cols = [
        c
        for c in columns
        if c.lower() not in pk_names and c.lower() not in _SOF_DIFF_EXCLUDED_COLUMNS
    ]

    if not diff_cols:
        return "1 = 0"

    src_cols = ", ".join(f"src.[{c}]" for c in diff_cols)
    tgt_cols = ", ".join(f"tgt.[{c}]" for c in diff_cols)
    return f"EXISTS (SELECT {src_cols} EXCEPT SELECT {tgt_cols})"


def build_fhir_merge_sql(
    table_name: str,
    columns: list[str],
    primary_keys: list[str],
    source_table: str = "#SofTemp",
    deduplicate_source_by_pk: bool = False,
    dedup_tiebreak_columns: list[str] | None = None,
    resurrect_soft_deleted: bool = False,
    computed_columns: dict[str, str] | None = None,
) -> str:
    """
    Generate a T-SQL MERGE statement for upserting into fhir.* tables.

    Uses a temp table approach: data is inserted into a temp table first,
    then MERGED into the target table.

    Args:
        table_name: Target table name (e.g., "Patient").
        columns: List of all column names to sync.
        primary_keys: Primary key columns for the MERGE ON clause.
        source_table: Source staging table name (default "#SofTemp").
        deduplicate_source_by_pk: When True, wrap the source in ROW_NUMBER()
            partitioned by the MERGE primary key and keep one row per key.
        dedup_tiebreak_columns: Optional columns used for deterministic winner
            selection when source duplicates exist.
        resurrect_soft_deleted: Clear ``dw_is_deleted`` when the source delivers
            a row again. Only for tables whose flag is set by a reconcile rather
            than projected from the resource -- where the view DOES project
            ``dw_is_deleted``, it is an ordinary payload column and already
            round-trips through the UPDATE.
        computed_columns: Optional target column -> SQL expression (over
            ``src.``) applied in both the UPDATE SET and the INSERT, for
            derived columns that are not part of the staged view (e.g.
            QuestionnaireResponse canonical/version). Not part of the diff
            predicate — they derive from columns that already are.

    Returns:
        T-SQL MERGE statement string.
    """
    target = f"[fhir].[{table_name}]"

    on_clause = " AND ".join(f"tgt.[{pk}] = src.[{pk}]" for pk in primary_keys)
    payload_diff_predicate = _build_payload_diff_predicate(columns, primary_keys)

    update_cols = [c for c in columns if c not in primary_keys]
    update_set = ", ".join(f"tgt.[{c}] = src.[{c}]" for c in update_cols)
    update_set += ", tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))"

    if resurrect_soft_deleted:
        # Soft-deleted rows still occupy the PK; a reappearing key is a MATCH.
        update_set += ", tgt.[dw_is_deleted] = 0"
        # Flag itself is a difference so an unchanged payload still resurrects.
        payload_diff_predicate = f"({payload_diff_predicate} OR tgt.[dw_is_deleted] <> 0)"

    if computed_columns:
        for col, expr in computed_columns.items():
            update_set += f", tgt.[{col}] = {expr}"

    insert_cols = ", ".join(f"[{c}]" for c in columns)
    insert_cols += ", [dw_timestamp_insert], [dw_timestamp_update]"
    insert_vals = ", ".join(f"src.[{c}]" for c in columns)
    insert_vals += ", CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))"
    if computed_columns:
        insert_cols += ", " + ", ".join(f"[{c}]" for c in computed_columns)
        insert_vals += ", " + ", ".join(computed_columns.values())

    if deduplicate_source_by_pk:
        partition_cols = ", ".join(f"[{pk}]" for pk in primary_keys)
        effective_tiebreak_cols = [
            c for c in (dedup_tiebreak_columns or ["last_updated", "created_at"]) if c in columns
        ]
        if effective_tiebreak_cols:
            order_by = ", ".join(f"[{c}] DESC" for c in effective_tiebreak_cols)
        else:
            order_by = "(SELECT 0)"
        source_cols = ", ".join(f"[{c}]" for c in columns)
        source_expr = f"""(
            SELECT {source_cols}
            FROM (
                SELECT
                    src_base.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY {partition_cols}
                        ORDER BY {order_by}
                    ) AS __sof_dedup_rn
                FROM {source_table} AS src_base
            ) AS dedup
            WHERE __sof_dedup_rn = 1
        )"""
    else:
        source_expr = source_table

    merge_sql = f"""
        MERGE {target} AS tgt
        USING {source_expr} AS src
        ON {on_clause}

        WHEN MATCHED AND {payload_diff_predicate} THEN
            UPDATE SET {update_set}

        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals});
    """
    return merge_sql


def _build_sof_duplicate_stats_sql(source_table: str, primary_keys: list[str]) -> str:
    """Return SQL that counts duplicate source groups and extra rows by PK."""
    pk_cols = ", ".join(f"[{pk}]" for pk in primary_keys)
    return f"""
        WITH duplicate_keys AS (
            SELECT {pk_cols}, COUNT_BIG(*) AS row_count
            FROM {source_table}
            GROUP BY {pk_cols}
            HAVING COUNT_BIG(*) > 1
        )
        SELECT
            COUNT_BIG(*) AS duplicate_group_count,
            COALESCE(SUM(row_count - 1), 0) AS duplicate_extra_row_count,
            COALESCE(SUM(row_count), 0) AS duplicate_source_row_count
        FROM duplicate_keys;
    """


def _semantic_duplicate_columns(
    columns: list[str],
    primary_keys: list[str],
    dedup_tiebreak_columns: list[str] | None,
) -> list[str]:
    """Columns that must be identical for duplicate PK rows to be safe to dedupe."""
    ignored = {
        *primary_keys,
        *(dedup_tiebreak_columns or []),
        "last_updated",
        "created_at",
        "dw_timestamp_insert",
        "dw_timestamp_update",
    }
    return [c for c in columns if c not in ignored]


def _build_null_safe_difference_predicate(columns: list[str]) -> str:
    """Build a null-safe SQL predicate for any payload difference."""
    return " OR ".join(
        f"((a.[{c}] IS NULL AND b.[{c}] IS NOT NULL) "
        f"OR (a.[{c}] IS NOT NULL AND b.[{c}] IS NULL) "
        f"OR (CONVERT(NVARCHAR(MAX), a.[{c}]) "
        f"<> CONVERT(NVARCHAR(MAX), b.[{c}])))"
        for c in columns
    )


def _build_sof_duplicate_conflict_count_sql(
    source_table: str,
    columns: list[str],
    primary_keys: list[str],
    dedup_tiebreak_columns: list[str] | None = None,
) -> str | None:
    """Return SQL that counts duplicate PK groups with differing payload values."""
    semantic_columns = _semantic_duplicate_columns(columns, primary_keys, dedup_tiebreak_columns)
    if not semantic_columns:
        return None

    pk_cols = ", ".join(f"[{pk}]" for pk in primary_keys)
    join_pk = " AND ".join(f"a.[{pk}] = b.[{pk}]" for pk in primary_keys)
    join_dk = " AND ".join(f"a.[{pk}] = dk.[{pk}]" for pk in primary_keys)
    difference_predicate = _build_null_safe_difference_predicate(semantic_columns)

    return f"""
        WITH duplicate_keys AS (
            SELECT {pk_cols}
            FROM {source_table}
            GROUP BY {pk_cols}
            HAVING COUNT_BIG(*) > 1
        ),
        conflict_keys AS (
            SELECT {", ".join(f"a.[{pk}]" for pk in primary_keys)}
            FROM {source_table} AS a
            INNER JOIN duplicate_keys AS dk
                ON {join_dk}
            INNER JOIN {source_table} AS b
                ON {join_pk}
            WHERE {difference_predicate}
            GROUP BY {", ".join(f"a.[{pk}]" for pk in primary_keys)}
        )
        SELECT COUNT_BIG(*) AS conflict_group_count
        FROM conflict_keys;
    """


def _build_sof_duplicate_conflict_sample_sql(
    source_table: str,
    columns: list[str],
    primary_keys: list[str],
    dedup_tiebreak_columns: list[str] | None = None,
    sample_size: int = 10,
) -> str:
    """Return SQL with sample PKs for duplicate groups that differ semantically."""
    semantic_columns = _semantic_duplicate_columns(columns, primary_keys, dedup_tiebreak_columns)
    if not semantic_columns:
        select_pk = ", ".join(f"[{pk}]" for pk in primary_keys)
        return f"SELECT TOP ({sample_size}) {select_pk} FROM {source_table};"

    pk_cols = ", ".join(f"[{pk}]" for pk in primary_keys)
    join_pk = " AND ".join(f"a.[{pk}] = b.[{pk}]" for pk in primary_keys)
    join_dk = " AND ".join(f"a.[{pk}] = dk.[{pk}]" for pk in primary_keys)
    select_pk = ", ".join(f"a.[{pk}]" for pk in primary_keys)
    group_pk = ", ".join(f"a.[{pk}]" for pk in primary_keys)
    difference_predicate = _build_null_safe_difference_predicate(semantic_columns)

    return f"""
        WITH duplicate_keys AS (
            SELECT {pk_cols}
            FROM {source_table}
            GROUP BY {pk_cols}
            HAVING COUNT_BIG(*) > 1
        )
        SELECT TOP ({sample_size}) {select_pk}
        FROM {source_table} AS a
        INNER JOIN duplicate_keys AS dk
            ON {join_dk}
        INNER JOIN {source_table} AS b
            ON {join_pk}
        WHERE {difference_predicate}
        GROUP BY {group_pk};
    """


def _get_target_column_types(
    cursor: Any,
    table_name: str,
    placeholder: str = "%s",
) -> dict[str, str]:
    """
    Fetch the actual column types from fhir.{table_name} so the temp table
    matches exactly. This allows SQL Server to use efficient merge joins
    instead of per-row type conversions.

    Args:
        cursor: Database cursor.
        table_name: Target table name (e.g., "Patient").
        placeholder: SQL parameter placeholder ("%s" for pymssql, "?" for mssql-python).

    Returns:
        Dict mapping column_name -> T-SQL type definition (e.g. "NVARCHAR(255)").
    """
    cursor.execute(
        f"""
        SELECT COLUMN_NAME,
               DATA_TYPE,
               CHARACTER_MAXIMUM_LENGTH,
               NUMERIC_PRECISION,
               NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'fhir' AND TABLE_NAME = {placeholder}
        ORDER BY ORDINAL_POSITION
        """,
        (table_name,),
    )
    col_types: dict[str, str] = {}
    for row in cursor.fetchall():
        col_name, data_type, char_max_len, num_prec, num_scale = row
        if data_type in ("nvarchar", "varchar"):
            if char_max_len == -1:
                col_types[col_name] = f"{data_type.upper()}(MAX)"
            else:
                col_types[col_name] = f"{data_type.upper()}({char_max_len})"
        elif data_type == "datetime2":
            col_types[col_name] = "DATETIME2"
        elif data_type == "int":
            col_types[col_name] = "INT"
        else:
            col_types[col_name] = data_type.upper()
    return col_types


# tempdb inherits the SERVER INSTANCE collation, which can differ from the staging
# database's collation. It does on QA/Prod: the instance is still
# SQL_Latin1_General_CP1_CI_AS while the staging DB was moved to
# Danish_Norwegian_CI_AS. A #/## temp char column therefore lands in the instance
# collation, and the MERGE against the (now Danish) fhir.* target raises "Cannot
# resolve the collation conflict". COLLATE DATABASE_DEFAULT pins temp char columns
# to the staging DB's own collation so src and tgt align -- a no-op when the instance
# and DB collations already match, and only legal on character types.
_TEMP_COLLATE = " COLLATE DATABASE_DEFAULT"
_CHAR_TYPE_PREFIXES = ("NVARCHAR", "VARCHAR", "NCHAR", "CHAR", "NTEXT", "TEXT")


def _temp_table_col_defs(columns: list[str], col_type_map: dict[str, str]) -> str:
    """Build the ``CREATE TABLE`` column list for a SoF staging temp table.

    Char columns get COLLATE DATABASE_DEFAULT so they match the target fhir.* table's
    collation instead of tempdb's (server-instance) collation -- see the note above.
    Non-character types are left untouched (COLLATE is illegal on int/datetime/etc.).
    """
    defs = []
    for c in columns:
        sql_type = col_type_map.get(c, "NVARCHAR(500)")
        collate = _TEMP_COLLATE if sql_type.upper().startswith(_CHAR_TYPE_PREFIXES) else ""
        defs.append(f"[{c}] {sql_type}{collate} NULL")
    return ", ".join(defs)


def apply_null_sentinels(
    rows: list[dict[str, Any]],
    null_sentinel_columns: list[str],
    sentinel_value: str = "",
) -> None:
    """
    Replace None values with a sentinel in specified columns (in-place).

    This is needed when forEachOrNull produces NULL values for columns that
    participate in a MSSQL composite primary key (which cannot contain NULLs).

    Args:
        rows: List of row dicts to mutate in place.
        null_sentinel_columns: Column names where None should become sentinel_value.
        sentinel_value: The value to replace None with (default: empty string).
    """
    for row in rows:
        for col in null_sentinel_columns:
            if row.get(col) is None:
                row[col] = sentinel_value


def _is_blank(value: Any) -> bool:
    """Return True when a value is None or empty after string coercion."""
    return value is None or str(value) == ""


def validate_required_columns(
    rows: list[dict[str, Any]],
    trigger_column: str,
    required_columns: list[str],
    table_name: str,
) -> None:
    """
    Validate required columns when a trigger column is populated.

    For flattened SOF rows (notably Encounter_Location), some columns can be NULL
    due to forEachOrNull projection semantics. This validator allows "all-empty"
    fallback rows, but rejects partial rows where a trigger value exists while one
    or more required columns are blank.
    """
    if not rows or not trigger_column or not required_columns:
        return

    invalid_count = 0
    for row in rows:
        if _is_blank(row.get(trigger_column)):
            continue
        if any(_is_blank(row.get(col)) for col in required_columns):
            invalid_count += 1

    if invalid_count:
        required_str = ", ".join(required_columns)
        raise ValueError(
            f"Data validation failed for [fhir].[{table_name}]: "
            f"{invalid_count} row(s) have '{trigger_column}' but missing required column(s): {required_str}"
        )


def deduplicate_by_pk(
    rows: list[dict[str, Any]],
    primary_keys: list[str],
    tiebreak_columns: list[str] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """
    Deterministically deduplicate rows by primary key.

    If duplicate keys exist, keep the row with the lexicographically largest
    tuple across ``tiebreak_columns``. Input order is preserved for first-seen keys.
    Returns the deduplicated rows and number of dropped rows.
    """
    if not rows or not primary_keys:
        return rows, 0

    effective_tiebreak = tiebreak_columns or ["last_updated", "created_at", "location_period_end"]
    kept_by_key: dict[tuple[str, ...], dict[str, Any]] = {}
    ordered_keys: list[tuple[str, ...]] = []
    dropped = 0

    def _tb_tuple(row: dict[str, Any]) -> tuple[str, ...]:
        return tuple(str(row.get(col) or "") for col in effective_tiebreak)

    for row in rows:
        key = tuple(str(row.get(pk, "")) for pk in primary_keys)
        if key not in kept_by_key:
            kept_by_key[key] = row
            ordered_keys.append(key)
            continue

        dropped += 1
        current = kept_by_key[key]
        if _tb_tuple(row) > _tb_tuple(current):
            kept_by_key[key] = row

    return [kept_by_key[k] for k in ordered_keys], dropped


def merge_into_mssql(
    mssql_target: MSSQLResource,
    table_name: str,
    rows: list[dict[str, Any]],
    primary_keys: list[str],
    context: dg.AssetExecutionContext | None = None,
    null_sentinel_columns: list[str] | None = None,
) -> int:
    """
    MERGE rows from a SQL on FHIR extraction into a fhir.* MSSQL table.

    Performance-optimised strategy:
    1. Apply NULL sentinels for forEachOrNull PK columns (if configured)
    2. Apply table-specific row validation/dedup safeguards (if configured)
    3. Create a temp table #SofTemp with EXACT column types from the target table
    4. Batch-insert extracted rows into #SofTemp (batch_size=1000)
    5. Create a clustered index on #SofTemp PK columns for fast MERGE joins
    6. MERGE from #SofTemp into fhir.{table_name}
    7. Return the number of rows affected

    Args:
        mssql_target: MSSQL resource to write to.
        table_name: Target table name (e.g., "Patient").
        rows: List of row dicts from extract_sof_view().
        primary_keys: PK columns for the MERGE.
        context: Optional Dagster context for logging.
        null_sentinel_columns: Columns where None should be replaced with ''
            before MERGE (for forEachOrNull PK columns).

    Returns:
        Number of rows affected by the MERGE.
    """
    if not rows:
        if context:
            context.log.info(f"No rows to merge into fhir.{table_name}")
        return 0

    # Replace None with '' in forEachOrNull PK columns so MSSQL NOT NULL PK works
    if null_sentinel_columns:
        apply_null_sentinels(rows, null_sentinel_columns)
        if context:
            context.log.info(f"Applied NULL sentinels for columns: {null_sentinel_columns}")

    # Apply table-specific validation/dedup safeguards when configured.
    table_cfg = next(
        (cfg for cfg in SOF_RESOURCE_CONFIGS.values() if cfg.get("mssql_table") == table_name),
        None,
    )
    if table_cfg:
        trigger_column = table_cfg.get("validation_trigger_column")
        required_columns = table_cfg.get("required_columns_when_trigger_present")
        if trigger_column and required_columns:
            validate_required_columns(
                rows=rows,
                trigger_column=trigger_column,
                required_columns=required_columns,
                table_name=table_name,
            )

        dedup_enabled = bool(table_cfg.get("deduplicate_by_pk"))
        tiebreak_columns = table_cfg.get("dedup_tiebreak_columns")
        if dedup_enabled:
            rows, dropped = deduplicate_by_pk(
                rows=rows,
                primary_keys=primary_keys,
                tiebreak_columns=tiebreak_columns,
            )
            if context and dropped:
                context.log.warning(
                    f"Deduplicated {dropped} rows by PK before MERGE into [fhir].[{table_name}]"
                )

    # Determine columns from the first row -- exclude DW audit columns
    dw_columns = {"dw_timestamp_insert", "dw_timestamp_update"}
    columns = [c for c in rows[0].keys() if c not in dw_columns]

    if context:
        context.log.info(
            f"Merging {len(rows)} rows into [fhir].[{table_name}] "
            f"({len(columns)} columns, PK: {primary_keys}, "
            f"driver: mssql-python)"
        )

    # Determine placeholder based on driver (mssql-python uses ?)
    placeholder = "?"

    with mssql_target.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # Step 1: Get real column types from the target table
            col_type_map = _get_target_column_types(cursor, table_name, placeholder)

            # Build temp table with matching types (fall back to NVARCHAR(500) if
            # unknown); char columns are collated to the target DB -- see note above.
            col_defs = _temp_table_col_defs(columns, col_type_map)
            cursor.execute(f"CREATE TABLE #SofTemp ({col_defs})")

            # Step 2: Batch-insert rows into temp table using multi-row VALUES
            # pymssql executemany is slow because it sends one INSERT per row.
            # Multi-row VALUES sends one INSERT per batch: INSERT INTO ... VALUES (...),(...),(...)
            # This reduces network round-trips dramatically.
            col_list = ", ".join(f"[{c}]" for c in columns)

            batch_size = max(1, MAX_DRIVER_PARAMS // len(columns))

            if context:
                context.log.info(
                    f"Batch size: {batch_size} rows/batch "
                    f"({len(columns)} columns, "
                    f"mssql-python limit={MAX_DRIVER_PARAMS})"
                )

            total_inserted = 0
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                value_rows = []
                params: list[Any] = []
                for row in batch:
                    placeholders = []
                    for c in columns:
                        v = row.get(c)
                        if v is None:
                            placeholders.append("NULL")
                        else:
                            placeholders.append(placeholder)
                            params.append(str(v))
                    value_rows.append(f"({', '.join(placeholders)})")

                values_clause = ", ".join(value_rows)
                insert_sql = f"INSERT INTO #SofTemp ({col_list}) VALUES {values_clause}"
                cursor.execute(insert_sql, tuple(params))
                total_inserted += len(batch)

            if context:
                context.log.info(f"Inserted {total_inserted} rows into #SofTemp, creating index...")

            # Step 3: Create clustered index on PK columns for fast MERGE join
            pk_cols = ", ".join(f"[{pk}]" for pk in primary_keys)
            cursor.execute(f"CREATE CLUSTERED INDEX IX_SofTemp_PK ON #SofTemp ({pk_cols})")

            # Step 4: Execute MERGE
            merge_sql = build_fhir_merge_sql(table_name, columns, primary_keys)
            cursor.execute(merge_sql)
            rows_affected = cursor.rowcount

            # Step 5: Clean up temp table
            cursor.execute("DROP TABLE #SofTemp")

            conn.commit()
        finally:
            cursor.close()

    if context:
        context.log.info(f"MERGE complete: {rows_affected} rows affected in [fhir].[{table_name}]")

    return rows_affected

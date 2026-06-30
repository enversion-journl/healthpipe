# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Shared utility functions for EVENTMARKER extract assets.

Provides common extraction patterns used across patient rights domains:
- With EPISODE_OF_CARE join: Udredningsret, Behandlingsret, Maksimale ventetider
- Without EPISODE_OF_CARE join: Kræftpakker, Alle markører

Individual extract files define domain-specific configuration:
- EVENT_CODE_FILTER (SQL WHERE criteria)
- _enrich_marker_row (enrichment/filtering callback)
- _classify_*_marker (classification logic)

These are passed as arguments to the shared extraction functions below.
"""

from calendar import monthrange
from collections.abc import Callable
from datetime import datetime
from typing import Any

import dagster as dg
from core.resources import IFDBOracleResource

# =============================================================================
# Constants
# =============================================================================

# EVENT_CODE parsing configuration
EVENT_CODE_PREFIX = "SKS.a.adm"
EVENT_CODE_PREFIX_LENGTH = len(EVENT_CODE_PREFIX)

# Base columns to extract from EVENTMARKER (with em. table alias)
EVENTMARKER_BASE_COLUMNS = """
    em.OBJECT_ID,
    em.PATIENT_ID,
    em.EPISODE_OF_CARE_ID,
    em.EVENT_CODE,
    em.EVENT_TEXT,
    em.TIME,
    em.EPJI_CREATED_DATETIME,
    em.IFDB_MODIFICATION_DATETIME,
    em.ADMINISTRATIVE_UNIT_CODE,
    em.ADMINISTRATIVE_UNIT_NAME,
    em.ADMINISTRATIVE_UNIT_ORGCODE,
    em.ADMINISTRATIVE_UNIT_SORCODE,
    em.RESPONSIBLE_STAFF_PERSID,
    em.RESPONSIBLE_STAFF_PERSNAME,
    em.RESPONSIBLE_STAFF_ROLE
"""

# EoC join columns with PREVIOUS_EOC_ID (for helbredsforløb: udredningsret, behandlingsret)
EOC_COLUMNS_WITH_PREVIOUS = """
    eoc.OBJECT_ID as EOC_OBJECT_ID,
    eoc.PREVIOUS_EOC_ID,
    eoc.HEALTHISSUE_ID
"""

# EoC join columns without PREVIOUS_EOC_ID (for maksimale ventetider)
EOC_COLUMNS_HEALTHISSUE_ONLY = """
    eoc.OBJECT_ID as EOC_OBJECT_ID,
    eoc.HEALTHISSUE_ID
"""

# Standalone EoC fetch column sets (for fetch_episode_of_care_data)
EOC_FETCH_COLUMNS_FULL = "OBJECT_ID, PATIENT_ID, PREVIOUS_EOC_ID, HEALTHISSUE_ID"
EOC_FETCH_COLUMNS_HEALTHISSUE = "OBJECT_ID, PATIENT_ID, HEALTHISSUE_ID"


# =============================================================================
# Common Parsing
# =============================================================================


def parse_event_code(event_code: str | None) -> dict[str, str | None]:
    """
    Parse EVENT_CODE to extract marker type and code components.

    EVENT_CODE format: SKS.a.adm{SKS_CODE}
    Example: SKS.a.admAFV01A1 → marker_type=AFV, marker_code=AFV01A1

    Args:
        event_code: Raw EVENT_CODE from EVENTMARKER table

    Returns:
        Dict with keys: marker_type, marker_code, marker_code_full
    """
    if not event_code or not event_code.startswith(EVENT_CODE_PREFIX):
        return {
            "marker_type": None,
            "marker_code": None,
            "marker_code_full": None,
        }

    code_without_prefix = event_code[EVENT_CODE_PREFIX_LENGTH:]
    marker_type = code_without_prefix[:3] if len(code_without_prefix) >= 3 else None

    return {
        "marker_type": marker_type,
        "marker_code": code_without_prefix,
        "marker_code_full": code_without_prefix,
    }


# =============================================================================
# Date Parsing Helpers
# =============================================================================


def parse_monthly_partition(partition_key: str) -> tuple[str, str]:
    """
    Parse monthly partition key (YYYY-MM) into start/end date strings.

    Returns:
        Tuple of (start_date, end_date) in 'YYYY-MM-DD HH24:MI:SS' format.
    """
    partition_date = datetime.strptime(partition_key, "%Y-%m")
    year = partition_date.year
    month = partition_date.month
    _, last_day = monthrange(year, month)

    start_date = f"{year:04d}-{month:02d}-01 00:00:00"
    end_date = f"{year:04d}-{month:02d}-{last_day:02d} 23:59:59"
    return start_date, end_date


# =============================================================================
# Episode of Care Fetching
# =============================================================================


def fetch_episode_of_care_data(
    ifdb_oracle: IFDBOracleResource,
    logger: dg.DagsterLogManager,
    patient_ids: list[Any] | None = None,
    eoc_fetch_columns: str = EOC_FETCH_COLUMNS_FULL,
) -> list[dict[str, Any]]:
    """
    Fetch EPISODE_OF_CARE data with configurable columns.

    Args:
        ifdb_oracle: Oracle connection resource
        logger: Dagster logger
        patient_ids: Optional patient ID list.
            None = fetch all (full load), [] = return empty, list = filter by IDs.
        eoc_fetch_columns: SQL column list for SELECT, e.g.
            "OBJECT_ID, PATIENT_ID, PREVIOUS_EOC_ID, HEALTHISSUE_ID"

    Returns:
        List of episode dicts.
    """
    if patient_ids is not None and len(patient_ids) == 0:
        logger.info("No patient IDs provided - returning empty episode list")
        return []

    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        try:
            if patient_ids is None:
                # Full load: fetch all
                logger.info("Fetching ALL EPISODE_OF_CARE data...")
                query = f"""
                    SELECT {eoc_fetch_columns}
                    FROM EPISODE_OF_CARE
                    WHERE DELETED = 0
                """
                cursor.execute(query)
            else:
                # Delta: fetch only for specified patients
                logger.info(f"Fetching EPISODE_OF_CARE data for {len(patient_ids)} patients...")

                # Oracle has a limit of ~1000 items in IN clause, so we batch
                all_rows: list[dict[str, Any]] = []
                batch_size = 500

                for i in range(0, len(patient_ids), batch_size):
                    batch = patient_ids[i : i + batch_size]
                    placeholders = ", ".join([f":p{j}" for j in range(len(batch))])
                    query = f"""
                        SELECT {eoc_fetch_columns}
                        FROM EPISODE_OF_CARE
                        WHERE DELETED = 0
                          AND PATIENT_ID IN ({placeholders})
                    """
                    params = {f"p{j}": pid for j, pid in enumerate(batch)}
                    cursor.execute(query, params)
                    columns = [desc[0] for desc in cursor.description]
                    all_rows.extend(
                        [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]
                    )

                logger.info(
                    f"Fetched {len(all_rows)} EPISODE_OF_CARE records "
                    f"for {len(patient_ids)} patients"
                )
                return all_rows

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
        finally:
            cursor.close()

    episodes = [dict(zip(columns, row, strict=False)) for row in rows]
    logger.info(f"Fetched {len(episodes)} EPISODE_OF_CARE records")
    return episodes


# =============================================================================
# Extraction with EoC (udredningsret, behandlingsret, maksimale_ventetider)
# =============================================================================


def extract_eventmarker_with_eoc_full_load(
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource,
    *,
    event_code_filter: str,
    enrich_fn: Callable[[dict[str, Any]], dict[str, Any] | None],
    domain_name: str,
    eventmarker_columns: str = EVENTMARKER_BASE_COLUMNS,
    eoc_columns: str = EOC_COLUMNS_WITH_PREVIOUS,
    eoc_fetch_columns: str = EOC_FETCH_COLUMNS_FULL,
) -> dict[str, Any]:
    """
    Full-load extract of ALL EVENTMARKER rows with EPISODE_OF_CARE join.

    Returns:
        Dict with:
        - markers: List of enriched marker dicts
        - episodes: List of episode dicts
    """
    context.log.info(f"Full-load extraction: Extracting ALL EVENTMARKER rows for {domain_name}...")

    # Fetch episode of care data
    episodes = fetch_episode_of_care_data(
        ifdb_oracle, context.log, eoc_fetch_columns=eoc_fetch_columns
    )

    # Extract markers with episode join
    extract_query = f"""
        SELECT
            {eventmarker_columns},
            {eoc_columns}
        FROM EVENTMARKER em
        LEFT JOIN EPISODE_OF_CARE eoc
            ON em.EPISODE_OF_CARE_ID = eoc.OBJECT_ID AND eoc.DELETED = 0
        WHERE em.DELETED = 0
          AND {event_code_filter}
    """

    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(extract_query)
            columns = [desc[0] for desc in cursor.description]
            raw_rows = cursor.fetchall()
        finally:
            cursor.close()

    context.log.info(
        f"Fetched {len(raw_rows)} total EVENTMARKER rows " f"matching {domain_name} patterns"
    )

    result_rows = _filter_and_enrich(raw_rows, columns, enrich_fn)
    context.log.info(f"After filtering: {len(result_rows)} {domain_name} markers")
    _log_classification_distribution(context, result_rows)

    return {"markers": result_rows, "episodes": episodes}


def extract_eventmarker_with_eoc_delta(
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource,
    start_date: str,
    end_date: str,
    *,
    event_code_filter: str,
    enrich_fn: Callable[[dict[str, Any]], dict[str, Any] | None],
    domain_name: str,
    eventmarker_columns: str = EVENTMARKER_BASE_COLUMNS,
    eoc_columns: str = EOC_COLUMNS_WITH_PREVIOUS,
    eoc_fetch_columns: str = EOC_FETCH_COLUMNS_FULL,
    apply_outer_time_limit: bool = True,
) -> dict[str, Any]:
    """
    Core delta extraction with EPISODE_OF_CARE join.

    Delta strategy:
    1. Find patients with IFDB_MODIFICATION_DATETIME changes in date range
    2. Extract ALL relevant markers for those patients
    3. Fetch EPISODE_OF_CARE data ONLY for those patients

    Args:
        start_date: Start of date range (format: 'YYYY-MM-DD HH24:MI:SS')
        end_date: End of date range (format: 'YYYY-MM-DD HH24:MI:SS')
        apply_outer_time_limit: If True, limits outer query to markers modified
            before end_date (avoids pulling future events for historical partitions).

    Returns:
        Dict with:
        - markers: List of enriched marker dicts
        - episodes: List of episode dicts
    """
    context.log.info(f"Delta extraction for {domain_name}")
    context.log.info(f"Date range: {start_date} to {end_date}")

    markers, patient_ids = _execute_eoc_delta_query(
        ifdb_oracle,
        context.log,
        start_date=start_date,
        end_date=end_date,
        eventmarker_columns=eventmarker_columns,
        eoc_columns=eoc_columns,
        event_code_filter=event_code_filter,
        enrich_fn=enrich_fn,
        domain_name=domain_name,
        apply_outer_time_limit=apply_outer_time_limit,
    )

    if not markers:
        return {"markers": [], "episodes": []}

    # Fetch episode of care data ONLY for patients in delta
    episodes = fetch_episode_of_care_data(
        ifdb_oracle,
        context.log,
        patient_ids=patient_ids,
        eoc_fetch_columns=eoc_fetch_columns,
    )

    return {"markers": markers, "episodes": episodes}


def extract_eventmarker_with_eoc_daily_delta(
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource,
    partition_key: str,
    *,
    event_code_filter: str,
    enrich_fn: Callable[[dict[str, Any]], dict[str, Any] | None],
    domain_name: str,
    eventmarker_columns: str = EVENTMARKER_BASE_COLUMNS,
    eoc_columns: str = EOC_COLUMNS_WITH_PREVIOUS,
    eoc_fetch_columns: str = EOC_FETCH_COLUMNS_FULL,
    apply_outer_time_limit: bool = True,
) -> dict[str, Any]:
    """
    Daily delta extraction with EPISODE_OF_CARE join.

    Convenience wrapper around extract_eventmarker_with_eoc_delta
    that parses a daily partition key (YYYY-MM-DD).
    """
    partition_date = datetime.strptime(partition_key, "%Y-%m-%d")
    start_date = partition_date.strftime("%Y-%m-%d 00:00:00")
    end_date = partition_date.strftime("%Y-%m-%d 23:59:59")

    context.log.info(f"Daily delta extraction for {domain_name}, partition: {partition_key}")

    return extract_eventmarker_with_eoc_delta(
        context,
        ifdb_oracle,
        start_date,
        end_date,
        event_code_filter=event_code_filter,
        enrich_fn=enrich_fn,
        domain_name=domain_name,
        eventmarker_columns=eventmarker_columns,
        eoc_columns=eoc_columns,
        eoc_fetch_columns=eoc_fetch_columns,
        apply_outer_time_limit=apply_outer_time_limit,
    )


def extract_eventmarker_with_eoc_monthly_delta(
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource,
    partition_key: str,
    *,
    event_code_filter: str,
    enrich_fn: Callable[[dict[str, Any]], dict[str, Any] | None],
    domain_name: str,
    eventmarker_columns: str = EVENTMARKER_BASE_COLUMNS,
    eoc_columns: str = EOC_COLUMNS_WITH_PREVIOUS,
    eoc_fetch_columns: str = EOC_FETCH_COLUMNS_FULL,
    apply_outer_time_limit: bool = True,
) -> dict[str, Any]:
    """
    Monthly delta extraction with EPISODE_OF_CARE join (simple approach).

    Same as daily delta but for a full month. Loads all results into memory.
    Use extract_eventmarker_with_eoc_monthly_delta_batched for domains with
    large data volumes that may cause out-of-memory errors.

    Convenience wrapper around extract_eventmarker_with_eoc_delta
    that parses a monthly partition key (YYYY-MM).
    """
    start_date, end_date = parse_monthly_partition(partition_key)

    context.log.info(f"Monthly delta extraction for {domain_name}, partition: {partition_key}")

    return extract_eventmarker_with_eoc_delta(
        context,
        ifdb_oracle,
        start_date,
        end_date,
        event_code_filter=event_code_filter,
        enrich_fn=enrich_fn,
        domain_name=domain_name,
        eventmarker_columns=eventmarker_columns,
        eoc_columns=eoc_columns,
        eoc_fetch_columns=eoc_fetch_columns,
        apply_outer_time_limit=apply_outer_time_limit,
    )


def extract_eventmarker_with_eoc_monthly_delta_batched(
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource,
    partition_key: str,
    *,
    event_code_filter: str,
    enrich_fn: Callable[[dict[str, Any]], dict[str, Any] | None],
    domain_name: str,
    eventmarker_columns: str = EVENTMARKER_BASE_COLUMNS,
    eoc_columns: str = EOC_COLUMNS_WITH_PREVIOUS,
    eoc_fetch_columns: str = EOC_FETCH_COLUMNS_FULL,
    patient_chunk_size: int = 5000,
) -> dict[str, Any]:
    """
    Monthly delta extraction with EPISODE_OF_CARE join (batched for OOM safety).

    Instead of loading all markers at once, this:
    1. Fetches patient IDs with changes (lightweight query)
    2. Processes markers in patient chunks
    3. Fetches EPISODE_OF_CARE per chunk

    Use for domains with large data volumes where the simple monthly approach
    may cause out-of-memory errors (e.g. udredningsret).
    """
    start_date, end_date = parse_monthly_partition(partition_key)

    context.log.info(
        f"Monthly delta extraction (batched) for {domain_name}, " f"partition: {partition_key}"
    )
    context.log.info(f"Date range: {start_date} to {end_date}")

    # Step 1: Get affected patient IDs (lightweight query)
    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        try:
            patient_ids_query = f"""
                SELECT DISTINCT PATIENT_ID
                FROM EVENTMARKER
                WHERE DELETED = 0
                  AND IFDB_MODIFICATION_DATETIME >= TO_TIMESTAMP(
                      :start_date, 'YYYY-MM-DD HH24:MI:SS')
                  AND IFDB_MODIFICATION_DATETIME < TO_TIMESTAMP(
                      :end_date, 'YYYY-MM-DD HH24:MI:SS') + INTERVAL '1' SECOND
                  AND {event_code_filter.replace('em.', '')}
            """
            cursor.execute(patient_ids_query, {"start_date": start_date, "end_date": end_date})
            patient_ids = [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

    if not patient_ids:
        context.log.info("No patients with changes found - returning empty result")
        return {"markers": [], "episodes": []}

    context.log.info(f"Found {len(patient_ids)} patients with changes in partition window")

    # Step 2: Process in chunks
    all_markers: list[dict[str, Any]] = []
    all_episodes: list[dict[str, Any]] = []

    for chunk_start in range(0, len(patient_ids), patient_chunk_size):
        chunk_end = min(chunk_start + patient_chunk_size, len(patient_ids))
        patient_chunk = patient_ids[chunk_start:chunk_end]

        context.log.info(
            f"Processing patient chunk "
            f"{chunk_start // patient_chunk_size + 1}: "
            f"patients {chunk_start + 1}-{chunk_end} of {len(patient_ids)}"
        )

        chunk_markers, chunk_episodes = _extract_for_patient_ids_with_eoc(
            ifdb_oracle,
            context.log,
            patient_chunk,
            eventmarker_columns=eventmarker_columns,
            eoc_columns=eoc_columns,
            event_code_filter=event_code_filter,
            enrich_fn=enrich_fn,
            eoc_fetch_columns=eoc_fetch_columns,
            end_date=end_date,
        )

        all_markers.extend(chunk_markers)
        all_episodes.extend(chunk_episodes)

        context.log.info(
            f"  → Chunk yielded {len(chunk_markers)} markers, " f"{len(chunk_episodes)} episodes"
        )

    context.log.info(
        f"Total extraction: {len(all_markers)} markers, " f"{len(all_episodes)} episodes"
    )

    return {"markers": all_markers, "episodes": all_episodes}


# =============================================================================
# Extraction without EoC (kræftpakker, alle_markoerer)
# =============================================================================


def extract_eventmarker_delta(
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource,
    start_date: str,
    end_date: str,
    *,
    eventmarker_columns: str,
    enrich_fn: Callable[[dict[str, Any]], dict[str, Any] | None],
    domain_name: str,
    subquery_event_code_filter: str | None = None,
) -> list[dict[str, Any]]:
    """
    Delta extraction without EPISODE_OF_CARE join.

    Strategy: Find patients with marker changes in date range, then extract
    ALL markers for those patients.

    Args:
        eventmarker_columns: SQL column list (with em. alias) for the outer SELECT.
        enrich_fn: Callable that enriches/filters a row dict. Return None to exclude.
        domain_name: Domain name for log messages.
        subquery_event_code_filter: Optional event code filter for the patient
            subquery (without em. prefix). If None, ALL marker changes trigger
            patient inclusion.

    Returns:
        List of enriched marker dicts.
    """
    context.log.info(f"Delta extraction for {domain_name}")
    context.log.info(f"Date range: {start_date} to {end_date}")

    subquery_filter = ""
    if subquery_event_code_filter:
        subquery_filter = f"AND {subquery_event_code_filter}"

    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        try:
            extract_query = f"""
                SELECT
                    {eventmarker_columns}
                FROM EVENTMARKER em
                WHERE em.DELETED = 0
                  AND em.PATIENT_ID IN (
                      SELECT DISTINCT PATIENT_ID
                      FROM EVENTMARKER
                      WHERE DELETED = 0
                        AND IFDB_MODIFICATION_DATETIME >= TO_TIMESTAMP(
                            :start_date, 'YYYY-MM-DD HH24:MI:SS')
                        AND IFDB_MODIFICATION_DATETIME < TO_TIMESTAMP(
                            :end_date, 'YYYY-MM-DD HH24:MI:SS') + INTERVAL '1' SECOND
                        {subquery_filter}
                  )
            """

            cursor.execute(extract_query, {"start_date": start_date, "end_date": end_date})
            columns = [desc[0] for desc in cursor.description]
            all_rows = cursor.fetchall()
        finally:
            cursor.close()

    context.log.info(
        f"Fetched {len(all_rows)} total EVENTMARKER rows " f"for patients with changes"
    )

    if not all_rows:
        context.log.info("No markers found for patients with changes - returning empty result")
        return []

    result_rows = _filter_and_enrich(all_rows, columns, enrich_fn)
    context.log.info(f"After filtering/enriching: {len(result_rows)} {domain_name} markers")

    unique_patients = len(set(r["PATIENT_ID"] for r in result_rows))
    context.log.info(f"Markers distributed across {unique_patients} unique patients")

    return result_rows


# =============================================================================
# Internal Helpers
# =============================================================================


def _execute_eoc_delta_query(
    ifdb_oracle: IFDBOracleResource,
    logger: dg.DagsterLogManager,
    *,
    start_date: str,
    end_date: str,
    eventmarker_columns: str,
    eoc_columns: str,
    event_code_filter: str,
    enrich_fn: Callable[[dict[str, Any]], dict[str, Any] | None],
    domain_name: str,
    apply_outer_time_limit: bool,
) -> tuple[list[dict[str, Any]], list[Any]]:
    """
    Execute the common EoC delta query and return (markers, unique_patient_ids).

    Used by both daily and simple monthly delta functions.
    """
    outer_time_limit = ""
    if apply_outer_time_limit:
        outer_time_limit = (
            "AND em.IFDB_MODIFICATION_DATETIME < "
            "TO_TIMESTAMP(:end_date, 'YYYY-MM-DD HH24:MI:SS') "
            "+ INTERVAL '1' SECOND"
        )

    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        try:
            extract_query = f"""
                SELECT
                    {eventmarker_columns},
                    {eoc_columns}
                FROM EVENTMARKER em
                LEFT JOIN EPISODE_OF_CARE eoc
                    ON em.EPISODE_OF_CARE_ID = eoc.OBJECT_ID AND eoc.DELETED = 0
                WHERE em.DELETED = 0
                  AND {event_code_filter}
                  {outer_time_limit}
                  AND em.PATIENT_ID IN (
                      SELECT DISTINCT PATIENT_ID
                      FROM EVENTMARKER
                      WHERE DELETED = 0
                        AND IFDB_MODIFICATION_DATETIME >= TO_TIMESTAMP(
                            :start_date, 'YYYY-MM-DD HH24:MI:SS')
                        AND IFDB_MODIFICATION_DATETIME < TO_TIMESTAMP(
                            :end_date, 'YYYY-MM-DD HH24:MI:SS')
                            + INTERVAL '1' SECOND
                        AND {event_code_filter.replace('em.', '')}
                  )
            """

            cursor.execute(extract_query, {"start_date": start_date, "end_date": end_date})
            columns = [desc[0] for desc in cursor.description]
            all_rows = cursor.fetchall()
        finally:
            cursor.close()

    logger.info(
        f"Fetched {len(all_rows)} total EVENTMARKER rows "
        f"for patients with changes (up to {end_date})"
    )

    if not all_rows:
        logger.info("No markers found for patients with changes - returning empty result")
        return [], []

    result_rows = _filter_and_enrich(all_rows, columns, enrich_fn)
    logger.info(f"After filtering: {len(result_rows)} {domain_name} markers")

    unique_patient_ids = list(set(r["PATIENT_ID"] for r in result_rows))
    logger.info(f"Markers distributed across {len(unique_patient_ids)} unique patients")

    return result_rows, unique_patient_ids


def _extract_for_patient_ids_with_eoc(
    ifdb_oracle: IFDBOracleResource,
    logger: dg.DagsterLogManager,
    patient_ids: list[Any],
    *,
    eventmarker_columns: str,
    eoc_columns: str,
    event_code_filter: str,
    enrich_fn: Callable[[dict[str, Any]], dict[str, Any] | None],
    eoc_fetch_columns: str,
    end_date: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Extract markers and episodes for a specific list of patient IDs.

    Used by the batched monthly extraction to process patients in chunks.

    Args:
        end_date: Optional end date to limit markers. If provided, only markers
            modified before this date are included (avoids pulling future events).
    """
    if not patient_ids:
        return [], []

    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        all_markers: list[dict[str, Any]] = []
        batch_size = 500  # Oracle IN clause limit
        try:
            for i in range(0, len(patient_ids), batch_size):
                batch = patient_ids[i : i + batch_size]
                placeholders = ", ".join([f":p{j}" for j in range(len(batch))])

                time_limit_clause = ""
                if end_date:
                    time_limit_clause = (
                        "AND em.IFDB_MODIFICATION_DATETIME < "
                        "TO_TIMESTAMP(:end_date, 'YYYY-MM-DD HH24:MI:SS') "
                        "+ INTERVAL '1' SECOND"
                    )

                extract_query = f"""
                    SELECT
                        {eventmarker_columns},
                        {eoc_columns}
                    FROM EVENTMARKER em
                    LEFT JOIN EPISODE_OF_CARE eoc
                        ON em.EPISODE_OF_CARE_ID = eoc.OBJECT_ID
                        AND eoc.DELETED = 0
                    WHERE em.DELETED = 0
                      AND {event_code_filter}
                      {time_limit_clause}
                      AND em.PATIENT_ID IN ({placeholders})
                """

                params: dict[str, Any] = {f"p{j}": pid for j, pid in enumerate(batch)}
                if end_date:
                    params["end_date"] = end_date
                cursor.execute(extract_query, params)
                columns = [desc[0] for desc in cursor.description]

                for row in cursor.fetchall():
                    row_dict = dict(zip(columns, row, strict=False))
                    enriched = enrich_fn(row_dict)
                    if enriched:
                        all_markers.append(enriched)
        finally:
            cursor.close()

    # Fetch episode of care data for these patients
    episodes = fetch_episode_of_care_data(
        ifdb_oracle,
        logger,
        patient_ids=patient_ids,
        eoc_fetch_columns=eoc_fetch_columns,
    )

    return all_markers, episodes


def _filter_and_enrich(
    raw_rows: list[tuple],
    columns: list[str],
    enrich_fn: Callable[[dict[str, Any]], dict[str, Any] | None],
) -> list[dict[str, Any]]:
    """Convert raw DB rows to dicts and apply enrichment/filtering function."""
    result_rows: list[dict[str, Any]] = []
    for row in raw_rows:
        row_dict = dict(zip(columns, row, strict=False))
        enriched = enrich_fn(row_dict)
        if enriched is not None:
            result_rows.append(enriched)
    return result_rows


def _log_classification_distribution(
    context: dg.AssetExecutionContext,
    markers: list[dict[str, Any]],
) -> None:
    """Log the distribution of MARKER_CLASSIFICATION values."""
    classifications: dict[str, int] = {}
    for r in markers:
        cls = r.get("MARKER_CLASSIFICATION", "unknown")
        classifications[cls] = classifications.get(cls, 0) + 1
    context.log.info(f"Marker classification distribution: {classifications}")

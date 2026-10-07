# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Incremental refresh of materialized fhir.SFI_datapoint_store on DSO.

Strategy:
    1. Read last watermark from meta.SyncWatermarks (reuses existing state infrastructure).
    2. Find changed resources (Observation + QuestionnaireResponse) since watermark
       using dw_timestamp_update / dw_timestamp_insert on upstream FHIR tables.
    3. Validate QR join-key health for only those resources.
    4. Apply via scoped DELETE + direct INSERT...SELECT within one transaction.
    5. Advance watermark only after successful commit.

First run (NULL watermark) can process all resources, but large historical rebuilds
should use the maintenance/reconciliation path rather than daily incremental.

Runs after FHIR flat-table sync and codesystem.sfi_* sync so consumer views
(fhir.SFI_datapoint, vw_fhir.SFI_datapoint) serve pre-joined rows with indexes.
"""

import os
import re
import time
from typing import Any

import dagster as dg
from core.resources import MSSQLResource
from DSO.load.sync_state import SyncStateManager
from infrastructure.utils.ddl_loader import execute_sql_file

_STORE_TABLE = "[fhir].[SFI_datapoint_store]"
_BUILD_VIEW = "[fhir].[SFI_datapoint_build]"
_REFRESH_LOCK_NAME = "fhir.SFI_datapoint_store.refresh"

_WATERMARK_SCHEMA = "sfi_datapoint"
_WATERMARK_TABLE_DAILY = "refresh_daily"
_WATERMARK_TABLE_MONTHLY = "refresh_monthly"
_WATERMARK_TABLE_RECONCILIATION = "refresh_reconciliation"
_QR_PREFLIGHT_MODE_ENV = "SFI_DATAPOINT_QR_PREFLIGHT_MODE"
_QR_PREFLIGHT_MODE_WARN = "warn"
_QR_PREFLIGHT_MODE_FAIL = "fail"
_QR_PREFLIGHT_MODE_SKIP = "skip"
_RECONCILIATION_BATCH_SIZE_ENV = "SFI_DATAPOINT_RECONCILIATION_BATCH_SIZE"
_DEFAULT_RECONCILIATION_BATCH_SIZE = 250_000
_INCREMENTAL_BATCH_SIZE_ENV = "SFI_DATAPOINT_INCREMENTAL_BATCH_SIZE"
_DEFAULT_INCREMENTAL_BATCH_SIZE = 50_000
_QR_ZERO_WARN_THRESHOLD_ENV = "SFI_DATAPOINT_QR_ZERO_WARN_THRESHOLD"
_DEFAULT_QR_ZERO_WARN_THRESHOLD = 0

_REFRESH_INSERT_COLUMNS = """
    [datapoint_key],
    [fhir_resource_id],
    [source_resource_id],
    [source_link_id],
    [source_item_key],
    [source_answer_index],
    [source_last_updated],
    [ifdb_aktivitet_id],
    [ifdb_aktivitet_sfi_id],
    [patient_id],
    [encounter_id],
    [organization_id],
    [practitioner_id],
    [practitioner_display],
    [source_resource],
    [occurred_at],
    [occurred_at_dt],
    [source_model_name],
    [source_model_version],
    [source_element_id],
    [source_element_name],
    [source_element_label],
    [sfi_path],
    [source_hierarchy_path],
    [source_group_path],
    [source_repetition_path],
    [klynge_niveau],
    [repetition_niveau],
    [raekke_type],
    [klynge_type],
    [klynge_valg_ledetekst],
    [element_code],
    [element_display],
    [value_type],
    [has_value],
    [value_int],
    [value_dec],
    [value_text],
    [value_datetime],
    [value_bool],
    [value_quantity],
    [value_unit],
    [value_coding_code],
    [value_coding_display],
    [element_nummer],
    [klynge_id],
    [klynge_id_hierarki],
    [klynge_parent_id],
    [repetition_id],
    [element_kode],
    [element_original_kode],
    [element_administrativ_enhed_id],
    [klynge_navne],
    [klynge_ledetekster],
    [klynge_ledetekst],
    [repetition_id_hierarki],
    [opdateret_tid],
    [xml_status],
    [xml_status_text],
    [refreshed_at]
"""

# NB: the SFI DSO syncs are the unpartitioned DFM assets
# (and Encounter already with [internal ticket]). A dep on a non-existent asset key is
# treated as an external asset by Dagster — the ordering edge silently
# disappears — so these lists MUST name the _dfm variants. The daily refresh
# asset itself must also live in fhir_dso_sync_dfm so it is selected by
# fhir_ifdb_dfm_sof_dso_job (the sensor fires that job in parallel with the
# partitioned daily job).
_SFI_DSO_DFM_DEPS = [
    "dso_sync_fhir_observation_dfm",
    "dso_sync_fhir_questionnaire_response_dfm",
    "dso_sync_fhir_questionnaire_response_item_dfm",
    "dso_sync_fhir_questionnaire_item_dfm",
    "dso_sync_fhir_encounter_dfm",
]

# One list for all three refresh assets: every variant reads the same DSO
# tables, so their upstreams are identical.
_SFI_UPSTREAM_DEPS = [
    "dso_infra_views_fhir",
    "dso_infra_fhir",
    *_SFI_DSO_DFM_DEPS,
    "dso_sync_codesystem_sfi_concept",
    "dso_sync_codesystem_sfi_properties",
]


def _refresh_columns() -> list[str]:
    return re.findall(r"\[(\w+)\]", _REFRESH_INSERT_COLUMNS)


def _build_changed_resources_sql(last_watermark_str: str | None) -> str:
    """Build SQL that populates #changed_resources with resource IDs modified since watermark.

    The timestamp string must be in 'YYYY-MM-DD HH:MM:SS' format (same convention as
    SyncStateManager / build_delta_merge_statement in the rest of DSO load).

    When last_watermark_str is None (first run), all resources are treated as changed
    by omitting the WHERE timestamp filter.

    Change detection filters on the PERSISTED computed column
    ``dw_timestamp_effective`` (= ISNULL(dw_timestamp_update, dw_timestamp_insert),
    added by migrations/20260609_fhir_delta_discovery_effective_timestamp.sql)
    so the watermark predicate is sargable and served by an index seek instead
    of three full clustered scans per run.
    """
    if last_watermark_str is not None:
        obs_where = f"AND o.[dw_timestamp_effective] >= '{last_watermark_str}'"
        qr_where = f"AND qr.[dw_timestamp_effective] >= '{last_watermark_str}'"
        qri_where = f"AND qri.[dw_timestamp_effective] >= '{last_watermark_str}'"
    else:
        obs_where = ""
        qr_where = ""
        qri_where = ""

    return f"""
        IF OBJECT_ID(N'tempdb..#changed_resources') IS NOT NULL
            DROP TABLE #changed_resources;

        WITH changed_observation AS (
            SELECT
                CAST(N'Observation' AS NVARCHAR(50)) AS source_resource,
                o.[observation_id] AS source_resource_id,
                o.[dw_timestamp_effective] AS changed_at
            FROM [fhir].[Observation] o
            WHERE o.[element_system] = N'urn:dk:ifdb:sfi'
                {obs_where}
        ),
        changed_qr AS (
            SELECT
                CAST(N'QuestionnaireResponse' AS NVARCHAR(50)) AS source_resource,
                qr.[qr_id] AS source_resource_id,
                qr.[dw_timestamp_effective] AS changed_at
            FROM [fhir].[QuestionnaireResponse] qr
            WHERE 1=1
                {qr_where}

            UNION

            SELECT
                CAST(N'QuestionnaireResponse' AS NVARCHAR(50)) AS source_resource,
                qri.[qr_id] AS source_resource_id,
                qri.[dw_timestamp_effective] AS changed_at
            FROM [fhir].[QuestionnaireResponse_item] qri
            WHERE 1=1
                {qri_where}
        )
        SELECT
            source_resource,
            source_resource_id,
            MAX(changed_at) AS changed_at
        INTO #changed_resources
        FROM (
            SELECT * FROM changed_observation
            UNION ALL
            SELECT * FROM changed_qr
        ) x
        GROUP BY source_resource, source_resource_id;

        CREATE CLUSTERED INDEX [IX_changed_resources_SourceResourceId]
            ON #changed_resources ([source_resource], [source_resource_id]);
    """


_SFI_PROPS_PIVOT_COLUMNS = [
    "source_model_name",
    "source_model_version",
    "last_seen_in_version",
    "source_element_id",
    "source_element_name",
    "source_element_label",
    "sfi_path",
    "source_hierarchy_path",
    "source_group_path",
    "source_repetition_path",
    "klynge_niveau",
    "repetition_niveau",
    "raekke_type",
    "klynge_type",
    "klynge_navne",
    "klynge_ledetekster",
    "klynge_ledetekst",
]


def _build_qr_zero_datapoint_count_sql() -> str:
    """Count changed QuestionnaireResponse resources that yielded zero datapoints.

    Cheap post-apply follow-up signal for silent under-emission (e.g. an
    incomplete fhir.Questionnaire_item dropping QR items in the build INNER
    JOIN): both sides of the NOT EXISTS are index seeks, so this costs
    milliseconds even for large deltas. Legitimate zeros exist (QRs whose
    items carry no SFI elements), so the count is a monitoring signal with a
    tunable WARNING threshold, not a hard error.
    """
    return f"""
        SELECT COUNT(*) AS qr_zero_datapoints
        FROM #changed_resources AS cr
        WHERE cr.[source_resource] = N'QuestionnaireResponse'
          AND NOT EXISTS (
              SELECT 1
              FROM {_STORE_TABLE} AS s
              WHERE s.[source_resource] = cr.[source_resource]
                AND s.[source_resource_id] = cr.[source_resource_id]
          );
    """


def _build_sfi_props_pivot_sql() -> str:
    """Pre-pivot codesystem.sfi_properties into #sfi_props_pivot temp table.

    Replaces per-row OUTER APPLY (O(N) correlated subqueries on ~600k+ rows)
    with a single GROUP BY on ~5-10k distinct Code values, then hash-join.
    """
    pivot_cols = ",\n            ".join(
        f"MAX(CASE WHEN sp.[property_code] = N'{col}' THEN sp.[value] END) AS [{col}]"
        for col in _SFI_PROPS_PIVOT_COLUMNS
    )
    return f"""
        SET NOCOUNT ON;

        IF OBJECT_ID(N'tempdb..#sfi_props_pivot') IS NOT NULL
            DROP TABLE #sfi_props_pivot;

        SELECT
            sp.[code],
            {pivot_cols}
        INTO #sfi_props_pivot
        FROM [codesystem].[SFIProperties] AS sp
        GROUP BY sp.[code];

        CREATE UNIQUE CLUSTERED INDEX [IX_sfi_props_pivot_Code]
            ON #sfi_props_pivot ([code]);
    """


def _outer_select_columns() -> str:
    """Build outer SELECT column expressions for the apply wrapper."""
    columns = _refresh_columns()
    return ",\n                ".join(
        (
            "SYSUTCDATETIME() AS [refreshed_at]"
            if column == "refreshed_at"
            else (
                "COALESCE(\n"
                "                        TRY_CONVERT(DATETIME2(0), b.[occurred_at], 126),\n"
                "                        TRY_CONVERT(DATETIME2(0), b.[occurred_at], 121),\n"
                "                        TRY_CONVERT(DATETIME2(0), b.[occurred_at], 23)\n"
                "                    ) AS [occurred_at_dt]"
                if column == "occurred_at_dt"
                else f"b.[{column}]"
            )
        )
        for column in columns
    )


def _inline_obs_build_leg(resources_table: str) -> str:
    """Observation leg of inline SFI_datapoint build, scoped to a resources temp table."""
    return f"""
            SELECT
                LOWER(CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', CONVERT(NVARCHAR(4000),
                    CONCAT_WS(N'|',
                        N'Observation',
                        COALESCE(o.[observation_id], N''),
                        N'',
                        N'',
                        N'',
                        COALESCE(o.[element_code], N'')
                ))), 2)) AS datapoint_key,
                o.[observation_id] AS fhir_resource_id,
                o.[observation_id] AS source_resource_id,
                CAST(NULL AS NVARCHAR(255)) AS source_link_id,
                CAST(NULL AS NVARCHAR(100)) AS source_item_key,
                CAST(NULL AS NVARCHAR(10)) AS source_answer_index,
                COALESCE(o.[dw_timestamp_update], o.[dw_timestamp_insert])
                    AS source_last_updated,
                o.[ifdb_aktivitet_id],
                o.[ifdb_aktivitet_sfi_id],
                o.[patient_id],
                o.[encounter_id],
                COALESCE(o.[organization_id], e.[service_provider_id]) AS organization_id,
                o.[practitioner_id],
                o.[practitioner_display],
                CAST(N'Observation' AS NVARCHAR(50)) AS source_resource,
                o.[effective_date] AS occurred_at,
                COALESCE(
                    op.[source_model_name],
                    CASE
                        WHEN CHARINDEX(N'.', o.[element_code]) > 1
                        THEN LEFT(o.[element_code],
                                  CHARINDEX(N'.', o.[element_code]) - 1)
                        ELSE NULL
                    END
                ) AS source_model_name,
                COALESCE(op.[last_seen_in_version], op.[source_model_version])
                    AS source_model_version,
                op.[source_element_id],
                op.[source_element_name],
                COALESCE(op.[source_element_label], o.[element_display], sc.[display])
                    AS source_element_label,
                op.[sfi_path],
                op.[source_hierarchy_path],
                op.[source_group_path],
                op.[source_repetition_path],
                op.[klynge_niveau],
                op.[repetition_niveau],
                op.[raekke_type],
                op.[klynge_type],
                o.[klynge_valg_ledetekst],
                o.[element_code],
                COALESCE(o.[element_display], sc.[display]) AS element_display,
                CASE
                    WHEN o.[value_int] IS NOT NULL THEN N'integer'
                    WHEN o.[value_quantity] IS NOT NULL
                        AND NULLIF(LTRIM(RTRIM(o.[value_unit])), N'') IS NULL
                        THEN N'decimal'
                    WHEN o.[value_quantity] IS NOT NULL THEN N'quantity'
                    WHEN o.[value_dec] IS NOT NULL THEN N'decimal'
                    WHEN o.[value_bool] IS NOT NULL THEN N'bool'
                    WHEN o.[value_datetime] IS NOT NULL THEN N'datetime'
                    WHEN o.[value_coding_code] IS NOT NULL THEN N'coding'
                    WHEN o.[value_string] IS NOT NULL THEN N'string'
                    ELSE NULL
                END AS value_type,
                CASE
                    WHEN o.[value_int] IS NOT NULL
                        OR o.[value_dec] IS NOT NULL
                        OR NULLIF(LTRIM(RTRIM(o.[value_string])), N'') IS NOT NULL
                        OR o.[value_datetime] IS NOT NULL
                        OR o.[value_bool] IS NOT NULL
                        OR o.[value_quantity] IS NOT NULL
                        OR NULLIF(LTRIM(RTRIM(o.[value_coding_code])), N'') IS NOT NULL
                        OR NULLIF(LTRIM(RTRIM(o.[value_coding_display])), N'') IS NOT NULL
                    THEN CAST(1 AS BIT)
                    ELSE CAST(0 AS BIT)
                END AS has_value,
                o.[value_int],
                o.[value_dec],
                o.[value_string] AS value_text,
                o.[value_datetime],
                o.[value_bool],
                o.[value_quantity],
                o.[value_unit],
                o.[value_coding_code],
                o.[value_coding_display],
                o.[element_nummer],
                o.[klynge_id],
                o.[klynge_id_hierarki],
                o.[klynge_parent_id],
                o.[repetition_id],
                o.[element_kode],
                o.[element_original_kode],
                o.[element_administrativ_enhed_id],
                op.[klynge_navne],
                op.[klynge_ledetekster],
                op.[klynge_ledetekst],
                o.[repetition_id_hierarki],
                o.[opdateret_tid],
                o.[xml_status],
                o.[xml_status_text]
            FROM [fhir].[Observation] AS o
            INNER JOIN {resources_table} AS cr
                ON cr.[source_resource] = N'Observation'
               AND cr.[source_resource_id] = o.[observation_id]
            LEFT JOIN [fhir].[Encounter] AS e
                ON e.[encounter_id] = o.[encounter_id]
            LEFT JOIN [codesystem].[SFIConcept] AS sc
                ON sc.[code] = o.[element_code]
            LEFT JOIN #sfi_props_pivot AS op
                ON op.[code] = o.[element_code]
            WHERE o.[element_system] = N'urn:dk:ifdb:sfi'
    """


def _inline_qr_build_leg(resources_table: str) -> str:
    """QR leg of inline SFI_datapoint build, scoped to a resources temp table."""
    return f"""
            SELECT
                LOWER(CONVERT(NVARCHAR(64), HASHBYTES('SHA2_256', CONVERT(NVARCHAR(4000),
                    CONCAT_WS(N'|',
                        N'QuestionnaireResponse',
                        COALESCE(qri.[qr_id], N''),
                        COALESCE(qri.[link_id], N''),
                        COALESCE(qri.[item_key], N''),
                        COALESCE(qri.[answer_index], N''),
                        -- element_code is the SFI code, which is the QR item's
                        -- own link_id (Questionnaire/QR co-derive it via
                        -- generate_sfi_code). Hashing link_id keeps datapoint_key
                        -- byte-identical to the prior qi.[element_code] form.
                        COALESCE(qri.[link_id], N'')
                ))), 2)) AS datapoint_key,
                qr.[qr_id] AS fhir_resource_id,
                qr.[qr_id] AS source_resource_id,
                qri.[link_id] AS source_link_id,
                qri.[item_key] AS source_item_key,
                qri.[answer_index] AS source_answer_index,
                COALESCE(
                    qri.[dw_timestamp_update],
                    qr.[dw_timestamp_update],
                    qri.[dw_timestamp_insert],
                    qr.[dw_timestamp_insert]
                ) AS source_last_updated,
                qr.[source_aktivitet_id] AS ifdb_aktivitet_id,
                CAST(NULL AS NVARCHAR(255)) AS ifdb_aktivitet_sfi_id,
                qr.[patient_id],
                qr.[encounter_id],
                COALESCE(qr.[organization_id], e.[service_provider_id])
                    AS organization_id,
                qr.[practitioner_id],
                qr.[practitioner_display],
                CAST(N'QuestionnaireResponse' AS NVARCHAR(50)) AS source_resource,
                qr.[authored] AS occurred_at,
                COALESCE(
                    qp.[source_model_name],
                    qr.[source_model_name],
                    qref.[questionnaire_canonical]
                ) AS source_model_name,
                COALESCE(
                    qp.[last_seen_in_version],
                    qp.[source_model_version],
                    qr.[source_model_version],
                    qref.[questionnaire_version]
                ) AS source_model_version,
                qp.[source_element_id],
                qp.[source_element_name],
                COALESCE(
                    qp.[source_element_label],
                    sc.[display],
                    qri.[text]
                ) AS source_element_label,
                qp.[sfi_path],
                qp.[source_hierarchy_path],
                qp.[source_group_path],
                qp.[source_repetition_path],
                qp.[klynge_niveau],
                qp.[repetition_niveau],
                qp.[raekke_type],
                qp.[klynge_type],
                qri.[klynge_valg_ledetekst],
                qri.[link_id] AS element_code,
                COALESCE(sc.[display], qri.[text]) AS element_display,
                CASE
                    WHEN qri.[value_int] IS NOT NULL THEN N'integer'
                    WHEN qri.[value_quantity] IS NOT NULL THEN N'quantity'
                    WHEN qri.[value_dec] IS NOT NULL THEN N'decimal'
                    WHEN qri.[value_bool] IS NOT NULL THEN N'bool'
                    WHEN qri.[value_datetime] IS NOT NULL THEN N'datetime'
                    WHEN qri.[value_coding_code] IS NOT NULL THEN N'coding'
                    WHEN qri.[value_string] IS NOT NULL THEN N'string'
                    ELSE NULL
                END AS value_type,
                CASE
                    WHEN qri.[value_int] IS NOT NULL
                        OR qri.[value_dec] IS NOT NULL
                        OR NULLIF(LTRIM(RTRIM(qri.[value_string])), N'') IS NOT NULL
                        OR qri.[value_datetime] IS NOT NULL
                        OR qri.[value_bool] IS NOT NULL
                        OR qri.[value_quantity] IS NOT NULL
                        OR NULLIF(LTRIM(RTRIM(qri.[value_coding_code])), N'') IS NOT NULL
                        OR NULLIF(LTRIM(RTRIM(qri.[value_coding_display])), N'') IS NOT NULL
                    THEN CAST(1 AS BIT)
                    ELSE CAST(0 AS BIT)
                END AS has_value,
                qri.[value_int],
                qri.[value_dec],
                qri.[value_string] AS value_text,
                qri.[value_datetime],
                qri.[value_bool],
                qri.[value_quantity],
                qri.[value_unit],
                qri.[value_coding_code],
                qri.[value_coding_display],
                qri.[element_nummer],
                qri.[klynge_id],
                qri.[klynge_id_hierarki],
                qri.[klynge_parent_id],
                qri.[repetition_id],
                qri.[element_kode],
                qri.[element_original_kode],
                qri.[element_administrativ_enhed_id],
                qp.[klynge_navne],
                qp.[klynge_ledetekster],
                qp.[klynge_ledetekst],
                qri.[repetition_id_hierarki],
                qri.[opdateret_tid],
                qri.[xml_status],
                qri.[xml_status_text]
            FROM [fhir].[QuestionnaireResponse_item] AS qri
            INNER JOIN {resources_table} AS cr
                ON cr.[source_resource] = N'QuestionnaireResponse'
               AND cr.[source_resource_id] = qri.[qr_id]
            INNER JOIN [fhir].[QuestionnaireResponse] AS qr
                ON qr.[qr_id] = qri.[qr_id]
            LEFT JOIN [fhir].[Encounter] AS e
                ON e.[encounter_id] = qr.[encounter_id]
            OUTER APPLY (
                SELECT
                    COALESCE(
                        qr.[questionnaire_canonical],
                        CASE
                            WHEN CHARINDEX(N'|', qr.[questionnaire]) > 0
                            THEN LEFT(qr.[questionnaire],
                                      CHARINDEX(N'|', qr.[questionnaire]) - 1)
                            ELSE qr.[questionnaire]
                        END
                    ) AS questionnaire_canonical,
                    COALESCE(
                        qr.[questionnaire_version],
                        CASE
                            WHEN CHARINDEX(N'|', qr.[questionnaire]) > 0
                            THEN SUBSTRING(
                                qr.[questionnaire],
                                CHARINDEX(N'|', qr.[questionnaire]) + 1,
                                LEN(qr.[questionnaire])
                            )
                            ELSE NULL
                        END
                    ) AS questionnaire_version
            ) AS qref
            -- No INNER JOIN to fhir.Questionnaire_item. The model-level
            -- Questionnaire is assembled by accumulating observed activity
            -- items and is therefore structurally allowed to be incomplete;
            -- gating datapoints on it silently DROPS every QR answer whose
            -- template row is missing (and can fan-out duplicate when the
            -- template holds duplicate (link_id,item_key)). The only fields the
            -- join provided -- element_code, element_display, element_system --
            -- are all derivable from the QR item itself: element_code IS the SFI
            -- code == qri.link_id; display falls back to the SFI catalog/qri.text;
            -- and every QuestionnaireResponse here is SFI-sourced (single writer
            -- = IFDB aktivitetsresultat). So we enrich via LEFT JOIN on the QR
            -- item's own link_id and never drop a clinical answer. Uncodeable
            -- items (empty link_id) are excluded and surfaced by the preflight
            -- guard rather than silently emitted.
            LEFT JOIN [codesystem].[SFIConcept] AS sc
                ON sc.[code] = qri.[link_id]
            LEFT JOIN #sfi_props_pivot AS qp
                ON qp.[code] = qri.[link_id]
            WHERE qri.[link_id] <> N''
    """


def _build_number_changed_resources_sql() -> str:
    """Add row_id to #changed_resources for batched iteration."""
    return """
        SET NOCOUNT ON;

        IF OBJECT_ID(N'tempdb..#changed_numbered') IS NOT NULL
            DROP TABLE #changed_numbered;

        SELECT
            ROW_NUMBER() OVER (ORDER BY source_resource, source_resource_id) AS row_id,
            source_resource,
            source_resource_id
        INTO #changed_numbered
        FROM #changed_resources;

        CREATE CLUSTERED INDEX [IX_changed_numbered_RowId]
            ON #changed_numbered ([row_id]);

        CREATE NONCLUSTERED INDEX [IX_changed_numbered_SourceResourceId]
            ON #changed_numbered ([source_resource], [source_resource_id]);

        SELECT COUNT(*) AS total_resources FROM #changed_numbered;
    """


def _build_inline_batch_apply_sql(batch_start: int, batch_end: int) -> str:
    """Build batch-scoped DELETE + INSERT using inline build with #sfi_props_pivot.

    Requires #changed_numbered and #sfi_props_pivot to exist in the session.
    """
    source_select = _outer_select_columns()
    insert_columns = _REFRESH_INSERT_COLUMNS
    obs_leg = _inline_obs_build_leg("#batch_resources")
    qr_leg = _inline_qr_build_leg("#batch_resources")

    return f"""
        SET NOCOUNT ON;
        SET XACT_ABORT ON;

        IF OBJECT_ID(N'tempdb..#batch_resources') IS NOT NULL
            DROP TABLE #batch_resources;

        SELECT source_resource, source_resource_id
        INTO #batch_resources
        FROM #changed_numbered
        WHERE row_id BETWEEN {batch_start} AND {batch_end};

        CREATE CLUSTERED INDEX [IX_batch_resources_SourceResourceId]
            ON #batch_resources ([source_resource], [source_resource_id]);

        -- No explicit BEGIN/COMMIT here: the connection runs autocommit=False, so
        -- the DELETE+INSERT below execute in the implicit transaction and are made
        -- durable by the per-batch conn.commit() in the Python loop. That keeps each
        -- batch an independently durable unit (relieves transaction-log pressure)
        -- while the session-level applock spans all batches. An in-SQL
        -- BEGIN/COMMIT would merely nest under the implicit transaction (no real
        -- commit until the connection commits) and defeat the batching.
        DELETE tgt
        FROM {_STORE_TABLE} tgt
        INNER JOIN #batch_resources br
            ON br.[source_resource] = tgt.[source_resource]
           AND br.[source_resource_id] = tgt.[source_resource_id];

        DECLARE @deleted_rows BIGINT = @@ROWCOUNT;

        INSERT INTO {_STORE_TABLE} (
            {insert_columns}
        )
        SELECT
            {source_select}
        FROM (
            {obs_leg}

            UNION ALL

            {qr_leg}
        ) AS b;

        DECLARE @inserted_rows BIGINT = @@ROWCOUNT;

        SELECT @deleted_rows AS deleted_rows, @inserted_rows AS inserted_rows;
    """


def _build_qr_sfi_preflight_sql() -> str:
    """Build SQL that checks QR -> SFI joinability for the current changed resources."""
    return """
        SET NOCOUNT ON;

        ;WITH changed_qr AS (
            SELECT cr.[source_resource_id] AS qr_id
            FROM #changed_resources AS cr
            WHERE cr.[source_resource] = N'QuestionnaireResponse'
        ),
        qr_key AS (
            SELECT
                qr.[qr_id],
                qr.[questionnaire],
                qr.[questionnaire_canonical],
                qr.[questionnaire_version],
                COALESCE(qr.[questionnaire_canonical], parsed.[questionnaire_canonical])
                    AS effective_questionnaire_canonical,
                COALESCE(qr.[questionnaire_version], parsed.[questionnaire_version])
                    AS effective_questionnaire_version,
                CASE
                    WHEN qr.[questionnaire] IS NOT NULL
                     AND qr.[questionnaire_canonical] IS NULL THEN 1
                    ELSE 0
                END AS missing_canonical,
                CASE
                    WHEN qr.[questionnaire] IS NOT NULL
                     AND CHARINDEX(N'|', qr.[questionnaire]) > 0
                     AND qr.[questionnaire_version] IS NULL THEN 1
                    ELSE 0
                END AS missing_version_when_reference_is_versioned,
                CASE
                    WHEN qr.[questionnaire] IS NOT NULL
                     AND (
                        qr.[questionnaire_canonical] IS NULL
                        OR (
                            CHARINDEX(N'|', qr.[questionnaire]) > 0
                            AND qr.[questionnaire_version] IS NULL
                        )
                     ) THEN 1
                    ELSE 0
                END AS fallback_key_usage
            FROM changed_qr AS cq
            INNER JOIN [fhir].[QuestionnaireResponse] AS qr
                ON qr.[qr_id] = cq.[qr_id]
            OUTER APPLY (
                SELECT
                    CASE
                        WHEN qr.[questionnaire] IS NULL THEN NULL
                        WHEN CHARINDEX(N'|', qr.[questionnaire]) > 0
                            THEN LEFT(qr.[questionnaire], CHARINDEX(N'|', qr.[questionnaire]) - 1)
                        ELSE qr.[questionnaire]
                    END AS questionnaire_canonical,
                    CASE
                        WHEN qr.[questionnaire] IS NULL THEN NULL
                        WHEN CHARINDEX(N'|', qr.[questionnaire]) > 0
                            THEN SUBSTRING(
                                qr.[questionnaire],
                                CHARINDEX(N'|', qr.[questionnaire]) + 1,
                                LEN(qr.[questionnaire])
                            )
                        ELSE NULL
                    END AS questionnaire_version
            ) AS parsed
        ),
        qri_scope AS (
            SELECT
                qri.[qr_id],
                qri.[link_id],
                qri.[item_key],
                qri.[answer_index],
                qk.[effective_questionnaire_canonical],
                qk.[effective_questionnaire_version]
            FROM [fhir].[QuestionnaireResponse_item] AS qri
            INNER JOIN qr_key AS qk
                ON qk.[qr_id] = qri.[qr_id]
        ),
        qri_dq AS (
            SELECT
                qri.[qr_id],
                qri.[link_id],
                qri.[item_key],
                qri.[answer_index],
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM [fhir].[Questionnaire_item] AS qi
                        WHERE qi.[questionnaire_url] = qri.[effective_questionnaire_canonical]
                          AND (
                                qi.[questionnaire_version] = qri.[effective_questionnaire_version]
                                OR (
                                    qi.[questionnaire_version] IS NULL
                                    AND qri.[effective_questionnaire_version] IS NULL
                                )
                          )
                          AND qi.[link_id] = qri.[link_id]
                          AND qi.[item_key] = qri.[item_key]
                    ) THEN 1
                    ELSE 0
                END AS joins_questionnaire_item,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM [fhir].[Questionnaire_item] AS qi
                        WHERE qi.[questionnaire_url] = qri.[effective_questionnaire_canonical]
                          AND (
                                qi.[questionnaire_version] = qri.[effective_questionnaire_version]
                                OR (
                                    qi.[questionnaire_version] IS NULL
                                    AND qri.[effective_questionnaire_version] IS NULL
                                )
                          )
                          AND qi.[link_id] = qri.[link_id]
                          AND qi.[item_key] = qri.[item_key]
                          AND qi.[element_system] = N'urn:dk:ifdb:sfi'
                    ) THEN 1
                    ELSE 0
                END AS joins_sfi_questionnaire_item
            FROM qri_scope AS qri
        )
        SELECT
            (SELECT COUNT(*) FROM changed_qr) AS qr_changed_resources,
            (SELECT COUNT(*) FROM qr_key) AS qr_total,
            (SELECT COUNT(*) FROM qr_key WHERE [questionnaire] IS NOT NULL)
                AS qr_with_questionnaire,
            (SELECT COALESCE(SUM([missing_canonical]), 0) FROM qr_key)
                AS qr_missing_canonical,
            (SELECT COUNT(*) FROM qr_key WHERE CHARINDEX(N'|', [questionnaire]) > 0)
                AS qr_versioned_reference_count,
            (SELECT COALESCE(SUM([missing_version_when_reference_is_versioned]), 0)
             FROM qr_key)
                AS qr_missing_version_when_reference_is_versioned,
            (
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT
                        qk.[effective_questionnaire_canonical],
                        qk.[effective_questionnaire_version]
                    FROM qr_key AS qk
                    WHERE qk.[effective_questionnaire_canonical] IS NOT NULL
                ) AS qr_ref
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM [fhir].[Questionnaire] AS q
                    WHERE q.[url] = qr_ref.[effective_questionnaire_canonical]
                      AND (
                            q.[version] = qr_ref.[effective_questionnaire_version]
                            OR (
                                q.[version] IS NULL
                                AND qr_ref.[effective_questionnaire_version] IS NULL
                            )
                      )
                )
            ) AS qr_questionnaire_reference_without_questionnaire,
            (SELECT COUNT(*) FROM qri_scope) AS qri_total,
            (SELECT COUNT(*) FROM qri_scope WHERE [link_id] IS NULL OR [link_id] = N'')
                AS qri_without_link_id,
            (SELECT COALESCE(SUM([joins_questionnaire_item]), 0) FROM qri_dq)
                AS qri_joinable_to_questionnaire_item,
            (SELECT COALESCE(SUM([joins_sfi_questionnaire_item]), 0) FROM qri_dq)
                AS qri_sfi_joinable,
            (SELECT COUNT(DISTINCT [qr_id]) FROM qri_dq WHERE [joins_sfi_questionnaire_item] = 1)
                AS qri_sfi_joinable_distinct_resources,
            (SELECT COALESCE(SUM([fallback_key_usage]), 0) FROM qr_key)
                AS qr_fallback_key_usage;
    """


def _build_reconciliation_qr_gap_sql() -> str:
    """Full-scope QR -> SFI join-gap check for the reconciliation/full-load path.

    Reconciliation has no #changed_resources to scope to, so this is a single
    set-based LEFT JOIN over ALL QuestionnaireResponse_item rows, mirroring the
    SFI_datapoint_build QR-leg join exactly (url+version+link_id+item_key,
    element_system='urn:dk:ifdb:sfi'). Returns the same three fields the
    incremental preflight feeds to _validate_qr_preflight_stats so the gap is
    evaluated by one shared rule. One hash/merge join, not per-row EXISTS.
    """
    return """
        SET NOCOUNT ON;

        ;WITH qr_key AS (
            SELECT
                qr.[qr_id],
                COALESCE(
                    qr.[questionnaire_canonical],
                    CASE
                        WHEN CHARINDEX(N'|', qr.[questionnaire]) > 0
                        THEN LEFT(qr.[questionnaire], CHARINDEX(N'|', qr.[questionnaire]) - 1)
                        ELSE qr.[questionnaire]
                    END
                ) AS effective_questionnaire_canonical,
                COALESCE(
                    qr.[questionnaire_version],
                    CASE
                        WHEN CHARINDEX(N'|', qr.[questionnaire]) > 0
                        THEN SUBSTRING(
                            qr.[questionnaire],
                            CHARINDEX(N'|', qr.[questionnaire]) + 1,
                            LEN(qr.[questionnaire])
                        )
                        ELSE NULL
                    END
                ) AS effective_questionnaire_version
            FROM [fhir].[QuestionnaireResponse] AS qr
        )
        SELECT
            COUNT_BIG(*) AS qri_total,
            COALESCE(SUM(CASE WHEN qi.[item_key] IS NOT NULL THEN 1 ELSE 0 END), 0)
                AS qri_sfi_joinable,
            COALESCE(SUM(CASE WHEN qri.[link_id] IS NULL OR qri.[link_id] = N''
                              THEN 1 ELSE 0 END), 0)
                AS qri_without_link_id
        FROM [fhir].[QuestionnaireResponse_item] AS qri
        INNER JOIN qr_key AS qk
            ON qk.[qr_id] = qri.[qr_id]
        LEFT JOIN [fhir].[Questionnaire_item] AS qi
            ON qi.[questionnaire_url] = qk.[effective_questionnaire_canonical]
           AND (
                qi.[questionnaire_version] = qk.[effective_questionnaire_version]
                OR (
                    qi.[questionnaire_version] IS NULL
                    AND qk.[effective_questionnaire_version] IS NULL
                )
           )
           AND qi.[link_id] = qri.[link_id]
           AND qi.[item_key] = qri.[item_key]
           AND qi.[element_system] = N'urn:dk:ifdb:sfi';
    """


def _build_reconciliation_setup_sql() -> str:
    """Build SQL that creates #reconcile_resources and returns total resource count.

    Reconciliation considers all current build sources *and* existing store
    resources so orphaned store rows are deleted.
    """
    return f"""
        SET NOCOUNT ON;
        SET XACT_ABORT ON;

        IF OBJECT_ID(N'tempdb..#reconcile_resources') IS NOT NULL
            DROP TABLE #reconcile_resources;

        WITH current_observation AS (
            SELECT
                CAST(N'Observation' AS NVARCHAR(50)) AS source_resource,
                o.[observation_id] AS source_resource_id,
                COALESCE(o.[dw_timestamp_update], o.[dw_timestamp_insert]) AS changed_at
            FROM [fhir].[Observation] AS o
            WHERE o.[element_system] = N'urn:dk:ifdb:sfi'
        ),
        current_qr AS (
            SELECT
                CAST(N'QuestionnaireResponse' AS NVARCHAR(50)) AS source_resource,
                qr.[qr_id] AS source_resource_id,
                COALESCE(qr.[dw_timestamp_update], qr.[dw_timestamp_insert]) AS changed_at
            FROM [fhir].[QuestionnaireResponse] AS qr

            UNION

            SELECT
                CAST(N'QuestionnaireResponse' AS NVARCHAR(50)) AS source_resource,
                qri.[qr_id] AS source_resource_id,
                COALESCE(qri.[dw_timestamp_update], qri.[dw_timestamp_insert]) AS changed_at
            FROM [fhir].[QuestionnaireResponse_item] AS qri
        ),
        existing_store AS (
            SELECT
                tgt.[source_resource],
                tgt.[source_resource_id],
                MAX(tgt.[source_last_updated]) AS changed_at
            FROM {_STORE_TABLE} AS tgt
            WHERE tgt.[source_resource_id] IS NOT NULL
            GROUP BY tgt.[source_resource], tgt.[source_resource_id]
        ),
        all_resources AS (
            SELECT * FROM current_observation
            UNION ALL
            SELECT * FROM current_qr
            UNION ALL
            SELECT * FROM existing_store
        ),
        grouped_resources AS (
            SELECT
                source_resource,
                source_resource_id,
                MAX(changed_at) AS changed_at
            FROM all_resources
            WHERE source_resource_id IS NOT NULL
            GROUP BY source_resource, source_resource_id
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY source_resource, source_resource_id) AS row_id,
            source_resource,
            source_resource_id,
            changed_at
        INTO #reconcile_resources
        FROM grouped_resources;

        CREATE CLUSTERED INDEX [IX_reconcile_resources_RowId]
            ON #reconcile_resources ([row_id]);

        CREATE NONCLUSTERED INDEX [IX_reconcile_resources_SourceResourceId]
            ON #reconcile_resources ([source_resource], [source_resource_id]);

        SELECT COUNT_BIG(*) AS total_resources FROM #reconcile_resources;
    """


def _build_reconciliation_batch_sql(batch_start: int, batch_end: int) -> str:
    """Build SQL for a single reconciliation batch (DELETE + INSERT for a row_id range)."""
    columns = _refresh_columns()
    source_select = ",\n                    ".join(
        (
            "SYSUTCDATETIME() AS [refreshed_at]"
            if column == "refreshed_at"
            else (
                "COALESCE(\n"
                "                            TRY_CONVERT(DATETIME2(0), b.[occurred_at], 126),\n"
                "                            TRY_CONVERT(DATETIME2(0), b.[occurred_at], 121),\n"
                "                            TRY_CONVERT(DATETIME2(0), b.[occurred_at], 23)\n"
                "                        ) AS [occurred_at_dt]"
                if column == "occurred_at_dt"
                else f"b.[{column}]"
            )
        )
        for column in columns
    )

    return f"""
        SET NOCOUNT ON;
        SET XACT_ABORT ON;

        IF OBJECT_ID(N'tempdb..#batch_resources') IS NOT NULL
            DROP TABLE #batch_resources;

        SELECT
            source_resource,
            source_resource_id
        INTO #batch_resources
        FROM #reconcile_resources
        WHERE row_id BETWEEN {batch_start} AND {batch_end};

        CREATE CLUSTERED INDEX [IX_batch_resources_SourceResourceId]
            ON #batch_resources ([source_resource], [source_resource_id]);

        -- No explicit BEGIN/COMMIT: autocommit=False means this DELETE+INSERT runs
        -- in the implicit transaction, committed per-batch by conn.commit() in the
        -- Python loop, so each reconciliation batch is independently durable.
        DELETE tgt
        FROM {_STORE_TABLE} AS tgt
        INNER JOIN #batch_resources AS br
            ON br.[source_resource] = tgt.[source_resource]
           AND br.[source_resource_id] = tgt.[source_resource_id];

        DECLARE @deleted_rows BIGINT = @@ROWCOUNT;

        INSERT INTO {_STORE_TABLE} (
            {_REFRESH_INSERT_COLUMNS}
        )
        SELECT
            {source_select}
        FROM {_BUILD_VIEW} AS b
        INNER JOIN #batch_resources AS br
            ON br.[source_resource] = b.[source_resource]
           AND br.[source_resource_id] = b.[source_resource_id];

        DECLARE @inserted_rows BIGINT = @@ROWCOUNT;

        SELECT @deleted_rows AS deleted_rows, @inserted_rows AS inserted_rows;
    """


def _ensure_store_table_exists(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> None:
    with mssql_target_dso.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT OBJECT_ID(N'[fhir].[SFI_datapoint_store]', N'U')")
            row = cursor.fetchone()
            if row and row[0]:
                return
        finally:
            cursor.close()

    context.log.warning("fhir.SFI_datapoint_store is missing; bootstrapping from DSO FHIR DDL.")
    execute_sql_file(mssql_target_dso, "tables/fhir/02_sfi_datapoint_store.sql", context)


def _is_store_missing_error(error: Exception) -> bool:
    message = str(error)
    return "SFI_datapoint_store" in message and "Cannot find the object" in message


def _fetch_single_row_result(cursor: Any, *, result_name: str) -> Any:
    """Return one row from the first tabular result set on the cursor.

    mssql-python can expose intermediate non-tabular results in multi-statement
    batches (e.g. rowcount/status messages). This helper advances via nextset()
    until a tabular result is available.
    """
    nextset = getattr(cursor, "nextset", None)

    while True:
        if getattr(cursor, "description", None):
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(f"Expected one row from {result_name}, but result set was empty")
            return row

        if nextset is None:
            break

        has_more = nextset()
        if not has_more:
            break

    raise RuntimeError(
        f"Could not locate tabular result set for {result_name}; "
        "driver returned no active result set"
    )


_QR_PREFLIGHT_COLUMNS = [
    "qr_changed_resources",
    "qr_total",
    "qr_with_questionnaire",
    "qr_missing_canonical",
    "qr_versioned_reference_count",
    "qr_missing_version_when_reference_is_versioned",
    "qr_questionnaire_reference_without_questionnaire",
    "qri_total",
    "qri_without_link_id",
    "qri_joinable_to_questionnaire_item",
    "qri_sfi_joinable",
    "qri_sfi_joinable_distinct_resources",
    "qr_fallback_key_usage",
]


def _row_to_qr_preflight_stats(row: Any) -> dict[str, int]:
    return {column: int(row[index] or 0) for index, column in enumerate(_QR_PREFLIGHT_COLUMNS)}


def _validate_qr_preflight_stats(stats: dict[str, int]) -> list[str]:
    """Return FATAL QR data-quality issues that would break the no-loss/no-dup
    invariant.

    Since the SFI_datapoint_build QR leg stopped gating on fhir.Questionnaire_item
    (element_code is the QR item's own link_id), an incomplete Questionnaire
    template no longer drops datapoints. The QR -> Questionnaire_item join gap is
    therefore an *advisory* completeness metric (logged by the caller), NOT a
    fatal error.

    What stays fatal is anything that means a clinical answer cannot be placed
    exactly once:
      - a QR item with no SFI code (empty/NULL link_id): the build excludes it
        (WHERE link_id <> ''), so passing it silently would be lost data;
      - a changed QR row whose questionnaire reference is malformed/unresolvable.
    Duplication is enforced independently by the
    UX_FHIR_SFI_datapoint_store_DatapointKey unique index, which fails the load
    loudly on any duplicate datapoint grain.
    """
    errors: list[str] = []

    qr_missing_canonical = stats.get("qr_missing_canonical", 0)
    if qr_missing_canonical > 0:
        errors.append(
            f"{qr_missing_canonical} changed QR rows with questionnaire "
            "are missing questionnaire_canonical"
        )

    qr_missing_version = stats.get("qr_missing_version_when_reference_is_versioned", 0)
    if qr_missing_version > 0:
        errors.append(
            f"{qr_missing_version} changed QR rows "
            "have a versioned questionnaire reference but missing questionnaire_version"
        )

    qr_missing_questionnaire = stats.get("qr_questionnaire_reference_without_questionnaire", 0)
    if qr_missing_questionnaire > 0:
        errors.append(
            f"{qr_missing_questionnaire} changed QR "
            "questionnaire references do not exist in fhir.Questionnaire"
        )

    # Uncodeable items: a changed QR item with an empty/NULL link_id has no SFI
    # code, so the build excludes it (WHERE link_id <> ''). link_id is the
    # generate_sfi_code output and is structurally never empty for a real SFI
    # item (it falls back to '_fb_*'), so an empty one is a genuine defect that
    # would silently drop a clinical answer -> fail loudly.
    qri_uncodeable = stats.get("qri_without_link_id", 0)
    if qri_uncodeable > 0:
        errors.append(
            f"{qri_uncodeable} changed QR items have no SFI link_id (empty/NULL) "
            "and cannot be coded; the build excludes them, which would silently "
            "drop those clinical answers"
        )

    return errors


def _qr_preflight_mode() -> str:
    """Resolve the QR -> SFI preflight mode.

    Defaults to 'fail' so the pipeline is safe WITHOUT any environment override:
    a per-item join gap that the build INNER JOIN would otherwise silently drop
    aborts the run *before* the DELETE (incl. the partial case). In a healthy
    pipeline the gap is structurally zero — Questionnaire_item is synced before
    the refresh (asset deps) and the QR item link_id/item_key and the
    Questionnaire_item element_code are co-derived from the same source row by
    the same generate_sfi_code — so 'fail' only fires on genuine incompleteness.
    Operators may set SFI_DATAPOINT_QR_PREFLIGHT_MODE=warn (report but continue,
    e.g. an exploratory historical pass) or =skip (opt out entirely).
    """
    mode = os.getenv(_QR_PREFLIGHT_MODE_ENV, _QR_PREFLIGHT_MODE_FAIL).strip().lower()
    if mode == _QR_PREFLIGHT_MODE_SKIP:
        return _QR_PREFLIGHT_MODE_SKIP
    if mode == _QR_PREFLIGHT_MODE_WARN:
        return _QR_PREFLIGHT_MODE_WARN
    return _QR_PREFLIGHT_MODE_FAIL


def _qr_zero_warn_threshold(context: dg.AssetExecutionContext) -> int:
    raw_value = os.getenv(_QR_ZERO_WARN_THRESHOLD_ENV)
    if raw_value is None or raw_value.strip() == "":
        return _DEFAULT_QR_ZERO_WARN_THRESHOLD

    try:
        value = int(raw_value)
    except ValueError:
        context.log.warning(
            "Invalid %s=%r; using default threshold %d.",
            _QR_ZERO_WARN_THRESHOLD_ENV,
            raw_value,
            _DEFAULT_QR_ZERO_WARN_THRESHOLD,
        )
        return _DEFAULT_QR_ZERO_WARN_THRESHOLD

    return max(value, 0)


def _reconciliation_batch_size(context: dg.AssetExecutionContext) -> int:
    raw_value = os.getenv(_RECONCILIATION_BATCH_SIZE_ENV)
    if raw_value is None or raw_value.strip() == "":
        return _DEFAULT_RECONCILIATION_BATCH_SIZE

    try:
        value = int(raw_value)
    except ValueError:
        context.log.warning(
            "Invalid %s=%r; using default batch size %d.",
            _RECONCILIATION_BATCH_SIZE_ENV,
            raw_value,
            _DEFAULT_RECONCILIATION_BATCH_SIZE,
        )
        return _DEFAULT_RECONCILIATION_BATCH_SIZE

    if value <= 0:
        context.log.warning(
            "Invalid %s=%r; using default batch size %d.",
            _RECONCILIATION_BATCH_SIZE_ENV,
            raw_value,
            _DEFAULT_RECONCILIATION_BATCH_SIZE,
        )
        return _DEFAULT_RECONCILIATION_BATCH_SIZE

    return value


def _incremental_batch_size(context: dg.AssetExecutionContext) -> int:
    raw_value = os.getenv(_INCREMENTAL_BATCH_SIZE_ENV)
    if raw_value is None or raw_value.strip() == "":
        return _DEFAULT_INCREMENTAL_BATCH_SIZE

    try:
        value = int(raw_value)
    except ValueError:
        context.log.warning(
            "Invalid %s=%r; using default batch size %d.",
            _INCREMENTAL_BATCH_SIZE_ENV,
            raw_value,
            _DEFAULT_INCREMENTAL_BATCH_SIZE,
        )
        return _DEFAULT_INCREMENTAL_BATCH_SIZE

    if value <= 0:
        context.log.warning(
            "Invalid %s=%r; using default batch size %d.",
            _INCREMENTAL_BATCH_SIZE_ENV,
            raw_value,
            _DEFAULT_INCREMENTAL_BATCH_SIZE,
        )
        return _DEFAULT_INCREMENTAL_BATCH_SIZE

    return value


def _execute_incremental_refresh(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    watermark_table: str,
) -> dg.MaterializeResult:
    """Execute watermark-driven incremental refresh."""
    start_ms = time.monotonic_ns() // 1_000_000

    state_mgr = SyncStateManager(mssql_target_dso, context)
    last_watermark = state_mgr.get_last_sync_timestamp(_WATERMARK_SCHEMA, watermark_table)

    is_first_run = last_watermark is None
    # Use the same timestamp format as the rest of the DSO watermark logic (sync_utils.py)
    # for safe embedding in generated SQL literals.
    last_watermark_str = last_watermark.strftime("%Y-%m-%d %H:%M:%S") if last_watermark else None
    # Human-friendly form for logs only
    log_watermark = last_watermark.isoformat() if last_watermark else None

    if is_first_run:
        context.log.info(
            "No watermark found for %s.%s — running full initial load.",
            _WATERMARK_SCHEMA,
            watermark_table,
        )
    else:
        context.log.info(
            "Incremental refresh since watermark %s for %s.%s",
            log_watermark,
            _WATERMARK_SCHEMA,
            watermark_table,
        )

    changed_sql = _build_changed_resources_sql(last_watermark_str)
    pivot_sql = _build_sfi_props_pivot_sql()
    number_sql = _build_number_changed_resources_sql()
    preflight_mode = _qr_preflight_mode()
    batch_size = _incremental_batch_size(context)

    with mssql_target_dso.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(changed_sql)

            cursor.execute("SELECT COUNT(*) FROM #changed_resources")
            changed_count = int(cursor.fetchone()[0])

            if changed_count == 0 and not is_first_run:
                cursor.execute(f"SELECT COUNT(*) FROM {_STORE_TABLE}")
                row_count = int(cursor.fetchone()[0])
                conn.commit()

                duration_ms = (time.monotonic_ns() // 1_000_000) - start_ms
                state_mgr.update_sync_state(
                    _WATERMARK_SCHEMA,
                    watermark_table,
                    rows_affected=0,
                    duration_ms=int(duration_ms),
                    status="success_noop",
                )

                context.log.info(
                    "No changed resources since watermark — skipping refresh. Store has %d rows.",
                    row_count,
                )
                return dg.MaterializeResult(
                    metadata={
                        "target_table": dg.MetadataValue.text("fhir.SFI_datapoint_store"),
                        "row_count": dg.MetadataValue.int(row_count),
                        "source_view": dg.MetadataValue.text("fhir.SFI_datapoint_build"),
                        "refresh_mode": dg.MetadataValue.text("incremental_noop"),
                        "changed_resources": dg.MetadataValue.int(0),
                        "deleted_rows": dg.MetadataValue.int(0),
                        "inserted_rows": dg.MetadataValue.int(0),
                    }
                )

            context.log.info("Found %d changed resources to process.", changed_count)

            pivot_start = time.monotonic_ns() // 1_000_000
            cursor.execute(pivot_sql)
            pivot_ms = (time.monotonic_ns() // 1_000_000) - pivot_start
            context.log.info("Pre-pivoted sfi_properties into #sfi_props_pivot (%dms).", pivot_ms)

            preflight_stats: dict[str, int] = {}
            preflight_errors: list[str] = []

            if preflight_mode == _QR_PREFLIGHT_MODE_SKIP:
                context.log.info(
                    "QR -> SFI preflight skipped (%s=%s). The build INNER JOIN on "
                    "Questionnaire_item still drops un-joinable QR rows, so no invalid "
                    "rows are written — but this does NOT detect silent under-emission "
                    "from an incomplete Questionnaire_item. Set %s=warn for join "
                    "diagnostics.",
                    _QR_PREFLIGHT_MODE_ENV,
                    _QR_PREFLIGHT_MODE_SKIP,
                    _QR_PREFLIGHT_MODE_ENV,
                )
            else:
                preflight_sql = _build_qr_sfi_preflight_sql()
                cursor.execute(preflight_sql)
                preflight_row = _fetch_single_row_result(
                    cursor, result_name="QR -> SFI preflight stats"
                )
                preflight_stats = _row_to_qr_preflight_stats(preflight_row)
                preflight_errors = _validate_qr_preflight_stats(preflight_stats)

                context.log.info(
                    "QR -> SFI preflight: %d changed QR resources, %d QRI rows, "
                    "%d SFI-joinable QRI rows across %d QR resources, "
                    "fallback_key_usage=%d.",
                    preflight_stats["qr_changed_resources"],
                    preflight_stats["qri_total"],
                    preflight_stats["qri_sfi_joinable"],
                    preflight_stats["qri_sfi_joinable_distinct_resources"],
                    preflight_stats["qr_fallback_key_usage"],
                )

                if preflight_errors:
                    message = "QR -> SFI preflight found issues: " + "; ".join(preflight_errors)
                    if preflight_mode == _QR_PREFLIGHT_MODE_FAIL:
                        conn.rollback()
                        raise RuntimeError(message)
                    context.log.warning(
                        "%s. Continuing because %s=%s.",
                        message,
                        _QR_PREFLIGHT_MODE_ENV,
                        _QR_PREFLIGHT_MODE_WARN,
                    )

            cursor.execute(number_sql)
            number_row = _fetch_single_row_result(cursor, result_name="number changed resources")
            total_resources = int(number_row[0])
            total_batches = (total_resources + batch_size - 1) // batch_size

            context.log.info(
                "Batched apply: %d resources in %d batches of %d.",
                total_resources,
                total_batches,
                batch_size,
            )

            lock_sql = f"""
                DECLARE @lock_result INT;
                EXEC @lock_result = sys.sp_getapplock
                    @Resource = N'{_REFRESH_LOCK_NAME}',
                    @LockMode = N'Exclusive',
                    @LockOwner = N'Session',
                    @LockTimeout = 0;
                SELECT @lock_result AS lock_result;
            """
            cursor.execute(lock_sql)
            lock_row = _fetch_single_row_result(cursor, result_name="applock acquire")
            if int(lock_row[0]) < 0:
                raise RuntimeError("Could not acquire SFI_datapoint_store refresh lock")

            deleted_total = 0
            inserted_total = 0
            batch_num = 0

            try:
                batch_start = 1
                while batch_start <= total_resources:
                    batch_end = batch_start + batch_size - 1
                    batch_num += 1

                    batch_sql = _build_inline_batch_apply_sql(batch_start, batch_end)
                    cursor.execute(batch_sql)
                    batch_row = _fetch_single_row_result(
                        cursor, result_name=f"batch {batch_num} stats"
                    )
                    batch_deleted = int(batch_row[0])
                    batch_inserted = int(batch_row[1])
                    deleted_total += batch_deleted
                    inserted_total += batch_inserted

                    # Commit each batch independently. The connection runs
                    # autocommit=False, so without this every batch would nest in one
                    # outer transaction and the log would never relieve (and a late
                    # failure would roll back all prior batches). The session-level
                    # applock acquired above spans these commits. On failure the
                    # exception propagates before update_sync_state, so the watermark
                    # is not advanced and the next run reprocesses idempotently.
                    conn.commit()

                    context.log.info(
                        "Batch %d/%d complete: deleted %d, inserted %d "
                        "(cumulative: %d deleted, %d inserted).",
                        batch_num,
                        total_batches,
                        batch_deleted,
                        batch_inserted,
                        deleted_total,
                        inserted_total,
                    )

                    batch_start = batch_end + 1
            finally:
                cursor.execute(
                    f"EXEC sys.sp_releaseapplock "
                    f"@Resource = N'{_REFRESH_LOCK_NAME}', @LockOwner = N'Session';"
                )

            cursor.execute(f"SELECT COUNT(*) FROM {_STORE_TABLE}")
            row_count = int(cursor.fetchone()[0])

            # Post-apply follow-up signal: changed QR resources that produced
            # zero datapoints (possible silent under-emission via the build
            # INNER JOIN on Questionnaire_item — undetectable in preflight
            # mode 'skip'). Reported as metadata; WARNING above threshold.
            cursor.execute(_build_qr_zero_datapoint_count_sql())
            qr_zero_datapoints = int(cursor.fetchone()[0])

            conn.commit()

            zero_threshold = _qr_zero_warn_threshold(context)
            if qr_zero_datapoints > zero_threshold:
                context.log.warning(
                    "%d changed QuestionnaireResponse resources yielded ZERO "
                    "datapoints (threshold=%d). Possible silent under-emission "
                    "from an incomplete fhir.Questionnaire_item — set %s=warn "
                    "for join diagnostics. Note: QRs without SFI elements "
                    "legitimately yield zero; tune %s if a steady baseline exists.",
                    qr_zero_datapoints,
                    zero_threshold,
                    _QR_PREFLIGHT_MODE_ENV,
                    _QR_ZERO_WARN_THRESHOLD_ENV,
                )

            duration_ms = (time.monotonic_ns() // 1_000_000) - start_ms
            state_mgr.update_sync_state(
                _WATERMARK_SCHEMA,
                watermark_table,
                rows_affected=inserted_total,
                duration_ms=int(duration_ms),
                status="success",
            )

            refresh_mode = "incremental_full_initial" if is_first_run else "incremental"
            context.log.info(
                "Refreshed %s: %d changed resources, %d batches, %d deleted, "
                "%d inserted, %d total rows (mode=%s, %dms)",
                _STORE_TABLE,
                changed_count,
                batch_num,
                deleted_total,
                inserted_total,
                row_count,
                refresh_mode,
                duration_ms,
            )

            return dg.MaterializeResult(
                metadata={
                    "target_table": dg.MetadataValue.text("fhir.SFI_datapoint_store"),
                    "row_count": dg.MetadataValue.int(row_count),
                    "source_view": dg.MetadataValue.text("fhir.SFI_datapoint_build"),
                    "refresh_mode": dg.MetadataValue.text(refresh_mode),
                    "qr_preflight_mode": dg.MetadataValue.text(preflight_mode),
                    "qr_preflight_status": dg.MetadataValue.text(
                        "skipped"
                        if preflight_mode == _QR_PREFLIGHT_MODE_SKIP
                        else ("failed_warn_only" if preflight_errors else "success")
                    ),
                    "qr_preflight_errors": dg.MetadataValue.text("; ".join(preflight_errors)),
                    "changed_resources": dg.MetadataValue.int(changed_count),
                    "deleted_rows": dg.MetadataValue.int(deleted_total),
                    "inserted_rows": dg.MetadataValue.int(inserted_total),
                    "qr_resources_zero_datapoints": dg.MetadataValue.int(qr_zero_datapoints),
                    "incremental_batches": dg.MetadataValue.int(batch_num),
                    "incremental_batch_size": dg.MetadataValue.int(batch_size),
                    "duration_ms": dg.MetadataValue.int(int(duration_ms)),
                    **{key: dg.MetadataValue.int(value) for key, value in preflight_stats.items()},
                }
            )
        except Exception as exc:
            if _is_store_missing_error(exc):
                conn.rollback()
                context.log.warning(
                    "Encountered missing %s during refresh; "
                    "rebuilding from DSO FHIR DDL and retrying with batched reconciliation.",
                    _STORE_TABLE,
                )
                _ensure_store_table_exists(context, mssql_target_dso)
                return _execute_full_reconciliation(context, mssql_target_dso, watermark_table)
            raise
        finally:
            cursor.close()


def _execute_full_reconciliation(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    watermark_table: str,
) -> dg.MaterializeResult:
    """Execute batched full reconciliation with per-batch progress logging."""
    start_ms = time.monotonic_ns() // 1_000_000
    batch_size = _reconciliation_batch_size(context)

    preflight_mode = _qr_preflight_mode()

    with mssql_target_dso.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # QR -> SFI join-gap guard BEFORE any DELETE. Reconciliation rebuilds
            # from all current Questionnaire_item, so a gap here means QR items
            # whose SFI Questionnaire_item is genuinely missing/version-skewed —
            # the build INNER JOIN would silently drop them and the per-batch
            # DELETE would make that permanent. In 'fail' (default) this aborts
            # the whole rebuild before touching the store.
            recon_preflight_stats: dict[str, int] = {}
            recon_preflight_errors: list[str] = []
            if preflight_mode == _QR_PREFLIGHT_MODE_SKIP:
                context.log.info(
                    "Reconciliation QR -> SFI gap check skipped (%s=%s).",
                    _QR_PREFLIGHT_MODE_ENV,
                    _QR_PREFLIGHT_MODE_SKIP,
                )
            else:
                cursor.execute(_build_reconciliation_qr_gap_sql())
                gap_row = _fetch_single_row_result(
                    cursor, result_name="reconciliation QR -> SFI gap"
                )
                recon_preflight_stats = {
                    "qri_total": int(gap_row[0] or 0),
                    "qri_sfi_joinable": int(gap_row[1] or 0),
                    "qri_without_link_id": int(gap_row[2] or 0),
                }
                recon_preflight_errors = _validate_qr_preflight_stats(recon_preflight_stats)
                context.log.info(
                    "Reconciliation QR -> SFI gap: %d QRI rows, %d SFI-joinable.",
                    recon_preflight_stats["qri_total"],
                    recon_preflight_stats["qri_sfi_joinable"],
                )
                if recon_preflight_errors:
                    message = "Reconciliation QR -> SFI gap check found issues: " + "; ".join(
                        recon_preflight_errors
                    )
                    if preflight_mode == _QR_PREFLIGHT_MODE_FAIL:
                        conn.rollback()
                        raise RuntimeError(message)
                    context.log.warning(
                        "%s. Continuing because %s=%s.",
                        message,
                        _QR_PREFLIGHT_MODE_ENV,
                        _QR_PREFLIGHT_MODE_WARN,
                    )

            setup_sql = _build_reconciliation_setup_sql()
            cursor.execute(setup_sql)
            setup_row = _fetch_single_row_result(cursor, result_name="reconciliation setup total")
            total_resources = int(setup_row[0])

            total_batches = (total_resources + batch_size - 1) // batch_size
            context.log.info(
                "Reconciliation setup complete: %d resources, %d batches of %d.",
                total_resources,
                total_batches,
                batch_size,
            )

            lock_sql = f"""
                DECLARE @lock_result INT;
                EXEC @lock_result = sys.sp_getapplock
                    @Resource = N'{_REFRESH_LOCK_NAME}',
                    @LockMode = N'Exclusive',
                    @LockOwner = N'Session',
                    @LockTimeout = 0;
                SELECT @lock_result AS lock_result;
            """
            cursor.execute(lock_sql)
            lock_row = _fetch_single_row_result(cursor, result_name="applock acquire")
            if int(lock_row[0]) < 0:
                raise RuntimeError("Could not acquire SFI_datapoint_store refresh lock")

            deleted_total = 0
            inserted_total = 0
            batch_num = 0

            try:
                batch_start = 1
                while batch_start <= total_resources:
                    batch_end = batch_start + batch_size - 1
                    batch_num += 1

                    batch_sql = _build_reconciliation_batch_sql(batch_start, batch_end)
                    cursor.execute(batch_sql)
                    batch_row = _fetch_single_row_result(
                        cursor, result_name=f"batch {batch_num} stats"
                    )
                    batch_deleted = int(batch_row[0])
                    batch_inserted = int(batch_row[1])
                    deleted_total += batch_deleted
                    inserted_total += batch_inserted

                    # Commit each batch independently (autocommit=False); see the
                    # incremental path for the rationale. Keeps reconciliation
                    # batches durable and bounds transaction-log growth.
                    conn.commit()

                    context.log.info(
                        "Batch %d/%d complete: deleted %d, inserted %d "
                        "(cumulative: %d deleted, %d inserted).",
                        batch_num,
                        total_batches,
                        batch_deleted,
                        batch_inserted,
                        deleted_total,
                        inserted_total,
                    )

                    batch_start = batch_end + 1
            finally:
                cursor.execute(
                    f"EXEC sys.sp_releaseapplock "
                    f"@Resource = N'{_REFRESH_LOCK_NAME}', @LockOwner = N'Session';"
                )

            cursor.execute(f"SELECT COUNT(*) FROM {_STORE_TABLE}")
            row = cursor.fetchone()
            row_count = int(row[0]) if row else 0
            conn.commit()

            duration_ms = (time.monotonic_ns() // 1_000_000) - start_ms

            state_mgr = SyncStateManager(mssql_target_dso, context)
            state_mgr.update_sync_state(
                _WATERMARK_SCHEMA,
                watermark_table,
                rows_affected=inserted_total,
                duration_ms=int(duration_ms),
                status="success_reconciliation",
            )

            context.log.info(
                "Batched reconciliation of %s completed: %d resources, %d batches, "
                "%d deleted, %d inserted, %d total rows (%dms)",
                _STORE_TABLE,
                total_resources,
                batch_num,
                deleted_total,
                inserted_total,
                row_count,
                duration_ms,
            )
            return dg.MaterializeResult(
                metadata={
                    "target_table": dg.MetadataValue.text("fhir.SFI_datapoint_store"),
                    "row_count": dg.MetadataValue.int(row_count),
                    "source_view": dg.MetadataValue.text("fhir.SFI_datapoint_build"),
                    "refresh_mode": dg.MetadataValue.text("batched_reconciliation"),
                    "qr_preflight_mode": dg.MetadataValue.text(preflight_mode),
                    "qr_preflight_status": dg.MetadataValue.text(
                        "skipped"
                        if preflight_mode == _QR_PREFLIGHT_MODE_SKIP
                        else ("warned" if recon_preflight_errors else "success")
                    ),
                    "qr_preflight_errors": dg.MetadataValue.text("; ".join(recon_preflight_errors)),
                    "reconciled_resources": dg.MetadataValue.int(total_resources),
                    "reconciliation_batches": dg.MetadataValue.int(batch_num),
                    "deleted_rows": dg.MetadataValue.int(deleted_total),
                    "inserted_rows": dg.MetadataValue.int(inserted_total),
                    "batch_size": dg.MetadataValue.int(batch_size),
                    "duration_ms": dg.MetadataValue.int(int(duration_ms)),
                }
            )
        finally:
            cursor.close()


def _refresh_sfi_datapoint_store(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    watermark_table: str,
) -> dg.MaterializeResult:
    _ensure_store_table_exists(context, mssql_target_dso)

    state_mgr = SyncStateManager(mssql_target_dso, context)
    last_watermark = state_mgr.get_last_sync_timestamp(_WATERMARK_SCHEMA, watermark_table)

    if last_watermark is None:
        context.log.info(
            "No watermark for %s.%s — using batched reconciliation for initial load.",
            _WATERMARK_SCHEMA,
            watermark_table,
        )
        return _execute_full_reconciliation(context, mssql_target_dso, watermark_table)

    return _execute_incremental_refresh(context, mssql_target_dso, watermark_table)


def _reconcile_sfi_datapoint_store(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    watermark_table: str,
) -> dg.MaterializeResult:
    _ensure_store_table_exists(context, mssql_target_dso)
    return _execute_full_reconciliation(context, mssql_target_dso, watermark_table)


def _create_refresh_asset(
    *,
    name: str,
    group_name: str,
    upstream_deps: list[str],
    description: str,
    watermark_table: str,
) -> dg.AssetsDefinition:
    @dg.asset(
        name=name,
        group_name=group_name,
        compute_kind="mssql",
        deps=upstream_deps,
        tags={"domain": "fhir", "layer": "dso_sync", "type": "sfi_datapoint_refresh"},
        description=description,
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_dso: MSSQLResource,
    ) -> dg.MaterializeResult:
        return _refresh_sfi_datapoint_store(context, mssql_target_dso, watermark_table)

    return _asset


def _create_reconciliation_asset(
    *,
    name: str,
    group_name: str,
    upstream_deps: list[str],
    description: str,
    watermark_table: str,
) -> dg.AssetsDefinition:
    @dg.asset(
        name=name,
        group_name=group_name,
        compute_kind="mssql",
        deps=upstream_deps,
        tags={"domain": "fhir", "layer": "dso_sync", "type": "sfi_datapoint_reconciliation"},
        description=description,
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_dso: MSSQLResource,
    ) -> dg.MaterializeResult:
        return _reconcile_sfi_datapoint_store(context, mssql_target_dso, watermark_table)

    return _asset


dso_refresh_sfi_datapoint_daily = _create_refresh_asset(
    name="dso_refresh_sfi_datapoint_daily",
    group_name="fhir_dso_sync_dfm",
    upstream_deps=_SFI_UPSTREAM_DEPS,
    watermark_table=_WATERMARK_TABLE_DAILY,
    description=(
        "Incremental refresh of fhir.SFI_datapoint_store on DSO — "
        "processes only resources changed since last watermark. Runs in "
        "fhir_ifdb_dfm_sof_dso_job after Observation/QR DSO sync."
    ),
)

dso_refresh_sfi_datapoint_monthly = _create_refresh_asset(
    name="dso_refresh_sfi_datapoint_monthly",
    group_name="fhir_dso_sync_maintenance",
    upstream_deps=_SFI_UPSTREAM_DEPS,
    watermark_table=_WATERMARK_TABLE_MONTHLY,
    description=(
        "Ad-hoc incremental refresh of fhir.SFI_datapoint_store (monthly watermark). "
        "Not scheduled — DFM SFI has no monthly cadence; nightly delta covers it."
    ),
)

dso_reconcile_sfi_datapoint_store = _create_reconciliation_asset(
    name="dso_reconcile_sfi_datapoint_store",
    group_name="fhir_dso_sync_maintenance",
    upstream_deps=_SFI_UPSTREAM_DEPS,
    watermark_table=_WATERMARK_TABLE_RECONCILIATION,
    description=(
        "Manual/periodic batched reconciliation of fhir.SFI_datapoint_store on DSO "
        "to rebuild historical SFI datapoints outside daily/monthly incremental scope"
    ),
)

all_sfi_datapoint_refresh_assets = [
    dso_refresh_sfi_datapoint_daily,
    dso_refresh_sfi_datapoint_monthly,
    dso_reconcile_sfi_datapoint_store,
]

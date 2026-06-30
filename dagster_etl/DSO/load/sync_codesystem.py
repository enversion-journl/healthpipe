# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSO code system sync assets: persist codesystem tables from staging to DSO."""

import dagster as dg
from core.resources import MSSQLResource
from DSO.load.sync_configs import CODESYSTEM_SYNC_CONFIGS
from DSO.load.sync_utils import (
    SYNC_ENGINE_PYTHON_FALLBACK,
    compare_row_counts,
    sync_table_via_linked_server,
)


def _materialize_sync_result(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
    config_key: str,
) -> dg.MaterializeResult:
    """Run one code system sync and return standardized metadata."""
    config = CODESYSTEM_SYNC_CONFIGS[config_key]
    result = sync_table_via_linked_server(
        context,
        mssql_target_dso,
        config,
        mssql_source_staging=mssql_source_staging,
        validate_schemas=True,
    )
    counts = compare_row_counts(
        context,
        mssql_target_dso,
        config.schema,
        config.table,
        mssql_source_staging=mssql_source_staging,
    )
    fallback_reason = result.get("fallback_reason")
    sync_engine = (
        SYNC_ENGINE_PYTHON_FALLBACK
        if fallback_reason
        else result.get("sync_engine", "linked_server")
    )
    metadata = {
        "rows_affected": result["rows_affected"],
        "sync_mode": result["mode"],
        "sync_engine": sync_engine,
        "sync_duration_seconds": result.get("sync_duration_seconds", 0.0),
        "rows_per_second": result.get("rows_per_second", 0.0),
        "staging_count": counts["staging_count"],
        "dso_count": counts["dso_count"],
        "counts_match": counts["match"],
    }
    if fallback_reason:
        metadata["fallback_reason"] = fallback_reason
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name="dso_sync_codesystem_sfi_concept",
    group_name="codesystem_dso_sync",
    compute_kind="mssql",
    deps=["cs_sfi", "dso_infra_codesystem", "sof_observation_daily"],
    tags={"domain": "codesystem", "layer": "dso_sync", "type": "codesystem"},
    description="Sync codesystem.sfi_concept from staging to DSO",
)
def dso_sync_codesystem_sfi_concept(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "codesystem_sfi_concept",
    )


@dg.asset(
    name="dso_sync_codesystem_sfi_properties",
    group_name="codesystem_dso_sync",
    compute_kind="mssql",
    deps=["cs_sfi", "dso_infra_codesystem", "sof_observation_daily"],
    tags={"domain": "codesystem", "layer": "dso_sync", "type": "codesystem"},
    description="Sync codesystem.sfi_properties from staging to DSO",
)
def dso_sync_codesystem_sfi_properties(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "codesystem_sfi_properties",
    )


@dg.asset(
    name="dso_sync_codesystem_sfi_hierarchy",
    group_name="codesystem_dso_sync",
    compute_kind="mssql",
    deps=["cs_sfi", "dso_infra_codesystem", "sof_observation_daily"],
    tags={"domain": "codesystem", "layer": "dso_sync", "type": "codesystem"},
    description="Sync codesystem.sfi_hierarchy from staging to DSO",
)
def dso_sync_codesystem_sfi_hierarchy(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "codesystem_sfi_hierarchy",
    )


# because it has an extra sof_observation_daily dependency).
_CODESYSTEM_SYNC_SPECS: list[tuple[str, str]] = [
    ("sks", "cs_sks"),
    ("sor", "cs_sor"),
    ("ean", "cs_ean"),
    ("lager", "cs_lager"),
    ("okosted", "cs_okosted"),
    ("rekvirent", "cs_rekvirent"),
]
_KINDS = ["concept", "properties", "hierarchy"]


def _make_codesystem_sync_asset(codesystem: str, kind: str, cs_asset: str):
    """Build one dso_sync_codesystem_<cs>_<kind> asset (codesystem.* -> codesystem.* on DSO)."""
    config_key = f"codesystem_{codesystem}_{kind}"

    @dg.asset(
        name=f"dso_sync_codesystem_{codesystem}_{kind}",
        group_name="codesystem_dso_sync",
        compute_kind="mssql",
        deps=[cs_asset, "dso_infra_codesystem"],
        tags={"domain": "codesystem", "layer": "dso_sync", "type": "codesystem"},
        description=f"Sync codesystem.{codesystem}_{kind} from staging to DSO",
    )
    def _sync_asset(
        context: dg.AssetExecutionContext,
        mssql_target_dso: MSSQLResource,
        mssql_source_staging: MSSQLResource,
    ) -> dg.MaterializeResult:
        return _materialize_sync_result(context, mssql_target_dso, mssql_source_staging, config_key)

    return _sync_asset


# Factory for the non-SFI codesystems (SFI keeps its hand-written assets above
_factory_codesystem_assets = [
    _make_codesystem_sync_asset(cs, kind, cs_asset)
    for cs, cs_asset in _CODESYSTEM_SYNC_SPECS
    for kind in _KINDS
]


def _build_sfi_search_document_refresh_sql() -> str:
    """Build the MERGE that refreshes codesystem.sfi_search_document.

    Hash-diffed upsert: rows only update when the SHA2_256 of the assembled
    search_text changes, new concepts insert, and concepts that disappear
    from the sources soft-delete via is_active = 0 (NOT MATCHED BY SOURCE).
    """
    return """
        ;WITH props AS (
            SELECT
                [Code],
                MAX(CASE WHEN [PropertyCode] = N'source_model_name' THEN [Value] END) AS source_model_name,
                MAX(CASE WHEN [PropertyCode] = N'source_element_label' THEN [Value] END) AS source_element_label,
                MAX(CASE WHEN [PropertyCode] = N'sfi_path' THEN [Value] END) AS sfi_path,
                MAX(CASE WHEN [PropertyCode] = N'source_element_name' THEN [Value] END) AS source_element_name,
                MAX(CASE WHEN [PropertyCode] = N'source_group_path' THEN [Value] END) AS source_group_path,
                MAX(CASE WHEN [PropertyCode] = N'source_hierarchy_path' THEN [Value] END) AS source_hierarchy_path,
                MAX(CASE WHEN [PropertyCode] = N'klynge_ledetekst' THEN [Value] END) AS klynge_ledetekst,
                MAX(CASE WHEN [PropertyCode] = N'klynge_ledetekster' THEN [Value] END) AS klynge_ledetekster,
                MAX(CASE WHEN [PropertyCode] = N'klynge_navne' THEN [Value] END) AS klynge_navne
            FROM [codesystem].[SFIProperties]
            GROUP BY [Code]
        ),
        alias_text AS (
            SELECT
                m.[SfiCode] AS element_code,
                STRING_AGG(CONVERT(NVARCHAR(MAX), CONCAT_WS(N' ',
                    a.[AliasCode],
                    a.[AliasDisplay],
                    a.[AliasType],
                    a.[Description],
                    m.[MemberRole]
                )), N' ') AS alias_text
            FROM [codesystem].[SFIAliasMember] AS m
            INNER JOIN [codesystem].[SFIAlias] AS a
                ON a.[AliasCode] = m.[AliasCode]
            WHERE a.[ActiveFrom] <= CAST(GETDATE() AS DATE)
              AND (a.[ActiveTo] IS NULL OR a.[ActiveTo] >= CAST(GETDATE() AS DATE))
            GROUP BY m.[SfiCode]
        ),
        override_text AS (
            SELECT
                [element_code],
                STRING_AGG(CONVERT(NVARCHAR(MAX), CONCAT_WS(N' ',
                    [search_phrase],
                    [normalized_phrase],
                    [reason]
                )), N' ') AS override_text
            FROM [codesystem].[SFISearchOverride]
            WHERE [is_active] = 1
            GROUP BY [element_code]
        ),
        doc AS (
            SELECT
                c.[Code] AS element_code,
                c.[Display] AS element_display,
                p.[source_model_name],
                p.[source_element_label],
                p.[sfi_path],
                at.[alias_text],
                CONCAT_WS(N' ',
                    c.[Code],
                    c.[Display],
                    p.[source_model_name],
                    p.[source_element_label],
                    p.[sfi_path],
                    p.[source_element_name],
                    p.[source_group_path],
                    p.[source_hierarchy_path],
                    p.[klynge_ledetekst],
                    p.[klynge_ledetekster],
                    p.[klynge_navne],
                    at.[alias_text],
                    ot.[override_text]
                ) AS search_text
            FROM [codesystem].[SFIConcept] AS c
            LEFT JOIN props AS p
                ON p.[Code] = c.[Code]
            LEFT JOIN alias_text AS at
                ON at.[element_code] = c.[Code]
            LEFT JOIN override_text AS ot
                ON ot.[element_code] = c.[Code]
        ),
        src AS (
            SELECT
                [element_code],
                [element_display],
                [source_model_name],
                [source_element_label],
                [sfi_path],
                [alias_text],
                [search_text],
                HASHBYTES('SHA2_256', CONVERT(NVARCHAR(MAX), [search_text])) AS search_source_hash
            FROM doc
        )
        MERGE [codesystem].[SFISearchDocument] AS tgt
        USING src
            ON tgt.[element_code] = src.[element_code]
        WHEN MATCHED AND (
                tgt.[is_active] = 0
                OR tgt.[search_source_hash] IS NULL
                OR tgt.[search_source_hash] <> src.[search_source_hash]
            ) THEN UPDATE SET
                [element_display] = src.[element_display],
                [source_model_name] = src.[source_model_name],
                [source_element_label] = src.[source_element_label],
                [sfi_path] = src.[sfi_path],
                [alias_text] = src.[alias_text],
                [search_text] = src.[search_text],
                [search_source_hash] = src.[search_source_hash],
                [is_active] = 1,
                [DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                [element_code],
                [element_display],
                [source_model_name],
                [source_element_label],
                [sfi_path],
                [alias_text],
                [search_text],
                [search_source_hash],
                [is_active],
                [DW_Timestamp_Insert],
                [DW_Timestamp_Update]
            )
            VALUES (
                src.[element_code],
                src.[element_display],
                src.[source_model_name],
                src.[source_element_label],
                src.[sfi_path],
                src.[alias_text],
                src.[search_text],
                src.[search_source_hash],
                1,
                CAST(SYSDATETIME() AS datetime2(0)),
                CAST(SYSDATETIME() AS datetime2(0))
            )
        WHEN NOT MATCHED BY SOURCE AND tgt.[is_active] = 1 THEN
            UPDATE SET
                [is_active] = 0,
                [DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0));
    """


@dg.asset(
    name="dso_refresh_codesystem_sfi_search_document",
    group_name="codesystem_dso_sync",
    compute_kind="mssql",
    deps=[
        "dso_sync_codesystem_sfi_concept",
        "dso_sync_codesystem_sfi_properties",
        "dso_sync_codesystem_sfi_hierarchy",
    ],
    tags={"domain": "codesystem", "layer": "dso_sync", "type": "sfi_search_document"},
    description=(
        "Build metadata-driven SFI search documents from codesystem.sfi_* tables "
        "so user free text resolves to element_code before querying SFI_datapoint."
    ),
)
def dso_refresh_codesystem_sfi_search_document(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    refresh_sql = _build_sfi_search_document_refresh_sql()

    with mssql_target_dso.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(refresh_sql)
            cursor.execute(
                "SELECT COUNT(*) FROM [codesystem].[SFISearchDocument] WHERE [is_active] = 1"
            )
            row = cursor.fetchone()
            active_count = int(row[0]) if row else 0
            conn.commit()
        finally:
            cursor.close()

    context.log.info("Refreshed codesystem.sfi_search_document with %d active rows", active_count)
    return dg.MaterializeResult(
        metadata={
            "target_table": dg.MetadataValue.text("codesystem.sfi_search_document"),
            "active_count": dg.MetadataValue.int(active_count),
        }
    )


all_codesystem_dso_sync_assets = [
    dso_sync_codesystem_sfi_concept,
    dso_sync_codesystem_sfi_properties,
    dso_sync_codesystem_sfi_hierarchy,
    *_factory_codesystem_assets,
    dso_refresh_codesystem_sfi_search_document,
]

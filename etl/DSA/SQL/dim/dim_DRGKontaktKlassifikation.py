# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dimension Update: DRGKontaktKlassifikation.

The primary analysis dimension: one member per combination of the
classification attributes, aggregated from the staged contacts. Type 1 SCD.

The attribute values are constant within a member by construction (the
clean_code hashes exactly those values), so MAX() per group reads the value
without inventing an order. drg_beskrivelse, trimpunkt and standardtakst are
functionally dependent on (takstsystem, drg_kode) — measured in the source
profiling — and are denormalised on without adding cardinality; being
Type 1, a tariff revision overwrites them.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.staging.drg_kontakt_kolonner import KLASSIFIKATION_ATTRIBUTES

ATTRIBUTES: tuple[str, ...] = ("leverancetype", "takstsystem", *KLASSIFIKATION_ATTRIBUTES)
DENORMALISED: tuple[str, ...] = ("drg_beskrivelse", "trimpunkt", "standardtakst")

_ALL = ("clean_code_DRGKontaktKlassifikation", *ATTRIBUTES, *DENORMALISED)
_src_cols = ",\n        ".join(f"src.[{c}]" for c in _ALL)
_tgt_cols = ",\n        ".join(f"tgt.[{c}]" for c in _ALL)
_updates = ",\n        ".join(f"tgt.[{c}] = src.[{c}]" for c in _ALL)
_insert_cols = ",\n        ".join(f"[{c}]" for c in _ALL)
_max_attrs = ",\n        ".join(f"MAX(ds.[{c}]) AS [{c}]" for c in ATTRIBUTES)

UNMAPPED_SQL = """
SELECT COUNT(*)
FROM [stg].[dim_DRGKontaktEgenskaber] ds
LEFT JOIN [map].[DRGKontaktKlassifikation] m
    ON m.[clean_code_DRGKontaktKlassifikation] = ds.[clean_code_DRGKontaktKlassifikation]
WHERE m.[dw_ek_DRGKontaktKlassifikation] IS NULL;
"""

# standardtakst = the DRG tariff (pris_drg), functionally dependent on
# (takstsystem, drg_kode); read via the fact staging joined on the shared
# contact grain. beskrivelse comes from the live tariff catalogue.
RESOLVE_SQL = f"""
DROP TABLE IF EXISTS #DRGKontaktKlassifikation;

SELECT
    m.[dw_ek_DRGKontaktKlassifikation],
    grp.*
INTO #DRGKontaktKlassifikation
FROM (
    SELECT
        ds.[clean_code_DRGKontaktKlassifikation],
        {_max_attrs},
        MAX(ds.[trimpunkt]) AS [trimpunkt],
        MAX(fs.[pris_drg]) AS [standardtakst]
    FROM [stg].[dim_DRGKontaktEgenskaber] ds
    LEFT JOIN [stg].[fct_DRGKontakt] fs
        ON fs.[clean_code_DRGKontakt] = ds.[clean_code_DRGKontakt]
    GROUP BY ds.[clean_code_DRGKontaktKlassifikation]
) AS grp
INNER JOIN [map].[DRGKontaktKlassifikation] m
    ON m.[clean_code_DRGKontaktKlassifikation] = grp.[clean_code_DRGKontaktKlassifikation];

ALTER TABLE #DRGKontaktKlassifikation ADD [drg_beskrivelse] NVARCHAR(500) NULL;

UPDATE k
SET k.[drg_beskrivelse] = t.[BESKRIVELSE]
FROM #DRGKontaktKlassifikation k
INNER JOIN [arc].[drg_takstbeskrivelse] t
    ON t.[takstsystem] = k.[takstsystem]
   AND UPPER(LTRIM(RTRIM(t.[DRG]))) = UPPER(LTRIM(RTRIM(k.[drg_kode])))
   AND t.[dw_is_deleted] = 0;
"""

MERGE_SQL = f"""
MERGE [dim].[DRGKontaktKlassifikation] AS tgt
USING #DRGKontaktKlassifikation AS src
ON tgt.[dw_ek_DRGKontaktKlassifikation] = src.[dw_ek_DRGKontaktKlassifikation]

WHEN MATCHED AND EXISTS (
    SELECT
        {_src_cols}
    EXCEPT
    SELECT
        {_tgt_cols}
) THEN
    UPDATE SET
        {_updates},
        tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        [dw_ek_DRGKontaktKlassifikation],
        {_insert_cols},
        [dw_timestamp_insert],
        [dw_timestamp_update]
    )
    VALUES (
        src.[dw_ek_DRGKontaktKlassifikation],
        {_src_cols},
        CAST(SYSDATETIME() AS datetime2(0)),
        CAST(SYSDATETIME() AS datetime2(0))
    );
"""

INSERT_SQL = f"""
INSERT INTO [dim].[DRGKontaktKlassifikation] WITH (TABLOCK) (
    [dw_ek_DRGKontaktKlassifikation],
    {_insert_cols},
    [dw_timestamp_insert],
    [dw_timestamp_update]
)
SELECT
    src.[dw_ek_DRGKontaktKlassifikation],
    {_src_cols},
    CAST(SYSDATETIME() AS datetime2(0)),
    CAST(SYSDATETIME() AS datetime2(0))
FROM #DRGKontaktKlassifikation src;
"""

# drg_beskrivelse is the only denormalised field sourced from the catalogue
# rather than from contacts (trimpunkt/standardtakst come from the contacts,
# so they only change when contacts do — and those are in the delta anyway).
# A catalogue-only revision therefore reaches existing members only through
# this global refresh across ALL members, keyed on (takstsystem, drg_kode).
BESKRIVELSE_REFRESH_SQL = """
UPDATE k
SET k.[drg_beskrivelse] = t.[BESKRIVELSE],
    k.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
FROM [dim].[DRGKontaktKlassifikation] k
INNER JOIN [arc].[drg_takstbeskrivelse] t
    ON t.[takstsystem] = k.[takstsystem]
   AND UPPER(LTRIM(RTRIM(t.[DRG]))) = UPPER(LTRIM(RTRIM(k.[drg_kode])))
   AND t.[dw_is_deleted] = 0
WHERE k.[dw_ek_DRGKontaktKlassifikation] <> -1
  AND ISNULL(k.[drg_beskrivelse], N'') <> ISNULL(t.[BESKRIVELSE], N'');
"""


@asset(
    group_name="drg_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_DRGKontaktKlassifikation"]),
        AssetKey(["drg", "stg_dim_DRGKontaktEgenskaber"]),
        AssetKey(["drg", "stg_fct_DRGKontakt"]),
        AssetKey(["arc_drg_takstbeskrivelse"]),
    ],
    tags={"domain": "drg", "layer": "dimensions", "module": "datawarehouse", "scd": "type1"},
    description=(
        "Sync dim.DRGKontaktKlassifikation (primary analysis dimension) from "
        "the staged attribute combinations, tariff fields denormalised on"
    ),
)
def dim_DRGKontaktKlassifikation(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim", "DRGKontaktKlassifikation", dagster_run_id=context.run_id, mode=config.mode
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(UNMAPPED_SQL)
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"dim_DRGKontaktKlassifikation: {unmapped_rows} staging rows have no "
                        "match in map.DRGKontaktKlassifikation. Run the map asset first."
                    )

                cursor.execute(RESOLVE_SQL)

                cursor.execute(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM [dim].[DRGKontaktKlassifikation] "
                    "WHERE [dw_ek_DRGKontaktKlassifikation] <> -1) THEN 1 ELSE 0 END"
                )
                if cursor.fetchone()[0]:
                    context.log.info("Target has existing rows - MERGE upsert path")
                    cursor.execute(MERGE_SQL)
                else:
                    context.log.info("Target empty (only -1 sentinel) - bulk INSERT WITH (TABLOCK)")
                    cursor.execute(INSERT_SQL)

                rows_affected = cursor.rowcount

                # Catalogue-only description changes reach members whose
                # contacts were not in this delta batch.
                cursor.execute(BESKRIVELSE_REFRESH_SQL)
                beskrivelse_refresh = cursor.rowcount or 0
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #DRGKontaktKlassifikation")
                cursor.execute("UPDATE STATISTICS [dim].[DRGKontaktKlassifikation]")

                cursor.execute(
                    "SELECT COUNT(*), COUNT([drg_beskrivelse]), COUNT([standardtakst]) "
                    "FROM [dim].[DRGKontaktKlassifikation] "
                    "WHERE [dw_ek_DRGKontaktKlassifikation] <> -1"
                )
                total_rows, has_beskrivelse, has_takst = cursor.fetchone()
                context.log.info(
                    f"dim.DRGKontaktKlassifikation: {rows_affected} affected, "
                    f"{total_rows} members ({has_beskrivelse} with catalogue "
                    f"description, {has_takst} with standardtakst)"
                )
            finally:
                cursor.close()

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "DRGKontaktKlassifikation", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "DRGKontaktKlassifikation")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "total_members": total_rows,
            "members_with_beskrivelse": has_beskrivelse,
            "members_with_standardtakst": has_takst,
            "beskrivelse_refreshed": beskrivelse_refresh,
            "table": "dim.DRGKontaktKlassifikation",
            "source_tables": (
                "stg.dim_DRGKontaktEgenskaber + stg.fct_DRGKontakt + "
                "map.DRGKontaktKlassifikation + arc.drg_takstbeskrivelse"
            ),
            "scd_type": "Type 1 (overwrite)",
        }
    )

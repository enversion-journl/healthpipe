# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Lager

Populate dim.Lager from stg.dim_Lager with resolved surrogate keys.
Logic: Type 1 SCD - overwrite all attributes on each run.

Resolves:
- dw_ek_Lager from map.Lager
- dw_ek_Lager_parent from map.Lager (self-join on parent_code)
- dw_ek_Organisation from ref.Organisation (via code_sor -> sor_code)
- dw_ek_Rekvirent_lagerejer and dw_ek_Rekvirent_svind from map.Rekvirent
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.common.ils_clean_code_shared import sql_clean_code_rekvirent, sql_norm_code

# Warehouse attributes carried through stg.dim_Lager. The dbo.lgr source columns
# are recorded in data_lineage/dim/dim_lager.yml, not in the column names.
LAGER_ATTRIBUTE_COLUMNS: tuple[str, ...] = (
    "adresse",
    "land",
    "postnummer",
    "telefon",
    "lagerejer_rekvirentnummer",
    "svind_rekvirentnummer",
    "standard_leveringsdage",
    "aktiv",
)

# Attributes that are typed in dim.Lager but NVARCHAR in stg.dim_Lager (property
# values are text). TRY_CAST gives the temp-table column the target type and
# turns a malformed property value into NULL instead of failing the MERGE.
_TYPED_ATTRIBUTE_SELECTS: dict[str, str] = {
    "standard_leveringsdage": (
        "TRY_CAST(stg.[standard_leveringsdage] AS INT) AS [standard_leveringsdage]"
    ),
    "aktiv": "TRY_CAST(stg.[aktiv] AS BIT) AS [aktiv]",
}

# Everything MERGE writes apart from the keys.
_MERGE_COLUMNS: tuple[str, ...] = (
    "code",
    "display",
    "parent_code",
    "level",
    "entity_type",
    "sgh",
    "lgr",
    *LAGER_ATTRIBUTE_COLUMNS,
    "code_sor",
    "dw_ek_Rekvirent_lagerejer",
    "dw_ek_Rekvirent_svind",
    "dw_ek_Organisation",
    "dw_ek_Lager_parent",
)


def build_resolve_sql() -> str:
    """SELECT ... INTO #Lager_Resolved with resolved DW_EK keys.

    Per Strategisk Indkoeb, lgrejer is "a requisitioner used for
    goods movements into the warehouse" and rekvsvind "a requisitioner used to
    record shrinkage", so both are resolved into real entity keys against
    map.Rekvirent rather than left as loose text. The requisitioner key is
    (sgh, rnr) and the warehouse carries sgh itself — the assumption is therefore
    that the requisitioner belongs to the warehouse's own hospital. Where that
    does not hold for a row it lands on -1 (unknown member), never on a wrong
    requisitioner.

    The guard in each JOIN is load-bearing: sql_clean_code_rekvirent falls back to
    the hospital-level member SGH_{sgh} when the child segment is missing, so
    without it every warehouse WITHOUT the requisitioner stated would quietly
    point at its hospital's requisitioner instead of at -1.
    """
    attribute_select = "".join(
        f"                    {_TYPED_ATTRIBUTE_SELECTS.get(c, f'stg.[{c}]')},\n"
        for c in LAGER_ATTRIBUTE_COLUMNS
    )
    lagerejer_code = sql_clean_code_rekvirent("stg.[sgh]", "stg.[lagerejer_rekvirentnummer]")
    svind_code = sql_clean_code_rekvirent("stg.[sgh]", "stg.[svind_rekvirentnummer]")
    # The same normalisation the clean code itself uses: an empty string would
    # otherwise slip past a bare IS NOT NULL and land in the hospital fallback.
    lagerejer_stated = f"{sql_norm_code('stg.[lagerejer_rekvirentnummer]')} IS NOT NULL"
    svind_stated = f"{sql_norm_code('stg.[svind_rekvirentnummer]')} IS NOT NULL"

    return f"""
                DROP TABLE IF EXISTS #Lager_Resolved;

                SELECT
                    m.dw_ek_Lager,
                    stg.code,
                    stg.display,
                    stg.parent_code,
                    stg.[level],
                    stg.entity_type,
                    stg.sgh,
                    stg.lgr,
{attribute_select}                    stg.code_sor,
                    COALESCE(m_rekv.dw_ek_Rekvirent, -1) AS dw_ek_Rekvirent_lagerejer,
                    COALESCE(m_svind.dw_ek_Rekvirent, -1) AS dw_ek_Rekvirent_svind,
                    COALESCE(m_parent.dw_ek_Lager, -1) AS dw_ek_Lager_parent,
                    COALESCE(m_org.dw_ek_Organisation, -1) AS dw_ek_Organisation
                INTO #Lager_Resolved
                FROM [stg].[dim_Lager] stg
                LEFT JOIN [map].[Lager] m
                    ON m.clean_code_Lager = stg.clean_code_Lager
                LEFT JOIN [map].[Lager] m_parent
                    ON m_parent.clean_code_Lager = stg.parent_code
                LEFT JOIN [ref].[Organisation] m_org
                    ON m_org.sor_code = stg.code_sor
                    AND m_org.dw_is_deleted = 0
                    AND stg.code_sor IS NOT NULL
                LEFT JOIN [map].[Rekvirent] m_rekv
                    ON m_rekv.clean_code_Rekvirent = {lagerejer_code}
                   AND {lagerejer_stated}
                LEFT JOIN [map].[Rekvirent] m_svind
                    ON m_svind.clean_code_Rekvirent = {svind_code}
                   AND {svind_stated}
"""


def _merge_sql() -> str:
    update_set = "".join(
        f"                        [{c}] = source.[{c}],\n" for c in _MERGE_COLUMNS if c != "level"
    )
    insert_cols = ",\n".join(f"                        [{c}]" for c in _MERGE_COLUMNS)
    insert_vals = ",\n".join(f"                        source.[{c}]" for c in _MERGE_COLUMNS)

    return f"""
                MERGE INTO [dim].[Lager] AS target
                USING #Lager_Resolved AS source
                ON target.dw_ek_Lager = source.dw_ek_Lager
                WHEN MATCHED THEN
                    UPDATE SET
                        [level] = source.[level],
{update_set}                        dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        [dw_ek_Lager],
{insert_cols},
                        [dw_timestamp_insert], [dw_timestamp_update]
                    )
                    VALUES (
                        source.[dw_ek_Lager],
{insert_vals},
                        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                    )
                OUTPUT $action;
"""


MERGE_SQL = _merge_sql()


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["map_Lager"]),
        AssetKey(["stg_dim_Lager"]),
        # lgrejer and rekvsvind are resolved against map.Rekvirent
        AssetKey(["map_Rekvirent"]),
    ],
    tags={"domain": "erp", "layer": "dimensions", "module": "datawarehouse", "scd": "type1"},
    description="Populate dim.Lager (Type 1 SCD) with resolved DW_EK keys",
)
def dim_Lager(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension table maintenance: resolve EKs and MERGE into dim.Lager."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("dim", "Lager", dagster_run_id=context.run_id, mode=config.mode)

    inserted = 0
    updated = 0
    total = 0
    has_org = 0
    has_parent = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Building temp table with resolved EKs...")

                cursor.execute(
                    """
                SELECT COUNT(*)
                FROM [stg].[dim_Lager] stg
                LEFT JOIN [map].[Lager] m
                    ON m.clean_code_Lager = stg.clean_code_Lager
                WHERE m.dw_ek_Lager IS NULL
            """
                )
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"dim_Lager: {unmapped_rows} staging rows have no match in map.Lager. "
                        "Ensure map_Lager runs before dim_Lager and staging CleanCodes are valid."
                    )

                cursor.execute(build_resolve_sql())

                cursor.execute("SELECT COUNT(*) FROM #Lager_Resolved")
                resolved_count = cursor.fetchone()[0]
                context.log.info(f"Resolved {resolved_count} rows for dim.Lager")

                cursor.execute(MERGE_SQL)

                merge_results = cursor.fetchall()
                inserted = sum(1 for r in merge_results if r[0] == "INSERT")
                updated = sum(1 for r in merge_results if r[0] == "UPDATE")

                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #Lager_Resolved")

                cursor.execute("UPDATE STATISTICS [dim].[Lager]")

                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN dw_ek_Organisation <> -1 THEN 1 ELSE 0 END) AS has_org,
                    SUM(CASE WHEN dw_ek_Lager_parent <> -1 THEN 1 ELSE 0 END) AS has_parent
                FROM [dim].[Lager]
                WHERE dw_ek_Lager <> -1
            """
                )
                stats = cursor.fetchone()
                total = stats[0]
                has_org = stats[1]
                has_parent = stats[2]
            finally:
                cursor.close()

        context.log.info(
            f"dim.Lager: {inserted} inserted, {updated} updated, {total} total "
            f"({has_org} with Organization FK, {has_parent} with Parent FK)"
        )

        rows_affected = inserted + updated
        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "Lager", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "Lager")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "inserted": inserted,
            "updated": updated,
            "total": total,
            "has_organization_fk": has_org,
            "has_parent_fk": has_parent,
            "table": "dim.Lager",
            "scd_type": "Type 1",
            "mode": config.mode,
        }
    )

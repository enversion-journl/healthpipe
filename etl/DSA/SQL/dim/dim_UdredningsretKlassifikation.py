# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dimension Update: UdredningsretKlassifikation.

The primary analysis dimension: one member per combination of the eight
indicator attributes (2,948 measured on dev), aggregated from the staged forloeb.
Type 1 SCD.

The attribute values are constant within a member by construction (the
clean_code hashes exactly those eight values), so MAX() per group reads the
value without inventing an order — the same idiom as DRG's classification.
Only two of the eight live on stg.dim_UdredningsretEgenskaber, so the raw
values are read from stg.pre_Udredningsforloeb joined on the forloeb
clean_code the two tables share (decision U12): the transaction dimension
stays free of indicator duplicates and US-3 staging is untouched.

Every attribute except speciale (free text from the delivery, which explains
itself) carries a rendered Danish text beside its raw value, so a report
reads the text and a check reads the code. The texts come from the dicts in
udredningsret_kolonner.py via tekst_case_sql — never hand-written here —
except forloebslabel_tekst, which is codesystem.SKSConcept.display and is
refreshed globally after the load (as DRG's drg_beskrivelse).

GDPR: attribute combinations and counts only — no borger key, no CPR.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.staging.udredningsret_kolonner import (
    AARSAG_TEKSTER,
    HJERTE_TEKSTER,
    INDEN_30_TEKSTER,
    KLASSIFIKATION_ATTRIBUTTER,
    KLASSIFIKATION_TEKST_KOLONNER,
    OVERHOLDT_TEKSTER,
    PSYKIATRI_TEKSTER,
    REGION_TEKSTER,
    REGION_UKENDT_TEKST,
    UKENDT_TEKST,
    tekst_case_sql,
)

ATTRIBUTTER: tuple[str, ...] = KLASSIFIKATION_ATTRIBUTTER

# DDL order: every attribute, each text column right after the raw value it
# explains. Derived here so the dimension SQL cannot drift from the
# attribute list; a unit test holds the static DDL to the same tuple.
KOLONNER: tuple[str, ...] = tuple(
    kolonne
    for attribut in ATTRIBUTTER
    for kolonne in (
        (attribut, f"{attribut}_tekst")
        if attribut in KLASSIFIKATION_TEKST_KOLONNER
        else (attribut,)
    )
)

_UKENDT_SQL = f"N'{UKENDT_TEKST}'"

# C_BOREGION_F: Statistics Denmark's region codes, blank -> 'Ukendt'.
# Any other non-blank code falls through to the ELSE branch below, which
# repeats the code itself rather than hiding an unexpected region.
BOPAELSREGION_TEKSTER: dict[str, str] = {**REGION_TEKSTER, "": REGION_UKENDT_TEKST}

# attribute -> (text dict, ELSE branch as SQL, target type from the DDL).
# forloebslabel is absent on purpose: its text is a catalogue lookup, not a
# dict (KATALOG_TEKST_ATTRIBUT below).
TEKST_REGLER: dict[str, tuple[dict[str, str], str, str]] = {
    "overholdt": (OVERHOLDT_TEKSTER, _UKENDT_SQL, "NVARCHAR(50)"),
    "aarsag": (AARSAG_TEKSTER, _UKENDT_SQL, "NVARCHAR(200)"),
    "inden_30_dage": (INDEN_30_TEKSTER, _UKENDT_SQL, "NVARCHAR(50)"),
    "psykiatri": (PSYKIATRI_TEKSTER, _UKENDT_SQL, "NVARCHAR(50)"),
    "hjerteomraade": (HJERTE_TEKSTER, _UKENDT_SQL, "NVARCHAR(50)"),
    "bopaelsregion_forloeb": (
        BOPAELSREGION_TEKSTER,
        "grp.[bopaelsregion_forloeb]",
        "NVARCHAR(50)",
    ),
}
KATALOG_TEKST_ATTRIBUT = "forloebslabel"


def _resolve_udtryk(kolonne: str) -> str:
    """SELECT expression for one dimension column in the resolve set."""
    if kolonne in ATTRIBUTTER:
        return f"grp.[{kolonne}]"
    attribut = kolonne[: -len("_tekst")]
    if attribut == KATALOG_TEKST_ATTRIBUT:
        return f"s.[display] AS [{kolonne}]"
    tekster, default_sql, sql_type = TEKST_REGLER[attribut]
    case_sql = tekst_case_sql(f"grp.[{attribut}]", tekster, default_sql)
    return f"CAST({case_sql} AS {sql_type}) AS [{kolonne}]"


_ALL = ("clean_code_UdredningsretKlassifikation", *KOLONNER)
_src_cols = ",\n        ".join(f"src.[{c}]" for c in _ALL)
_tgt_cols = ",\n        ".join(f"tgt.[{c}]" for c in _ALL)
_updates = ",\n        ".join(f"tgt.[{c}] = src.[{c}]" for c in _ALL)
_insert_cols = ",\n        ".join(f"[{c}]" for c in _ALL)
_max_attrs = ",\n        ".join(f"MAX(p.[{a}]) AS [{a}]" for a in ATTRIBUTTER)
_resolve_cols = ",\n    ".join(_resolve_udtryk(c) for c in KOLONNER)

UNMAPPED_SQL = """
SELECT COUNT(*)
FROM [stg].[dim_UdredningsretEgenskaber] ds
LEFT JOIN [map].[UdredningsretKlassifikation] m
    ON m.[clean_code_UdredningsretKlassifikation] = ds.[clean_code_UdredningsretKlassifikation]
WHERE m.[dw_ek_UdredningsretKlassifikation] IS NULL;
"""

# Six of the eight attributes only exist on stg.pre_Udredningsforloeb, so the
# member is grouped on the STAGED classification clean_code while the values
# are read from pre joined on the forloeb clean_code the two tables share
# (one row each, same grain). forloebslabel_tekst is the SKS display text for
# the ALAL codes; no catalogue row leaves it NULL until the global refresh
# below finds one.
RESOLVE_SQL = f"""
DROP TABLE IF EXISTS #UdredningsretKlassifikation;

SELECT
    m.[dw_ek_UdredningsretKlassifikation],
    grp.[clean_code_UdredningsretKlassifikation],
    {_resolve_cols}
INTO #UdredningsretKlassifikation
FROM (
    SELECT
        ds.[clean_code_UdredningsretKlassifikation],
        {_max_attrs}
    FROM [stg].[dim_UdredningsretEgenskaber] ds
    INNER JOIN [stg].[pre_Udredningsforloeb] p
        ON p.[clean_code_Udredningsforloeb] = ds.[clean_code_Udredningsforloeb]
    GROUP BY ds.[clean_code_UdredningsretKlassifikation]
) AS grp
INNER JOIN [map].[UdredningsretKlassifikation] m
    ON m.[clean_code_UdredningsretKlassifikation] = grp.[clean_code_UdredningsretKlassifikation]
LEFT JOIN [codesystem].[SKSConcept] s
    ON s.[code] = UPPER(LTRIM(RTRIM(grp.[forloebslabel])));

-- The MERGE below matches on this key against a target whose clustered PK is
-- the same column; without the key here the resolve set is sorted or hashed on
-- every run. Clustered but NOT unique on purpose, same reasoning as the fact's
-- temp table: a duplicated resolve row must reach the MERGE, not die as a
-- failed index build.
CREATE CLUSTERED INDEX [IX_UdredningsretKlassifikation]
    ON #UdredningsretKlassifikation([dw_ek_UdredningsretKlassifikation]);
"""

MERGE_SQL = f"""
MERGE [dim].[UdredningsretKlassifikation] AS tgt
USING #UdredningsretKlassifikation AS src
ON tgt.[dw_ek_UdredningsretKlassifikation] = src.[dw_ek_UdredningsretKlassifikation]

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
        [dw_ek_UdredningsretKlassifikation],
        {_insert_cols},
        [dw_timestamp_insert],
        [dw_timestamp_update]
    )
    VALUES (
        src.[dw_ek_UdredningsretKlassifikation],
        {_src_cols},
        CAST(SYSDATETIME() AS datetime2(0)),
        CAST(SYSDATETIME() AS datetime2(0))
    );
"""

INSERT_SQL = f"""
INSERT INTO [dim].[UdredningsretKlassifikation] WITH (TABLOCK) (
    [dw_ek_UdredningsretKlassifikation],
    {_insert_cols},
    [dw_timestamp_insert],
    [dw_timestamp_update]
)
SELECT
    src.[dw_ek_UdredningsretKlassifikation],
    {_src_cols},
    CAST(SYSDATETIME() AS datetime2(0)),
    CAST(SYSDATETIME() AS datetime2(0))
FROM #UdredningsretKlassifikation src;
"""

# forloebslabel_tekst is the only field sourced from the catalogue rather
# than from the forloeb, so a catalogue-only revision reaches members whose
# forloeb were not in this delta batch only through this global refresh
# across ALL members. Idempotent: it touches a row only when the text
# actually differs, and never the -1 sentinel.
FORLOEBSLABEL_REFRESH_SQL = """
UPDATE k
SET k.[forloebslabel_tekst] = s.[display],
    k.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
FROM [dim].[UdredningsretKlassifikation] k
INNER JOIN [codesystem].[SKSConcept] s
    ON s.[code] = UPPER(LTRIM(RTRIM(k.[forloebslabel])))
WHERE k.[dw_ek_UdredningsretKlassifikation] <> -1
  AND ISNULL(k.[forloebslabel_tekst], N'') <> ISNULL(s.[display], N'');
"""


@asset(
    group_name="udredningsret_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_UdredningsretKlassifikation"]),
        AssetKey(["udredningsret", "stg_dim_UdredningsretEgenskaber"]),
        AssetKey(["udredningsret", "pre_stg_Udredningsforloeb"]),
        AssetKey(["cs_sks"]),
    ],
    tags={
        "domain": "udredningsret",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description=(
        "Sync dim.UdredningsretKlassifikation (primary analysis dimension) "
        "from the staged indicator-attribute combinations, Danish texts "
        "rendered beside every raw value"
    ),
)
def dim_UdredningsretKlassifikation(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim", "UdredningsretKlassifikation", dagster_run_id=context.run_id, mode=config.mode
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(UNMAPPED_SQL)
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"dim_UdredningsretKlassifikation: {unmapped_rows} staging rows have "
                        "no match in map.UdredningsretKlassifikation. Run "
                        "map_UdredningsretKlassifikation first."
                    )

                cursor.execute(RESOLVE_SQL)

                # Short-circuit: a target holding only the -1 sentinel takes a
                # minimally logged bulk INSERT; map keys are IDENTITY >= 1 and
                # never collide with the sentinel.
                cursor.execute(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM [dim].[UdredningsretKlassifikation] "
                    "WHERE [dw_ek_UdredningsretKlassifikation] <> -1) THEN 1 ELSE 0 END"
                )
                if cursor.fetchone()[0]:
                    context.log.info("Target has existing rows - MERGE upsert path")
                    cursor.execute(MERGE_SQL)
                else:
                    context.log.info("Target empty (only -1 sentinel) - bulk INSERT WITH (TABLOCK)")
                    cursor.execute(INSERT_SQL)

                rows_affected = cursor.rowcount

                # Catalogue-only label changes reach members whose forloeb
                # were not in this delta batch.
                cursor.execute(FORLOEBSLABEL_REFRESH_SQL)
                forloebslabel_refresh = cursor.rowcount or 0
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #UdredningsretKlassifikation")
                cursor.execute("UPDATE STATISTICS [dim].[UdredningsretKlassifikation]")
                # UPDATE STATISTICS is transactional in SQL Server and the
                # connection is not autocommit, so without this commit the
                # refreshed statistics are rolled back when it closes.
                conn.commit()

                cursor.execute(
                    "SELECT COUNT(*), COUNT([forloebslabel_tekst]) "
                    "FROM [dim].[UdredningsretKlassifikation] "
                    "WHERE [dw_ek_UdredningsretKlassifikation] <> -1"
                )
                total_rows, har_forloebslabel_tekst = cursor.fetchone()
                context.log.info(
                    f"dim.UdredningsretKlassifikation: {rows_affected} affected, "
                    f"{total_rows} members ({har_forloebslabel_tekst} with a "
                    f"catalogue label text, {forloebslabel_refresh} refreshed)"
                )
            finally:
                cursor.close()

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "UdredningsretKlassifikation", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "UdredningsretKlassifikation")
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
            "members_with_forloebslabel_tekst": har_forloebslabel_tekst,
            "forloebslabel_refreshed": forloebslabel_refresh,
            "table": "dim.UdredningsretKlassifikation",
            "source_tables": (
                "stg.dim_UdredningsretEgenskaber + stg.pre_Udredningsforloeb + "
                "map.UdredningsretKlassifikation + codesystem.SKSConcept"
            ),
            "scd_type": "Type 1 (overwrite)",
            "mode": config.mode,
        }
    )

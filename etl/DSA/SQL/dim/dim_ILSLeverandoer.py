# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: ILSLeverandoer

Type 1 SCD maintenance for dim.ILSLeverandoer — one row per ILS supplier
number (dbo.lev.lnr), dw_ek_ILSLeverandoer from map.ILSLeverandoer.

dim.Kreditor is an outrigger of this dimension. dw_ek_Kreditor takes the first
staged candidate CVR that map.Kreditor holds — se_nr, then cvr_nr, then lnr —
and is -1 when none matches, always for giro = 'Person' rows.
kreditor_match_kilde names the code that matched.

No soft delete: arc.ils_lev keeps rows the source removes (detect_deletes=False),
so aktiv is the status as last seen in ILS.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, Failure, MaterializeResult, asset
from DSA.SQL.common.dato_dimension import dato_key

# Outrigger cascade to dim.Kreditor, in priority order:
# (staging clean code, map.Kreditor alias, kreditor_match_kilde value).
_KREDITOR_MATCH = (
    ("clean_code_Kreditor_se_nr", "mk_se", "se_nr"),
    ("clean_code_Kreditor_cvr_nr", "mk_cvr", "cvr_nr"),
    ("clean_code_Kreditor_lnr", "mk_lnr", "lnr"),
)

_KREDITOR_EK = (
    "COALESCE(" + ", ".join(f"{alias}.dw_ek_Kreditor" for _, alias, _ in _KREDITOR_MATCH) + ", -1)"
)
_KREDITOR_MATCH_KILDE = (
    "CASE "
    + " ".join(
        f"WHEN {alias}.dw_ek_Kreditor IS NOT NULL THEN '{kilde}'"
        for _, alias, kilde in _KREDITOR_MATCH
    )
    + " END"
)
_KREDITOR_JOINS = "\n".join(
    f"""    LEFT JOIN [map].[Kreditor] {alias}
        ON {alias}.clean_code_Kreditor = s.{clean_code}"""
    for clean_code, alias, _ in _KREDITOR_MATCH
)

SOURCE_SQL = f"""
    DROP TABLE IF EXISTS #ILSLeverandoer_Source;

    SELECT
        m.dw_ek_ILSLeverandoer,
        s.clean_code_ILSLeverandoer,
        s.leverandoernummer,
        {_KREDITOR_EK} AS dw_ek_Kreditor,
        {_KREDITOR_MATCH_KILDE} AS kreditor_match_kilde,
        s.leverandoernavn,
        s.adresse,
        s.adresse2,
        s.land,
        s.postnummer,
        s.giro,
        s.aktiv,
        s.standard_leveringsdage,
        {dato_key('s.dato_tid_oprettet')} AS dw_ek_Dato_oprettet,
        s.dato_tid_oprettet,
        s.opr_bruger,
        {dato_key('s.dato_tid_ajourfoert')} AS dw_ek_Dato_ajourfoert,
        s.dato_tid_ajourfoert,
        s.ajourfoert_bruger
    INTO #ILSLeverandoer_Source
    FROM [stg].[dim_ILSLeverandoer] s
    INNER JOIN [map].[ILSLeverandoer] m
        ON m.clean_code_ILSLeverandoer = s.clean_code_ILSLeverandoer
{_KREDITOR_JOINS};

    CREATE UNIQUE CLUSTERED INDEX IX_ILSLeverandoer_Source_EK
    ON #ILSLeverandoer_Source(dw_ek_ILSLeverandoer);
"""

UNMAPPED_SQL = """
    SELECT s.clean_code_ILSLeverandoer
    FROM [stg].[dim_ILSLeverandoer] s
    LEFT JOIN [map].[ILSLeverandoer] m
        ON m.clean_code_ILSLeverandoer = s.clean_code_ILSLeverandoer
    WHERE m.dw_ek_ILSLeverandoer IS NULL;
"""

# Attributes that constitute a change; dw_timestamp_update is the DSO sync watermark.
_CHANGE_CHECK = """
                   ISNULL(tgt.clean_code_ILSLeverandoer, '') <> ISNULL(src.clean_code_ILSLeverandoer, '')
                OR ISNULL(tgt.leverandoernummer, '') <> ISNULL(src.leverandoernummer, '')
                OR ISNULL(tgt.dw_ek_Kreditor, -1) <> ISNULL(src.dw_ek_Kreditor, -1)
                OR ISNULL(tgt.kreditor_match_kilde, '') <> ISNULL(src.kreditor_match_kilde, '')
                OR ISNULL(tgt.leverandoernavn, '') <> ISNULL(src.leverandoernavn, '')
                OR ISNULL(tgt.adresse, '') <> ISNULL(src.adresse, '')
                OR ISNULL(tgt.adresse2, '') <> ISNULL(src.adresse2, '')
                OR ISNULL(tgt.land, '') <> ISNULL(src.land, '')
                OR ISNULL(tgt.postnummer, '') <> ISNULL(src.postnummer, '')
                OR ISNULL(tgt.giro, '') <> ISNULL(src.giro, '')
                OR ISNULL(CAST(tgt.aktiv AS INT), -1) <> ISNULL(CAST(src.aktiv AS INT), -1)
                OR ISNULL(tgt.standard_leveringsdage, -1) <> ISNULL(src.standard_leveringsdage, -1)
                OR ISNULL(tgt.dw_ek_Dato_oprettet, -1) <> ISNULL(src.dw_ek_Dato_oprettet, -1)
                OR ISNULL(tgt.dato_tid_oprettet, '1900-01-01') <>
                   ISNULL(src.dato_tid_oprettet, '1900-01-01')
                OR ISNULL(tgt.opr_bruger, '') <> ISNULL(src.opr_bruger, '')
                OR ISNULL(tgt.dw_ek_Dato_ajourfoert, -1) <> ISNULL(src.dw_ek_Dato_ajourfoert, -1)
                OR ISNULL(tgt.dato_tid_ajourfoert, '1900-01-01') <>
                   ISNULL(src.dato_tid_ajourfoert, '1900-01-01')
                OR ISNULL(tgt.ajourfoert_bruger, '') <> ISNULL(src.ajourfoert_bruger, '')
"""

MERGE_SQL = f"""
    MERGE [dim].[ILSLeverandoer] AS tgt
    USING #ILSLeverandoer_Source AS src
        ON tgt.dw_ek_ILSLeverandoer = src.dw_ek_ILSLeverandoer

    WHEN MATCHED AND (
        {_CHANGE_CHECK.strip()}
    ) THEN
        UPDATE SET
            tgt.clean_code_ILSLeverandoer = src.clean_code_ILSLeverandoer,
            tgt.leverandoernummer = src.leverandoernummer,
            tgt.dw_ek_Kreditor = src.dw_ek_Kreditor,
            tgt.kreditor_match_kilde = src.kreditor_match_kilde,
            tgt.leverandoernavn = src.leverandoernavn,
            tgt.adresse = src.adresse,
            tgt.adresse2 = src.adresse2,
            tgt.land = src.land,
            tgt.postnummer = src.postnummer,
            tgt.giro = src.giro,
            tgt.aktiv = src.aktiv,
            tgt.standard_leveringsdage = src.standard_leveringsdage,
            tgt.dw_ek_Dato_oprettet = src.dw_ek_Dato_oprettet,
            tgt.dato_tid_oprettet = src.dato_tid_oprettet,
            tgt.opr_bruger = src.opr_bruger,
            tgt.dw_ek_Dato_ajourfoert = src.dw_ek_Dato_ajourfoert,
            tgt.dato_tid_ajourfoert = src.dato_tid_ajourfoert,
            tgt.ajourfoert_bruger = src.ajourfoert_bruger,
            tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            dw_ek_ILSLeverandoer,
            clean_code_ILSLeverandoer,
            leverandoernummer,
            dw_ek_Kreditor,
            kreditor_match_kilde,
            leverandoernavn,
            adresse,
            adresse2,
            land,
            postnummer,
            giro,
            aktiv,
            standard_leveringsdage,
            dw_ek_Dato_oprettet,
            dato_tid_oprettet,
            opr_bruger,
            dw_ek_Dato_ajourfoert,
            dato_tid_ajourfoert,
            ajourfoert_bruger,
            dw_timestamp_insert,
            dw_timestamp_update
        )
        VALUES (
            src.dw_ek_ILSLeverandoer,
            src.clean_code_ILSLeverandoer,
            src.leverandoernummer,
            src.dw_ek_Kreditor,
            src.kreditor_match_kilde,
            src.leverandoernavn,
            src.adresse,
            src.adresse2,
            src.land,
            src.postnummer,
            src.giro,
            src.aktiv,
            src.standard_leveringsdage,
            src.dw_ek_Dato_oprettet,
            src.dato_tid_oprettet,
            src.opr_bruger,
            src.dw_ek_Dato_ajourfoert,
            src.dato_tid_ajourfoert,
            src.ajourfoert_bruger,
            CAST(SYSDATETIME() AS datetime2(0)),
            CAST(SYSDATETIME() AS datetime2(0))
        )
    OUTPUT $action;
"""


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_ILSLeverandoer"]),
        AssetKey(["map_Kreditor"]),
        AssetKey(["erp", "stg_dim_ILSLeverandoer"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="Type 1 SCD sync for dim.ILSLeverandoer using map.ILSLeverandoer + stg.dim_ILSLeverandoer",
)
def dim_ILSLeverandoer(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension table maintenance: Type 1 upsert for ILS suppliers."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim",
        "ILSLeverandoer",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    inserted = 0
    updated = 0
    uden_kreditor = 0
    match_counts: dict[str, int] = {}
    total_rows = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting ILSLeverandoer Type 1 sync...")

                # map_ILSLeverandoer runs before this asset and mints every staged
                # code, so a staged row without a map row is a data-integrity error.
                cursor.execute(UNMAPPED_SQL)
                unmapped = [row[0] for row in cursor.fetchall()]
                if unmapped:
                    raise Failure(
                        description=(
                            f"{len(unmapped)} stg.dim_ILSLeverandoer row(s) have no "
                            f"dw_ek_ILSLeverandoer in map.ILSLeverandoer (sample: {unmapped[:5]})"
                        )
                    )

                cursor.execute(SOURCE_SQL)
                cursor.execute(MERGE_SQL)
                actions = cursor.fetchall()
                inserted = sum(1 for row in actions if row[0] == "INSERT")
                updated = sum(1 for row in actions if row[0] == "UPDATE")

                cursor.execute("DROP TABLE IF EXISTS #ILSLeverandoer_Source;")

                conn.commit()

                cursor.execute("UPDATE STATISTICS [dim].[ILSLeverandoer]")

                cursor.execute(
                    """
                    SELECT
                        COUNT(*),
                        COALESCE(SUM(CASE WHEN dw_ek_Kreditor = -1 THEN 1 ELSE 0 END), 0)
                    FROM [dim].[ILSLeverandoer]
                    WHERE dw_ek_ILSLeverandoer <> -1
                """
                )
                total_rows, uden_kreditor = (v or 0 for v in cursor.fetchone())

                cursor.execute(
                    """
                    SELECT kreditor_match_kilde, COUNT(*)
                    FROM [dim].[ILSLeverandoer]
                    WHERE kreditor_match_kilde IS NOT NULL
                    GROUP BY kreditor_match_kilde
                """
                )
                match_counts = {kilde: count for kilde, count in cursor.fetchall()}

                context.log.info(
                    "ILSLeverandoer Type 1 sync complete: "
                    f"inserted={inserted}, updated={updated}, total={total_rows}, "
                    f"uden dim.Kreditor={uden_kreditor}, matches={match_counts}"
                )
            finally:
                cursor.close()

        rows_affected = inserted + updated
        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "ILSLeverandoer", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "ILSLeverandoer")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "dim.ILSLeverandoer",
            "inserted": inserted,
            "updated": updated,
            "total_rows": total_rows,
            "uden_kreditor": uden_kreditor,
            **{
                f"kreditor_match_{kilde}": match_counts.get(kilde, 0)
                for _, _, kilde in _KREDITOR_MATCH
            },
            "scd_type": "Type 1",
            "grain": "one row per dw_ek_ILSLeverandoer (lnr)",
            "source_table": "stg.dim_ILSLeverandoer",
            "mode": config.mode,
        }
    )

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: UNSPSC (Type 1)

Business logic identical to DimUpd_UNSPSC.py but uses DfmManager for checkpoint
management and run logging.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource

BUILD_SQL = """
DROP TABLE IF EXISTS #DeltaToMerge;

SELECT
    m.dw_ek_UNSPSC,
    m.clean_code_UNSPSC                                                 AS unspsc_kode,
    -- Source flags come from map.UNSPSC, NOT from stg.dim_UNSPSC. The stg delta
    -- only carries the sources seen in the CURRENT delta, so it can show a code as
    -- belonging to a single source even though an earlier run already observed it in
    -- another source. map.UNSPSC holds the sticky, cumulative truth across all runs.
    m.dw_source_ils                                                    AS dw_source_ils,
    m.dw_source_kmd                                                    AS dw_source_kmd,
    m.dw_source_udbud                                                  AS dw_source_udbud,
    LTRIM(RTRIM(REPLACE(u.[betegn],                       CHAR(160), ''))) AS beskrivelse_ils,
    LTRIM(RTRIM(REPLACE(u.[un_name],                      CHAR(160), ''))) AS beskrivelse_engelsk_ils,
    LTRIM(RTRIM(REPLACE(k.[UNSPSCClassDescription],       CHAR(160), ''))) AS beskrivelse_kmd,
    k.[UNSPSCLevel]                                                    AS unspsc_level_kmd,
    k.[UNSPSCClassLev1ID]                                              AS unspsc_level_1_kode_kmd,
    LTRIM(RTRIM(REPLACE(k.[UNSPSCClassLev1Description],   CHAR(160), ''))) AS unspsc_level_1_beskrivelse_kmd,
    k.[UNSPSCClassLev2ID]                                              AS unspsc_level_2_kode_kmd,
    LTRIM(RTRIM(REPLACE(k.[UNSPSCClassLev2Description],   CHAR(160), ''))) AS unspsc_level_2_beskrivelse_kmd,
    k.[UNSPSCClassLev3ID]                                              AS unspsc_level_3_kode_kmd,
    LTRIM(RTRIM(REPLACE(k.[UNSPSCClassLev3Description],   CHAR(160), ''))) AS unspsc_level_3_beskrivelse_kmd,
    k.[UNSPSCClassLev4ID]                                              AS unspsc_level_4_kode_kmd,
    LTRIM(RTRIM(REPLACE(k.[UNSPSCClassLev4Description],   CHAR(160), ''))) AS unspsc_level_4_beskrivelse_kmd,
    ud.[beskrivelse_udbud]                                            AS beskrivelse_udbud,
    ud.[udbudstype]                                                   AS udbudstype,
    ud.[udbudskategori_niveau_1]                                      AS udbudskategori_niveau_1,
    ud.[udbudskategori_niveau_2]                                      AS udbudskategori_niveau_2,
    ud.[udbudskategori_niveau_3]                                      AS udbudskategori_niveau_3,
    ud.[udbudskategori_niveau_4]                                      AS udbudskategori_niveau_4
INTO #DeltaToMerge
FROM [stg].[dim_UNSPSC] delta
JOIN [map].[UNSPSC] m
    ON m.clean_code_UNSPSC = delta.clean_code_UNSPSC

LEFT JOIN [arc].[ils_unspsc] u
    ON u.[unspsc]         = m.clean_code_UNSPSC

LEFT JOIN [arc].[kmd_uddata_unspsc] k
    ON k.[UNSPSCClassID] = m.clean_code_UNSPSC AND k.[DW_IsCurrent] = 1

LEFT JOIN (
    SELECT
        clean_code_UNSPSC,
        LEFT(LTRIM(RTRIM(REPLACE([UNSPSC_Tekst],        CHAR(160), ''))), 255) AS beskrivelse_udbud,
        LEFT(LTRIM(RTRIM(REPLACE([udbudstype],          CHAR(160), ''))), 255) AS udbudstype,
        LEFT(LTRIM(RTRIM(REPLACE([Udbudskategori_niv1], CHAR(160), ''))), 255) AS udbudskategori_niveau_1,
        LEFT(LTRIM(RTRIM(REPLACE([Udbudskategori_niv2], CHAR(160), ''))), 255) AS udbudskategori_niveau_2,
        LEFT(LTRIM(RTRIM(REPLACE([Udbudskategori_niv3], CHAR(160), ''))), 255) AS udbudskategori_niveau_3,
        LEFT(LTRIM(RTRIM(REPLACE([Udbudskategori_niv4], CHAR(160), ''))), 255) AS udbudskategori_niveau_4
    FROM (
        SELECT
            LTRIM(RTRIM([UNSPSC_Nr])) AS clean_code_UNSPSC,
            [UNSPSC_Tekst], [udbudstype],
            [Udbudskategori_niv1], [Udbudskategori_niv2],
            [Udbudskategori_niv3], [Udbudskategori_niv4],
            ROW_NUMBER() OVER (
                PARTITION BY LTRIM(RTRIM([UNSPSC_Nr]))
                ORDER BY (SELECT NULL)
            ) AS rn
        FROM [arc].[unspsc_udbudskategori]
        WHERE [UNSPSC_Nr] IS NOT NULL AND LTRIM(RTRIM([UNSPSC_Nr])) <> ''
    ) d
    WHERE d.rn = 1
) ud ON ud.clean_code_UNSPSC = m.clean_code_UNSPSC;

CREATE UNIQUE CLUSTERED INDEX IX_tmp ON #DeltaToMerge (dw_ek_UNSPSC);
"""

MERGE_SQL = """
MERGE INTO [dim].[UNSPSC] AS tgt
USING #DeltaToMerge AS src
ON tgt.dw_ek_UNSPSC = src.dw_ek_UNSPSC
WHEN MATCHED AND EXISTS (
    SELECT
        src.unspsc_kode, src.dw_source_ils, src.dw_source_kmd, src.dw_source_udbud,
        src.beskrivelse_ils, src.beskrivelse_engelsk_ils, src.beskrivelse_kmd,
        src.unspsc_level_kmd,
        src.unspsc_level_1_kode_kmd, src.unspsc_level_1_beskrivelse_kmd,
        src.unspsc_level_2_kode_kmd, src.unspsc_level_2_beskrivelse_kmd,
        src.unspsc_level_3_kode_kmd, src.unspsc_level_3_beskrivelse_kmd,
        src.unspsc_level_4_kode_kmd, src.unspsc_level_4_beskrivelse_kmd,
        src.beskrivelse_udbud, src.udbudstype,
        src.udbudskategori_niveau_1, src.udbudskategori_niveau_2,
        src.udbudskategori_niveau_3, src.udbudskategori_niveau_4
    EXCEPT
    SELECT
        tgt.unspsc_kode, tgt.dw_source_ils, tgt.dw_source_kmd, tgt.dw_source_udbud,
        tgt.beskrivelse_ils, tgt.beskrivelse_engelsk_ils, tgt.beskrivelse_kmd,
        tgt.unspsc_level_kmd,
        tgt.unspsc_level_1_kode_kmd, tgt.unspsc_level_1_beskrivelse_kmd,
        tgt.unspsc_level_2_kode_kmd, tgt.unspsc_level_2_beskrivelse_kmd,
        tgt.unspsc_level_3_kode_kmd, tgt.unspsc_level_3_beskrivelse_kmd,
        tgt.unspsc_level_4_kode_kmd, tgt.unspsc_level_4_beskrivelse_kmd,
        tgt.beskrivelse_udbud, tgt.udbudstype,
        tgt.udbudskategori_niveau_1, tgt.udbudskategori_niveau_2,
        tgt.udbudskategori_niveau_3, tgt.udbudskategori_niveau_4
) THEN UPDATE SET
    tgt.unspsc_kode = src.unspsc_kode,
    tgt.dw_source_ils = src.dw_source_ils,
    tgt.dw_source_kmd = src.dw_source_kmd,
    tgt.dw_source_udbud = src.dw_source_udbud,
    tgt.beskrivelse_ils = src.beskrivelse_ils,
    tgt.beskrivelse_engelsk_ils = src.beskrivelse_engelsk_ils,
    tgt.beskrivelse_kmd = src.beskrivelse_kmd,
    tgt.unspsc_level_kmd = src.unspsc_level_kmd,
    tgt.unspsc_level_1_kode_kmd = src.unspsc_level_1_kode_kmd,
    tgt.unspsc_level_1_beskrivelse_kmd = src.unspsc_level_1_beskrivelse_kmd,
    tgt.unspsc_level_2_kode_kmd = src.unspsc_level_2_kode_kmd,
    tgt.unspsc_level_2_beskrivelse_kmd = src.unspsc_level_2_beskrivelse_kmd,
    tgt.unspsc_level_3_kode_kmd = src.unspsc_level_3_kode_kmd,
    tgt.unspsc_level_3_beskrivelse_kmd = src.unspsc_level_3_beskrivelse_kmd,
    tgt.unspsc_level_4_kode_kmd = src.unspsc_level_4_kode_kmd,
    tgt.unspsc_level_4_beskrivelse_kmd = src.unspsc_level_4_beskrivelse_kmd,
    tgt.beskrivelse_udbud = src.beskrivelse_udbud,
    tgt.udbudstype = src.udbudstype,
    tgt.udbudskategori_niveau_1 = src.udbudskategori_niveau_1,
    tgt.udbudskategori_niveau_2 = src.udbudskategori_niveau_2,
    tgt.udbudskategori_niveau_3 = src.udbudskategori_niveau_3,
    tgt.udbudskategori_niveau_4 = src.udbudskategori_niveau_4,
    tgt.dw_timestamp_update = SYSDATETIME()
WHEN NOT MATCHED BY TARGET THEN INSERT (
    dw_ek_UNSPSC, unspsc_kode, dw_source_ils, dw_source_kmd, dw_source_udbud,
    beskrivelse_ils, beskrivelse_engelsk_ils,
    beskrivelse_kmd, unspsc_level_kmd,
    unspsc_level_1_kode_kmd, unspsc_level_1_beskrivelse_kmd,
    unspsc_level_2_kode_kmd, unspsc_level_2_beskrivelse_kmd,
    unspsc_level_3_kode_kmd, unspsc_level_3_beskrivelse_kmd,
    unspsc_level_4_kode_kmd, unspsc_level_4_beskrivelse_kmd,
    beskrivelse_udbud, udbudstype,
    udbudskategori_niveau_1, udbudskategori_niveau_2,
    udbudskategori_niveau_3, udbudskategori_niveau_4,
    dw_timestamp_insert, dw_timestamp_update
) VALUES (
    src.dw_ek_UNSPSC, src.unspsc_kode, src.dw_source_ils, src.dw_source_kmd, src.dw_source_udbud,
    src.beskrivelse_ils, src.beskrivelse_engelsk_ils,
    src.beskrivelse_kmd, src.unspsc_level_kmd,
    src.unspsc_level_1_kode_kmd, src.unspsc_level_1_beskrivelse_kmd,
    src.unspsc_level_2_kode_kmd, src.unspsc_level_2_beskrivelse_kmd,
    src.unspsc_level_3_kode_kmd, src.unspsc_level_3_beskrivelse_kmd,
    src.unspsc_level_4_kode_kmd, src.unspsc_level_4_beskrivelse_kmd,
    src.beskrivelse_udbud, src.udbudstype,
    src.udbudskategori_niveau_1, src.udbudskategori_niveau_2,
    src.udbudskategori_niveau_3, src.udbudskategori_niveau_4,
    SYSDATETIME(), SYSDATETIME()
);
"""


@dg.asset(
    name="dim_UNSPSC",
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        dg.AssetKey(["map_UNSPSC"]),
        dg.AssetKey(["erp", "stg_dim_UNSPSC"]),
    ],
    tags={"domain": "erp", "layer": "dimensions", "scd": "type1", "entity": "unspsc"},
    description="Type 1 MERGE into dim.UNSPSC; advances DFM checkpoint on success.",
)
def dim_UNSPSC(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim",
        "UNSPSC",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(BUILD_SQL)
                cursor.execute("SELECT COUNT(*) FROM #DeltaToMerge")
                resolved_count = cursor.fetchone()[0]

                cursor.execute(MERGE_SQL)
                merged = cursor.rowcount

                cursor.execute("DROP TABLE IF EXISTS #DeltaToMerge;")
                cursor.execute("UPDATE STATISTICS [dim].[UNSPSC];")
                cursor.execute("SELECT COUNT(*) FROM [dim].[UNSPSC] WHERE dw_ek_UNSPSC <> -1")
                total = cursor.fetchone()[0]

                conn.commit()
            finally:
                cursor.close()

        context.log.info(f"dim.UNSPSC: resolved={resolved_count}, merged={merged}, total={total}")

        logged_checkpoint: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "UNSPSC", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "UNSPSC")

        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=merged
        )

    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "resolved": resolved_count,
            "merged": merged,
            "total": total,
            "target_table": "dim.UNSPSC",
            "scd_type": "Type 1",
        }
    )

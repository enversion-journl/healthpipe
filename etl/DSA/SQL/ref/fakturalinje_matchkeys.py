# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ref.FakturalinjeMatchKeys — komplet match-nøgle-tabel pr. fakturalinje.

Persisterer CVR/fakturanr/beløb pr. dw_ek_Fakturalinje for ALLE linjer, så
fct_apply_fakturalinje_unspsc kan joine mod en komplet nøglemængde (uafhængigt
af delta-staging). Vedligeholdes fra stg.fct_Fakturalinje_mysupply + map.Fakturalinje.
Log-only (intet eget checkpoint): behandler den staging-delta der allerede ligger.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from DSA.SQL.common.dato_dimension import dato_key


def build_matchkeys_merge_sql() -> str:
    """MERGE MySupply match keys into ref.FakturalinjeMatchKeys (grain dw_ek_Fakturalinje).

    Reads the (delta) staging set joined to map.Fakturalinje for the shared PK.
    Old rows persist across runs, so the table stays complete for every line ever
    seen — this is what lets the apply-pass's ref-change branch reach old lines.

    Amounts are stored raw (DECIMAL(18,4)); ROUND(,2) happens at ref-join time in
    build_apply_resolve_sql. Both timestamps are set on INSERT (dw_timestamp_update
    is NOT NULL with no default). Change detection uses INTERSECT (NULL-safe).
    fct.Fakturalinje never deletes rows, so there is no delete branch.

    dw_ek_Dato_faktura is denormalised from the same source expression fct.Fakturalinje
    uses, so the apply-pass can apply its APPLY_PASS_MIN_FAKTURA_DATE scope without
    joining the fact table. It is part of the INTERSECT set on purpose: a corrected
    invoice date that moves a line into scope must bump dw_timestamp_update, because
    this table's watermark is the apply-pass's only delta trigger.
    """
    return f"""
MERGE [ref].[FakturalinjeMatchKeys] AS tgt
USING (
    SELECT
        fl_map.[dw_ek_Fakturalinje] AS dw_ek_Fakturalinje,
        src.[clean_code_Kreditor],
        src.[clean_code_Kreditor_header],
        src.[clean_code_Faktura],
        TRY_CONVERT(DECIMAL(18, 4), src.[linje_beloeb]) AS linje_beloeb,
        TRY_CONVERT(DECIMAL(18, 4), src.[base_price_enheds_pris]) AS enheds_pris,
        TRY_CONVERT(DECIMAL(18, 4), src.[antal_enheder_faktureret]) AS antal_enheder_faktureret,
        TRY_CONVERT(DECIMAL(18, 4),
            COALESCE(src.[metadata_faktura_total], src.[header_faktura_total])
        ) AS faktura_total,
        {dato_key("src.[faktura_dato]")} AS dw_ek_Dato_faktura
    FROM [stg].[fct_Fakturalinje_mysupply] src
    INNER JOIN [map].[Fakturalinje] fl_map
        ON fl_map.[clean_code_Fakturalinje] = src.[clean_code_Fakturalinje]
       AND src.[clean_code_Fakturalinje] IS NOT NULL
       AND src.[clean_code_Fakturalinje] <> ''
) AS s
ON tgt.[dw_ek_Fakturalinje] = s.[dw_ek_Fakturalinje]

WHEN MATCHED AND NOT EXISTS (
    SELECT tgt.[clean_code_Kreditor], tgt.[clean_code_Kreditor_header], tgt.[clean_code_Faktura],
           tgt.[linje_beloeb], tgt.[enheds_pris], tgt.[antal_enheder_faktureret], tgt.[faktura_total],
           tgt.[dw_ek_Dato_faktura]
    INTERSECT
    SELECT s.[clean_code_Kreditor], s.[clean_code_Kreditor_header], s.[clean_code_Faktura],
           s.[linje_beloeb], s.[enheds_pris], s.[antal_enheder_faktureret], s.[faktura_total],
           s.[dw_ek_Dato_faktura]
) THEN
    UPDATE SET
        tgt.[clean_code_Kreditor]        = s.[clean_code_Kreditor],
        tgt.[clean_code_Kreditor_header] = s.[clean_code_Kreditor_header],
        tgt.[clean_code_Faktura]         = s.[clean_code_Faktura],
        tgt.[linje_beloeb]               = s.[linje_beloeb],
        tgt.[enheds_pris]                = s.[enheds_pris],
        tgt.[antal_enheder_faktureret]   = s.[antal_enheder_faktureret],
        tgt.[faktura_total]              = s.[faktura_total],
        tgt.[dw_ek_Dato_faktura]         = s.[dw_ek_Dato_faktura],
        tgt.[dw_timestamp_update]        = CAST(SYSDATETIME() AS datetime2(0))

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        [dw_ek_Fakturalinje],
        [clean_code_Kreditor], [clean_code_Kreditor_header], [clean_code_Faktura],
        [linje_beloeb], [enheds_pris], [antal_enheder_faktureret], [faktura_total],
        [dw_ek_Dato_faktura],
        [dw_timestamp_insert], [dw_timestamp_update]
    )
    VALUES (
        s.[dw_ek_Fakturalinje],
        s.[clean_code_Kreditor], s.[clean_code_Kreditor_header], s.[clean_code_Faktura],
        s.[linje_beloeb], s.[enheds_pris], s.[antal_enheder_faktureret], s.[faktura_total],
        s.[dw_ek_Dato_faktura],
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );
"""


@dg.asset(
    key=dg.AssetKey(["erp", "ref_fakturalinje_matchkeys"]),
    group_name="erp_ref",
    compute_kind="mssql",
    deps=[
        dg.AssetKey(["erp", "stg_fct_Fakturalinje_mysupply"]),
        "map_Fakturalinje",
    ],
    tags={"domain": "erp", "layer": "ref", "entity": "fakturalinje_matchkeys"},
    description=(
        "Komplet match-nøgle-tabel pr. fakturalinje (dw_ek_Fakturalinje) til "
        "UNSPSC apply-pass; upsertes fra stg.fct_Fakturalinje_mysupply + map.Fakturalinje."
    ),
)
def ref_fakturalinje_matchkeys(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "ref",
        "FakturalinjeMatchKeys",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(build_matchkeys_merge_sql())
                merged = cursor.rowcount
                cursor.execute("SELECT COUNT(*) FROM [ref].[FakturalinjeMatchKeys]")
                row_count = cursor.fetchone()[0]
                conn.commit()
            finally:
                cursor.close()

        context.log.info(f"ref.FakturalinjeMatchKeys: {merged} rows touched, {row_count} total")
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=merged)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "merged": merged,
            "row_count": row_count,
            "target_table": "ref.FakturalinjeMatchKeys",
        }
    )

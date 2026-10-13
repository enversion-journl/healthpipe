# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Shared SQL fragments for the Vare/ILSVare staging flow.

The entity-delta hub (pre_stg_Leverandoervare_ILSVare_delta_entiteter), stg_dim_Leverandoervare and
stg_bridge_Leverandoervare_ILSVare key entities by the same CleanCode expressions and
resolve supplier CVR through the same lev_current CTE. The fragments are
single-sourced here so an edit in one consumer cannot desync the keys the
others join on — and the CVR normalisation follows utils.sql.tax_id_sql,
keeping Vare keys aligned with map.Kreditor semantics.
"""

from utils.sql.tax_id_sql import sql_cvr_cleancode_from_se_nr

# CleanCode templates ({col} = source column expression)
LVNR_CC = "CAST('LEVVARE_' + UPPER(LTRIM(RTRIM(REPLACE({col}, ' ', '')))) AS NVARCHAR(500))"
VNR_CC = "CAST('ILSVARE_' + UPPER(LTRIM(RTRIM(REPLACE(CAST({col} AS NVARCHAR(100)), ' ', '')))) AS NVARCHAR(120))"

# Alternative leverandoervarenummer-slots (spec: 2026-08-27-alternate-lvnr-rows).
# laftdstam har lvnr##1-4, laftd har lvnr##1-3. Slot ##1 er den primaere og
# haandteres af de eksisterende branches — listerne her er KUN alternativerne.
# Single-sourced saa hub, bridge, dim og fct-staging aldrig kan desynce paa
# hvilke slots der fanner ud.
LAFTDSTAM_ALT_LVNR_SLOTS = ["lvnr##2", "lvnr##3", "lvnr##4"]
LAFTD_ALT_LVNR_SLOTS = ["lvnr##2", "lvnr##3"]


def sql_lev_current_cte() -> str:
    """Named CTE ``lev_current``: one current row per supplier (lnr) with its
    CVR CleanCode and lev-timestamp; suppliers without a resolvable CVR are
    excluded. Use as ``;WITH{sql_lev_current_cte()}, ...``."""
    cvr_cc = sql_cvr_cleancode_from_se_nr("lev.se_nr")
    return f"""
    lev_current AS (
        SELECT lnr, clean_code_Kreditor, lev_dw_timestamp_update
        FROM (
            SELECT
                lev.lnr,
                CAST({cvr_cc} AS NVARCHAR(200)) AS clean_code_Kreditor,
                lev.dw_timestamp_update AS lev_dw_timestamp_update,
                ROW_NUMBER() OVER (
                    PARTITION BY lev.lnr
                    ORDER BY
                        CASE WHEN lev.se_nr IS NOT NULL AND LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))) <> '' THEN 0 ELSE 1 END,
                        COALESCE(lev.pro2modified, lev.pro2created, lev.dw_timestamp_update) DESC,
                        lev.dw_timestamp_update DESC
                ) AS rn
            FROM [arc].[ils_lev] lev
            WHERE lev.lnr IS NOT NULL
        ) ranked
        WHERE rn = 1 AND clean_code_Kreditor IS NOT NULL AND clean_code_Kreditor <> ''
    )"""

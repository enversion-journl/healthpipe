# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL fragments for ref.RekvirentUndtagelse full rebuild.

CleanCode is raw SGH_{sgh}_RNR_{rnr}, as stg_dim_Rekvirent and map.Rekvirent
hold it — never zero-padded, never RIGHT(...,4)-truncated. See
DSA/SQL/common/ils_clean_code_shared.

sql_norm_code serves two jobs here, and one trim-only definition is right for
both: it builds the key that has to match map.Rekvirent, and it joins the
exclusion list to the arc.ils_rekv master for navn. Measured against the dev
archive, the master join keeps all 115 of its 134 matches under trim-only —
identical to the previous de-spacing form — and no rekvundtagne value diverges
between the two forms at all, so the master join needs no separate, stricter
normalisation. The 19 non-matches are the genuine orphans the list preserves
with dw_ek_Rekvirent NULL.
"""

from DSA.SQL.common.ils_clean_code_shared import sql_norm_code

REF_TABLE = "[ref].[RekvirentUndtagelse]"

# sgh_n/rnr_n are the already-normalised aliases from the inner SELECT below,
# which filters out NULL and blank sgh/rnr — so both segments are always set.
CLEAN_CODE_EXPR = """
'SGH_' + sgh_n + '_RNR_' + rnr_n
""".strip()

BUILD_SOURCE_SQL = f"""
SELECT
    {CLEAN_CODE_EXPR} AS clean_code_Rekvirent,
    sgh_n AS sgh,
    rnr_n AS rnr,
    COALESCE(r.navn, u.navn) AS navn,
    u.navn AS navn_manuelt,
    m.dw_ek_Rekvirent
FROM (
    SELECT
        {sql_norm_code('u0.[sgh]')} AS sgh_n,
        {sql_norm_code('u0.[rnr]')} AS rnr_n,
        u0.[navn]
    FROM [arc].[ils_rekvundtagne] u0
    WHERE u0.[sgh] IS NOT NULL
      AND LTRIM(RTRIM(CAST(u0.[sgh] AS NVARCHAR(50)))) <> ''
      AND u0.[rnr] IS NOT NULL
      AND LTRIM(RTRIM(CAST(u0.[rnr] AS NVARCHAR(50)))) <> ''
) u
LEFT JOIN [arc].[ils_rekv] r
    ON {sql_norm_code('r.[sgh]')} = u.sgh_n
   AND {sql_norm_code('r.[rnr]')} = u.rnr_n
LEFT JOIN [map].[Rekvirent] m
    ON m.[clean_code_Rekvirent] = {CLEAN_CODE_EXPR}
""".strip()

REBUILD_SQL = f"""
DELETE FROM {REF_TABLE};

INSERT INTO {REF_TABLE} (
    [clean_code_Rekvirent],
    [sgh],
    [rnr],
    [navn],
    [navn_manuelt],
    [dw_ek_Rekvirent],
    [dw_timestamp_update]
)
SELECT
    src.clean_code_Rekvirent,
    src.sgh,
    src.rnr,
    src.navn,
    src.navn_manuelt,
    src.dw_ek_Rekvirent,
    CAST(SYSDATETIME() AS datetime2(0))
FROM (
{BUILD_SOURCE_SQL}
) src;
""".strip()

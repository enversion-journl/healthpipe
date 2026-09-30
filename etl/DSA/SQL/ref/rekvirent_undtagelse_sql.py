# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL fragments for ref.RekvirentUndtagelse full rebuild.

CleanCode is pad-only SGH_{sgh}_RNR_{rnr} (same as stg_dim_Rekvirent /
clean_codes.udlev_rekvirent_clean_code) — never RIGHT(...,4) truncation.
"""

REF_TABLE = "[ref].[RekvirentUndtagelse]"

# Pad-only rnr: left-pad with zeros to length 4 when shorter; keep longer values.
_RNR_PADDED = """
CASE
    WHEN LEN(rnr_n) < 4 THEN RIGHT(REPLICATE('0', 4) + rnr_n, 4)
    ELSE rnr_n
END
""".strip()

CLEAN_CODE_EXPR = f"""
'SGH_' + sgh_n + '_RNR_' + {_RNR_PADDED}
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
        UPPER(LTRIM(RTRIM(REPLACE(CAST(u0.[sgh] AS NVARCHAR(50)), ' ', '')))) AS sgh_n,
        UPPER(LTRIM(RTRIM(REPLACE(CAST(u0.[rnr] AS NVARCHAR(50)), ' ', '')))) AS rnr_n,
        u0.[navn]
    FROM [arc].[ils_rekvundtagne] u0
    WHERE u0.[sgh] IS NOT NULL
      AND LTRIM(RTRIM(CAST(u0.[sgh] AS NVARCHAR(50)))) <> ''
      AND u0.[rnr] IS NOT NULL
      AND LTRIM(RTRIM(CAST(u0.[rnr] AS NVARCHAR(50)))) <> ''
) u
LEFT JOIN [arc].[ils_rekv] r
    ON UPPER(LTRIM(RTRIM(REPLACE(CAST(r.[sgh] AS NVARCHAR(50)), ' ', '')))) = u.sgh_n
   AND UPPER(LTRIM(RTRIM(REPLACE(CAST(r.[rnr] AS NVARCHAR(50)), ' ', '')))) = u.rnr_n
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

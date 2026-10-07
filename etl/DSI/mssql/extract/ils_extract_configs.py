# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DFM extract configs and factory-generated assets for ILS source."""

import os

from core.resources import ILSMSSQLResource
from DSI.connectors.extract_config import DfmExtractConfig
from DSI.connectors.factory import create_dfm_extraction_assets

_ILS_REKVUNDTAGNE_FQN = os.getenv(
    "ILS_REKVUNDTAGNE_FQN",
    "[ils_pro2_log].[dbo].[rekvUndtagne]",
)

ILS_EXTRACT_CONFIGS = [
    DfmExtractConfig(
        source_schema="dbo",
        source_table="unspsc",
        target_table="ils_unspsc",
        delta_column="sysdato",
        checkpoint_type="datetime",
        checkpoint_from=("arc", "ils_unspsc"),
        description="ILS UNSPSC reference (delta via sysdato).",
    ),
    DfmExtractConfig(
        source_schema="dbo",
        source_table="vare",
        target_table="ils_vare",
        delta_column="sysdato",
        checkpoint_type="datetime",
        checkpoint_from=("arc", "ils_vare"),
        description="ILS Vare delta extract for dim.UNSPSC.",
    ),
    DfmExtractConfig(
        source_schema="dbo",
        source_table="lgrbeh",
        target_table="ils_lgrbeh",
        is_full_load=True,
        checkpoint_from=None,
        description="Warehouse stock from ILS (full load).",
    ),
    DfmExtractConfig(
        source_schema="dbo",
        source_table="afdlgrbeh",
        target_table="ils_afdlgrbeh",
        is_full_load=True,
        checkpoint_from=None,
        description="Department stock from ILS (full load).",
    ),
]

ils_unspsc_extract_assets = create_dfm_extraction_assets(
    configs=ILS_EXTRACT_CONFIGS[:2],
    source_resource_key="ils_mssql",
    source_resource_type=ILSMSSQLResource,
    source_system="ILS",
    group_name="ils_archive",
    domain_tag="erp",
    dialect="mssql",
)

lagerbeholdning_extract_assets = create_dfm_extraction_assets(
    configs=ILS_EXTRACT_CONFIGS[2:],
    source_resource_key="ils_mssql",
    source_resource_type=ILSMSSQLResource,
    source_system="ILS",
    group_name="ils_archive",
    domain_tag="erp",
    dialect="mssql",
)

# ---------------------------------------------------------------------------
# ILS DFM extract configs
# ---------------------------------------------------------------------------
#
# The DFM extracts target the source-prefixed ext.ils_<t>. The DSI/mssql/ils.py
# partition extract targets the source-suffixed ext.<t>_ils, so the two naming
# schemes are distinct.
#
# Two patterns:
#   A) Full load (14): vareart, varegrp, varetype, varetype2, projekt, moms, lev,
#      sgh, lgr, rekv, rekvgrp, lafth, lafthsgh, zz_oper — small reference tables,
#      truncate-and-load each run.
#   B) Delta on [sysdato] (7): laftdpris, laftdstam, laftd, udlev, lgrvare,
#      pmdm, indkfak — sysdato is the canonical "row touched at" column for
#      ILS (evidenced by the existing arc.ils_vare precedent using MAX([sysdato])).
#   C) Manual full load (1): rekvUndtagne via select_from_expr from ils_pro2_log
#      (override with ILS_REKVUNDTAGNE_FQN for local seed).
#
# All 22 archive into arc.ils_<t>. Already-archived ILS tables
# (lgrbeh, afdlgrbeh, unspsc, vare) are NOT in this list — see
# ils_unspsc_extract_assets and lagerbeholdning_extract_assets above.

_ILS_FULL_LOAD_TABLES = (
    "vareart",
    "varegrp",
    "varetype",
    "varetype2",
    "projekt",
    "moms",
    "lev",
    "sgh",
    "lgr",
    "rekv",
    "rekvgrp",
    "lafth",
    "lafthsgh",
    "zz_oper",
)

_ILS_SYSDATO_TABLES = (
    "laftdpris",
    "laftdstam",
    "laftd",
    "udlev",
    "lgrvare",
    "pmdm",
    "indkfak",
)


def _ils_full_load_config(src_table: str) -> DfmExtractConfig:
    return DfmExtractConfig(
        source_schema="dbo",
        source_table=src_table,
        target_table=f"ils_{src_table}",
        is_full_load=True,
        checkpoint_from=None,
        description=f"ILS {src_table} reference (full load).",
    )


def _ils_sysdato_config(src_table: str) -> DfmExtractConfig:
    return DfmExtractConfig(
        source_schema="dbo",
        source_table=src_table,
        target_table=f"ils_{src_table}",
        delta_column="sysdato",
        checkpoint_type="datetime",
        checkpoint_from=("arc", f"ils_{src_table}"),
        description=f"ILS {src_table} delta (sysdato).",
    )


# Manual exclusion list (Strategisk Indkøb). Not a Progress Pro2 table — no
# prrowid / sysdato. Cross-DB via select_from_expr (default ils_pro2_log).
REKVUNDTAGNE_EXTRACT_CONFIG = DfmExtractConfig(
    source_schema="dbo",
    source_table="rekvUndtagne",
    target_table="ils_rekvundtagne",
    is_full_load=True,
    checkpoint_from=None,
    columns=("sgh", "rnr", "navn"),
    select_from_expr=(
        "SELECT "
        "LTRIM(RTRIM([sgh])) AS [sgh], "
        "LTRIM(RTRIM([rnr])) AS [rnr], "
        "NULLIF(LTRIM(RTRIM([navn])), '') AS [navn] "
        f"FROM {_ILS_REKVUNDTAGNE_FQN}"
    ),
    index_keys=("sgh", "rnr"),
    description=(
        "ILS rekvUndtagne exclusion list from ils_pro2_log (full load). "
        "Udlev with (sgh,rnr) not in this list = reelt forbrug."
    ),
)

ILS_DFM_EXTRACT_CONFIGS = (
    [_ils_full_load_config(t) for t in _ILS_FULL_LOAD_TABLES]
    + [_ils_sysdato_config(t) for t in _ILS_SYSDATO_TABLES]
    + [REKVUNDTAGNE_EXTRACT_CONFIG]
)

ils_dfm_extract_assets = create_dfm_extraction_assets(
    configs=ILS_DFM_EXTRACT_CONFIGS,
    source_resource_key="ils_mssql",
    source_resource_type=ILSMSSQLResource,
    source_system="ILS",
    group_name="ils_archive",
    domain_tag="erp",
    dialect="mssql",
)

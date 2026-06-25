# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DFM extract configs and factory-generated assets for ILS source."""

from core.resources import ILSMSSQLResource
from DSI.connectors.extract_config import DfmExtractConfig
from DSI.connectors.factory import create_dfm_extraction_assets

ILS_EXTRACT_CONFIGS = [
    DfmExtractConfig(
        source_schema="dbo",
        source_table="unspsc",
        target_table="unspsc_ils",
        delta_column="sysdato",
        checkpoint_type="datetime",
        checkpoint_from=("arc", "unspsc_ils"),
        description="ILS UNSPSC reference (delta via sysdato).",
    ),
    DfmExtractConfig(
        source_schema="dbo",
        source_table="vare",
        target_table="vare_ils_delta",
        delta_column="sysdato",
        checkpoint_type="datetime",
        checkpoint_from=("arc", "vare_ils"),
        description="ILS Vare delta extract for dim.UNSPSC.",
    ),
    DfmExtractConfig(
        source_schema="dbo",
        source_table="lgrbeh",
        target_table="lgrbeh_ils",
        is_full_load=True,
        checkpoint_from=None,
        description="Warehouse stock from ILS (full load).",
    ),
    DfmExtractConfig(
        source_schema="dbo",
        source_table="afdlgrbeh",
        target_table="afdlgrbeh_ils",
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
    group_name="extract_ils",
    domain_tag="erp",
    dialect="mssql",
)

lagerbeholdning_extract_assets = create_dfm_extraction_assets(
    configs=ILS_EXTRACT_CONFIGS[2:],
    source_resource_key="ils_mssql",
    source_resource_type=ILSMSSQLResource,
    source_system="ILS",
    group_name="extract_lagerbeholdning",
    domain_tag="erp",
    dialect="mssql",
)

# ---------------------------------------------------------------------------
# ILS DFM extract configs (parallel to legacy DSI/mssql/ils.py partition extract)
# ---------------------------------------------------------------------------
#
# Runs alongside the legacy DSI/mssql/ils.py extract — the new DFM extracts
# target ext.*_ils_dfm so they don't clash with ext.*_ils produced by the
# legacy flow.
#
# Two patterns:
#   A) Full load (10): vareart, varegrp, varetype, moms, lev, sgh, lgr, rekv,
#      rekvgrp, lafth — small reference tables, truncate-and-load each run.
#   B) Delta on [sysdato] (7): laftdpris, laftdstam, laftd, udlev, lgrvare,
#      pmdm, indkfak — sysdato is the canonical "row touched at" column for
#      ILS (evidenced by the existing arc.vare_ils precedent using MAX([sysdato])).
#
# All 17 archive into arc.<t>_ils (no postfix). Already-archived ILS tables
# (lgrbeh, afdlgrbeh, unspsc, vare) are NOT in this list — see
# ils_unspsc_extract_assets and lagerbeholdning_extract_assets above.
#
# TODO: When the legacy ils.py partition extract is removed:
#   1. Migrate ext.*_ils_dfm → ext.*_ils (see precedent migration
#      infrastructure/ddl/migrations/20260528_dfm_erp_migration_rename_checkpoints.sql)
#   2. Update each `target_table` below to drop the _dfm postfix.
#   3. Update each ArchiveMergeConfig.source_table in
#      DSI/mssql/archive/arc_*_ils.py to drop the _dfm postfix.

_ILS_FULL_LOAD_TABLES = (
    "vareart",
    "varegrp",
    "varetype",
    "moms",
    "lev",
    "sgh",
    "lgr",
    "rekv",
    "rekvgrp",
    "lafth",
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
        target_table=f"{src_table}_ils_dfm",
        is_full_load=True,
        checkpoint_from=None,
        description=f"ILS {src_table} reference (full load).",
    )


def _ils_sysdato_config(src_table: str) -> DfmExtractConfig:
    return DfmExtractConfig(
        source_schema="dbo",
        source_table=src_table,
        target_table=f"{src_table}_ils_dfm",
        delta_column="sysdato",
        checkpoint_type="datetime",
        checkpoint_from=("arc", f"{src_table}_ils"),
        description=f"ILS {src_table} delta (sysdato).",
    )


ILS_DFM_EXTRACT_CONFIGS = [_ils_full_load_config(t) for t in _ILS_FULL_LOAD_TABLES] + [
    _ils_sysdato_config(t) for t in _ILS_SYSDATO_TABLES
]

ils_dfm_extract_assets = create_dfm_extraction_assets(
    configs=ILS_DFM_EXTRACT_CONFIGS,
    source_resource_key="ils_mssql",
    source_resource_type=ILSMSSQLResource,
    source_system="ILS",
    group_name="ils_archive",
    domain_tag="erp",
    dialect="mssql",
)

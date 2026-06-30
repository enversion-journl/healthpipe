# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ILS source system extraction configs and assets.

Full-load tables: vareart, varegrp, lafth, vare, lev, sgh, lgr, rekv, rekvgrp (9 tables)
Partitioned tables: laftdpris, laftdstam, laftd, lgrvare, pmdm, indkfak (6 × daily + monthly)

Note: lgrbeh and afdlgrbeh are managed by the standalone lagerbeholdning extract assets
in extract_lagerbeholdning.py — they run in the dedicated lagerbeholdning pipeline.
"""

from collections.abc import Sequence

from core.resources import ILSMSSQLResource
from DSI.connectors.mssql.asset_factory import create_erp_extraction_assets
from DSI.connectors.mssql.models import (
    TABLE_TYPE_MASTER_DATA_PARTITIONED,
    TableConfig,
    generate_partition_configs,
)

# ---------------------------------------------------------------------------
# Full-load configs (small reference tables, non-partitioned)
# ---------------------------------------------------------------------------

ILS_TABLE_CONFIGS: Sequence[TableConfig] = (
    TableConfig(
        output_name="ext_vareart_ils",
        source_schema="dbo",
        source_table="vareart",
        target_table="vareart_ils",
        description="Item type definitions from ILS (191 rows - full load).",
        is_full_load=True,
        consistency_columns=("vareart", "navn"),
    ),
    TableConfig(
        output_name="ext_varegrp_ils",
        source_schema="dbo",
        source_table="varegrp",
        target_table="varegrp_ils",
        description="Item group definitions from ILS (34 rows - full load).",
        is_full_load=True,
        consistency_columns=("varegrp", "navn"),
    ),
    TableConfig(
        output_name="ext_lafth_ils",
        source_schema="dbo",
        source_table="lafth",
        target_table="lafth_ils",
        description="Contract header records from ILS (750 rows - full load).",
        is_full_load=True,
        consistency_columns=("aftale", "lnr", "aftdto"),
    ),
    TableConfig(
        output_name="ext_vare_ils",
        source_schema="dbo",
        source_table="vare",
        target_table="vare_ils",
        description="Item master data from ILS (261K rows - full load).",
        is_full_load=True,
        consistency_columns=("vnr", "navn"),
    ),
    TableConfig(
        output_name="ext_lev_ils",
        source_schema="dbo",
        source_table="lev",
        target_table="lev_ils",
        description="Supplier master data from ILS (38K rows - full load).",
        is_full_load=True,
    ),
    TableConfig(
        output_name="ext_sgh_ils",
        source_schema="dbo",
        source_table="sgh",
        target_table="sgh_ils",
        description="Hospital (sygehus) master data from ILS - used by rekvirent and lager codesystems.",
        is_full_load=True,
    ),
    TableConfig(
        output_name="ext_lgr_ils",
        source_schema="dbo",
        source_table="lgr",
        target_table="lgr_ils",
        description="Warehouse (lager) master data from ILS - used by lager codesystem.",
        is_full_load=True,
    ),
    TableConfig(
        output_name="ext_rekv_ils",
        source_schema="dbo",
        source_table="rekv",
        target_table="rekv_ils",
        description="Requisitioner (rekvirent) master data from ILS - used by rekvirent codesystem.",
        is_full_load=True,
    ),
    TableConfig(
        output_name="ext_rekvgrp_ils",
        source_schema="dbo",
        source_table="rekvgrp",
        target_table="rekvgrp_ils",
        description="Requisitioner group master data from ILS - used by rekvirent codesystem.",
        is_full_load=True,
    ),
)

# ---------------------------------------------------------------------------
# Partitioned base configs (large tables requiring daily/monthly extraction)
# ---------------------------------------------------------------------------

ILS_PARTITIONED_BASE_TABLE_CONFIGS: Sequence[TableConfig] = (
    TableConfig(
        output_name="ext_laftdpris_ils",
        source_schema="dbo",
        source_table="laftdpris",
        target_table="laftdpris_ils",
        description="Contract line pricing from ILS (187K rows, partitioned by pro2created OR pro2modified)",
        partition_columns=("pro2created", "pro2modified"),
        table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
    ),
    TableConfig(
        output_name="ext_laftdstam_ils",
        source_schema="dbo",
        source_table="laftdstam",
        target_table="laftdstam_ils",
        description="Contract header data from ILS (420K rows, partitioned by pro2created OR pro2modified)",
        exclude_columns=("billed",),
        partition_columns=("pro2created", "pro2modified"),
        table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
    ),
    TableConfig(
        output_name="ext_laftd_ils",
        source_schema="dbo",
        source_table="laftd",
        target_table="laftd_ils",
        description="Contract line records from ILS (62K rows, partitioned by pro2created OR pro2modified)",
        partition_columns=("pro2created", "pro2modified"),
        table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
    ),
    TableConfig(
        output_name="ext_lgrvare_ils",
        source_schema="dbo",
        source_table="lgrvare",
        target_table="lgrvare_ils",
        description="Warehouse item mappings from ILS (188K rows, partitioned by pro2created OR sysdato)",
        partition_columns=("pro2created", "sysdato"),
        table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
    ),
    TableConfig(
        output_name="ext_pmdm_ils",
        source_schema="dbo",
        source_table="pmdm",
        target_table="pmdm_ils",
        description="Product master data mappings from ILS (96K rows, partitioned by pro2created OR sysdato)",
        partition_columns=("pro2created", "sysdato"),
        table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
    ),
    TableConfig(
        output_name="ext_indkfak_ils",
        source_schema="dbo",
        source_table="indkfak",
        target_table="indkfak_ils",
        description="Historical invoice mappings from ILS (434K rows, partitioned by pro2created OR sysdato)",
        partition_columns=("pro2created", "sysdato"),
        table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
    ),
    # NOTE: udlev (Vareudlevering) migrated to the DFM/archive pipeline
    # (ext.ils_udlev_dfm -> arc.ils_udlev), consumed by the DFM-based
    # vareudlevering assets. The legacy partition extract was removed (US 28691).
)

ILS_TABLE_CONFIGS_DAILY = generate_partition_configs(ILS_PARTITIONED_BASE_TABLE_CONFIGS, "daily")
ILS_TABLE_CONFIGS_MONTHLY = generate_partition_configs(
    ILS_PARTITIONED_BASE_TABLE_CONFIGS, "monthly"
)

# ---------------------------------------------------------------------------
# Generate assets via factory
# ---------------------------------------------------------------------------

_full_load_assets: list = []
for _cfg in ILS_TABLE_CONFIGS:
    (_asset,) = create_erp_extraction_assets(
        config=_cfg,
        source_system="ILS",
        source_resource_type=ILSMSSQLResource,
        source_resource_key="ils_mssql",
        include_daily=False,
        include_monthly=False,
        include_full_load=True,
    )
    _full_load_assets.append(_asset)

_daily_assets: list = []
for _cfg in ILS_TABLE_CONFIGS_DAILY:
    (_asset,) = create_erp_extraction_assets(
        config=_cfg,
        source_system="ILS",
        source_resource_type=ILSMSSQLResource,
        source_resource_key="ils_mssql",
        include_daily=True,
        include_monthly=False,
        include_full_load=False,
    )
    _daily_assets.append(_asset)

_monthly_assets: list = []
for _cfg in ILS_TABLE_CONFIGS_MONTHLY:
    (_asset,) = create_erp_extraction_assets(
        config=_cfg,
        source_system="ILS",
        source_resource_type=ILSMSSQLResource,
        source_resource_key="ils_mssql",
        include_daily=False,
        include_monthly=True,
        include_full_load=False,
    )
    _monthly_assets.append(_asset)

ils_full_load_assets = _full_load_assets
ils_daily_assets = _daily_assets
ils_monthly_assets = _monthly_assets
ils_all_assets = _full_load_assets + _daily_assets + _monthly_assets

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SydAX source system extraction configs and assets.

Master data (full-load): Afdeling, Hovedkonto, Gruppering, GrupperingAutoriseret, Art, Funktion,
    Dranst, Finanstransaktionstype (8 tables)
Partitioned master data: Finanskonto, FinanskontoOmkostningssted, OmkostningsstedHierarki,
    Kreditor, Finanskladde (5 × daily + monthly + full_load)
Transactional: Finanspostering, Finanskladdelinje (2 × daily + monthly + full_load)
Dependent/Bridge: Kreditortransaktion, FinansposteringKreditorTransRelation (2 × daily + monthly + full_load)
"""

import dataclasses
from collections.abc import Sequence

from core.resources import SydaxMSSQLResource
from DSI.connectors.mssql.asset_factory import create_erp_extraction_assets
from DSI.connectors.mssql.models import (
    TABLE_TYPE_BRIDGE,
    TABLE_TYPE_DEPENDENT,
    TABLE_TYPE_MASTER_DATA,
    TABLE_TYPE_MASTER_DATA_PARTITIONED,
    TABLE_TYPE_TRANSACTIONAL,
    TableConfig,
    generate_partition_configs,
)

# ---------------------------------------------------------------------------
# Base configs (all SydAX tables before daily/monthly suffix generation)
# ---------------------------------------------------------------------------

SYDAX_BASE_TABLE_CONFIGS: Sequence[TableConfig] = (
    TableConfig(
        output_name="ext_afdeling_sydax",
        source_schema="Finans",
        source_table="Afdeling",
        target_table="afdeling_sydax",
        description="Afdeling master data from SydAX",
        table_type=TABLE_TYPE_MASTER_DATA,
    ),
    TableConfig(
        output_name="ext_finanskonto_sydax",
        source_schema="Finans",
        source_table="Finanskonto",
        target_table="finanskonto_sydax",
        description="Finance account master data from SydAX (partitioned by CreatedOn OR Modified)",
        partition_columns=("CreatedOn", "Modified"),
        table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
    ),
    TableConfig(
        output_name="ext_finanskontoomkostningssted_sydax",
        source_schema="Finans",
        source_table="FinanskontoOmkostningssted",
        target_table="finanskontoomkostningssted_sydax",
        description="Mapping between finance accounts and cost centres (partitioned by CreatedOn OR Modified)",
        partition_columns=("CreatedOn", "Modified"),
        table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
    ),
    TableConfig(
        output_name="ext_hovedkonto_sydax",
        source_schema="Finans",
        source_table="Hovedkonto",
        target_table="hovedkonto_sydax",
        description="Finance main account hierarchy from SydAX",
        table_type=TABLE_TYPE_MASTER_DATA,
    ),
    TableConfig(
        output_name="ext_omkostningsstedhierarki_sydax",
        source_schema="Finans",
        source_table="OmkostningsstedHierarki",
        target_table="omkostningsstedhierarki_sydax",
        description="Cost centre hierarchy from SydAX (partitioned by CreatedOn OR Modified)",
        partition_columns=("CreatedOn", "Modified"),
        table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
    ),
    TableConfig(
        output_name="ext_gruppering_sydax",
        source_schema="Finans",
        source_table="Gruppering",
        target_table="gruppering_sydax",
        description="Grouping metadata from SydAX",
        table_type=TABLE_TYPE_MASTER_DATA,
    ),
    TableConfig(
        output_name="ext_grupperingautoriseret_sydax",
        source_schema="Finans",
        source_table="GrupperingAutoriseret",
        target_table="grupperingautoriseret_sydax",
        description="Grouping metadata from SydAX",
        table_type=TABLE_TYPE_MASTER_DATA,
    ),
    TableConfig(
        output_name="ext_art_sydax",
        source_schema="Finans",
        source_table="Art",
        target_table="art_sydax",
        description="Account type details from SydAX",
        table_type=TABLE_TYPE_MASTER_DATA,
    ),
    TableConfig(
        output_name="ext_funktion_sydax",
        source_schema="Finans",
        source_table="Funktion",
        target_table="funktion_sydax",
        description="Function classification from SydAX",
        table_type=TABLE_TYPE_MASTER_DATA,
    ),
    TableConfig(
        output_name="ext_dranst_sydax",
        source_schema="Finans",
        source_table="Dranst",
        target_table="dranst_sydax",
        description="Dranst code register from SydAX",
        table_type=TABLE_TYPE_MASTER_DATA,
    ),
    TableConfig(
        output_name="ext_kreditor_sydax",
        source_schema="Kreditor",
        source_table="Kreditor",
        target_table="kreditor_sydax",
        description="Supplier master data from SydAX (partitioned by CreatedOn OR Modified)",
        partition_columns=("CreatedOn", "Modified"),
        table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
    ),
    TableConfig(
        output_name="ext_finanspostering_sydax",
        source_schema="Finans",
        source_table="Finanspostering",
        target_table="finanspostering_sydax",
        description="Journey-level finance postings from SydAX",
        partition_column="Oprettelsestidspunkt",
        table_type=TABLE_TYPE_TRANSACTIONAL,
        parallel_workers=8,
    ),
    TableConfig(
        output_name="ext_kreditortransaktion_sydax",
        source_schema="Kreditor",
        source_table="Kreditortransaktion",
        target_table="kreditortransaktion_sydax",
        description="Supplier transaction ledger from SydAX",
        table_type=TABLE_TYPE_DEPENDENT,
        parent_table="Finanspostering",
        parent_partition_col="Oprettelsestidspunkt",
        fk_column="KreditortransaktionId",
        fk_path=(
            (
                "FinansposteringKreditorTransRelation",
                "Finans",
                "KreditortransaktionId",
                "FinansposteringId",
            ),
        ),
        parallel_workers=8,
    ),
    TableConfig(
        output_name="ext_finansposteringkreditortransrelation_sydax",
        source_schema="Finans",
        source_table="FinansposteringKreditorTransRelation",
        target_table="finansposteringkreditortransrelation_sydax",
        description="Bridge table between finance postings and supplier transactions",
        table_type=TABLE_TYPE_BRIDGE,
        parent_table="Finanspostering",
        parent_partition_col="Oprettelsestidspunkt",
        fk_column="FinansposteringId",
        fk_cast="VARCHAR(50)",
        fk_path=(),
    ),
    TableConfig(
        output_name="ext_finanskladde_sydax",
        source_schema="Finans",
        source_table="Finanskladde",
        target_table="finanskladde_sydax",
        description="Finance journal header from SydAX (partitioned by CreatedOn OR Modified)",
        partition_columns=("CreatedOn", "Modified"),
        table_type=TABLE_TYPE_MASTER_DATA_PARTITIONED,
    ),
    TableConfig(
        output_name="ext_finanskladdelinje_sydax",
        source_schema="Finans",
        source_table="Finanskladdelinje",
        target_table="finanskladdelinje_sydax",
        description="Finance journal line from SydAX",
        partition_column="CreatedOn",
        table_type=TABLE_TYPE_TRANSACTIONAL,
        parallel_workers=8,
    ),
    TableConfig(
        output_name="ext_finanstransaktionstype_sydax",
        source_schema="Finans",
        source_table="Finanstransaktionstype",
        target_table="finanstransaktionstype_sydax",
        description="Finance transaction type lookup from SydAX",
        table_type=TABLE_TYPE_MASTER_DATA,
    ),
)

SYDAX_TABLE_CONFIGS_DAILY = generate_partition_configs(SYDAX_BASE_TABLE_CONFIGS, "daily")
SYDAX_TABLE_CONFIGS_MONTHLY = generate_partition_configs(SYDAX_BASE_TABLE_CONFIGS, "monthly")
SYDAX_TABLE_CONFIGS_FULL_LOAD = generate_partition_configs(SYDAX_BASE_TABLE_CONFIGS, "full_load")

# ---------------------------------------------------------------------------
# Identify master data tables (full-load, non-partitioned)
# ---------------------------------------------------------------------------

_SYDAX_MASTER_DATA_CONFIGS = [
    cfg for cfg in SYDAX_BASE_TABLE_CONFIGS if cfg.table_type == TABLE_TYPE_MASTER_DATA
]

_SYDAX_PARTITIONED_BASE_CONFIGS = [
    cfg for cfg in SYDAX_BASE_TABLE_CONFIGS if cfg.table_type != TABLE_TYPE_MASTER_DATA
]

# ---------------------------------------------------------------------------
# Generate assets via factory
# ---------------------------------------------------------------------------

_full_load_assets: list = []
for _cfg in _SYDAX_MASTER_DATA_CONFIGS:
    (_asset,) = create_erp_extraction_assets(
        config=_cfg,
        source_system="SydAX",
        source_resource_type=SydaxMSSQLResource,
        source_resource_key="sydax_mssql",
        include_daily=False,
        include_monthly=False,
        include_full_load=True,
    )
    _full_load_assets.append(_asset)

for _cfg in SYDAX_TABLE_CONFIGS_FULL_LOAD:
    if _cfg.table_type == TABLE_TYPE_MASTER_DATA:
        continue
    (_asset,) = create_erp_extraction_assets(
        config=dataclasses.replace(_cfg, is_full_load=True),
        source_system="SydAX",
        source_resource_type=SydaxMSSQLResource,
        source_resource_key="sydax_mssql",
        include_daily=False,
        include_monthly=False,
        include_full_load=True,
    )
    _full_load_assets.append(_asset)

_daily_assets: list = []
for _cfg in SYDAX_TABLE_CONFIGS_DAILY:
    if _cfg.table_type == TABLE_TYPE_MASTER_DATA:
        continue
    (_asset,) = create_erp_extraction_assets(
        config=_cfg,
        source_system="SydAX",
        source_resource_type=SydaxMSSQLResource,
        source_resource_key="sydax_mssql",
        include_daily=True,
        include_monthly=False,
        include_full_load=False,
    )
    _daily_assets.append(_asset)

_monthly_assets: list = []
for _cfg in SYDAX_TABLE_CONFIGS_MONTHLY:
    if _cfg.table_type == TABLE_TYPE_MASTER_DATA:
        continue
    (_asset,) = create_erp_extraction_assets(
        config=_cfg,
        source_system="SydAX",
        source_resource_type=SydaxMSSQLResource,
        source_resource_key="sydax_mssql",
        include_daily=False,
        include_monthly=True,
        include_full_load=False,
    )
    _monthly_assets.append(_asset)

sydax_full_load_assets = _full_load_assets
sydax_daily_assets = _daily_assets
sydax_monthly_assets = _monthly_assets
sydax_all_assets = _full_load_assets + _daily_assets + _monthly_assets

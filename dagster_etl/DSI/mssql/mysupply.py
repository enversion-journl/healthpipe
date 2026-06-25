# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
MySupply source system extraction configs and assets.

Transactional: statistic_invoice_header (1 × daily + monthly + full_load)
Dependent: metadata, statistic_invoice_line (2 × daily + monthly + full_load)
"""

import dataclasses
from collections.abc import Sequence

from core.resources import MySupplyMSSQLResource
from DSI.connectors.mssql.asset_factory import create_erp_extraction_assets
from DSI.connectors.mssql.models import (
    TABLE_TYPE_DEPENDENT,
    TABLE_TYPE_TRANSACTIONAL,
    TableConfig,
    generate_partition_configs,
)

# ---------------------------------------------------------------------------
# Base configs
# ---------------------------------------------------------------------------

MYSUPPLY_BASE_TABLE_CONFIGS: Sequence[TableConfig] = (
    TableConfig(
        output_name="ext_statistic_invoice_header_mysupply",
        source_schema="dbo",
        source_table="statistic_invoice_header",
        target_table="statistic_invoice_header_mysupply",
        description="Invoice header statistics from MySupply",
        partition_column="FakturaDato",
        table_type=TABLE_TYPE_TRANSACTIONAL,
        is_string_date=True,
        parallel_workers=8,
        # Cap below the 10k ceiling to avoid the mssql-python bulkcopy wedge.
        bcp_max_chunk_size=5000,
    ),
    TableConfig(
        output_name="ext_metadata_mysupply",
        source_schema="dbo",
        source_table="metadata",
        target_table="metadata_mysupply",
        description="Supplier metadata from MySupply",
        table_type=TABLE_TYPE_DEPENDENT,
        parent_table="statistic_invoice_header",
        parent_partition_col="FakturaDato",
        fk_column="id",
        parent_fk_column="metadata_id",
        fk_path=(),
    ),
    TableConfig(
        output_name="ext_statistic_invoice_line_mysupply",
        source_schema="dbo",
        source_table="statistic_invoice_line",
        target_table="statistic_invoice_line_mysupply",
        description="Invoice line statistics from MySupply",
        table_type=TABLE_TYPE_DEPENDENT,
        parent_table="statistic_invoice_header",
        parent_partition_col="FakturaDato",
        fk_column="statistic_invoice_header_id",
        parent_fk_column="id",
        fk_path=(),
        parallel_workers=8,
        # Cap below the 10k ceiling to avoid the mssql-python bulkcopy wedge.
        bcp_max_chunk_size=5000,
    ),
    TableConfig(
        output_name="ext_statistic_invoice_header_allowance_charge_mysupply",
        source_schema="dbo",
        source_table="statistic_invoice_header_allowance_charge",
        target_table="statistic_invoice_header_allowance_charge_mysupply",
        description="Invoice header allowance/charge from MySupply",
        table_type=TABLE_TYPE_DEPENDENT,
        parent_table="statistic_invoice_header",
        parent_partition_col="FakturaDato",
        fk_column="statistic_invoice_header_id",
        parent_fk_column="id",
        fk_path=(),
    ),
    TableConfig(
        output_name="ext_statistic_invoice_line_allowance_charge_mysupply",
        source_schema="dbo",
        source_table="statistic_invoice_line_allowance_charge",
        target_table="statistic_invoice_line_allowance_charge_mysupply",
        description="Invoice line allowance/charge from MySupply",
        table_type=TABLE_TYPE_DEPENDENT,
        parent_table="statistic_invoice_header",
        parent_partition_col="FakturaDato",
        fk_column="statistic_invoice_line_id",
        parent_fk_column="id",
        fk_path=(("statistic_invoice_line", "dbo", "id", "statistic_invoice_header_id"),),
    ),
)

MYSUPPLY_TABLE_CONFIGS_DAILY = generate_partition_configs(MYSUPPLY_BASE_TABLE_CONFIGS, "daily")
MYSUPPLY_TABLE_CONFIGS_MONTHLY = generate_partition_configs(MYSUPPLY_BASE_TABLE_CONFIGS, "monthly")
MYSUPPLY_TABLE_CONFIGS_FULL_LOAD = generate_partition_configs(
    MYSUPPLY_BASE_TABLE_CONFIGS, "full_load"
)

# ---------------------------------------------------------------------------
# Generate assets via factory
# ---------------------------------------------------------------------------

_daily_assets: list = []
for _cfg in MYSUPPLY_TABLE_CONFIGS_DAILY:
    (_asset,) = create_erp_extraction_assets(
        config=_cfg,
        source_system="MySupply",
        source_resource_type=MySupplyMSSQLResource,
        source_resource_key="mysupply_mssql",
        include_daily=True,
        include_monthly=False,
        include_full_load=False,
    )
    _daily_assets.append(_asset)

_monthly_assets: list = []
for _cfg in MYSUPPLY_TABLE_CONFIGS_MONTHLY:
    (_asset,) = create_erp_extraction_assets(
        config=_cfg,
        source_system="MySupply",
        source_resource_type=MySupplyMSSQLResource,
        source_resource_key="mysupply_mssql",
        include_daily=False,
        include_monthly=True,
        include_full_load=False,
    )
    _monthly_assets.append(_asset)

_full_load_assets: list = []
for _cfg in MYSUPPLY_TABLE_CONFIGS_FULL_LOAD:
    (_asset,) = create_erp_extraction_assets(
        config=dataclasses.replace(_cfg, is_full_load=True),
        source_system="MySupply",
        source_resource_type=MySupplyMSSQLResource,
        source_resource_key="mysupply_mssql",
        include_daily=False,
        include_monthly=False,
        include_full_load=True,
    )
    _full_load_assets.append(_asset)

mysupply_daily_assets = _daily_assets
mysupply_monthly_assets = _monthly_assets
mysupply_full_load_assets = _full_load_assets
mysupply_all_assets = _daily_assets + _monthly_assets + _full_load_assets

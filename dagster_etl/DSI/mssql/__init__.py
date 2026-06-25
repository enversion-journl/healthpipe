# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSI MSSQL source configs — ILS, SydAX, MySupply, KMD UdData.

Each module defines table configs and generates extraction assets
using the shared MSSQL connector.
"""

from .ils import (  # noqa: F401
    ILS_PARTITIONED_BASE_TABLE_CONFIGS,
    ILS_TABLE_CONFIGS,
    ILS_TABLE_CONFIGS_DAILY,
    ILS_TABLE_CONFIGS_MONTHLY,
    ils_all_assets,
    ils_daily_assets,
    ils_full_load_assets,
    ils_monthly_assets,
)
from .mysupply import (  # noqa: F401
    MYSUPPLY_BASE_TABLE_CONFIGS,
    MYSUPPLY_TABLE_CONFIGS_DAILY,
    MYSUPPLY_TABLE_CONFIGS_FULL_LOAD,
    MYSUPPLY_TABLE_CONFIGS_MONTHLY,
    mysupply_all_assets,
    mysupply_daily_assets,
    mysupply_full_load_assets,
    mysupply_monthly_assets,
)
from .sydax import (  # noqa: F401
    SYDAX_BASE_TABLE_CONFIGS,
    SYDAX_TABLE_CONFIGS_DAILY,
    SYDAX_TABLE_CONFIGS_FULL_LOAD,
    SYDAX_TABLE_CONFIGS_MONTHLY,
    sydax_all_assets,
    sydax_daily_assets,
    sydax_full_load_assets,
    sydax_monthly_assets,
)
from .ils_enriched import ils_enriched_assets
from .sydax_enriched import sydax_enriched_assets
from .sydax_enriched_finanskladde import sydax_enriched_finanskladde_assets
from .sydax_enriched_budget import sydax_enriched_budget_assets
from .extract import (  # noqa: F401
    all_factory_extract_assets,
    ils_unspsc_extract_assets,
    kmd_uddata_extract_assets,
    lagerbeholdning_extract_assets,
)
from .archive import (  # noqa: F401
    all_archive_assets,
    arc_afdlgrbeh_ils,
    arc_lgrbeh_ils,
    arc_spend_analysis_lines_kmd_uddata,
    arc_unspsc_kmd_uddata,
    kmd_uddata_archive_assets,
)

SYDAX_TABLE_CONFIGS = SYDAX_BASE_TABLE_CONFIGS

daily_extract_assets = sydax_daily_assets + mysupply_daily_assets + ils_daily_assets
monthly_extract_assets = sydax_monthly_assets + mysupply_monthly_assets + ils_monthly_assets
full_load_extract_assets = ils_full_load_assets + sydax_full_load_assets + mysupply_full_load_assets

all_extract_assets = (
    daily_extract_assets
    + monthly_extract_assets
    + full_load_extract_assets
    + sydax_enriched_assets
    + sydax_enriched_finanskladde_assets
    + sydax_enriched_budget_assets
    + ils_enriched_assets
)

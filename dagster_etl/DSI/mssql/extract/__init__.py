# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DFM-based extract assets (factory-generated from configs).

Each source system has its own configs file. This package re-exports the
public symbols needed by definitions.py and tests, plus the combined
all_factory_extract_assets list.
"""

from .extract_configs_ils import (
    ILS_DFM_EXTRACT_CONFIGS,
    ILS_EXTRACT_CONFIGS,
    ils_dfm_extract_assets,
    ils_unspsc_extract_assets,
    lagerbeholdning_extract_assets,
)
from .extract_configs_kmd_uddata import (
    KMD_EXTRACT_CONFIGS,
    kmd_uddata_extract_assets,
)
from .extract_configs_mysupply import (
    MYSUPPLY_DFM_EXTRACT_CONFIGS,
    mysupply_dfm_extract_assets,
    mysupply_flow_start_assets,
)

all_factory_extract_assets = (
    ils_unspsc_extract_assets
    + lagerbeholdning_extract_assets
    + ils_dfm_extract_assets
    + kmd_uddata_extract_assets
    + mysupply_dfm_extract_assets
    + mysupply_flow_start_assets
)

__all__ = [
    "ILS_DFM_EXTRACT_CONFIGS",
    "ILS_EXTRACT_CONFIGS",
    "KMD_EXTRACT_CONFIGS",
    "MYSUPPLY_DFM_EXTRACT_CONFIGS",
    "all_factory_extract_assets",
    "ils_dfm_extract_assets",
    "ils_unspsc_extract_assets",
    "kmd_uddata_extract_assets",
    "lagerbeholdning_extract_assets",
    "mysupply_dfm_extract_assets",
    "mysupply_flow_start_assets",
]

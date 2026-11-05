# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DFM-based extract assets (factory-generated from configs).

Each source system has its own configs file. This package builds the combined
all_factory_extract_assets list.
"""

from .ils_extract_configs import (
    ils_dfm_extract_assets,
    ils_unspsc_extract_assets,
    lagerbeholdning_extract_assets,
)
from .kmd_uddata_extract_configs import kmd_uddata_extract_assets
from .mysupply_extract_configs import mysupply_dfm_extract_assets, mysupply_flow_start_assets

all_factory_extract_assets = (
    ils_unspsc_extract_assets
    + lagerbeholdning_extract_assets
    + ils_dfm_extract_assets
    + kmd_uddata_extract_assets
    + mysupply_dfm_extract_assets
    + mysupply_flow_start_assets
)

__all__ = ["all_factory_extract_assets"]

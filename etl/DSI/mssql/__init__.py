# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSI MSSQL source configs — ILS, SydAX, MySupply, KMD UdData.

Each module defines table configs and generates extraction assets
using the shared MSSQL connector.
"""

from .archive import (  # noqa: F401
    all_archive_assets,
    arc_ils_afdlgrbeh,
    arc_ils_lgrbeh,
    arc_kmd_uddata_spend_analysis_lines,
    arc_kmd_uddata_unspsc,
    kmd_uddata_archive_assets,
)
from .extract import (  # noqa: F401
    all_factory_extract_assets,
    ils_unspsc_extract_assets,
    kmd_uddata_extract_assets,
    lagerbeholdning_extract_assets,
)

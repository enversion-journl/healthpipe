# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSI extraction configs and assets for BOM_IFDB tables (BOM booking system).

Note: BOM_IFDB.AFTALE extraction is handled by DSI/oracle/ifdb.py (APPOINTMENT_CONFIG)
with pre-joined columns required by the FHIR Appointment transform.
"""

bom_ifdb_daily_assets: list = []
bom_ifdb_monthly_assets: list = []
bom_ifdb_full_load_assets: list = []

all_assets: list = []

__all__ = [
    "all_assets",
    "bom_ifdb_daily_assets",
    "bom_ifdb_monthly_assets",
    "bom_ifdb_full_load_assets",
]

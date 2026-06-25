# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSI CSV file extraction assets."""

from DSI.csv_files.os2mo import os2mo_assets
from DSI.csv_files.sks_register import extract_sks_register
from DSI.csv_files.sor_register import extract_sor_register

all_extract_assets = [*os2mo_assets, extract_sor_register, extract_sks_register]

__all__ = ["all_extract_assets"]

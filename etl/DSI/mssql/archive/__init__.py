# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Archive merge assets for the ERP (ILS, MySupply, SydAX) and KMD UdData extract layers."""

from .arc_ils_afdlgrbeh import arc_ils_afdlgrbeh
from .arc_ils_lgrbeh import arc_ils_lgrbeh
from .ils_archive_configs import (
    arc_ils_unspsc,
    arc_ils_vare,
    ils_dfm_archive_assets,
    ils_dfm_archive_configs,
)
from .kmd_uddata_archive_configs import (
    INVOICE_ARCHIVE_CONFIG,
    SPEND_ANALYSIS_LINES_ARCHIVE_CONFIG,
    UNSPSC_ARCHIVE_CONFIG,
    arc_kmd_uddata_invoice,
    arc_kmd_uddata_spend_analysis_lines,
    arc_kmd_uddata_unspsc,
    kmd_uddata_archive_assets,
    kmd_uddata_archive_configs,
)
from .mysupply_archive_configs import (
    arc_metadata_mysupply,
    arc_statistic_invoice_header_allowance_charge_mysupply,
    arc_statistic_invoice_header_mysupply,
    arc_statistic_invoice_line_allowance_charge_mysupply,
    arc_statistic_invoice_line_mysupply,
    mysupply_archive_assets,
)
from .sydax_archive_configs import (
    arc_afdeling_sydax,
    arc_art_sydax,
    arc_dranst_sydax,
    arc_finanskladde_sydax,
    arc_finanskladdelinje_sydax,
    arc_finanskonto_omkostningssted_sydax,
    arc_finanskonto_sydax,
    arc_finanspostering_kreditortrans_relation_sydax,
    arc_finanspostering_sydax,
    arc_finanstransaktionstype_sydax,
    arc_funktion_sydax,
    arc_gruppering_autoriseret_sydax,
    arc_gruppering_sydax,
    arc_hovedkonto_sydax,
    arc_kreditor_sydax,
    arc_kreditortransaktion_sydax,
    arc_omkostningssted_hierarki_sydax,
    sydax_archive_assets,
)

all_archive_assets = [
    arc_ils_lgrbeh,
    arc_ils_afdlgrbeh,
    arc_ils_unspsc,
    arc_ils_vare,
    *kmd_uddata_archive_assets,
    *mysupply_archive_assets,
    *ils_dfm_archive_assets,
    *sydax_archive_assets,
]

__all__ = [
    # Existing exports
    "arc_ils_lgrbeh",
    "arc_ils_afdlgrbeh",
    "arc_ils_unspsc",
    "arc_ils_vare",
    "arc_kmd_uddata_invoice",
    "arc_kmd_uddata_unspsc",
    "arc_kmd_uddata_spend_analysis_lines",
    # KMD UdData exports (consolidated in archive/kmd_uddata_archive_configs.py)
    "INVOICE_ARCHIVE_CONFIG",
    "SPEND_ANALYSIS_LINES_ARCHIVE_CONFIG",
    "UNSPSC_ARCHIVE_CONFIG",
    "kmd_uddata_archive_assets",
    "kmd_uddata_archive_configs",
    # MySupply exports (consolidated in archive/mysupply_archive_configs.py)
    "arc_statistic_invoice_header_mysupply",
    "arc_metadata_mysupply",
    "arc_statistic_invoice_line_mysupply",
    "arc_statistic_invoice_header_allowance_charge_mysupply",
    "arc_statistic_invoice_line_allowance_charge_mysupply",
    # Sydax exports (consolidated in archive/sydax_archive_configs.py)
    "arc_afdeling_sydax",
    "arc_finanskonto_sydax",
    "arc_finanskonto_omkostningssted_sydax",
    "arc_hovedkonto_sydax",
    "arc_omkostningssted_hierarki_sydax",
    "arc_gruppering_sydax",
    "arc_gruppering_autoriseret_sydax",
    "arc_art_sydax",
    "arc_funktion_sydax",
    "arc_dranst_sydax",
    "arc_kreditor_sydax",
    "arc_finanspostering_sydax",
    "arc_kreditortransaktion_sydax",
    "arc_finanspostering_kreditortrans_relation_sydax",
    "arc_finanskladde_sydax",
    "arc_finanskladdelinje_sydax",
    "arc_finanstransaktionstype_sydax",
    # Lists
    "all_archive_assets",
    "mysupply_archive_assets",
    "ils_dfm_archive_assets",
    "ils_dfm_archive_configs",
    "sydax_archive_assets",
]

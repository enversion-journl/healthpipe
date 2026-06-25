# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Archive merge assets for the ERP (ILS, MySupply, SydAX) and KMD UdData extract layers."""

from .arc_afdlgrbeh_ils import arc_afdlgrbeh_ils
from .arc_invoice_kmd_uddata import arc_invoice_kmd_uddata
from .arc_lgrbeh_ils import arc_lgrbeh_ils
from .arc_spend_analysis_lines_kmd_uddata import arc_spend_analysis_lines_kmd_uddata
from .arc_unspsc_ils import arc_unspsc_ils
from .arc_unspsc_kmd_uddata import arc_unspsc_kmd_uddata
from .arc_vare_ils import arc_vare_ils
from .ils_archive_configs import ils_dfm_archive_assets, ils_dfm_archive_configs
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
    arc_lgrbeh_ils,
    arc_afdlgrbeh_ils,
    arc_unspsc_ils,
    arc_vare_ils,
    arc_unspsc_kmd_uddata,
    arc_spend_analysis_lines_kmd_uddata,
    arc_invoice_kmd_uddata,
    *mysupply_archive_assets,
    *ils_dfm_archive_assets,
    *sydax_archive_assets,
]

kmd_uddata_archive_assets = [
    arc_unspsc_kmd_uddata,
    arc_spend_analysis_lines_kmd_uddata,
    arc_invoice_kmd_uddata,
]

__all__ = [
    # Existing exports
    "arc_lgrbeh_ils",
    "arc_afdlgrbeh_ils",
    "arc_invoice_kmd_uddata",
    "arc_unspsc_ils",
    "arc_vare_ils",
    "arc_unspsc_kmd_uddata",
    "arc_spend_analysis_lines_kmd_uddata",
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
    "kmd_uddata_archive_assets",
    "mysupply_archive_assets",
    "ils_dfm_archive_assets",
    "ils_dfm_archive_configs",
    "sydax_archive_assets",
]

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSA SQL Staging Assets

Consolidated staging assets from all domains: ERP, shared dimensions.
Owns all pre_stg, stg_dim, stg_fct, and organisation staging assets.
"""

from .pre_stg_afdlgrbeh_ils import pre_stg_AfdelingLagerbeholdning_ils
from .pre_stg_bridge_vare_ils import pre_stg_bridge_vare_ils
from .pre_stg_fakturalinje_mysupply import pre_stg_fakturalinje_mysupply
from .pre_stg_finanskladdelinje_sydax import pre_stg_finanskladdelinje_sydax
from .pre_stg_lgrbeh_ils import pre_stg_Lagerbeholdning_ils
from .pre_stg_postering_sydax import pre_stg_postering_sydax
from .pre_stg_vareaftaler_ils import pre_stg_vareaftaler_ils
from .pre_stg_vareudlevering_ils import pre_stg_vareudlevering_ils
from .stg_dim_aftale_ils import stg_dim_aftale_ils
from .stg_dim_ean import stg_dim_ean
from .stg_dim_faktura_mysupply import stg_dim_faktura_mysupply
from .stg_dim_fakturalinje_mysupply import stg_dim_fakturalinje_mysupply
from .stg_dim_budgetegenskaber_sydax import stg_dim_budget_egenskaber_sydax
from .stg_dim_kontoplan_sydax import stg_dim_kontoplan_sydax
from .stg_dim_finanskladde_sydax import stg_dim_finanskladde_sydax
from .stg_dim_finanskladdelinje_sydax import stg_dim_finanskladdelinje_sydax
from .stg_dim_kreditor_ils import stg_dim_kreditor_ils
from .stg_dim_kreditor_mysupply import stg_dim_kreditor_mysupply
from .stg_dim_LagerbeholdningEgenskaber_ils import stg_dim_LagerbeholdningEgenskaber_ils
from .stg_dim_AfdelingLagerbeholdningEgenskaber_ils import (
    stg_dim_AfdelingLagerbeholdningEgenskaber_ils,
)
from .stg_dim_kreditor_sydax import stg_dim_kreditor_sydax
from .stg_dim_lager import stg_dim_lager
from .stg_dim_postering_sydax import stg_dim_postering_sydax
from .stg_dim_rekvirent import stg_dim_rekvirent
from .stg_dim_unspsc_ils import stg_dim_unspsc_ils
from .stg_dim_unspsc_kmd import stg_dim_unspsc_kmd
from .stg_kmd_fakturalinje_unspsc_match import stg_kmd_fakturalinje_unspsc_match
from .stg_dim_vare import stg_dim_vare
from .stg_dim_vare_ils import stg_dim_vare_ils
from .stg_dim_vare_mysupply import stg_dim_vare_mysupply
from .stg_dim_vareudlevering_ils import stg_dim_vareudlevering_ils
from .stg_fct_budget_sydax import stg_fct_budget_sydax
from .stg_fct_faktura_mysupply import stg_fct_faktura_mysupply
from .stg_fct_fakturalinje_mysupply import stg_fct_fakturalinje_mysupply
from .stg_fct_lagerbeholdning_ils import stg_fct_Lagerbeholdning_ils
from .stg_fct_afdeling_lagerbeholdning_ils import stg_fct_AfdelingLagerbeholdning_ils
from .stg_fct_postering_sydax import stg_fct_postering_sydax
from .stg_fct_finanskladdelinje_sydax import stg_fct_finanskladdelinje_sydax
from .stg_fct_vareudlevering_ils import stg_fct_vareudlevering_ils
from .stg_fct_indkoebsanalyse_linje_kmd import stg_fct_indkoebsanalyse_linje_kmd
from .stg_okosted import stg_okosted
from .stg_map_ilsvare_ils import stg_map_ilsvare_ils
from .stg_organisation_sor import stg_organisation_sor

staging_assets = [
    # Pre-staging (joins, CleanCode generation, dedup)
    pre_stg_fakturalinje_mysupply,
    pre_stg_finanskladdelinje_sydax,
    pre_stg_postering_sydax,
    pre_stg_vareaftaler_ils,
    pre_stg_bridge_vare_ils,
    pre_stg_vareudlevering_ils,
    pre_stg_Lagerbeholdning_ils,
    pre_stg_AfdelingLagerbeholdning_ils,
    # Dimension staging
    stg_dim_aftale_ils,
    stg_dim_faktura_mysupply,
    stg_dim_fakturalinje_mysupply,
    stg_dim_kreditor_mysupply,
    stg_dim_vare_mysupply,
    stg_dim_kreditor_ils,
    stg_dim_vare,
    stg_dim_vare_ils,
    stg_dim_budget_egenskaber_sydax,
    stg_dim_kreditor_sydax,
    stg_dim_kontoplan_sydax,
    stg_dim_finanskladde_sydax,
    stg_dim_finanskladdelinje_sydax,
    stg_dim_postering_sydax,
    stg_dim_vareudlevering_ils,
    stg_dim_LagerbeholdningEgenskaber_ils,
    stg_dim_AfdelingLagerbeholdningEgenskaber_ils,
    stg_dim_unspsc_ils,
    stg_dim_unspsc_kmd,
    stg_kmd_fakturalinje_unspsc_match,
    stg_map_ilsvare_ils,
    # Fact staging
    stg_fct_budget_sydax,
    stg_fct_fakturalinje_mysupply,
    stg_fct_faktura_mysupply,
    stg_fct_Lagerbeholdning_ils,
    stg_fct_AfdelingLagerbeholdning_ils,
    stg_fct_postering_sydax,
    stg_fct_finanskladdelinje_sydax,
    stg_fct_vareudlevering_ils,
    stg_fct_indkoebsanalyse_linje_kmd,
    # Codesystem-based staging
    stg_dim_lager,
    stg_dim_rekvirent,
    stg_dim_ean,
    # Organisation staging
    stg_organisation_sor,
    stg_okosted,
]

all_assets = [*staging_assets]

__all__ = [
    "staging_assets",
    "all_assets",
    "stg_map_ilsvare_ils",
    "pre_stg_bridge_vare_ils",
    "pre_stg_fakturalinje_mysupply",
    "pre_stg_finanskladdelinje_sydax",
    "pre_stg_postering_sydax",
    "pre_stg_vareudlevering_ils",
    "pre_stg_Lagerbeholdning_ils",
    "pre_stg_AfdelingLagerbeholdning_ils",
    "pre_stg_vareaftaler_ils",
    "stg_dim_aftale_ils",
    "stg_dim_ean",
    "stg_dim_faktura_mysupply",
    "stg_dim_fakturalinje_mysupply",
    "stg_dim_budget_egenskaber_sydax",
    "stg_dim_kontoplan_sydax",
    "stg_dim_finanskladde_sydax",
    "stg_dim_finanskladdelinje_sydax",
    "stg_dim_kreditor_ils",
    "stg_dim_kreditor_mysupply",
    "stg_dim_kreditor_sydax",
    "stg_dim_lager",
    "stg_dim_postering_sydax",
    "stg_dim_rekvirent",
    "stg_dim_unspsc_ils",
    "stg_dim_unspsc_kmd",
    "stg_kmd_fakturalinje_unspsc_match",
    "stg_dim_vareudlevering_ils",
    "stg_dim_LagerbeholdningEgenskaber_ils",
    "stg_dim_AfdelingLagerbeholdningEgenskaber_ils",
    "stg_dim_vare",
    "stg_dim_vare_ils",
    "stg_dim_vare_mysupply",
    "stg_map_ilsvare_ils",
    "stg_fct_budget_sydax",
    "stg_fct_faktura_mysupply",
    "stg_fct_fakturalinje_mysupply",
    "stg_fct_Lagerbeholdning_ils",
    "stg_fct_AfdelingLagerbeholdning_ils",
    "stg_fct_postering_sydax",
    "stg_fct_finanskladdelinje_sydax",
    "stg_fct_vareudlevering_ils",
    "stg_fct_indkoebsanalyse_linje_kmd",
    "stg_okosted",
    "stg_organisation_sor",
]

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

from .pre_stg_AfdelingLagerbeholdning_ils import pre_stg_AfdelingLagerbeholdning_ils
from .pre_stg_Budget_sydax import pre_stg_Budget_sydax
from .pre_stg_DRGKontakt import pre_stg_DRGKontakt
from .pre_stg_Fakturalinje_mysupply import pre_stg_Fakturalinje_mysupply
from .pre_stg_Finanskladdelinje_sydax import pre_stg_Finanskladdelinje_sydax
from .pre_stg_Lagerbeholdning_ils import pre_stg_Lagerbeholdning_ils
from .pre_stg_Leverandoervare_ILSVare_delta_entiteter import (
    pre_stg_Leverandoervare_ILSVare_delta_entiteter,
)
from .pre_stg_Postering_sydax import pre_stg_Postering_sydax
from .pre_stg_Vareaftaler_ils import pre_stg_Vareaftaler_ils
from .pre_stg_Vareudlevering_ils import pre_stg_Vareudlevering_ils
from .stg_bridge_DRGKontaktDiagnose import stg_bridge_DRGKontaktDiagnose
from .stg_bridge_DRGKontaktProcedure import stg_bridge_DRGKontaktProcedure
from .stg_bridge_Leverandoervare_ILSVare import stg_bridge_Leverandoervare_ILSVare
from .stg_DRGDiagnoseGruppe import stg_DRGDiagnoseGruppe
from .stg_DRGProcedureGruppe import stg_DRGProcedureGruppe
from .stg_dim_AfdelingLagerbeholdningEgenskaber_ils import (
    stg_dim_AfdelingLagerbeholdningEgenskaber_ils,
)
from .stg_dim_Aftale_ils import stg_dim_Aftale_ils
from .stg_dim_BudgetEgenskaber_sydax import stg_dim_BudgetEgenskaber_sydax
from .stg_dim_DRGKontaktEgenskaber import stg_dim_DRGKontaktEgenskaber
from .stg_dim_EAN import stg_dim_EAN
from .stg_dim_Faktura_mysupply import stg_dim_Faktura_mysupply
from .stg_dim_Fakturalinje_mysupply import stg_dim_Fakturalinje_mysupply
from .stg_dim_Finanskladde_sydax import stg_dim_Finanskladde_sydax
from .stg_dim_Finanskladdelinje_sydax import stg_dim_Finanskladdelinje_sydax
from .stg_dim_ILSVare import stg_dim_ILSVare
from .stg_dim_Kontoplan_sydax import stg_dim_Kontoplan_sydax
from .stg_dim_Kreditor import stg_dim_Kreditor
from .stg_dim_Lager import stg_dim_Lager
from .stg_dim_LagerbeholdningEgenskaber_ils import stg_dim_LagerbeholdningEgenskaber_ils
from .stg_dim_Leverandoervare import stg_dim_Leverandoervare
from .stg_dim_Postering_sydax import stg_dim_Postering_sydax
from .stg_dim_Rekvirent import stg_dim_Rekvirent
from .stg_dim_UNSPSC import stg_dim_UNSPSC
from .stg_dim_Vareudlevering_ils import stg_dim_Vareudlevering_ils
from .stg_fct_AfdelingLagerbeholdning_ils import stg_fct_AfdelingLagerbeholdning_ils
from .stg_fct_AftaleVarePris_ils import stg_fct_AftaleVarePris_ils
from .stg_fct_Budget_sydax import stg_fct_Budget_sydax
from .stg_fct_DRGKontakt import stg_fct_DRGKontakt
from .stg_fct_Faktura_mysupply import stg_fct_Faktura_mysupply
from .stg_fct_Fakturalinje_mysupply import stg_fct_Fakturalinje_mysupply
from .stg_fct_Finanskladdelinje_sydax import stg_fct_Finanskladdelinje_sydax
from .stg_fct_Lagerbeholdning_ils import stg_fct_Lagerbeholdning_ils
from .stg_fct_Postering_sydax import stg_fct_Postering_sydax
from .stg_fct_Vareudlevering_ils import stg_fct_Vareudlevering_ils
from .stg_Okosted import stg_Okosted
from .stg_Organisation_sydax import stg_Organisation_sydax
from .stg_SHAK import stg_SHAK

staging_assets = [
    # Pre-staging (joins, clean_code generation, dedup)
    pre_stg_Budget_sydax,
    pre_stg_Fakturalinje_mysupply,
    pre_stg_Finanskladdelinje_sydax,
    pre_stg_Postering_sydax,
    pre_stg_Vareaftaler_ils,
    pre_stg_Leverandoervare_ILSVare_delta_entiteter,
    pre_stg_Vareudlevering_ils,
    pre_stg_DRGKontakt,
    stg_fct_DRGKontakt,
    stg_dim_DRGKontaktEgenskaber,
    stg_bridge_DRGKontaktDiagnose,
    stg_bridge_DRGKontaktProcedure,
    stg_DRGDiagnoseGruppe,
    stg_DRGProcedureGruppe,
    pre_stg_Lagerbeholdning_ils,
    pre_stg_AfdelingLagerbeholdning_ils,
    stg_bridge_Leverandoervare_ILSVare,
    # Dimension staging
    stg_dim_Aftale_ils,
    stg_dim_Faktura_mysupply,
    stg_dim_Fakturalinje_mysupply,
    stg_dim_ILSVare,
    stg_dim_Kreditor,
    stg_dim_Leverandoervare,
    stg_dim_BudgetEgenskaber_sydax,
    stg_dim_Kontoplan_sydax,
    stg_dim_Finanskladde_sydax,
    stg_dim_Finanskladdelinje_sydax,
    stg_dim_Postering_sydax,
    stg_dim_Vareudlevering_ils,
    stg_dim_LagerbeholdningEgenskaber_ils,
    stg_dim_AfdelingLagerbeholdningEgenskaber_ils,
    stg_dim_UNSPSC,
    # Fact staging
    stg_fct_AftaleVarePris_ils,
    stg_fct_Budget_sydax,
    stg_fct_Fakturalinje_mysupply,
    stg_fct_Faktura_mysupply,
    stg_fct_Lagerbeholdning_ils,
    stg_fct_AfdelingLagerbeholdning_ils,
    stg_fct_Postering_sydax,
    stg_fct_Finanskladdelinje_sydax,
    stg_fct_Vareudlevering_ils,
    # Codesystem-based staging
    stg_dim_Lager,
    stg_dim_Rekvirent,
    stg_dim_EAN,
    # Organisation staging
    stg_Organisation_sydax,
    stg_Okosted,
    stg_SHAK,
]

all_assets = [*staging_assets]

__all__ = [
    "staging_assets",
    "all_assets",
    "pre_stg_Budget_sydax",
    "pre_stg_Leverandoervare_ILSVare_delta_entiteter",
    "pre_stg_Fakturalinje_mysupply",
    "pre_stg_Finanskladdelinje_sydax",
    "pre_stg_Postering_sydax",
    "pre_stg_Vareudlevering_ils",
    "pre_stg_Lagerbeholdning_ils",
    "pre_stg_AfdelingLagerbeholdning_ils",
    "pre_stg_Vareaftaler_ils",
    "stg_bridge_Leverandoervare_ILSVare",
    "stg_bridge_DRGKontaktDiagnose",
    "stg_bridge_DRGKontaktProcedure",
    "stg_DRGDiagnoseGruppe",
    "stg_DRGProcedureGruppe",
    "stg_dim_Aftale_ils",
    "stg_dim_EAN",
    "stg_dim_Faktura_mysupply",
    "stg_dim_Fakturalinje_mysupply",
    "stg_dim_BudgetEgenskaber_sydax",
    "stg_dim_Kontoplan_sydax",
    "stg_dim_Finanskladde_sydax",
    "stg_dim_Finanskladdelinje_sydax",
    "stg_dim_ILSVare",
    "stg_dim_Kreditor",
    "stg_dim_Lager",
    "stg_dim_Postering_sydax",
    "stg_dim_Rekvirent",
    "stg_dim_UNSPSC",
    "stg_dim_Vareudlevering_ils",
    "stg_dim_LagerbeholdningEgenskaber_ils",
    "stg_dim_AfdelingLagerbeholdningEgenskaber_ils",
    "stg_dim_Leverandoervare",
    "stg_fct_AftaleVarePris_ils",
    "stg_fct_Budget_sydax",
    "stg_fct_Faktura_mysupply",
    "stg_fct_Fakturalinje_mysupply",
    "stg_fct_Lagerbeholdning_ils",
    "stg_fct_AfdelingLagerbeholdning_ils",
    "stg_fct_Postering_sydax",
    "stg_fct_Finanskladdelinje_sydax",
    "stg_fct_Vareudlevering_ils",
    "stg_Okosted",
    "stg_SHAK",
    "stg_Organisation_sydax",
]

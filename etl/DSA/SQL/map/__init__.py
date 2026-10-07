# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSA SQL Map Assets

Consolidated map update assets from all domains.
"""

from .map_AfdelingLagerbeholdning import map_AfdelingLagerbeholdning
from .map_AfdelingLagerbeholdningEgenskaber import map_AfdelingLagerbeholdningEgenskaber
from .map_Aftale import map_Aftale
from .map_AftaleVarePris import map_AftaleVarePris
from .map_Budget import map_Budget
from .map_BudgetEgenskaber import map_BudgetEgenskaber
from .map_DRGDiagnose import map_DRGDiagnose
from .map_DRGDiagnoseGruppe import map_DRGDiagnoseGruppe
from .map_DRGKontakt import map_DRGKontakt
from .map_DRGKontaktKlassifikation import map_DRGKontaktKlassifikation
from .map_DRGProcedure import map_DRGProcedure
from .map_DRGProcedureGruppe import map_DRGProcedureGruppe
from .map_EAN import map_EAN
from .map_Faktura import map_Faktura
from .map_Fakturalinje import map_Fakturalinje
from .map_Finanskladde import map_Finanskladde
from .map_Finanskladdelinje import map_Finanskladdelinje
from .map_ILSVare import map_ILSVare
from .map_Kontoplan import map_Kontoplan
from .map_Kreditor import map_Kreditor
from .map_Lager import map_Lager
from .map_Lagerbeholdning import map_Lagerbeholdning
from .map_LagerbeholdningEgenskaber import map_LagerbeholdningEgenskaber
from .map_Leverandoervare import map_Leverandoervare
from .map_Okosted import map_Okosted
from .map_Postering import map_Postering
from .map_Rekvirent import map_Rekvirent
from .map_SHAK import map_SHAK
from .map_UNSPSC import map_UNSPSC
from .map_Vareudlevering import map_Vareudlevering

all_assets = [
    map_DRGProcedure,
    map_DRGProcedureGruppe,
    map_DRGDiagnose,
    map_DRGDiagnoseGruppe,
    map_DRGKontaktKlassifikation,
    map_DRGKontakt,
    map_Aftale,
    map_AftaleVarePris,
    map_Budget,
    map_BudgetEgenskaber,
    map_EAN,
    map_Faktura,
    map_ILSVare,
    map_Fakturalinje,
    map_Finanskladde,
    map_Finanskladdelinje,
    map_Kontoplan,
    map_Kreditor,
    map_Lager,
    map_Postering,
    map_Rekvirent,
    map_Vareudlevering,
    map_Leverandoervare,
    map_LagerbeholdningEgenskaber,
    map_AfdelingLagerbeholdningEgenskaber,
    map_Lagerbeholdning,
    map_AfdelingLagerbeholdning,
    map_Okosted,
    map_SHAK,
    map_UNSPSC,
]

__all__ = [
    "map_DRGProcedure",
    "map_DRGProcedureGruppe",
    "map_DRGDiagnose",
    "map_DRGDiagnoseGruppe",
    "map_DRGKontaktKlassifikation",
    "map_DRGKontakt",
    "all_assets",
    "map_Aftale",
    "map_AftaleVarePris",
    "map_Budget",
    "map_BudgetEgenskaber",
    "map_EAN",
    "map_Faktura",
    "map_ILSVare",
    "map_Fakturalinje",
    "map_Finanskladde",
    "map_Finanskladdelinje",
    "map_Kontoplan",
    "map_Kreditor",
    "map_Lager",
    "map_Postering",
    "map_Rekvirent",
    "map_Vareudlevering",
    "map_Leverandoervare",
    "map_LagerbeholdningEgenskaber",
    "map_AfdelingLagerbeholdningEgenskaber",
    "map_Lagerbeholdning",
    "map_AfdelingLagerbeholdning",
    "map_Okosted",
    "map_SHAK",
    "map_UNSPSC",
]

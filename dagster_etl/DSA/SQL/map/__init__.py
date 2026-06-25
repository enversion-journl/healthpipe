# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSA SQL Map Assets

Consolidated map update assets from all domains.
"""

from .MapUpd_aftale import map_aftale
from .MapUpd_budget import map_budget
from .MapUpd_budgetegenskaber import map_budget_egenskaber
from .MapUpd_ean import map_ean
from .MapUpd_faktura import map_faktura
from .MapUpd_ilsvare import map_ilsvare
from .MapUpd_fakturalinje import map_fakturalinje
from .MapUpd_Finanskladde import map_finanskladde
from .MapUpd_Finanskladdelinje import map_finanskladdelinje
from .MapUpd_kontoplan import map_kontoplan
from .MapUpd_kreditor import map_kreditor
from .MapUpd_lager import map_lager
from .MapUpd_postering import map_postering
from .MapUpd_rekvirent import map_rekvirent
from .MapUpd_vareudlevering import map_vareudlevering
from .MapUpd_vare import map_vare
from .MapUpd_UNSPSC import map_UNSPSC
from .MapUpd_LagerbeholdningEgenskaber import map_LagerbeholdningEgenskaber
from .MapUpd_AfdelingLagerbeholdningEgenskaber import map_AfdelingLagerbeholdningEgenskaber
from .MapUpd_Lagerbeholdning import map_Lagerbeholdning
from .MapUpd_AfdelingLagerbeholdning import map_AfdelingLagerbeholdning
from .map_organisation import map_organisation
from .map_okosted import map_okosted
from .MapUpd_IndkoebsanalyseLinje import map_IndkoebsanalyseLinje

all_assets = [
    map_aftale,
    map_budget,
    map_budget_egenskaber,
    map_ean,
    map_faktura,
    map_ilsvare,
    map_fakturalinje,
    map_finanskladde,
    map_finanskladdelinje,
    map_kontoplan,
    map_kreditor,
    map_lager,
    map_postering,
    map_rekvirent,
    map_vareudlevering,
    map_vare,
    map_LagerbeholdningEgenskaber,
    map_AfdelingLagerbeholdningEgenskaber,
    map_Lagerbeholdning,
    map_AfdelingLagerbeholdning,
    map_organisation,
    map_okosted,
    map_UNSPSC,
    map_IndkoebsanalyseLinje,
]

__all__ = [
    "all_assets",
    "map_aftale",
    "map_budget",
    "map_budget_egenskaber",
    "map_ean",
    "map_faktura",
    "map_ilsvare",
    "map_fakturalinje",
    "map_finanskladde",
    "map_finanskladdelinje",
    "map_kontoplan",
    "map_kreditor",
    "map_lager",
    "map_postering",
    "map_rekvirent",
    "map_vareudlevering",
    "map_vare",
    "map_LagerbeholdningEgenskaber",
    "map_AfdelingLagerbeholdningEgenskaber",
    "map_Lagerbeholdning",
    "map_AfdelingLagerbeholdning",
    "map_organisation",
    "map_okosted",
    "map_UNSPSC",
    "map_IndkoebsanalyseLinje",
]

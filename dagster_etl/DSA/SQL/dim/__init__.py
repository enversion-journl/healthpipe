# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSA SQL Dimension Assets

Consolidated dimension update assets from all domains.
"""

from .borger import dim_borger
from .dim_organisation import dim_organisation
from .dim_okosted import dim_okosted
from .DimUpd_AfdelingLagerbeholdningEgenskaber import (
    dim_AfdelingLagerbeholdningEgenskaber,
)
from .DimUpd_LagerbeholdningEgenskaber import dim_LagerbeholdningEgenskaber
from .DimUpd_UNSPSC import dim_UNSPSC
from .DimUpd_aftale import dim_aftale
from .DimUpd_budget_egenskaber import dim_budget_egenskaber
from .DimUpd_ean import dim_ean
from .DimUpd_faktura import dim_faktura
from .DimUpd_fakturalinje import dim_fakturalinje
from .DimUpd_Finanskladde import dim_finanskladde
from .DimUpd_Finanskladdelinje import dim_finanskladdelinje
from .DimUpd_ilsvare import dim_ilsvare
from .DimUpd_kontoplan import dim_kontoplan
from .DimUpd_kreditor import dim_kreditor
from .DimUpd_lager import dim_lager
from .DimUpd_postering import dim_postering
from .DimUpd_rekvirent import dim_rekvirent
from .DimUpd_vare import dim_vare
from .DimUpd_vareudlevering_egenskaber import dim_vareudlevering_egenskaber

all_assets = [
    dim_aftale,
    dim_budget_egenskaber,
    dim_ean,
    dim_faktura,
    dim_ilsvare,
    dim_fakturalinje,
    dim_finanskladde,
    dim_finanskladdelinje,
    dim_kontoplan,
    dim_kreditor,
    dim_lager,
    dim_postering,
    dim_rekvirent,
    dim_UNSPSC,
    dim_vareudlevering_egenskaber,
    dim_vare,
    dim_LagerbeholdningEgenskaber,
    dim_AfdelingLagerbeholdningEgenskaber,
    dim_organisation,
    dim_okosted,
    dim_borger,
]

__all__ = [
    "all_assets",
    "dim_aftale",
    "dim_budget_egenskaber",
    "dim_ean",
    "dim_faktura",
    "dim_ilsvare",
    "dim_fakturalinje",
    "dim_finanskladde",
    "dim_finanskladdelinje",
    "dim_kontoplan",
    "dim_kreditor",
    "dim_lager",
    "dim_postering",
    "dim_rekvirent",
    "dim_UNSPSC",
    "dim_vareudlevering_egenskaber",
    "dim_vare",
    "dim_LagerbeholdningEgenskaber",
    "dim_AfdelingLagerbeholdningEgenskaber",
    "dim_organisation",
    "dim_okosted",
    "dim_borger",
]

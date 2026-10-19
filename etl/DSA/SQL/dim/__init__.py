# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSA SQL Dimension Assets

Consolidated dimension update assets from all domains.
"""

from .borger import dim_Borger
from .dim_AfdelingLagerbeholdningEgenskaber import (
    dim_AfdelingLagerbeholdningEgenskaber,
)
from .dim_Aftale import dim_Aftale
from .dim_BudgetEgenskaber import dim_BudgetEgenskaber
from .dim_DRGDiagnose import dim_DRGDiagnose
from .dim_DRGKontaktEgenskaber import dim_DRGKontaktEgenskaber
from .dim_DRGKontaktKlassifikation import dim_DRGKontaktKlassifikation
from .dim_DRGProcedure import dim_DRGProcedure
from .dim_Dato import dim_Dato
from .dim_EAN import dim_EAN
from .dim_Faktura import dim_Faktura
from .dim_Fakturalinje import dim_Fakturalinje
from .dim_Finanskladde import dim_Finanskladde
from .dim_Finanskladdelinje import dim_Finanskladdelinje
from .dim_Forloebsmarkoer import dim_Forloebsmarkoer
from .dim_ILSBruger import dim_ILSBruger
from .dim_ILSVare import dim_ILSVare
from .dim_ILSLeverandoer import dim_ILSLeverandoer
from .dim_Kontoplan import dim_Kontoplan
from .dim_Kreditor import dim_Kreditor
from .dim_Lager import dim_Lager
from .dim_LagerbeholdningEgenskaber import dim_LagerbeholdningEgenskaber
from .dim_Leverandoervare import dim_Leverandoervare
from .dim_Okosted import dim_Okosted
from .dim_Organisation import dim_Organisation
from .dim_Postering import dim_Postering
from .dim_Rekvirent import dim_Rekvirent
from .dim_SHAK import dim_SHAK
from .dim_UdredningsretEgenskaber import dim_UdredningsretEgenskaber
from .dim_UdredningsretKlassifikation import dim_UdredningsretKlassifikation
from .dim_SHAKAlike import dim_SHAKAlike
from .dim_UNSPSC import dim_UNSPSC
from .dim_Vareudlevering import dim_Vareudlevering

all_assets = [
    dim_DRGProcedure,
    dim_DRGDiagnose,
    dim_DRGKontaktKlassifikation,
    dim_DRGKontaktEgenskaber,
    dim_UdredningsretKlassifikation,
    dim_UdredningsretEgenskaber,
    dim_Forloebsmarkoer,
    dim_Aftale,
    dim_BudgetEgenskaber,
    dim_Dato,
    dim_EAN,
    dim_Faktura,
    dim_ILSBruger,
    dim_ILSVare,
    dim_ILSLeverandoer,
    dim_Fakturalinje,
    dim_Finanskladde,
    dim_Finanskladdelinje,
    dim_Kontoplan,
    dim_Kreditor,
    dim_Lager,
    dim_Postering,
    dim_Rekvirent,
    dim_UNSPSC,
    dim_Vareudlevering,
    dim_Leverandoervare,
    dim_LagerbeholdningEgenskaber,
    dim_AfdelingLagerbeholdningEgenskaber,
    dim_Organisation,
    dim_Okosted,
    dim_SHAK,
    dim_SHAKAlike,
    dim_Borger,
]

__all__ = [
    "dim_DRGProcedure",
    "dim_DRGDiagnose",
    "dim_DRGKontaktKlassifikation",
    "dim_DRGKontaktEgenskaber",
    "dim_UdredningsretKlassifikation",
    "dim_UdredningsretEgenskaber",
    "dim_Forloebsmarkoer",
    "all_assets",
    "dim_Aftale",
    "dim_BudgetEgenskaber",
    "dim_Dato",
    "dim_EAN",
    "dim_Faktura",
    "dim_ILSBruger",
    "dim_ILSVare",
    "dim_ILSLeverandoer",
    "dim_Fakturalinje",
    "dim_Finanskladde",
    "dim_Finanskladdelinje",
    "dim_Kontoplan",
    "dim_Kreditor",
    "dim_Lager",
    "dim_Postering",
    "dim_Rekvirent",
    "dim_UNSPSC",
    "dim_Vareudlevering",
    "dim_Leverandoervare",
    "dim_LagerbeholdningEgenskaber",
    "dim_AfdelingLagerbeholdningEgenskaber",
    "dim_Organisation",
    "dim_Okosted",
    "dim_SHAK",
    "dim_SHAKAlike",
    "dim_Borger",
]

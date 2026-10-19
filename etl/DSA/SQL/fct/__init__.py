# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSA SQL Fact Assets

Consolidated fact update assets.
"""

from .apply_faktura_postering_relations import fct_apply_faktura_postering_relations
from .apply_fakturalinje_unspsc import fct_apply_fakturalinje_unspsc
from .fct_AfdelingLagerbeholdning import fct_AfdelingLagerbeholdning
from .fct_AftaleVarePris import fct_AftaleVarePris
from .fct_Budget import fct_Budget
from .fct_DRGKontakt import fct_DRGKontakt
from .fct_Faktura import fct_Faktura
from .fct_Fakturalinje import fct_Fakturalinje
from .fct_Finanskladdelinje import fct_Finanskladdelinje
from .fct_Lagerbeholdning import fct_Lagerbeholdning
from .fct_Postering import fct_Postering
from .fct_Udredningsforloeb import fct_Udredningsforloeb
from .fct_UdredningsretMarkoer import fct_UdredningsretMarkoer
from .fct_Vareudlevering import fct_Vareudlevering

all_assets = [
    fct_DRGKontakt,
    fct_Udredningsforloeb,
    fct_UdredningsretMarkoer,
    fct_AftaleVarePris,
    fct_Budget,
    fct_Faktura,
    fct_Fakturalinje,
    fct_Finanskladdelinje,
    fct_Postering,
    fct_apply_faktura_postering_relations,
    fct_apply_fakturalinje_unspsc,
    fct_Vareudlevering,
    fct_Lagerbeholdning,
    fct_AfdelingLagerbeholdning,
]

__all__ = [
    "fct_DRGKontakt",
    "fct_Udredningsforloeb",
    "fct_UdredningsretMarkoer",
    "all_assets",
    "fct_AftaleVarePris",
    "fct_apply_faktura_postering_relations",
    "fct_apply_fakturalinje_unspsc",
    "fct_Budget",
    "fct_Faktura",
    "fct_Fakturalinje",
    "fct_Finanskladdelinje",
    "fct_Postering",
    "fct_Vareudlevering",
    "fct_Lagerbeholdning",
    "fct_AfdelingLagerbeholdning",
]

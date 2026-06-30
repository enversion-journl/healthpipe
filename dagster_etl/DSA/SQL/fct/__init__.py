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
from .FctUpd_budget import fct_budget
from .FctUpd_faktura import fct_faktura
from .FctUpd_fakturalinje import fct_fakturalinje
from .FctUpd_Finanskladdelinje import fct_finanskladdelinje
from .FctUpd_postering import fct_postering
from .FctUpd_vareudlevering import fct_vareudlevering
from .FctUpd_lagerbeholdning import fct_Lagerbeholdning
from .FctUpd_afdeling_lagerbeholdning import fct_AfdelingLagerbeholdning
from .FctUpd_indkoebsanalyse_linje import fct_indkoebsanalyse_linje

all_assets = [
    fct_budget,
    fct_faktura,
    fct_fakturalinje,
    fct_finanskladdelinje,
    fct_postering,
    fct_apply_faktura_postering_relations,
    fct_vareudlevering,
    fct_Lagerbeholdning,
    fct_AfdelingLagerbeholdning,
    fct_indkoebsanalyse_linje,
]

__all__ = [
    "all_assets",
    "fct_apply_faktura_postering_relations",
    "fct_budget",
    "fct_faktura",
    "fct_fakturalinje",
    "fct_finanskladdelinje",
    "fct_postering",
    "fct_vareudlevering",
    "fct_Lagerbeholdning",
    "fct_AfdelingLagerbeholdning",
    "fct_indkoebsanalyse_linje",
]

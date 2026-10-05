# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSA ConceptMap Assets

Dagster assets that build concept mappings from codesystem.* tables
(using fuzzy matching and manual overrides) and MERGE into
persistent database tables in the conceptmap schema.
"""

from .ean_to_sor.ean_to_sor import cm_ean_to_sor
from .lager_to_sor.lager_to_sor import cm_lager_to_sor
from .okosted_to_sor.okosted_to_sor import cm_okosted_to_sor
from .rekvirent_to_sor.rekvirent_to_sor import cm_rekvirent_to_sor
from .shak_to_sor.shak_to_sor import cm_shak_to_sor

conceptmap_dsa_assets = [
    cm_rekvirent_to_sor,
    cm_lager_to_sor,
    cm_ean_to_sor,
    cm_okosted_to_sor,
    cm_shak_to_sor,
]

all_assets = [*conceptmap_dsa_assets]

__all__ = [
    "conceptmap_dsa_assets",
    "all_assets",
    "cm_rekvirent_to_sor",
    "cm_lager_to_sor",
    "cm_ean_to_sor",
    "cm_okosted_to_sor",
    "cm_shak_to_sor",
]

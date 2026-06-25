# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSA CodeSystem Assets

Dagster assets that build codesystem structures from ext.* tables
and MERGE into persistent database tables in the codesystem schema.

Each codesystem produces three tables:
- codesystem.{name}_concept    (Code, Display)
- codesystem.{name}_properties (Code, PropertyCode, Value, Type)
- codesystem.{name}_hierarchy  (ParentCode, ChildCode)
"""

from .ean.ean import cs_ean
from .lager.lager import cs_lager
from .okosted.okosted import cs_okosted
from .rekvirent.rekvirent import cs_rekvirent
from .sfi.sfi import cs_sfi
from .sks.sks import cs_sks
from .sor.sor import cs_sor

codesystem_dsa_assets = [
    cs_rekvirent,
    cs_lager,
    cs_ean,
    cs_sor,
    cs_okosted,
    cs_sfi,
    cs_sks,
]

all_assets = [*codesystem_dsa_assets]

__all__ = [
    "codesystem_dsa_assets",
    "all_assets",
    "cs_rekvirent",
    "cs_lager",
    "cs_ean",
    "cs_sor",
    "cs_okosted",
    "cs_sfi",
    "cs_sks",
]

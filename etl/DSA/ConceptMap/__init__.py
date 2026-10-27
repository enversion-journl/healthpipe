# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSA ConceptMap Assets

Dagster assets that MERGE concept mappings into persistent database tables in
the conceptmap schema. The ERP maps (ean/lager/rekvirent/okosted -> SOR) come
exclusively from curated CSV files in DSI/csv_files/data/; SHAK and SHAK-a-like
come from their authoritative source crosswalks.
"""

from .csv_to_sor import (
    cm_ean_to_sor,
    cm_lager_to_sor,
    cm_okosted_to_sor,
    cm_rekvirent_to_sor,
)
from .shak_to_sor.shak_to_sor import cm_shak_to_sor
from .shakalike_to_sor.shakalike_to_sor import cm_shakalike_to_sor

conceptmap_dsa_assets = [
    cm_rekvirent_to_sor,
    cm_lager_to_sor,
    cm_ean_to_sor,
    cm_okosted_to_sor,
    cm_shak_to_sor,
    cm_shakalike_to_sor,
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
    "cm_shakalike_to_sor",
]

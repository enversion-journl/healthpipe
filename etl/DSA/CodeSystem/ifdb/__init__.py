# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""IFDB's egne administrative kodelister som ét CodeSystem."""

from DSA.CodeSystem.ifdb.ifdb import KODELISTE_FELTER, cs_ifdb_kodelister

all_assets = [cs_ifdb_kodelister]

__all__ = ["KODELISTE_FELTER", "all_assets", "cs_ifdb_kodelister"]

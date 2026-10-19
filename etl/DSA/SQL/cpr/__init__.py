# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""CPR-derived tables in the DSA layer."""

from DSA.SQL.cpr.abonnement_kandidater import cpr_abonnement_kandidater

all_assets = [cpr_abonnement_kandidater]

__all__ = ["all_assets", "cpr_abonnement_kandidater"]

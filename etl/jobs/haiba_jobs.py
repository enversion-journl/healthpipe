# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dagster job for the HAIBA delivery pipeline (extract -> archive).

Reads the newest usable weekly delivery per HAI type from the SDS drop folder
into ext.haiba_*, then MERGEs it into the persistent arc.haiba_* archive.

Deliberately unscheduled. Detecting a new delivery and mounting the drop folder
are separate work items; until they land, run this manually after a delivery
arrives. The extract picks the newest usable file per type on its own, so
running it twice against the same delivery is a no-op in the archive.
"""

import dagster as dg

haiba_job = dg.define_asset_job(
    name="haiba_job",
    selection=dg.AssetSelection.groups("haiba_extract", "haiba_archive"),
    description=(
        "Load the newest HAIBA delivery per HAI type into ext.haiba_* and MERGE "
        "into the arc.haiba_* archive."
    ),
)

__all__ = ["haiba_job"]

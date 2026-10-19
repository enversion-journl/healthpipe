# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Job for the municipality-to-population-responsibility reference table."""

import dagster as dg

populationsansvar_ref_job = dg.define_asset_job(
    name="populationsansvar_ref_job",
    selection=dg.AssetSelection.assets("ref_KommuneToPopulationsansvar"),
    description=(
        "Write the municipality mapping to ref.KommuneToPopulationsansvar on staging and DSO. "
        "Safe to run at any time; the next dim_Borger run applies a changed mapping."
    ),
)

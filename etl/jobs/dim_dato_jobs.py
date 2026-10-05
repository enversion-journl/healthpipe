# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Job for daily dim.Dato relative refresh."""

import dagster as dg

dim_dato_daily_job = dg.define_asset_job(
    name="dim_dato_daily_job",
    selection=dg.AssetSelection.assets("dim_Dato"),
    description=(
        "Daily refresh of relative and afsluttet columns in dim.Dato "
        "on both staging and DSO databases."
    ),
)

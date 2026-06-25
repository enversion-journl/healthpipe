# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Sydax archive job — selects all ext.*_sydax_dfm + arc.*_sydax assets.

No schedule yet. Run manually via Launchpad until downstream consumers
adopt the arc layer.
"""

import dagster as dg

sydax_archive_job = dg.define_asset_job(
    name="sydax_archive_job",
    selection=dg.AssetSelection.groups("sydax_archive"),
    description=(
        "Daily-style DFM run for SydAX archive: ext.*_sydax_dfm → "
        "arc.*_sydax across all 17 base tables. No schedule yet — run "
        "manually until downstream pipelines migrate to the arc layer."
    ),
)

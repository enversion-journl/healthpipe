# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Daily DSA + DSO disk-space monitor job.

Not part of the ETL graph: a full filegroup must surface here, not as a
failed dso_sync_* MERGE.
"""

import dagster as dg

db_space_daily_job = dg.define_asset_job(
    name="db_space_daily_job",
    selection=dg.AssetSelection.groups("infra_ops"),
    tags={"type": "ops", "object": "disk_space"},
    description=(
        "Snapshot DSA and DSO file/volume occupancy and fail if remaining "
        "space or weeks-until-full crosses the critical threshold."
    ),
)

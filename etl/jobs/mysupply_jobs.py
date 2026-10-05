# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
MySupply pipeline jobs.

Dedicated job for the MySupply DFM archive pipeline. Pulls a 7-day tail
of invoice header + dependents (filtered on FakturaDato via TRY_CAST),
MERGE-upserts into arc.*_mysupply with change-detection, and advances
the DFM checkpoint to SYSDATETIME.

Runs in parallel with the legacy partition-based MySupply extract; the
new DFM extracts populate ext.*_mysupply_dfm to avoid clashing with
ext.*_mysupply produced by the legacy flow.

Manual / on-demand run available via this job. Also wired into
erp_daily_dfm_job so the archive runs daily alongside other DFM groups.
"""

import dagster as dg

mysupply_archive_job = dg.define_asset_job(
    name="mysupply_archive_job",
    selection=dg.AssetSelection.groups("mysupply_archive"),
    description=(
        "DFM extract + archive of MySupply invoice header and dependents. "
        "Delta-loaded via FakturaDato + 7-day tail; change-detect UPSERT "
        "into arc.*_mysupply."
    ),
    tags={"domain": "erp", "layer": "archive", "source": "mysupply"},
)

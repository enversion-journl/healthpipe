# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ILS pipeline jobs.

Dedicated job for the ILS DFM archive pipeline. Runs in parallel with the
legacy partition-based ILS extract (DSI/mssql/ils.py); the new DFM extracts
populate ext.*_ils_dfm to avoid clashing with ext.*_ils produced by the
legacy flow, then merge into arc.*_ils.

Manual / on-demand run available via this job. Also wired into
erp_daily_dfm_job so the archive runs daily alongside other DFM groups.
"""

import dagster as dg

ils_archive_job = dg.define_asset_job(
    name="ils_archive_job",
    selection=dg.AssetSelection.groups("ils_archive"),
    description=(
        "DFM extract + archive of 17 ILS source tables. Pattern A (full "
        "load) for reference tables, Pattern B (pro2modified) and C "
        "(GREATEST(pro2created, sysdato)) for partitioned tables. UPSERT "
        "into arc.*_ils keyed on prrowid with change detection."
    ),
    tags={"domain": "erp", "layer": "archive", "source": "ils"},
)

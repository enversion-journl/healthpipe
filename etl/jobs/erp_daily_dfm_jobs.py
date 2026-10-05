# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ERP daily DFM (dim/fact/master) pipeline job.

Daily refresh of the ERP dimensional model. For each domain it runs the full
transform chain — archive → pre-staging → staging → maps → dimensions →
facts → DSO sync — building the published dimensions and facts from the DFM
archive layer (``arc.*``), together with current-state snapshots and
master/reference dimensions.

Assets are selected by group name. The standalone per-archive jobs remain
available for ad-hoc runs.
"""

import dagster as dg

erp_daily_dfm_job = dg.define_asset_job(
    name="erp_daily_dfm_job",
    selection=dg.AssetSelection.groups(
        # Archive layer — one group per source system (ext + arc together)
        "ils_archive",
        "sydax_archive",
        "mysupply_archive",
        "kmd_uddata_archive",
        # Downstream transform layers (all ERP domains)
        "erp_pre_staging",
        "erp_staging",
        "erp_ref",
        "erp_map",
        "erp_dimension",
        "erp_fact",
        "erp_dso_sync",
    ),
    description=(
        "Daily refresh of the ERP dimensional model: builds the published "
        "dimensions and facts from the DFM archive layer, running each domain's "
        "full transform chain from archive through to DSO sync, alongside "
        "current-state snapshots and master/reference dimensions."
    ),
)

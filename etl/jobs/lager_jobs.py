# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Lager dimension ETL jobs.

Convenience job for running the Lager pipeline independently.
Lager assets also run as part of erp_daily_dfm_job via the standard
erp_staging / erp_map / erp_dimension groups.
"""

import dagster as dg

lager_dim_job = dg.define_asset_job(
    name="lager_dim_job",
    selection=dg.AssetSelection.assets(
        # Codesystem + ConceptMap
        "cs_lager",
        "cm_lager_to_sor",
        # Staging
        "stg_dim_Lager",
        # Map + Dim
        "map_Lager",
        "dim_Lager",
    ),
    description=(
        "Standalone Lager dimension ETL from ILS database. "
        "Extracts Lager codesystem, creates SOR ConceptMap, "
        "stages to stg.dim_Lager, then populates map.Lager and dim.Lager. "
        "NOTE: Lager assets also run as part of erp_daily_dfm_job."
    ),
)

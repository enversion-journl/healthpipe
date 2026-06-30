# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Lager dimension ETL jobs.

Convenience job for running the Lager pipeline independently.
Lager assets are also included in erp_daily_etl and erp_monthly_etl
via standard erp_staging / erp_maps / erp_dimensions groups.
"""

import dagster as dg

lager_dim_job = dg.define_asset_job(
    name="lager_dim_job",
    selection=dg.AssetSelection.assets(
        # Codesystem + ConceptMap
        "cs_lager",
        "cm_lager_to_sor",
        # Staging
        "stg_dim_lager",
        # Map + Dim
        "map_lager",
        "dim_lager",
    ),
    description=(
        "Standalone Lager dimension ETL from ILS database. "
        "Extracts Lager codesystem, creates SOR ConceptMap, "
        "stages to stg.dim_lager, then populates map.Lager and dim.Lager. "
        "NOTE: Lager assets also run as part of erp_daily_etl / erp_monthly_etl."
    ),
)

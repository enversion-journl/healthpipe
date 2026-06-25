# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Data-quality job definitions.

Selects every asset in the ``data_quality`` group so new checks added to
``dagster_etl/data_quality/`` are automatically included.
"""

import dagster as dg

data_quality_job = dg.define_asset_job(
    name="data_quality_job",
    selection=dg.AssetSelection.groups("data_quality"),
    description=(
        "Cross-tier data quality checks: FHIR grain consistency "
        "(IFDB -> FHIR Server -> DSA -> DSO) and deleted-resource reconciliation."
    ),
)

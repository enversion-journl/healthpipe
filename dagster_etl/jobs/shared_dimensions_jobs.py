# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Jobs for Shared Dimensions

NOTE: Infrastructure is now centralized. Use `full_infrastructure_job` from
the `infrastructure` module for all DDL setup (staging + DSO).
"""

from dagster import AssetSelection, define_asset_job

shared_dimensions_data_job = define_asset_job(
    name="shared_dimensions_data_update",
    description="Update shared dimensions data (staging to maps to dimensions to DSO sync)",
    selection=AssetSelection.groups(
        "shared_dimensions_staging",
        "shared_dimensions_maps",
        "ref",
        "shared_dimensions",
        "shared_dimensions_dso_sync",
    ),
    tags={
        "domain": "shared_dimensions",
        "type": "data",
        "frequency": "daily",
    },
)

__all__ = [
    "shared_dimensions_data_job",
]

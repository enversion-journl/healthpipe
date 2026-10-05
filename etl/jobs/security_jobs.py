# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Security (DPA / RBAC) daily pipeline job.

The chain: sec.* DDL on staging and DSO -> extract_security (LDAPS) ->
populate_security (staging) -> dso_sync_security.
"""

import dagster as dg

# The DDL is in the selection, not just a dep: a deps edge guarantees ordering,
# not that the tables exist. Idempotent (IF NOT EXISTS / CREATE OR ALTER).
# Only the two security assets — not the whole infra groups, the migrations or
# cs_sor.
security_infrastructure_assets = dg.AssetSelection.assets(
    "staging_infra_security",
    "dso_infra_security",
)

security_daily_job = dg.define_asset_job(
    name="security_daily_job",
    selection=security_infrastructure_assets
    | dg.AssetSelection.groups(
        "extract_security",
        "populate_security",
        "dso_sync_security",
    ),
    description=(
        "Daily refresh of the DPA security model: deploys sec.* DDL, extracts HealthPipe "
        "AD groups over LDAPS, reloads sec.RolleDomaene / sec.RBOrganisation on "
        "staging, and syncs to DSO."
    ),
)

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSO security sync assets: sync sec.* tables from staging to DSO.

Part of the additive DPA security model. The RolleDomaene /
RBOrganisation tables are maintained on staging (source of truth) and synced to
DSO, where the secured views read them. Population of these tables from source
systems is a later branch; until then these assets sync empty tables (a 0-row
truncate_insert, which is harmless).
"""

import dagster as dg
from core.resources import MSSQLResource
from DSO.load.sync_configs import SECURITY_SYNC_CONFIGS
from DSO.load.sync_dim import _materialize_sync_result


@dg.asset(
    name="dso_sync_sec_rolledomaene",
    group_name="dso_sync_security",
    compute_kind="mssql",
    deps=["populate_security", "dso_infra_security"],
    tags={"domain": "security", "layer": "dso_sync", "type": "security"},
    description="Sync sec.RolleDomaene from staging to DSO database",
)
def dso_sync_sec_rolledomaene(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "sec_rolledomaene",
        SECURITY_SYNC_CONFIGS,
    )


@dg.asset(
    name="dso_sync_sec_rborganisation",
    group_name="dso_sync_security",
    compute_kind="mssql",
    deps=["populate_security", "dso_infra_security"],
    tags={"domain": "security", "layer": "dso_sync", "type": "security"},
    description="Sync sec.RBOrganisation from staging to DSO database",
)
def dso_sync_sec_rborganisation(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "sec_rborganisation",
        SECURITY_SYNC_CONFIGS,
    )


all_dso_sync_sec_assets = [
    dso_sync_sec_rolledomaene,
    dso_sync_sec_rborganisation,
]

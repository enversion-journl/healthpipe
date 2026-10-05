# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR: Organization extraction asset (unpartitioned).

Extracts the full Organization flat view from Aidbox sof schema and MERGEs
into fhir.Organization. This runs as part of the organisation_cs_job after
fhir_organization_sor has posted all SOR organizations to the FHIR server.

Unlike other SoF assets (Patient, Encounter, etc.) which are partitioned
daily/monthly because their upstream IFDB data is incremental, Organization
is loaded from a full SOR snapshot and is therefore unpartitioned.
"""

import dagster as dg
from core.etl_thresholds import SoftDeleteThresholdConfig
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.sql_on_fhir.sof_utils import (
    SOF_ASSET_RETRY_POLICY,
    SOF_RESOURCE_CONFIGS,
    extract_and_merge_sof_view,
)

_CFG = SOF_RESOURCE_CONFIGS["organization_view"]


@dg.asset(
    name="sof_organization",
    group_name="fhir_organization_sor",
    compute_kind="postgres",
    deps=["fhir_sof_infrastructure", "fhir_organization_sor"],
    tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
    retry_policy=SOF_ASSET_RETRY_POLICY,
    description=(
        "Extract full Organization flat view from Aidbox sof schema "
        "and MERGE into fhir.Organization (unpartitioned, full refresh)."
    ),
)
def sof_organization(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
    config: SoftDeleteThresholdConfig,
) -> dg.MaterializeResult:
    row_count, rows_affected = extract_and_merge_sof_view(
        fhir_database,
        mssql_target_staging,
        _CFG["view_name"],
        _CFG["resource_table"],
        _CFG["id_column"],
        _CFG["mssql_table"],
        _CFG["primary_keys"],
        context=context,
        soft_delete_threshold=config.soft_delete_threshold,
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "rows_affected": rows_affected,
            "source_view": f"sof.{_CFG['view_name']}",
            "target_table": f"fhir.{_CFG['mssql_table']}",
        }
    )

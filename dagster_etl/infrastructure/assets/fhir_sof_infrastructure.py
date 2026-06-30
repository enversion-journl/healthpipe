# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR SQL on FHIR (SOF) Infrastructure Assets

Creates the infrastructure required for the SOF pipeline:
1. fhir_sof_infrastructure -- ViewDefinitions on Aidbox (sof schema)
2. fhir_sof_mssql_tables   -- Target fhir.* tables on MSSQL (handled by staging_infra_fhir)

The fhir_sof_infrastructure asset requires the FHIR server resource and ensures
all ViewDefinition JSON files are PUT to Aidbox and materialized as SQL views
in the 'sof' schema.
"""

import dagster as dg
from core.resources import FHIRServerResource
from DSA.FHIR.utils.sql_on_fhir.sof_utils import ensure_view_definition, load_view_definitions


@dg.asset(
    name="fhir_sof_infrastructure",
    group_name="staging_infrastructure",
    compute_kind="fhir",
    # Gate the entire SOF chain on the staging MSSQL target tables + their
    # migrations. Every sof_* asset depends on this one, so this single dep
    # guarantees the fhir.* target tables (incl. schema migrations such as the
    # item_key occurrence-grain) are current before any SOF MERGE runs — a
    # stale target column/PK would otherwise fail the MERGE at runtime.
    deps=["staging_infra_fhir"],
    tags={"domain": "fhir", "layer": "staging", "source": "sof", "type": "infrastructure"},
    description="Ensure all SQL on FHIR ViewDefinitions exist and are materialized in Aidbox",
)
def fhir_sof_infrastructure(
    context: dg.AssetExecutionContext,
    fhir_server: FHIRServerResource,
) -> dg.MaterializeResult:
    """
    Load all ViewDefinition JSON files and ensure each one is created
    and materialized as a SQL view in the Aidbox 'sof' schema.
    """
    view_defs = load_view_definitions()
    context.log.info(f"Found {len(view_defs)} ViewDefinition JSON files")

    created = []
    for view_def in view_defs:
        name = view_def["name"]
        context.log.info(f"Ensuring ViewDefinition '{name}' exists and is materialized...")
        ensure_view_definition(fhir_server, view_def, context)
        created.append(name)

    context.log.info(f"All {len(created)} ViewDefinitions are ready")
    return dg.MaterializeResult(
        metadata={
            "view_definitions_count": len(created),
            "view_definitions": ", ".join(created),
        }
    )

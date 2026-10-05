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

The fhir_sof_infrastructure asset ensures all ViewDefinition JSON files are
PUT to Aidbox and materialized as SQL views in the 'sof' schema — but skips
the PUT + $materialize when the server already holds an identical
ViewDefinition AND the sof view exists. Re-creating an identical plain VIEW
is a no-op data-wise, yet CREATE OR REPLACE VIEW queues behind any open
reader of the view: measured 2026-08-26, $materialize of
questionnaire_response_view spent 25 minutes waiting for the concurrent
SOF extract's server-side cursor on the same view. Skipping unchanged
definitions removes that lock wait entirely.
"""

import dagster as dg
from core.resources import FHIRDatabaseResource, FHIRServerResource
from DSA.FHIR.utils.sql_on_fhir.sof_utils import (
    ensure_view_definition,
    existing_sof_view_names,
    load_view_definitions,
    server_view_definition_matches,
)


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
    description=(
        "Ensure all SQL on FHIR ViewDefinitions exist and are materialized in "
        "Aidbox (unchanged definitions with an existing sof view are skipped)."
    ),
)
def fhir_sof_infrastructure(
    context: dg.AssetExecutionContext,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """
    Load all ViewDefinition JSON files and ensure each one is created
    and materialized as a SQL view in the Aidbox 'sof' schema.
    """
    view_defs = load_view_definitions()
    context.log.info(f"Found {len(view_defs)} ViewDefinition JSON files")

    existing_views = existing_sof_view_names(fhir_database)

    ensured: list[str] = []
    skipped: list[str] = []
    for view_def in view_defs:
        name = view_def["name"]
        if name in existing_views and server_view_definition_matches(
            fhir_server, view_def, context
        ):
            context.log.info(f"ViewDefinition '{name}' unchanged and sof view exists — skipping.")
            skipped.append(name)
            continue
        context.log.info(f"Ensuring ViewDefinition '{name}' exists and is materialized...")
        ensure_view_definition(fhir_server, view_def, context)
        ensured.append(name)

    context.log.info(
        f"All {len(view_defs)} ViewDefinitions are ready "
        f"({len(ensured)} materialized, {len(skipped)} unchanged/skipped)"
    )
    return dg.MaterializeResult(
        metadata={
            "view_definitions_count": len(view_defs),
            "materialized_count": len(ensured),
            "skipped_unchanged_count": len(skipped),
            "materialized": ", ".join(ensured) or "(none)",
            "view_definitions": ", ".join(v["name"] for v in view_defs),
        }
    )

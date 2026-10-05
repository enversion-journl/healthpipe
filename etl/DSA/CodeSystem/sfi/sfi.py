# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
CodeSystem Asset: SFI (Sundhedsfagligt Indhold)

Dagster asset definition for the SFI CodeSystem.  All pure catalog logic
(SfiElement, extract_sfi_elements, build_sfi_codesystem, etc.) lives in
``catalog.py`` so FHIR pipeline modules can import it without triggering
Dagster asset registration.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.CodeSystem.sfi.catalog import (  # noqa: F401 — re-export for backward compat
    SfiElement,
    build_sfi_codesystem,
    extract_sfi_elements,
    merge_sfi_catalog_from_datapoints,
    sfi_datapoint_to_element,
    summarize_sfi_conflicts,
)
from DSA.CodeSystem.utils import codesystem_table_name
from DSA.FHIR.utils.ifdb.sfi_contract import (  # noqa: F401 — re-export for tests
    SFI_SYSTEM,
    generate_sfi_code,
)


@dg.asset(
    name="cs_sfi",
    group_name="codesystem",
    compute_kind="mssql",
    deps=["staging_infra_codesystem"],
    tags={"domain": "codesystem", "layer": "codesystem", "module": "sfi"},
    description=(
        "SFI codesystem verification marker. The actual codesystem.sfi_* "
        "tables are populated as a side-effect of the AKTIVITETSRESULTAT "
        "FHIR pipeline (no separate Oracle scan needed). This asset verifies "
        "the tables exist and reports row counts."
    ),
)
def cs_sfi(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Verify SFI CodeSystem tables are populated on staging.

    This is a VERIFICATION / GRAPH-MARKER asset, not a builder. It does NOT
    produce the SFI catalog — the heavy lifting (XML parsing, SFI extraction,
    MERGE) happens inside ``_run_aktivitetsresultat_pipeline_chunked`` as a
    side-effect of the existing XML processing (see
    ``merge_sfi_catalog_from_datapoints``). cs_sfi only reads row counts so
    downstream DSO codesystem-sync assets have a deterministic dependency on
    "the SFI catalog has been materialised". Keeping it as a stable asset key
    (rather than an asset check) preserves that downstream dependency edge; a
    future cleanup could convert it to an asset check if the DSO graph is
    reworked to depend on the real producer instead.
    """
    counts: dict[str, int] = {}
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for suffix in ("concept", "properties", "hierarchy"):
                cursor.execute(
                    f"SELECT COUNT(*) FROM [codesystem].[{codesystem_table_name('sfi', suffix)}]"
                )
                row = cursor.fetchone()
                counts[f"sfi_{suffix}_rows"] = row[0] if row else 0
        finally:
            cursor.close()

    context.log.info(
        "cs_sfi verify: concept=%d, properties=%d, hierarchy=%d",
        counts["sfi_concept_rows"],
        counts["sfi_properties_rows"],
        counts["sfi_hierarchy_rows"],
    )

    return dg.MaterializeResult(metadata={k: dg.MetadataValue.int(v) for k, v in counts.items()})

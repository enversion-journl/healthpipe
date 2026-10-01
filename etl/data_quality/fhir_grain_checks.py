# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Cross-tier grain consistency checks for FHIR resources.

For each configured resource this module generates a Dagster asset that counts
distinct grain-key values across the four pipeline tiers:

    IFDB Oracle  ->  FHIR Server (Aidbox PG)  ->  DSA MSSQL  ->  DSO MSSQL

A mismatch in distinct counts between adjacent tiers signals data loss or
duplication.  The check never fails the asset; it logs warnings and exposes
structured ``MaterializeResult`` metadata for alerting.

Adding a new resource requires only a ``GrainCheckConfig`` entry in
``GRAIN_CHECK_CONFIGS`` -- no new asset code.
"""

import json
from typing import Any

import dagster as dg
from core.resources import FHIRDatabaseResource, IFDBOracleResource, MSSQLResource
from data_quality.models import GrainCheckConfig, TierCounts
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
)

# =============================================================================
# Resource Configurations
# =============================================================================

GRAIN_CHECK_CONFIGS: list[GrainCheckConfig] = [
    GrainCheckConfig(
        resource_name="Patient",
        ifdb_table="IFDB.PATIENT_OPLYSNINGER_COLUMNA",
        ifdb_grain_column="PATIENT_ID",
        fhir_pg_table="patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        mssql_table="Patient",
        mssql_grain_column="ifdb_patient_id",
    ),
    GrainCheckConfig(
        resource_name="Encounter",
        ifdb_table="IFDB.ENCOUNTER",
        ifdb_grain_column="OBJECT_ID",
        fhir_pg_table="encounter",
        identifier_system=SYSTEM_IFDB_OBJECT_ID,
        mssql_table="Encounter",
        mssql_grain_column="identifier_value",
    ),
    GrainCheckConfig(
        resource_name="EpisodeOfCare",
        ifdb_table="IFDB.EPISODE_OF_CARE",
        ifdb_grain_column="OBJECT_ID",
        fhir_pg_table="episodeofcare",
        identifier_system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
        mssql_table="EpisodeOfCare",
        mssql_grain_column="identifier_value",
    ),
]

# =============================================================================
# Tier Query Helpers
# =============================================================================


def _count_ifdb(
    ifdb_oracle: IFDBOracleResource,
    config: GrainCheckConfig,
) -> TierCounts:
    """Count total rows and distinct grain keys in IFDB Oracle."""
    query = (
        f"SELECT COUNT(*), COUNT(DISTINCT {config.ifdb_grain_column}) " f"FROM {config.ifdb_table}"
    )
    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            total, distinct = cursor.fetchone()
        finally:
            cursor.close()
    return TierCounts(total_rows=int(total), distinct_grain_keys=int(distinct))


def _count_fhir_server(
    fhir_database: FHIRDatabaseResource,
    config: GrainCheckConfig,
) -> TierCounts:
    """Count total resources and distinct identifier values in Aidbox PostgreSQL.

    Uses the proven ``resource @>`` containment pattern from the existing
    ``_get_fhir_patient_ids`` helper in the Patient FHIR pipeline.
    """
    containment_filter = json.dumps({"identifier": [{"system": config.identifier_system}]})

    total_query = f"SELECT COUNT(*) FROM {config.fhir_pg_table}"

    distinct_query = f"""
        SELECT COUNT(DISTINCT identifier_elem->>'value')
        FROM {config.fhir_pg_table} r,
             jsonb_array_elements(
                 COALESCE(r.resource->'identifier', '[]'::jsonb)
             ) AS identifier_elem
        WHERE r.resource @> %s::jsonb
          AND identifier_elem->>'system' = %s
          AND identifier_elem->>'value' IS NOT NULL
    """

    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(total_query)
            total = cur.fetchone()[0]

            cur.execute(distinct_query, [containment_filter, config.identifier_system])
            distinct = cur.fetchone()[0]

    return TierCounts(total_rows=int(total), distinct_grain_keys=int(distinct))


def _count_mssql(
    mssql_resource: MSSQLResource,
    config: GrainCheckConfig,
) -> TierCounts:
    """Count total rows and distinct grain values in an MSSQL ``[fhir].*`` table."""
    query = (
        f"SELECT COUNT(*), COUNT(DISTINCT [{config.mssql_grain_column}]) "
        f"FROM [fhir].[{config.mssql_table}]"
    )
    with mssql_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            total, distinct = cursor.fetchone()
        finally:
            cursor.close()
    return TierCounts(total_rows=int(total), distinct_grain_keys=int(distinct))


# =============================================================================
# Result Builder
# =============================================================================


def _build_metadata(
    config: GrainCheckConfig,
    ifdb: TierCounts,
    fhir: TierCounts,
    dsa: TierCounts,
    dso: TierCounts,
) -> dict[str, Any]:
    """Assemble structured metadata dict for MaterializeResult."""
    ifdb_vs_fhir = ifdb.distinct_grain_keys - fhir.distinct_grain_keys
    fhir_vs_dsa = fhir.distinct_grain_keys - dsa.distinct_grain_keys
    dsa_vs_dso = dsa.distinct_grain_keys - dso.distinct_grain_keys

    passed = ifdb_vs_fhir == 0 and fhir_vs_dsa == 0 and dsa_vs_dso == 0

    return {
        "resource": dg.MetadataValue.text(config.resource_name),
        "passed": dg.MetadataValue.bool(passed),
        "ifdb_total_rows": dg.MetadataValue.int(ifdb.total_rows),
        "ifdb_distinct": dg.MetadataValue.int(ifdb.distinct_grain_keys),
        "ifdb_duplicates": dg.MetadataValue.int(ifdb.duplicate_count),
        "fhir_total_rows": dg.MetadataValue.int(fhir.total_rows),
        "fhir_distinct": dg.MetadataValue.int(fhir.distinct_grain_keys),
        "fhir_duplicates": dg.MetadataValue.int(fhir.duplicate_count),
        "dsa_total_rows": dg.MetadataValue.int(dsa.total_rows),
        "dsa_distinct": dg.MetadataValue.int(dsa.distinct_grain_keys),
        "dsa_duplicates": dg.MetadataValue.int(dsa.duplicate_count),
        "dso_total_rows": dg.MetadataValue.int(dso.total_rows),
        "dso_distinct": dg.MetadataValue.int(dso.distinct_grain_keys),
        "dso_duplicates": dg.MetadataValue.int(dso.duplicate_count),
        "diff_ifdb_vs_fhir": dg.MetadataValue.int(ifdb_vs_fhir),
        "diff_fhir_vs_dsa": dg.MetadataValue.int(fhir_vs_dsa),
        "diff_dsa_vs_dso": dg.MetadataValue.int(dsa_vs_dso),
    }


# =============================================================================
# Asset Factory
# =============================================================================


def _build_grain_check_asset(config: GrainCheckConfig) -> dg.AssetsDefinition:
    """Generate a Dagster asset that checks grain consistency for *config*."""

    @dg.asset(
        name=f"dq_grain_{config.resource_name.lower()}",
        group_name="data_quality",
        compute_kind="multi_db",
        description=(
            f"Cross-tier grain consistency check for {config.resource_name}. "
            f"Compares distinct {config.ifdb_grain_column} counts across "
            f"IFDB -> FHIR Server -> DSA -> DSO."
        ),
    )
    def _check(
        context: dg.AssetExecutionContext,
        ifdb_oracle: IFDBOracleResource,
        fhir_database: FHIRDatabaseResource,
        mssql_target_staging: MSSQLResource,
        mssql_target_dso: MSSQLResource,
    ) -> dg.MaterializeResult:
        context.log.info(f"Grain check: {config.resource_name}")

        context.log.info(f"  Querying IFDB ({config.ifdb_table}) ...")
        ifdb = _count_ifdb(ifdb_oracle, config)
        context.log.info(
            f"  IFDB: {ifdb.total_rows:,} rows, "
            f"{ifdb.distinct_grain_keys:,} distinct, "
            f"{ifdb.duplicate_count:,} duplicates"
        )

        context.log.info(f"  Querying FHIR server ({config.fhir_pg_table}) ...")
        fhir = _count_fhir_server(fhir_database, config)
        context.log.info(
            f"  FHIR: {fhir.total_rows:,} rows, "
            f"{fhir.distinct_grain_keys:,} distinct, "
            f"{fhir.duplicate_count:,} duplicates"
        )

        context.log.info(f"  Querying DSA ([fhir].[{config.mssql_table}]) ...")
        dsa = _count_mssql(mssql_target_staging, config)
        context.log.info(
            f"  DSA: {dsa.total_rows:,} rows, "
            f"{dsa.distinct_grain_keys:,} distinct, "
            f"{dsa.duplicate_count:,} duplicates"
        )

        context.log.info(f"  Querying DSO ([fhir].[{config.mssql_table}]) ...")
        dso = _count_mssql(mssql_target_dso, config)
        context.log.info(
            f"  DSO: {dso.total_rows:,} rows, "
            f"{dso.distinct_grain_keys:,} distinct, "
            f"{dso.duplicate_count:,} duplicates"
        )

        metadata = _build_metadata(config, ifdb, fhir, dsa, dso)

        ifdb_vs_fhir = ifdb.distinct_grain_keys - fhir.distinct_grain_keys
        fhir_vs_dsa = fhir.distinct_grain_keys - dsa.distinct_grain_keys
        dsa_vs_dso = dsa.distinct_grain_keys - dso.distinct_grain_keys

        if ifdb_vs_fhir != 0:
            context.log.warning(
                f"  MISMATCH IFDB->FHIR: {ifdb_vs_fhir:+,} "
                f"({ifdb.distinct_grain_keys:,} vs {fhir.distinct_grain_keys:,})"
            )
        if fhir_vs_dsa != 0:
            context.log.warning(
                f"  MISMATCH FHIR->DSA: {fhir_vs_dsa:+,} "
                f"({fhir.distinct_grain_keys:,} vs {dsa.distinct_grain_keys:,})"
            )
        if dsa_vs_dso != 0:
            context.log.warning(
                f"  MISMATCH DSA->DSO: {dsa_vs_dso:+,} "
                f"({dsa.distinct_grain_keys:,} vs {dso.distinct_grain_keys:,})"
            )

        if ifdb_vs_fhir == 0 and fhir_vs_dsa == 0 and dsa_vs_dso == 0:
            context.log.info(
                f"  PASSED: all tiers consistent at "
                f"{ifdb.distinct_grain_keys:,} distinct grain keys"
            )

        return dg.MaterializeResult(metadata=metadata)

    return _check


# =============================================================================
# Generated Assets
# =============================================================================

grain_check_assets: list[dg.AssetsDefinition] = [
    _build_grain_check_asset(cfg) for cfg in GRAIN_CHECK_CONFIGS
]

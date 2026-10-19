# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared orchestration for sources that supply Patient CPRs; ``patient_service`` builds and writes.

``cpr_sql`` selects a source's distinct CPRs as one column; it may be a callable resolved per
run, and when it returns None the run is skipped.
"""

from collections.abc import Callable

import dagster as dg
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.Patient.patient_service import ensure_patients
from pydantic import Field


class CprStubConfig(dg.Config):
    """Run config shared by the source assets."""

    max_create: int | None = Field(
        default=None,
        description=(
            "Create at most this many patients this run, lowest CPR first. "
            "Writing to Aidbox is not cheaply reversible, so a first run "
            "against a fresh environment can be capped to verify the shape. "
            "Leave unset to create every missing patient; the remainder is "
            "created by running again — already-created CPRs resolve against "
            "Aidbox and are skipped, never posted twice."
        ),
    )


# Temp tables give the tier join real row counts; arc statistics lag a fresh archive merge.
TIER_STATEMENTS = (
    "DROP TABLE IF EXISTS #source_cpr",
    "DROP TABLE IF EXISTS #ref_cpr",
    "DROP TABLE IF EXISTS #fhir_cpr",
    "SELECT cpr INTO #source_cpr FROM ({cpr_sql}) AS source_cpr(cpr)",
    "CREATE TABLE #ref_cpr (cpr NVARCHAR(20) COLLATE DATABASE_DEFAULT NOT NULL PRIMARY KEY)",
    """INSERT INTO #ref_cpr (cpr)
    SELECT DISTINCT cpr FROM [ref].[PatientToBorger]
    WHERE dw_is_deleted = 0 AND cpr IS NOT NULL""",
    "CREATE TABLE #fhir_cpr (ssn NVARCHAR(20) COLLATE DATABASE_DEFAULT NOT NULL PRIMARY KEY)",
    """INSERT INTO #fhir_cpr (ssn)
    SELECT patient_ssn FROM [fhir].[Patient] WHERE patient_ssn IS NOT NULL
    UNION
    SELECT previous_ssn FROM [fhir].[Patient] WHERE previous_ssn IS NOT NULL""",
)

TIER_SQL = """
SELECT s.cpr,
       CASE WHEN r.cpr IS NOT NULL THEN 'A'
            WHEN f.ssn IS NOT NULL THEN 'B'
            ELSE 'C' END AS tier
FROM #source_cpr s
LEFT JOIN #ref_cpr  r ON r.cpr = s.cpr
LEFT JOIN #fhir_cpr f ON f.ssn = s.cpr
"""


def classify_cprs(mssql: MSSQLResource, cpr_sql: str) -> dict[str, list[str]]:
    """Split the CPRs ``cpr_sql`` selects into tiers A, B and C."""
    tiers: dict[str, list[str]] = {"A": [], "B": [], "C": []}
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for statement in TIER_STATEMENTS:
                cursor.execute(statement.format(cpr_sql=cpr_sql))
            cursor.execute(TIER_SQL)
            for cpr, tier in cursor.fetchall():
                tiers[tier].append(cpr)
        finally:
            cursor.close()
    return tiers


def post_missing_patients(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    cprs: list[str],
    *,
    stub_source: str,
    source_name: str,
) -> tuple[int, int, int]:
    """Delegate to the Patient entry point; return written, failed and existing (failed writes raise)."""
    result = ensure_patients(
        context,
        mssql,
        fhir_database,
        cprs=cprs,
        stub_source=stub_source,
        source_name=source_name,
    )
    if result.existing:
        context.log.warning(
            f"{result.existing} CPR(s) already exist in Aidbox but not in fhir.Patient — "
            f"left untouched. Run sof_patient_dfm to project them; if they are still "
            f"missing after mode=full_load, the gap is in the Patient chain."
        )
    return result.written, 0, result.existing


def build_cpr_stub_asset(
    *,
    name: str,
    group_name: str,
    deps: list[str],
    tags: dict[str, str],
    description: str,
    cpr_sql: str | Callable[[dg.AssetExecutionContext, MSSQLResource], str | None],
    stub_source: str,
    source_label: str,
    source_name: str,
) -> dg.AssetsDefinition:
    """Build one source asset; ``stub_source`` is the Patient source tag the builder records."""

    @dg.asset(
        name=name,
        group_name=group_name,
        compute_kind="fhir",
        deps=deps,
        tags=tags,
        description=description,
    )
    def _asset(
        context: dg.AssetExecutionContext,
        config: CprStubConfig,
        mssql_target_staging: MSSQLResource,
        fhir_database: FHIRDatabaseResource,
    ) -> dg.MaterializeResult:
        resolved_sql = cpr_sql(context, mssql_target_staging) if callable(cpr_sql) else cpr_sql
        if resolved_sql is None:
            context.log.info(f"{source_label}: nothing to classify this run")
            return dg.MaterializeResult(
                metadata={"skipped": dg.MetadataValue.text("nothing new this run")}
            )
        tiers = classify_cprs(mssql_target_staging, resolved_sql)
        a, b, c = len(tiers["A"]), len(tiers["B"]), len(tiers["C"])
        total = a + b + c

        context.log.info(
            f"{source_label} CPRs: {total} distinct — tier A (resolves) {a}, "
            f"tier B (needs ref_PatientToBorger) {b}, tier C (create) {c}"
        )
        if b:
            context.log.warning(
                f"{b} CPR(s) exist in fhir.Patient without a ref.PatientToBorger row. "
                f"Run ref_PatientToBorger (mode=full_load if a delta run skipped them) "
                f"— do not insert into that table from here."
            )

        to_create = sorted(tiers["C"])
        if config.max_create is not None and len(to_create) > config.max_create:
            context.log.warning(
                f"CAPPED to {config.max_create} of {c} missing patients this run; "
                f"the remainder is created by running again."
            )
            to_create = to_create[: config.max_create]

        malformed = sum(1 for cpr in to_create if not (len(cpr) == 10 and cpr.isdigit()))
        if malformed:
            context.log.warning(
                f"{malformed} of {len(to_create)} CPR(s) to create are not 10 digits. "
                f"Created anyway by decision; a signal, not a rejection."
            )

        posted = failed = skipped_existing = 0
        if to_create:
            posted, failed, skipped_existing = post_missing_patients(
                context,
                mssql_target_staging,
                fhir_database,
                to_create,
                stub_source=stub_source,
                source_name=source_name,
            )
        else:
            context.log.info("No unknown CPRs — nothing to create.")

        return dg.MaterializeResult(
            metadata={
                "cpr_total": dg.MetadataValue.int(total),
                "tier_a_resolves": dg.MetadataValue.int(a),
                "tier_b_missing_ref_row": dg.MetadataValue.int(b),
                "tier_c_missing": dg.MetadataValue.int(c),
                "attempted_this_run": dg.MetadataValue.int(len(to_create)),
                "already_in_aidbox": dg.MetadataValue.int(skipped_existing),
                "posted": dg.MetadataValue.int(posted),
                "failed": dg.MetadataValue.int(failed),
                "malformed_cpr": dg.MetadataValue.int(malformed),
                "source_tag": dg.MetadataValue.text(stub_source),
            }
        )

    return _asset

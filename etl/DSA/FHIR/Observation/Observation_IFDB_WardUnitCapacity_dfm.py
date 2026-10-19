# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Observation (sengeenhed kapacitet) asset using DFM.

One Observation per archive row from SENGEENHED_DISPONIBLE / _NORMEREDE.
subject is the sengeenhed Location. ANTAL = 0 is a valid value. An open
period (TIL NULL) omits effectivePeriod.end.

Own DFM checkpoint (fhir_server / Observation_WardUnitCapacity). Not a
producer of fhir.Observation — that grain is SFI-only.
"""

from datetime import UTC, datetime
from typing import Any

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from DSA.FHIR.utils.ifdb.fhir_posting import prepare_resources_for_upsert
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.ifdb.transforms import is_true_flag, normalize_oracle_id
from DSA.FHIR.utils.ifdb.ward_unit_bed import (
    KAPACITET_TYPE_DISPONIBLE,
    KAPACITET_TYPE_NORMEREDE,
    archive_datetime_to_fhir,
    build_capacity_identifier_value,
    parse_antal,
)
from DSA.FHIR.utils.reference_resolver import ReferenceFieldConfig
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.observation import Observation
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference
from utils.fhir.identifier_systems import (
    CODE_OBSERVATION_CATEGORY_BED_CAPACITY,
    SYSTEM_IFDB_SENGEENHED,
    SYSTEM_IFDB_SENGEENHED_KAPACITET,
    SYSTEM_RSD_OBSERVATION_CATEGORY,
    SYSTEM_RSD_WARD_UNIT_CAPACITY_TYPE,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

BATCH_SIZE = 10_000

_KAPACITET_SOURCES: tuple[tuple[str, str], ...] = (
    ("arc.ifdb_sengeenhed_disponible", KAPACITET_TYPE_DISPONIBLE),
    ("arc.ifdb_sengeenhed_normerede", KAPACITET_TYPE_NORMEREDE),
)

KAPACITET_REFERENCE_CONFIGS = [
    ReferenceFieldConfig(
        field_name="subject",
        target_resource_type="Location",
        identifier_system=SYSTEM_IFDB_SENGEENHED,
        is_required=False,
        description="WardUnit Location from SENGEENHED_ID",
    ),
]


def _transform_capacity_to_fhir(
    rows: list[dict[str, Any]],
    context: dg.AssetExecutionContext,
) -> list[Observation]:
    """Transform disponible/normerede archive rows to Observation resources."""
    observations: list[Observation] = []
    skipped = 0

    for row in rows:
        kapacitet_type = row.get("kapacitet_type")
        sengeenhed_id = normalize_oracle_id(row.get("SENGEENHED_ID"))
        fra_fhir = archive_datetime_to_fhir(row.get("FRA"))
        count = parse_antal(row.get("ANTAL"))
        if not kapacitet_type or not sengeenhed_id or fra_fhir is None or count is None:
            skipped += 1
            continue

        til_fhir = archive_datetime_to_fhir(row.get("TIL"))
        deleted = 1 if is_true_flag(row.get("dw_is_deleted")) else 0
        identifier_value = build_capacity_identifier_value(
            kapacitet_type, sengeenhed_id, row.get("FRA")
        )

        observations.append(
            Observation(
                identifier=[
                    Identifier(
                        system=SYSTEM_IFDB_SENGEENHED_KAPACITET,
                        value=identifier_value,
                    )
                ],
                status="final",
                category=[
                    CodeableConcept(
                        coding=[
                            Coding(
                                system=SYSTEM_RSD_OBSERVATION_CATEGORY,
                                code=CODE_OBSERVATION_CATEGORY_BED_CAPACITY,
                            )
                        ]
                    )
                ],
                code=CodeableConcept(
                    coding=[
                        Coding(
                            system=SYSTEM_RSD_WARD_UNIT_CAPACITY_TYPE,
                            code=kapacitet_type,
                            display=kapacitet_type,
                        )
                    ]
                ),
                subject=Reference(
                    type="Location",
                    identifier=Identifier(
                        system=SYSTEM_IFDB_SENGEENHED,
                        value=sengeenhed_id,
                    ),
                ),
                effectivePeriod=Period(start=fra_fhir, end=til_fhir),
                valueInteger=count,
                extension=[build_is_deleted_extension(deleted)],
            )
        )

    if skipped:
        context.log.warning(
            f"Skipped {skipped} kapacitet rows missing type/SENGEENHED_ID/FRA/ANTAL"
        )
    return observations


def _filter_unresolved_subjects(
    observations: list[Observation],
    context: dg.AssetExecutionContext,
    counters: dict[str, int] | None = None,
) -> list[Observation]:
    """Drop Observations whose sengeenhed Location subject did not resolve.

    ``counters`` carries the drop count out to the asset's metadata: with a
    half-built fhir.Sengeenhed the capacity would otherwise disappear with no
    trace but a log line.
    """
    kept: list[Observation] = []
    dropped = 0
    for obs in observations:
        if obs.subject and getattr(obs.subject, "id", None):
            kept.append(obs)
            continue
        dropped += 1
        subject_id = "unknown"
        if obs.subject and getattr(obs.subject, "identifier", None):
            subject_id = str(obs.subject.identifier.value or "unknown")
        context.log.warning(
            f"Could not resolve sengeenhed Location for Observation subject {subject_id}"
        )
    if dropped:
        context.log.warning(f"Dropped {dropped} kapacitet Observations with unresolved subject")
    if counters is not None:
        counters["unresolved_subject_dropped"] = (
            counters.get("unresolved_subject_dropped", 0) + dropped
        )
    return kept


def _post_capacity_to_fhir(
    context: dg.AssetExecutionContext,
    observations: list[Observation],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    counters: dict[str, int] | None = None,
) -> int:
    """Resolve subject Locations and upsert capacity Observations."""
    logger = dg.get_dagster_logger()
    if not observations:
        return 0

    observations = prepare_resources_for_upsert(
        resources=observations,
        fhir_database=fhir_database,
        resource_type="Observation",
        preferred_id_systems=[SYSTEM_IFDB_SENGEENHED_KAPACITET],
        reference_configs=KAPACITET_REFERENCE_CONFIGS,
        logger=logger,
    )
    observations = _filter_unresolved_subjects(observations, context, counters=counters)
    if not observations:
        return 0

    successes, failures, failed_ids = fhir_database.batch_upsert_resources(
        resources=observations,
        resource_type="Observation",
    )
    if failures > 0:
        raise dg.Failure(
            description=(
                f"Failed to post {failures} of {len(observations)} kapacitet "
                f"Observation resources. Failed IDs: {failed_ids[:10]}"
            )
        )
    context.log.info(f"Posted {successes} kapacitet Observations (extracted {extracted_count})")
    return successes


@dg.asset(
    name="fhir_server_observation_ward_unit_capacity_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=[
        "arc_sengeenhed_disponible_ifdb",
        "arc_sengeenhed_normerede_ifdb",
        "fhir_server_location_ward_unit_dfm",
    ],
    tags={
        TAG_CHUNK_SIZE: "500",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Fetch sengeenhed capacity periods, transform to FHIR Observation "
        "(subject = sengeenhed Location), and post. Checkpoint owner for "
        "fhir_server.Observation_WardUnitCapacity."
    ),
)
def fhir_server_observation_ward_unit_capacity_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Fetch both capacity tables → transform → post (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "Observation_WardUnitCapacity",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        total_fetched = 0
        total_posted = 0
        tagged_rows: list[dict[str, Any]] = []

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                for table, kapacitet_type in _KAPACITET_SOURCES:
                    query = build_fhir_archive_query(
                        table,
                        run_ctx.checkpoint,
                        config,
                        include_deleted=True,
                    )
                    context.log.info(f"Querying {table} (mode={config.mode})")
                    cursor.execute(query)
                    columns = [desc[0] for desc in cursor.description]
                    while True:
                        raw_rows = cursor.fetchmany(BATCH_SIZE)
                        if not raw_rows:
                            break
                        for raw in raw_rows:
                            row = dict(zip(columns, raw, strict=False))
                            row["kapacitet_type"] = kapacitet_type
                            tagged_rows.append(row)
                        total_fetched += len(raw_rows)
            finally:
                cursor.close()

        dropped: dict[str, int] = {}
        if tagged_rows:
            observations = _transform_capacity_to_fhir(tagged_rows, context)
            total_posted = _post_capacity_to_fhir(
                context=context,
                observations=observations,
                fhir_database=fhir_database,
                extracted_count=total_fetched,
                counters=dropped,
            )

        context.log.info(f"Done: {total_fetched} fetched, {total_posted} posted")

        new_checkpoint = advance_fhir_checkpoint(
            dfm, run_ctx, "Observation_WardUnitCapacity", config, total_fetched
        )

        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=new_checkpoint,
            rows_affected=total_posted,
        )
        return dg.MaterializeResult(
            metadata={
                "posted_count": total_posted,
                "fetched_count": total_fetched,
                # Kapacitet uden en oploest sengeenhed forsvinder. Tallet hoerer
                # i metadata, ikke kun i loggen: en tom eller halv
                # fhir.Sengeenhed viser sig praecis her.
                "unresolved_subject_dropped": dropped.get("unresolved_subject_dropped", 0),
                "mode": config.mode,
                "decorated_at": dg.MetadataValue.text(datetime.now(UTC).isoformat()),
            }
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

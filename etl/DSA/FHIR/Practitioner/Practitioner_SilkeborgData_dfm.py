# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Practitioner asset from SilkeborgData E26PERS using DFM.

One resource per person, built from that person's newest archive row. The
archive accumulates one payroll snapshot per month, so the grain differs from the
resource grain and the query picks a winner per person — see
practitioner_common.build_practitioner_query.

The source is a payroll extract: it carries no authorisation code, so no DK Core
profile is claimed, and `active` means "present in the most recent payroll
delivery", never "licensed to practise".

A CPR correction produces two resources for the person: the surviving number
active, the superseded number inactive, soft deleted and without roles. That is
deliberate — one resource holding both numbers is not representable, because the
identifier lookup and its unique index read only the first identifier per system.
"""

from datetime import date

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.Practitioner.practitioner_common import (
    SOURCE_TABLE_CODE,
    build_practitioner_query,
    cpr_gender,
    plausible_birth_date,
    split_name,
)
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from DSA.FHIR.utils.ifdb.fhir_posting import prepare_resources_for_upsert
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.humanname import HumanName
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.meta import Meta
from fhir.resources.R4B.practitioner import Practitioner
from utils.fhir.fhir_extensions import EXTENSION_SILKEBORGDATA_AFLOEST_CPR
from utils.fhir.identifier_systems import (
    SYSTEM_CPR,
    SYSTEM_SILKEBORGDATA_SOURCE_TABLE,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

BATCH_SIZE = 10_000

# meta.DFMState entity under the "fhir_server" scope. Source-suffixed on
# purpose: the key names how far THIS source has posted, and the resource type
# alone is not that. Limitation #1 already says IFDB staff must not be mapped
# "right now", which is a second Practitioner producer waiting to happen — and
# two producers sharing one row move each other's delta window. The repo settles
# this the same way elsewhere (Location_SOR / Location_Bed / Location_WardUnit,
# Condition_Diagnose / Condition_PasDiagnose, DocumentReference_Epikrise).
#
# Free to pick now, expensive later: changing it after go-live means migrating
# meta.DFMState AND a full_load, because an unknown key reads as "never ran".
_CHECKPOINT_ENTITY = "Practitioner_SilkeborgData"


def transform_practitioner_rows(
    rows: list[dict],
    logger: dg.DagsterLogManager | None = None,
) -> list[Practitioner]:
    """Build Practitioner resources from archive rows.

    Rows carry the query's `_alias_cpr` (a superseded CPR for the same person)
    and `_is_latest` (row belongs to the newest delivery).

    A corrected CPR yields TWO resources: the surviving number active, the
    superseded number inactive. The query emits both rows; this function needs no
    branch for it, because `active` follows `_is_latest` and the alias row carries
    0. The superseded number rides along on the active resource as an extension —
    never as a second SYSTEM_CPR identifier, which would make the unique index
    key depend on identifier order and could collide with the inactive resource.
    """
    resources: list[Practitioner] = []
    skipped_no_cpr = 0
    without_gender = 0
    without_birth_date = 0

    for row in rows:
        cpr = (row.get("CprNr") or "").strip()
        if not cpr:
            skipped_no_cpr += 1
            continue

        # A retired identity is soft deleted, not just inactive. active=false is a
        # statement about a person ("not currently on the payroll"), and the
        # superseded number is not a person — it is an identity that stopped
        # existing. dw_is_deleted is the flag consumers already filter on, so the
        # duplicate disappears from reports without anyone learning a new rule.
        # Both are set: they answer different questions and both are true.
        #
        # Keyed on _is_superseded and never on _is_latest: a person who simply
        # left also has _is_latest = 0, and there are some 15.000 of those.
        extensions = [build_is_deleted_extension(1 if row.get("_is_superseded") else 0)]
        alias = (row.get("_alias_cpr") or "").strip()
        if alias:
            extensions.append(Extension(url=EXTENSION_SILKEBORGDATA_AFLOEST_CPR, valueString=alias))

        practitioner = Practitioner(
            identifier=[Identifier(system=SYSTEM_CPR, value=cpr)],
            active=bool(row.get("_is_latest")),
            meta=Meta(
                tag=[Coding(system=SYSTEM_SILKEBORGDATA_SOURCE_TABLE, code=SOURCE_TABLE_CODE)]
            ),
            extension=extensions,
        )

        family, given, text = split_name(row.get("Navn"))
        if text:
            name = HumanName(text=text)
            if family:
                name.family = family
            if given:
                name.given = given
            practitioner.name = [name]

        period = row.get("Udtraeksdato")
        gender = cpr_gender(cpr)
        birth = plausible_birth_date(cpr, period if isinstance(period, date) else None)
        if gender:
            practitioner.gender = gender
        if birth:
            practitioner.birthDate = birth
        # Counted apart: the validity gate suppresses BOTH, but the age
        # plausibility bound suppresses only birthDate, so one number would
        # hide the other.
        if not gender:
            without_gender += 1
        if not birth:
            without_birth_date += 1

        resources.append(practitioner)

    if logger:
        if skipped_no_cpr:
            logger.warning(f"Skipped {skipped_no_cpr} rows without CprNr")
        if without_gender:
            logger.warning(f"{without_gender} practitioners without gender — CPR failed the gate")
        if without_birth_date:
            logger.warning(
                f"{without_birth_date} practitioners without birthDate — CPR failed the gate "
                "or the date implied an age below the plausibility bound"
            )
    return resources


@dg.asset(
    name="fhir_server_practitioner_dfm",
    group_name="fhir_silkeborgdata_dfm",
    compute_kind="fhir",
    deps=["arc_silkeborgdata_e26_pers"],
    tags={
        TAG_CHUNK_SIZE: "500",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Fetch the newest archive row per person from arc.silkeborgdata_e26_pers, "
        "transform to FHIR Practitioner, and post to the FHIR server. "
        f"Checkpoint owner for fhir_server.{_CHECKPOINT_ENTITY}."
    ),
)
def fhir_server_practitioner_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch -> transform -> post for Practitioner resources."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        _CHECKPOINT_ENTITY,
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        query = build_practitioner_query(run_ctx.checkpoint, config)
        context.log.info(f"Querying arc.silkeborgdata_e26_pers (mode={config.mode})")

        total_fetched = 0
        total_posted = 0
        batch_num = 0

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]

                while True:
                    raw_rows = cursor.fetchmany(BATCH_SIZE)
                    if not raw_rows:
                        break

                    batch_num += 1
                    batch = [dict(zip(columns, row, strict=False)) for row in raw_rows]
                    total_fetched += len(batch)

                    practitioners = transform_practitioner_rows(batch, context.log)
                    if not practitioners:
                        continue

                    practitioners = prepare_resources_for_upsert(
                        resources=practitioners,
                        fhir_database=fhir_database,
                        resource_type="Practitioner",
                        preferred_id_systems=[SYSTEM_CPR],
                        logger=context.log,
                    )
                    successes, failures, failed_ids = fhir_database.batch_upsert_resources(
                        resources=practitioners,
                        resource_type="Practitioner",
                    )
                    if failures > 0:
                        raise dg.Failure(
                            description=(
                                f"Failed to post {failures} of {len(practitioners)} "
                                f"Practitioner resources — checkpoint NOT advanced. "
                                f"Failed IDs: {failed_ids[:10]}"
                            )
                        )
                    total_posted += successes
            finally:
                cursor.close()

        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted in {batch_num} batches"
        )

        new_checkpoint = advance_fhir_checkpoint(
            dfm, run_ctx, _CHECKPOINT_ENTITY, config, total_fetched
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
                "batch_count": batch_num,
                "mode": config.mode,
                "source_table": SOURCE_TABLE_CODE,
                "target_table": f"fhir_server.{_CHECKPOINT_ENTITY}",
            }
        )

    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

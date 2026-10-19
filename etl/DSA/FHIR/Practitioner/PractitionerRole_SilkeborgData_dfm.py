# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR PractitionerRole asset from SilkeborgData E26PERS using DFM.

One resource per employment EPISODE — (Inst + Tjenestenummer + the episode's
first delivery period) — built from that episode's newest archive row. A person
can hold several concurrent employments, so this is not one resource per
practitioner; and 36 tjenestenumre have been held by more than one person over
time, so each holder gets a resource of their own rather than only the latest.

Runs after fhir_server_practitioner_dfm: the practitioner reference is resolved
by CPR, and the Practitioner must exist first.

Each source code gets its own coding.system so the codes can be told apart
downstream — filtering on system, never on array position, which FHIR does not
guarantee. Inst and Afdelingskode are organisational units with no reference
target (neither SOR nor SHAK), so they are extensions rather than roles.

Statuskode, Jobstatuskode and Statusdato are carried verbatim, untranslated: the
consumers of the flat tables know the codes from the payroll system, so a code
map would translate for an audience that does not need it. They sit beside
`active`, which means only "present in the most recent delivery" — so a consumer
who needs employment status reads the raw codes rather than over-reading the
flag. Statusdato is not mapped to period.end: that would claim to know that a
particular Statuskode means "left", which is exactly what is not known here.

Tjenestekode and Omraadekode are carried the same way and for the same reason.
Both are per-employment, which was measured rather than assumed: persons holding
several employments carry differing values across them.
"""

from datetime import date

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.Practitioner.practitioner_common import (
    SOURCE_TABLE_CODE,
    build_practitioner_role_query,
    disco_code,
    role_identifier_value,
    usable_date,
)
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from DSA.FHIR.utils.ifdb.fhir_posting import prepare_resources_for_upsert
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.reference_resolver import ReferenceFieldConfig
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.meta import Meta
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.practitionerrole import PractitionerRole
from fhir.resources.R4B.reference import Reference
from utils.fhir.fhir_extensions import (
    EXTENSION_SILKEBORGDATA_AFDELINGSKODE,
    EXTENSION_SILKEBORGDATA_INST,
    EXTENSION_SILKEBORGDATA_JOBSTATUSKODE,
    EXTENSION_SILKEBORGDATA_OMRAADEKODE,
    EXTENSION_SILKEBORGDATA_STATUSDATO,
    EXTENSION_SILKEBORGDATA_STATUSKODE,
    EXTENSION_SILKEBORGDATA_TJENESTEKODE,
)
from utils.fhir.identifier_systems import (
    SYSTEM_CPR,
    SYSTEM_SILKEBORGDATA_DISCOKODE,
    SYSTEM_SILKEBORGDATA_LEDERKODE,
    SYSTEM_SILKEBORGDATA_SOURCE_TABLE,
    SYSTEM_SILKEBORGDATA_STILLINGSKODE,
    SYSTEM_SILKEBORGDATA_TJENESTENUMMER,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

BATCH_SIZE = 10_000

# Source-suffixed for the same reason as the person asset — see
# Practitioner_SilkeborgData_dfm._CHECKPOINT_ENTITY.
_CHECKPOINT_ENTITY = "PractitionerRole_SilkeborgData"

_PRACTITIONER_REFERENCE = ReferenceFieldConfig(
    field_name="practitioner",
    target_resource_type="Practitioner",
    identifier_system=SYSTEM_CPR,
    is_required=False,
    description="Employment -> person, by CPR",
)


def _clean(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def transform_practitioner_role_rows(
    rows: list[dict],
    logger: dg.DagsterLogManager | None = None,
) -> list[PractitionerRole]:
    """Build PractitionerRole resources from archive rows."""
    resources: list[PractitionerRole] = []
    skipped_no_key = 0
    skipped_no_episode = 0
    without_period = 0

    for row in rows:
        episode_key = row.get("_episode_key")
        identifier_value = role_identifier_value(
            row.get("Inst"), row.get("Tjenestenummer"), episode_key
        )
        if not identifier_value:
            # Two failures with nothing in common: a row the source delivered
            # without its own key, and a row whose episode the query failed to
            # resolve. The second is a defect in build_practitioner_role_query,
            # so it is counted apart rather than folded into the first.
            if _clean(row.get("Inst")) and _clean(row.get("Tjenestenummer")):
                skipped_no_episode += 1
            else:
                skipped_no_key += 1
            continue

        role = PractitionerRole(
            identifier=[
                Identifier(system=SYSTEM_SILKEBORGDATA_TJENESTENUMMER, value=identifier_value)
            ],
            active=bool(row.get("_is_latest")),
            meta=Meta(
                tag=[Coding(system=SYSTEM_SILKEBORGDATA_SOURCE_TABLE, code=SOURCE_TABLE_CODE)]
            ),
        )

        cpr = _clean(row.get("_practitioner_cpr"))
        if cpr:
            role.practitioner = Reference(identifier=Identifier(system=SYSTEM_CPR, value=cpr))

        codes: list[CodeableConcept] = []
        stillingskode = _clean(row.get("Stillingskode"))
        if stillingskode:
            concept = CodeableConcept(
                coding=[Coding(system=SYSTEM_SILKEBORGDATA_STILLINGSKODE, code=stillingskode)]
            )
            # Stilling is free text and more granular than the code (2.565
            # distinct values against 328 codes), so it is the concept's text —
            # never the coding's display.
            stilling = _clean(row.get("Stilling"))
            if stilling:
                concept.text = stilling
            codes.append(concept)

        lederkode = _clean(row.get("Lederkode"))
        if lederkode:
            codes.append(
                CodeableConcept(
                    coding=[Coding(system=SYSTEM_SILKEBORGDATA_LEDERKODE, code=lederkode)]
                )
            )
        if codes:
            role.code = codes

        disco = disco_code(row.get("Discokode"))
        if disco:
            role.specialty = [
                CodeableConcept(coding=[Coding(system=SYSTEM_SILKEBORGDATA_DISCOKODE, code=disco)])
            ]

        period_date = row.get("Udtraeksdato")
        start = usable_date(
            row.get("Ansaettelsesdato"), period_date if isinstance(period_date, date) else None
        )
        if start:
            role.period = Period(start=start)
        else:
            without_period += 1

        extensions = [build_is_deleted_extension(0)]
        inst = _clean(row.get("Inst"))
        if inst:
            extensions.append(
                Extension(url=EXTENSION_SILKEBORGDATA_INST, valueCoding=Coding(code=inst))
            )
        afdelingskode = _clean(row.get("Afdelingskode"))
        if afdelingskode:
            extensions.append(
                Extension(
                    url=EXTENSION_SILKEBORGDATA_AFDELINGSKODE,
                    valueCoding=Coding(code=afdelingskode),
                )
            )
        # Untranslated payroll status codes. No warning when a value is unknown:
        # nothing maps them, so an unseen code is data, not a defect.
        statuskode = _clean(row.get("Statuskode"))
        if statuskode:
            extensions.append(
                Extension(
                    url=EXTENSION_SILKEBORGDATA_STATUSKODE,
                    valueCoding=Coding(code=statuskode),
                )
            )
        jobstatuskode = _clean(row.get("Jobstatuskode"))
        if jobstatuskode:
            extensions.append(
                Extension(
                    url=EXTENSION_SILKEBORGDATA_JOBSTATUSKODE,
                    valueCoding=Coding(code=jobstatuskode),
                )
            )
        # Without a date the status code has no time anchor. Same validity gate
        # as period.start: 1.328 rows carry 0001-01-01.
        statusdato = usable_date(
            row.get("Statusdato"), period_date if isinstance(period_date, date) else None
        )
        if statusdato:
            extensions.append(
                Extension(url=EXTENSION_SILKEBORGDATA_STATUSDATO, valueDate=statusdato)
            )
        # "0" is a value, not a blank: it is 86,8 % of Tjenestekode. Whether it
        # means "none" is unknown, so it is carried like any other code rather
        # than folded into absent — the same restraint as everywhere else here.
        tjenestekode = _clean(row.get("Tjenestekode"))
        if tjenestekode:
            extensions.append(
                Extension(
                    url=EXTENSION_SILKEBORGDATA_TJENESTEKODE,
                    valueCoding=Coding(code=tjenestekode),
                )
            )
        omraadekode = _clean(row.get("Omraadekode"))
        if omraadekode:
            extensions.append(
                Extension(
                    url=EXTENSION_SILKEBORGDATA_OMRAADEKODE,
                    valueCoding=Coding(code=omraadekode),
                )
            )
        role.extension = extensions

        resources.append(role)

    if logger:
        if skipped_no_key:
            logger.warning(f"Skipped {skipped_no_key} rows without Inst/Tjenestenummer")
        if skipped_no_episode:
            logger.warning(
                f"Skipped {skipped_no_episode} rows whose employment episode did not "
                "resolve — the episode CTE in build_practitioner_role_query should "
                "cover every archive row, so this is a defect, not source data"
            )
        if without_period:
            logger.warning(
                f"{without_period} roles without period.start — Ansaettelsesdato was a "
                "missing-marker or later than the delivery period"
            )
    return resources


@dg.asset(
    name="fhir_server_practitioner_role_dfm",
    group_name="fhir_silkeborgdata_dfm",
    compute_kind="fhir",
    deps=["arc_silkeborgdata_e26_pers", "fhir_server_practitioner_dfm"],
    tags={
        TAG_CHUNK_SIZE: "500",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Fetch the newest archive row per employment episode from "
        "arc.silkeborgdata_e26_pers, transform to FHIR PractitionerRole, and "
        "post to the FHIR server. Checkpoint owner for "
        f"fhir_server.{_CHECKPOINT_ENTITY}."
    ),
)
def fhir_server_practitioner_role_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch -> transform -> post for PractitionerRole resources."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        _CHECKPOINT_ENTITY,
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        query = build_practitioner_role_query(run_ctx.checkpoint, config)
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

                    roles = transform_practitioner_role_rows(batch, context.log)
                    if not roles:
                        continue

                    roles = prepare_resources_for_upsert(
                        resources=roles,
                        fhir_database=fhir_database,
                        resource_type="PractitionerRole",
                        preferred_id_systems=[SYSTEM_SILKEBORGDATA_TJENESTENUMMER],
                        reference_configs=[_PRACTITIONER_REFERENCE],
                        logger=context.log,
                    )
                    successes, failures, failed_ids = fhir_database.batch_upsert_resources(
                        resources=roles,
                        resource_type="PractitionerRole",
                    )
                    if failures > 0:
                        raise dg.Failure(
                            description=(
                                f"Failed to post {failures} of {len(roles)} PractitionerRole "
                                f"resources — checkpoint NOT advanced. "
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

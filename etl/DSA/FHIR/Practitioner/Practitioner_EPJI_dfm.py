# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Practitioner asset from EPJI PRACTITIONER using DFM.

One resource per EPJ account, which is the unit IFDB's staff codes refer to. One
person often holds several accounts; identifier RegionsId groups them, and the
grouping happens in the warehouse, not here. The resources are never merged with
the SilkeborgData Practitioners.

`active` is the account's state in EPJ. An inactive account is not deleted: IFDB
rows written under it still refer to it, so isDeleted follows only the archive's
dw_is_deleted, i.e. the account disappearing from EPJ.
"""

from collections import defaultdict
from collections.abc import Iterable

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.dfm_query import (
    build_fhir_archive_query,
    delta_column_ref,
    delta_lower_bound,
)
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from DSA.FHIR.utils.ifdb.fhir_posting import prepare_resources_for_upsert
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.humanname import HumanName
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.meta import Meta
from fhir.resources.R4B.practitioner import Practitioner
from utils.fhir.identifier_systems import (
    EPJI_UNMAPPED_IDENTIFIER_SYSTEMS,
    SYSTEM_EPJI_SOURCE_TABLE,
    SYSTEM_IFDB_STAFF,
    SYSTEM_REGION_REGIONSID,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension
from utils.sql_safety import safe_table_ref

BATCH_SIZE = 10_000

ARCHIVE_TABLE = "arc.epji_practitioner"
IDENTIFIER_ARCHIVE_TABLE = "arc.epji_practitioner_identifier"
SOURCE_TABLE_CODE = "epji_practitioner"

_CHECKPOINT_ENTITY = "Practitioner_EPJI"

_ACTIVE_BY_STATE = {"1": True, "0": False}


def build_practitioner_epji_query(checkpoint: str | None, config: RunModeConfig) -> str:
    """Accounts in the run window, deleted included, each with its live RegionsId.

    In delta an account is also in the window when only its identifier rows
    changed. Takes the RegionsId system as its one bound parameter.
    """
    identifier_table = safe_table_ref(IDENTIFIER_ARCHIVE_TABLE)
    identifier_predicate = None
    if config.mode == "delta" and checkpoint:
        identifier_predicate = (
            f"[PK] IN (SELECT [PRACTITIONER_PK] FROM {identifier_table} "
            f"WHERE {delta_column_ref(config)} > {delta_lower_bound(checkpoint, config)})"
        )
    windowed = build_fhir_archive_query(
        ARCHIVE_TABLE,
        checkpoint,
        config,
        include_deleted=True,
        extra_or_predicate=identifier_predicate,
    )
    return f"""
        SELECT p.[PK], p.[EXTERNAL_ID], p.[STATE], p.[NAME], p.[GIVEN_NAME],
               p.[FAMILY_NAME], p.[dw_is_deleted], r.[VALUE] AS [_regionsid]
        FROM ({windowed}) p
        LEFT JOIN {identifier_table} r
          ON r.[PRACTITIONER_PK] = p.[PK]
         AND r.[SYSTEM] = ?
         AND r.[dw_is_deleted] = 0
    """


_KNOWN_IDENTIFIER_SYSTEMS = (SYSTEM_REGION_REGIONSID, *sorted(EPJI_UNMAPPED_IDENTIFIER_SYSTEMS))


def build_unknown_identifier_systems_query() -> str:
    """Live identifier rows per system the transform does not know.

    Takes the known systems as bound parameters, in _KNOWN_IDENTIFIER_SYSTEMS order.
    """
    placeholders = ", ".join("?" for _ in _KNOWN_IDENTIFIER_SYSTEMS)
    return f"""
        SELECT [SYSTEM], COUNT_BIG(*) AS [row_count]
        FROM {safe_table_ref(IDENTIFIER_ARCHIVE_TABLE)}
        WHERE [dw_is_deleted] = 0
          AND [SYSTEM] NOT IN ({placeholders})
        GROUP BY [SYSTEM]
    """


def _stripped(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _pk_order(pk: object) -> tuple[int, str]:
    # PK is an Oracle NUMBER held as text; length first sorts it numerically.
    text = str(pk).strip()
    return len(text), text


def resolve_staff_code_owners(
    rows: Iterable[tuple[object, object, object]],
) -> tuple[dict[object, str], dict[str, int]]:
    """Split (PK, EXTERNAL_ID, dw_is_deleted) rows that share a staff code into owner and losers.

    Per code, keyed as the transform keys it, a live row beats a deleted one and the
    highest PK breaks a tie. Returns the losers as PK -> code, and the codes that more
    than one live row holds.
    """
    holders: dict[str, list[tuple[bool, tuple[int, str], object]]] = defaultdict(list)
    for pk, external_id, is_deleted in rows:
        code = _stripped(external_id)
        if code is not None:
            holders[code].append((not is_deleted, _pk_order(pk), pk))

    superseded: dict[object, str] = {}
    live_clashes: dict[str, int] = {}
    for code, code_rows in holders.items():
        if len(code_rows) < 2:
            continue
        live = sum(1 for is_live, _, _ in code_rows if is_live)
        if live > 1:
            live_clashes[code] = live
        *_, owner_pk = max(code_rows, key=lambda row: row[:2])
        superseded.update({pk: code for _, _, pk in code_rows if pk != owner_pk})
    return superseded, live_clashes


def transform_practitioner_epji_rows(
    rows: list[dict],
    logger: dg.DagsterLogManager | None = None,
) -> list[Practitioner]:
    """Build one Practitioner per archive row, keyed on EXTERNAL_ID with its case kept."""
    resources: list[Practitioner] = []
    skipped_no_external_id = 0
    unknown_state = 0

    for row in rows:
        external_id = _stripped(row.get("EXTERNAL_ID"))
        if external_id is None:
            skipped_no_external_id += 1
            continue

        identifiers = [Identifier(system=SYSTEM_IFDB_STAFF, value=external_id)]
        regionsid = _stripped(row.get("_regionsid"))
        if regionsid:
            identifiers.append(Identifier(system=SYSTEM_REGION_REGIONSID, value=regionsid))

        practitioner = Practitioner(
            identifier=identifiers,
            meta=Meta(tag=[Coding(system=SYSTEM_EPJI_SOURCE_TABLE, code=SOURCE_TABLE_CODE)]),
            extension=[build_is_deleted_extension(1 if row.get("dw_is_deleted") else 0)],
        )

        active = _ACTIVE_BY_STATE.get(_stripped(row.get("STATE")) or "")
        if active is None:
            unknown_state += 1
        else:
            practitioner.active = active

        text = _stripped(row.get("NAME"))
        family = _stripped(row.get("FAMILY_NAME"))
        given = (_stripped(row.get("GIVEN_NAME")) or "").split()
        if text or family or given:
            name = HumanName()
            if text:
                name.text = text
            if family:
                name.family = family
            if given:
                name.given = given
            practitioner.name = [name]

        resources.append(practitioner)

    if logger:
        if skipped_no_external_id:
            logger.warning(f"Skipped {skipped_no_external_id} accounts without EXTERNAL_ID")
        if unknown_state:
            logger.warning(f"{unknown_state} accounts with a STATE outside 0/1 — active left out")
    return resources


def _log_unknown_identifier_systems(cursor, logger: dg.DagsterLogManager) -> int:
    cursor.execute(build_unknown_identifier_systems_query(), _KNOWN_IDENTIFIER_SYSTEMS)
    unknown = cursor.fetchall()
    for system, count in unknown:
        logger.warning(
            f"{count} live rows in {IDENTIFIER_ARCHIVE_TABLE} with unmapped system {system!r}"
        )
    return sum(count for _, count in unknown)


def _superseded_staff_code_pks(cursor, logger: dg.DagsterLogManager) -> set[object]:
    cursor.execute(
        f"SELECT [PK], [EXTERNAL_ID], [dw_is_deleted] FROM {safe_table_ref(ARCHIVE_TABLE)}"
    )
    superseded, live_clashes = resolve_staff_code_owners(cursor.fetchall())
    if live_clashes:
        examples = ", ".join(
            f"{code!r} ({count} live rows)" for code, count in sorted(live_clashes.items())[:10]
        )
        raise dg.Failure(
            description=(
                f"{len(live_clashes)} staff codes are held by more than one live row in "
                f"{ARCHIVE_TABLE}; which account a Practitioner shows would depend on "
                f"posting order. Nothing posted, checkpoint NOT advanced. Codes: {examples}"
            )
        )
    if superseded:
        codes = ", ".join(repr(code) for code in sorted(set(superseded.values()))[:10])
        logger.warning(
            f"{len(superseded)} rows in {ARCHIVE_TABLE} hold a staff code that another row "
            f"owns and are not posted. Codes: {codes}"
        )
    return set(superseded)


@dg.asset(
    name="fhir_server_practitioner_epji_dfm",
    group_name="fhir_epji_dfm",
    compute_kind="fhir",
    deps=["arc_epji_practitioner", "arc_epji_practitioner_identifier"],
    tags={
        TAG_CHUNK_SIZE: "500",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Fetch EPJ accounts from arc.epji_practitioner with their RegionsId from "
        "arc.epji_practitioner_identifier, transform to FHIR Practitioner, and post "
        "to the FHIR server. A staff code held by several archive rows posts only "
        "its owner: a live row before a deleted one, then the highest PK. Fails "
        "before posting when more than one live row holds a code. "
        f"Checkpoint owner for fhir_server.{_CHECKPOINT_ENTITY}."
    ),
)
def fhir_server_practitioner_epji_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch -> transform -> post for EPJI Practitioner resources."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        _CHECKPOINT_ENTITY,
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        query = build_practitioner_epji_query(run_ctx.checkpoint, config)
        context.log.info(f"Querying {ARCHIVE_TABLE} (mode={config.mode})")

        total_fetched = 0
        total_posted = 0
        batch_num = 0

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                unknown_system_rows = _log_unknown_identifier_systems(cursor, context.log)
                superseded = _superseded_staff_code_pks(cursor, context.log)

                cursor.execute(query, (SYSTEM_REGION_REGIONSID,))
                columns = [desc[0] for desc in cursor.description]

                while True:
                    raw_rows = cursor.fetchmany(BATCH_SIZE)
                    if not raw_rows:
                        break

                    batch_num += 1
                    batch = [dict(zip(columns, row, strict=False)) for row in raw_rows]
                    total_fetched += len(batch)
                    batch = [row for row in batch if row["PK"] not in superseded]

                    practitioners = transform_practitioner_epji_rows(batch, context.log)
                    if not practitioners:
                        continue

                    practitioners = prepare_resources_for_upsert(
                        resources=practitioners,
                        fhir_database=fhir_database,
                        resource_type="Practitioner",
                        preferred_id_systems=[SYSTEM_IFDB_STAFF],
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
                "unknown_identifier_system_rows": unknown_system_rows,
                "superseded_staff_code_rows": len(superseded),
                "mode": config.mode,
                "source_table": ARCHIVE_TABLE,
                "target_table": f"fhir_server.{_CHECKPOINT_ENTITY}",
            }
        )

    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

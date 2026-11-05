# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Semi-automatic remediation: ids-mode re-extract + archive merge for arc gaps.

``source_archive_reconciliation`` finds IFDB merge keys that never reached the archive
(``missing_in_arc``) and captures them to ``dqa.RemediationQueue``
(``reference_column='source_archive_reconciliation'``, ``replay_entity`` = the arc subject
name, e.g. ``arc_ifdb_encounter`` -- deliberately NOT an FHIR replay entity, so
these rows never leak into the ``dqa.v_replay_*`` feeder views).

One operator action (``dqa_arc_ifdb_replay_job``) per entity:

1. read the pending keys from the queue, guarded by ``max_keys``;
2. child run(s) over ``ext_<entity>_ifdb`` (``ids`` mode, keys passed INLINE via
   ``id_filter_values`` -- the extract's filter subquery runs on ORACLE, where the
   MSSQL feeder views don't exist) + ``arc_<entity>_ifdb`` (``ids`` mode merge).
   Keys are chunked at 1000 per run (Oracle's IN-list literal cap); neither asset
   advances the DFM checkpoint (``advance_checkpoint=False``, which is also the
   ids-mode default).
3. re-check each key (does it exist in arc now?) and flip resolved keys; keys
   still missing after ``unresolvable_after`` attempts flip to ``unresolvable``.

Only the single-merge-key entities are supported (encounter, episode_of_care,
patient). Location is excluded because its merge key is composite
(``OBJECT_ID`` + ``PERIOD_START``) while the replay filters on one column; its
gaps are closed with a ``period_orchestrator`` full_load instead.

This MUTATES the arc archive (re-extracts + merges rows); operator-triggered,
never scheduled. ``dry_run`` first.
"""

from typing import cast

import dagster as dg
from dqa.checks.helpers import get_conn, split_table, sql_literal
from dqa.config import ARC_SUBJECTS
from dqa.model import DqaSubject, ReplaySpec

# The replay machinery (job resolution, config binding, in-process launch) is the
# same as the FHIR replay's -- reuse it rather than fork it.
from dqa.remediation.replay import _build_run_config, _launch, _resolve_job
from pydantic import Field

# entity (== arc subject name / ReplaySpec.entity) -> (ext asset, arc asset).
# Ext assets follow the factory naming ext_<target_table> (ext_ifdb_encounter);
# arc assets follow arc_<entity>_ifdb.
_ARC_DFM_ASSETS: dict[str, tuple[str, str]] = {
    "arc_ifdb_encounter": ("ext_ifdb_encounter", "arc_encounter_ifdb"),
    "arc_ifdb_episode_of_care": ("ext_ifdb_episode_of_care", "arc_episode_of_care_ifdb"),
    "arc_ifdb_patient": ("ext_ifdb_patient", "arc_patient_ifdb"),
    "arc_ifdb_diagnose": ("ext_ifdb_diagnose", "arc_diagnose_ifdb"),
    "arc_ifdb_pas_diagnose": ("ext_ifdb_pas_diagnose", "arc_pas_diagnose_ifdb"),
    "arc_ifdb_administrative_procedure": (
        "ext_ifdb_administrative_procedure",
        "arc_administrative_procedure_ifdb",
    ),
    "arc_ifdb_ris_ydelse": ("ext_ifdb_ris_ydelse", "arc_ris_ydelse_ifdb"),
    "arc_ifdb_henvisning": ("ext_ifdb_henvisning", "arc_henvisning_ifdb"),
    "arc_ifdb_referralinformation": (
        "ext_ifdb_referralinformation",
        "arc_referralinformation_ifdb",
    ),
    # arc_ifdb_henvisning_historik is absent for the composite-key reason below.
    "arc_ifdb_aktivitetsresultat": (
        "ext_ifdb_aktivitetsresultat",
        "arc_aktivitetsresultat_ifdb",
    ),
    "arc_ifdb_aktivitetsudfoerelse": (
        "ext_ifdb_aktivitetsudfoerelse",
        "arc_aktivitetsudfoerelse_ifdb",
    ),
    # Sengeenhed: single merge key (OBJEKT_ID) + delta on MODTIDSPUNKT, so an
    # ids-mode replay is both safe and cheap (279 rows). The four other
    # senge/kapacitet tables are absent: composite merge keys, and three of them
    # are full snapshots whose gaps self-heal on the next load.
    "arc_ifdb_sengeenhed": ("ext_ifdb_sengeenhed", "arc_sengeenhed_ifdb"),
    "arc_ifdb_eventmarker": ("ext_ifdb_eventmarker", "arc_eventmarker_ifdb"),
    # arc_ifdb_location intentionally absent -- see the module docstring.
    # The full-load child/ref tables (tillaegskode + PAS ref tables +
    # aktivitetsudfoerelse_speciale) are likewise absent: composite merge keys
    # and NOT-MATCHED-BY-SOURCE delete detection make a partial ids-mode replay
    # unsafe; their gaps self-heal on the next full load.
}

_ORACLE_IN_LIMIT = 1000  # Oracle rejects IN-lists with more than 1000 literals


def _chunk(keys: list[str], size: int = _ORACLE_IN_LIMIT) -> list[list[str]]:
    return [keys[i : i + size] for i in range(0, len(keys), size)]


def _desired_config(
    ext_asset: str, arc_asset: str, filter_column: str, chunk: list[str]
) -> dict[str, dict]:
    """Per-op run config for one chunk of keys.

    BOTH ops get the identical ids config: the arc MERGE ignores the id filter (it
    upserts whatever the ids-extract staged into ext), but its ``RunModeConfig``
    VALIDATES ``mode='ids'`` -- without the filter fields pydantic rejects the
    config before the op body even runs -- and the mode lands in the
    ``meta.dfm_log`` row, so 'ids' there is also the truthful audit trail.
    Neither op advances the DFM checkpoint.
    """
    ids_cfg = {
        "mode": "ids",
        "id_filter_column": filter_column,
        "id_filter_values": list(chunk),
        "advance_checkpoint": False,
    }
    return {ext_asset: dict(ids_cfg), arc_asset: dict(ids_cfg)}


class ArcReplayConfig(dg.Config):
    entity: str = Field(
        description="Which archive entity to repair: one of "
        "arc_ifdb_encounter | arc_ifdb_episode_of_care | arc_ifdb_patient.",
    )
    max_keys: int = Field(
        default=5_000,
        description="Abort if more than this many keys are pending -- an archive gap "
        "that large is systemic (investigate the extract), not key-by-key repairable. "
        "Keys are re-extracted in child runs of 1000 (Oracle IN-list cap).",
    )
    dry_run: bool = Field(
        default=False,
        description="Log the plan (pending count, chunking, run config) without "
        "launching runs or updating queue status.",
    )
    mark_resolved: bool = Field(
        default=True,
        description="Re-check each key against arc after the merge and flip resolved keys.",
    )
    unresolvable_after: int = Field(
        default=3,
        description="Flip a key to status='unresolvable' when it is still missing after "
        "this many replay attempts.",
    )


def _subject_for_entity(entity: str) -> DqaSubject:
    for s in ARC_SUBJECTS:
        if s.replay and s.replay.entity == entity:
            return s
    raise dg.Failure(description=f"No arc subject declares a ReplaySpec for entity {entity!r}.")


def _pending_keys(cur, entity: str) -> list[str]:
    cur.execute(
        f"SELECT business_key FROM [dqa].[RemediationQueue] "
        f"WHERE replay_entity = {sql_literal(entity)} AND status = 'pending' "
        f"AND business_key IS NOT NULL ORDER BY business_key"
    )
    return [r[0] for r in cur.fetchall()]


def _recheck_and_resolve(
    cur, subject: DqaSubject, entity: str, unresolvable_after: int
) -> tuple[int, int, int]:
    """Flip keys that now exist in arc to resolved; bump/park the rest."""
    schema, table = split_table(subject.table)
    key = cast(ReplaySpec, subject.replay).filter_column
    lit_entity = sql_literal(entity)
    in_arc = f"EXISTS (SELECT 1 FROM [{schema}].[{table}] a WHERE a.[{key}] = q.business_key)"
    base = (
        f"FROM [dqa].[RemediationQueue] q "
        f"WHERE q.replay_entity = {lit_entity} AND q.status = 'pending' "
        f"AND q.business_key IS NOT NULL"
    )
    cur.execute(
        f"UPDATE q SET status = 'resolved', attempts = attempts + 1, "
        f"last_attempt_at = SYSUTCDATETIME(), note = 'replayed' {base} AND {in_arc}"
    )
    resolved = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    cur.execute(
        f"UPDATE q SET attempts = attempts + 1, last_attempt_at = SYSUTCDATETIME(), "
        f"note = 'still missing in arc after replay' {base} AND NOT {in_arc}"
    )
    remaining = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    # The two UPDATEs partition the pending rows, so what is still pending here is
    # exactly the still-missing set (same pattern as the FHIR replay recheck).
    cur.execute(
        f"UPDATE q SET status = 'unresolvable', "
        f"note = 'unresolvable: still missing after ' "
        f"+ CAST(attempts AS NVARCHAR(10)) + ' replay attempts' "
        f"{base} AND q.attempts >= {int(unresolvable_after)}"
    )
    unresolvable = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    return resolved, remaining, unresolvable


@dg.asset(
    name="dqa_arc_ifdb_replay",
    group_name="dqa_operations",
    compute_kind="python",
    required_resource_keys={"mssql_target_staging"},
    description=(
        "Operator-triggered: re-extract queued missing IFDB keys (ids mode, inline "
        "IN-lists, checkpoint untouched) and merge them into the arc archive, then "
        "mark resolved. Mutates arc.* -- run with dry_run=true first."
    ),
)
def dqa_arc_ifdb_replay(
    context: dg.AssetExecutionContext,
    config: ArcReplayConfig,
) -> dg.MaterializeResult:
    entity = config.entity
    if entity not in _ARC_DFM_ASSETS:
        raise dg.Failure(
            description=f"Unknown entity {entity!r}; expected one of {sorted(_ARC_DFM_ASSETS)}."
        )
    subject = _subject_for_entity(entity)
    ext_asset, arc_asset = _ARC_DFM_ASSETS[entity]
    filter_column = cast(ReplaySpec, subject.replay).filter_column
    resources = context.resources

    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        keys = _pending_keys(cur, entity)

    if not keys:
        context.log.info(f"DQA arc replay: no pending keys for {entity}; nothing to do.")
        return dg.MaterializeResult(metadata={"entity": entity, "pending": 0, "status": "noop"})
    if len(keys) > config.max_keys:
        raise dg.Failure(
            description=(
                f"DQA arc replay: {len(keys)} pending keys for {entity} exceed max_keys "
                f"({config.max_keys}). An archive gap that large is systemic -- "
                f"investigate the extract (or run the period orchestrator) instead."
            ),
        )

    chunks = _chunk(keys)
    assets = [ext_asset, arc_asset]
    asset_keys, job_def = _resolve_job(context, assets)
    node_names = {n.name for n in job_def.all_node_defs}
    missing_nodes = [a for a in assets if a not in node_names]
    if missing_nodes:
        raise dg.Failure(
            description=(
                f"DQA arc replay: no op node found for {missing_nodes} in the resolved "
                f"job. Aborting rather than replaying a partial selection."
            ),
        )

    def _run_config_for(chunk: list[str]) -> dict:
        rc = _build_run_config(job_def, _desired_config(ext_asset, arc_asset, filter_column, chunk))
        # Fail loudly rather than extract everything: without the inline ids filter
        # the ext op would run its default delta/full window. (The arc op needs the
        # same fields purely to pass RunModeConfig's mode='ids' validation.)
        for op in (ext_asset, arc_asset):
            cfg = rc["ops"].get(op, {}).get("config", {})
            if cfg.get("mode") != "ids" or not cfg.get("id_filter_values"):
                raise dg.Failure(
                    description=(
                        f"DQA arc replay: op '{op}' did not accept ids-mode config "
                        f"({cfg}); aborting to avoid a full/delta re-extract or an "
                        f"invalid run config."
                    ),
                )
        return rc

    if config.dry_run:
        preview = _run_config_for(chunks[0])
        # Don't dump up to 1000 keys into the log (twice) -- sample them.
        sampled = chunks[0][:5] + (["..."] if len(chunks[0]) > 5 else [])
        for op in (ext_asset, arc_asset):
            preview["ops"][op]["config"]["id_filter_values"] = sampled
        context.log.info(
            f"DQA arc replay DRY-RUN for {entity}: {len(keys)} pending keys in "
            f"{len(chunks)} chunk(s) of <= {_ORACLE_IN_LIMIT}.\n"
            f"  assets={assets}\n"
            f"  run_config (chunk 1, ids sampled)={preview}"
        )
        return dg.MaterializeResult(
            metadata={
                "entity": entity,
                "pending": len(keys),
                "chunks": len(chunks),
                "status": "dry_run",
            }
        )

    run_ids = []
    for i, chunk in enumerate(chunks, start=1):
        run_config = _run_config_for(chunk)
        run_ids.append(
            _launch(
                context,
                job_def,
                asset_keys,
                run_config,
                f"arc replay {entity} chunk {i}/{len(chunks)} ({len(chunk)} keys)",
            )
        )

    resolved = remaining = unresolvable = -1
    if config.mark_resolved:
        with get_conn(resources, subject.resource_key) as conn:
            cur = conn.cursor()
            resolved, remaining, unresolvable = _recheck_and_resolve(
                cur, subject, entity, config.unresolvable_after
            )
            conn.commit()
        context.log.info(
            f"DQA arc replay {entity}: re-check resolved {resolved}, {remaining} still "
            f"missing ({unresolvable} flipped to unresolvable after "
            f">= {config.unresolvable_after} attempts)."
        )

    return dg.MaterializeResult(
        metadata={
            "entity": entity,
            "pending": len(keys),
            "chunks": len(chunks),
            "run_ids": run_ids,
            "resolved": resolved,
            "still_missing": remaining,
            "unresolvable": unresolvable,
        }
    )


dqa_arc_ifdb_replay_job = dg.define_asset_job(
    name="dqa_arc_ifdb_replay_job",
    selection=dg.AssetSelection.assets("dqa_arc_ifdb_replay"),
    description=(
        "Re-extract queued missing IFDB keys into the arc archive "
        "(ids mode, checkpoint untouched; operator-triggered)."
    ),
    tags={"domain": "dqa", "dqa_track": "arc"},
)

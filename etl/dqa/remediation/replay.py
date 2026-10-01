# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Semi-automatic remediation: replay DFM children for queued dangling references.

One operator action (``dqa_remediation_replay_job``) per entity:

1. read the pending business keys from ``dqa.RemediationQueue`` (via the feeder
   view), guarded by ``max_keys``;
2. one child run over ``fhir_server_<entity>_dfm`` (``ids`` mode, ``id_filter_table``
   pointed at the feeder view -> re-resolves each business key to the surviving
   parent) + ``sof_<entity>_dfm`` + ``dso_sync_fhir_<entity>_dfm`` (delta). Config
   is per-asset, so ids + delta coexist in the one run; the asset deps
   (server -> sof -> dso) make Dagster run them in order, so the server post lands
   before SOF reads. (SOF has no ids mode, but the re-post bumps ``lastUpdated``
   so delta catches exactly the replayed resources.)
3. re-check each captured reference and flip resolved keys to ``status='resolved'``
   (bumping ``attempts`` on any that still dangle). Keys still failing after
   ``unresolvable_after`` attempts (default 3) flip to ``status='unresolvable'`` --
   they leave the replay loop (the feeder views select 'pending' only) and stay
   visible for the operator until manually reset to 'pending'.

The child run executes synchronously in-process via ``execute_run`` (the proven
``period_orchestrator`` pattern). ``dry_run`` previews the plan + run_config
without launching or mutating anything -- run it first.

This MUTATES production FHIR data (re-posts resources, reprojects flat tables);
it is operator-triggered, never scheduled.
"""

import dagster as dg
from core.branch_postfix import get_postfixed_asset_key
from dagster._core.definitions.reconstruct import ReconstructableJob, ReconstructableRepository
from dagster._core.execution.api import execute_run
from dqa.checks.helpers import get_conn, split_table, sql_literal
from dqa.checks.referential_integrity import violation_predicate
from dqa.config import FHIR_SUBJECTS
from dqa.model import DqaSubject
from pydantic import Field

# entity (== ReplaySpec.entity / arc base name) -> (server, sof, dso) DFM assets.
# Explicit because the names are not uniform (server uses "eoc"; sof/dso use
# "episode_of_care").
_DFM_ASSETS: dict[str, tuple[str, str, str]] = {
    "ifdb_encounter": (
        "fhir_server_encounter_dfm",
        "sof_encounter_dfm",
        "dso_sync_fhir_encounter_dfm",
    ),
    "ifdb_episode_of_care": (
        "fhir_server_eoc_dfm",
        "sof_episode_of_care_dfm",
        "dso_sync_fhir_episode_of_care_dfm",
    ),
    "ifdb_patient": (
        "fhir_server_patient_dfm",
        "sof_patient_dfm",
        "dso_sync_fhir_patient_dfm",
    ),
    "bom_ifdb_aftale": (
        "fhir_server_appointment_dfm",
        "sof_appointment_dfm",
        "dso_sync_fhir_appointment_dfm",
    ),
}


class RemediationReplayConfig(dg.Config):
    entity: str = Field(
        description="Which archive entity to replay: one of "
        "ifdb_encounter | ifdb_episode_of_care | ifdb_patient | bom_ifdb_aftale.",
    )
    max_keys: int = Field(
        default=100_000,
        description="Abort if more than this many keys are pending -- a spike that "
        "large is systemic and should be investigated, not bulk-replayed.",
    )
    run_sof_dso: bool = Field(
        default=True,
        description="Also reproject SOF + DSO (delta) after the FHIR-server replay.",
    )
    dry_run: bool = Field(
        default=False,
        description="Log the plan + run_config without launching runs or updating status.",
    )
    mark_resolved: bool = Field(
        default=True,
        description="Re-check each captured reference after reproject and flip resolved keys.",
    )
    unresolvable_after: int = Field(
        default=3,
        description="Flip a key to status='unresolvable' when it is still failing after "
        "this many replay attempts -- it leaves the replay loop (feeder views select "
        "'pending' only) until an operator investigates and resets it to 'pending'.",
    )


def _subject_for_entity(entity: str) -> DqaSubject:
    for s in FHIR_SUBJECTS:
        if s.replay and s.replay.entity == entity:
            return s
    raise dg.Failure(description=f"No FHIR subject declares a ReplaySpec for entity {entity!r}.")


def _reconstructable_job(context, job_name: str) -> ReconstructableJob:
    """Reconstructable reference to a target job from the running process (for execute_run)."""
    step_ctx = context.get_step_execution_context()
    current_job = step_ctx.plan_data.job
    if isinstance(current_job, ReconstructableJob):
        recon_repo: ReconstructableRepository = current_job.get_reconstructable_repository()
        return recon_repo.get_reconstructable_job(job_name)
    raise dg.Failure(
        description=(
            "Cannot obtain a reconstructable job reference -- dqa_remediation_replay "
            "must run inside a Dagster environment that supports process hand-off "
            "(dagster dev / production deployment)."
        ),
    )


def _resolve_job(context, asset_names: list[str]):
    """Resolve (postfixed asset keys, implicit job) for a set of bare asset names.

    On a feature-branch deployment the asset *keys* are postfixed (``..._feature_x``)
    so the selection must use the postfixed keys (``get_postfixed_asset_key`` is a
    no-op off feature branches).
    """
    asset_keys = frozenset(get_postfixed_asset_key(n) for n in asset_names)
    job_def = context.repository_def.get_implicit_job_def_for_assets(asset_keys)
    if job_def is None:
        raise dg.Failure(description=f"Could not build an implicit job for assets: {asset_names}.")
    return asset_keys, job_def


def _build_run_config(job_def, desired: dict[str, dict]) -> dict:
    """Bind desired per-asset config onto the resolved job's ACTUAL op nodes.

    Mirrors ``period_orchestrator._build_run_config`` instead of hardcoding op keys:

    - op names are read from ``job_def.all_node_defs`` (asset postfixing renames asset
      *keys*, not op node names -- verified on a feature-branch deployment), so we
      never guess the key; and
    - only config fields the op actually declares are set, so a field an op doesn't
      accept (e.g. ``advance_checkpoint``) can't make ``create_run_for_job`` reject the
      whole run. An op that declares NONE of the desired fields -- e.g.
      ``dso_sync_fhir_*_dfm``, which takes no run config at all -- ends up unbound and
      simply runs with its defaults.

    ``desired`` is keyed by bare asset name; ``_check_replay_binding`` validates the
    result (the server op's ids config is the safety-critical part -- a silently-
    dropped ids config would make the server re-post in delta/full mode over
    everything).
    """
    ops: dict = {}
    for node in job_def.all_node_defs:
        want = desired.get(node.name)
        if not want:
            continue
        cs = node.config_schema
        fields = getattr(getattr(cs, "config_type", None), "fields", None)
        if not fields:
            continue
        cfg = {k: v for k, v in want.items() if k in fields}
        if cfg:
            ops[node.name] = {"config": cfg}
    return {"ops": ops}


def _check_replay_binding(
    assets: list[str], server_asset: str, node_names: set[str], run_config: dict
) -> list[str]:
    """Validate the resolved job / bound config before launching the replay.

    Fails loudly on the two dangerous cases; returns the harmless remainder:

    - an intended asset has NO op node in the resolved job (naming drift -- e.g.
      a rename, or postfixing unexpectedly reaching op names) -> abort;
    - the SERVER op did not end up with the ids-mode config (a silently-dropped
      ids config would re-post EVERY resource in delta/full mode) -> abort;
    - returns the ops that exist but bound no config -- they declare none of the
      desired fields (e.g. ``dso_sync_fhir_*_dfm`` takes no run config; its delta
      sync is watermark-driven) and just run with their defaults.
    """
    missing = [a for a in assets if a not in node_names]
    if missing:
        raise dg.Failure(
            description=(
                f"DQA replay: no op node found for {missing}; resolved job nodes do not "
                f"include the intended asset(s) (renamed op / drifted asset name?). "
                f"Aborting rather than replaying a partial selection."
            ),
        )
    server_cfg = run_config["ops"].get(server_asset, {}).get("config", {})
    if server_cfg.get("mode") != "ids" or not server_cfg.get("id_filter_table"):
        raise dg.Failure(
            description=(
                f"DQA replay: server op '{server_asset}' did not accept ids-mode config "
                f"({server_cfg}); aborting to avoid a full/delta re-post of every resource."
            ),
        )
    return [a for a in assets if a not in run_config["ops"]]


def _launch(context, job_def, asset_keys, run_config: dict, label: str) -> str:
    """Run the resolved implicit job over ``asset_keys`` synchronously, in-process."""
    recon_job = _reconstructable_job(context, job_def.name)
    dagster_run = context.instance.create_run_for_job(
        job_def=job_def,
        run_config=run_config,
        asset_selection=asset_keys,
        tags={
            "dagster/orchestrator": "dqa_remediation_replay",
            "dagster/parent_run_id": context.run_id,
        },
    )
    selected = sorted(k.to_user_string() for k in asset_keys)
    context.log.info(f"Launching {label}: assets={selected} run_id={dagster_run.run_id}")
    result = execute_run(job=recon_job, dagster_run=dagster_run, instance=context.instance)
    if not result.success:
        raise dg.Failure(
            description=f"{label} child run failed (run_id={dagster_run.run_id}); see its logs.",
        )
    return dagster_run.run_id


def _recheck_and_resolve(
    context, resources, subject: DqaSubject, entity: str, unresolvable_after: int = 3
) -> tuple[int, int, int]:
    """Per captured reference, resolve keys that no longer dangle; bump the rest.

    Returns ``(resolved, remaining, unresolvable)``. Keys still failing once
    ``attempts`` reaches ``unresolvable_after`` flip to ``status='unresolvable'``
    (a subset of ``remaining``): they stop feeding ``dqa.v_replay_<entity>``
    (pending-only) so a key that never converges doesn't get replayed forever --
    the operator investigates and resets it to 'pending' to retry. NB: the capture
    MERGE only reopens 'resolved' keys, so a re-detected unresolvable key stays
    unresolvable rather than silently re-entering the loop.
    """
    rp = subject.replay
    s_schema, s_table = split_table(subject.table)
    flat = f"[{s_schema}].[{s_table}]"
    key = rp.key_column
    lit_subj = sql_literal(subject.name)
    lit_entity = sql_literal(entity)

    # parent_table/parent_key/ignore per reference, from the subject's referential specs.
    ref_specs = {
        sp.params["column"]: sp.params
        for sp in subject.checks
        if sp.check_type == "referential_integrity"
    }

    resolved = remaining = unresolvable = 0
    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT DISTINCT reference_column FROM [dqa].[RemediationQueue] "
            f"WHERE subject = {lit_subj} AND replay_entity = {lit_entity} AND status = 'pending'"
        )
        refs = [r[0] for r in cur.fetchall()]

        for ref in refs:
            p = ref_specs.get(ref)
            if not p:
                # Non-referential finding (e.g. patient_arc_integrity's
                # kept_deleted_dup): we can't rebuild a dangling predicate, but the
                # replay re-posted these keys, so resolve optimistically. If still
                # broken, the next check re-captures (the capture MERGE reopens it).
                cur.execute(
                    f"UPDATE [dqa].[RemediationQueue] SET status = 'resolved', "
                    f"attempts = attempts + 1, last_attempt_at = SYSUTCDATETIME(), "
                    f"note = 'replayed' WHERE subject = {lit_subj} "
                    f"AND reference_column = {sql_literal(ref)} "
                    f"AND replay_entity = {lit_entity} AND status = 'pending'"
                )
                resolved += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
                continue
            # The SAME full predicate the check flagged (violation_predicate from
            # checks/referential_integrity.py): dangling reference, plus -- for
            # required edges -- the NULL/sentinel branch. Rebuilding only the
            # dangling half would flip a still-NULL required FK to 'resolved'
            # (NULL fails the IS NOT NULL branch), and the next weekly check would
            # re-open it -- endless churn with misleading statuses.
            viol = violation_predicate(p, "src")
            # Scope the recheck to ACTIVE rows, exactly as referential_integrity does
            # (checks/referential_integrity.py) -- otherwise a soft-deleted straggler
            # (an old, tomb-stoned version still carrying the pre-replay dangling FK)
            # keeps the queue row "still dangling" forever, while the active-scoped
            # weekly check already passes. Without this the key never converges to
            # resolved and keeps resurfacing in dqa.v_replay_<entity>.
            active = ""
            if subject.deleted_column:
                dc = subject.deleted_column
                active = f" AND (src.[{dc}] IS NULL OR src.[{dc}] <> 1)"
            still = (
                f"EXISTS (SELECT 1 FROM {flat} src "
                f"WHERE src.[{key}] = q.business_key AND ({viol}){active})"
            )
            lit_ref = sql_literal(ref)
            base = (
                f"FROM [dqa].[RemediationQueue] q "
                f"WHERE q.subject = {lit_subj} AND q.reference_column = {lit_ref} "
                f"AND q.status = 'pending' AND q.business_key IS NOT NULL"
            )
            cur.execute(
                f"UPDATE q SET status = 'resolved', attempts = attempts + 1, "
                f"last_attempt_at = SYSUTCDATETIME(), note = 'replayed' {base} AND NOT {still}"
            )
            resolved += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            cur.execute(
                f"UPDATE q SET attempts = attempts + 1, last_attempt_at = SYSUTCDATETIME(), "
                f"note = 'still dangling after replay' {base} AND {still}"
            )
            remaining += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            # The two UPDATEs above partition this ref's pending rows (resolved vs
            # still-dangling), so the rows matching `base` here are exactly the
            # still-dangling ones -- no need to re-evaluate the EXISTS.
            cur.execute(
                f"UPDATE q SET status = 'unresolvable', "
                f"note = 'unresolvable: still failing after ' "
                f"+ CAST(attempts AS NVARCHAR(10)) + ' replay attempts' "
                f"{base} AND q.attempts >= {int(unresolvable_after)}"
            )
            unresolvable += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        conn.commit()
    return resolved, remaining, unresolvable


@dg.asset(
    name="dqa_remediation_replay",
    group_name="dqa_operations",
    compute_kind="python",
    required_resource_keys={"mssql_target_staging"},
    description=(
        "Operator-triggered: replay DFM children for queued dangling FHIR references "
        "(dqa.RemediationQueue) via ids mode, reproject SOF/DSO, and mark resolved. "
        "Mutates production FHIR data -- run with dry_run=true first."
    ),
)
def dqa_remediation_replay(
    context: dg.AssetExecutionContext,
    config: RemediationReplayConfig,
) -> dg.MaterializeResult:
    entity = config.entity
    if entity not in _DFM_ASSETS:
        raise dg.Failure(
            description=f"Unknown entity {entity!r}; expected one of {sorted(_DFM_ASSETS)}."
        )
    subject = _subject_for_entity(entity)
    server_asset, sof_asset, dso_asset = _DFM_ASSETS[entity]
    feeder_view = f"dqa.v_replay_{entity}"
    filter_column = subject.replay.filter_column
    resources = context.resources

    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM [{feeder_view.replace('.', '].[')}]")
        pending = int(cur.fetchone()[0] or 0)

    if pending == 0:
        context.log.info(f"DQA replay: no pending keys for {entity}; nothing to do.")
        return dg.MaterializeResult(metadata={"entity": entity, "pending": 0, "status": "noop"})
    if pending > config.max_keys:
        raise dg.Failure(
            description=(
                f"DQA replay: {pending} pending keys for {entity} exceed max_keys "
                f"({config.max_keys}). A spike that large is systemic -- investigate "
                f"before replaying."
            ),
        )

    # One child run, per-asset config intent: server in ids mode + SOF/DSO delta. The
    # asset deps (server -> sof -> dso) make Dagster run them in order within the
    # single run, so the server post completes before SOF reads.
    assets = [server_asset]
    desired: dict[str, dict] = {
        server_asset: {
            "mode": "ids",
            "id_filter_column": filter_column,
            "id_filter_table": feeder_view,
            "advance_checkpoint": False,
        }
    }
    if config.run_sof_dso:
        assets += [sof_asset, dso_asset]
        desired[sof_asset] = {"advance_checkpoint": False}
        desired[dso_asset] = {"advance_checkpoint": False}

    # Resolve the real job and bind the config to its ACTUAL op nodes (see
    # _build_run_config), rather than hardcoding op keys / config fields.
    asset_keys, job_def = _resolve_job(context, assets)
    run_config = _build_run_config(job_def, desired)

    # Fail loudly rather than replay with the wrong config -- but only on the
    # genuinely dangerous cases (missing op node; server op without ids config).
    # An op that declares none of the desired fields (dso_sync_fhir_*_dfm has no
    # run config at all) legitimately binds nothing and runs with its defaults.
    node_names = {n.name for n in job_def.all_node_defs}
    defaulted = _check_replay_binding(assets, server_asset, node_names, run_config)
    for a in defaulted:
        context.log.info(
            f"DQA replay: op '{a}' declares none of the desired config fields; "
            f"running it with its default config."
        )

    if config.dry_run:
        context.log.info(
            f"DQA replay DRY-RUN for {entity}: {pending} pending keys.\n"
            f"  assets={assets}\n"
            f"  run_config={run_config}"
        )
        return dg.MaterializeResult(
            metadata={"entity": entity, "pending": pending, "status": "dry_run"}
        )

    run_id = _launch(context, job_def, asset_keys, run_config, f"replay {entity}")

    resolved = remaining = unresolvable = -1
    if config.mark_resolved:
        resolved, remaining, unresolvable = _recheck_and_resolve(
            context, resources, subject, entity, unresolvable_after=config.unresolvable_after
        )
        context.log.info(
            f"DQA replay {entity}: re-check resolved {resolved}, {remaining} still "
            f"dangling ({unresolvable} flipped to unresolvable after "
            f">= {config.unresolvable_after} attempts)."
        )

    return dg.MaterializeResult(
        metadata={
            "entity": entity,
            "pending": pending,
            "run_id": run_id,
            "reprojected": config.run_sof_dso,
            "resolved": resolved,
            "still_dangling": remaining,
            "unresolvable": unresolvable,
        }
    )


dqa_remediation_replay_job = dg.define_asset_job(
    name="dqa_remediation_replay_job",
    selection=dg.AssetSelection.assets("dqa_remediation_replay"),
    description="Replay DFM children for queued dangling FHIR references (operator-triggered).",
    tags={"domain": "dqa", "dqa_track": "fhir"},
)

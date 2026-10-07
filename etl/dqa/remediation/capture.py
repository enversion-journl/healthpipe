# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Capture business keys into ``dqa.RemediationQueue`` for DFM ids-mode replay.

Three producers feed the queue, all for subjects that declare a ``ReplaySpec``:

- ``capture_referential_violations`` -- a ``referential_integrity`` failure: the
  DISTINCT business keys of the dangling (active) child rows. Replaying re-resolves
  them to the surviving parent.
- ``capture_kept_deleted_dups`` -- ``patient_arc_integrity``'s ``kept_deleted_dup``:
  fhir kept a deleted arc PATIENT_ID while an active CPR sibling exists. Replaying
  re-runs the CPR-dedup (``_build_patient_query`` ranks the whole CPR group), which
  re-points fhir to the active id.
- ``capture_source_reconciliation_gaps`` -- ``source_reconciliation``'s arc rows
  that never reached the flat table (``missing_in_target``) or whose arc soft-delete
  never propagated (arc-deleted ``delete_status_mismatch``). Replaying posts /
  tomb-stones them. Only valid when ``source_key == ReplaySpec.filter_column``.
- ``capture_archive_reconciliation_gaps`` -- ``archive_reconciliation``'s
  ``missing_in_arc`` keys (in the DFM source, absent from the archive). Fixed by
  ``dqa_arc_ifdb_replay`` (ids-mode re-extract + archive merge), NOT the FHIR
  replay -- the queue rows are namespaced by ``replay_entity`` (``arc_ifdb_*``),
  so they never leak into the ``dqa.v_replay_<entity>`` feeder views.

Set-based throughout (``INSERT ... SELECT`` via ``MERGE``) -- the keys never enter
Python, so it scales. Best-effort (never fails the check). A guard
(``DQA_REMEDIATION_MAX_KEYS``, default 100k) records an ``overflow`` marker instead
of enqueuing an oversized, systemic spike.
"""

from __future__ import annotations

import os
from contextvars import ContextVar

from dqa.checks.helpers import dqa_environment, get_conn, split_table, sql_literal
from dqa.model import CheckSpecification, DqaSubject

_DEFAULT_MAX_KEYS = 100_000

# Per-run override of the capture guard, set by the factory from DqaCheckConfig
# around each check. A ContextVar (not a parameter) so the override scopes to the
# running check without threading an argument through every check/capture signature.
_max_keys_override: ContextVar[int | None] = ContextVar("dqa_max_keys_override", default=None)


def set_max_keys_override(value: int | None):
    """Set the per-run capture-guard override; returns a token for ``reset``."""
    return _max_keys_override.set(value)


def reset_max_keys_override(token) -> None:
    """Restore the previous override (call in a ``finally`` with ``set``'s token)."""
    _max_keys_override.reset(token)


def _max_keys() -> int:
    override = _max_keys_override.get()
    if override is not None:
        return override
    try:
        return int(os.environ.get("DQA_REMEDIATION_MAX_KEYS", _DEFAULT_MAX_KEYS))
    except ValueError:
        return _DEFAULT_MAX_KEYS


def _enqueue(
    context,
    subject: DqaSubject,
    resources,
    *,
    reference_column: str,
    target: str,
    key_expr: str,
    source: str,
) -> None:
    """MERGE the distinct ``key_expr`` values selected by ``source`` into the queue.

    ``source`` is a full ``FROM ... WHERE ...`` clause; ``key_expr`` the business-key
    column expression within it (e.g. ``src.[identifier_value]``). No-op without a
    ReplaySpec.
    """
    if subject.replay is None:
        return
    rp = subject.replay
    _, s_table = split_table(subject.table)

    run_id = getattr(context, "run_id", None)
    environment = dqa_environment()
    lit_run = sql_literal(run_id) if run_id else "NULL"
    lit_env = sql_literal(environment)
    lit_subj = sql_literal(subject.name)
    lit_track = sql_literal(subject.track.value)
    lit_rtype = sql_literal(s_table)
    lit_ref = sql_literal(reference_column)
    lit_target = sql_literal(target)
    lit_entity = sql_literal(rp.entity)
    lit_filter = sql_literal(rp.filter_column)
    guard = _max_keys()

    # At most one overflow marker per (subject, reference_column) -- the unique
    # index treats NULLs as equal. Cleared in BOTH paths: before re-inserting on a
    # new overflow, and on a successful under-guard MERGE, so a drained backlog
    # doesn't leave a stale marker lingering in queue queries.
    marker_delete = (
        f"DELETE FROM [dqa].[RemediationQueue] "
        f"WHERE [subject] = {lit_subj} AND [reference_column] = {lit_ref} "
        f"AND [business_key] IS NULL"
    )

    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(DISTINCT {key_expr}) {source}")
        n = int(cur.fetchone()[0] or 0)
        if n == 0:
            return

        if n > guard:
            note = sql_literal(
                f"{n} distinct keys exceed guard ({guard}); not enqueued -- investigate"
            )
            context.log.error(
                f"DQA remediation: {subject.name}.{reference_column} has {n} keys "
                f"> guard {guard}; recording overflow marker instead of enqueuing."
            )
            cur.execute(marker_delete)
            cur.execute(
                f"INSERT INTO [dqa].[RemediationQueue] "
                f"([run_id],[environment],[track],[subject],[resource_type],[reference_column],"
                f" [target],[business_key],[replay_entity],[replay_filter_column],[status],[note]) "
                f"VALUES ({lit_run},{lit_env},{lit_track},{lit_subj},{lit_rtype},{lit_ref},"
                f"{lit_target},NULL,{lit_entity},{lit_filter},'overflow',{note})"
            )
            conn.commit()
            return

        # Under the guard again: clear any overflow marker a previous oversized
        # capture left behind (the backlog has drained / is being enqueued now).
        cur.execute(marker_delete)

        # Upsert: insert new keys; reopen a previously 'resolved' key that recurs.
        # When ``source`` reads ##dqa_recon, s.business_key inherits that temp's
        # column collation; pinning it at DDL keeps this MERGE aligned with
        # dqa.RemediationQueue.business_key (Danish staging).
        cur.execute(
            f"""
            MERGE [dqa].[RemediationQueue] WITH (HOLDLOCK) AS t
            USING (
                SELECT DISTINCT {key_expr} AS business_key {source}
            ) AS s
            ON (t.[subject] = {lit_subj}
                AND t.[reference_column] = {lit_ref}
                AND t.[business_key] = s.business_key)
            WHEN MATCHED AND t.[status] = 'resolved' THEN
                UPDATE SET t.[status] = 'pending',
                           t.[detected_at] = SYSUTCDATETIME(),
                           t.[run_id] = {lit_run},
                           t.[note] = 'recurred'
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ([run_id],[environment],[track],[subject],[resource_type],
                        [reference_column],[target],[business_key],[replay_entity],
                        [replay_filter_column])
                VALUES ({lit_run},{lit_env},{lit_track},{lit_subj},{lit_rtype},
                        {lit_ref},{lit_target},s.business_key,{lit_entity},{lit_filter});
            """
        )
        conn.commit()

    context.log.info(
        f"DQA remediation: enqueued {n} distinct {subject.name}.{reference_column} "
        f"business keys for replay entity '{rp.entity}'"
    )


def capture_referential_violations(
    context,
    subject: DqaSubject,
    spec: CheckSpecification,
    resources,
    *,
    where: str,
    reference_column: str,
    target: str,
) -> None:
    """Enqueue the distinct business keys of the dangling rows matching ``where``.

    ``where`` is the dangling-reference predicate (qualified to the ``src`` alias).
    """
    if subject.replay is None:
        return
    s_schema, s_table = split_table(subject.table)
    key = f"src.[{subject.replay.key_column}]"
    source = f"FROM [{s_schema}].[{s_table}] src WHERE ({where}) AND {key} IS NOT NULL"
    _enqueue(
        context,
        subject,
        resources,
        reference_column=reference_column,
        target=target,
        key_expr=key,
        source=source,
    )


def _recon_deleted_expr(col: str | None) -> str:
    """1 when the (string-or-int) flag equals 1/'1', else 0 (NULL -> 0)."""
    if not col:
        return "0"
    return f"CASE WHEN CAST([{col}] AS NVARCHAR(10)) = '1' THEN 1 ELSE 0 END"


def _recon_where(key: str, extra: str | None) -> str:
    clause = f"WHERE [{key}] IS NOT NULL"
    if extra:
        clause += f" AND ({extra})"
    return clause


def capture_source_reconciliation_gaps(
    context,
    subject: DqaSubject,
    resources,
    *,
    source_table: str,
    source_key: str,
    target_key: str,
    source_deleted: str | None,
    target_deleted: str | None,
    source_filter: str | None = None,
    target_filter: str | None = None,
) -> None:
    """Enqueue the arc keys ``source_reconciliation`` found unposted/mismatched.

    Captures the SOURCE-side business keys to replay via DFM ids mode:

    - ``missing_in_target``: arc rows absent from the flat table (never posted).
    - arc-deleted ``delete_status_mismatch``: arc is soft-deleted (``DELETED=1``)
      but the flat row is still active -- the delete never propagated; replaying
      re-posts it tomb-stoned. The reverse (arc active, flat deleted) is left
      alone: it can be a deliberate FHIR-server delete, not an arc gap.

    The captured ``business_key`` is the arc ``source_key`` value, which the feeder
    view (``dqa.v_replay_<entity>``) aliases to the DFM ``filter_column``. That is
    only correct when ``source_key == ReplaySpec.filter_column`` (e.g. both
    ``OBJECT_ID``); otherwise this no-ops with a warning -- the replay would filter
    the wrong arc column (e.g. a CPR-grained reconciliation whose replay keys on
    PATIENT_ID).
    """
    if subject.replay is None:
        return
    if source_key != subject.replay.filter_column:
        context.log.warning(
            f"DQA source_reconciliation capture skipped for {subject.name}: source_key "
            f"{source_key!r} != replay filter_column {subject.replay.filter_column!r}; "
            f"captured keys would not match the DFM ids filter."
        )
        return

    s_schema, s_table = split_table(source_table)
    t_schema, t_table = split_table(subject.table)
    sdel, tdel = _recon_deleted_expr(source_deleted), _recon_deleted_expr(target_deleted)
    compare_deleted = bool(source_deleted and target_deleted)

    # MIN (deleted iff ALL rows deleted), matching source_reconciliation: a business
    # key with a soft-deleted straggler plus a live row is live (t.d = 0), so the
    # arc-deleted branch below captures it for replay only when no live row remains.
    src_agg = (
        f"SELECT [{source_key}] AS k, MIN({sdel}) AS d "
        f"FROM [{s_schema}].[{s_table}] {_recon_where(source_key, source_filter)} "
        f"GROUP BY [{source_key}]"
    )
    tgt_agg = (
        f"SELECT [{target_key}] AS k, MIN({tdel}) AS d "
        f"FROM [{t_schema}].[{t_table}] {_recon_where(target_key, target_filter)} "
        f"GROUP BY [{target_key}]"
    )
    branches = [
        f"SELECT s.k AS k FROM ({src_agg}) s "
        f"WHERE NOT EXISTS (SELECT 1 FROM ({tgt_agg}) t WHERE t.k = s.k)"
    ]
    if compare_deleted:
        branches.append(
            f"SELECT s.k AS k FROM ({src_agg}) s JOIN ({tgt_agg}) t ON t.k = s.k "
            f"WHERE s.d = 1 AND t.d = 0"
        )
    source = f"FROM ({' UNION '.join(branches)}) AS d WHERE d.k IS NOT NULL"
    _enqueue(
        context,
        subject,
        resources,
        reference_column="source_reconciliation",
        target=source_table,
        key_expr="d.k",
        source=source,
    )


def capture_archive_reconciliation_gaps(
    context,
    subject: DqaSubject,
    resources,
    *,
    missing_source: str,
    key_expr: str,
    source_table: str,
) -> None:
    """Enqueue ``archive_reconciliation``'s ``missing_in_arc`` keys for arc replay.

    ``missing_source`` is the check's own missing-keys FROM/WHERE clause over its
    (still-live) ``##dqa_recon_*`` staging temp; ``key_expr`` the single merge-key
    expression within it. Global temp tables are visible cross-session, so the
    MERGE (which opens its own connection) can read it -- call this BEFORE the
    check drops the staging table. Single-key subjects only (the replay filters
    one source column); the caller enforces that.
    """
    if subject.replay is None:
        return
    _enqueue(
        context,
        subject,
        resources,
        reference_column="archive_reconciliation",
        target=source_table,
        key_expr=key_expr,
        source=missing_source,
    )


def capture_kept_deleted_dups(
    context,
    subject: DqaSubject,
    resources,
    *,
    joined: str,
    kept_pred: str,
    target: str,
) -> None:
    """Enqueue the kept (deleted) ifdb ids of ``kept_deleted_dup`` rows for replay.

    ``joined`` is the fhir(``p``)-JOIN-arc(``a``) clause and ``kept_pred`` its
    predicate, both from ``patient_arc_integrity``; the business key is the fhir
    row's ``ReplaySpec.key_column`` (``p`` alias).
    """
    if subject.replay is None:
        return
    key = f"p.[{subject.replay.key_column}]"
    source = f"FROM {joined} WHERE ({kept_pred}) AND {key} IS NOT NULL"
    _enqueue(
        context,
        subject,
        resources,
        reference_column="kept_deleted_dup",
        target=target,
        key_expr=key,
        source=source,
    )

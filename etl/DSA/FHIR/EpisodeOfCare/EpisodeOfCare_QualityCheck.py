# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Quality-checks for EpisodeOfCare FHIR resources.

1. ``fhir_quality_check_eoc_previous_references``

Finds EpisodeOfCare where the archive has PREVIOUS_EOC_ID but the FHIR resource
is missing the resolved previous reference (typically after a full_load where
the target landed in a later batch). Fetches the relevant archive rows,
re-resolves self-references against FHIR (where targets now exist) and upserts
the fixed resources.

Reports the number of corrected resources in metadata: ``eoc_references_fixed``.

Detection signal: when a previous reference cannot be resolved, the extension is
removed from the stored resource, so ``fhir.EpisodeOfCare.previous_episode_of_care_id``
(the flattened SoF column) is NULL for exactly the broken rows. The archive is
the source of truth for which episodes *should* carry a previous reference.

2. ``fhir_quality_check_eoc_healthissue_code``

Reconciles drift between ``arc.ifdb_healthissue`` (source of truth for the
category code) and the flattened ``fhir.EpisodeOfCare.healthissue_code``/
``healthissue_text``. After a healthissue row is updated in the source system,
the archive reflects the new code immediately but the already-posted FHIR
resource retains the stale value until this check re-transforms and upserts it.

Reports the number of corrected resources in metadata:
``eoc_healthissue_code_fixed``.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.EpisodeOfCare.EpisodeOfCare_IFDB_dfm import (
    EPISODE_OF_CARE_REFERENCE_CONFIGS,
    _build_healthissue_code_map,
    _collect_previous_eoc_identifiers,
    _ensure_previous_episodes_from_archive,
    _resolve_self_references_with_retry,
    _transform_episodes_to_fhir,
)
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.ifdb.fhir_posting import prepare_resources_for_upsert
from DSA.FHIR.utils.ifdb.patient_cpr_fallback import build_patient_cpr_fallback
from utils.fhir.fhir_extensions import EXTENSION_PREVIOUS_EPISODE_OF_CARE
from utils.fhir.identifier_systems import SYSTEM_IFDB_EPISODE_OF_CARE_ID

_FIX_FETCH_BATCH = 1000

# Candidates: the archive has a previous reference, but FHIR (flattened) is
# missing it — AND the referenced previous actually exists as a non-deleted
# archive row. The EXISTS guard excludes "hashed"/dangling PREVIOUS_EOC_ID
# values that point to no real episode (and a deleted previous, which is never
# posted to FHIR). Without it those rows would be retried on every run and
# never converge, since they can never be resolved.
_DETECT_SQL = """
SELECT a.[OBJECT_ID]
FROM   arc.ifdb_episode_of_care AS a
JOIN   fhir.EpisodeOfCare       AS f ON f.episode_of_care_id = a.[OBJECT_ID]
WHERE  a.[PREVIOUS_EOC_ID] IS NOT NULL
  AND  a.[dw_is_deleted] = 0
  AND  f.previous_episode_of_care_id IS NULL
  AND  EXISTS (
           SELECT 1
           FROM   arc.ifdb_episode_of_care AS p
           WHERE  p.[OBJECT_ID]   = a.[PREVIOUS_EOC_ID]
             AND  p.[dw_is_deleted] = 0
       )
"""


def _has_resolved_previous(eoc) -> bool:
    """True if the episode now carries a resolved previous reference (FHIR id set)."""
    for ext in eoc.extension or []:
        if (
            ext.url == EXTENSION_PREVIOUS_EPISODE_OF_CARE
            and ext.valueReference
            and getattr(ext.valueReference, "id", None)
        ):
            return True
    return False


@dg.asset(
    name="fhir_quality_check_eoc_previous_references",
    group_name="fhir_quality_dfm",
    compute_kind="fhir",
    deps=[
        "fhir_server_eoc_dfm",  # FHIR resources must be posted
        "sof_episode_of_care_dfm",  # flattened table must be current (detection)
    ],
    tags={"domain": "ifdb", "layer": "quality", "entity": "episode_of_care"},
    description=(
        "Reconcile missing previousEpisodeOfCare references in FHIR. "
        "Reports eoc_references_fixed in metadata."
    ),
)
def fhir_quality_check_eoc_previous_references(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    # 1) Find candidates (OBJECT_ID) missing their previous reference in FHIR
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(_DETECT_SQL)
            candidate_ids = [str(r[0]) for r in cursor.fetchall() if r[0] is not None]
        finally:
            cursor.close()

    context.log.info(f"{len(candidate_ids)} EpisodeOfCare candidates missing a previous reference")
    if not candidate_ids:
        return dg.MaterializeResult(metadata={"eoc_candidates": 0, "eoc_references_fixed": 0})

    # Patient-id → CPR fallback: episodes referencing a soft-deleted PATIENT_ID
    # only ever reached FHIR via this bridge, so re-resolving them here without
    # it would raise (patient is a required reference). Built once, reused for
    # every batch — same wiring as the main EoC pipeline.
    patient_cpr_fallback = build_patient_cpr_fallback(mssql_target_staging, fhir_database)

    total_fixed = 0
    for i in range(0, len(candidate_ids), _FIX_FETCH_BATCH):
        sub = candidate_ids[i : i + _FIX_FETCH_BATCH]

        # 2) Fetch full archive rows for the candidates (ids-mode, reuse builder)
        sql = build_fhir_archive_query(
            "arc.ifdb_episode_of_care",
            None,
            RunModeConfig(mode="ids", id_filter_column="OBJECT_ID", id_filter_values=sub),
        )
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                cols = [d[0] for d in cursor.description]
                rows = [dict(zip(cols, r, strict=False)) for r in cursor.fetchall()]
            finally:
                cursor.close()
        if not rows:
            continue

        # 3) Transform and set each episode's own FHIR id (upsert, not insert).
        # Pass the healthissue_code_map: this check upserts the FULL resource, so
        # without the map it would strip the healthissue_code/text valueCoding
        # extension off every episode it fixes.
        healthissue_code_map = _build_healthissue_code_map(rows, mssql_target_staging, context)
        episodes = _transform_episodes_to_fhir(
            rows, context, healthissue_code_map=healthissue_code_map
        )
        episodes = prepare_resources_for_upsert(
            resources=episodes,
            fhir_database=fhir_database,
            resource_type="EpisodeOfCare",
            preferred_id_systems=[SYSTEM_IFDB_EPISODE_OF_CARE_ID],
            reference_configs=EPISODE_OF_CARE_REFERENCE_CONFIGS,
            reference_fallback_fn=patient_cpr_fallback,
            logger=context.log,
        )

        # 4) Resolve previous references against FHIR (targets now exist)
        _resolve_self_references_with_retry(
            episodes=episodes,
            fhir_database=fhir_database,
            mssql=None,
            context=context,
        )

        # 5) Upsert only those that actually got a resolved previous ref → "fixed"
        fixed = [ep for ep in episodes if _has_resolved_previous(ep)]
        if fixed:
            fhir_database.batch_upsert_resources(resources=fixed, resource_type="EpisodeOfCare")
            total_fixed += len(fixed)
            context.log.info(f"Fixed {len(fixed)} EpisodeOfCare in this chunk")

    context.log.info(f"Done: {total_fixed} EpisodeOfCare previous references fixed")
    return dg.MaterializeResult(
        metadata={
            "eoc_candidates": len(candidate_ids),
            "eoc_references_fixed": total_fixed,
        }
    )


# =============================================================================
# Quality-check 2: healthissue_code drift
# =============================================================================

_HEALTHISSUE_FIX_BATCH = 1000

# Both-NULL guard (NOT (h... IS NULL AND f... IS NULL)): the transform only emits
# the valueCoding extension when the source code is non-null, so a NULL source code
# flattens to f.healthissue_code IS NULL. Those rows would otherwise match on every
# run, get re-transformed to the same (extension-less) resource, upsert nothing, and
# be re-detected forever — hence the exclusion. It deliberately excludes only the
# both-NULL case, NOT every NULL source code: guarding on the source side alone would
# also skip the set→NULL direction, leaving a removed code standing in
# fhir.EpisodeOfCare forever, invisible to this check (the EOC delta does not catch it
# either — a HEALTHISSUE-only update does not bump the EOC row's timestamp).
# The transition is not observable today (IFDB.HEALTHISSUE has zero NULL codes, and a
# soft-deleted health issue keeps its code — the lookup passes include_deleted=True),
# so the symmetric form is defensive, not a fix for live drift.
_DETECT_HEALTHISSUE_CODE_SQL = """
SELECT a.[OBJECT_ID]
FROM   arc.ifdb_episode_of_care AS a
JOIN   arc.ifdb_healthissue     AS h ON h.[OBJECT_ID] = a.[HEALTHISSUE_ID]
JOIN   fhir.EpisodeOfCare       AS f ON f.episode_of_care_id = a.[OBJECT_ID]
WHERE  a.[dw_is_deleted] = 0
  AND  NOT (h.[HEALTHISSUE_CODE] IS NULL AND f.healthissue_code IS NULL)
  AND  (
           ISNULL(f.healthissue_code, '') <> ISNULL(h.[HEALTHISSUE_CODE], '')
        OR ISNULL(f.healthissue_text, '') <> ISNULL(h.[HEALTHISSUE_TEXT], '')
      )
"""


@dg.asset(
    name="fhir_quality_check_eoc_healthissue_code",
    group_name="fhir_quality_dfm",
    compute_kind="fhir",
    deps=[
        "fhir_server_eoc_dfm",
        "sof_episode_of_care_dfm",
        "arc_healthissue_ifdb",
    ],
    tags={"domain": "ifdb", "layer": "quality", "entity": "episode_of_care"},
    description=(
        "Reconcile HEALTHISSUE_CODE drift: detects episodes where the flattened "
        "healthissue_code/text diverges from arc.ifdb_healthissue, re-transforms "
        "and upserts the corrected resources. Reports eoc_healthissue_code_fixed."
    ),
)
def fhir_quality_check_eoc_healthissue_code(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(_DETECT_HEALTHISSUE_CODE_SQL)
            candidate_ids = [str(r[0]) for r in cursor.fetchall() if r[0] is not None]
        finally:
            cursor.close()

    context.log.info(f"{len(candidate_ids)} EpisodeOfCare candidates with healthissue_code drift")
    if not candidate_ids:
        return dg.MaterializeResult(metadata={"eoc_candidates": 0, "eoc_healthissue_code_fixed": 0})

    # Patient-id → CPR fallback — see fhir_quality_check_eoc_previous_references.
    patient_cpr_fallback = build_patient_cpr_fallback(mssql_target_staging, fhir_database)

    total_fixed = 0
    for i in range(0, len(candidate_ids), _HEALTHISSUE_FIX_BATCH):
        sub = candidate_ids[i : i + _HEALTHISSUE_FIX_BATCH]

        sql = build_fhir_archive_query(
            "arc.ifdb_episode_of_care",
            None,
            RunModeConfig(mode="ids", id_filter_column="OBJECT_ID", id_filter_values=sub),
        )
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                cols = [d[0] for d in cursor.description]
                rows = [dict(zip(cols, r, strict=False)) for r in cursor.fetchall()]
            finally:
                cursor.close()
        if not rows:
            continue

        healthissue_code_map = _build_healthissue_code_map(rows, mssql_target_staging, context)
        episodes = _transform_episodes_to_fhir(
            rows, context, healthissue_code_map=healthissue_code_map
        )
        episodes = prepare_resources_for_upsert(
            resources=episodes,
            fhir_database=fhir_database,
            resource_type="EpisodeOfCare",
            preferred_id_systems=[SYSTEM_IFDB_EPISODE_OF_CARE_ID],
            reference_configs=EPISODE_OF_CARE_REFERENCE_CONFIGS,
            reference_fallback_fn=patient_cpr_fallback,
            logger=context.log,
        )

        if episodes:
            # This upserts the FULL resource, so a previousEpisodeOfCare reference
            # that isn't resolved here would be stripped from the stored resource
            # Ensure the referenced previous-episodes exist in FHIR, then resolve
            # the self-references, before upserting — same order as the main
            # pipeline and the previous-references quality check.
            prev_ids = _collect_previous_eoc_identifiers(episodes)
            _ensure_previous_episodes_from_archive(
                prev_ids, mssql_target_staging, fhir_database, context
            )
            _resolve_self_references_with_retry(
                episodes=episodes,
                fhir_database=fhir_database,
                mssql=None,
                context=context,
            )

            fhir_database.batch_upsert_resources(resources=episodes, resource_type="EpisodeOfCare")
            total_fixed += len(episodes)
            context.log.info(f"Fixed {len(episodes)} EpisodeOfCare healthissue_code in this chunk")

    context.log.info(f"Done: {total_fixed} EpisodeOfCare healthissue_code fixed")
    return dg.MaterializeResult(
        metadata={
            "eoc_candidates": len(candidate_ids),
            "eoc_healthissue_code_fixed": total_fixed,
        }
    )

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Quality-check: reconcile missing previousEpisodeOfCare references.

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
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.EpisodeOfCare.EpisodeOfCare_IFDB_dfm import (
    EPISODE_OF_CARE_REFERENCE_CONFIGS,
    _resolve_self_references_with_retry,
    _transform_episodes_to_fhir,
)
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.ifdb.fhir_posting import prepare_resources_for_upsert
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
FROM   arc.episode_of_care_ifdb AS a
JOIN   fhir.EpisodeOfCare       AS f ON f.episode_of_care_id = a.[OBJECT_ID]
WHERE  a.[PREVIOUS_EOC_ID] IS NOT NULL
  AND  a.[DW_IsDeleted] = 0
  AND  f.previous_episode_of_care_id IS NULL
  AND  EXISTS (
           SELECT 1
           FROM   arc.episode_of_care_ifdb AS p
           WHERE  p.[OBJECT_ID]   = a.[PREVIOUS_EOC_ID]
             AND  p.[DW_IsDeleted] = 0
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

    total_fixed = 0
    for i in range(0, len(candidate_ids), _FIX_FETCH_BATCH):
        sub = candidate_ids[i : i + _FIX_FETCH_BATCH]

        # 2) Fetch full archive rows for the candidates (ids-mode, reuse builder)
        sql = build_fhir_archive_query(
            "arc.episode_of_care_ifdb",
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

        # 3) Transform and set each episode's own FHIR id (upsert, not insert)
        episodes = _transform_episodes_to_fhir(rows, context)
        episodes = prepare_resources_for_upsert(
            resources=episodes,
            fhir_database=fhir_database,
            resource_type="EpisodeOfCare",
            preferred_id_systems=[SYSTEM_IFDB_EPISODE_OF_CARE_ID],
            reference_configs=EPISODE_OF_CARE_REFERENCE_CONFIGS,
            logger=context.log,
        )

        # 4) Resolve previous references against FHIR (targets now exist)
        _resolve_self_references_with_retry(
            episodes=episodes,
            fhir_database=fhir_database,
            ifdb_oracle=None,
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

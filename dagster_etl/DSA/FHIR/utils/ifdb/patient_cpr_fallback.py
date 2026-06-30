# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""CPR fallback for IFDB ``PATIENT_ID`` reference resolution.

When a FHIR resource references a Patient by IFDB ``PATIENT_ID`` that no longer
resolves directly, this fallback translates the unresolved ``PATIENT_ID`` → CPR
via ``arc.patient_ifdb`` (staging) and resolves the patient through her CPR
identifier (``urn:dk:cpr``) instead.

Why this is needed
------------------
A patient can have **several** ``PATIENT_ID``s on a single CPR — e.g. a
soft-deleted duplicate (``SLETTET=1``) alongside the active record. The Patient
asset deduplicates ``arc.patient_ifdb`` per CPR and posts only the active twin
to FHIR (see ``Patient_IFDB_dfm._build_patient_query``), so the deleted
``PATIENT_ID`` never becomes a FHIR Patient. Other resources (EpisodeOfCare,
Encounter, …) can still reference the deleted ``PATIENT_ID``; resolving that
reference via ``SYSTEM_IFDB_PATIENT_ID`` then fails:

    ValueError: Patient not found for identifier 3283517527 (field: patient, required: True)

Both ``PATIENT_ID``s share the same CPR, and the active twin carries that CPR in
FHIR, so the deleted id resolves cleanly once we go through the CPR.

DRY usage
---------
:func:`build_patient_cpr_fallback` returns a ``ResolveValuesFn``-shaped callable
that slots into the shared reference resolver
(:func:`DSA.FHIR.utils.reference_resolver.resolve_and_apply_all_references`,
``prepare_resources_for_upsert``) via the ``fallback_resolve_fn`` /
``reference_fallback_fn`` hook. It self-filters to Patient references keyed by
``SYSTEM_IFDB_PATIENT_ID`` and is a no-op for every other
``(resource_type, identifier_system)`` pair, so a single instance can be wired
into any posting asset that resolves a required patient id.
"""

from collections.abc import Iterable

import dagster as dg
from core.dfm_config import RunModeConfig
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.reference_resolver import ResolveValuesFn, batch_resolve_references
from utils.fhir.identifier_systems import SYSTEM_CPR, SYSTEM_IFDB_PATIENT_ID

# arc.patient_ifdb IN-list chunk size. Mirrors the previous-EpisodeOfCare archive
# fetch (_PREV_FETCH_BATCH) — keeps the inlined IN clause well clear of SQL
# Server's expression limits.
_LOOKUP_CHUNK = 1000

_PATIENT_ARCHIVE_TABLE = "arc.patient_ifdb"


def lookup_cprs_for_patient_ids(
    patient_ids: Iterable[str],
    mssql: MSSQLResource,
    *,
    chunk_size: int = _LOOKUP_CHUNK,
) -> dict[str, str]:
    """Translate IFDB ``PATIENT_ID`` values to CPR via ``arc.patient_ifdb``.

    Queries the staging archive (deleted rows included — a deleted duplicate
    still carries the shared CPR) and returns ``{PATIENT_ID: CPR}`` for every id
    that has a non-empty CPR. Ids with no row or no CPR are simply absent from
    the result.

    The IN clause is built through :func:`build_fhir_archive_query` in ``ids``
    mode so the value escaping/quoting is identical to the rest of the DFM
    archive queries.
    """
    ids = [str(pid).strip() for pid in patient_ids if pid is not None and str(pid).strip()]
    if not ids:
        return {}

    pid_to_cpr: dict[str, str] = {}
    for start in range(0, len(ids), chunk_size):
        chunk = ids[start : start + chunk_size]
        sql = build_fhir_archive_query(
            _PATIENT_ARCHIVE_TABLE,
            None,
            RunModeConfig(
                mode="ids",
                id_filter_column="PATIENT_ID",
                id_filter_values=chunk,
            ),
            include_deleted=True,
            select_clause="SELECT [PATIENT_ID], [CPR]",
        )
        with mssql.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                columns = [desc[0] for desc in cursor.description]
                for row in cursor.fetchall():
                    row_dict = dict(zip(columns, row, strict=False))
                    pid = row_dict.get("PATIENT_ID")
                    cpr = row_dict.get("CPR")
                    if pid is None:
                        continue
                    cpr_str = str(cpr).strip() if cpr is not None else ""
                    if cpr_str:
                        pid_to_cpr[str(pid).strip()] = cpr_str
            finally:
                cursor.close()

    return pid_to_cpr


def build_patient_cpr_fallback(
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
) -> ResolveValuesFn:
    """Build a reusable CPR fallback for patient-id reference resolution.

    The returned callable matches the ``ResolveValuesFn`` shape used by the
    shared reference resolver. It is a no-op unless asked to resolve Patient
    references keyed by ``SYSTEM_IFDB_PATIENT_ID``; for those it:

      1. translates the still-unresolved ``PATIENT_ID``s to CPR via
         ``arc.patient_ifdb``, then
      2. resolves those CPRs to FHIR Patient ids via ``SYSTEM_CPR``,

    returning ``{PATIENT_ID: fhir_patient_id}`` for the ids it could bridge.
    """

    def _fallback(
        *,
        resource_type: str,
        identifier_system: str,
        values: list[str],
    ) -> dict[str, str]:
        if resource_type != "Patient" or identifier_system != SYSTEM_IFDB_PATIENT_ID:
            return {}

        unresolved = [str(v).strip() for v in values if v is not None and str(v).strip()]
        if not unresolved:
            return {}

        logger = dg.get_dagster_logger()

        pid_to_cpr = lookup_cprs_for_patient_ids(unresolved, mssql)
        if not pid_to_cpr:
            logger.warning(
                f"[CPR-FALLBACK] No CPR found in {_PATIENT_ARCHIVE_TABLE} for any of "
                f"{len(unresolved)} unresolved PATIENT_ID(s) — cannot bridge"
            )
            return {}

        cpr_to_fhir_id = batch_resolve_references(
            identifiers={cpr: "" for cpr in set(pid_to_cpr.values())},
            resource_type="Patient",
            identifier_system=SYSTEM_CPR,
            fhir_database=fhir_database,
        )

        resolved = {
            pid: cpr_to_fhir_id[cpr] for pid, cpr in pid_to_cpr.items() if cpr in cpr_to_fhir_id
        }

        if resolved:
            logger.info(
                f"[CPR-FALLBACK] Bridged {len(resolved)}/{len(unresolved)} unresolved "
                f"PATIENT_ID reference(s) to existing FHIR patients via CPR"
            )
        return resolved

    return _fallback

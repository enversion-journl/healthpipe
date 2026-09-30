# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DQA subjects for the IFDB archive tables (arc.* on the Staging server / DSA).

Each arc table is reconciled weekly against its IFDB Oracle source via the
source-generic ``archive_reconciliation`` check (pinned here with
``dialect="oracle"`` / ``source_resource="ifdb_oracle"``): every current IFDB
merge key must be archived (missing_in_arc = archive gap), and arc keys absent
from IFDB must be soft-deleted (dw_is_deleted) rather than left stale. This is
the upstream half of the lineage IFDB -> arc -> fhir.

The reconciliation key is the archive's **merge key**, taken straight from
``ArchiveMergeConfig.merge_keys`` so DQA can't drift from the archive's grain --
e.g. Location is keyed by ``(OBJECT_ID, PERIOD_START)`` (composite), not OBJECT_ID
alone. Only the source-table name (Oracle) and the Oracle resource differ from the
archive config.

Own track (Track.ARCHIVE), split per source (``source="ifdb"``) -> anchors
``dqa_arc_ifdb_<entity>_weekly``, job ``dqa_arc_ifdb_weekly_job`` + its weekly
schedule, built automatically by the factory/jobs/schedules machinery. arc.* spans
several sources (IFDB today; KMD/ILS/Sydax/MySupply archives later, each its own
source -> its own ``dqa_arc_<source>_weekly_job``).
"""

from __future__ import annotations

from dqa.model import Cadence, CheckSpecification, DqaSubject, ReplaySpec, Severity, Track
from DSI.oracle.ifdb_archive import (
    ADM_PROCEDURE_SUPPL_CODES_ARCHIVE_CONFIG,
    ADMINISTRATIVE_PROCEDURE_ARCHIVE_CONFIG,
    AKTIVITETSRESULTAT_ARCHIVE_CONFIG,
    AKTIVITETSUDFOERELSE_ARCHIVE_CONFIG,
    AKTIVITETSUDFOERELSE_SPECIALE_ARCHIVE_CONFIG,
    BOM_IFDB_AFTALE_ARCHIVE_CONFIG,
    BOM_IFDB_AFTALERESSOURCEDELTAGER_ARCHIVE_CONFIG,
    BOM_IFDB_ORGENHED_SOR_ARCHIVE_CONFIG,
    BOM_IFDB_PATIENTINFO_ARCHIVE_CONFIG,
    DIAGNOSE_ARCHIVE_CONFIG,
    DIAGNOSE_TILLAEGSKODE_ARCHIVE_CONFIG,
    ENCOUNTER_ARCHIVE_CONFIG,
    EOC_ARCHIVE_CONFIG,
    HENVISNING_ARCHIVE_CONFIG,
    HENVISNING_HISTORIK_ARCHIVE_CONFIG,
    LOCATION_ARCHIVE_CONFIG,
    PAS_DIAGNOSE_ARCHIVE_CONFIG,
    PAS_DIAGNOSE_ENCOUNTER_REF_ARCHIVE_CONFIG,
    PAS_DIAGNOSE_HENVISNING_REF_ARCHIVE_CONFIG,
    PAS_DIAGNOSE_TILLAEGSKODE_ARCHIVE_CONFIG,
    PATIENT_ARCHIVE_CONFIG,
    REFERRALINFORMATION_ARCHIVE_CONFIG,
    RIS_YDELSE_ARCHIVE_CONFIG,
)
from DSI.oracle.ifdb_extract import (
    ADM_PROCEDURE_SUPPL_CODES_EXTRACT_CONFIG,
    ADMINISTRATIVE_PROCEDURE_EXTRACT_CONFIG,
    AKTIVITETSRESULTAT_EXTRACT_CONFIG,
    AKTIVITETSUDFOERELSE_EXTRACT_CONFIG,
    AKTIVITETSUDFOERELSE_SPECIALE_EXTRACT_CONFIG,
    BOM_IFDB_AFTALE_EXTRACT_CONFIG,
    BOM_IFDB_AFTALERESSOURCEDELTAGER_EXTRACT_CONFIG,
    BOM_IFDB_ORGENHED_SOR_EXTRACT_CONFIG,
    BOM_IFDB_PATIENTINFO_EXTRACT_CONFIG,
    DIAGNOSE_EXTRACT_CONFIG,
    DIAGNOSE_TILLAEGSKODE_EXTRACT_CONFIG,
    ENCOUNTER_EXTRACT_CONFIG,
    EOC_EXTRACT_CONFIG,
    HENVISNING_EXTRACT_CONFIG,
    HENVISNING_HISTORIK_EXTRACT_CONFIG,
    LOCATION_EXTRACT_CONFIG,
    PAS_DIAGNOSE_ENCOUNTER_REF_EXTRACT_CONFIG,
    PAS_DIAGNOSE_EXTRACT_CONFIG,
    PAS_DIAGNOSE_HENVISNING_REF_EXTRACT_CONFIG,
    PAS_DIAGNOSE_TILLAEGSKODE_EXTRACT_CONFIG,
    PATIENT_EXTRACT_CONFIG,
    REFERRALINFORMATION_EXTRACT_CONFIG,
    RIS_YDELSE_EXTRACT_CONFIG,
)

_RES = "mssql_target_staging"  # arc.* live on the Staging server (DSA)
_DELETED = "dw_is_deleted"  # archive's "removed from source" marker

# (name, arc_table, ifdb_oracle_source_table, archive_config, extract_config).
# merge_keys come from the archive config; the IFDB flow-start upper-bound column
# comes from the extract config. Both stay single-sourced from the DFM configs.
#
# A join-delta extract's bound column lives on the parent, so its subject repeats
# the join as a derived table -- otherwise the column isn't in scope (ORA-00904).
_SOURCE = "ifdb"  # IFDB Oracle; other arc.* sources (kmd/ils/...) get their own

# name -> anchor dqa_<name>_weekly (source in the name so it namespaces by source).
_ARC = [
    (
        "arc_ifdb_encounter",
        "arc.ifdb_encounter",
        "IFDB.ENCOUNTER",
        ENCOUNTER_ARCHIVE_CONFIG,
        ENCOUNTER_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_episode_of_care",
        "arc.ifdb_episode_of_care",
        "IFDB.EPISODE_OF_CARE",
        EOC_ARCHIVE_CONFIG,
        EOC_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_patient",
        "arc.ifdb_patient",
        "IFDB.PATIENT_OPLYSNINGER_COLUMNA",
        PATIENT_ARCHIVE_CONFIG,
        PATIENT_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_location",
        "arc.ifdb_location",
        "(SELECT l.OBJECT_ID, l.PERIOD_START, e.EPJI_MODIFICATION_DATETIME "
        "FROM IFDB.LOCATION l "
        "INNER JOIN IFDB.ENCOUNTER e ON e.OBJECT_ID = l.OBJECT_ID) loc",
        LOCATION_ARCHIVE_CONFIG,
        LOCATION_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_diagnose",
        "arc.ifdb_diagnose",
        "IFDB.DIAGNOSE",
        DIAGNOSE_ARCHIVE_CONFIG,
        DIAGNOSE_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_diagnose_tillaegskode",
        "arc.ifdb_diagnose_tillaegskode",
        "IFDB.DIAGNOSE_TILLAEGSKODE",
        DIAGNOSE_TILLAEGSKODE_ARCHIVE_CONFIG,
        DIAGNOSE_TILLAEGSKODE_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_pas_diagnose",
        "arc.ifdb_pas_diagnose",
        "IFDB.PAS_DIAGNOSE",
        PAS_DIAGNOSE_ARCHIVE_CONFIG,
        PAS_DIAGNOSE_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_pas_diagnose_encounter_ref",
        "arc.ifdb_pas_diagnose_encounter_ref",
        "IFDB.PAS_DIAGNOSE_ENCOUNTER_REF",
        PAS_DIAGNOSE_ENCOUNTER_REF_ARCHIVE_CONFIG,
        PAS_DIAGNOSE_ENCOUNTER_REF_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_pas_diagnose_tillaegskode",
        "arc.ifdb_pas_diagnose_tillaegskode",
        "IFDB.PAS_DIAGNOSE_TILLAEGSKODE",
        PAS_DIAGNOSE_TILLAEGSKODE_ARCHIVE_CONFIG,
        PAS_DIAGNOSE_TILLAEGSKODE_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_pas_diagnose_henvisning_ref",
        "arc.ifdb_pas_diagnose_henvisning_ref",
        "IFDB.PAS_DIAGNOSE_HENVISNING_REF",
        PAS_DIAGNOSE_HENVISNING_REF_ARCHIVE_CONFIG,
        PAS_DIAGNOSE_HENVISNING_REF_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_administrative_procedure",
        "arc.ifdb_administrative_procedure",
        "IFDB.ADMINISTRATIVE_PROCEDURE",
        ADMINISTRATIVE_PROCEDURE_ARCHIVE_CONFIG,
        ADMINISTRATIVE_PROCEDURE_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_adm_procedure_suppl_codes",
        "arc.ifdb_adm_procedure_suppl_codes",
        "IFDB.ADM_PROCEDURE_SUPPL_CODES",
        ADM_PROCEDURE_SUPPL_CODES_ARCHIVE_CONFIG,
        ADM_PROCEDURE_SUPPL_CODES_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_ris_ydelse",
        "arc.ifdb_ris_ydelse",
        "IFDB.RIS_YDELSE",
        RIS_YDELSE_ARCHIVE_CONFIG,
        RIS_YDELSE_EXTRACT_CONFIG,
    ),
    # ServiceRequest (henvisning side)
    (
        "arc_ifdb_henvisning",
        "arc.ifdb_henvisning",
        "IFDB.HENVISNING",
        HENVISNING_ARCHIVE_CONFIG,
        HENVISNING_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_henvisning_historik",
        "arc.ifdb_henvisning_historik",
        "IFDB.HENVISNING_HISTORIK",
        HENVISNING_HISTORIK_ARCHIVE_CONFIG,
        HENVISNING_HISTORIK_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_referralinformation",
        "arc.ifdb_referralinformation",
        "IFDB.REFERRALINFORMATION",
        REFERRALINFORMATION_ARCHIVE_CONFIG,
        REFERRALINFORMATION_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_aktivitetsresultat",
        "arc.ifdb_aktivitetsresultat",
        "IFDB.AKTIVITETSRESULTAT",
        AKTIVITETSRESULTAT_ARCHIVE_CONFIG,
        AKTIVITETSRESULTAT_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_aktivitetsudfoerelse",
        "arc.ifdb_aktivitetsudfoerelse",
        "IFDB.AKTIVITETSUDFOERELSE",
        AKTIVITETSUDFOERELSE_ARCHIVE_CONFIG,
        AKTIVITETSUDFOERELSE_EXTRACT_CONFIG,
    ),
    (
        "arc_ifdb_aktivitetsudfoerelse_speciale",
        "arc.ifdb_aktivitetsudfoerelse_speciale",
        "IFDB.AKTIVITETSUDFOERELSE_SPECIALE",
        AKTIVITETSUDFOERELSE_SPECIALE_ARCHIVE_CONFIG,
        AKTIVITETSUDFOERELSE_SPECIALE_EXTRACT_CONFIG,
    ),
    # BOM_IFDB (Appointment-familien + Location_SOR). Ingen af tabellerne har
    # delete detection i det daglige flow (join-delta/delta-slices), saa den
    # ugentlige reconciliation her ER mekanismen, der goer fjernede raekker
    # synlige (vanished_not_marked_deleted), og dqa_arc_ifdb_vanished_cleanup
    # er den, der pensionerer dem. Noeglerne er Oracle NUMBER uden _ID-suffix,
    # derfor eksplicit normalize_columns.
    (
        "arc_bom_ifdb_aftale",
        "arc.bom_ifdb_aftale",
        "BOM_IFDB.AFTALE",
        BOM_IFDB_AFTALE_ARCHIVE_CONFIG,
        BOM_IFDB_AFTALE_EXTRACT_CONFIG,
        ["OID"],
    ),
    (
        "arc_bom_ifdb_aftaleressourcedeltager",
        "arc.bom_ifdb_aftaleressourcedeltager",
        "BOM_IFDB.AFTALERESSOURCEDELTAGER",
        BOM_IFDB_AFTALERESSOURCEDELTAGER_ARCHIVE_CONFIG,
        BOM_IFDB_AFTALERESSOURCEDELTAGER_EXTRACT_CONFIG,
        ["OID"],
    ),
    (
        "arc_bom_ifdb_patientinfo",
        "arc.bom_ifdb_patientinfo",
        "BOM_IFDB.PATIENTINFO",
        BOM_IFDB_PATIENTINFO_ARCHIVE_CONFIG,
        BOM_IFDB_PATIENTINFO_EXTRACT_CONFIG,
        ["PATIENTINFOOID"],
    ),
    (
        "arc_bom_ifdb_orgenhed_sor",
        "arc.bom_ifdb_orgenhed_sor",
        "(SELECT oe.OID, se.SORKODE FROM BOM_IFDB.ORGENHED oe "
        "INNER JOIN BOM_IFDB.SORENHED se ON se.OID = oe.SORENHEDOID) org",
        BOM_IFDB_ORGENHED_SOR_ARCHIVE_CONFIG,
        BOM_IFDB_ORGENHED_SOR_EXTRACT_CONFIG,
        ["OID"],
    ),
]


def _arc(
    name: str,
    table: str,
    source_table: str,
    cfg,
    extract_cfg,
    normalize_columns: list[str] | None = None,
) -> DqaSubject:
    key_columns = list(cfg.merge_keys)
    params = {
        "source_table": source_table,
        "key_columns": key_columns,
        "deleted_column": _DELETED,
        # archive_reconciliation is source-generic; these pin it to IFDB Oracle.
        # A future arc source (SydAX/MySupply/...) passes dialect="mssql" + its
        # own source_resource in its arc_<source>_subjects.py.
        "dialect": "oracle",
        "source_resource": "ifdb_oracle",
    }
    if normalize_columns:
        params["normalize_columns"] = list(normalize_columns)
    # Delta tables: bound the MISSING direction by the same flow-start +
    # upper-bound column the extract uses -- both taken from the extract config
    # (flow_start_from names the meta.DFMState row), so DQA can't drift from the
    # extract's delta window. A full-load source has neither -> reconciled
    # unbounded.
    bound_col = getattr(extract_cfg, "upper_bound_column", None)
    if bound_col:
        bound_col = bound_col.split(".")[-1]  # join-delta qualifies it; the view doesn't
    fs_from = getattr(extract_cfg, "flow_start_from", None)
    if bound_col:
        params["flow_start_bound_column"] = bound_col
        if fs_from:
            params["flow_start_state"] = list(fs_from)

    # Delta tables with a SINGLE merge key get an arc replay: missing_in_arc keys
    # are captured to dqa.RemediationQueue (replay_entity = the subject name, so
    # they never leak into the FHIR dqa.v_replay_* feeder views) and fixed by
    # dqa_arc_ifdb_replay -- an ids-mode re-extract + archive merge that does NOT
    # advance the DFM checkpoint. Location is excluded: its merge key is
    # composite (OBJECT_ID + PERIOD_START) and the replay filters on a single
    # column, so its gaps are closed with a period_orchestrator full_load
    # instead.
    replay = None
    if bound_col and len(key_columns) == 1:
        replay = ReplaySpec(key_column=key_columns[0], entity=name, filter_column=key_columns[0])
        params["capture_replay"] = True

    return DqaSubject(
        name=name,
        track=Track.ARCHIVE,
        source=_SOURCE,
        table=table,
        resource_key=_RES,
        identity_column=key_columns[0],
        checks=(
            CheckSpecification(
                check_type="archive_reconciliation",
                severity=Severity.ERROR,
                cadence=Cadence.WEEKLY,
                params=params,
                name_suffix="vs_ifdb",
                requires=("ifdb_oracle",),
            ),
        ),
        replay=replay,
    )


ARC_SUBJECTS: list[DqaSubject] = [_arc(*spec) for spec in _ARC]

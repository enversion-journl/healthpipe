# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DQA subject configs for the flat FHIR tables (DSA / mssql_target_staging).

Daily checks are cheap, single-table uniqueness (required columns -- PKs and the
patient references -- are enforced ``NOT NULL`` in the DDL, not by a check). Weekly
checks are the heavier referential-integrity checks, auto-expanded from the FHIR
lineage ``FK-Target`` edges via ``references.referential_specs`` -- so adding or
removing a relationship in the lineage YAML adds/removes the check here.

``depends_on`` is intentionally empty for now; it will be set to the SoF sync
asset keys when we wire the post-ETL sensor (orchestration phase).
"""

from __future__ import annotations

from dqa.model import Cadence, CheckSpecification, DqaSubject, ReplaySpec, Severity, Track
from dqa.references import referential_specs

_RES = "mssql_target_staging"  # DSA


def _fhir(
    name: str,
    table: str,
    identity: str,
    daily: list[CheckSpecification],
    extra_weekly: list[CheckSpecification] | None = None,
    replay: ReplaySpec | None = None,
    deleted_column: str | None = None,
) -> DqaSubject:
    """A flat FHIR subject: daily checks + lineage-driven weekly refs + any extras.

    ``deleted_column`` (set for tables that have ``dw_is_deleted``) scopes the
    row-level checks (not_null, uniqueness, referential_integrity) to active rows.
    """
    return DqaSubject(
        name=name,
        track=Track.FHIR,
        table=table,
        resource_key=_RES,
        identity_column=identity,
        checks=(
            tuple(daily)
            + tuple(referential_specs(table, cadence=Cadence.WEEKLY))
            + tuple(extra_weekly or ())
        ),
        replay=replay,
        deleted_column=deleted_column,
    )


def _reconcile_vs_archive(
    source_table: str,
    *,
    source_key: str = "OBJECT_ID",
    target_key: str = "identifier_value",
    source_deleted: str | None = "DELETED",
    target_deleted: str | None = "dw_is_deleted",
    source_filter: str | None = None,
    target_filter: str | None = None,
    name_suffix: str = "vs_arc",
    capture_replay: bool = False,
) -> CheckSpecification:
    """Reconcile a flat FHIR table's business key against its arc.* source archive.

    Compares the full key set (missing/orphaned); when both delete columns are
    given, also checks the soft-delete flag agrees for keys present on both sides
    (delete propagation). Pass ``source_deleted=None`` to compare key presence
    only -- e.g. at a grain (CPR) where a single per-key delete flag isn't
    well-defined.

    ``capture_replay=True`` enqueues the gap keys (arc rows missing from the flat
    table + arc-deleted rows whose delete never propagated) to
    ``dqa.RemediationQueue`` for ``dqa_remediation_replay``. Only set it where the
    subject's ``ReplaySpec.filter_column`` equals ``source_key`` (the capture
    no-ops with a warning otherwise)."""
    return CheckSpecification(
        "source_reconciliation",
        Severity.ERROR,
        Cadence.WEEKLY,
        {
            "source_table": source_table,
            "source_key": source_key,
            "target_key": target_key,
            "source_deleted": source_deleted,
            "target_deleted": target_deleted,
            "source_filter": source_filter,
            "target_filter": target_filter,
            "capture_replay": capture_replay,
        },
        name_suffix=name_suffix,
    )


def _sks_procedure_prefix_sql(column: str = "AKTIVITETTYPE_KODE") -> str:
    """SQL-praedikat for "denne kilderaekke bliver en Procedure".

    Genereres af de repo-styrede koderegler (samme fil som pipelinens
    klassifikation laeser), saa daekningens afgraensning foelger reglerne
    automatisk, naar de aendres."""
    from DSA.FHIR.utils.code_normalization import load_ifdb_procedure_code_rules

    rules = load_ifdb_procedure_code_rules()

    def _lit(value: str) -> str:
        if "'" in value or ";" in value:
            raise ValueError(f"Koderegel-vaerdi uegnet som SQL-literal: {value!r}")
        return value

    parts = [f"[{column}] LIKE '{_lit(pfx)}%'" for pfx in rules.get("prefixes", [])]
    parts += [f"[{column}] = '{_lit(code)}'" for code in rules.get("exact", [])]
    positive = " OR ".join(parts) if parts else "1 = 0"
    negative = ""
    if rules.get("exclude_prefixes"):
        negative = (
            " AND NOT ("
            + " OR ".join(f"[{column}] LIKE '{_lit(pfx)}%'" for pfx in rules["exclude_prefixes"])
            + ")"
        )
    return f"({positive}){negative}"


def _procedure_slice_coverage(
    *,
    slice_name: str,
    source_table: str,
    source_key: str,
    source_scope: str,
    system: str,
) -> CheckSpecification:
    """Daekning for en af proceduretabellens tre kildeskiver: hver levende
    kilderaekke i arkivet skal findes som raekke i den flade tabel.

    ENVEJS med vilje: arkivtabellerne paa delte miljoeer holder et vindue, ikke
    fuld historik, saa "ekstra i maal" ville stoeje. Noeglen er source_key +
    source_system (samme raekkefoelge i view og tabel), og boern (deloperationer
    og RIS-underydelser) rammes aldrig: deres noegle er sammensat og matcher
    ikke kildens."""
    return CheckSpecification(
        "custom_sql",
        Severity.WARN,
        Cadence.WEEKLY,
        {
            "sql": (
                f"SELECT COUNT(*) FROM {source_table} a "
                f"WHERE {source_scope} "
                f"AND NOT EXISTS (SELECT 1 FROM [fhir].[Procedure] p "
                f"WHERE p.[source_identifier_value] = CAST(a.[{source_key}] AS NVARCHAR(255)) "
                f"AND p.[source_system] = '{system}')"
            ),
            "checked_sql": f"SELECT COUNT(*) FROM {source_table} a WHERE {source_scope}",
            "examples_sql": (
                f"SELECT TOP 20 CAST(a.[{source_key}] AS NVARCHAR(255)) FROM {source_table} a "
                f"WHERE {source_scope} "
                f"AND NOT EXISTS (SELECT 1 FROM [fhir].[Procedure] p "
                f"WHERE p.[source_identifier_value] = CAST(a.[{source_key}] AS NVARCHAR(255)) "
                f"AND p.[source_system] = '{system}')"
            ),
        },
        name_suffix=f"daekning_{slice_name}",
    )


def _reconcile_vs_fhir_server(
    resource_table: str,
    *,
    deleted_column: str | None = "dw_is_deleted",
    report_missing_in_flat: bool = True,
    severity: Severity = Severity.ERROR,
) -> CheckSpecification:
    """Reconcile a flat FHIR table against the live FHIR server (Aidbox).

    The daily SoF extraction is delta-by-timestamp and never sees a deletion, so a
    resource hard-deleted in Aidbox lingers in the flat table (surfacing as a
    duplicate business key). Flags active flat rows whose resource id is gone from
    the server. ``deleted_column=None`` for a flat table without a soft-delete
    column (fhir.Organization/Location) -> detection only, no cleanup possible.
    Pass ``report_missing_in_flat=False`` for a sparse projection (e.g.
    Encounter_Location) where a server id with no flat row is expected."""
    params: dict = {"resource_table": resource_table, "deleted_column": deleted_column}
    if not report_missing_in_flat:
        params["report_missing_in_flat"] = False
    return CheckSpecification(
        "fhir_server_reconciliation",
        severity,
        Cadence.WEEKLY,
        params,
        name_suffix="vs_fhir_server",
        requires=("fhir_database",),
    )


FHIR_SUBJECTS: list[DqaSubject] = [
    _fhir(
        "fhir_encounter",
        "fhir.Encounter",
        "encounter_id",
        [
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                # deleted_column=None: STRICT (all rows) -- a duplicate resource id,
                # even soft-deleted, would indicate a projection bug.
                {"grain": "encounter_id", "deleted_column": None},
                name_suffix="resource_id",
            ),
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                {"grain": "identifier_value"},
                name_suffix="business_key",
            ),
        ],
        extra_weekly=[
            _reconcile_vs_archive("arc.ifdb_encounter", capture_replay=True),
            # Flat table vs the FHIR server (Aidbox): flags active rows whose
            # encounter_id was hard-deleted on the server but not propagated here
            # (delta SoF can't see deletions). Fixed by dqa_fhir_server_cleanup.
            _reconcile_vs_fhir_server("encounter"),
        ],
        replay=ReplaySpec("identifier_value", "ifdb_encounter", "OBJECT_ID"),
        deleted_column="dw_is_deleted",
    ),
    _fhir(
        "fhir_episode_of_care",
        "fhir.EpisodeOfCare",
        "episode_of_care_id",
        [
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                # deleted_column=None: STRICT (see fhir_encounter's resource_id check).
                {"grain": "episode_of_care_id", "deleted_column": None},
                name_suffix="resource_id",
            ),
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                {"grain": "identifier_value"},
                name_suffix="business_key",
            ),
        ],
        extra_weekly=[
            _reconcile_vs_archive("arc.ifdb_episode_of_care", capture_replay=True),
            _reconcile_vs_fhir_server("episodeofcare"),
        ],
        replay=ReplaySpec("identifier_value", "ifdb_episode_of_care", "OBJECT_ID"),
        deleted_column="dw_is_deleted",
    ),
    _fhir(
        "fhir_patient",
        "fhir.Patient",
        "patient_id",
        [
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                # deleted_column=None: STRICT (see fhir_encounter's resource_id check).
                {"grain": "patient_id", "deleted_column": None},
                name_suffix="resource_id",
            ),
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                {"grain": "ifdb_patient_id"},
                name_suffix="ifdb_id",
            ),
        ],
        extra_weekly=[
            # Person-level: every CPR in arc reached fhir. Clean 1:1 (arc.CPR ==
            # fhir.patient_ssn); key-set only -- at CPR grain a single delete flag
            # isn't well-defined (the ETL dedups to one row per CPR).
            _reconcile_vs_archive(
                "arc.ifdb_patient",
                source_key="CPR",
                target_key="patient_ssn",
                source_deleted=None,
                name_suffix="vs_arc_cpr",
            ),
            # patient_id-level: dedup-aware. A deleted PATIENT_ID dropped per CPR is
            # NOT a gap (a plain source_reconciliation would false-flag it), so this
            # asserts only that the *chosen* ifdb_patient_id is valid, non-deleted-
            # when-possible, and delete-consistent.
            CheckSpecification(
                "patient_arc_integrity",
                Severity.ERROR,
                Cadence.WEEKLY,
                {
                    "source_table": "arc.ifdb_patient",
                    "source_key": "PATIENT_ID",
                    "source_cpr": "CPR",
                    "source_deleted": "SLETTET",
                    "target_key": "ifdb_patient_id",
                    "target_deleted": "dw_is_deleted",
                },
                name_suffix="vs_arc_patient_id",
            ),
            # vs the FHIR server: a patient resource hard-deleted in Aidbox (e.g.
            # duplicate-resource cleanup) that lingers here. Distinct from the
            # arc-driven delete propagation above -- fixed by dqa_fhir_server_cleanup.
            _reconcile_vs_fhir_server("patient"),
        ],
        replay=ReplaySpec("ifdb_patient_id", "ifdb_patient", "PATIENT_ID"),
        deleted_column="dw_is_deleted",
    ),
    _fhir(
        "fhir_organization",
        "fhir.Organization",
        "organization_id",
        [
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                # deleted_column=None: STRICT (see fhir_encounter's resource_id check).
                {"grain": "organization_id", "deleted_column": None},
                name_suffix="resource_id",
            ),
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                {"grain": "identifier_value_sor"},
                name_suffix="sor_code",
            ),
        ],
        extra_weekly=[
            _reconcile_vs_fhir_server("organization"),
        ],
        deleted_column="dw_is_deleted",
    ),
    _fhir(
        "fhir_location",
        "fhir.Location",
        "location_id",
        [
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                # deleted_column=None: STRICT (see fhir_encounter's resource_id check).
                {"grain": "location_id", "deleted_column": None},
                name_suffix="resource_id",
            ),
            CheckSpecification(
                "uniqueness",
                Severity.WARN,
                Cadence.DAILY,
                {"grain": "identifier_value_sor"},
                name_suffix="sor_code",
            ),
        ],
        extra_weekly=[
            # Person-level analogue of patient's vs_arc_cpr: the transform dedups
            # arc.ifdb_location to DISTINCT LOCATION_SORCODE (many OBJECT_IDs ->
            # one Location), so reconcile on the SOR code, not OBJECT_ID. Key-set
            # only: the deduped SOR grain has no per-key delete flag on the arc side.
            # The source side filters dw_is_deleted=0 to match what the transform posts
            # (build_fhir_archive_query default), else SOR codes present only in
            # warehouse-deleted rows would false-flag as missing. No capture_replay:
            # Location has no DFM ids-mode replay pipeline (detection only).
            _reconcile_vs_archive(
                "arc.ifdb_location",
                source_key="LOCATION_SORCODE",
                target_key="identifier_value_sor",
                source_deleted=None,
                target_deleted=None,
                source_filter="[dw_is_deleted] = 0",
            ),
            _reconcile_vs_fhir_server("location"),
        ],
        deleted_column="dw_is_deleted",
    ),
    # Derived table: referential checks (composite PK; inherits its keys) + the
    # FHIR-server reconciliation (same Encounter resource, finer grain -> rows go
    # stale when an encounter is deleted on the server, just like fhir.Encounter).
    _fhir(
        "fhir_encounter_location",
        "fhir.Encounter_Location",
        "encounter_id",
        [],
        extra_weekly=[
            # Same Encounter resource at a finer grain: not every encounter has a
            # location row, so a server id with no flat row is expected (not a finding).
            _reconcile_vs_fhir_server("encounter", report_missing_in_flat=False),
        ],
        deleted_column="dw_is_deleted",
    ),
    # Multi-source flat table: DIAGNOSE + PAS_DIAGNOSE are DFM-backed (vs_arc per
    # source_system slice below); ServiceRequest referral-conditions and
    # Aktivitetsresultat conditions are DFM-backed too but not
    # arc-reconciled yet. No ReplaySpec: the business
    # key is composite (source_system + source_identifier_value) across two arc
    # entities, which the single-entity replay pipeline cannot express.
    _fhir(
        "fhir_condition",
        "fhir.Condition",
        "condition_id",
        [
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                {"grain": "condition_id"},
                name_suffix="resource_id",
            ),
            CheckSpecification(
                "uniqueness",
                Severity.WARN,
                Cadence.DAILY,
                {"grain": "source_system,source_identifier_value"},
                name_suffix="business_key",
            ),
        ],
        # No dw_is_deleted column -> key-presence reconciliation only. Soft-deleted
        # diagnoses ARE posted (entered-in-error), so every arc key must exist in
        # the flat table regardless of SLETTET.
        extra_weekly=[
            _reconcile_vs_archive(
                "arc.ifdb_diagnose",
                source_key="OBJEKT_ID",
                target_key="source_identifier_value",
                source_deleted=None,
                target_deleted=None,
                target_filter="[source_system] = 'urn:source:ifdb:diagnose'",
                name_suffix="vs_arc_diagnose",
            ),
            _reconcile_vs_archive(
                "arc.ifdb_pas_diagnose",
                source_key="OBJEKT_ID",
                target_key="source_identifier_value",
                source_deleted=None,
                target_deleted=None,
                target_filter="[source_system] = 'urn:source:ifdb:pas_diagnose'",
                name_suffix="vs_arc_pas_diagnose",
            ),
            _reconcile_vs_fhir_server("condition", deleted_column=None, severity=Severity.WARN),
        ],
    ),
    # Multi-source flat table: AdminProc + RisYdelse are DFM-backed, and the
    # Aktivitetsresultat slice is DFM-backed too. No vs_arc
    # checks yet: the flat table carries no generic source_identifier_value
    # column (only source_system + the SFI-specific ifdb_aktivitet_id), and the
    # RisYdelse grain is OBJEKT_ID:ElementNummer with deliberate non-final-STATE
    # skips — add reconciliation when the view exposes a per-slice business key.
    # No ReplaySpec (multi-source composite grain).
    _fhir(
        "fhir_procedure",
        "fhir.Procedure",
        "procedure_id",
        [
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                {"grain": "procedure_id"},
                name_suffix="resource_id",
            ),
            # Byggelaget garanterer, at varigheden aldrig er negativ (et
            # negativt spaend udelades). Findes en, er det en pipelinefejl.
            CheckSpecification(
                "custom_sql",
                Severity.ERROR,
                Cadence.DAILY,
                {
                    "sql": "SELECT COUNT(*) FROM [fhir].[Procedure] WHERE [knivtid_minutter] < 0",
                    "checked_sql": "SELECT COUNT(*) FROM [fhir].[Procedure] WHERE [knivtid_minutter] IS NOT NULL",
                },
                name_suffix="knivtid_negativ",
            ),
            # Datakvalitetssignal fra kilden, ikke en pipelinefejl: maalt til
            # 53 af ~300.000 med knivtid. Overvaages saa en stigning opdages.
            CheckSpecification(
                "custom_sql",
                Severity.WARN,
                Cadence.DAILY,
                {
                    "sql": (
                        "SELECT COUNT(*) FROM [fhir].[Procedure] "
                        "WHERE [knivtid_start] IS NOT NULL AND [knivtid_slut] IS NULL"
                    ),
                    "checked_sql": "SELECT COUNT(*) FROM [fhir].[Procedure] WHERE [knivtid_start] IS NOT NULL",
                },
                name_suffix="knivtid_start_uden_slut",
            ),
        ],
        extra_weekly=[
            _reconcile_vs_fhir_server("procedure", deleted_column=None, severity=Severity.WARN),
            # --- Raekkeafstemning pr. kilde (daekning: alt i arkivet skal ---
            # --- findes i den flade tabel). Bevidst ENVEJS: arkivet paa   ---
            # --- delte miljoeer er et vindue, ikke fuld historik, saa den ---
            # --- modsatte retning ville drukne i falske "ekstra" raekker. ---
            # WARN indtil genkoerslen har befolket de nye noeglekolonner;
            # herefter boer de haeves til ERROR.
            _procedure_slice_coverage(
                slice_name="sfi",
                source_table="arc.ifdb_aktivitetsresultat",
                source_key="OBJEKT_ID",
                # Afgraenses via SKS-kodereglerne -- samme repo-styrede kilde
                # som selve klassifikationen, saa check og pipeline aldrig kan
                # divergere. Revisionstabellen bruges bevidst IKKE: den er
                # besluttet fjernet (25 GB, informationsfri default-rute).
                source_scope=("[SLETTET] <> '1' AND (" + _sks_procedure_prefix_sql() + ")"),
                system="urn:source:ifdb:aktivitetsresultat_procedure",
            ),
            _procedure_slice_coverage(
                slice_name="adm",
                source_table="arc.ifdb_administrative_procedure",
                source_key="OBJECT_ID",
                source_scope="[DELETED] <> '1'",
                system="urn:source:ifdb:administrative_procedure",
            ),
            _procedure_slice_coverage(
                slice_name="ris",
                source_table="arc.ifdb_ris_ydelse",
                source_key="OBJEKT_ID",
                # Kun endelige svar bygges; oevrige STATE-vaerdier springes
                # bevidst over af udtraekket.
                source_scope=(
                    "[SLETTET] <> '1' AND [STATE] IN ('FINAL_REPORT', 'MODIFIED_REPORT')"
                ),
                system="urn:source:ifdb:ris_ydelse",
            ),
            # Kanttilfaelde: hovedkode-kolonnen tom, men aktive tillaegskoder
            # findes -- signaturen paa en raekke bygget foer valgt-markeringen.
            # Maalt til 76 % af bestanden foer genkoerslen; skal vaere nul efter.
            CheckSpecification(
                "custom_sql",
                Severity.WARN,
                Cadence.WEEKLY,
                {
                    "sql": (
                        "SELECT COUNT(*) FROM [fhir].[Procedure] p "
                        "WHERE p.[code] IS NULL AND EXISTS ("
                        "SELECT 1 FROM [fhir].[Procedure_tillaegskode] t "
                        "WHERE t.[procedure_id] = p.[procedure_id] AND t.[dw_is_deleted] = 0)"
                    ),
                    "checked_sql": "SELECT COUNT(*) FROM [fhir].[Procedure]",
                },
                name_suffix="hovedkode_i_tillaegskoder",
            ),
            # Kanttilfaelde: kladde i kilden men gennemfoert i den flade tabel.
            # Skal vaere nul efter genkoerslen (kladder faar status in-progress).
            CheckSpecification(
                "custom_sql",
                Severity.WARN,
                Cadence.WEEKLY,
                {
                    "sql": (
                        "SELECT COUNT(*) FROM arc.ifdb_aktivitetsresultat a "
                        "JOIN [fhir].[Procedure] p ON p.[source_identifier_value] = CAST(a.[OBJEKT_ID] AS NVARCHAR(255)) "
                        "AND p.[source_system] = 'urn:source:ifdb:aktivitetsresultat_procedure' "
                        "WHERE a.[KLADDE] = '1' AND p.[status] = 'completed'"
                    ),
                    "checked_sql": (
                        "SELECT COUNT(*) FROM arc.ifdb_aktivitetsresultat WHERE [KLADDE] = '1'"
                    ),
                },
                name_suffix="kladde_som_gennemfoert",
            ),
            # Slet-propagering: slettet i kilden, men stadig aktiv i den flade
            # tabel. Proceduretabellen har ingen sletteflag-kolonne, saa status
            # er markoeren.
            CheckSpecification(
                "custom_sql",
                Severity.WARN,
                Cadence.WEEKLY,
                {
                    "sql": (
                        "SELECT COUNT(*) FROM arc.ifdb_aktivitetsresultat a "
                        "JOIN [fhir].[Procedure] p ON p.[source_identifier_value] = CAST(a.[OBJEKT_ID] AS NVARCHAR(255)) "
                        "AND p.[source_system] = 'urn:source:ifdb:aktivitetsresultat_procedure' "
                        "WHERE a.[SLETTET] = '1' AND p.[status] <> 'entered-in-error'"
                    ),
                    "checked_sql": (
                        "SELECT COUNT(*) FROM arc.ifdb_aktivitetsresultat WHERE [SLETTET] = '1'"
                    ),
                },
                name_suffix="slet_propagering",
            ),
        ],
    ),
    # DFM-backed (henvisning + RIS streams; RehabilitationPlan
    # slice was already DFM). No vs_arc checks yet: the flat table spans three
    # source slices whose business keys live in different arc tables
    # (arc.ifdb_henvisning OBJEKT_ID, arc.ifdb_ris_ydelse OBJEKT_ID with
    # deliberate non-final-STATE skips, HealthIssue for rehab plans) — add
    # per-slice reconciliation on source_table when the grains are pinned.
    # No ReplaySpec (multi-source grain, same reasoning as fhir_procedure).
    _fhir(
        "fhir_service_request",
        "fhir.ServiceRequest",
        "service_request_id",
        [
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                {"grain": "service_request_id", "deleted_column": None},
                name_suffix="resource_id",
            ),
            CheckSpecification(
                "uniqueness",
                Severity.WARN,
                Cadence.DAILY,
                {"grain": "identifier_system,identifier_value"},
                name_suffix="business_key",
            ),
        ],
        extra_weekly=[_reconcile_vs_fhir_server("servicerequest")],
        deleted_column="dw_is_deleted",
    ),
    # DFM-backed: reads arc.bom_ifdb_aftale (soft-delete from DATOSLETTET ->
    # dw_is_deleted; the transform posts soft-deleted rows, so key presence AND
    # delete propagation reconcile). Business key: identifier_value = AFTALE.OID.
    _fhir(
        "fhir_appointment",
        "fhir.Appointment",
        "appointment_id",
        [
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                {"grain": "appointment_id", "deleted_column": None},
                name_suffix="resource_id",
            ),
            # WARN until the business-key grain is confirmed against real data.
            CheckSpecification(
                "uniqueness",
                Severity.WARN,
                Cadence.DAILY,
                {"grain": "identifier_value"},
                name_suffix="business_key",
            ),
        ],
        extra_weekly=[
            _reconcile_vs_archive(
                "arc.bom_ifdb_aftale",
                source_key="OID",
                source_deleted="dw_is_deleted",
                capture_replay=True,
            ),
            _reconcile_vs_fhir_server("appointment"),
        ],
        replay=ReplaySpec("identifier_value", "bom_ifdb_aftale", "OID"),
        deleted_column="dw_is_deleted",
    ),
]

# ---------------------------------------------------------------------------
# Non-DFM resources (legacy daily FHIR pipeline) -- NO REPLAY REMEDIATION.
#
# These resources have NOT been migrated to DFM, so there is no ids-mode replay
# pipeline for them: no ReplaySpec, no capture_replay, and referential findings
# are NOT enqueued to dqa.RemediationQueue (the capture no-ops without a
# ReplaySpec). There are also no arc.* archives for them, so no source/oracle
# reconciliation. Fixing a finding here means re-running the legacy daily job
# for the affected period / fixing the transform. Their server reconciliation
# runs as detection-only WARN until a dw_is_deleted column is added.
#
# When one of these resources is migrated to DFM: add ReplaySpec + vs_arc checks
# and move it up to the DFM-backed list above.
#
# fhir_careplan and fhir_careplan_activity are the exception in this list: their
# producer HAS moved to DFM and reads arc.ifdb_eventmarker, so the first two
# sentences above no longer describe them. They stay here because a vs_arc check
# for CarePlan is not the same shape as the others: elsewhere one archive row
# (OBJECT_ID) is one resource, whereas CarePlan is many marker rows collapsed
# into one resource per PERSON, keyed on the FHIR patient id — a value that does
# not appear in the archive at all. The reconciliation has to go through the
# patient bridge, and that is its own piece of work rather than a config entry.
# ---------------------------------------------------------------------------

_NON_DFM_SUBJECTS: list[DqaSubject] = [
    _fhir(
        "fhir_observation",
        "fhir.Observation",
        "observation_id",
        [
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                {"grain": "observation_id"},
                name_suffix="resource_id",
            ),
        ],
        # No dw_is_deleted -> detection only. NB: Observation is the largest flat table;
        # the weekly server-id staging + anti-join is the heaviest check here.
        extra_weekly=[
            _reconcile_vs_fhir_server("observation", deleted_column=None, severity=Severity.WARN)
        ],
    ),
    _fhir(
        "fhir_careplan",
        "fhir.CarePlan",
        "careplan_id",
        [
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                {"grain": "careplan_id"},
                name_suffix="resource_id",
            ),
            # Alle Markoerer is one living document per person, and the CarePlan
            # is keyed on the FHIR patient id, so the UNIQUE partial index on
            # the identifier already makes a second live resource per patient
            # unwritable. This guards the *projection* rather than the server:
            # two flat rows on one patient would mean a resource whose subject
            # moved without its identifier following, or a SOF bug — either way
            # two rows per marker in the next CarePlan_Activity MERGE.
            #
            # It does NOT catch the orphan left when a Patient resource is
            # deleted and re-created: the new resource gets a new id, so the
            # old CarePlan and the new one carry DIFFERENT patient_ids and
            # never collide here.
            #
            # Nor does anything else today. The weekly referential_integrity
            # check looks like the natural net, but it is existence-only on the
            # parent by design (see referential_integrity.py: whether a resolved
            # parent is itself soft-deleted is delete-propagation, not
            # referential integrity). A deleted Patient leaves its flat row in
            # place with dw_is_deleted = 1, so the orphan's parent still
            # resolves and the check stays green. That hole closes with the
            # patient-bridged arc reconciliation, not here.
            #
            # deleted_column is explicit because the subject does not set one:
            # this is a business-key grain, where a correctly soft-deleted
            # duplicate is expected and a strict check would fail forever. The
            # careplan_id check above stays strict on purpose — a soft-deleted
            # duplicate resource id would be a projection bug.
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                {"grain": "patient_id", "deleted_column": "dw_is_deleted"},
                name_suffix="one_per_patient",
            ),
        ],
        extra_weekly=[
            # deleted_column is the DEFAULT dw_is_deleted, not None: both CarePlan
            # tables carry the column, and dqa_fhir_server_cleanup(entity=care_plan)
            # is what soft-deletes their rows when a CarePlan is hard-deleted in
            # Aidbox. Scoped to live rows, the check reports exactly what still
            # needs cleaning and goes green once it has been done — with None it
            # keeps counting the rows it already asked us to retire.
            _reconcile_vs_fhir_server("careplan", severity=Severity.WARN)
        ],
    ),
    # Finer-grain projection of the same CarePlan resource (one row per activity),
    # like Encounter_Location is to Encounter: no daily checks of its own; weekly
    # lineage refs + server reconciliation on the parent resource id.
    _fhir(
        "fhir_careplan_activity",
        "fhir.CarePlan_Activity",
        "careplan_id",
        [],
        extra_weekly=[
            # Same as the parent: the cleanup retires this table's rows on the
            # PARENT careplan_id, so the check must not re-count them.
            _reconcile_vs_fhir_server(
                "careplan",
                severity=Severity.WARN,
                report_missing_in_flat=False,
            ),
        ],
    ),
    # Finer-grain projection of Patient (one row per address period).
    _fhir(
        "fhir_patient_address",
        "fhir.Patient_Address",
        "patient_id",
        [],
        extra_weekly=[
            _reconcile_vs_fhir_server(
                "patient",
                severity=Severity.WARN,
                report_missing_in_flat=False,
            ),
        ],
        deleted_column="dw_is_deleted",
    ),
    _fhir(
        "fhir_questionnaire",
        "fhir.Questionnaire",
        "questionnaire_id",
        [
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                {"grain": "questionnaire_id"},
                name_suffix="resource_id",
            ),
        ],
        extra_weekly=[
            _reconcile_vs_fhir_server("questionnaire", deleted_column=None, severity=Severity.WARN)
        ],
    ),
    _fhir(
        "fhir_questionnaire_response",
        "fhir.QuestionnaireResponse",
        "qr_id",
        [
            CheckSpecification(
                "uniqueness",
                Severity.ERROR,
                Cadence.DAILY,
                {"grain": "qr_id"},
                name_suffix="resource_id",
            ),
        ],
        extra_weekly=[
            _reconcile_vs_fhir_server(
                "questionnaireresponse", deleted_column=None, severity=Severity.WARN
            )
        ],
    ),
]

FHIR_SUBJECTS += _NON_DFM_SUBJECTS

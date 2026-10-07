# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the DQA framework scaffolding: registry, model defaults, the
factory (anchors + bound checks + the per-run config param), and the
remediation-capture guard override. Pure/mock -- no DB.
"""

import pytest
from dqa.check_config import DqaCheckConfig
from dqa.config import ALL_SUBJECTS
from dqa.factory import build_all, build_subject
from dqa.model import Cadence, CheckSpecification, DqaSubject, Severity, Track

pytestmark = pytest.mark.unit


def test_subject_results_resource_key_defaults_to_none():
    s = DqaSubject(
        name="x", track=Track.FHIR, table="fhir.X", resource_key="r", identity_column="x_id"
    )
    assert s.results_resource_key is None
    assert s.deleted_column is None


def _subject(*checks: CheckSpecification) -> DqaSubject:
    return DqaSubject(
        name="fhir_thing",
        track=Track.FHIR,
        table="fhir.Thing",
        resource_key="mssql_target_staging",
        identity_column="thing_id",
        checks=tuple(checks),
    )


def test_build_subject_emits_anchor_per_cadence_and_named_checks():
    subj = _subject(
        CheckSpecification("not_null", Severity.ERROR, Cadence.DAILY, {"columns": ["thing_id"]}),
        CheckSpecification(
            "uniqueness", Severity.ERROR, Cadence.DAILY, {"grain": "thing_id"}, name_suffix="rid"
        ),
        CheckSpecification("custom_sql", Severity.WARN, Cadence.WEEKLY, {"sql": "SELECT 1"}),
    )
    assets, checks = build_subject(subj)

    anchor_keys = {a.key.to_user_string() for a in assets}
    assert anchor_keys == {"dqa_fhir_thing_daily", "dqa_fhir_thing_weekly"}

    check_names = {next(iter(c.check_keys)).name for c in checks}
    assert check_names == {
        "fhir_thing__not_null",
        "fhir_thing__uniqueness__rid",
        "fhir_thing__custom_sql",
    }


def test_check_config_max_keys_is_optional():
    # The factory binds `config: DqaCheckConfig` on each check (verified end-to-end);
    # here we pin the config contract: max_keys exists and is optional (None default),
    # so existing jobs need no run config.
    assert "max_keys" in DqaCheckConfig.model_fields
    assert DqaCheckConfig().max_keys is None


def test_build_all_has_unique_check_names_and_nonzero():
    assets, checks = build_all(ALL_SUBJECTS)
    assert assets and checks
    names = [next(iter(c.check_keys)).name for c in checks]
    assert len(names) == len(set(names)), "duplicate asset-check names across subjects"


def test_max_keys_override_contextvar():
    from dqa.remediation.capture import _max_keys, reset_max_keys_override, set_max_keys_override

    base = _max_keys()
    token = set_max_keys_override(base + 12345)
    try:
        assert _max_keys() == base + 12345
    finally:
        reset_max_keys_override(token)
    assert _max_keys() == base


class TestProcedureAfstemning:
    """Afstemningen for procedurefamilien: daekning pr. kildeskive + kanttilfaelde."""

    def _procedure_subject(self):
        from dqa.config.fhir_subjects import FHIR_SUBJECTS

        return next(s for s in FHIR_SUBJECTS if s.name == "fhir_procedure")

    def test_daekning_findes_for_alle_tre_kildeskiver(self):
        subject = self._procedure_subject()
        suffixes = {c.name_suffix for c in subject.checks}
        assert {"daekning_sfi", "daekning_adm", "daekning_ris"} <= suffixes

    def test_daekningen_er_envejs_og_noeglet_paa_kildenoeglen(self):
        # Arkivet paa delte miljoeer er et vindue, ikke fuld historik, saa
        # afstemningen maa aldrig taelle "ekstra i maal" som fejl.
        subject = self._procedure_subject()
        for c in subject.checks:
            if not (c.name_suffix or "").startswith("daekning_"):
                continue
            sql = c.params["sql"]
            assert "NOT EXISTS" in sql
            assert "[source_identifier_value]" in sql and "[source_system]" in sql
            assert "checked_sql" in c.params and "examples_sql" in c.params

    def test_ris_daekning_respekterer_udtraekkets_state_filter(self):
        subject = self._procedure_subject()
        ris = next(c for c in subject.checks if c.name_suffix == "daekning_ris")
        assert "FINAL_REPORT" in ris.params["sql"] and "MODIFIED_REPORT" in ris.params["sql"]

    def test_sfi_daekning_afgraenses_via_kodereglerne(self):
        # Afgraensningen genereres af de repo-styrede koderegler -- samme kilde
        # som pipelinens klassifikation -- og maa ALDRIG laese revisionstabellen,
        # som er besluttet fjernet.
        subject = self._procedure_subject()
        sfi = next(c for c in subject.checks if c.name_suffix == "daekning_sfi")
        sql = sfi.params["sql"]
        assert "IFDBAktivitetsresultatRoutingDecision" not in sql
        assert "SKS.a." in sql  # hele SKS-familien er populationen
        for ekskl in ("KPYdelser.", "SKS.a.dia", "SKS.a.til"):
            assert ekskl in sql  # eksklusionerne skal ogsaa med
        for besoeg in ("hbdHjemmebesoeg", "hbdUdebesoeg", "hbdInstitutionsbesoeg"):
            assert besoeg not in sql  # ingen særregel på skematype

    def test_kanttilfaelde_checkene_findes(self):
        subject = self._procedure_subject()
        suffixes = {c.name_suffix for c in subject.checks}
        assert {
            "hovedkode_i_tillaegskoder",
            "kladde_som_gennemfoert",
            "slet_propagering",
            "knivtid_negativ",
            "knivtid_start_uden_slut",
        } <= suffixes

    def test_foraeldreloese_daekkes_af_lineage_genererede_checks(self):
        # part_of_id og tillaegskodens procedure_id baerer FK-Target i lineage,
        # saa forældreloes-checkene autogenereres -- de maa ikke forsvinde.
        from dqa.references import referential_specs

        cols = {s.params.get("column") for s in referential_specs("fhir.Procedure")}
        assert "part_of_id" in cols
        tk = {s.params.get("column") for s in referential_specs("fhir.Procedure_tillaegskode")}
        assert "procedure_id" in tk


class TestIndeksnavne:
    """Postgres afkorter identifikatorer til 63 tegn. Kolliderer to afkortede
    indeksnavne, no-opper CREATE INDEX IF NOT EXISTS lydløst, og indekset
    findes aldrig — det skete for diagnosefamiliens præfiks-indeks og kostede
    en fuld indeksscanning pr. opslag i oprydningen (33 ms mod 0,05 ms)."""

    def test_afkortede_indeksnavne_kolliderer_ikke(self):
        from infrastructure.assets.fhir_ifdb_infrastructure import _prefix_index_name
        from utils.fhir.identifier_index_names import sanitize_system_for_identifier
        from utils.fhir.index_config import (
            IDENTIFIER_PREFIX_INDEX_SPECS,
            IDENTIFIER_UNIQUE_INDEX_SPECS,
        )

        navne: dict[str, tuple] = {}
        for rt, system in IDENTIFIER_UNIQUE_INDEX_SPECS:
            tbl, safe = rt.lower(), sanitize_system_for_identifier(system)
            navn = f"idx_{tbl}_{safe}_uniq"[:63]
            assert navn not in navne, f"kollision: {navn} ({navne[navn]} vs {(rt, system)})"
            navne[navn] = (rt, system, "uniq")
        for rt, system in IDENTIFIER_PREFIX_INDEX_SPECS:
            tbl, safe = rt.lower(), sanitize_system_for_identifier(system)
            navn = _prefix_index_name(tbl, safe)[:63]
            assert navn not in navne, f"kollision: {navn} ({navne[navn]} vs {(rt, system)})"
            navne[navn] = (rt, system, "prefix")

    def test_praefiksnavne_beholder_kendetegnet_efter_afkortning(self):
        from infrastructure.assets.fhir_ifdb_infrastructure import _prefix_index_name

        kort = _prefix_index_name("procedure", "urn_source_ifdb_ris_ydelse")
        assert kort.endswith("_prefix") and len(kort) <= 63
        langt = _prefix_index_name("condition", "urn_source_ifdb_aktivitetsresultat_xml_condition")
        assert "_pfx_" in langt and len(langt) <= 63

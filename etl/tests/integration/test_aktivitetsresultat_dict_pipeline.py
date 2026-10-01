# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration test for the dict-based AKTIVITETSRESULTAT pipeline.

Tests the full flow: row_dicts -> XML parse -> dict build -> orjson serialize,
verifying structural correctness of the output dicts that would be upserted
to the FHIR database.

Does NOT require a live database — tests the transform/build/serialize chain
in isolation.
"""

from __future__ import annotations

import orjson
import pytest
from core.resources import _transform_references_for_aidbox
from DSA.FHIR.Aktivitetsresultat import orchestrator as akt_orchestrator
from DSA.FHIR.Aktivitetsresultat.references import ResolvedRefLookup
from DSA.FHIR.Observation.observation_dict_builder import _build_observation_lossless_dict
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
    parse_aktivitetsresultat_xml,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
)

pytestmark = pytest.mark.integration

SAMPLE_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Blodtryk</Overskrift>
  <Type>Observation</Type>
  <Element type="string">
    <Navn>Systolisk</Navn>
    <Vaerdi>120</Vaerdi>
    <Kode>ZZ3170</Kode>
  </Element>
  <Element type="string">
    <Navn>Diastolisk</Navn>
    <Vaerdi>80</Vaerdi>
  </Element>
</Afdelingsydelsesresultat>"""

BASE_ROW = {
    "OBJEKT_ID": 999,
    "PATIENT_ID": 100,
    "ENCOUNTER_ID": 200,
    "EPISODE_OF_CARE_ID": 300,
    "AKTIVITETTYPE_KODE": "A01",
    "AKTIVITETTYPE_KODETEKST": "Aktivitet",
    "RESULTATTYPE_KODE": None,
    "RESULTATTYPE_KODETEKST": None,
    "OVERSKRIFT": "Blodtryk",
    "XML": SAMPLE_XML,
    "KLADDE": 0,
    "PERIOD_START": "2024-01-15T10:30:00",
    "SLETTET": 0,
}


class TestDictPipelineE2E:
    def test_full_transform_produces_valid_dicts(self, monkeypatch):
        from DSA.FHIR.Aktivitetsresultat import orchestrator as akt_orchestrator
        from DSA.FHIR.Aktivitetsresultat import routing_glue as akt_routing

        monkeypatch.setattr(akt_orchestrator, "_FAST_MODE", False)
        monkeypatch.setattr(akt_routing, "_SFI_MODE", "legacy")

        rows = [BASE_ROW]
        dicts, _, _ = akt_orchestrator._transform_aktivitetsresultat_to_fhir_dicts(
            rows, context=None
        )

        assert len(dicts) == 1
        rid, obs = dicts[0]

        assert obs["resourceType"] == "Observation"
        assert obs["status"] == "final"
        assert obs["identifier"][0]["value"] == "999"
        assert "component" in obs
        assert len(obs["component"]) == 2

    def test_orjson_serializes_without_error(self, monkeypatch):
        from DSA.FHIR.Aktivitetsresultat import orchestrator as akt_orchestrator
        from DSA.FHIR.Aktivitetsresultat import routing_glue as akt_routing

        monkeypatch.setattr(akt_orchestrator, "_FAST_MODE", False)
        monkeypatch.setattr(akt_routing, "_SFI_MODE", "legacy")

        dicts, _, _ = akt_orchestrator._transform_aktivitetsresultat_to_fhir_dicts(
            [BASE_ROW], context=None
        )
        _, obs = dicts[0]

        serialized = orjson.dumps(obs)
        parsed_back = orjson.loads(serialized)

        assert parsed_back["resourceType"] == "Observation"
        assert parsed_back["status"] == "final"
        assert parsed_back["component"][0]["code"]["text"] == "Systolisk"

    def test_transform_for_aidbox_works_on_dict(self, monkeypatch):
        from DSA.FHIR.Aktivitetsresultat import routing_glue as akt_routing

        monkeypatch.setattr(akt_routing, "_SFI_MODE", "legacy")

        dicts, _, _ = akt_orchestrator._transform_aktivitetsresultat_to_fhir_dicts(
            [BASE_ROW], context=None
        )
        _, obs = dicts[0]

        _transform_references_for_aidbox(obs)
        serialized = orjson.dumps(obs)
        assert len(serialized) > 0


class TestResolvedRefLookup:
    def _make_lookup(self):
        return ResolvedRefLookup(
            patient={(SYSTEM_IFDB_PATIENT_ID, "100"): "pat-uuid-1"},
            encounter={(SYSTEM_IFDB_OBJECT_ID, "200"): "enc-uuid-1"},
            episode={},
            observation={(SYSTEM_IFDB_AKTIVITETSRESULTAT, "999"): "obs-uuid-1"},
        )

    def test_patient_ref_resolved(self):
        lookup = self._make_lookup()
        ref = lookup.patient_ref(100)
        assert ref == {"id": "pat-uuid-1", "resourceType": "Patient"}

    def test_patient_ref_unresolved(self):
        lookup = self._make_lookup()
        ref = lookup.patient_ref(999)
        assert ref["identifier"]["system"] == SYSTEM_IFDB_PATIENT_ID

    def test_observation_id_existing(self):
        lookup = self._make_lookup()
        # Cache hit reuses the Aidbox-assigned UUID so re-runs of the same
        # partition update the existing row via ON CONFLICT (id) DO UPDATE.
        oid = lookup.observation_id(999)
        assert oid == "obs-uuid-1"

    def test_observation_id_new(self):
        lookup = self._make_lookup()
        # Cache miss falls back to a fresh uuid4 — Aidbox assigns the
        # persisted UUID on first insert and the cache picks it up next run.
        oid = lookup.observation_id(888)
        assert len(oid) == 36 and oid.count("-") == 4

    def test_dict_with_resolved_refs(self):
        lookup = self._make_lookup()
        xml_rows, kontekst = parse_aktivitetsresultat_xml(SAMPLE_XML, 999)

        rid, obs = _build_observation_lossless_dict(BASE_ROW, xml_rows, kontekst, lookup)

        assert rid == "obs-uuid-1"
        assert obs["id"] == "obs-uuid-1"
        assert obs["subject"]["id"] == "pat-uuid-1"
        assert obs["subject"]["resourceType"] == "Patient"
        assert obs["encounter"]["id"] == "enc-uuid-1"
        assert obs["encounter"]["resourceType"] == "Encounter"


class TestMultiRowPipeline:
    def test_batch_transform_ordering(self, monkeypatch):
        from DSA.FHIR.Aktivitetsresultat import orchestrator as akt_orchestrator
        from DSA.FHIR.Aktivitetsresultat import routing_glue as akt_routing

        monkeypatch.setattr(akt_orchestrator, "_FAST_MODE", False)
        monkeypatch.setattr(akt_routing, "_SFI_MODE", "legacy")

        rows = []
        for i in range(10):
            rows.append({**BASE_ROW, "OBJEKT_ID": 1000 + i})

        dicts, _, _ = akt_orchestrator._transform_aktivitetsresultat_to_fhir_dicts(
            rows, context=None
        )

        for i, (_, obs) in enumerate(dicts):
            expected = str(1000 + i)
            assert obs["identifier"][0]["value"] == expected

    def test_batch_no_data_loss(self, monkeypatch):
        from DSA.FHIR.Aktivitetsresultat import orchestrator as akt_orchestrator
        from DSA.FHIR.Aktivitetsresultat import routing_glue as akt_routing

        monkeypatch.setattr(akt_orchestrator, "_FAST_MODE", False)
        monkeypatch.setattr(akt_routing, "_SFI_MODE", "legacy")

        rows = [{**BASE_ROW, "OBJEKT_ID": i} for i in range(50)]
        dicts, _, _ = akt_orchestrator._transform_aktivitetsresultat_to_fhir_dicts(
            rows, context=None
        )

        assert len(dicts) == 50

        all_ids = {obs["identifier"][0]["value"] for _, obs in dicts}
        assert all_ids == {str(i) for i in range(50)}

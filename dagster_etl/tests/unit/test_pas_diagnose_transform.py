# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for PAS_DIAGNOSE → Condition FHIR transform."""

from unittest.mock import MagicMock

import dagster as dg
from DSA.FHIR.Condition.Condition_IFDB_PasDiagnose import (
    _transform_pas_diagnose_to_fhir,
)


def _make_context() -> dg.AssetExecutionContext:
    ctx = MagicMock(spec=dg.AssetExecutionContext)
    ctx.log = MagicMock()
    return ctx


def _make_row(**overrides) -> dict:
    base = {
        "OBJEKT_ID": "100",
        "PATIENT_ID": "200",
        "DIAGNOSE_KODE": "DZ038",
        "DIAGNOSE_KODETEKST": "Observation for mistanke om anden sygdom",
        "SLETTET": 0,
        "DIAGNOSETYPE": "B",
        "LATER_DISPROVED": 0,
        "OPDATERET_TID": None,
        "MODIFICERET_PERSID": None,
        "MODIFICERET_PERSNAVN": None,
        "MODIFICERET_BRUGERROLLE": None,
        "MODIFICERET_ADMENHED_KODE": None,
        "MODIFICERET_ADMENHED_KODETEKST": None,
        "MODIFICERET_ADMENHED_ORGKODE": None,
        "MODIFICERET_ADMENHED_SORKODE": None,
    }
    base.update(overrides)
    return base


class TestPasDiagnoseTransform:
    def test_basic_transform_produces_condition(self):
        ctx = _make_context()
        rows = [_make_row()]
        conditions = _transform_pas_diagnose_to_fhir(rows, ctx)
        assert len(conditions) == 1
        c = conditions[0]
        assert c.identifier[0].system == "urn:source:ifdb:pas_diagnose"
        assert c.identifier[0].value == "100"
        assert c.code.coding[0].code == "DZ038"
        assert c.verificationStatus.coding[0].code == "confirmed"
        assert c.clinicalStatus.coding[0].code == "active"

    def test_deleted_row_sets_entered_in_error(self):
        ctx = _make_context()
        rows = [_make_row(SLETTET=1)]
        conditions = _transform_pas_diagnose_to_fhir(rows, ctx)
        c = conditions[0]
        assert c.verificationStatus.coding[0].code == "entered-in-error"
        assert c.clinicalStatus is None

    def test_later_disproved_sets_refuted(self):
        ctx = _make_context()
        rows = [_make_row(LATER_DISPROVED=1)]
        conditions = _transform_pas_diagnose_to_fhir(rows, ctx)
        c = conditions[0]
        assert c.verificationStatus.coding[0].code == "refuted"
        assert c.clinicalStatus is None

    def test_encounter_ref_applied(self):
        ctx = _make_context()
        rows = [_make_row()]
        encounter_refs = {"100": "ENC-999"}
        conditions = _transform_pas_diagnose_to_fhir(rows, ctx, encounter_refs=encounter_refs)
        c = conditions[0]
        assert c.encounter is not None
        assert c.encounter.identifier.value == "ENC-999"

    def test_tillaegskoder_appended_to_coding(self):
        ctx = _make_context()
        rows = [_make_row()]
        tillaegskoder = {
            "100": [
                {
                    "PAS_DIAGNOSE_ID": "100",
                    "TILLAEGSKODE_KODE": "TK01",
                    "TILLAEGSKODE_KODETEKST": "Tillæg 1",
                },
                {
                    "PAS_DIAGNOSE_ID": "100",
                    "TILLAEGSKODE_KODE": "TK02",
                    "TILLAEGSKODE_KODETEKST": "Tillæg 2",
                },
            ]
        }
        conditions = _transform_pas_diagnose_to_fhir(rows, ctx, tillaegskoder=tillaegskoder)
        c = conditions[0]
        assert len(c.code.coding) == 3
        assert c.code.coding[1].code == "TK01"
        assert c.code.coding[2].code == "TK02"

    def test_duplicate_tillaegskoder_do_not_duplicate_code_codings(self):
        ctx = _make_context()
        rows = [_make_row()]
        tillaegskoder = {
            "100": [
                {
                    "PAS_DIAGNOSE_ID": "100",
                    "TILLAEGSKODE_KODE": "DZ038",
                    "TILLAEGSKODE_KODETEKST": "Duplicate",
                },
                {
                    "PAS_DIAGNOSE_ID": "100",
                    "TILLAEGSKODE_KODE": "TK01",
                    "TILLAEGSKODE_KODETEKST": "Tillæg 1",
                },
                {
                    "PAS_DIAGNOSE_ID": "100",
                    "TILLAEGSKODE_KODE": "TK01",
                    "TILLAEGSKODE_KODETEKST": "Tillæg 1",
                },
            ]
        }
        conditions = _transform_pas_diagnose_to_fhir(rows, ctx, tillaegskoder=tillaegskoder)
        c = conditions[0]
        assert [coding.code for coding in c.code.coding] == ["DZ038", "TK01"]
        assert c.code.coding[0].display == "Observation for mistanke om anden sygdom"

    def test_henvisning_refs_in_source_meta(self):
        ctx = _make_context()
        rows = [_make_row()]
        henvisning_refs = {"100": ["HENV-1", "HENV-2"]}
        conditions = _transform_pas_diagnose_to_fhir(rows, ctx, henvisning_refs=henvisning_refs)
        c = conditions[0]
        meta_ext = [e for e in c.extension if e.url.endswith("pasDiagnoseSourceMeta")]
        assert len(meta_ext) == 1
        henv_parts = [p for p in meta_ext[0].extension if p.url == "henvisningId"]
        assert len(henv_parts) == 2
        assert {p.valueString for p in henv_parts} == {"HENV-1", "HENV-2"}

    def test_is_deleted_extension_always_present(self):
        ctx = _make_context()
        rows = [_make_row()]
        conditions = _transform_pas_diagnose_to_fhir(rows, ctx)
        c = conditions[0]
        is_del = [e for e in c.extension if e.url == "urn:fhir:isDeleted"]
        assert len(is_del) == 1
        assert is_del[0].valueInteger == 0

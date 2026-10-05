# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for DIAGNOSE tillægskode lookup and integration."""

from unittest.mock import MagicMock

import dagster as dg
import pytest
from DSA.FHIR.Condition.Condition_IFDB_Diagnose_dfm import (
    _transform_diagnose_to_fhir,
)

pytestmark = pytest.mark.unit


def _make_context() -> dg.AssetExecutionContext:
    ctx = MagicMock(spec=dg.AssetExecutionContext)
    ctx.log = MagicMock()
    return ctx


def _make_row(**overrides) -> dict:
    base = {
        "OBJEKT_ID": "500",
        "PATIENT_ID": "600",
        "DIAGNOSE_KODE": "DM142",
        "DIAGNOSE_KODETEKST": "Type 2 diabetes",
        "SLETTET": 0,
        "BESLUT_TIDSPUNKT": None,
        "OPRET_TIDSPUNKT": None,
        "AFSLUT_TIDSPUNKT": None,
    }
    base.update(overrides)
    return base


class TestDiagnoseTillaegskode:
    def test_transform_without_tillaegskoder(self):
        ctx = _make_context()
        rows = [_make_row()]
        conditions = _transform_diagnose_to_fhir(rows, ctx)
        assert len(conditions) == 1
        assert len(conditions[0].code.coding) == 1
        assert conditions[0].code.coding[0].code == "DM142"
        # Primary diagnosis coding is marked userSelected so the fhir.Condition
        # SoF view can select it deterministically.
        assert conditions[0].code.coding[0].userSelected is True

    def test_tillaegskoder_appended_to_code_codings(self):
        ctx = _make_context()
        rows = [_make_row()]
        tillaegskoder = {
            "500": [
                {
                    "DIAGNOSE_ID": "500",
                    "TILLAEGSKODE_KODE": "DZTA0",
                    "TILLAEGSKODE_KODETEKST": "Tillæg A",
                },
            ]
        }
        conditions = _transform_diagnose_to_fhir(rows, ctx, tillaegskoder)
        c = conditions[0]
        assert len(c.code.coding) == 2
        assert c.code.coding[0].code == "DM142"
        assert c.code.coding[1].code == "DZTA0"
        assert c.code.coding[1].display == "Tillæg A"
        # Only the primary diagnosis coding is userSelected; supplementary
        # tillaegskoder are not — so they are never projected as the diagnosis.
        assert c.code.coding[0].userSelected is True
        assert c.code.coding[1].userSelected is None

    def test_duplicate_tillaegskoder_do_not_duplicate_code_codings(self):
        ctx = _make_context()
        rows = [_make_row()]
        tillaegskoder = {
            "500": [
                {
                    "DIAGNOSE_ID": "500",
                    "TILLAEGSKODE_KODE": "DM142",
                    "TILLAEGSKODE_KODETEKST": "Duplicate",
                },
                {
                    "DIAGNOSE_ID": "500",
                    "TILLAEGSKODE_KODE": "DZTA0",
                    "TILLAEGSKODE_KODETEKST": "Tillæg A",
                },
                {
                    "DIAGNOSE_ID": "500",
                    "TILLAEGSKODE_KODE": "DZTA0",
                    "TILLAEGSKODE_KODETEKST": "Tillæg A",
                },
            ]
        }
        conditions = _transform_diagnose_to_fhir(rows, ctx, tillaegskoder)
        c = conditions[0]
        assert [coding.code for coding in c.code.coding] == ["DM142", "DZTA0"]
        assert c.code.coding[0].display == "Type 2 diabetes"

    def test_tillaegskoder_missing_for_id_leaves_coding_unchanged(self):
        ctx = _make_context()
        rows = [_make_row(OBJEKT_ID="999")]
        tillaegskoder = {
            "500": [
                {"DIAGNOSE_ID": "500", "TILLAEGSKODE_KODE": "DZTA0", "TILLAEGSKODE_KODETEKST": "X"}
            ]
        }
        conditions = _transform_diagnose_to_fhir(rows, ctx, tillaegskoder)
        assert len(conditions[0].code.coding) == 1

    def test_no_diagnose_kode_still_gets_tillaegskoder(self):
        ctx = _make_context()
        rows = [_make_row(DIAGNOSE_KODE=None)]
        tillaegskoder = {
            "500": [
                {
                    "DIAGNOSE_ID": "500",
                    "TILLAEGSKODE_KODE": "TK01",
                    "TILLAEGSKODE_KODETEKST": "Supplement",
                }
            ]
        }
        conditions = _transform_diagnose_to_fhir(rows, ctx, tillaegskoder)
        assert len(conditions[0].code.coding) == 1
        assert conditions[0].code.coding[0].code == "TK01"
        # No primary diagnosis code -> nothing is marked userSelected, so the
        # supplementary code is NOT projected as the diagnosis (the fhir.Condition
        # view yields NULL rather than surfacing a tillaegskode as a diagnosis).
        assert conditions[0].code.coding[0].userSelected is None

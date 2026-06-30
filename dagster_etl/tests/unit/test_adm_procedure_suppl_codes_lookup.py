# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for ADMINISTRATIVE_PROCEDURE supplementary codes integration."""

from unittest.mock import MagicMock

import dagster as dg
from DSA.FHIR.Procedure.Procedure_IFDB_AdminProc import (
    _transform_adm_procedure_to_fhir,
)


def _make_context() -> dg.AssetExecutionContext:
    ctx = MagicMock(spec=dg.AssetExecutionContext)
    ctx.log = MagicMock()
    return ctx


def _make_row(**overrides) -> dict:
    base = {
        "OBJECT_ID": "700",
        "PATIENT_ID": "800",
        "ENCOUNTER_ID": None,
        "EPISODE_OF_CARE_ID": None,
        "PROCEDURE_CODE": "BKHA20",
        "PROCEDURE_TEXT": "Knee arthroscopy",
        "DELETED": 0,
        "PERIOD_START": None,
        "PERIOD_END": None,
        "INDICATION_OF_SIDE_CODE": None,
        "INDICATION_OF_SIDE_TEXT": None,
        "EPJI_CREATED_DATETIME": None,
        "EPJI_MODIFICATION_DATETIME": None,
        "STATUS_CODE": None,
        "STATUS_TEXT": None,
        "USED_CONTRAST_CODE": None,
        "USED_CONTRAST_TEXT": None,
        "STAFF_CATEGORY_CODE": None,
        "STAFF_CATEGORY_TEXT": None,
        "USER_ID": None,
        "USER_NAME": None,
        "USER_ROLE": None,
        "USER_ROLE_NAME": None,
        "IFDB_MODIFICATION_DATETIME": None,
        "LPR3_IGNORED": None,
        "LOCATION_CODE": None,
        "LOCATION_NAME": None,
        "LOCATION_ORGCODE": None,
        "LOCATION_SORCODE": None,
    }
    base.update(overrides)
    return base


class TestAdmProcedureSupplCodes:
    def test_transform_without_suppl_codes(self):
        ctx = _make_context()
        rows = [_make_row()]
        procedures = _transform_adm_procedure_to_fhir(rows, ctx)
        assert len(procedures) == 1
        assert len(procedures[0].code.coding) == 1
        assert procedures[0].code.coding[0].code == "BKHA20"

    def test_suppl_codes_appended_in_idx_order(self):
        ctx = _make_context()
        rows = [_make_row()]
        suppl_codes = {
            "700": [
                {
                    "PARENT_OBJECT_ID": "700",
                    "SUPPL_CODE": "SKS.a.proSC01",
                    "SUPPL_TEXT": "First",
                    "IDX": 1,
                },
                {
                    "PARENT_OBJECT_ID": "700",
                    "SUPPL_CODE": "SKS.a.oprSC02",
                    "SUPPL_TEXT": "Second",
                    "IDX": 2,
                },
            ]
        }
        procedures = _transform_adm_procedure_to_fhir(rows, ctx, suppl_codes)
        p = procedures[0]
        assert len(p.code.coding) == 3
        assert p.code.coding[0].code == "BKHA20"
        assert p.code.coding[1].code == "SC01"
        assert p.code.coding[2].code == "SC02"

    def test_suppl_codes_missing_for_id_leaves_coding_unchanged(self):
        ctx = _make_context()
        rows = [_make_row(OBJECT_ID="999")]
        suppl_codes = {
            "700": [{"PARENT_OBJECT_ID": "700", "SUPPL_CODE": "SC01", "SUPPL_TEXT": "X", "IDX": 1}]
        }
        procedures = _transform_adm_procedure_to_fhir(rows, ctx, suppl_codes)
        assert len(procedures[0].code.coding) == 1

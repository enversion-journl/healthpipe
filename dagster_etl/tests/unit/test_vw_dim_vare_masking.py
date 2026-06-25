# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Guardrail: vw_dim.Vare must column-restrict free-text/PII-risk fields.

Varenavn/VarenavnKort can hold invoice free-text (MySupply) that may contain
names/CPR, and VarenummerKreditor can be CPR-formatted. Access to these columns
is column-restricted via sec.BrugerRoller.Fritekst and enforced in vw_dim.Vare.

This test fails if vw_dim.Vare is reintroduced as a SELECT * pass-through or if
the masking function wiring is removed.
"""

import re
from pathlib import Path

import pytest

_DDL_ROOT = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl"
_SECURITY_VIEW = _DDL_ROOT / "views" / "security" / "01_vw_dim_security.sql"
_ERP_VIEW = _DDL_ROOT / "views" / "erp" / "01_vw_dim.sql"
_SECURITY_FUNCS = _DDL_ROOT / "tables" / "security" / "02_functions.sql"

_VARE_VIEW_PATTERN = re.compile(r"CREATE\s+OR\s+ALTER\s+VIEW\s+\[vw_dim\]\.\[Vare\]", re.IGNORECASE)
_FAKTURALINJE_VIEW_PATTERN = re.compile(
    r"CREATE\s+OR\s+ALTER\s+VIEW\s+\[vw_dim\]\.\[Fakturalinje\]", re.IGNORECASE
)


@pytest.mark.unit
class TestVwDimVareMasking:
    """Ensure vw_dim.Vare enforces column-restricted access to free-text fields."""

    def test_masking_functions_exist(self):
        sql = _SECURITY_FUNCS.read_text()
        assert (
            "[sec].[BeskytDimVare_v1]" in sql
        ), "sec.BeskytDimVare_v1 must exist to mask dim.Vare free-text columns."
        assert (
            "[sec].[HentFritekstAdgang_v1]" in sql
        ), "sec.HentFritekstAdgang_v1 must exist to resolve free-text access."

    def test_security_view_defines_and_masks_vare(self):
        sql = _SECURITY_VIEW.read_text()
        assert _VARE_VIEW_PATTERN.search(
            sql
        ), "vw_dim.Vare must be defined in views/security/01_vw_dim_security.sql."
        assert (
            "[sec].[BeskytDimVare_v1]" in sql
        ), "vw_dim.Vare must apply sec.BeskytDimVare_v1 to mask free-text columns."

    def test_erp_view_does_not_define_vare(self):
        sql = _ERP_VIEW.read_text()
        assert not _VARE_VIEW_PATTERN.search(sql), (
            "vw_dim.Vare must NOT be defined in views/erp/01_vw_dim.sql "
            "(non-deterministic deploy order would override the masked view "
            "with a SELECT * pass-through)."
        )

    def test_security_view_defines_and_masks_fakturalinje(self):
        sql = _SECURITY_VIEW.read_text()
        assert _FAKTURALINJE_VIEW_PATTERN.search(sql), (
            "vw_dim.Fakturalinje must be defined in " "views/security/01_vw_dim_security.sql."
        )
        assert "[sec].[HentFritekstAdgang_v1]" in sql, (
            "vw_dim.Fakturalinje must resolve free-text access via " "sec.HentFritekstAdgang_v1."
        )
        assert (
            "N'[MASKERET]'" in sql
        ), "vw_dim.Fakturalinje must mask free-text columns when access is absent."

    def test_erp_view_does_not_define_fakturalinje(self):
        sql = _ERP_VIEW.read_text()
        assert not _FAKTURALINJE_VIEW_PATTERN.search(sql), (
            "vw_dim.Fakturalinje must NOT be defined in views/erp/01_vw_dim.sql "
            "(non-deterministic deploy order would override the masked view "
            "with a SELECT * pass-through)."
        )

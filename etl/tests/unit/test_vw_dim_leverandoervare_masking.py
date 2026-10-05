# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Guardrail: vw_dim.Leverandoervare must column-restrict free-text/PII-risk fields.

varenavn/varenavn_mysupply can hold invoice free-text (MySupply) that may contain
names/CPR, and varenummer_kreditor can be CPR-formatted. Access is enforced via
sec.RolleDomaene (DPA_ERP): Rolle 'U' sees free-text, 'K' gets [MASKERET].
Users without a DPA_ERP role see no rows (CROSS JOIN on HentBrugerRolleDomaene).

This test fails if vw_dim.Leverandoervare is reintroduced as a SELECT * pass-through or if
the masking function wiring is removed.
"""

import re
from pathlib import Path

import pytest

_DDL_ROOT = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl"
_SECURITY_VIEW = _DDL_ROOT / "views" / "security" / "01_vw_dim_security.sql"
_ERP_VIEW = _DDL_ROOT / "views" / "erp" / "01_vw_dim.sql"
_SECURITY_FUNCS = _DDL_ROOT / "tables" / "security" / "02_functions.sql"
_DPA_FUNCS = _DDL_ROOT / "tables" / "security" / "05_dpa_functions.sql"
_DPA_ERP_DOMAIN = "N'DPA_ERP'"

_VARE_VIEW_PATTERN = re.compile(
    r"CREATE\s+OR\s+ALTER\s+VIEW\s+\[vw_dim\]\.\[Leverandoervare\]", re.IGNORECASE
)
_FAKTURALINJE_VIEW_PATTERN = re.compile(
    r"CREATE\s+OR\s+ALTER\s+VIEW\s+\[vw_dim\]\.\[Fakturalinje\]", re.IGNORECASE
)


@pytest.mark.unit
class TestVwDimLeverandoervareMasking:
    """Ensure vw_dim.Leverandoervare enforces column-restricted access to free-text fields."""

    def test_masking_functions_exist(self):
        sql = _SECURITY_FUNCS.read_text()
        combined_sql = sql + _DPA_FUNCS.read_text()
        assert (
            "[sec].[BeskytDimLeverandoervare_v1]" in sql
        ), "sec.BeskytDimLeverandoervare_v1 must exist to mask dim.Leverandoervare free-text columns."
        assert (
            "[sec].[HentBrugerRolleDomaene]" in combined_sql
        ), "sec.HentBrugerRolleDomaene must exist to resolve DPA_ERP role."
        assert "[sec].[HentFritekstAdgang_v1]" not in sql, (
            "sec.HentFritekstAdgang_v1 is retired; use sec.RolleDomaene via "
            "sec.HentBrugerRolleDomaene."
        )

    def test_security_view_defines_and_masks_vare(self):
        sql = _SECURITY_VIEW.read_text()
        assert _VARE_VIEW_PATTERN.search(
            sql
        ), "vw_dim.Leverandoervare must be defined in views/security/01_vw_dim_security.sql."
        assert (
            "[sec].[BeskytDimLeverandoervare_v1]" in sql
        ), "vw_dim.Leverandoervare must apply sec.BeskytDimLeverandoervare_v1 to mask free-text columns."
        assert (
            "[sec].[HentBrugerRolleDomaene]" in sql
        ), "vw_dim.Leverandoervare must resolve DPA_ERP role via sec.HentBrugerRolleDomaene."
        assert _DPA_ERP_DOMAIN in sql, "vw_dim.Leverandoervare must gate access on DPA_ERP domain."

    def test_security_view_masks_current_column_set(self):
        """vw_dim.Leverandoervare must match dim.Leverandoervare's column set: varenavn_mysupply
        masked, no ILS attribute columns and no dw_ek_ILSVare (the ILS
        coupling lives in bridge.LeverandoervareILSVare)."""
        sql = _SECURITY_VIEW.read_text()
        vare_view_start = _VARE_VIEW_PATTERN.search(sql).start()
        vare_view_end = sql.index("GO", vare_view_start)
        vare_view_sql = sql[vare_view_start:vare_view_end]

        assert "[varenavn_mysupply]" in vare_view_sql, (
            "vw_dim.Leverandoervare must expose the renamed varenavn_mysupply column via "
            "the masking function."
        )
        assert "beskyt.[varenavn_mysupply]" in vare_view_sql, (
            "varenavn_mysupply must be sourced from the masking CROSS APPLY, not "
            "the raw dim.Leverandoervare column."
        )
        assert (
            "[varenavn_kort]" not in vare_view_sql
        ), "varenavn_kort was renamed to varenavn_mysupply; the old name must not linger."

        dropped_columns = [
            "dw_ek_ILSVare",
            "ils_varenummer",
            "faelles_varenummer",
            "stregkode",
            "stoerrelse_tekst",
            "sammensat_navn",
            "basisenhed",
            "vareart_kode",
            "vareart_navn",
            "varegruppe_kode",
            "varegruppe_navn",
            "varetype",
            "kreditornummer",
            "momskd",
            "afgiftkd",
        ]
        for col in dropped_columns:
            assert f"[{col}]" not in vare_view_sql, (
                f"vw_dim.Leverandoervare must not reference dropped dim.Leverandoervare column {col} "
                "(ILS coupling now goes via bridge.LeverandoervareILSVare)."
            )

    def test_erp_view_does_not_define_vare(self):
        sql = _ERP_VIEW.read_text()
        assert not _VARE_VIEW_PATTERN.search(sql), (
            "vw_dim.Leverandoervare must NOT be defined in views/erp/01_vw_dim.sql "
            "(non-deterministic deploy order would override the masked view "
            "with a SELECT * pass-through)."
        )

    def test_security_view_defines_and_masks_fakturalinje(self):
        sql = _SECURITY_VIEW.read_text()
        assert _FAKTURALINJE_VIEW_PATTERN.search(sql), (
            "vw_dim.Fakturalinje must be defined in " "views/security/01_vw_dim_security.sql."
        )
        assert "[sec].[HentBrugerRolleDomaene]" in sql, (
            "vw_dim.Fakturalinje must resolve DPA_ERP role via " "sec.HentBrugerRolleDomaene."
        )
        assert (
            "R.[Rolle] = 'U'" in sql
        ), "vw_dim.Fakturalinje must reveal free-text only for U-role in DPA_ERP."
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

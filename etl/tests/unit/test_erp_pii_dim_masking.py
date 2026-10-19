# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Guardrail: SydAX/ILS ERP dimensions with free-text PII must be column-restricted.

Three dimensions carry user-entered free-text that may contain names/CPR and must be
masked via sec.RolleDomaene (DPA_ERP), not exposed as SELECT * pass-throughs:

- dim.Postering        -> posteringstekst / transaktionstekst (SydAX ledger free-text)
- dim.Finanskladdelinje-> beskrivelse (SydAX journal-line free-text)
- dim.Rekvirent        -> display (rekvirent name; personal for home patients rtyp HP/OHP)

Unlike vw_dim.Leverandoervare/Fakturalinje these views keep all rows (OUTER APPLY) and mask
only the sensitive columns, because the dimensions carry non-sensitive structured data
needed broadly. Rekvirent masks the name only for rtyp HP/OHP.

Each test names the bug it catches: reintroducing a SELECT * pass-through, or dropping the
masking wiring, would re-expose PII.
"""

import re
from pathlib import Path

import pytest
from DSA.SQL.common.rekvirent_kontaktinfo import (
    KONTAKTINFO_COLUMNS,
    KONTAKTINFO_PII_COLUMNS,
)

_DDL_ROOT = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl"
_SECURITY_VIEW = _DDL_ROOT / "views" / "security" / "01_vw_dim_security.sql"
_ERP_VIEW = _DDL_ROOT / "views" / "erp" / "01_vw_dim.sql"
_DPA_ERP_DOMAIN = "N'DPA_ERP'"
_MASK = "N'[MASKERET]'"

# Derived from the shared contract rather than hand-listed, so a column moved
# between the masked and visible halves cannot silently pass both tests.
_REKVIRENT_PII_COLUMNS = KONTAKTINFO_PII_COLUMNS
_REKVIRENT_VISIBLE_COLUMNS = tuple(
    c for c in KONTAKTINFO_COLUMNS if c not in KONTAKTINFO_PII_COLUMNS
)


def _view_block(sql: str, view: str) -> str:
    """Return the SQL text of a single CREATE OR ALTER VIEW block up to its terminating GO."""
    pattern = re.compile(
        r"CREATE\s+OR\s+ALTER\s+VIEW\s+\[vw_dim\]\.\[" + re.escape(view) + r"\]",
        re.IGNORECASE,
    )
    match = pattern.search(sql)
    assert match, f"vw_dim.{view} must be defined in views/security/01_vw_dim_security.sql."
    return sql[match.start() : sql.index("GO", match.start())]


@pytest.mark.unit
@pytest.mark.erp
class TestErpPiiDimMasking:
    """Ensure SydAX/ILS free-text PII dimensions are masked in the security view layer."""

    @pytest.mark.parametrize("view", ["Postering", "Finanskladdelinje", "Rekvirent"])
    def test_defined_and_gated_in_security_view(self, view):
        block = _view_block(_SECURITY_VIEW.read_text(), view)
        assert (
            "[sec].[HentBrugerRolleDomaene]" in block
        ), f"vw_dim.{view} must resolve the caller's role via sec.HentBrugerRolleDomaene."
        assert _DPA_ERP_DOMAIN in block, f"vw_dim.{view} must gate on the DPA_ERP domain."
        assert _MASK in block, f"vw_dim.{view} must mask its free-text column(s) with [MASKERET]."

    @pytest.mark.parametrize("view", ["Postering", "Finanskladdelinje", "Rekvirent"])
    def test_not_a_pass_through_in_erp_view(self, view):
        sql = _ERP_VIEW.read_text()
        pattern = re.compile(
            r"CREATE\s+OR\s+ALTER\s+VIEW\s+\[vw_dim\]\.\[" + re.escape(view) + r"\]",
            re.IGNORECASE,
        )
        assert not pattern.search(sql), (
            f"vw_dim.{view} must NOT be defined in views/erp/01_vw_dim.sql — a SELECT * "
            "pass-through there could override the masked security view (non-deterministic "
            "deploy order)."
        )

    def test_postering_masks_free_text_columns(self):
        block = _view_block(_SECURITY_VIEW.read_text(), "Postering")
        for col in ("posteringstekst", "transaktionstekst"):
            assert re.search(
                r"\[" + col + r"\]\s*=\s*CASE\s+WHEN\s+R\.\[Rolle\]\s*=\s*'U'", block
            ), f"vw_dim.Postering must mask {col} unless the caller has the U-role in DPA_ERP."

    def test_finanskladdelinje_masks_beskrivelse(self):
        block = _view_block(_SECURITY_VIEW.read_text(), "Finanskladdelinje")
        assert re.search(
            r"\[beskrivelse\]\s*=\s*CASE\s+WHEN\s+R\.\[Rolle\]\s*=\s*'U'", block
        ), "vw_dim.Finanskladdelinje must mask beskrivelse unless the caller has the U-role."

    def test_rekvirent_masks_display_only_for_home_patients(self):
        block = _view_block(_SECURITY_VIEW.read_text(), "Rekvirent")
        # display is masked, and the mask is conditioned on the HP/OHP rekvirent types.
        assert "[display] = CASE" in block, "vw_dim.Rekvirent must wrap display in a masking CASE."
        assert re.search(
            r"\[rtyp\]\s+IN\s*\(\s*N'HP'\s*,\s*N'OHP'\s*\)", block
        ), "vw_dim.Rekvirent must condition masking on rtyp IN ('HP','OHP')."

    @pytest.mark.parametrize("col", _REKVIRENT_PII_COLUMNS)
    def test_rekvirent_masks_address_and_contact_for_home_patients(self, col):
        """adr/adr2 carry the home patient's own address, and kontakt is a named
        contact person. Strategisk Indkoeb flagged adr/adr2 as
        "skal sloeres for de fleste brugere ved rtyp = HP eller OHP"; kontakt is masked
        on the same rule because a HP/OHP contact is the patient or a relative.
        Adding these columns to dim.Rekvirent without extending the mask would leak
        home addresses to every DSO reader."""
        block = _view_block(_SECURITY_VIEW.read_text(), "Rekvirent")
        assert f"[{col}] = CASE" in block, f"vw_dim.Rekvirent must wrap {col} in a masking CASE."
        masked = re.search(
            r"\[" + col + r"\]\s*=\s*CASE(.*?)END",
            block,
            re.DOTALL,
        )
        assert masked, f"vw_dim.Rekvirent must mask {col}."
        assert re.search(
            r"\[rtyp\]\s+IN\s*\(\s*N'HP'\s*,\s*N'OHP'\s*\)", masked.group(1)
        ), f"{col} must be masked on rtyp IN ('HP','OHP'), same rule as display."
        assert _MASK in masked.group(1), f"{col} must be replaced by [MASKERET]."

    @pytest.mark.parametrize("col", _REKVIRENT_VISIBLE_COLUMNS)
    def test_rekvirent_exposes_non_sensitive_address_columns(self, col):
        """The non-PII half must stay readable — masking everything would make the
        change pointless for the consumption reporting it was requested for."""
        block = _view_block(_SECURITY_VIEW.read_text(), "Rekvirent")
        assert f"rek.[{col}]" in block, f"vw_dim.Rekvirent must expose {col} unmasked."

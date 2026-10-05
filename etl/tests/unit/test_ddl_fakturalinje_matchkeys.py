# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit test: ref.FakturalinjeMatchKeys DDL is present and well-formed."""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_DDL = (
    Path(__file__).resolve().parents[2]
    / "infrastructure"
    / "ddl"
    / "tables"
    / "ref"
    / "01_tables.sql"
).read_text(encoding="utf-8")

# Normalize whitespace for column/constraint/type checks (make them whitespace-insensitive)
_DDL_NORM = re.sub(r"\s+", " ", _DDL)


class TestFakturalinjeMatchKeysDDL:
    def test_table_created_idempotently(self):
        assert "[ref].[FakturalinjeMatchKeys]" in _DDL
        assert "CREATE TABLE [ref].[FakturalinjeMatchKeys]" in _DDL

    def test_has_grain_pk(self):
        assert "[dw_ek_Fakturalinje] INT" in _DDL_NORM
        assert "PRIMARY KEY CLUSTERED ([dw_ek_Fakturalinje]" in _DDL_NORM

    def test_has_match_key_columns(self):
        for col in (
            "[clean_code_Kreditor]",
            "[clean_code_Kreditor_header]",
            "[clean_code_Faktura]",
            "[linje_beloeb]",
            "[enheds_pris]",
            "[antal_enheder_faktureret]",
            "[faktura_total]",
        ):
            assert col in _DDL

    def test_has_denormalised_faktura_date_key(self):
        # Denormalised from fct.Fakturalinje so the apply-pass's delta can apply
        # APPLY_PASS_MIN_FAKTURA_DATE without joining the fact table. NOT NULL with
        # a -1 (UNKNOWN) default, which the scope filter explicitly lets through.
        assert "[dw_ek_Dato_faktura] INT NOT NULL" in _DDL_NORM
        assert "DF_FakturalinjeMatchKeys_dw_ek_Dato_faktura] DEFAULT -1" in _DDL_NORM

    def test_timestamp_update_not_null(self):
        # dw_timestamp_update must exist NOT NULL (mirrors #12042 lesson).
        assert "[dw_timestamp_update] DATETIME2(0) NOT NULL" in _DDL_NORM

    def test_has_lookup_indexes(self):
        assert "IX_ref_fakturalinje_matchkeys_primary" in _DDL
        assert "IX_ref_fakturalinje_matchkeys_header" in _DDL

    def test_has_watermark_index(self):
        # The apply-pass's delta extract seeks this; without it every run is a
        # clustered scan of every fakturalinje ever seen.
        assert "IX_ref_fakturalinje_matchkeys_timestamp" in _DDL
        assert (
            "CREATE NONCLUSTERED INDEX [IX_ref_fakturalinje_matchkeys_timestamp] "
            "ON [ref].[FakturalinjeMatchKeys] ([dw_timestamp_update])" in _DDL_NORM
        )

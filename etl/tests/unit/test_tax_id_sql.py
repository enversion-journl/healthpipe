# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for shared MySupply/KMD tax-id SQL normalisation."""

import pytest
from utils.sql.tax_id_sql import (
    sql_cvr_cleancode_from_column,
    sql_strip_tax_id_country_prefix,
)

pytestmark = pytest.mark.unit


class TestTaxIdSql:
    def test_strips_dk_se_de_country_prefix_pattern(self):
        expr = sql_strip_tax_id_country_prefix("col")
        assert "LIKE '[A-Z][A-Z][0-9]%'" in expr
        assert "SUBSTRING(LTRIM(RTRIM(col)), 3, LEN(LTRIM(RTRIM(col))))" in expr

    def test_cvr_cleancode_wraps_prefix_strip(self):
        expr = sql_cvr_cleancode_from_column("m.leverandoer_cvr")
        assert "'CVR_' +" in expr
        assert "m.leverandoer_cvr IS NOT NULL" in expr
        assert "LIKE '[A-Z][A-Z][0-9]%'" in expr

    def test_cvr_cleancode_handles_header_column(self):
        expr = sql_cvr_cleancode_from_column("h.faktura_udsteder_cvr_nummer")
        assert "h.faktura_udsteder_cvr_nummer" in expr

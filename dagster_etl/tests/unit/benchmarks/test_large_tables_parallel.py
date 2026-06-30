# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The 5 long-pole tables are configured for parallel extraction; others stay serial."""

import pytest
from DSI.mssql.mysupply import MYSUPPLY_BASE_TABLE_CONFIGS
from DSI.mssql.sydax import SYDAX_BASE_TABLE_CONFIGS

_EXPECTED = {
    "statistic_invoice_header",
    "statistic_invoice_line",
    "Finanspostering",
    "Finanskladdelinje",
    "Kreditortransaktion",
}


@pytest.mark.unit
def test_large_tables_have_parallel_workers_8():
    configs = list(MYSUPPLY_BASE_TABLE_CONFIGS) + list(SYDAX_BASE_TABLE_CONFIGS)
    by_src = {c.source_table: c for c in configs}
    for name in _EXPECTED:
        assert name in by_src, f"missing config: {name}"
        assert by_src[name].parallel_workers == 8, name


@pytest.mark.unit
def test_other_tables_stay_serial():
    configs = list(MYSUPPLY_BASE_TABLE_CONFIGS) + list(SYDAX_BASE_TABLE_CONFIGS)
    for c in configs:
        if c.source_table not in _EXPECTED:
            assert c.parallel_workers == 1, c.source_table

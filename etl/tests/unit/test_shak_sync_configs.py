# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SHAK is wired into the conceptmap DW_EK resolution."""

import pytest
from DSA.ConceptMap.utils import _load_conceptmap_dw_ek_update_sql, sql_update_conceptmap_dw_ek

pytestmark = pytest.mark.unit


def test_shak_dw_ek_resolution_block():
    assert "SHAKToSOR" in _load_conceptmap_dw_ek_update_sql()
    sql = sql_update_conceptmap_dw_ek("SHAKToSOR")
    assert "[conceptmap].[SHAKToSOR]" in sql
    assert "[map].[SHAK]" in sql
    assert "[dw_ek_SHAK]" in sql
    assert "[ref].[Organisation]" in sql

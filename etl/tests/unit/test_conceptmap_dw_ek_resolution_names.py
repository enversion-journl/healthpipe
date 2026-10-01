# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""conceptmap_dw_ek_resolution.sql is keyed by PascalCase table names and
references PascalCase base tables."""

import pytest
from DSA.ConceptMap.utils import _load_conceptmap_dw_ek_update_sql, sql_update_conceptmap_dw_ek

pytestmark = pytest.mark.unit

PASCAL = ["RekvirentToSOR", "LagerToSOR", "EANToSOR", "OkostedToSOR", "SHAKToSOR"]
SNAKE = ["rekvirent_to_sor", "lager_to_sor", "ean_to_sor", "okosted_to_sor", "shak_to_sor"]


def test_headers_are_pascalcase():
    keys = set(_load_conceptmap_dw_ek_update_sql())
    assert set(PASCAL) <= keys, f"missing PascalCase headers; have {keys}"
    assert keys.isdisjoint(SNAKE), f"snake headers still present: {keys & set(SNAKE)}"


@pytest.mark.parametrize("name", PASCAL)
def test_update_sql_targets_pascalcase_table(name):
    sql = sql_update_conceptmap_dw_ek(name)
    assert f"[conceptmap].[{name}]" in sql

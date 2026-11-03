# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""write_to_ext_table against a local SQL Server: text outside cp1252 must arrive unchanged.

Run with:
    USE_REAL_DB_FOR_TESTS=true TEST_ENV=dev ENABLE_TEST_WRITES=true \
        pytest tests/integration/test_silkeborgdata_ext_unicode_e2e.py -v
"""

import os
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from DSI.csv_files import silkeborgdata
from DSI.csv_files.silkeborgdata import write_to_ext_table

pytestmark = [pytest.mark.integration, pytest.mark.real_resources]

_TABLE = "test_silkeborgdata_unicode"


@pytest.fixture
def local_staging(real_mssql_target_staging_resource):
    resource = real_mssql_target_staging_resource
    if os.getenv("ENABLE_TEST_WRITES", "false").lower() != "true":
        pytest.skip("requires ENABLE_TEST_WRITES=true")
    if resource.host != "mssql" or resource.database != "staging":
        pytest.fail("This destructive fixture requires local mssql/staging")
    yield resource
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS [ext].[{_TABLE}]")
        conn.commit()
        cursor.close()


def test_characters_outside_cp1252_survive_an_ascii_longest_value(local_staging, monkeypatch):
    monkeypatch.setattr(silkeborgdata, "INSERT_CHUNK_SIZE", 2)
    rows = [
        {"Navn": "Jensen", "Dato": date(2026, 9, 24), "Beloeb": Decimal("12.50")},
        {"Navn": "Hansen", "Dato": None, "Beloeb": Decimal("-999999999.99")},
        {"Navn": "A much longer ascii name", "Dato": None, "Beloeb": Decimal("-0.10")},
        {"Navn": "Ærø ╡╨ Σ", "Dato": date(2026, 1, 1), "Beloeb": Decimal("123456789.01")},
    ]
    column_types = {
        "Navn": "NVARCHAR(40) NULL",
        "Dato": "DATE NULL",
        "Beloeb": "DECIMAL(11,2) NULL",
    }
    with local_staging.get_connection() as conn:
        write_to_ext_table(conn, _TABLE, column_types, iter(rows), SimpleNamespace(log=MagicMock()))
        cursor = conn.cursor()
        cursor.execute(f"SELECT Navn, Dato, Beloeb FROM [ext].[{_TABLE}]")
        stored = sorted(tuple(r) for r in cursor.fetchall())
        cursor.close()

    assert stored == sorted(tuple(r.values()) for r in rows)

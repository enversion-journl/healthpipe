# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Canonical mssql-python connection-string builder (shared by all resources)."""

import pytest
from core.resources import build_mssql_conn_str


@pytest.mark.unit
def test_build_mssql_conn_str_canonical_format():
    assert build_mssql_conn_str("h", 1433, "db", "u", "pw") == (
        "SERVER=h,1433;DATABASE=db;UID=u;PWD=pw;" "Encrypt=yes;TrustServerCertificate=yes;"
    )


@pytest.mark.unit
def test_build_mssql_conn_str_resolves_secret_password():
    class _Secret:
        def get_value(self):
            return "resolved"

    assert "PWD=resolved;" in build_mssql_conn_str("h", 1433, "db", "u", _Secret())

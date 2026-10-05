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


@pytest.mark.unit
def test_get_raw_connection_uses_canonical_path(monkeypatch):
    """get_raw_connection() must build its connection via the same canonical
    path as get_connection() (so the DSO sync layer reuses the resource's
    conn-str/login-timeout/secret handling) and hand the open connection back
    to the caller without auto-closing it."""
    import core.resources as resources

    captured = {}
    sentinel = object()

    def _fake_connect(conn_str, timeout=0, login_timeout=60):
        captured["conn_str"] = conn_str
        captured["timeout"] = timeout
        return sentinel

    monkeypatch.setattr(resources, "_connect_mssql_python", _fake_connect)

    res = resources.MSSQLResource(host="h", port=1433, database="db", user="u", password="pw")
    conn = res.get_raw_connection()

    assert conn is sentinel
    assert captured["conn_str"] == build_mssql_conn_str("h", 1433, "db", "u", "pw")

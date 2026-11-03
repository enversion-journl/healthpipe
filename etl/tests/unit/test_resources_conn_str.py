# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Connection construction in core.resources."""

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


@pytest.mark.unit
def test_connect_mssql_python_sets_query_timeout_on_connection(monkeypatch):
    """The driver reads connect(timeout=) as a login timeout, so the query
    timeout must land on Connection.timeout."""
    from types import SimpleNamespace

    import core.resources as resources

    captured = {}

    class _Conn:
        timeout = 0

    def _fake_connect(conn_str, timeout=None, attrs_before=None):
        captured["login_timeout"] = timeout
        captured["attrs_before"] = attrs_before
        return _Conn()

    monkeypatch.setattr(
        resources,
        "mssql_python",
        SimpleNamespace(connect=_fake_connect, SQL_ATTR_LOGIN_TIMEOUT=103),
    )

    conn = resources._connect_mssql_python("cs", timeout=6000)

    assert conn.timeout == 6000
    assert captured == {"login_timeout": None, "attrs_before": {103: 60}}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("user", "expected"),
    [
        ("ext_svc", '"ext_svc"'),
        ("EXT_svc", '"EXT_svc"'),
        ('"ext_svc"', '"ext_svc"'),
        ("svc", "svc"),
    ],
)
def test_ifdb_get_connection_quotes_ext_user(monkeypatch, user, expected):
    from unittest.mock import MagicMock

    import core.resources as resources

    oracledb = MagicMock()
    monkeypatch.setattr(resources, "oracledb", oracledb)

    res = resources.IFDBOracleResource(
        host="h", port=1521, database="db", user=user, password="pw", schema_name="s"
    )
    with res.get_connection():
        pass

    assert oracledb.connect.call_args.kwargs["user"] == expected


@pytest.mark.unit
def test_missing_resource_env_vars_names_every_unset_or_empty_variable(monkeypatch):
    import dagster as dg
    from core.resources import MSSQLResource, missing_resource_env_vars

    monkeypatch.setenv("T_HOST", "h")
    monkeypatch.delenv("T_PORT", raising=False)
    monkeypatch.setenv("T_USER", "")
    monkeypatch.delenv("T_PASSWORD", raising=False)
    resources = {
        "mssql": MSSQLResource(
            host=dg.EnvVar("T_HOST"),
            port=dg.EnvVar.int("T_PORT"),
            database="db",
            user=dg.EnvVar("T_USER"),
            password=dg.EnvVar("T_PASSWORD"),
        ),
        "io_manager": dg.FilesystemIOManager(base_dir="."),
    }

    assert missing_resource_env_vars(resources) == ["T_PASSWORD", "T_PORT", "T_USER"]

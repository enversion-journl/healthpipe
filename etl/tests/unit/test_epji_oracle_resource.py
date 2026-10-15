# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for EPJIOracleResource's Oracle fetch bindings.

EPJI's NLS_CHARACTERSET is WE8ISO8859P1 (confirmed against prod-epjsyd),
distinct from IFDB's WE8MSWIN1252 — free text (e.g. LOCATION.NAME) needs
its own output type handler rather than reusing IFDB's.
"""

from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.unit


class _VarRecorder:
    arraysize = 10_000

    def var(self, db_type, arraysize=None, encoding_errors=None):
        return (db_type, arraysize, encoding_errors)


def test_epji_handler_maps_strings_and_lobs():
    import oracledb
    from core.resources import _epji_oracle_output_type_handler

    cursor = _VarRecorder()
    for str_type in (
        oracledb.DB_TYPE_VARCHAR,
        oracledb.DB_TYPE_CHAR,
        oracledb.DB_TYPE_LONG,
        oracledb.DB_TYPE_NVARCHAR,
        oracledb.DB_TYPE_NCHAR,
    ):
        meta = SimpleNamespace(type_code=str_type)
        assert _epji_oracle_output_type_handler(cursor, meta) == (str, 10_000, "replace")

    for lob_type, expected in (
        (oracledb.DB_TYPE_CLOB, oracledb.DB_TYPE_LONG),
        (oracledb.DB_TYPE_NCLOB, oracledb.DB_TYPE_LONG),
    ):
        meta = SimpleNamespace(type_code=lob_type)
        assert _epji_oracle_output_type_handler(cursor, meta) == (expected, 10_000, "replace")

    meta = SimpleNamespace(type_code=oracledb.DB_TYPE_BLOB)
    assert _epji_oracle_output_type_handler(cursor, meta) == (
        oracledb.DB_TYPE_LONG_RAW,
        10_000,
        None,
    )

    meta = SimpleNamespace(type_code=oracledb.DB_TYPE_NUMBER)
    assert _epji_oracle_output_type_handler(cursor, meta) is None


def test_get_connection_installs_epji_output_type_handler(monkeypatch):
    import core.resources as resources

    captured = {}

    class _FakeConn:
        def __init__(self):
            self.outputtypehandler = None

        def cursor(self):
            class _Cursor:
                def execute(self, *a, **kw):
                    pass

                def close(self):
                    pass

            return _Cursor()

        def close(self):
            pass

    def _fake_connect(user, password, dsn):
        conn = _FakeConn()
        captured["conn"] = conn
        return conn

    monkeypatch.setattr(resources.oracledb, "connect", _fake_connect)
    monkeypatch.setattr(resources.oracledb, "makedsn", lambda *a, **kw: "dsn")

    resource = resources.EPJIOracleResource(
        host="h", port=1522, database="PROD", user="u", password="pw", schema_name="EPJI"
    )
    with resource.get_connection():
        pass

    assert captured["conn"].outputtypehandler is resources._epji_oracle_output_type_handler

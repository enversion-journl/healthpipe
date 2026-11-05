# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for EPJIOracleResource's connection setup."""

import pytest

pytestmark = pytest.mark.unit


def test_get_connection_installs_oracle_output_type_handler(monkeypatch):
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

    assert captured["conn"].outputtypehandler is resources._oracle_output_type_handler

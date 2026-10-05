# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Behavioral tests for the DB-settings enforcement assets.

ALTER DATABASE is rejected inside the driver's implicit multi-statement
transaction, so the settings script must run on an autocommit connection —
without it the TRY/CATCH swallows the error and the settings silently never
apply. The assets must also read the resulting sys.databases flags back and
report them as metadata instead of trusting the script.
"""

import dagster as dg
import pytest
from infrastructure.assets.dso_infrastructure import dso_infra_db_settings
from infrastructure.assets.staging_infrastructure import staging_infra_db_settings


class _RecordingCursor:
    def __init__(self, conn, flag_rows):
        self._conn = conn
        self._flag_rows = flag_rows
        self.rowcount = -1

    def execute(self, sql, *params):
        self._conn.executed.append((sql, self._conn.autocommit))

    def fetchone(self):
        return self._flag_rows.pop(0)

    def close(self):
        pass


class _RecordingConnection:
    def __init__(self, flag_rows):
        self.autocommit = False
        self.executed = []
        self._flag_rows = flag_rows

    def cursor(self):
        return _RecordingCursor(self, self._flag_rows)

    def commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _RecordingResource:
    def __init__(self, flag_row):
        self.connections = []
        self._flag_rows = [flag_row]

    def get_connection(self):
        conn = _RecordingConnection(self._flag_rows)
        self.connections.append(conn)
        return conn


def _run(asset_def, flag_row):
    resource = _RecordingResource(flag_row)
    result = asset_def(dg.build_asset_context(), resource)
    return resource, result


@pytest.mark.unit
@pytest.mark.parametrize(
    "asset_def",
    [staging_infra_db_settings, dso_infra_db_settings],
    ids=["staging", "dso"],
)
def test_settings_script_runs_on_autocommit_connection(asset_def):
    resource, _ = _run(asset_def, flag_row=(1, 0))

    script_conn = resource.connections[0]
    assert script_conn.executed, "The settings script never executed"
    assert all(autocommit for _, autocommit in script_conn.executed), (
        "ALTER DATABASE batches must run autocommit; inside the implicit "
        "transaction they fail and the settings never apply"
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "asset_def",
    [staging_infra_db_settings, dso_infra_db_settings],
    ids=["staging", "dso"],
)
def test_metadata_reflects_flags_read_back_from_sys_databases(asset_def):
    _, result = _run(asset_def, flag_row=(1, 0))

    assert result.metadata["is_auto_update_stats_async_on"].value is True
    assert result.metadata["is_auto_close_on"].value is False


@pytest.mark.unit
@pytest.mark.parametrize(
    "asset_def",
    [staging_infra_db_settings, dso_infra_db_settings],
    ids=["staging", "dso"],
)
def test_unenforced_settings_are_reported_not_raised(asset_def):
    # Missing ALTER DATABASE permission is tolerated (warning path): the asset
    # must still materialize and expose the true flags.
    _, result = _run(asset_def, flag_row=(0, 1))

    assert result.metadata["is_auto_update_stats_async_on"].value is False
    assert result.metadata["is_auto_close_on"].value is True

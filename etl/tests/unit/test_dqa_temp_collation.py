# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Pin DQA ##temp DDL to COLLATE DATABASE_DEFAULT.

tempdb inherits the instance collation (SQL_Latin1 on QA/Prod) while
healthpipe_staging is Danish_Norwegian_CI_AS. Char columns on ##dqa_fhir_ids /
##dqa_ifdb_keys / ##dqa_recon must be collated at CREATE TABLE -- not on the
join -- so IX_dqa_fhir_ids / IX_dqa_recon stay sargable.

String-level only; no live MSSQL.
"""

from pathlib import Path

import dqa
import pytest
from dqa.checks.archive_reconciliation import _stage_ddl as recon_ddl
from dqa.checks.dso_reconciliation import _COLLATE
from dqa.checks.fhir_server import _stage_ddl as fhir_ddl
from dqa.checks.helpers import COLLATE_DB_DEFAULT
from dqa.checks.oracle_source import _stage_ddl as ifdb_ddl

pytestmark = pytest.mark.unit

_DQA_ROOT = Path(dqa.__file__).resolve().parent
_ALLOWED_CREATE_TABLE = {
    "checks/fhir_server.py",
    "checks/oracle_source.py",
    "checks/archive_reconciliation.py",
}


def test_temp_collate_token():
    assert COLLATE_DB_DEFAULT == " COLLATE DATABASE_DEFAULT"
    assert _COLLATE is COLLATE_DB_DEFAULT


def test_fhir_ids_stage_ddl_collates_id():
    sql = fhir_ddl("##dqa_fhir_ids_abc")
    assert sql == (
        "CREATE TABLE ##dqa_fhir_ids_abc ([id] NVARCHAR(255) COLLATE DATABASE_DEFAULT NOT NULL)"
    )


def test_ifdb_keys_stage_ddl_collates_single_and_composite():
    single = ifdb_ddl("##dqa_ifdb_keys_abc", ["k"])
    assert "[k] NVARCHAR(255) COLLATE DATABASE_DEFAULT NOT NULL" in single
    assert "CREATE TABLE ##dqa_ifdb_keys_abc" in single

    composite = ifdb_ddl("##dqa_ifdb_keys_abc", ["k0", "k1"])
    assert "[k0] NVARCHAR(255) COLLATE DATABASE_DEFAULT NOT NULL" in composite
    assert "[k1] NVARCHAR(255) COLLATE DATABASE_DEFAULT NOT NULL" in composite
    assert "INT" not in composite


def test_recon_stage_ddl_collates_keys_not_within_bound():
    sql = recon_ddl("##dqa_recon_abc", ["k0", "k1"])
    assert "[k0] NVARCHAR(255) COLLATE DATABASE_DEFAULT NULL" in sql
    assert "[k1] NVARCHAR(255) COLLATE DATABASE_DEFAULT NULL" in sql
    assert "[within_bound] INT NULL" in sql
    # COLLATE must not be applied to the INT column.
    int_part = sql.split("[within_bound]", 1)[1]
    assert "COLLATE" not in int_part


def test_create_table_stage_allowlist():
    """A fourth ##temp creator under dqa/ must fail this test and look at COLLATE_DB_DEFAULT."""
    found: set[str] = set()
    for path in _DQA_ROOT.rglob("*.py"):
        if "CREATE TABLE " in path.read_text(encoding="utf-8"):
            found.add(path.relative_to(_DQA_ROOT).as_posix())
    assert (
        found == _ALLOWED_CREATE_TABLE
    ), f"unexpected CREATE TABLE site(s): {found - _ALLOWED_CREATE_TABLE}"

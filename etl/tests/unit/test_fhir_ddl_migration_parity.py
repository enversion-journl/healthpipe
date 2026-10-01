# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Base-DDL <-> migration parity for the FHIR flat tables.

THE BUG THIS CATCHES: a schema change lands as a migration but is never
mirrored into ``tables/fhir/*.sql`` (or vice versa). Existing databases get the
column from the migration; fresh ones are stamped by ``baseline_migrations()``
and therefore NEVER execute it, so they only ever get what the base DDL says.
The column then exists in one half of the environments and the SOF merge fails
on the other half.

Until the fhir migrations were folded into the central runner they re-ran on
every ``staging_infra_fhir`` materialization, which silently self-healed this
class of drift. Tracked, run-once migrations remove that safety net, so the
invariant needs a guard.

The parity model follows the migrations in filename order and applies
ADD / DROP COLUMN / DROP TABLE / sp_rename (table and column), because later
migrations legitimately supersede earlier ones — ``reason_code`` is added by
20260701_fhir_servicerequest_add_reason_code.sql and dropped again by
20260804_fhir_servicerequest_reason_reference.sql, and ``is_deleted`` is renamed
to ``dw_is_deleted`` by 20260708_rename_05_fhir_audit.sql. A naive "every ADD
must appear in the base DDL" check reports both as failures.

DROP TABLE is modelled for the same reason: it discards every column an earlier
migration added, because the base DDL's CREATE TABLE is what the environment ends
up with (20260812_fhir_patient_address_typed_period_columns.sql recreates
fhir.Patient_Address this way). Note the consequence — for a table dropped by a
migration this test's invariant becomes vacuous, since both fresh and existing
databases then get their shape from the base DDL alone.

KNOWN LIMITATION — the reverse direction (base DDL has a column that no
migration adds) cannot be asserted statically: it needs the original CREATE to
tell a legitimately-new table from a drifted one. Two historical breaches are
known and predate migration tracking: ``fhir.Patient`` (19 columns added
2026-05-28, covered at the time by the DROP TABLE in
20260515_dim_borger_int_keys_ref_tables.sql) and ``fhir.Observation``
(~26 element_*/value_* columns added 2026-06-01, never carried by a migration).
"""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]  # etl/
MIGRATIONS = REPO / "infrastructure" / "ddl" / "migrations"
FHIR_TABLES = REPO / "infrastructure" / "ddl" / "tables" / "fhir"


def _create_table_blocks(sql: str) -> dict[str, str]:
    """Map table name -> column-definition body for every CREATE TABLE [fhir].[X]."""
    blocks: dict[str, str] = {}
    for match in re.finditer(r"CREATE TABLE \[fhir\]\.\[(\w+)\]\s*\(", sql):
        index, depth = match.end() - 1, 0
        while index < len(sql):
            if sql[index] == "(":
                depth += 1
            elif sql[index] == ")":
                depth -= 1
                if depth == 0:
                    break
            index += 1
        blocks[match.group(1)] = sql[match.end() : index]
    return blocks


def _columns(block: str) -> set[str]:
    return {name.lower() for name in re.findall(r"^\s*\[(\w+)\]", block, re.M)}


def _fhir_migrations() -> list[Path]:
    return sorted(path for path in MIGRATIONS.glob("*.sql") if "fhir" in path.name)


def _net_schema() -> dict[str, set[str]]:
    """Columns each fhir table ends up with after replaying every migration in order."""
    aliases: dict[str, str] = {}
    net: dict[str, set[str]] = {}

    def resolve(table: str) -> str:
        seen: set[str] = set()
        while table in aliases and table not in seen:
            seen.add(table)
            table = aliases[table]
        return table

    for path in _fhir_migrations():
        # Commented-out statements are documentation, not schema changes.
        sql = "\n".join(
            line
            for line in path.read_text(encoding="utf-8").splitlines()
            if not line.lstrip().startswith("--")
        )

        for old, new in re.findall(r"sp_rename\s+'fhir\.(\w+)'\s*,\s*'(\w+)'\s*(?:;|\))", sql):
            aliases[old] = new

        # Dropping the table discards every column an earlier migration added to
        # it -- the base DDL's CREATE TABLE is what the environment ends up with.
        # Processed before the ADDs so a migration that drops and then extends in
        # the same file still requires its new columns to be mirrored.
        for table in re.findall(r"DROP TABLE\s+(?:IF EXISTS\s+)?\[fhir\]\.\[(\w+)\]", sql, re.I):
            net.pop(resolve(table), None)

        # `ADD [col] TYPE` / `ADD [col] AS <computed>` — not ADD CONSTRAINT/PRIMARY KEY.
        for table, column in re.findall(
            r"ALTER TABLE \[fhir\]\.\[(\w+)\]\s*ADD\s+\[(\w+)\]\s+(?:\w|AS)", sql, re.I
        ):
            net.setdefault(resolve(table), set()).add(column.lower())

        for table, column in re.findall(
            r"ALTER TABLE \[fhir\]\.\[(\w+)\]\s*DROP COLUMN\s+\[(\w+)\]", sql, re.I
        ):
            net.get(resolve(table), set()).discard(column.lower())

        for table, old, new in re.findall(
            r"sp_rename\s+'fhir\.(\w+)\.(\w+)'\s*,\s*'(\w+)'\s*,\s*'COLUMN'", sql, re.I
        ):
            columns = net.get(resolve(table))
            if columns and old.lower() in columns:
                columns.discard(old.lower())
                columns.add(new.lower())

    return net


@pytest.mark.unit
def test_migration_columns_are_mirrored_in_base_ddl():
    base = _create_table_blocks(
        "\n".join(path.read_text(encoding="utf-8") for path in sorted(FHIR_TABLES.glob("*.sql")))
    )
    net = _net_schema()
    assert net, "no fhir migrations parsed — check the naming convention or the glob"

    missing = [
        f"fhir.{table}.{column}"
        for table, columns in sorted(net.items())
        for column in sorted(columns - _columns(base.get(table, "")))
    ]
    assert missing == [], (
        "Columns added by a fhir migration but absent from the base DDL in "
        "infrastructure/ddl/tables/fhir/. Fresh databases are baselined and never run "
        "the migration, so they would never get these columns. Mirror the change into "
        f"the CREATE TABLE statement: {missing}"
    )


@pytest.mark.unit
def test_migrations_target_tables_that_exist_in_base_ddl():
    base = _create_table_blocks(
        "\n".join(path.read_text(encoding="utf-8") for path in sorted(FHIR_TABLES.glob("*.sql")))
    )
    unknown = sorted(table for table in _net_schema() if table not in base)
    assert unknown == [], (
        "fhir migrations alter tables that no CREATE TABLE in tables/fhir/ produces — "
        f"the migration or the base DDL is stale: {unknown}"
    )

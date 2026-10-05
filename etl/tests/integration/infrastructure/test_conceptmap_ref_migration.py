# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Validate 20260604_conceptmap_move_from_ref_schema.sql drops ref.ConceptMap_* objects."""

from __future__ import annotations

import os

import pytest
from core.resources import MSSQLResource
from infrastructure.utils.ddl_loader import execute_sql, load_sql_files_from_directory

MIGRATION_FILE = "20260604_conceptmap_move_from_ref_schema.sql"
REF_CONCEPTMAP_OBJECTS = [
    ("ref", "ConceptMap_Rekvirent_To_SOR"),
    ("ref", "ConceptMap_Lager_To_SOR"),
    ("ref", "ConceptMap_EAN_To_SOR"),
    ("ref", "ConceptMap_Okosted_To_SOR"),
]


def _mssql_resource(database: str) -> MSSQLResource:
    return MSSQLResource(
        host=os.getenv("MSSQL_TARGET_DSO_HOST", "mssql"),
        port=int(os.getenv("MSSQL_TARGET_DSO_PORT", "1433")),
        database=database,
        user=os.getenv("MSSQL_TARGET_DSO_USER", "sa"),
        password=os.getenv("MSSQL_TARGET_DSO_PASSWORD", "tempPassword1!"),
    )


def _list_ref_conceptmap_objects(resource: MSSQLResource) -> list[tuple[str, str, str]]:
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT s.name, o.name, o.type_desc
                FROM sys.objects o
                INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE s.name = 'ref'
                  AND o.name IN (
                      'ConceptMap_Rekvirent_To_SOR',
                      'ConceptMap_Lager_To_SOR',
                      'ConceptMap_EAN_To_SOR',
                      'ConceptMap_Okosted_To_SOR'
                  )
                ORDER BY o.name
                """
            )
            return [(row[0], row[1], row[2]) for row in cursor.fetchall()]
        finally:
            cursor.close()


def _migration_sql() -> str:
    files = dict(load_sql_files_from_directory("migrations"))
    assert MIGRATION_FILE in files, f"Missing migration file {MIGRATION_FILE}"
    return files[MIGRATION_FILE]


@pytest.mark.integration
@pytest.mark.parametrize("database", ["staging", "dso"])
def test_conceptmap_migration_drops_ref_objects(database: str) -> None:
    """Migration is idempotent and leaves no ref.ConceptMap_* tables or views."""
    resource = _mssql_resource(database)
    sql = _migration_sql()

    execute_sql(resource, sql, description=MIGRATION_FILE)
    after_first = _list_ref_conceptmap_objects(resource)
    assert (
        after_first == []
    ), f"{database}: ref.ConceptMap_* still present after migration: {after_first}"

    execute_sql(resource, sql, description=f"{MIGRATION_FILE} (idempotent re-run)")
    after_second = _list_ref_conceptmap_objects(resource)
    assert (
        after_second == []
    ), f"{database}: ref.ConceptMap_* reappeared after second run: {after_second}"


@pytest.mark.integration
def test_dso_conceptmap_tables_exist_after_migration() -> None:
    """DSO keeps lowercase conceptmap.* reporting tables; PascalCase ones are dropped."""
    resource = _mssql_resource("dso")
    expected_present = ["rekvirent_to_sor", "lager_to_sor", "ean_to_sor", "okosted_to_sor"]
    expected_absent = ["Rekvirent_To_SOR", "Lager_To_SOR", "EAN_To_SOR", "Okosted_To_SOR"]
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for table in expected_present:
                cursor.execute(
                    f"""
                    SELECT 1 FROM sys.tables t
                    INNER JOIN sys.schemas sch ON t.schema_id = sch.schema_id
                    WHERE sch.name = N'conceptmap'
                      AND t.name COLLATE Latin1_General_CS_AS = N'{table}'
                    """
                )
                assert cursor.fetchone() is not None, f"Missing conceptmap.{table} on DSO"
            for table in expected_absent:
                cursor.execute(
                    f"""
                    SELECT 1 FROM sys.tables t
                    INNER JOIN sys.schemas sch ON t.schema_id = sch.schema_id
                    WHERE sch.name = N'conceptmap'
                      AND t.name COLLATE Latin1_General_CS_AS = N'{table}'
                    """
                )
                assert cursor.fetchone() is None, f"PascalCase conceptmap.{table} still on DSO"
        finally:
            cursor.close()

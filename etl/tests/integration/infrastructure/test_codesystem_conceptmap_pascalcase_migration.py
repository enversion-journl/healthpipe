# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Validate 20260616_codesystem_conceptmap_pascalcase_rename.sql renames
codesystem.* and conceptmap.* tables to PascalCase, in place, idempotently."""

from __future__ import annotations

import os

import pytest
from core.resources import MSSQLResource
from infrastructure.utils.ddl_loader import execute_sql, load_sql_files_from_directory

MIGRATION_FILE = "20260616_codesystem_conceptmap_pascalcase_rename.sql"

CODESYSTEM_PASCAL = [
    "RekvirentConcept",
    "RekvirentProperties",
    "RekvirentHierarchy",
    "LagerConcept",
    "LagerProperties",
    "LagerHierarchy",
    "EANConcept",
    "EANProperties",
    "EANHierarchy",
    "SORConcept",
    "SORProperties",
    "SORHierarchy",
    "OkostedConcept",
    "OkostedProperties",
    "OkostedHierarchy",
    "SFIConcept",
    "SFIProperties",
    "SFIHierarchy",
    "SFIAlias",
    "SFIAliasMember",
    "SFISearchDocument",
    "SFISearchOverride",
    "SKSConcept",
    "SKSProperties",
    "SKSHierarchy",
]
CODESYSTEM_SNAKE = [
    "rekvirent_concept",
    "rekvirent_properties",
    "rekvirent_hierarchy",
    "lager_concept",
    "lager_properties",
    "lager_hierarchy",
    "ean_concept",
    "ean_properties",
    "ean_hierarchy",
    "sor_concept",
    "sor_properties",
    "sor_hierarchy",
    "okosted_concept",
    "okosted_properties",
    "okosted_hierarchy",
    "sfi_concept",
    "sfi_properties",
    "sfi_hierarchy",
    "sfi_alias",
    "sfi_alias_member",
    "sfi_search_document",
    "sfi_search_override",
    "sks_concept",
    "sks_properties",
    "sks_hierarchy",
]
CONCEPTMAP_PASCAL = ["RekvirentToSOR", "LagerToSOR", "EANToSOR", "OkostedToSOR"]
CONCEPTMAP_SNAKE = ["rekvirent_to_sor", "lager_to_sor", "ean_to_sor", "okosted_to_sor"]


def _resource(database: str) -> MSSQLResource:
    prefix = "MSSQL_TARGET_DSO" if database == "dso" else "MSSQL_TARGET_STAGING"
    return MSSQLResource(
        host=os.getenv(f"{prefix}_HOST", "mssql"),
        port=int(os.getenv(f"{prefix}_PORT", "1433")),
        database=database,
        user=os.getenv(f"{prefix}_USER", "sa"),
        password=os.getenv(f"{prefix}_PASSWORD", "tempPassword1!"),
    )


def _migration_sql() -> str:
    files = dict(load_sql_files_from_directory("migrations"))
    assert MIGRATION_FILE in files, f"Missing migration file {MIGRATION_FILE}"
    return files[MIGRATION_FILE]


def _table_exists_cs(resource: MSSQLResource, schema: str, table: str) -> bool:
    """Case-SENSITIVE existence check, so 'sks_concept' != 'SKSConcept'."""
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT 1 FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = ?
                  AND t.name COLLATE Latin1_General_CS_AS = ?
                """,
                (schema, table),
            )
            return cursor.fetchone() is not None
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.parametrize("database", ["staging", "dso"])
def test_pascalcase_tables_present_snake_absent(database: str) -> None:
    resource = _resource(database)
    sql = _migration_sql()

    execute_sql(resource, sql, description=MIGRATION_FILE)

    for table in CODESYSTEM_PASCAL:
        assert _table_exists_cs(
            resource, "codesystem", table
        ), f"{database}: missing codesystem.{table}"
    for table in CONCEPTMAP_PASCAL:
        assert _table_exists_cs(
            resource, "conceptmap", table
        ), f"{database}: missing conceptmap.{table}"
    for table in CODESYSTEM_SNAKE:
        assert not _table_exists_cs(
            resource, "codesystem", table
        ), f"{database}: codesystem.{table} still present"
    for table in CONCEPTMAP_SNAKE:
        assert not _table_exists_cs(
            resource, "conceptmap", table
        ), f"{database}: conceptmap.{table} still present"


@pytest.mark.integration
@pytest.mark.parametrize("database", ["staging", "dso"])
def test_migration_is_idempotent(database: str) -> None:
    resource = _resource(database)
    sql = _migration_sql()
    execute_sql(resource, sql, description=f"{MIGRATION_FILE} (re-run 1)")
    execute_sql(resource, sql, description=f"{MIGRATION_FILE} (re-run 2)")
    for table in CODESYSTEM_PASCAL:
        assert _table_exists_cs(resource, "codesystem", table)
    for table in CONCEPTMAP_PASCAL:
        assert _table_exists_cs(resource, "conceptmap", table)

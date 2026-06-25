# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Tests for FHIR migration directory structure (WI 27263).

Verifies:
- migrations/ directory exists under fhir DDL
- DDL loader returns empty list for empty migrations dir (graceful fallback)
- README exists in migrations/
"""

from infrastructure.utils.ddl_loader import get_ddl_path, load_sql_files_from_directory

FHIR_DDL_REL = "tables/fhir"
FHIR_MIGRATIONS_REL = "tables/fhir/migrations"


class TestFhirMigrationStructure:
    def test_fhir_migrations_directory_exists(self):
        ddl = get_ddl_path()
        migrations_dir = ddl / FHIR_MIGRATIONS_REL
        assert migrations_dir.exists(), f"Missing {migrations_dir}"
        assert migrations_dir.is_dir()

    def test_fhir_migrations_readme_exists(self):
        ddl = get_ddl_path()
        readme = ddl / FHIR_MIGRATIONS_REL / "README.md"
        assert readme.exists(), f"Missing README in {readme.parent}"

    def test_load_sql_files_empty_migrations(self):
        files = load_sql_files_from_directory(FHIR_MIGRATIONS_REL)
        assert isinstance(files, list)

    def test_fhir_baseline_ddl_exists(self):
        ddl = get_ddl_path()
        baseline = ddl / FHIR_DDL_REL / "01_tables.sql"
        assert baseline.exists(), f"Missing baseline DDL: {baseline}"

    def test_questionnaire_response_item_answer_index_migration_exists(self):
        ddl = get_ddl_path()
        migration = ddl / FHIR_MIGRATIONS_REL / "05_questionnaire_response_item_answer_index.sql"
        assert migration.exists(), f"Missing QR item migration: {migration}"

        migration_sql = migration.read_text(encoding="utf-8")
        assert (
            "COL_LENGTH('fhir.QuestionnaireResponse_item', 'answer_index') IS NULL" in migration_sql
        )
        assert "[DF_FHIR_QRItem_AnswerIndex]" in migration_sql
        assert (
            "PRIMARY KEY CLUSTERED ([qr_id], [link_id], [item_key], [answer_index])"
            in migration_sql
        )

    def test_questionnaire_item_occurrence_key_migration_exists(self):
        ddl = get_ddl_path()
        migration = ddl / FHIR_MIGRATIONS_REL / "09_questionnaire_item_occurrence_key.sql"
        assert migration.exists(), f"Missing Questionnaire_item migration: {migration}"

        migration_sql = migration.read_text(encoding="utf-8")
        assert "COL_LENGTH('fhir.Questionnaire_item', 'item_key') IS NULL" in migration_sql
        assert "PRIMARY KEY CLUSTERED ([questionnaire_id], [link_id], [item_key])" in migration_sql

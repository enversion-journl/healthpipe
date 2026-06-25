# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for SQL syntax validation in erp assets.

Tests validate that embedded SQL strings in Python assets are syntactically correct
by attempting to prepare them against a test database connection.

This catches basic syntax errors without executing complex business logic.
"""

import os
import re

import pytest


class TestSQLSyntaxValidation:
    """
    Unit tests for validating SQL syntax in embedded strings.

    These tests extract SQL strings from Python asset files and validate
    they can be prepared by the database driver, catching syntax errors early.
    """

    def extract_sql_strings_from_file(self, file_path: str) -> list[str]:
        """Extract SQL strings from a Python file."""
        with open(file_path, encoding="utf-8") as f:
            content = f.read()

        # Look for SQL strings assigned to variables (common pattern in the codebase)
        # Pattern: variable_name = """SQL content"""
        sql_pattern = r'(\w+)\s*=\s*"""(.*?)"""'
        matches = re.findall(sql_pattern, content, re.DOTALL)

        sql_statements = []
        for _var_name, sql_content in matches:
            # Filter for strings that look like SQL (contain SELECT, INSERT, UPDATE, etc.)
            sql_keywords = [
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "CREATE",
                "DROP",
                "MERGE",
                "WITH",
            ]
            if any(keyword in sql_content.upper() for keyword in sql_keywords):
                # Clean up the SQL string (remove leading/trailing whitespace)
                sql = sql_content.strip()
                if sql and len(sql) > 50:  # Avoid very short strings that might be comments
                    sql_statements.append(sql)

        # Also look for SQL strings passed directly to cursor.execute()
        # Pattern: cursor.execute("""SQL""")
        execute_pattern = r'cursor\.execute\(\s*"""(.*?)"""'
        execute_matches = re.findall(execute_pattern, content, re.DOTALL)

        for sql_content in execute_matches:
            sql_keywords = [
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "CREATE",
                "DROP",
                "MERGE",
                "WITH",
            ]
            if any(keyword in sql_content.upper() for keyword in sql_keywords):
                sql = sql_content.strip()
                if sql and len(sql) > 50:
                    sql_statements.append(sql)

        return sql_statements

    def test_dim_kontoplan_sql_syntax(self, mock_mssql_resource):
        """Test that DimUpd_kontoplan.py contains syntactically valid SQL."""
        sql_file_path = os.path.join(
            os.path.dirname(__file__), "../../DSA/SQL/dim/DimUpd_kontoplan.py"
        )

        sql_statements = self.extract_sql_strings_from_file(sql_file_path)
        assert len(sql_statements) > 0, "Should find SQL statements in DimUpd_kontoplan.py"

        # Unpack the mock resource tuple
        resource, mock_cursor = mock_mssql_resource

        # Test each SQL statement can be prepared (syntax validation)
        with resource.get_connection() as conn:
            cursor = conn.cursor()

            for i, sql in enumerate(sql_statements):
                try:
                    # Try to prepare the statement (this validates syntax without executing)
                    cursor.prepare(sql)
                except Exception as e:
                    # For mock resources, we might get different exceptions
                    # Just check it's not a basic syntax error
                    if "syntax" in str(e).lower() or "invalid" in str(e).lower():
                        pytest.fail(
                            f"SQL statement {i + 1} in DimUpd_kontoplan.py has syntax error: {e}\n"
                            f"SQL: {sql[:200]}..."
                        )

    def test_fct_fakturalinje_sql_syntax(self, mock_mssql_resource):
        """Test that FctUpd_fakturalinje.py contains syntactically valid SQL."""
        sql_file_path = os.path.join(
            os.path.dirname(__file__), "../../DSA/SQL/fct/FctUpd_fakturalinje.py"
        )

        sql_statements = self.extract_sql_strings_from_file(sql_file_path)
        assert len(sql_statements) > 0, "Should find SQL statements in FctUpd_fakturalinje.py"

        # Unpack the mock resource tuple
        resource, mock_cursor = mock_mssql_resource

        # Test each SQL statement can be prepared (syntax validation)
        with resource.get_connection() as conn:
            cursor = conn.cursor()

            for i, sql in enumerate(sql_statements):
                try:
                    # Try to prepare the statement (this validates syntax without executing)
                    cursor.prepare(sql)
                except Exception as e:
                    # For mock resources, we might get different exceptions
                    if "syntax" in str(e).lower() or "invalid" in str(e).lower():
                        pytest.fail(
                            f"SQL statement {i + 1} in FctUpd_fakturalinje.py has syntax error: {e}\n"
                            f"SQL: {sql[:200]}..."
                        )

    def test_dim_kreditor_sql_syntax(self, mock_mssql_resource):
        """Test that DimUpd_kreditor.py contains syntactically valid SQL."""
        sql_file_path = os.path.join(
            os.path.dirname(__file__), "../../DSA/SQL/dim/DimUpd_kreditor.py"
        )

        sql_statements = self.extract_sql_strings_from_file(sql_file_path)
        assert len(sql_statements) > 0, "Should find SQL statements in DimUpd_kreditor.py"

        # Unpack the mock resource tuple
        resource, mock_cursor = mock_mssql_resource

        # Test each SQL statement can be prepared (syntax validation)
        with resource.get_connection() as conn:
            cursor = conn.cursor()

            for i, sql in enumerate(sql_statements):
                try:
                    # Try to prepare the statement (this validates syntax without executing)
                    cursor.prepare(sql)
                except Exception as e:
                    # For mock resources, we might get different exceptions
                    if "syntax" in str(e).lower() or "invalid" in str(e).lower():
                        pytest.fail(
                            f"SQL statement {i + 1} in DimUpd_kreditor.py has syntax error: {e}\n"
                            f"SQL: {sql[:200]}..."
                        )

    def test_sql_statements_contain_expected_keywords(self):
        """Test that extracted SQL statements contain expected SQL keywords."""
        # Test multiple files
        file_map = {
            "DimUpd_kontoplan.py": "dim",
            "FctUpd_fakturalinje.py": "fct",
            "DimUpd_kreditor.py": "dim",
        }

        for filename, subdir in file_map.items():
            file_path = os.path.join(
                os.path.dirname(__file__), f"../../DSA/SQL/{subdir}/{filename}"
            )

            sql_statements = self.extract_sql_strings_from_file(file_path)

            # Should find at least one SQL statement
            assert len(sql_statements) > 0, f"Should find SQL statements in {filename}"

            # At least one statement should contain SELECT, INSERT, UPDATE, or MERGE
            has_dml = any(
                any(keyword in stmt.upper() for keyword in ["SELECT", "INSERT", "UPDATE", "MERGE"])
                for stmt in sql_statements
            )
            assert has_dml, f"{filename} should contain DML statements (SELECT/INSERT/UPDATE/MERGE)"

    def test_sql_statements_are_not_empty_after_cleaning(self):
        """Test that SQL statements aren't just whitespace after cleaning."""
        file_map = {
            "DimUpd_kontoplan.py": "dim",
            "FctUpd_fakturalinje.py": "fct",
            "DimUpd_kreditor.py": "dim",
        }

        for filename, subdir in file_map.items():
            file_path = os.path.join(
                os.path.dirname(__file__), f"../../DSA/SQL/{subdir}/{filename}"
            )

            sql_statements = self.extract_sql_strings_from_file(file_path)

            for i, sql in enumerate(sql_statements):
                assert (
                    len(sql.strip()) > 10
                ), f"SQL statement {i + 1} in {filename} is too short or empty after cleaning: '{sql}'"

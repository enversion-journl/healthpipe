#!/usr/bin/env python

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Script to execute the seed_erp_test_data.sql against the databases.
Parses the SQL file and executes statements per database.
"""

import re
import sys
from pathlib import Path

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.resources import ILSMSSQLResource, MySupplyMSSQLResource, SydaxMSSQLResource


def parse_sql_by_database(sql_content):
    """Parse SQL file into sections by database."""
    sections = {}
    current_db = None
    current_statements = []

    lines = sql_content.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Check for USE statement
        use_match = re.match(r"USE\s+\[([^\]]+)\];", line)
        if use_match:
            # Save previous section
            if current_db and current_statements:
                sections[current_db] = "\n".join(current_statements)

            current_db = use_match.group(1)
            current_statements = []
            i += 1
            continue

        # Skip GO statements
        if line.upper() == "GO":
            i += 1
            continue

        # Skip comments and empty lines unless we're in a database section
        if current_db:
            if line.startswith("--") and not line.startswith("-- USE"):
                # Skip comment lines in verification section
                if "SELECT" not in line:
                    i += 1
                    continue
            current_statements.append(lines[i])

        i += 1

    # Save last section
    if current_db and current_statements:
        sections[current_db] = "\n".join(current_statements)

    return sections


def execute_sql_statements(resource, sql_content, db_name):
    """Execute SQL statements for a database."""
    print(f"\n{'=' * 60}")
    print(f"Seeding {db_name} database...")
    print(f"{'=' * 60}")

    # Split by MERGE statements to execute individually
    merge_pattern = re.compile(r"(MERGE INTO.*?;)", re.DOTALL | re.IGNORECASE)
    statements = merge_pattern.findall(sql_content)

    if not statements:
        print(f"⚠️  No MERGE statements found in {db_name} section")
        return

    try:
        with resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                executed = 0
                for stmt in statements:
                    stmt = stmt.strip()
                    if not stmt or stmt.startswith("--"):
                        continue

                    # Extract table name for logging
                    table_match = re.search(
                        r"MERGE INTO\s+\[?([^\]\s]+)\]?\.?\[?([^\]\s]+)\]?", stmt, re.IGNORECASE
                    )
                    if table_match:
                        if table_match.group(2):
                            table_name = f"{table_match.group(1)}.{table_match.group(2)}"
                        else:
                            table_name = table_match.group(1)
                    else:
                        table_name = "unknown"

                    print(f"  Executing MERGE for {table_name}...", end=" ")
                    try:
                        cursor.execute(stmt)
                        rows_affected = cursor.rowcount
                        print(f"✅ ({rows_affected} rows affected)")
                        executed += 1
                    except Exception as e:
                        print(f"❌ Error: {e}")
                        # Continue with other statements even if one fails

                conn.commit()
                print(
                    f"\n✅ {db_name}: Successfully executed {executed}/{len(statements)} statements"
                )
            finally:
                cursor.close()

    except Exception as e:
        print(f"❌ {db_name}: Failed to connect or execute - {e}")
        raise


def main():
    """Main execution function."""
    # Read SQL file
    sql_file = Path(__file__).parent.parent / "tests" / "fixtures" / "seed_erp_test_data.sql"

    if not sql_file.exists():
        print(f"❌ Error: SQL file not found at {sql_file}")
        sys.exit(1)

    print(f"📄 Reading seed script from {sql_file}")
    sql_content = sql_file.read_text()

    # Parse SQL by database
    sections = parse_sql_by_database(sql_content)

    if not sections:
        print("❌ Error: No database sections found in SQL file")
        sys.exit(1)

    print(f"📋 Found {len(sections)} database sections: {', '.join(sections.keys())}")

    # Execute for each database
    success_count = 0

    # MySupply
    if "mysupply" in sections:
        try:
            mysupply = MySupplyMSSQLResource().process_config_and_initialize()
            execute_sql_statements(mysupply, sections["mysupply"], "MySupply")
            success_count += 1
        except Exception as e:
            print(f"❌ MySupply seeding failed: {e}")

    # Sydax
    if "sydax" in sections:
        try:
            sydax = SydaxMSSQLResource().process_config_and_initialize()
            execute_sql_statements(sydax, sections["sydax"], "Sydax")
            success_count += 1
        except Exception as e:
            print(f"❌ Sydax seeding failed: {e}")

    # ILS
    if "ils" in sections:
        try:
            ils = ILSMSSQLResource().process_config_and_initialize()
            execute_sql_statements(ils, sections["ils"], "ILS")
            success_count += 1
        except Exception as e:
            print(f"❌ ILS seeding failed: {e}")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Seeding Summary: {success_count}/{len(sections)} databases seeded successfully")
    print(f"{'=' * 60}")

    if success_count == len(sections):
        print("✅ All test data seeded successfully!")
        return 0
    else:
        print("⚠️  Some databases failed to seed - check errors above")
        return 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python
"""Check actual database schemas to understand column names."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.resources import ILSMSSQLResource, MySupplyMSSQLResource, SydaxMSSQLResource


def check_table_schema(resource, schema, table, db_name):
    """Check columns in a table."""
    print(f"\n{db_name}.{schema}.{table}:")
    try:
        with resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                query = f"SELECT TOP 0 * FROM [{schema}].[{table}]"
                cursor.execute(query)

                if cursor.description:
                    print(f"  Columns: {', '.join([col[0] for col in cursor.description])}")
                else:
                    print("  No columns found")
            finally:
                cursor.close()
    except Exception as e:
        print(f"  Error: {e}")


def main():
    """Check schemas."""
    print("=" * 60)
    print("Checking Database Schemas")
    print("=" * 60)

    # MySupply
    print("\n--- MySupply Database ---")
    mysupply = MySupplyMSSQLResource()
    check_table_schema(mysupply, "dbo", "metadata", "MySupply")
    check_table_schema(mysupply, "dbo", "statistic_invoice_header", "MySupply")
    check_table_schema(mysupply, "dbo", "statistic_invoice_line", "MySupply")

    # Sydax
    print("\n--- Sydax Database ---")
    sydax = SydaxMSSQLResource()
    check_table_schema(sydax, "Kreditor", "Kreditor", "Sydax")
    check_table_schema(sydax, "Finans", "Finanskonto", "Sydax")
    check_table_schema(sydax, "Finans", "Finanspostering", "Sydax")

    # ILS
    print("\n--- ILS Database ---")
    ils = ILSMSSQLResource()
    check_table_schema(ils, "dbo", "lev", "ILS")
    check_table_schema(ils, "dbo", "vare", "ILS")


if __name__ == "__main__":
    main()

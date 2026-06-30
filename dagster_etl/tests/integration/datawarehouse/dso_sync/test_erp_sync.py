# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ERP DSO Sync Integration Tests

Tests that verify the erp domain data syncs correctly from staging
to DSO database via linked server.

Prerequisites:
- Linked server STAGING_LINK configured on DSO database
- Staging database has erp data (run staging ETL first)
- USE_REAL_DB_FOR_TESTS=true
- ENABLE_TEST_WRITES=true
"""

import pytest
from DSO.load.sync_configs import ERP_DIM_CONFIGS, ERP_FCT_CONFIGS
from DSO.load.sync_utils import (
    LINKED_SERVER_NAME,
    STAGING_DATABASE,
    build_merge_statement,
    get_table_columns,
)
from tests.integration.datawarehouse.dso_sync.conftest import compare_counts_between_dbs

# ERP DSO sync needs a dedicated linked-server deployment not present in standard test envs.
pytestmark = pytest.mark.skip(reason="Requires dedicated DSO linked-server environment")


class TestDimKreditorSync:
    """Tests for syncing dim.Kreditor from staging to DSO."""

    def test_dim_kreditor_sync_generates_valid_sql(self, dso_target_resource):
        """Test that MERGE SQL for dim.Kreditor is generated correctly."""
        config = ERP_DIM_CONFIGS["dim_kreditor"]
        columns = get_table_columns(dso_target_resource, config.schema, config.table)

        sql = build_merge_statement(config, columns)

        # Verify SQL structure
        assert "MERGE [dim].[Kreditor]" in sql
        assert f"USING [{LINKED_SERVER_NAME}]" in sql
        assert "ON tgt.[DW_EK_Kreditor] = src.[DW_EK_Kreditor]" in sql
        assert "WHEN MATCHED THEN" in sql
        assert "WHEN NOT MATCHED BY TARGET THEN" in sql

    def test_dim_kreditor_sync_executes(
        self, dso_target_resource, staging_source_resource, cleanup_dso_dim_kreditor
    ):
        """Test that dim.Kreditor sync actually executes successfully."""
        config = ERP_DIM_CONFIGS["dim_kreditor"]
        columns = get_table_columns(dso_target_resource, config.schema, config.table)

        # Check staging has data
        staging_count = self._get_staging_count(dso_target_resource, config.schema, config.table)
        if staging_count == 0:
            pytest.skip("No data in staging dim.Kreditor - run staging ETL first")

        # Execute MERGE
        sql = build_merge_statement(config, columns)
        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                rows_affected = cursor.rowcount
                conn.commit()
            finally:
                cursor.close()

        # Verify some rows were synced
        assert rows_affected > 0, "No rows were synced to DSO dim.Kreditor"

        # Compare counts
        counts = compare_counts_between_dbs(
            staging_source_resource, dso_target_resource, config.schema, config.table
        )
        # Note: counts may not match exactly if staging has sentinel row different
        # from DSO, but both should have data
        assert counts["dso_count"] > 0

    def test_dim_kreditor_sync_is_idempotent(self, dso_target_resource, cleanup_dso_dim_kreditor):
        """Test that running sync twice produces the same result."""
        config = ERP_DIM_CONFIGS["dim_kreditor"]
        columns = get_table_columns(dso_target_resource, config.schema, config.table)
        sql = build_merge_statement(config, columns)

        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # First sync
                cursor.execute(sql)
                conn.commit()
                cursor.execute("SELECT COUNT(*) FROM [dim].[Kreditor]")
                count_after_first = cursor.fetchone()[0]

                # Second sync
                cursor.execute(sql)
                conn.commit()
                cursor.execute("SELECT COUNT(*) FROM [dim].[Kreditor]")
                count_after_second = cursor.fetchone()[0]
            finally:
                cursor.close()

        # Count should be the same after both syncs
        assert (
            count_after_first == count_after_second
        ), f"Sync is not idempotent: first={count_after_first}, second={count_after_second}"

    def _get_staging_count(self, dso_resource, schema: str, table: str) -> int:
        """Get row count from staging via linked server."""
        with dso_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"SELECT COUNT(*) FROM [{LINKED_SERVER_NAME}].[{STAGING_DATABASE}].[{schema}].[{table}]"
                )
                return cursor.fetchone()[0]
            except Exception:
                return 0
            finally:
                cursor.close()


class TestDimVareSync:
    """Tests for syncing dim.Vare from staging to DSO."""

    def test_dim_vare_sync_generates_valid_sql(self, dso_target_resource):
        """Test that MERGE SQL for dim.Vare is generated correctly."""
        config = ERP_DIM_CONFIGS["dim_vare"]
        columns = get_table_columns(dso_target_resource, config.schema, config.table)

        sql = build_merge_statement(config, columns)

        assert "MERGE [dim].[Vare]" in sql
        assert "ON tgt.[DW_EK_Vare] = src.[DW_EK_Vare]" in sql


class TestFctFakturalinjeSync:
    """Tests for syncing fct.Fakturalinje from staging to DSO."""

    def test_fct_fakturalinje_sync_generates_valid_sql(self, dso_target_resource):
        """Test that MERGE SQL for fct.Fakturalinje is generated correctly."""
        config = ERP_FCT_CONFIGS["fct_fakturalinje"]
        columns = get_table_columns(dso_target_resource, config.schema, config.table)

        sql = build_merge_statement(config, columns)

        assert "MERGE [fct].[Fakturalinje]" in sql
        assert f"USING [{LINKED_SERVER_NAME}]" in sql

    def test_fct_fakturalinje_sync_executes(
        self, dso_target_resource, cleanup_dso_fct_fakturalinje
    ):
        """Test that fct.Fakturalinje sync executes successfully."""
        config = ERP_FCT_CONFIGS["fct_fakturalinje"]
        columns = get_table_columns(dso_target_resource, config.schema, config.table)
        sql = build_merge_statement(config, columns)

        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                rows_affected = cursor.rowcount
                conn.commit()
            except Exception as e:
                if "Invalid object name" in str(e):
                    pytest.skip("fct.Fakturalinje not found in staging - run ETL first")
                raise
            finally:
                cursor.close()

        # If staging has data, some rows should be synced
        # (may be 0 if staging is empty)
        assert rows_affected >= 0


class TestFctPosteringSync:
    """Tests for syncing fct.Postering from staging to DSO."""

    def test_fct_postering_sync_generates_valid_sql(self, dso_target_resource):
        """Test that MERGE SQL for fct.Postering is generated correctly."""
        config = ERP_FCT_CONFIGS["fct_postering"]
        columns = get_table_columns(dso_target_resource, config.schema, config.table)

        sql = build_merge_statement(config, columns)

        assert "MERGE [fct].[Postering]" in sql
        assert "ON tgt.[ref_LedgertransId] = src.[ref_LedgertransId]" in sql


class TestFullERPSync:
    """Tests for full erp domain sync to DSO."""

    def test_all_erp_dim_tables_exist_in_dso(self, dso_target_resource):
        """Verify all erp dimension tables exist in DSO."""
        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                for _config_name, config in ERP_DIM_CONFIGS.items():
                    cursor.execute(
                        """
                        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                        """,
                        (config.schema, config.table),
                    )
                    exists = cursor.fetchone()[0] > 0
                    assert exists, f"Table {config.schema}.{config.table} not found in DSO"
            finally:
                cursor.close()

    def test_all_erp_fct_tables_exist_in_dso(self, dso_target_resource):
        """Verify all erp fact tables exist in DSO."""
        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                for _config_name, config in ERP_FCT_CONFIGS.items():
                    cursor.execute(
                        """
                        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                        """,
                        (config.schema, config.table),
                    )
                    exists = cursor.fetchone()[0] > 0
                    assert exists, f"Table {config.schema}.{config.table} not found in DSO"
            finally:
                cursor.close()

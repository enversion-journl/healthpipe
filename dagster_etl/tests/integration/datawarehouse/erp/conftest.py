# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Configuration and fixtures for erp integration tests.

This conftest.py provides integration-level fixtures specific to the erp domain
that manage selective cleanup of staging, map, dim, and fact tables.

Design principle: Each cleanup fixture only cleans its own layer.
- cleanup_erp_staging_tables → cleans only stg.* tables
- cleanup_erp_map_tables → cleans only map.* tables
- cleanup_erp_dim_tables → cleans only dim.* tables
- cleanup_erp_fact_tables → cleans only fct.* tables
- cleanup_erp_all_tables → cleans all layers (for full reset)

Tests are responsible for materializing their own dependencies. If a map test
needs staging data, it should include the staging assets in its materialization.
"""

import logging
import os
from dataclasses import replace
from datetime import date

import pytest
from dagster import ResourceDefinition
from tests.utils.materialize_helpers import materialize_assets_with_dependencies

logger = logging.getLogger(__name__)

ERP_STAGING_FULLLOAD_DRIVER_KEYS = [
    # Triggers full-load ILS multi-asset (including ext_lev_ils, ext_vare_ils, etc.)
    "ext_lev_ils",
]

ERP_STAGING_DAILY_DRIVER_KEYS = [
    # One output per daily multi-asset is sufficient to trigger all outputs.
    "ext_laftd_ils_daily",
    "ext_kreditor_sydax_daily",
    "ext_statistic_invoice_line_mysupply_daily",
]

ERP_STAGING_MONTHLY_DRIVER_KEYS = [
    # Monthly dependencies used by pre-staging assets.
    "ext_kreditor_sydax_monthly",
    "ext_statistic_invoice_line_mysupply_monthly",
]


@pytest.fixture(autouse=True)
def require_real_erp_environment():
    """
    ERP integration suites require real DB connectivity and explicit write enablement.

    When these flags are not enabled, skip the entire ERP integration module to
    avoid deterministic false-negative failures in local/mock environments.
    """
    use_real = os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() == "true"
    writes_enabled = os.getenv("ENABLE_TEST_WRITES", "false").lower() == "true"
    if not (use_real and writes_enabled):
        pytest.skip(
            "ERP integration tests require USE_REAL_DB_FOR_TESTS=true and ENABLE_TEST_WRITES=true"
        )


ERP_SESSION_ASSET_KEYS = [
    # Pre-staging assets (Layer 0)
    "erp/pre_stg_fakturalinje_mysupply",
    "erp/pre_stg_postering_sydax",
    "erp/pre_stg_vareaftaler_ils",
    "erp/pre_stg_bridge_vare_ils",
    # Dimension staging assets (Layer 1)
    "erp/stg_dim_kreditor_ils",
    "erp/stg_dim_kreditor_mysupply",
    "erp/stg_dim_kreditor_sydax",
    "erp/stg_dim_vare_ils",
    "erp/stg_dim_vare_mysupply",
    "erp/stg_dim_vare",
    "erp/stg_dim_kontoplan_sydax",
    "erp/stg_dim_postering_sydax",
    "erp/stg_dim_faktura_mysupply",
    "erp/stg_dim_fakturalinje_mysupply",
    "erp/stg_dim_aftale_ils",
    # Fact staging assets (Layer 1)
    "erp/stg_fct_postering_sydax",
    "erp/stg_fct_fakturalinje_mysupply",
    "erp/stg_fct_faktura_mysupply",
    # Map assets
    "map_kreditor",
    "map_kontoplan",
    "map_vare",
    "map_aftale",
    "map_faktura",
    "map_fakturalinje",
    "map_postering",
    # Dimension assets
    "dim_kreditor",
    "dim_kontoplan",
    "dim_vare",
    "dim_aftale",
    "dim_faktura",
    "dim_fakturalinje",
    "dim_postering",
    # Fact assets
    "fct_faktura",
    "fct_fakturalinje",
    "fct_postering",
    # Bridge + apply-pass
    "ref_faktura_to_postering",
    "fct_apply_faktura_postering_relations",
]


@pytest.fixture(autouse=True)
def fast_sydax_sampling():
    if (
        os.getenv("ERP_STAGING_FULL_LOAD", "false").lower() == "true"
        or os.getenv("STAGING_FULL_LOAD", "false").lower() == "true"
    ):
        yield
        return

    import DSI.mssql as erp_staging

    key_columns = {
        ("Kreditor", "Kreditor"): "KreditorId",
        ("Finans", "Finanskonto"): "FinanskontoId",
        ("Finans", "Afdeling"): "Afdelingsnummer",
        ("Finans", "Finanspostering"): "FinansposteringId",
        ("Finans", "FinansposteringKreditorTransRelation"): "FinansposteringId",
        ("Kreditor", "Kreditortransaktion"): "KreditortransaktionId",
    }

    original_configs = erp_staging.SYDAX_TABLE_CONFIGS
    patched_configs = []
    for config in original_configs:
        key_col = key_columns.get((config.source_schema, config.source_table))
        if key_col:
            sample_clause = (
                f"{key_col} IN (SELECT TOP 10 {key_col} "
                f"FROM [{config.source_schema}].[{config.source_table}])"
            )
            if config.custom_where_clause:
                new_clause = f"({config.custom_where_clause}) AND ({sample_clause})"
            else:
                new_clause = sample_clause
            patched_configs.append(replace(config, custom_where_clause=new_clause))
        else:
            patched_configs.append(config)

    erp_staging.SYDAX_TABLE_CONFIGS = tuple(patched_configs)
    try:
        yield
    finally:
        erp_staging.SYDAX_TABLE_CONFIGS = original_configs


def _should_run_cleanup() -> bool:
    """Check if cleanup should run based on environment flags."""
    use_real = os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() == "true"
    writes_enabled = os.getenv("ENABLE_TEST_WRITES", "false").lower() == "true"
    return use_real and writes_enabled


_staging_cleanup_done_modules: set[str] = set()


@pytest.fixture
def cleanup_erp_staging_tables(request):
    """
    Clean only staging tables before test (stg.* tables).

    Usage:
        def test_staging_asset(cleanup_erp_staging_tables, ...):
            ...
    """
    if not _should_run_cleanup():
        logger.info("🧹 ERP staging cleanup skipped: real resources not enabled")
        yield
        return

    module_key = str(getattr(request.node, "fspath", ""))
    if module_key in _staging_cleanup_done_modules:
        yield
        return

    try:
        target_resource = request.getfixturevalue("real_mssql_target_resource")
    except pytest.skip.Exception:
        logger.info("🧹 ERP staging cleanup skipped: target resource unavailable")
        yield
        return

    # Import here to avoid circular imports
    from tests.fixtures.resources import _cleanup_erp_staging_tables

    logger.info("🧹 Cleaning erp staging tables before test...")
    _cleanup_erp_staging_tables(target_resource)
    logger.info("✅ ERP staging tables cleaned")
    if module_key:
        _staging_cleanup_done_modules.add(module_key)

    yield

    # No cleanup after - leave data for next ordered test


@pytest.fixture
def cleanup_erp_map_tables(request):
    """
    Clean only map tables before test (map.* tables, preserves staging).

    Usage:
        def test_map_asset(cleanup_erp_map_tables, ...):
            ...
    """
    if not _should_run_cleanup():
        logger.info("🧹 ERP map cleanup skipped: real resources not enabled")
        yield
        return

    try:
        target_resource = request.getfixturevalue("real_mssql_target_resource")
    except pytest.skip.Exception:
        logger.info("🧹 ERP map cleanup skipped: target resource unavailable")
        yield
        return

    from tests.fixtures.resources import _cleanup_erp_map_tables

    logger.info("🧹 Cleaning erp map tables before test...")
    _cleanup_erp_map_tables(target_resource)
    logger.info("✅ ERP map tables cleaned")

    yield

    # No cleanup after - leave data for next ordered test


@pytest.fixture
def cleanup_erp_dim_tables(request):
    """
    Clean only dim tables before test (dim.* tables, preserves map and staging).

    Usage:
        def test_dim_asset(cleanup_erp_dim_tables, ...):
            ...
    """
    if not _should_run_cleanup():
        logger.info("🧹 ERP dim cleanup skipped: real resources not enabled")
        yield
        return

    try:
        target_resource = request.getfixturevalue("real_mssql_target_resource")
    except pytest.skip.Exception:
        logger.info("🧹 ERP dim cleanup skipped: target resource unavailable")
        yield
        return

    from tests.fixtures.resources import _cleanup_erp_dim_tables

    logger.info("🧹 Cleaning erp dim tables before test...")
    _cleanup_erp_dim_tables(target_resource)
    logger.info("✅ ERP dim tables cleaned")

    yield

    # No cleanup after - leave data for next ordered test


@pytest.fixture
def cleanup_erp_fact_tables(request):
    """
    Clean only fact tables before test (fct.* tables, preserves dim, map, and staging).

    Usage:
        def test_fact_asset(cleanup_erp_fact_tables, ...):
            ...
    """
    if not _should_run_cleanup():
        logger.info("🧹 ERP fact cleanup skipped: real resources not enabled")
        yield
        return

    try:
        target_resource = request.getfixturevalue("real_mssql_target_resource")
    except pytest.skip.Exception:
        logger.info("🧹 ERP fact cleanup skipped: target resource unavailable")
        yield
        return

    from tests.fixtures.resources import _cleanup_erp_fact_tables

    logger.info("🧹 Cleaning erp fact tables before test...")
    _cleanup_erp_fact_tables(target_resource)
    logger.info("✅ ERP fact tables cleaned")

    yield

    # No cleanup after - leave data for next ordered test


@pytest.fixture
def cleanup_erp_all_tables(request):
    """
    Clean all erp tables (fact + dim + map + staging) before test.

    Use this for full reset scenarios or full flow tests.

    Usage:
        def test_full_flow(cleanup_erp_all_tables, ...):
            ...
    """
    if not _should_run_cleanup():
        logger.info("🧹 ERP full cleanup skipped: real resources not enabled")
        yield
        return

    try:
        target_resource = request.getfixturevalue("real_mssql_target_resource")
    except pytest.skip.Exception:
        logger.info("🧹 ERP full cleanup skipped: target resource unavailable")
        yield
        return

    from tests.fixtures.resources import _cleanup_erp_tables

    logger.info("🧹 Cleaning all erp tables before test...")
    _cleanup_erp_tables(target_resource)
    logger.info("✅ All erp tables cleaned")

    yield

    # No cleanup after


# Global flag to ensure infrastructure is set up only once
_infrastructure_setup_complete = False

_session_materialization_complete = False


@pytest.fixture
def erp_materialized_session(
    dagster_instance,
    real_mysupply_mssql_resource,
    real_sydax_mssql_resource,
    real_ils_mssql_resource,
    real_mssql_target_resource,
    setup_erp_infrastructure,
):
    """
    Materialize the full erp pipeline once per test session.

    Tests should request this fixture and then perform read-only verification queries.
    """
    use_real = os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() == "true"
    writes_enabled = os.getenv("ENABLE_TEST_WRITES", "false").lower() == "true"

    if not (use_real and writes_enabled):
        logger.info("🏗️ ERP session materialization skipped: real resources not enabled")
        yield {
            "instance": dagster_instance,
            "target_resource": real_mssql_target_resource,
        }
        return

    global _session_materialization_complete

    if not _session_materialization_complete:
        logger.info("🏗️ Materializing full erp pipeline for test session...")
        from tests.fixtures.resources import (
            _cleanup_erp_staging_tables,
            _cleanup_erp_tables,
        )

        _cleanup_erp_staging_tables(real_mssql_target_resource)
        _cleanup_erp_tables(real_mssql_target_resource)

        resources = {
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
            "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
            "ils_mssql": ResourceDefinition.hardcoded_resource(real_ils_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        }

        daily_partition = os.getenv("ERP_TEST_DAILY_PARTITION", date.today().isoformat())
        monthly_partition = os.getenv("ERP_TEST_MONTHLY_PARTITION", daily_partition[:7])

        # Phase 1: full-load staging assets (unpartitioned).
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=ERP_STAGING_FULLLOAD_DRIVER_KEYS,
            resources=resources,
        )

        if result is None or not result.success:
            raise RuntimeError("ERP session full-load staging materialization failed")

        # Phase 2: daily partitioned staging assets.
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=ERP_STAGING_DAILY_DRIVER_KEYS,
            partition=daily_partition,
            resources=resources,
        )

        if result is None or not result.success:
            raise RuntimeError("ERP session daily staging materialization failed")

        # Phase 3: monthly partitioned staging assets.
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=ERP_STAGING_MONTHLY_DRIVER_KEYS,
            partition=monthly_partition,
            resources=resources,
        )

        if result is None or not result.success:
            raise RuntimeError("ERP session monthly staging materialization failed")

        # Phase 4: downstream pre_stg/map/dim/fct assets.
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=ERP_SESSION_ASSET_KEYS,
            resources=resources,
        )

        if result is None or not result.success:
            raise RuntimeError("ERP session downstream materialization failed")

        logger.info("✅ ERP session materialization complete")
        _session_materialization_complete = True

    yield {
        "instance": dagster_instance,
        "target_resource": real_mssql_target_resource,
    }


@pytest.fixture
def setup_erp_infrastructure(dagster_instance, real_mssql_target_resource):
    """
    One-time setup of erp infrastructure (tables, schemas, functions).

    Runs once per test session before any tests execute.
    Creates all required schemas, helper functions, and tables for map/dim/fact layers.
    """
    use_real = os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() == "true"
    writes_enabled = os.getenv("ENABLE_TEST_WRITES", "false").lower() == "true"

    global _infrastructure_setup_complete

    if not (use_real and writes_enabled):
        logger.info("🏗️ ERP infrastructure setup skipped: real resources not enabled")
        return

    if _infrastructure_setup_complete:
        logger.info("🏗️ ERP infrastructure already set up, skipping...")
        return

    logger.info("🏗️ Setting up erp infrastructure (schemas, tables, functions)...")

    try:
        # Ensure stale test data does not block infra creation
        from tests.fixtures.resources import _cleanup_erp_tables

        _cleanup_erp_tables(real_mssql_target_resource)

        # Materialize centralized infrastructure assets
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=[
                "staging_infra_schemas",
                "staging_infra_erp",
                "staging_infra_ref",
                "staging_infra_organisation",
            ],
            resources={
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
        )
        if result is None or not result.success:
            raise RuntimeError("ERP infrastructure materialization failed")

        _create_allowance_charge_stubs(real_mssql_target_resource)

        logger.info("✅ ERP infrastructure setup complete")
        _infrastructure_setup_complete = True

    except Exception as e:
        logger.error(f"❌ ERP infrastructure setup failed: {e}")
        raise


def _create_allowance_charge_stubs(target_resource):
    """Create empty ext tables for allowance_charge if not present.

    The source mysupply database may not have these tables yet; the downstream
    staging SQL uses LEFT JOINs so empty stubs are sufficient.
    """
    stubs = {
        "ext.statistic_invoice_header_allowance_charge_mysupply": """
            IF OBJECT_ID(N'[ext].[statistic_invoice_header_allowance_charge_mysupply]', 'U') IS NULL
            BEGIN
                CREATE TABLE [ext].[statistic_invoice_header_allowance_charge_mysupply](
                    id                              BIGINT NULL,
                    statistic_invoice_header_id     BIGINT NULL,
                    GebyrKategori                   NVARCHAR(200) NULL,
                    GebyrKategoriIndikering         NVARCHAR(200) NULL,
                    [GebyrKategoriÅrsag]            NVARCHAR(200) NULL,
                    GebyrKategoriKvantitet           DECIMAL(18,4) NULL,
                    GebyrKategoriKvantitet_Type      NVARCHAR(200) NULL,
                    GebyrKategoriKvantitet_List      NVARCHAR(200) NULL,
                    GebyrKategoriTotal               DECIMAL(18,4) NULL,
                    GebyrKategoriFinanskonto         NVARCHAR(200) NULL,
                    DW_LoadTimestamp                  DATETIME2(0) NULL
                )
            END
        """,
        "ext.statistic_invoice_line_allowance_charge_mysupply": """
            IF OBJECT_ID(N'[ext].[statistic_invoice_line_allowance_charge_mysupply]', 'U') IS NULL
            BEGIN
                CREATE TABLE [ext].[statistic_invoice_line_allowance_charge_mysupply](
                    id                                          BIGINT NULL,
                    statistic_invoice_line_id                   BIGINT NULL,
                    FakturaUdstederGebyrKategori                NVARCHAR(200) NULL,
                    FakturaUdstederGebyrKategoriIndikering      NVARCHAR(200) NULL,
                    [FakturaUdstederGebyrKategoriÅrsag]         NVARCHAR(200) NULL,
                    FakturaUdstederGebyrKategoriKvantitet        DECIMAL(18,4) NULL,
                    FakturaUdstederGebyrKategoriKvantitet_Type   NVARCHAR(200) NULL,
                    FakturaUdstederGebyrKategoriKvantitet_List   NVARCHAR(200) NULL,
                    FakturaUdstederGebyrKategoriTotal             DECIMAL(18,4) NULL,
                    FakturaUdstederGebyrKategoriFinanskonto       NVARCHAR(200) NULL,
                    DW_LoadTimestamp                              DATETIME2(0) NULL
                )
            END
        """,
    }
    with target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for table_name, ddl in stubs.items():
                cursor.execute(ddl)
                logger.info(f"Ensured stub: {table_name}")
            conn.commit()
        finally:
            cursor.close()

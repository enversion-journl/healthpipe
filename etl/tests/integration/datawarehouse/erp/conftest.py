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

import pytest
from dagster import ResourceDefinition
from tests.utils.materialize_helpers import materialize_assets_with_dependencies

logger = logging.getLogger(__name__)


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
    "erp/pre_stg_Fakturalinje_mysupply",
    "erp/pre_stg_Postering_sydax",
    "erp/pre_stg_Vareaftaler_ils",
    "erp/pre_stg_Leverandoervare_ILSVare_delta_entiteter",
    # Dimension staging assets (Layer 1)
    "erp/stg_dim_Kreditor",
    "erp/stg_dim_Leverandoervare",
    "erp/stg_dim_ILSVare",
    "erp/stg_bridge_Leverandoervare_ILSVare",
    "erp/stg_dim_UNSPSC",
    "erp/stg_dim_Kontoplan_sydax",
    "erp/stg_dim_Postering_sydax",
    "erp/stg_dim_Faktura_mysupply",
    "erp/stg_dim_Fakturalinje_mysupply",
    "erp/stg_dim_Aftale_ils",
    # Fact staging assets (Layer 1)
    "erp/stg_fct_Postering_sydax",
    "erp/stg_fct_Fakturalinje_mysupply",
    "erp/stg_fct_Faktura_mysupply",
    # Map assets
    "map_Kreditor",
    "map_Kontoplan",
    "map_Leverandoervare",
    "map_ILSVare",
    "map_UNSPSC",
    "map_Aftale",
    "map_Faktura",
    "map_Fakturalinje",
    "map_Postering",
    # Dimension assets
    "dim_Kreditor",
    "dim_Kontoplan",
    "dim_Leverandoervare",
    "dim_ILSVare",
    "dim_Aftale",
    "dim_Faktura",
    "dim_Fakturalinje",
    "dim_Postering",
    # Fact assets
    "fct_Faktura",
    "fct_Fakturalinje",
    "fct_Postering",
    # Bridge + apply-pass
    "ref_faktura_to_postering",
    "fct_apply_faktura_postering_relations",
    "bridge_LeverandoervareILSVare",
]


@pytest.fixture(autouse=True)
def fast_sydax_sampling():
    # The legacy SydAX extract-config sampling (TOP-10 row patching) was removed
    # together with the partitioned ERP extract pipeline. DFM archive extraction
    # manages its own source access, so this autouse fixture is now a no-op.
    yield


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

        # Phase 1: DFM archive — populate ext.* and arc.* for all ERP source
        # systems. The DFM archive (non-partitioned, checkpoint-based) replaced
        # the legacy partitioned extract pipeline, so the archive groups are
        # driven directly here (resolved from the definitions so this stays in
        # sync as archive tables are added/removed).
        # TODO(devcontainer): validate this DFM-archive-driven session build
        # end-to-end against a real DB — the legacy full-load/daily/monthly
        # extract phases it replaced could not be exercised here.
        from definitions import defs as _defs

        archive_keys = sorted(
            spec.key.to_user_string()
            for spec in _defs.resolve_all_asset_specs()
            if spec.group_name in ("ils_archive", "sydax_archive", "mysupply_archive")
        )
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=archive_keys,
            resources=resources,
        )

        if result is None or not result.success:
            raise RuntimeError("ERP session DFM archive materialization failed")

        # Phase 2: downstream pre_stg/map/dim/fct assets.
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
                "staging_infra_organisation",
                "staging_infra_ref",
                "staging_infra_udbudskategori",
                "staging_infra_udbudskategori_seed",
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
    """Create empty arc tables for allowance_charge if not present.

    NOTE(#28693): Repointed from ext.* to arc.* after DFM migration — the production
    staging CTEs (HAC_SUM_CTE_SQL / HAC_DIM_CTE_SQL in stg_fct/stg_dim_Faktura_mysupply)
    now read from arc.[mysupply_statistic_invoice_header/line_allowance_charge].
    Empty stubs are sufficient because staging uses LEFT JOINs.
    TODO(#28693): validate in devcontainer that the arc schema is created by
    staging_infra_erp before these stubs are needed.
    """
    stubs = {
        "arc.mysupply_statistic_invoice_header_allowance_charge": """
            IF OBJECT_ID(N'[arc].[mysupply_statistic_invoice_header_allowance_charge]', 'U') IS NULL
            BEGIN
                CREATE TABLE [arc].[mysupply_statistic_invoice_header_allowance_charge](
                    id                              BIGINT NOT NULL,
                    statistic_invoice_header_id     BIGINT NOT NULL,
                    GebyrKategori                   NVARCHAR(200) NULL,
                    GebyrKategoriIndikering         NVARCHAR(200) NULL,
                    [GebyrKategoriÅrsag]            NVARCHAR(200) NULL,
                    GebyrKategoriKvantitet           NVARCHAR(200) NULL,
                    GebyrKategoriKvantitet_Type      NVARCHAR(200) NULL,
                    GebyrKategoriKvantitet_List      NVARCHAR(200) NULL,
                    GebyrKategoriTotal               NVARCHAR(200) NULL,
                    GebyrKategoriFinanskonto         NVARCHAR(200) NULL,
                    dw_timestamp_insert              DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
                    dw_timestamp_update              DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
                )
            END
        """,
        "arc.mysupply_statistic_invoice_line_allowance_charge": """
            IF OBJECT_ID(N'[arc].[mysupply_statistic_invoice_line_allowance_charge]', 'U') IS NULL
            BEGIN
                CREATE TABLE [arc].[mysupply_statistic_invoice_line_allowance_charge](
                    id                                          BIGINT NOT NULL,
                    statistic_invoice_line_id                   BIGINT NOT NULL,
                    FakturaUdstederGebyrKategori                NVARCHAR(200) NULL,
                    FakturaUdstederGebyrKategoriIndikering      NVARCHAR(200) NULL,
                    [FakturaUdstederGebyrKategoriÅrsag]         NVARCHAR(200) NULL,
                    FakturaUdstederGebyrKategoriKvantitet        NVARCHAR(200) NULL,
                    FakturaUdstederGebyrKategoriKvantitet_Type   NVARCHAR(200) NULL,
                    FakturaUdstederGebyrKategoriKvantitet_List   NVARCHAR(200) NULL,
                    FakturaUdstederGebyrKategoriTotal             NVARCHAR(200) NULL,
                    FakturaUdstederGebyrKategoriFinanskonto       NVARCHAR(200) NULL,
                    dw_timestamp_insert              DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
                    dw_timestamp_update              DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
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

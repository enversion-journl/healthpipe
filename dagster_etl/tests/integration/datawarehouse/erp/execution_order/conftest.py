# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fixtures for the ERP execution-order test suite.

Provides:
- DW truncation per scenario (reuses ``setup_erp_infrastructure`` from parent)
- Source-DB seed / teardown lifecycle
- ``PipelineRunner`` wired with real resources
- Override of ``fast_sydax_sampling`` (parent autouse fixture would exclude
  seeded rows via TOP-N sampling)

All tests require ``USE_REAL_DB_FOR_TESTS=true`` and ``ENABLE_TEST_WRITES=true``
(enforced by the parent conftest's autouse ``require_real_erp_environment``).
"""

from __future__ import annotations

import logging
import re

import pytest
from dagster import ResourceDefinition
from tests.integration.datawarehouse.erp.execution_order.dw_ek_assert import (
    truncate_all_dw_tables,
)
from tests.integration.datawarehouse.erp.execution_order.runner import (
    PipelineRunner,
)
from tests.integration.datawarehouse.erp.execution_order.seeds import (
    teardown_mysupply,
    teardown_sydax,
)

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.erp,
    pytest.mark.real_resources,
]


# ── Override fast_sydax_sampling ──────────────────────────────────────
# The parent erp conftest applies an autouse ``fast_sydax_sampling``
# fixture that patches SydAx extraction configs with ``TOP 10`` sampling.
# This would exclude our seeded rows.  We override it at this package
# level to be a no-op.


@pytest.fixture(autouse=True)
def fast_sydax_sampling():
    """Disable TOP-N sampling so seeded rows are extracted."""
    yield


# ── Migrations ────────────────────────────────────────────────────────
# setup_erp_infrastructure creates schemas/tables/functions but does not run
# the ddl/migrations step — without it this branch's 20260611_* migrations
# (Gebyr split, zero-rated move, Postering descriptor cleanup) are missing
# and the staging/dim SQL references columns that do not exist yet.
#
# The full staging_migrations asset cannot replay on the devcontainer DW —
# older unapplied migrations (e.g. 20260604_conceptmap_move_from_ref_schema)
# assume a table state the local DW never had. We therefore execute only this
# branch's migration files directly; they are all guarded with IF EXISTS /
# IF NOT EXISTS so re-running is a no-op.

_BRANCH_MIGRATIONS = (
    "20260611_dim_gebyr_rabat_tillaeg_split.sql",
    "20260611_faktura_fakturalinje_add_gebyr_measures.sql",
    "20260611_fakturalinje_move_zerorated_to_faktura.sql",
    "20260611_fakturalinje_remove_header_measures.sql",
    "20260611_postering_descriptor_cleanup.sql",
)


@pytest.fixture(autouse=True)
def apply_branch_migrations(real_mssql_target_resource, setup_erp_infrastructure):
    """Apply this branch's 20260611_* DDL migrations to the devcontainer DW."""
    from pathlib import Path

    migrations_dir = Path(__file__).resolve().parents[5] / "infrastructure" / "ddl" / "migrations"
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for filename in _BRANCH_MIGRATIONS:
                sql = (migrations_dir / filename).read_text(encoding="utf-8")
                for batch in re.split(r"^\s*GO\s*$", sql, flags=re.MULTILINE):
                    if batch.strip():
                        cursor.execute(batch)
            conn.commit()
        finally:
            cursor.close()
    yield


# ── DW cleanup ────────────────────────────────────────────────────────


@pytest.fixture
def clean_dw(real_mssql_target_resource, setup_erp_infrastructure):
    """Truncate every DW table used by ERP before the test."""
    with real_mssql_target_resource.get_connection() as conn:
        truncate_all_dw_tables(conn)
    yield
    with real_mssql_target_resource.get_connection() as conn:
        truncate_all_dw_tables(conn)


# ── Budget source stubs ──────────────────────────────────────────────
# The local devcontainer sydax database predates the budget model — the
# sydax_enriched_budget extractor reads [Finans].[Budgetpost] / [Budgetmodel]
# / [BudgetpostStatus], which the init image does not ship. Empty stubs with
# the columns the extractor SELECTs are sufficient (all joins are LEFT).

_BUDGET_SOURCE_STUBS = {
    "Finans.Budgetpost": """
        IF OBJECT_ID(N'[Finans].[Budgetpost]', 'U') IS NULL
        CREATE TABLE [Finans].[Budgetpost](
            [ref_LedgerbudgetId]        BIGINT NULL,
            [BudgetmodelId]             BIGINT NULL,
            [BudgetpostStatusId]        BIGINT NULL,
            [FinanskontoId]             BIGINT NULL,
            [Budgetdato]                NVARCHAR(50) NULL,
            [BudgetAdoptedDate]         NVARCHAR(50) NULL,
            [Oprettelsestidspunkt]      DATETIME2(0) NULL,
            [SidstAendretTidspunkt]     DATETIME2(0) NULL,
            [Budgetbeloeb]              DECIMAL(30,12) NULL,
            [BudgetbeloebValuta]        DECIMAL(30,12) NULL,
            [Valutakode]                NVARCHAR(10) NULL,
            [DebetKredit]               NVARCHAR(10) NULL,
            [Budgetkode]                NVARCHAR(50) NULL,
            [Ændringstype]              NVARCHAR(100) NULL,
            [ÆndringstypeBeskrivelse]   NVARCHAR(255) NULL,
            [BeloebstypeId]             BIGINT NULL,
            [Kildesystem]               NVARCHAR(50) NULL
        )
    """,
    "Finans.Budgetmodel": """
        IF OBJECT_ID(N'[Finans].[Budgetmodel]', 'U') IS NULL
        CREATE TABLE [Finans].[Budgetmodel](
            [BudgetmodelId]     BIGINT NULL,
            [ref_BudgetmodelId] BIGINT NULL,
            [Budgetmodelnummer] NVARCHAR(100) NULL,
            [Budgetmodeltekst]  NVARCHAR(255) NULL,
            [Budgetmodeltype]   NVARCHAR(100) NULL,
            [ErLukket]          BIT NULL,
            [Kildesystem]       NVARCHAR(50) NULL
        )
    """,
    "Finans.BudgetpostStatus": """
        IF OBJECT_ID(N'[Finans].[BudgetpostStatus]', 'U') IS NULL
        CREATE TABLE [Finans].[BudgetpostStatus](
            [BudgetpostStatusId]    BIGINT NULL,
            [BudgetpostStatustekst] NVARCHAR(255) NULL
        )
    """,
    "Finans.BudgetbeløbType": """
        IF OBJECT_ID(N'[Finans].[BudgetbeløbType]', 'U') IS NULL
        CREATE TABLE [Finans].[BudgetbeløbType](
            [BudgetbeløbTypeId]    BIGINT NULL,
            [BudgetbeløbTypetekst] NVARCHAR(1000) NULL
        )
    """,
}


@pytest.fixture(autouse=True)
def ensure_budget_source_stubs(real_sydax_mssql_resource):
    """Create empty budget source tables if the local sydax lacks them."""
    with real_sydax_mssql_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for _name, ddl in _BUDGET_SOURCE_STUBS.items():
                cursor.execute(ddl)
            conn.commit()
        finally:
            cursor.close()
    yield


# ── Allowance-charge ext schema repair ───────────────────────────────
# The parent conftest creates ext allowance stubs only IF the table is
# missing. A previous extraction against the local (stale-schema) source may
# have left ext tables with ChargeIndicator/Amount columns instead of the
# GebyrKategori* columns the staging SQL reads — recreate them to the
# expected shape.

_ALLOWANCE_EXT_DDL = {
    "statistic_invoice_header_allowance_charge_mysupply": """
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
    """,
    "statistic_invoice_line_allowance_charge_mysupply": """
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
    """,
}


@pytest.fixture(autouse=True)
def ensure_allowance_ext_schema(real_mssql_target_resource, setup_erp_infrastructure):
    """Recreate ext allowance stubs if they lack the GebyrKategori* columns."""
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for table, ddl in _ALLOWANCE_EXT_DDL.items():
                cursor.execute(
                    f"""
                    IF OBJECT_ID(N'[ext].[{table}]', 'U') IS NOT NULL
                       AND COL_LENGTH('ext.{table}', 'GebyrKategoriTotal') IS NULL
                       AND COL_LENGTH('ext.{table}', 'FakturaUdstederGebyrKategoriTotal') IS NULL
                        DROP TABLE [ext].[{table}]
                """
                )
                cursor.execute(f"IF OBJECT_ID(N'[ext].[{table}]', 'U') IS NULL {ddl}")
            conn.commit()
        finally:
            cursor.close()
    yield


# ── Source teardown ───────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def cleanup_source_dbs(
    real_mysupply_mssql_resource,
    real_sydax_mssql_resource,
    ensure_budget_source_stubs,
):
    """Clean test-namespace rows from source DBs before and after each test."""
    with real_mysupply_mssql_resource.get_connection() as conn:
        teardown_mysupply(conn)
    with real_sydax_mssql_resource.get_connection() as conn:
        teardown_sydax(conn)

    yield

    with real_mysupply_mssql_resource.get_connection() as conn:
        teardown_mysupply(conn)
    with real_sydax_mssql_resource.get_connection() as conn:
        teardown_sydax(conn)


# ── Pipeline runner ───────────────────────────────────────────────────


@pytest.fixture
def pipeline_runner(
    dagster_instance,
    real_mysupply_mssql_resource,
    real_sydax_mssql_resource,
    real_mssql_target_resource,
    setup_erp_infrastructure,
) -> PipelineRunner:
    """PipelineRunner wired with real resources."""
    resources = {
        "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
        "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
        "mssql_target_staging": ResourceDefinition.hardcoded_resource(real_mssql_target_resource),
    }
    return PipelineRunner(instance=dagster_instance, resources=resources)


# ── Source connection fixtures ────────────────────────────────────────


@pytest.fixture
def mysupply_conn(real_mysupply_mssql_resource):
    """Yield a live connection to the MySupply source DB."""
    with real_mysupply_mssql_resource.get_connection() as conn:
        yield conn


@pytest.fixture
def sydax_conn(real_sydax_mssql_resource):
    """Yield a live connection to the SydAx source DB."""
    with real_sydax_mssql_resource.get_connection() as conn:
        yield conn


@pytest.fixture
def target_conn(real_mssql_target_resource):
    """Yield a live connection to the target DW."""
    with real_mssql_target_resource.get_connection() as conn:
        yield conn

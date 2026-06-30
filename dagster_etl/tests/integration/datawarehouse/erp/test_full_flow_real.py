# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import pytest
from dagster import Definitions, FilesystemIOManager, load_assets_from_modules
from DSA.SQL import dim as dsa_dim
from DSA.SQL import fct as dsa_fct
from DSA.SQL import map as dsa_map
from DSA.SQL import staging as dsa_staging
from DSI.mssql import all_extract_assets as erp_extract_assets
from jobs.erp_jobs import erp_daily_job

pytestmark = pytest.mark.skip(
    reason="Full-flow real-db test is too environment-specific for standard integration runs"
)


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(5)
def test_erp_full_flow_real(
    dagster_instance,
    cleanup_erp_all_tables,
    real_mysupply_mssql_resource,
    real_sydax_mssql_resource,
    real_ils_mssql_resource,
    real_mssql_target_resource,
    tmp_path,
):
    """
    Execute the full ERP ETL flow against real resources.

    The cleanup_erp_all_tables fixture clears all erp tables before the run to
    ensure deterministic results. We assert that the run succeeds and that the
    updated map_kreditor materialization records all upstream source tables.
    """
    definitions = Definitions(
        assets=[
            *load_assets_from_modules([dsa_staging, dsa_dim, dsa_map, dsa_fct]),
            *erp_extract_assets,
        ],
        jobs=[erp_daily_job],
        resources={
            "mysupply_mssql": real_mysupply_mssql_resource,
            "sydax_mssql": real_sydax_mssql_resource,
            "ils_mssql": real_ils_mssql_resource,
            "mssql_target_staging": real_mssql_target_resource,
            "io_manager": FilesystemIOManager(base_dir=str(tmp_path / "storage")),
        },
    )

    result = definitions.get_job_def("erp_daily_etl").execute_in_process(
        instance=dagster_instance,
    )

    assert result.success, "ERP daily ETL job failed"

    materializations = result.asset_materializations_for_node("map_kreditor")
    assert materializations, "No materialization emitted for map_kreditor"
    latest_event = materializations[-1]
    metadata = latest_event.metadata
    source_tables = metadata["source_tables"].value

    assert "stg.dim_kreditor_sydax" in source_tables
    assert "stg.dim_kreditor_ils" in source_tables
    assert "stg.dim_kreditor_mysupply" in source_tables

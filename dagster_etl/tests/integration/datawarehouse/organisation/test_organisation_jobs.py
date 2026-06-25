# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for organization job definitions.

Tests that Dagster can resolve the complete job graphs and that
execute_in_process succeeds when real resources are available.

Test levels:
- Graph resolution (unit-level): Verifies asset selection, node resolution, and
  dependency wiring — no database required.
- execute_in_process (real_resources): Full materialization against staging DB.
"""

import os
from unittest.mock import MagicMock

import dagster as dg
import pytest
from DSA.CodeSystem import cs_okosted
from DSA.ConceptMap import cm_okosted_to_sor
from DSA.FHIR.Organization.Organization_SOR_IFDB import fhir_organization_sor
from DSA.FHIR.SqlOnFhir.Organization import sof_organization
from DSA.SQL.dim import dim_organisation
from DSA.SQL.ref import ref_organisation
from DSA.SQL.staging import (
    stg_organisation_sor,
)
from DSO.load.sync_dim import (
    dso_sync_dim_organisation,
)
from DSO.load.sync_fhir import dso_sync_fhir_organization
from infrastructure.assets.dso_infrastructure import dso_infra_organisation
from infrastructure.assets.fhir_sof_infrastructure import fhir_sof_infrastructure
from infrastructure.assets.staging_infrastructure import (
    staging_infra_organisation,
    staging_infra_schemas,
)
from jobs.organization_jobs import organisation_cs_job

pytestmark = pytest.mark.skip(
    reason="Organization job graph still depends on missing ext_os2mo_* extract assets"
)

ORGANISATION_CS_JOB_ASSETS = [
    stg_organisation_sor,
    cs_okosted,
    cm_okosted_to_sor,
    dso_sync_dim_organisation,
    staging_infra_schemas,
    staging_infra_organisation,
    dso_infra_organisation,
    ref_organisation,
    dim_organisation,
    fhir_organization_sor,
    fhir_sof_infrastructure,
    sof_organization,
    dso_sync_fhir_organization,
]


def _mock_resources_for_assets(assets: list[dg.AssetsDefinition]) -> dict:
    """Build a dict of MagicMock resources satisfying all asset requirements."""
    resource_keys = set()
    for asset in assets:
        if isinstance(asset, dg.AssetsDefinition):
            for req in asset.get_resource_requirements():
                resource_keys.add(req.key)
    mocks = {key: MagicMock() for key in resource_keys if key != "io_manager"}
    mocks["io_manager"] = dg.FilesystemIOManager(base_dir="/tmp/dagster_test_org")
    return mocks


# =============================================================================
# Graph Resolution Tests (no DB required)
# =============================================================================


class TestOrganisationCsJobGraphResolution:
    """Verify organisation_cs_job resolves all expected nodes."""

    @pytest.fixture()
    def resolved_job(self):
        test_defs = dg.Definitions(
            assets=ORGANISATION_CS_JOB_ASSETS,
            jobs=[organisation_cs_job],
            resources=_mock_resources_for_assets(ORGANISATION_CS_JOB_ASSETS),
        )
        return test_defs.resolve_job_def("organisation_cs_job")

    def test_job_resolves(self, resolved_job):
        assert resolved_job is not None
        assert resolved_job.name == "organisation_cs_job"

    def test_expected_node_count(self, resolved_job):
        node_names = {node.name for node in resolved_job.nodes}
        assert len(node_names) == 11, f"Expected 11 nodes, got {len(node_names)}: {node_names}"

    @pytest.mark.parametrize(
        "asset_name",
        [
            "cs_okosted",
            "cm_okosted_to_sor",
            "stg_organisation_sor",
            "staging_infra_organisation",
            "ref_organisation",
            "dim_organisation",
            "dso_sync_dim_organisation",
            "fhir_organization_sor",
            "fhir_sof_infrastructure",
            "sof_organization",
            "dso_sync_fhir_organization",
        ],
    )
    def test_asset_in_job(self, resolved_job, asset_name):
        node_names = {node.name for node in resolved_job.nodes}
        assert asset_name in node_names, f"{asset_name} not found in job graph"

    def test_staging_assets_imported_from_dsa(self):
        """Staging assets must live in DSA.SQL, not DSI.stg."""
        for asset in [stg_organisation_sor]:
            fn_module = asset.op.compute_fn.decorated_fn.__module__
            assert (
                "DSA.SQL.staging" in fn_module
            ), f"{asset.key} function module is {fn_module}, expected DSA.SQL.staging"


# =============================================================================
# execute_in_process Tests (requires real resources)
# =============================================================================


def _should_run_real():
    return (
        os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() == "true"
        and os.getenv("ENABLE_TEST_WRITES", "false").lower() == "true"
    )


def _db_is_reachable(resource) -> bool:
    """Quick connectivity check — returns False if the database is unreachable."""
    import socket

    try:
        sock = socket.create_connection((resource.host, resource.port), timeout=3)
        sock.close()
        return True
    except (TimeoutError, OSError, ConnectionRefusedError):
        return False


@pytest.mark.integration
@pytest.mark.real_resources
@pytest.mark.organization_test
class TestOrganisationCsJobExecution:
    """Run organisation_cs_job.execute_in_process against real databases.

    Requires: USE_REAL_DB_FOR_TESTS=true and ENABLE_TEST_WRITES=true
    """

    def test_organisation_cs_job_execute_in_process(
        self,
        dagster_instance,
        real_mssql_target_resource,
        real_sydax_mssql_resource,
        real_fhir_server_resource,
        real_fhir_database_resource,
        tmp_path,
    ):
        """Full organization pipeline: staging -> FHIR/SoF -> ref -> dim -> DSO sync."""
        if not _should_run_real():
            pytest.skip("Requires USE_REAL_DB_FOR_TESTS=true and ENABLE_TEST_WRITES=true")
        if not _db_is_reachable(real_mssql_target_resource):
            pytest.skip(
                f"MSSQL target {real_mssql_target_resource.host}:"
                f"{real_mssql_target_resource.port} is unreachable"
            )

        definitions = dg.Definitions(
            assets=ORGANISATION_CS_JOB_ASSETS,
            jobs=[organisation_cs_job],
            resources={
                "mssql_target_staging": real_mssql_target_resource,
                "mssql_target_dso": real_mssql_target_resource,
                "mssql_source_staging": real_mssql_target_resource,
                "sydax_mssql": real_sydax_mssql_resource,
                "fhir_server": real_fhir_server_resource,
                "fhir_database": real_fhir_database_resource,
                "io_manager": dg.FilesystemIOManager(base_dir=str(tmp_path / "storage")),
            },
        )

        result = definitions.get_job_def("organisation_cs_job").execute_in_process(
            instance=dagster_instance,
        )

        assert result.success, "organisation_cs_job failed"

        sor_mats = result.asset_materializations_for_node("stg_organisation_sor")
        assert sor_mats, "No materialization emitted for stg_organisation_sor"

        ref_mats = result.asset_materializations_for_node("ref_organisation")
        assert ref_mats, "No materialization emitted for ref_organisation"

        dim_mats = result.asset_materializations_for_node("dim_organisation")
        assert dim_mats, "No materialization emitted for dim_organisation"

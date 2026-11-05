# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Registration/contract tests for the OS2mo (Magenta) integration.

Pure asset-graph validation — no live OS2mo, no database. What is guarded here
is the wiring that silently breaks: that each delivery hangs off a freshly built
codesystem, that each upload sends its own export, that the read-back waits for
its archive table to exist, and that the groups the job selects on are the
groups the assets actually declare.

Also guards the group-name split from ``DSI/csv_files/conceptmap_csv.py``: those
four ``ext_csv_*`` assets live in ``conceptmap_csv_extract``, and pulling them
into this job by reusing that name would run unrelated ConceptMap inputs on
every delivery.
"""

import pytest
from dagster import AssetKey
from definitions import defs
from DSI.os2mo.org_unit import arc_os2mo_org_unit
from DSO.export.os2mo.codesystem_assets import (
    OS2moUploadConfig,
    export_asset_name,
    upload_asset_name,
)
from DSO.export.os2mo.codesystems import CODESYSTEM_EXPORTS
from jobs.os2mo_jobs import os2mo_codesystem_export_job
from sensors.os2mo_delivery_sensor import SENSOR_NAME, delivery_run_config

pytestmark = pytest.mark.unit

EXPORT_GROUP = "os2mo_export"
IMPORT_GROUP = "os2mo_import"

ALL_EXPORT_ASSETS = [
    name
    for spec in CODESYSTEM_EXPORTS
    for name in (export_asset_name(spec), upload_asset_name(spec))
]


def _deps(name: str) -> set[AssetKey]:
    graph = defs.resolve_asset_graph()
    return set(graph.get(AssetKey.from_user_string(name)).parent_keys)


def _keys(*names: str) -> set[AssetKey]:
    return {AssetKey.from_user_string(name) for name in names}


def test_spec_names_and_prefixes_are_unique():
    names = [spec.name for spec in CODESYSTEM_EXPORTS]
    prefixes = [spec.asset_prefix for spec in CODESYSTEM_EXPORTS]
    assert len(names) == len(set(names))
    assert len(prefixes) == len(set(prefixes))


class TestAssetWiring:
    def test_sor_is_built_from_a_fresh_codesystem_into_its_table(self):
        assert _deps("os2mo_sor_export") == _keys("cs_sor", "staging_infra_export")

    def test_okosted_is_built_from_its_codesystem_alone(self):
        # Mappings left the file; the delivery depends only on the codesystem.
        assert _deps("os2mo_okosted_export") == _keys("cs_okosted", "staging_infra_export")

    def test_ean_is_built_from_the_invoice_receivers_and_its_codesystem(self):
        assert _deps("os2mo_ean_export") == _keys(
            "cs_ean", "arc_mysupply_statistic_invoice_header", "staging_infra_export"
        )

    def test_lager_is_built_from_its_codesystem_alone(self):
        assert _deps("os2mo_lager_export") == _keys("cs_lager", "staging_infra_export")

    @pytest.mark.parametrize("spec", CODESYSTEM_EXPORTS, ids=lambda s: s.name)
    def test_each_upload_consumes_its_own_export(self, spec):
        assert _deps(upload_asset_name(spec)) == _keys(export_asset_name(spec))

    def test_the_archive_waits_for_the_extract_and_its_table(self):
        key = AssetKey.from_user_string("arc_os2mo_org_unit")
        assert arc_os2mo_org_unit.asset_deps[key] == _keys(
            "ext_os2mo_org_unit", "staging_infra_source_os2mo"
        )


def _group_of(name: str) -> str:
    graph = defs.resolve_asset_graph()
    return graph.get(AssetKey.from_user_string(name)).group_name


class TestGroups:
    def test_export_assets_are_in_the_export_group(self):
        for name in ALL_EXPORT_ASSETS:
            assert _group_of(name) == EXPORT_GROUP

    def test_import_assets_are_in_their_own_group(self):
        for name in ("ext_os2mo_org_unit", "arc_os2mo_org_unit"):
            assert _group_of(name) == IMPORT_GROUP

    def test_the_csv_override_assets_are_not_pulled_in(self):
        """The hand-maintained ConceptMap CSVs keep their own group."""
        assert _group_of("ext_csv_ean_to_sor") == "conceptmap_csv_extract"


class TestJobAndSensor:
    def test_the_job_covers_every_codesystem_and_the_read_back(self):
        selected = os2mo_codesystem_export_job.selection.resolve(defs.resolve_asset_graph())
        assert _keys(*ALL_EXPORT_ASSETS, "ext_os2mo_org_unit", "arc_os2mo_org_unit") <= selected

    def test_the_sensor_waits_for_the_codesystem_rebuild(self):
        """The monitored job is not exposed on the definition, so this pins the
        description the factory builds from ``codesystem_job.name``."""
        sensor = next(s for s in defs.sensors if s.name.startswith(SENSOR_NAME))
        assert "organisation_cs_job SUCCESS" in sensor.description
        assert [job.name for job in sensor.jobs] == ["os2mo_codesystem_export_job"]

    @pytest.mark.parametrize("spec", CODESYSTEM_EXPORTS, ids=lambda s: s.name)
    def test_the_sensor_delivers_every_codesystem(self, spec):
        """dry_run is pinned False per upload, not inherited from the default."""
        ops = delivery_run_config()["ops"]
        assert ops[upload_asset_name(spec)]["config"]["dry_run"] is False

    def test_the_run_config_addresses_the_job_it_requests(self):
        """A key naming no op, or no config field, would only fail at run time."""
        job = defs.get_job_def("os2mo_codesystem_export_job")
        ops = delivery_run_config()["ops"]

        assert set(ops) <= {node.name for node in job.graph.nodes}
        for op_config in ops.values():
            assert set(op_config["config"]) <= set(OS2moUploadConfig.model_fields)

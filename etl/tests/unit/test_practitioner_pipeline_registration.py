# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Registration/contract tests for the SilkeborgData Practitioner DFM pipeline.

Guards the wiring from the archive through both transforms to the flat tables:
  arc.silkeborgdata_e26_pers
    -> fhir_server_practitioner_dfm / fhir_server_practitioner_role_dfm
    -> sof_practitioner_dfm / sof_practitioner_role_dfm -> fhir.* tables.

Pure asset-graph + config/view-JSON validation (no live Aidbox/MSSQL). Mirrors
``test_procedure_pipeline_registration.py``.

The view-JSON assertions are the only place the positional-code risk can be
caught: the transform is order-independent by construction, so a Python test
cannot fail on it — but a FHIRPath that read ``code[0]`` instead of filtering on
``system`` would silently shift columns once a role lacks one of its codes.
"""

import json
import re

import pytest
from dagster import AssetKey
from definitions import defs
from DSA.FHIR.Practitioner.Practitioner_SilkeborgData_dfm import fhir_server_practitioner_dfm
from DSA.FHIR.Practitioner.PractitionerRole_SilkeborgData_dfm import (
    fhir_server_practitioner_role_dfm,
)
from DSA.FHIR.SqlOnFhir.Practitioner_dfm import sof_practitioner_dfm
from DSA.FHIR.SqlOnFhir.PractitionerRole_dfm import sof_practitioner_role_dfm
from DSA.FHIR.utils.sql_on_fhir.sof_utils import SOF_RESOURCE_CONFIGS, VIEW_DEFINITIONS_DIR
from DSO.load.sync_configs import FHIR_SYNC_CONFIGS
from DSO.load.sync_fhir import all_fhir_dso_sync_dfm_assets
from tests.utils.asset_graph_helpers import (
    DFM_PRODUCER_PREFIXES,
    DSO_SYNC_PREFIXES,
    SOF_PREFIXES,
    assert_deps,
)

# Columns the SOF engine fills from r.ts / r.cts rather than from the view.
_SOF_MANAGED_COLUMNS = {"last_updated", "created_at"}
_AUDIT_COLUMNS = {"dw_timestamp_insert", "dw_timestamp_update"}
# Computed in the table from a view column; the SOF merge never writes it.
_COMPUTED_COLUMNS = {"identifier_staff_exact"}


def _view_columns(view_name: str) -> list[str]:
    with open(VIEW_DEFINITIONS_DIR / f"{view_name}.json") as f:
        view_def = json.load(f)
    return [c["name"] for block in view_def["select"] for c in block.get("column", [])]


def _view_paths(view_name: str) -> dict[str, str]:
    with open(VIEW_DEFINITIONS_DIR / f"{view_name}.json") as f:
        view_def = json.load(f)
    return {c["name"]: c["path"] for block in view_def["select"] for c in block.get("column", [])}


def _ddl_columns(table: str) -> set[str]:
    """Column names declared for [fhir].[<table>] in the FHIR table DDL."""
    ddl = VIEW_DEFINITIONS_DIR / ".." / ".." / ".." / ".." / "infrastructure"
    path = ddl.resolve() / "ddl" / "tables" / "fhir" / "01_tables.sql"
    text = path.read_text(encoding="utf-8")
    start = text.index(f"CREATE TABLE [fhir].[{table}] (")
    end = text.index("CONSTRAINT [PK_FHIR_", start)
    return set(re.findall(r"^\s{4,}\[(\w+)\]\s", text[start:end], flags=re.MULTILINE))


@pytest.mark.unit
class TestPractitionerPipelineRegistration:
    """Validate Practitioner/PractitionerRole DFM wiring + SOF contracts."""

    def test_practitioner_dfm_depends_on_the_archive(self):
        assert_deps(
            fhir_server_practitioner_dfm,
            "fhir_server_practitioner_dfm",
            allowed_prefixes=DFM_PRODUCER_PREFIXES,
            required={"arc_silkeborgdata_e26_pers"},
        )

    def test_role_dfm_waits_for_the_practitioner(self):
        """The role references the person by CPR, so the person must exist."""
        assert_deps(
            fhir_server_practitioner_role_dfm,
            "fhir_server_practitioner_role_dfm",
            allowed_prefixes=DFM_PRODUCER_PREFIXES,
            required={"arc_silkeborgdata_e26_pers", "fhir_server_practitioner_dfm"},
        )

    def test_sof_assets_name_their_producer(self):
        assert_deps(
            sof_practitioner_dfm,
            "sof_practitioner_dfm",
            allowed_prefixes=SOF_PREFIXES,
            required={
                "fhir_sof_infrastructure",
                "fhir_server_practitioner_dfm",
                "fhir_server_practitioner_epji_dfm",
            },
        )
        assert_deps(
            sof_practitioner_role_dfm,
            "sof_practitioner_role_dfm",
            allowed_prefixes=SOF_PREFIXES,
            required={"fhir_sof_infrastructure", "fhir_server_practitioner_role_dfm"},
        )

    @pytest.mark.parametrize(
        ("view_name", "table"),
        [("practitioner_view", "Practitioner"), ("practitionerrole_view", "PractitionerRole")],
    )
    def test_view_json_and_ddl_do_not_drift(self, view_name, table):
        view = set(_view_columns(view_name))
        ddl = _ddl_columns(table) - _AUDIT_COLUMNS - _SOF_MANAGED_COLUMNS - _COMPUTED_COLUMNS
        assert view == ddl, (
            f"{view_name} vs fhir.{table}: only in view={sorted(view - ddl)}, "
            f"only in DDL={sorted(ddl - view)}"
        )

    @pytest.mark.parametrize(
        ("view_name", "config_key"),
        [
            ("practitioner_view", "practitioner_view"),
            ("practitionerrole_view", "practitionerrole_view"),
        ],
    )
    def test_sof_config_matches_the_view(self, view_name, config_key):
        cfg = SOF_RESOURCE_CONFIGS[config_key]
        columns = _view_columns(view_name)
        assert cfg["view_name"] == view_name
        assert cfg["id_column"] in columns
        assert cfg["primary_keys"] == [cfg["id_column"]]
        # An absent isDeleted extension must land 0, not NULL: the column is
        # NOT NULL, so a NULL fails the MERGE outright.
        assert cfg["boolean_false_default_columns"] == ["dw_is_deleted"]

    @pytest.mark.parametrize(
        ("column", "expected_system"),
        [
            ("role_stillingskode", "urn:source:silkeborgdata:stillingskode"),
            ("role_stilling_text", "urn:source:silkeborgdata:stillingskode"),
            ("role_lederkode", "urn:source:silkeborgdata:lederkode"),
            ("specialty_disco", "urn:source:silkeborgdata:discokode"),
        ],
    )
    def test_role_codes_are_projected_by_system(self, column, expected_system):
        """Each code column filters on its own system.

        This is what keeps the four source codes apart. A path that indexed the
        repeating element instead would shift columns for any role missing a
        code — and lederkode is absent in 96,7 % of rows, so that is the norm.
        """
        path = _view_paths("practitionerrole_view")[column]
        assert f"system='{expected_system}'" in path
        assert "[0]" not in path, f"{column} must not depend on array position: {path}"

    def test_no_role_column_depends_on_array_position(self):
        for column, path in _view_paths("practitionerrole_view").items():
            assert "[0]" not in path, f"{column} depends on array position: {path}"

    def test_org_unit_is_projected_from_extensions_not_references(self):
        """No Inst -> SOR mapping exists, so organization/location are absent."""
        paths = _view_paths("practitionerrole_view")
        assert paths["inst"].startswith("extension('urn:source:silkeborgdata:inst')")
        assert paths["afdelingskode"].startswith(
            "extension('urn:source:silkeborgdata:afdelingskode')"
        )
        assert "organization_id" not in paths
        assert "location_id" not in paths

    def test_practitioner_assets_are_materializable(self):
        graph = defs.resolve_asset_graph()
        required = [
            "arc_silkeborgdata_e26_pers",
            "fhir_server_practitioner_dfm",
            "fhir_server_practitioner_role_dfm",
            "sof_practitioner_dfm",
            "sof_practitioner_role_dfm",
        ]
        for key_str in required:
            key = AssetKey.from_user_string(key_str)
            assert graph.has(key), f"Missing asset key: {key_str}"
            assert graph.get(key).is_materializable, f"Asset is not materializable: {key_str}"


def _ddl_primary_key(table: str) -> list[str]:
    """Columns in the PK constraint of [fhir].[<table>] in the FHIR table DDL."""
    ddl = VIEW_DEFINITIONS_DIR / ".." / ".." / ".." / ".." / "infrastructure"
    path = ddl.resolve() / "ddl" / "tables" / "fhir" / "01_tables.sql"
    text = path.read_text(encoding="utf-8")
    start = text.index(f"CREATE TABLE [fhir].[{table}] (")
    constraint = text.index(f"CONSTRAINT [PK_FHIR_{table}]", start)
    columns = text[constraint : text.index(")", constraint) + 1]
    return re.findall(r"\[(\w+)\]", columns[columns.index("(") :])


@pytest.mark.unit
class TestPractitionerDsoSync:
    """The DSO leg: staging fhir.* -> DSO fhir.*, and its DQA parity subjects.

    Without this the flat tables exist only on staging, while every BI consumer
    reads DSO — the resources would be invisible where they are actually used.
    """

    @pytest.mark.parametrize(
        "sof_name",
        ["practitioner", "practitioner_role"],
    )
    def test_dso_sync_follows_the_sof_asset(self, sof_name):
        by_name = {next(iter(a.keys)).to_user_string(): a for a in all_fhir_dso_sync_dfm_assets}
        dso_name = f"dso_sync_fhir_{sof_name}_dfm"
        assert dso_name in by_name, f"Missing DFM DSO sync asset: {dso_name}"
        assert_deps(
            by_name[dso_name],
            dso_name,
            allowed_prefixes=DSO_SYNC_PREFIXES,
            required={
                f"sof_{sof_name}_dfm",
                # Gates the MERGE on the DSO fhir.* tables, so it never runs
                # against a stale target schema/PK.
                "dso_infra_fhir",
            },
        )

    @pytest.mark.parametrize(
        ("config_key", "table"),
        [
            ("fhir_practitioner", "Practitioner"),
            ("fhir_practitioner_role", "PractitionerRole"),
        ],
    )
    def test_sync_config_agrees_with_the_ddl(self, config_key, table):
        """PK and watermark must be real columns, and the PK the table's real PK.

        A config naming a column the table lacks fails at MERGE time in the
        target environment, not here — so it is asserted here.
        """
        config = FHIR_SYNC_CONFIGS[config_key]
        columns = _ddl_columns(table)
        primary_key = (
            config.primary_key if isinstance(config.primary_key, list) else [config.primary_key]
        )
        assert config.schema == "fhir"
        assert config.table == table
        assert primary_key == _ddl_primary_key(table)
        assert set(primary_key) <= columns
        assert config.mode == "watermark"
        assert config.watermark_column in columns

    @pytest.mark.parametrize("config_key", ["fhir_practitioner", "fhir_practitioner_role"])
    def test_soft_delete_flag_is_synced_to_dso(self, config_key):
        """dw_is_deleted must reach DSO: BI filters on it, and only DSO is read."""
        config = FHIR_SYNC_CONFIGS[config_key]
        assert "dw_is_deleted" not in (config.exclude_columns or [])

    def test_dqa_parity_subjects_exist_for_both_tables(self):
        """Staging<->DSO row-count parity is derived from the sync configs."""
        from dqa.dso import _dso_subjects

        names = {getattr(subject, "name", "") for subject, _ in _dso_subjects()}
        assert {"dso_fhir_practitioner", "dso_fhir_practitioner_role"} <= names


class TestPractitionerDailyJob:
    """Both sources' chains run in one job on its own schedule, not on IFDB's."""

    @staticmethod
    def _job_keys() -> set[str]:
        job = defs.resolve_job_def("practitioner_daily_job")
        return {k.to_user_string() for k in job.asset_layer.executable_asset_keys}

    def test_job_selects_every_layer_of_the_chain(self):
        """THE BUG THIS CATCHES: a layer left out of the job.

        The layers live in six asset groups, and a selection naming only some of
        them looks complete — the job runs green while the layers it omits never
        run, or run later on another source's schedule with nothing guaranteeing
        this chain finished first.
        """
        keys = self._job_keys()
        for key in (
            "extract_silkeborgdata_e26_pers",
            "arc_silkeborgdata_e26_pers",
            "fhir_server_practitioner_dfm",
            "fhir_server_practitioner_role_dfm",
            "sof_practitioner_dfm",
            "sof_practitioner_role_dfm",
            "dso_sync_fhir_practitioner_dfm",
            "dso_sync_fhir_practitioner_role_dfm",
            "ext_epji_practitioner",
            "ext_epji_practitioner_identifier",
            "ext_epji_practitioner_role",
            "arc_epji_practitioner",
            "arc_epji_practitioner_identifier",
            "arc_epji_practitioner_role",
            "fhir_server_practitioner_epji_dfm",
        ):
            assert key in keys, f"{key} is not in practitioner_daily_job"

    def test_job_carries_no_infrastructure(self):
        """THE BUG THIS CATCHES: selecting infra into a scheduled job.

        None of the three is scoped to Practitioner — the DDL assets apply the
        schema for every fhir.* table, and fhir_sof_infrastructure globs every
        ViewDefinition and re-materializes the changed ones. Selecting them would
        make this the only scheduled job in the repo that applies them, so an
        unrelated migration would first reach production at 10:00 inside this run
        and could fail it. Infrastructure is a deploy step.
        """
        keys = self._job_keys()
        for key in ("staging_infra_fhir", "fhir_sof_infrastructure", "dso_infra_fhir"):
            assert key not in keys, f"{key} must not run on the Practitioner schedule"

    def test_no_ifdb_assets_ride_along(self):
        """The shared SOF/DSO groups must not be pulled in wholesale."""
        keys = self._job_keys()
        assert "sof_patient_dfm" not in keys
        assert "dso_sync_fhir_patient_dfm" not in keys

    def test_epji_location_stays_with_organisation_cs_job(self):
        """The epji_archive group also holds LOCATION; selecting it by group pulls that in."""
        keys = self._job_keys()
        assert "ext_epji_location" not in keys
        assert "arc_epji_shakalike" not in keys

    def test_practitioner_sof_and_dso_left_the_shared_groups(self):
        """Otherwise they stay in the IFDB jobs too, and run twice on two clocks."""
        ifdb_job = defs.resolve_job_def("ifdb_dfm_daily_job")
        ifdb_keys = {k.to_user_string() for k in ifdb_job.asset_layer.executable_asset_keys}
        for key in (
            "sof_practitioner_dfm",
            "sof_practitioner_role_dfm",
            "dso_sync_fhir_practitioner_dfm",
            "dso_sync_fhir_practitioner_role_dfm",
        ):
            assert key not in ifdb_keys, f"{key} still runs with the IFDB job"

    def test_schedule_drives_that_job_daily(self):
        schedule = defs.resolve_schedule_def("practitioner_daily_job_schedule")
        assert schedule.job_name == "practitioner_daily_job"
        assert schedule.cron_schedule == "0 10 * * *"


class TestArchiveAssetChecks:
    """Which of the four archive checks guards the chain, and how tightly.

    The blocking one is the only thing between a truncated delivery and the
    reporting database: `active` is recomputed for the whole population on every
    delivery, so a half-written file marks everyone it does not contain as
    departed. That is a deliberate posture, not an accident of a default, so
    changing it should cost a test edit.
    """

    @pytest.mark.parametrize(
        "name, blocking",
        [
            ("silkeborgdata_e26_pers_period_volume", True),
            # History and DFM state, not this delivery's bytes: they can be red
            # for days without this run being wrong, so they must not block.
            ("silkeborgdata_e26_pers_period_continuity", False),
            ("silkeborgdata_e26_pers_checkpoint_alignment", False),
            ("silkeborgdata_e26_pers_checkpoint_not_stalled", False),
        ],
    )
    def test_only_the_volume_check_blocks(self, name, blocking):
        from DSA.SQL.checks import silkeborgdata_checks

        check = getattr(silkeborgdata_checks, name)
        spec = next(s for s in check.check_specs if s.name == name)
        assert spec.blocking is blocking
        assert spec.asset_key == AssetKey("arc_silkeborgdata_e26_pers")

    def test_volume_tolerance_stays_tight(self):
        """10 % is ~3.800 persons — a file cut at 91 % would pass a blocking check."""
        from DSA.SQL.checks.silkeborgdata_checks import _VOLUME_TOLERANCE_PCT

        assert _VOLUME_TOLERANCE_PCT == 3.0

    def test_a_confirmed_large_change_can_be_let_through(self):
        """A blocking check ignores run mode, so full_load is not an escape hatch."""
        from DSA.SQL.checks.silkeborgdata_checks import PeriodVolumeConfig

        assert PeriodVolumeConfig().tolerance_pct is None
        assert PeriodVolumeConfig(tolerance_pct=25.0).tolerance_pct == 25.0


class TestCheckpointKeys:
    """The fhir_server checkpoint names the SOURCE, not just the resource type.

    EPJI is a second Practitioner producer. Two producers sharing one
    meta.DFMState row move each other's delta window, and the fix is only free
    before go-live: afterwards it needs a migration AND a full_load, because an
    unknown key reads as "never ran" and reloads the whole history.
    """

    @pytest.mark.parametrize(
        "module, expected",
        [
            (
                "DSA.FHIR.Practitioner.Practitioner_SilkeborgData_dfm",
                "Practitioner_SilkeborgData",
            ),
            (
                "DSA.FHIR.Practitioner.PractitionerRole_SilkeborgData_dfm",
                "PractitionerRole_SilkeborgData",
            ),
            ("DSA.FHIR.Practitioner.Practitioner_EPJI_dfm", "Practitioner_EPJI"),
        ],
    )
    def test_fhir_server_checkpoint_is_source_specific(self, module, expected):
        import importlib

        assert importlib.import_module(module)._CHECKPOINT_ENTITY == expected

    @pytest.mark.parametrize(
        "asset, config_key",
        [(sof_practitioner_dfm, "Practitioner"), (sof_practitioner_role_dfm, "PractitionerRole")],
    )
    def test_sof_checkpoint_stays_per_flat_table(self, asset, config_key):
        """Deliberately NOT source-suffixed, unlike the fhir_server one.

        The SOF checkpoint tracks a flat table, and fhir.Practitioner is one
        table no matter how many sources posted into it. Suffixing it would make
        two sources each believe they had read the whole view.
        """
        description = next(iter(asset.descriptions_by_key.values()))
        assert f"{config_key}_SilkeborgData" not in description

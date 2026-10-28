# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Registration/wiring test for the Genoptræningsplan (G-GOP) ServiceRequest pipeline.

Guards the DFM asset graph: the transform must be registered and wired to its
three upstream assets. A wrong/renamed dep silently breaks run ordering (the
transform would run before its arc tables are refreshed), so this is exactly the
"non-trivial wiring where a mistake silently breaks the pipeline" case.

Pure asset-graph validation (no live Aidbox/MSSQL). Mirrors
``test_documentreference_pipeline_registration.py``.

The SOF flatten-back layer for the recipient child (servicerequest_recipient_view.json
+ config + sof_service_request_recipient_dfm) is covered by TestServiceRequestRecipientSof.
"""

import json

import pytest
from dagster import AssetKey
from definitions import defs
from DSA.FHIR.ServiceRequest.ServiceRequest_IFDB_RehabilitationPlan_dfm import (
    fhir_server_rehabilitation_plan_dfm,
)
from DSA.FHIR.SqlOnFhir.ServiceRequest_Recipient_dfm import (
    sof_service_request_recipient_dfm,
)
from DSA.FHIR.utils.sql_on_fhir.sof_utils import SOF_RESOURCE_CONFIGS, VIEW_DEFINITIONS_DIR

_EXPECTED_DEPS = {
    AssetKey.from_user_string("arc_rehabilitation_plan_ifdb"),
    AssetKey.from_user_string("arc_rehabilitation_plan_receiver_ifdb"),
    AssetKey.from_user_string("fhir_server_patient_dfm"),
}


@pytest.mark.unit
class TestRehabilitationPlanPipelineRegistration:
    def test_transform_depends_on_arc_tables_and_patient(self):
        key = AssetKey.from_user_string("fhir_server_rehabilitation_plan_dfm")
        assert fhir_server_rehabilitation_plan_dfm.asset_deps[key] == _EXPECTED_DEPS

    def test_transform_is_registered_and_materializable(self):
        graph = defs.resolve_asset_graph()
        key = AssetKey.from_user_string("fhir_server_rehabilitation_plan_dfm")
        assert graph.has(key)
        assert graph.get(key).is_materializable

    def test_arc_assets_registered(self):
        graph = defs.resolve_asset_graph()
        for key_str in ("arc_rehabilitation_plan_ifdb", "arc_rehabilitation_plan_receiver_ifdb"):
            assert graph.has(AssetKey.from_user_string(key_str)), f"Missing: {key_str}"


# DDL business columns of [fhir].[ServiceRequest_Recipient] (audit cols excluded) —
# the flatten-back view MUST project exactly these so the MERGE column set matches.
_RECIPIENT_DDL_COLUMNS = {
    "service_request_id",
    "recipient_key",
    "receiver_type",
    "yder_nummer",
    "yder_navn",
    "afdeling_kode",
    "afdeling_kodetekst",
    "organization_ref",
    "status_code",
    "status_text",
    "transmission_time",
}


@pytest.mark.unit
class TestServiceRequestRecipientSof:
    """Contract between the recipient ViewDefinition, its SOF config, and the DDL."""

    def test_recipient_view_config_matches_contract(self):
        cfg = SOF_RESOURCE_CONFIGS["servicerequest_recipient_view"]
        assert cfg["resource_table"] == "servicerequest"
        assert cfg["id_column"] == "service_request_id"
        assert cfg["mssql_table"] == "ServiceRequest_Recipient"
        assert cfg["primary_keys"] == ["service_request_id", "recipient_key"]
        assert cfg["null_sentinel_columns"] == ["recipient_key"]

    def test_recipient_view_projects_exactly_the_ddl_columns(self):
        with open(VIEW_DEFINITIONS_DIR / "servicerequest_recipient_view.json") as f:
            view_def = json.load(f)
        columns = {c["name"] for block in view_def["select"] for c in block.get("column", [])}
        assert columns == _RECIPIENT_DDL_COLUMNS

    def test_recipient_view_uses_plain_foreach_over_receivers_extension(self):
        # Bug caught: forEachOrNull would emit an all-NULL child row for EVERY
        # ServiceRequest (incl. all HENVISNING/RIS), not just the rehab receivers.
        with open(VIEW_DEFINITIONS_DIR / "servicerequest_recipient_view.json") as f:
            view_def = json.load(f)
        for_each = [b["forEach"] for b in view_def["select"] if "forEach" in b]
        for_each_or_null = [b for b in view_def["select"] if "forEachOrNull" in b]
        assert len(for_each) == 1
        assert not for_each_or_null, "must use plain forEach (inner-join), not forEachOrNull"
        assert "urn:source:ifdb:rehabilitation_plan_receivers" in for_each[0]

    def test_dso_sync_primary_key_matches_sof_primary_key(self):
        # The DSO MERGE key must equal the SOF MERGE key (both = the DDL composite
        # PK). A mismatch would silently mis-merge recipient rows into DSO rather
        # than fail, so the two configs are pinned to each other here.
        from DSO.load.sync_configs import FHIR_SYNC_CONFIGS

        dso_cfg = FHIR_SYNC_CONFIGS["fhir_service_request_recipient"]
        assert dso_cfg.table == "ServiceRequest_Recipient"
        assert (
            dso_cfg.primary_key
            == SOF_RESOURCE_CONFIGS["servicerequest_recipient_view"]["primary_keys"]
        )

    def test_sof_recipient_asset_wired_to_transform(self):
        key = AssetKey.from_user_string("sof_service_request_recipient_dfm")
        deps = sof_service_request_recipient_dfm.asset_deps[key]
        assert AssetKey.from_user_string("fhir_server_rehabilitation_plan_dfm") in deps
        assert AssetKey.from_user_string("fhir_sof_infrastructure") in deps

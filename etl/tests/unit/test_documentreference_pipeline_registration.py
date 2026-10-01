# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Registration/contract tests for the DocumentReference SOF pipeline.

Guards the wiring from the two IFDB transforms (epikrise + care_discharge_report)
into the SOF flat tables:
  IFDB transforms -> SOF documentreference view/materialization -> fhir.* tables.

Pure asset-graph + config/view-JSON validation (no live Aidbox/MSSQL). Mirrors
``test_procedure_pipeline_registration.py`` / ``test_servicerequest_pipeline_registration.py``.
"""

import json

import pytest
from dagster import AssetKey
from definitions import defs
from DSA.FHIR.SqlOnFhir.DocumentReference_dfm import sof_documentreference_dfm
from DSA.FHIR.SqlOnFhir.DocumentReference_Recipient_dfm import (
    sof_documentreference_recipient_dfm,
)
from DSA.FHIR.utils.sql_on_fhir.sof_utils import VIEW_DEFINITIONS_DIR

_TRANSFORM_KEYS = {
    AssetKey.from_user_string("fhir_sof_infrastructure"),
    AssetKey.from_user_string("fhir_server_documentreference_epikrise_dfm"),
    AssetKey.from_user_string("fhir_server_documentreference_care_discharge_report_dfm"),
}


@pytest.mark.unit
class TestDocumentReferencePipelineRegistration:
    """Validate DocumentReference SOF wiring + view/config contracts."""

    def test_sof_documentreference_depends_on_both_transforms(self):
        key = AssetKey.from_user_string("sof_documentreference_dfm")
        assert sof_documentreference_dfm.asset_deps[key] == _TRANSFORM_KEYS

    def test_sof_recipient_depends_on_both_transforms(self):
        key = AssetKey.from_user_string("sof_documentreference_recipient_dfm")
        assert sof_documentreference_recipient_dfm.asset_deps[key] == _TRANSFORM_KEYS

    def test_documentreference_view_includes_required_columns(self):
        with open(VIEW_DEFINITIONS_DIR / "documentreference_view.json") as f:
            view_def = json.load(f)
        columns = [c["name"] for block in view_def["select"] for c in block.get("column", [])]
        expected = {
            "documentreference_id",
            "source_table",
            "status",
            "doc_status",
            "type_code",
            "type_system",
            "type_display",
            "patient_id",
            "author_org_ref",
            "doc_date",
            "epikrise_id",
            "care_discharge_report_id",
            "notification_identifier",
            "priority",
            "dw_is_deleted",
        }
        missing = expected - set(columns)
        assert not missing, f"documentreference_view is missing required columns: {missing}"

    def test_recipient_view_includes_required_columns(self):
        with open(VIEW_DEFINITIONS_DIR / "documentreference_recipient_view.json") as f:
            view_def = json.load(f)
        columns = [c["name"] for block in view_def["select"] for c in block.get("column", [])]
        expected = {
            "documentreference_id",
            "recipient_key",
            "recipient_type",
            "organization_ref",
            "recipient_name",
            "recipient_address",
            "cc_flag",
            "status_code",
            "status_text",
            "transmission_time",
        }
        missing = expected - set(columns)
        assert (
            not missing
        ), f"documentreference_recipient_view is missing required columns: {missing}"

    def test_recipient_view_foreach_iterates_recipients_extension(self):
        with open(VIEW_DEFINITIONS_DIR / "documentreference_recipient_view.json") as f:
            view_def = json.load(f)
        for_each = [block["forEach"] for block in view_def["select"] if "forEach" in block]
        assert len(for_each) == 1
        assert "urn:source:ifdb:document_reference_recipients" in for_each[0]

    def test_documentreference_assets_are_materializable(self):
        graph = defs.resolve_asset_graph()
        required = [
            "fhir_server_documentreference_epikrise_dfm",
            "fhir_server_documentreference_care_discharge_report_dfm",
            "sof_documentreference_dfm",
            "sof_documentreference_recipient_dfm",
        ]
        for key_str in required:
            key = AssetKey.from_user_string(key_str)
            assert graph.has(key), f"Missing asset key: {key_str}"
            assert graph.get(key).is_materializable, f"Asset is not materializable: {key_str}"

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The real source assets keep their contracts and delegate Patient work."""

from unittest.mock import MagicMock

import dagster as dg
import pytest
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.Patient import Patient_HAIBA, Patient_Udredningsret, cpr_stub_asset
from DSA.FHIR.Patient.patient_ownership import SOURCE_HAIBA, SOURCE_UDRED
from DSA.FHIR.Patient.patient_service import EnsureResult

pytestmark = pytest.mark.unit

# DRG's CPR query is resolved per run from the delivery manifest; its assets are
# covered in test_drg_extract.py. These two sources have a fixed query.
SOURCES = {
    "haiba": (Patient_HAIBA, Patient_HAIBA.fhir_patient_haiba, SOURCE_HAIBA, "haiba_stubs"),
    "udredningsret": (
        Patient_Udredningsret,
        Patient_Udredningsret.fhir_patient_udredningsret,
        SOURCE_UDRED,
        "udredningsret_stubs",
    ),
}


@pytest.fixture(params=sorted(SOURCES))
def source(request):
    module, asset, tag, source_name = SOURCES[request.param]
    return module, asset, cpr_stub_asset.CprStubConfig, tag, source_name


def _resources(rows):
    mssql = MagicMock(spec=MSSQLResource)
    cursor = mssql.get_connection.return_value.__enter__.return_value.cursor.return_value
    cursor.fetchall.return_value = rows
    fhir = MagicMock(spec=FHIRDatabaseResource)
    return {"mssql_target_staging": mssql, "fhir_database": fhir}, cursor


def _asset_context(resources):
    return dg.build_asset_context(
        resources={k: dg.ResourceDefinition.hardcoded_resource(v) for k, v in resources.items()}
    )


@pytest.mark.parametrize("cap", [None, 2, 0])
def test_source_asset_classifies_caps_and_delegates_to_patient_service(source, cap, monkeypatch):
    module, asset, config_type, tag, source_name = source
    resources, cursor = _resources(
        [
            ("9999", "C"),
            ("0202020202", "C"),
            ("0101010101", "C"),
            ("0303030303", "A"),
            ("0404040404", "B"),
        ]
    )
    candidates = ["0101010101", "0202020202", "9999"][:cap]
    # Simulate a candidate that the service finds in Aidbox despite tier C.
    ensure = MagicMock(return_value=EnsureResult(written=max(0, len(candidates) - 1), existing=1))
    monkeypatch.setattr(cpr_stub_asset, "ensure_patients", ensure)

    with _asset_context(resources) as context:
        result = asset(context, config=config_type(max_create=cap))

    executed = [call.args[0] for call in cursor.execute.call_args_list]
    assert any(module.CPR_SQL in sql for sql in executed)
    assert executed[-1] == cpr_stub_asset.TIER_SQL
    cursor.close.assert_called_once()
    if candidates:
        ensure.assert_called_once()
        assert ensure.call_args.args[1:] == (
            resources["mssql_target_staging"],
            resources["fhir_database"],
        )
        assert ensure.call_args.kwargs == {
            "cprs": candidates,
            "stub_source": tag,
            "source_name": source_name,
        }
    else:
        ensure.assert_not_called()
    # A source never writes directly to FHIR; only the service may do so.
    resources["fhir_database"].batch_upsert_resources.assert_not_called()
    assert {k: v.value for k, v in result.metadata.items()} == {
        "cpr_total": 5,
        "tier_a_resolves": 1,
        "tier_b_missing_ref_row": 1,
        "tier_c_missing": 3,
        "attempted_this_run": len(candidates),
        "already_in_aidbox": int(bool(candidates)),
        "posted": max(0, len(candidates) - 1),
        "failed": 0,
        "malformed_cpr": int(cap is None),
        "source_tag": tag,
    }


def test_source_asset_does_not_create_patients_for_tier_a_or_b(source, monkeypatch):
    _, asset, config_type, _, _ = source
    resources, _ = _resources([("0101010101", "A"), ("0202020202", "B")])
    ensure = MagicMock()
    monkeypatch.setattr(cpr_stub_asset, "ensure_patients", ensure)

    with _asset_context(resources) as context:
        result = asset(context, config=config_type())

    ensure.assert_not_called()
    assert result.metadata["attempted_this_run"].value == 0
    assert result.metadata["posted"].value == 0


def test_patient_service_failure_fails_the_source_asset(source, monkeypatch):
    _, asset, config_type, _, _ = source
    resources, cursor = _resources([("0101010101", "C")])
    failure = dg.Failure("1 Patient could not be written")
    monkeypatch.setattr(cpr_stub_asset, "ensure_patients", MagicMock(side_effect=failure))

    with _asset_context(resources) as context:
        with pytest.raises(dg.Failure) as raised:
            asset(context, config=config_type())

    assert raised.value is failure
    cursor.close.assert_called_once()


def test_classification_closes_cursor_on_sql_failure():
    resources, cursor = _resources([])
    cursor.execute.side_effect = RuntimeError("query failed")
    with pytest.raises(RuntimeError, match="query failed"):
        cpr_stub_asset.classify_cprs(resources["mssql_target_staging"], "source query")
    cursor.close.assert_called_once()

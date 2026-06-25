# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the shared content-complete CodeSystem PUT helper.

The SKS and SOR content-complete assets must post to Aidbox identically:
a single PUT, retried with exponential backoff on transient failures, and a
loud dg.Failure (with the hybrid-engine hint) once retries are exhausted.
"""

from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
import requests
from infrastructure.assets.fhir_codesystem_sks_complete import (
    put_content_complete_codesystem,
)

MODULE = "infrastructure.assets.fhir_codesystem_sks_complete"


def _fhir_server(put_mock):
    fs = MagicMock()
    fs.effective_base_url = "http://fhir-server:8888/fhir"
    fs.session.put = put_mock
    return fs


def _ok_response():
    r = MagicMock()
    r.status_code = 200
    r.raise_for_status = MagicMock()
    return r


def test_put_succeeds_first_attempt_targets_correct_url():
    put = MagicMock(return_value=_ok_response())
    resp = put_content_complete_codesystem(
        MagicMock(),
        _fhir_server(put),
        resource_id="sor",
        resource_json="{}",
        json_size_mb=0.0,
    )
    assert resp.status_code == 200
    assert put.call_count == 1
    assert put.call_args.args[0] == "http://fhir-server:8888/fhir/CodeSystem/sor"


@patch(f"{MODULE}.time.sleep")
def test_put_retries_then_succeeds(mock_sleep):
    put = MagicMock(
        side_effect=[
            requests.exceptions.ConnectionError("dropped"),
            requests.exceptions.ConnectionError("dropped"),
            _ok_response(),
        ]
    )
    resp = put_content_complete_codesystem(
        MagicMock(),
        _fhir_server(put),
        resource_id="sor",
        resource_json="{}",
        json_size_mb=0.0,
    )
    assert resp.status_code == 200
    assert put.call_count == 3
    assert mock_sleep.call_count == 2  # backoff between the three attempts


@patch(f"{MODULE}.time.sleep")
def test_put_raises_failure_after_max_retries(mock_sleep):
    put = MagicMock(side_effect=requests.exceptions.ConnectionError("dropped"))
    with pytest.raises(dg.Failure):
        put_content_complete_codesystem(
            MagicMock(),
            _fhir_server(put),
            resource_id="sor",
            resource_json="{}",
            json_size_mb=0.0,
            max_retries=3,
        )
    assert put.call_count == 3
    assert mock_sleep.call_count == 2

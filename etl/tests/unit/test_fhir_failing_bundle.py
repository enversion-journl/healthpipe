# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import os

import pytest
from core.resources import save_failing_bundle_to_disk

pytestmark = pytest.mark.unit

# Benign, non-PII bundle — this path keeps raw data (for local repro), so the
# safety control under test is the opt-in gate, not masking.
BUNDLE = {
    "resourceType": "Bundle",
    "entry": [
        {"resource": {"resourceType": "Patient", "id": "patient-abc"}, "request": {"method": "PUT"}}
    ],
}


class TestSaveFailingBundle:
    def test_disabled_by_default_writes_nothing(self, tmp_path, monkeypatch):
        monkeypatch.delenv("FHIR_DEBUG_SAVE_FAILING_BUNDLE", raising=False)
        monkeypatch.setenv("FHIR_FAILURES_DIR", str(tmp_path))

        result = save_failing_bundle_to_disk(0, BUNDLE, "error body", 400)

        assert result is None
        assert list(tmp_path.iterdir()) == []

    def test_enabled_writes_bundle_and_response(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FHIR_DEBUG_SAVE_FAILING_BUNDLE", "1")
        monkeypatch.setenv("FHIR_FAILURES_DIR", str(tmp_path))

        result = save_failing_bundle_to_disk(0, BUNDLE, "error body", 400)

        assert result is not None
        assert os.path.exists(result)
        assert len(list(tmp_path.iterdir())) == 2  # bundle + response

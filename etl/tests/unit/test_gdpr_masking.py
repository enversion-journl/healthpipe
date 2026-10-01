# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import pytest
from DSA.FHIR.utils.gdpr_masking import (
    mask_cpr_numbers,
    mask_fhir_resource_for_logging,
    mask_identifier_for_logging,
)

pytestmark = pytest.mark.unit

# A CPR is DDMMYY-XXXX. It is stored/transmitted both with and without the dash.
# Synthetic, non-real test values (sequential digits).
DASHED_CPR = "010203-1234"  # gitleaks:allow
DASHLESS_CPR = "0102031234"  # gitleaks:allow


class TestMaskCprNumbers:
    def test_masks_dashed_cpr(self):
        assert mask_cpr_numbers(f"patient {DASHED_CPR} failed") == "patient XXXXXX-XXXX failed"

    def test_masks_dashless_cpr(self):
        # Dash-less 10-digit CPR is the common storage form and MUST be masked.
        out = mask_cpr_numbers(f"patient {DASHLESS_CPR} failed")
        assert DASHLESS_CPR not in out

    def test_masks_dashless_cpr_embedded_in_json(self):
        out = mask_cpr_numbers(f'{{"value": "{DASHLESS_CPR}"}}')
        assert DASHLESS_CPR not in out


class TestMaskIdentifierForLogging:
    def test_dashless_cpr_masked_for_cpr_system(self):
        out = mask_identifier_for_logging(DASHLESS_CPR, "urn:oid:1.2.208.176.1.2")  # CPR OID
        assert DASHLESS_CPR not in out

    def test_dashless_cpr_masked_for_cpr_named_system(self):
        out = mask_identifier_for_logging(DASHLESS_CPR, "http://cpr.dk")
        assert DASHLESS_CPR not in out


class TestMaskFhirResourceForLogging:
    def test_dashless_cpr_in_identifier_value_is_masked(self):
        resource = {
            "resourceType": "Patient",
            "identifier": [{"system": "urn:oid:1.2.208.176.1.2", "value": DASHLESS_CPR}],
        }
        out = mask_fhir_resource_for_logging(resource)
        assert DASHLESS_CPR not in out

    def test_structured_patient_name_is_masked(self):
        # family/given are single words, so the two-word NAME_PATTERN misses them;
        # structured FHIR name fields must be masked wholesale regardless.
        resource = {
            "resourceType": "Patient",
            "name": [{"family": "Hansen", "given": ["Anders", "Bo"]}],
        }
        out = mask_fhir_resource_for_logging(resource)
        assert "Hansen" not in out
        assert "Anders" not in out
        assert '"Bo"' not in out

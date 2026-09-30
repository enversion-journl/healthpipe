# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import pytest
from dqa.result import _redact_examples

pytestmark = pytest.mark.unit

DASHLESS_CPR = "0102031234"  # gitleaks:allow


class TestRedactExamples:
    def test_masks_cpr_but_keeps_internal_keys(self):
        examples = [
            f"PATIENT_ID={DASHLESS_CPR}",
            "OBJECT_ID=12345",
            ["EOC", DASHLESS_CPR],
        ]

        out = _redact_examples(examples)
        rendered = str(out)

        assert DASHLESS_CPR not in rendered
        # Internal keys (not CPR-shaped) are preserved for remediation.
        assert "OBJECT_ID=12345" in rendered
        assert "EOC" in rendered

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import logging

import pytest
from core.logging_redaction import CprRedactingFilter

pytestmark = pytest.mark.unit

DASHLESS_CPR = "0102031234"  # gitleaks:allow


def _record(msg, *args):
    return logging.LogRecord("t", logging.INFO, __file__, 1, msg, args, None)


class TestCprRedactingFilter:
    def test_masks_dashless_cpr_in_message(self):
        rec = _record("patient %s failed", DASHLESS_CPR)
        assert CprRedactingFilter().filter(rec) is True
        assert DASHLESS_CPR not in rec.getMessage()

    def test_masks_dashless_cpr_in_plain_message(self):
        rec = _record(f"patient {DASHLESS_CPR} failed")
        CprRedactingFilter().filter(rec)
        assert DASHLESS_CPR not in rec.getMessage()

    def test_preserves_four_digit_numbers(self):
        # CPR-only masking must not mangle counts, years, or ports.
        rec = _record("run in year 2026 processed 4321 rows on port 1433")
        CprRedactingFilter().filter(rec)
        out = rec.getMessage()
        assert "2026" in out
        assert "4321" in out
        assert "1433" in out

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import logging

import dagster as dg
import pytest
from core.logging_redaction import CprRedactingFilter, install_pii_redaction

pytestmark = pytest.mark.unit

DASHLESS_CPR = "0102031234"  # gitleaks:allow
SYNTHETIC_CPR = "999999-9999"


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


def _context_log(context):
    context.log.info(f"patient {SYNTHETIC_CPR}")


def _named_logger(context):
    logging.getLogger("DSA.FHIR.redaction_test").warning(f"patient {SYNTHETIC_CPR}")


def _exception(context):
    raise ValueError(f"patient {SYNTHETIC_CPR}")


def _failure(context):
    raise dg.Failure(description=f"patient {SYNTHETIC_CPR}")


def _metadata(context):
    return dg.MaterializeResult(metadata={"patient": SYNTHETIC_CPR})


@pytest.mark.parametrize(
    "emit",
    [
        _context_log,
        pytest.param(
            _named_logger,
            marks=pytest.mark.xfail(strict=True, reason="managed Python loggers are not masked"),
        ),
        _exception,
        _failure,
        pytest.param(
            _metadata, marks=pytest.mark.xfail(strict=True, reason="metadata is not masked")
        ),
    ],
)
def test_event_log_masks_cpr(emit):
    install_pii_redaction()

    @dg.asset
    def leaky(context):
        return emit(context)

    instance = dg.DagsterInstance.ephemeral(
        settings={"python_logs": {"managed_python_loggers": ["DSA"]}}
    )
    result = dg.materialize([leaky], instance=instance, raise_on_error=False)
    assert SYNTHETIC_CPR not in repr(instance.all_logs(result.run_id))

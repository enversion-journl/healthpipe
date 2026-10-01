# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Central PII redaction for logging.

A best-effort safety net that masks Danish CPR numbers in log records across the
whole process, so a stray ``logger.info(...)`` that happens to include a CPR does
not leak it. Deliberately CPR-only: masking names/addresses globally would mangle
ordinary log content (counts, years, ports, table names), so those are handled at
the specific sinks that carry them (e.g. ``mask_fhir_resource_for_logging``).

Not covered: raw ``print``/stdout captured by Dagster's ComputeLogManager, which
bypasses the logging framework. Those paths are hardened individually.
"""

from __future__ import annotations

import logging

from DSA.FHIR.utils.gdpr_masking import mask_cpr_numbers

# Loggers we attach to directly. Attaching to a logger masks records emitted on it;
# attaching to that logger's handlers masks records that propagate up into them
# (covering module loggers that log through the root's handlers).
_DEFAULT_LOGGER_NAMES = ("", "dagster", "dagster.builtin")


class CprRedactingFilter(logging.Filter):
    """Logging filter that masks CPR numbers in the rendered message."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003 - logging API
        try:
            message = record.getMessage()
        except Exception:
            # If the record can't be rendered, don't block it — just let it through.
            return True
        masked = mask_cpr_numbers(message)
        if masked != message:
            record.msg = masked
            record.args = ()
        return True


def install_pii_redaction(logger_names: tuple[str, ...] = _DEFAULT_LOGGER_NAMES) -> None:
    """Attach a :class:`CprRedactingFilter` to the given loggers and their handlers.

    Idempotent: skips loggers/handlers that already have the filter.
    """
    pii_filter = CprRedactingFilter()

    def _add(target: logging.Logger | logging.Handler) -> None:
        if not any(isinstance(f, CprRedactingFilter) for f in target.filters):
            target.addFilter(pii_filter)

    for name in logger_names:
        logger = logging.getLogger(name)
        _add(logger)
        for handler in logger.handlers:
            _add(handler)

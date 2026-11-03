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

from dagster._core.log_manager import (
    DagsterLogManager,
    get_log_record_event,
    has_log_record_event,
    set_log_record_event,
)
from DSA.FHIR.utils.gdpr_masking import mask_cpr_numbers

# Loggers we attach to directly. Attaching to a logger masks records emitted on it;
# attaching to that logger's handlers masks records that propagate up into them
# (covering module loggers that log through the root's handlers).
_DEFAULT_LOGGER_NAMES = ("", "dagster", "dagster.builtin")
_DAGSTER_MAKE_RECORD = DagsterLogManager.makeRecord


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


def _mask_error(error):
    if error is None:
        return None
    return error._replace(
        message=mask_cpr_numbers(error.message),
        stack=[mask_cpr_numbers(line) for line in error.stack],
        cause=_mask_error(error.cause),
        context=_mask_error(error.context),
    )


def _mask_event(event):
    data = event.event_specific_data
    if getattr(data, "error", None) is not None:
        data = data._replace(error=_mask_error(data.error))
    failure = getattr(data, "user_failure_data", None)
    if failure is not None and failure.description:
        data = data._replace(
            user_failure_data=failure._replace(description=mask_cpr_numbers(failure.description))
        )
    if getattr(data, "first_step_failure_event", None) is not None:
        data = data._replace(first_step_failure_event=_mask_event(data.first_step_failure_event))
    message = event.message and mask_cpr_numbers(event.message)
    return event._replace(message=message, event_specific_data=data)


def _redact_dagster_log_manager(pii_filter: CprRedactingFilter) -> None:
    if DagsterLogManager.makeRecord is not _DAGSTER_MAKE_RECORD:
        return

    def redacting_make_record(self, *args, **kwargs):
        record = _DAGSTER_MAKE_RECORD(self, *args, **kwargs)
        pii_filter.filter(record)
        if has_log_record_event(record):
            set_log_record_event(record, _mask_event(get_log_record_event(record)))
        return record

    DagsterLogManager.makeRecord = redacting_make_record  # type: ignore[method-assign]


def install_pii_redaction(logger_names: tuple[str, ...] = _DEFAULT_LOGGER_NAMES) -> None:
    """Attach a :class:`CprRedactingFilter` to the given loggers and their handlers.

    Also masks records from ``context.log`` and the error text of Dagster events, which
    ``DagsterLogManager`` creates outside the logging registry.

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
    _redact_dagster_log_manager(pii_filter)

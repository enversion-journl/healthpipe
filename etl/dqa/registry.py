# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Check-type registry.

A check is any callable with the ``CheckFn`` signature::

    (context, subject, spec, resources) -> CheckOutcome

Reusable check types register a named implementation here via
``@register_check("key")``; one-off custom checks pass an inline ``fn`` on the
``CheckSpecification`` instead (same signature). The factory resolves them identically:
``impl = spec.fn or CHECK_TYPES[spec.check_type]``.
"""

from __future__ import annotations

from collections.abc import Callable

from dqa.model import CheckOutcome

CheckType = Callable[..., CheckOutcome]

CHECK_TYPES: dict[str, CheckType] = {}


def register_check(key: str) -> Callable[[CheckType], CheckType]:
    def _wrap(fn: CheckType) -> CheckType:
        if key in CHECK_TYPES:
            raise ValueError(f"Duplicate DQA check type registered: {key!r}")
        CHECK_TYPES[key] = fn
        return fn

    return _wrap

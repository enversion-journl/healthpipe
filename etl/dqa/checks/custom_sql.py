# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""custom_sql check: a convenience wrapper for one-off SQL assertions.

For anything more complex (cross-resource, Python-side logic) write an inline
``CheckFn`` and pass it via ``CheckSpecification(fn=...)`` instead -- same contract.

params:
    sql:          str            -- returns one row; first column = violation count
    checked_sql:  str | None     -- optional; first column = rows examined
    examples_sql: str | None     -- optional; first column per row = offending key
"""

from __future__ import annotations

from dqa.checks.helpers import run_violation_sql
from dqa.model import CheckOutcome
from dqa.registry import register_check


@register_check("custom_sql")
def custom_sql(context, subject, spec, resources) -> CheckOutcome:
    p = spec.params
    return run_violation_sql(
        resources,
        subject.resource_key,
        count_sql=p["sql"],
        examples_sql=p.get("examples_sql"),
        checked_sql=p.get("checked_sql"),
    )

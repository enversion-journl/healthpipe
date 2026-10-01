# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DQA check implementations.

Importing this package registers every reusable check type in
``dqa.registry.CHECK_TYPES``. Add a new reusable check by dropping a module here
that calls ``@register_check`` and importing it below.
"""

from dqa.checks import (  # noqa: F401  (imported for registration side effects)
    chain,
    completeness,
    custom_sql,
    dso_reconciliation,
    fhir_server_reconciliation,
    not_null,
    archive_reconciliation,
    patient_arc_integrity,
    referential_integrity,
    sentinel_rate,
    source_reconciliation,
    uniqueness,
)

__all__ = [
    "not_null",
    "sentinel_rate",
    "uniqueness",
    "referential_integrity",
    "source_reconciliation",
    "archive_reconciliation",
    "completeness",
    "custom_sql",
    "chain",
    "patient_arc_integrity",
    "fhir_server_reconciliation",
    "dso_reconciliation",
]

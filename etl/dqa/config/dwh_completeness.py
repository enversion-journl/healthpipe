# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Per-fact completeness specs: which source system + month-count query + date
surrogate to compare Source -> DSA -> DSO for each fact.

The source queries are reused verbatim from the existing partition-completeness
checks (single source of truth), so there's no duplicated SQL to drift.
"""

from __future__ import annotations

from dqa.model import Cadence, CheckSpecification, Severity
from DSA.SQL.checks.partition_completeness_checks import (
    _BUDGET_SOURCE_QUERY,
    _FAKTURA_SOURCE_QUERY,
    _FAKTURALINJE_SOURCE_QUERY,
    _FINANSKLADDELINJE_SOURCE_QUERY,
    _POSTERING_SOURCE_QUERY,
    _UDLEVERING_ARCHIVE_QUERY,
)

# table -> (source_resource_key, source_month_count_query, fact_date_column)

_COMPLETENESS: dict[str, tuple[str, str, str]] = {
    "fct.Faktura": ("mysupply_mssql", _FAKTURA_SOURCE_QUERY, "dw_ek_Dato_faktura"),
    "fct.Fakturalinje": ("mysupply_mssql", _FAKTURALINJE_SOURCE_QUERY, "dw_ek_Dato_faktura"),
    "fct.Postering": ("sydax_mssql", _POSTERING_SOURCE_QUERY, "dw_ek_Dato_bogfoering"),
    "fct.Vareudlevering": (
        "mssql_target_staging",
        _UDLEVERING_ARCHIVE_QUERY,
        "dw_ek_Dato_vareudlevering",
    ),
    "fct.Finanskladdelinje": ("sydax_mssql", _FINANSKLADDELINJE_SOURCE_QUERY, "dw_ek_Dato_linje"),
    "fct.Budget": ("sydax_mssql", _BUDGET_SOURCE_QUERY, "dw_ek_Dato_budget"),
}


def completeness_spec(table: str) -> CheckSpecification | None:
    """Completeness CheckSpecification for a fact, or None if the fact has no source mapping.

    Weekly (a 3-tier scan across Source/DSA/DSO); declares the source resource +
    mssql_target_dso via ``requires`` so the factory injects them.
    """
    entry = _COMPLETENESS.get(table)
    if entry is None:
        return None
    source_resource, source_query, date_column = entry
    return CheckSpecification(
        check_type="completeness",
        severity=Severity.ERROR,
        cadence=Cadence.WEEKLY,
        params={
            "source_resource": source_resource,
            "source_query": source_query,
            "date_column": date_column,
        },
        name_suffix="src_dsa_dso",
        requires=(source_resource, "mssql_target_dso"),
    )

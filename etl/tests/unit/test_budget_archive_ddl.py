# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Structural test: the 4 budget arc tables exist in the SydAX archive DDL."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

DDL = "\n".join(
    p.read_text(encoding="utf-8")
    for p in sorted(
        (Path(__file__).parent.parent.parent / "infrastructure" / "ddl" / "sources" / "sydax").glob(
            "*.sql"
        )
    )
)


def test_four_budget_arc_tables_present():
    for tbl in (
        "[arc].[sydax_budgetpost]",
        "[arc].[sydax_budgetmodel]",
        "[arc].[sydax_budgetpoststatus]",
        "[arc].[sydax_budgetbeloebtype]",
    ):
        assert tbl in DDL, tbl


def test_budgetpost_has_grain_and_scd_columns():
    # Grain key + the columns pre_stg / merge config rely on.
    # arc source columns stay verbatim (1:1 source case); audit columns snake_case.
    for col in (
        "[ref_LedgerbudgetId]",
        "[FinanskontoId]",
        "[BudgetmodelId]",
        "[BudgetpostStatusId]",
        "[BeloebstypeId]",
        "[Budgetdato]",
        "[BudgetAdoptedDate]",
        "[Oprettelsestidspunkt]",
        "[SidstAendretTidspunkt]",
        "[Budgetbeloeb]",
        "[BudgetbeloebValuta]",
        "[Valutakode]",
        "[DebetKredit]",
        "[Budgetkode]",
        "[Kildesystem]",
        "[StartDateTime]",
        "[EndDateTime]",
        "[dw_timestamp_insert]",
        "[dw_timestamp_update]",
    ):
        assert col in DDL, col

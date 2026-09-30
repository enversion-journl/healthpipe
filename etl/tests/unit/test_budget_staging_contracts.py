# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Column contract between pre_stg_Budget_sydax and its two stg consumers.

The graph tests (test_erp_dfm_pipeline_registration.py) pin the asset wiring;
this pins the column handoff: the producer must emit every column the stg
assets select from [stg].[pre_Budget_sydax], or the daily job fails at
runtime with "Invalid column name".
"""

import re
from pathlib import Path

import pytest

_STAGING = Path(__file__).resolve().parents[2] / "DSA" / "SQL" / "staging"

# Union of columns stg_fct_Budget_sydax + stg_dim_BudgetEgenskaber_sydax read
# from stg.pre_Budget_sydax (kept honest by the reverse-direction test below).
CONSUMED_COLUMNS = [
    "ref_LedgerbudgetId",
    "clean_code_Budget",
    "clean_code_BudgetEgenskaber",
    "clean_code_Kontoplan",
    "bk_finanskonto_id",
    "omkostningsstednummer",
    "Budgetdato",
    "BudgetAdoptedDate",
    "Oprettelsestidspunkt",
    "SidstAendretTidspunkt",
    "budgetbeloeb",
    "budgetbeloeb_valuta",
    "ref_BudgetmodelId",
    "budgetmodelnummer",
    "budgetmodeltekst",
    "budgetmodeltype",
    "ErLukket_Budgetmodel",
    "valutakode",
    "debet_kredit",
    "budgetkode",
    "aendringstype",
    "aendringstype_beskrivelse",
    "beloebstype_id",
    "budgetbeloeb_typetekst",
    "budgetpost_status_id",
    "budgetpost_status_tekst",
    "dw_timestamp_update",
]


def _has_column(src: str, column: str) -> bool:
    # \b keeps budgetbeloeb from matching inside budgetbeloeb_valuta etc.
    return re.search(rf"\b{re.escape(column)}\b", src) is not None


@pytest.mark.unit
def test_pre_stg_budget_emits_every_consumed_column():
    src = (_STAGING / "pre_stg_Budget_sydax.py").read_text(encoding="utf-8")
    missing = [c for c in CONSUMED_COLUMNS if not _has_column(src, c)]
    assert not missing, f"pre_stg_Budget_sydax no longer emits consumed columns: {missing}"


@pytest.mark.unit
def test_consumed_columns_list_matches_the_consumers():
    consumers = (_STAGING / "stg_fct_Budget_sydax.py").read_text(encoding="utf-8") + (
        _STAGING / "stg_dim_BudgetEgenskaber_sydax.py"
    ).read_text(encoding="utf-8")
    stale = [c for c in CONSUMED_COLUMNS if not _has_column(consumers, c)]
    assert not stale, f"columns no longer consumed by any stg budget asset: {stale}"

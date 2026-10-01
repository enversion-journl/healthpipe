# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Guardrail — dim.Leverandoervare maa ikke laengere baere aftale-scoped kolonner.

Aftale-relationen er mange-mange og lever i fct.AftaleVarePris.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

DSA_SQL = Path(__file__).resolve().parents[2] / "DSA" / "SQL"

FORBIDDEN_IN_DIMUPD = [
    "map].[Aftale",
    "dw_ek_Aftale",
    "src.pris1",
    "src.pakkestoerrelse1",
    "src.kreditorvarenummer1",
    "src.aftaletekst",
    "src.maerke",
    "ils_dsa_aftale_varenummer",
]

FORBIDDEN_IN_STG = [
    "aftaler.aftale,",
    "aftaler.afttype",
    "aftaler.aftaletekst",
    "aftaler.pris1",
    "aftaler.pakstr1",
    "aftaler.lvnr1",
    "aftaler.maerke",
    "clean_code_Aftale",
    "ils_dsa_aftale_varenummer",
]


def test_dimupd_vare_has_no_aftale_references():
    src = (DSA_SQL / "dim" / "dim_Leverandoervare.py").read_text(encoding="utf-8")
    for token in FORBIDDEN_IN_DIMUPD:
        assert token not in src, f"dim_Leverandoervare.py maa ikke indeholde '{token}'"


def test_stg_dim_leverandoervare_has_no_aftale_references():
    src = (DSA_SQL / "staging" / "stg_dim_Leverandoervare.py").read_text(encoding="utf-8")
    for token in FORBIDDEN_IN_STG:
        assert token not in src, f"stg_dim_Leverandoervare.py maa ikke indeholde '{token}'"

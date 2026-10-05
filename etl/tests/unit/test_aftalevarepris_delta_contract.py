# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Kontrakt-guards for delta-loadet i AftaleVarePris-kaeden (OPS-28980).

Staging er delta-drevet fra fct.AftaleVarePris-checkpointet; disse tests
pinner de kontrakt-egenskaber der ville genindfoere full load eller
fejlagtig soft delete, hvis de forsvandt.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

DSA_SQL = Path(__file__).resolve().parents[2] / "DSA" / "SQL"


def test_stg_fct_aftalevarepris_delta_is_checkpoint_gated():
    """Plausibel bug: checkpoint-filteret fjernes/glemmes -> full load vender tilbage."""
    src = (DSA_SQL / "staging" / "stg_fct_AftaleVarePris_ils.py").read_text(encoding="utf-8")
    assert (
        'checkpoint_from=("fct", "AftaleVarePris")' in src
    ), "Staging skal laese checkpointet fra ejeren fct.AftaleVarePris"
    assert (
        "INNER JOIN #delta_lines dl" in src
    ), "Backbone-laesningen skal gates af #delta_lines i delta-mode"


def test_stg_fct_aftalevarepris_delta_includes_time_flip():
    """Plausibel bug: dw_is_current gaar i staa for fremtidsdaterede prisversioner,
    hvis time-flip-signalet (prisversion traadt i kraft siden sidste checkpoint)
    fjernes fra delta-signalerne."""
    src = (DSA_SQL / "staging" / "stg_fct_AftaleVarePris_ils.py").read_text(encoding="utf-8")
    assert (
        "CAST(p.[start_] AS DATE) > @cp_date" in src
    ), "Time-flip-signalet skal sammenligne start_ mod @cp_date"


def test_stg_fct_aftalevarepris_dedups_all_natural_key_joins():
    """Plausibel bug: alle arc.ils_*-tabeller upsertes paa prrowid, ikke paa deres
    naturlige noegle. Flere rowids for samme (lnr, aftale, vnr) / (lnr, aftale) / lnr
    fan-out'er prisraekken til bk_prrowid-dubletter (bryder det unikke indeks).
    Alle tre join'ede tabeller skal derfor dedup'es til én raekke pr. naturlig noegle."""
    src = (DSA_SQL / "staging" / "stg_fct_AftaleVarePris_ils.py").read_text(encoding="utf-8")
    assert (
        "PARTITION BY d.lnr, d.aftale, d.vnr" in src
    ), "laftd-join'et skal dedup'es (ROW_NUMBER rn=1) paa (lnr, aftale, vnr)"
    assert (
        "PARTITION BY h.lnr, h.aftale" in src
    ), "lafth-join'et skal dedup'es (ROW_NUMBER rn=1) paa (lnr, aftale)"
    assert "PARTITION BY lev.lnr" in src, "lev-join'et skal dedup'es (ROW_NUMBER rn=1) paa lnr"


def test_fct_aftalevarepris_has_no_staging_anti_join_delete():
    """Plausibel bug: soft delete-anti-join mod delta-staging ville soft-delete
    alle raekker uden for delta. Arc sletter aldrig (detect_deletes=False paa
    alle ILS-arkiver), saa passet ville ogsaa vaere doedt kode."""
    src = (DSA_SQL / "fct" / "fct_AftaleVarePris.py").read_text(encoding="utf-8")
    assert "SOFT_DELETE_SQL" not in src

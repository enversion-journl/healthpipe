# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Guard tests for the Faktura / Fakturalinje -> Kontoplan linkage (finding 2.2).

The old fact-to-fact antipattern — "inherit dw_ek_Kontoplan onto the invoice
facts from fct.Postering via a TOP-1 correlated subquery on faktura_nr" — was
removed. The account genuinely lives at the posting grain (one finanskonto_id per
posting), so an invoice is many-valued over accounts. The link is now exposed to
consumers through vw_fct.Faktura / vw_fct.Fakturalinje, which join the newest
related posting via the ref.Faktura_To_Postering bridge (on dw_ek_Faktura).

These tests pin that contract at the source level, without a database, so the
antipattern cannot silently reappear. Account-weighted measures must be
aggregated on fct.Postering, never read off the (now removed) invoice FK.
"""

import os

import pytest

pytestmark = pytest.mark.unit

_FCT_DIR = os.path.join(os.path.dirname(__file__), "../../DSA/SQL/fct")
_VIEW_SQL = os.path.join(
    os.path.dirname(__file__),
    "../../infrastructure/ddl/views/erp/02_vw_fct.sql",
)


def _read(*parts: str) -> str:
    with open(os.path.join(*parts), encoding="utf-8") as f:
        return f.read()


class TestNoFactToFactKontoplanInheritance:
    """The TOP-1 fct.Postering subquery must not reappear in the fact loaders."""

    def test_faktura_loader_does_not_read_postering(self):
        sql = _read(_FCT_DIR, "fct_Faktura.py")
        assert "FROM [fct].[Postering]" not in sql, (
            "fct.Faktura must not read fct.Postering — the fact-to-fact Kontoplan "
            "inheritance (finding 2.2) was removed; resolve it via vw_fct.Faktura."
        )
        assert "SELECT TOP 1 p.dw_ek_Kontoplan" not in sql
        assert "dw_ek_Postering" not in sql, (
            "fct.Faktura no longer stores dw_ek_Postering — the fact-to-fact FK was "
            "dropped; the newest posting is exposed via vw_fct.Faktura instead."
        )

    def test_fakturalinje_loader_does_not_read_postering(self):
        sql = _read(_FCT_DIR, "fct_Fakturalinje.py")
        assert "FROM [fct].[Postering]" not in sql, (
            "fct.Fakturalinje must not read fct.Postering — the fact-to-fact "
            "Kontoplan inheritance (finding 2.2) was removed."
        )
        assert "SELECT TOP 1 p.dw_ek_Kontoplan" not in sql
        assert "AS dw_ek_Kontoplan" not in sql, (
            "fct.Fakturalinje no longer carries dw_ek_Kontoplan; the account is "
            "exposed via vw_fct.Fakturalinje.dw_ek_Kontoplan_postering."
        )


class TestKontoplanResolvedViaView:
    """vw_fct exposes the account from the newest related posting via the bridge."""

    def test_both_invoice_views_expose_kontoplan_from_postering(self):
        sql = _read(_VIEW_SQL)
        # vw_fct.Faktura AND vw_fct.Fakturalinje must each surface the account
        # from the newest related posting as dw_ek_Kontoplan_postering.
        assert sql.count("dw_ek_Kontoplan_postering") >= 2, (
            "Both vw_fct.Faktura and vw_fct.Fakturalinje must expose "
            "dw_ek_Kontoplan_postering (resolved from the newest posting via the "
            "ref.Faktura_To_Postering bridge)."
        )

    def test_faktura_view_joins_postering_via_faktura_key(self):
        sql = _read(_VIEW_SQL)
        faktura_view = sql.split("CREATE OR ALTER VIEW [vw_fct].[Faktura]", 1)[1]
        faktura_view = faktura_view.split("GO", 1)[0]
        assert (
            "dw_ek_Kontoplan_postering" in faktura_view
        ), "vw_fct.Faktura must expose dw_ek_Kontoplan_postering."
        assert "[vw_fct].[Postering]" in faktura_view, (
            "vw_fct.Faktura must resolve the account by joining vw_fct.Postering "
            "on dw_ek_Faktura (the bridge-stamped invoice link)."
        )

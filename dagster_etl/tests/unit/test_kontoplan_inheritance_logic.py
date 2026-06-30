# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Guard tests for the Faktura / Fakturalinje -> Kontoplan linkage (finding 2.2).

The old fact-to-fact antipattern — "inherit DW_EK_Kontoplan onto the invoice
facts from fct.Postering via a TOP-1 correlated subquery on FakturaNr" — was
removed. The account genuinely lives at the posting grain (one FinanskontoId per
posting), so an invoice is many-valued over accounts. The link is now exposed to
consumers through vw_fct.Faktura / vw_fct.Fakturalinje, which join the newest
related posting via the ref.Faktura_To_Postering bridge (on DW_EK_Faktura).

These tests pin that contract at the source level, without a database, so the
antipattern cannot silently reappear. Account-weighted measures must be
aggregated on fct.Postering, never read off the (now removed) invoice FK.
"""

import os

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
        sql = _read(_FCT_DIR, "FctUpd_faktura.py")
        assert "FROM [fct].[Postering]" not in sql, (
            "fct.Faktura must not read fct.Postering — the fact-to-fact Kontoplan "
            "inheritance (finding 2.2) was removed; resolve it via vw_fct.Faktura."
        )
        assert "SELECT TOP 1 p.DW_EK_Kontoplan" not in sql
        assert "DW_EK_Postering" not in sql, (
            "fct.Faktura no longer stores DW_EK_Postering — the fact-to-fact FK was "
            "dropped; the newest posting is exposed via vw_fct.Faktura instead."
        )

    def test_fakturalinje_loader_does_not_read_postering(self):
        sql = _read(_FCT_DIR, "FctUpd_fakturalinje.py")
        assert "FROM [fct].[Postering]" not in sql, (
            "fct.Fakturalinje must not read fct.Postering — the fact-to-fact "
            "Kontoplan inheritance (finding 2.2) was removed."
        )
        assert "SELECT TOP 1 p.DW_EK_Kontoplan" not in sql
        assert "AS DW_EK_Kontoplan" not in sql, (
            "fct.Fakturalinje no longer carries DW_EK_Kontoplan; the account is "
            "exposed via vw_fct.Fakturalinje.DW_EK_Kontoplan_Postering."
        )


class TestKontoplanResolvedViaView:
    """vw_fct exposes the account from the newest related posting via the bridge."""

    def test_both_invoice_views_expose_kontoplan_from_postering(self):
        sql = _read(_VIEW_SQL)
        # vw_fct.Faktura AND vw_fct.Fakturalinje must each surface the account
        # from the newest related posting as DW_EK_Kontoplan_Postering.
        assert sql.count("DW_EK_Kontoplan_Postering") >= 2, (
            "Both vw_fct.Faktura and vw_fct.Fakturalinje must expose "
            "DW_EK_Kontoplan_Postering (resolved from the newest posting via the "
            "ref.Faktura_To_Postering bridge)."
        )

    def test_faktura_view_joins_postering_via_faktura_key(self):
        sql = _read(_VIEW_SQL)
        faktura_view = sql.split("CREATE OR ALTER VIEW [vw_fct].[Faktura]", 1)[1]
        faktura_view = faktura_view.split("GO", 1)[0]
        assert (
            "DW_EK_Kontoplan_Postering" in faktura_view
        ), "vw_fct.Faktura must expose DW_EK_Kontoplan_Postering."
        assert "[vw_fct].[Postering]" in faktura_view, (
            "vw_fct.Faktura must resolve the account by joining vw_fct.Postering "
            "on DW_EK_Faktura (the bridge-stamped invoice link)."
        )

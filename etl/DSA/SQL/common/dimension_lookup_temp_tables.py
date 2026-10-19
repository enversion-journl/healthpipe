# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL for session-local CPR and SOR lookup tables used by DRG and udredningsret.

Execute the returned SQL on the SAME connection as the consuming query:

* create_borger_cpr_lookup_tables_sql() reads dim.Borger and replaces
  #borger_current_cpr(cpr, dw_ek_Borger) and
  #borger_previous_cpr(cpr, dw_ek_Borger). Both have one row per CPR.
* create_organisation_sor_lookup_table_sql() reads live dim.Organisation rows
  and replaces #organisation_sor(kode, dw_ek_Organisation), one row per SOR code.

The functions return SQL text; they do not open a connection or execute it.
The caller owns the connection lifetime. Temp tables disappear when it closes.
"""


def create_borger_cpr_lookup_tables_sql() -> str:
    """#borger_current_cpr (cpr_nummer) + #borger_previous_cpr (tidligere_cpr fallback).

    dw_ek_Borger is constant across dim.Borger's T2 address rows; MIN() is a
    deterministic collapse, mirroring HAIBA's guard against the one measured
    CPR with two patient ids. tidligere_cpr matters because DRG reaches years
    back in time; coverage with/without fallback is reported.
    """
    return """
DROP TABLE IF EXISTS #borger_current_cpr;
SELECT CAST([cpr_nummer] AS NVARCHAR(10)) AS [cpr], MIN([dw_ek_Borger]) AS [dw_ek_Borger]
INTO #borger_current_cpr
FROM [dim].[Borger]
WHERE [cpr_nummer] IS NOT NULL AND [cpr_nummer] <> ''
GROUP BY CAST([cpr_nummer] AS NVARCHAR(10));
CREATE UNIQUE CLUSTERED INDEX [IX_borger_cpr] ON #borger_current_cpr([cpr]);

DROP TABLE IF EXISTS #borger_previous_cpr;
SELECT [cpr], MIN([dw_ek_Borger]) AS [dw_ek_Borger]
INTO #borger_previous_cpr
FROM (
    SELECT CAST([tidligere_cpr] AS NVARCHAR(10)) AS [cpr], [dw_ek_Borger]
    FROM [dim].[Borger]
    WHERE [tidligere_cpr] IS NOT NULL AND [tidligere_cpr] <> ''
) AS t
GROUP BY [cpr];
CREATE UNIQUE CLUSTERED INDEX [IX_borger_tidl_cpr] ON #borger_previous_cpr([cpr]);
"""


def create_organisation_sor_lookup_table_sql() -> str:
    """#organisation_sor: live dim.Organisation rows with kode cast to NVARCHAR, uniquely indexed.

    Same rationale as HAIBA (dm/haiba.py): joining dim.Organisation's varchar
    kode against an nvarchar source column is unseekable; the temp copy makes
    the join type-matched and indexed. The schema only guarantees kode
    uniqueness among LIVE rows (a retired organisation keeps its row, and its
    kode can be reissued under a new key) — so soft-deleted rows are filtered
    both to keep the unique index valid and so new facts never resolve to a
    retired organisation.
    """
    return """
DROP TABLE IF EXISTS #organisation_sor;
SELECT CAST([kode] AS NVARCHAR(50)) AS [kode], [dw_ek_Organisation]
INTO #organisation_sor
FROM [dim].[Organisation]
WHERE [dw_is_deleted] = 0;
CREATE UNIQUE CLUSTERED INDEX [IX_org_kode] ON #organisation_sor([kode]);
"""

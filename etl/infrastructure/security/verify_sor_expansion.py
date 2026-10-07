#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Verificér SOR-hierarki-udpakningen i sec.RBOrganisation med dummy-brugere (dev).

Formål
------
AD-grupperne har endnu for få medlemmer til at validere udpakningen end-to-end
med rigtige brugere. Dette værktøj indlæser derfor det SENESTE reelle
AD-udtræk (audit-JSONL fra ``export_healthpipe_ad_groups.py``) plus to hardcodede
dummy-brugere gennem den rigtige loader, og krydstjekker resultatet mod en
UAFHÆNGIG rekursiv CTE-traversering af ``codesystem.SORHierarchy`` i SQL:

- ``zzdummy1`` tildeles en mellemstor organisation + (redundant) et barn af
  den — verificerer udpakning OG dedup af overlappende tildelinger.
- ``zzdummy2`` tildeles samme organisation med
  ``giv_adgang_til_underorganisationer=false`` — verificerer at flaget slår
  udpakning fra (præcis 1 række).

Organisationen vælges dynamisk fra det levende hierarki (mindste subtræ med
børn, max ``--max-subtree`` koder, og til stede i ``dim.Organisation``), så
værktøjet er robust over for SOR-ændringer. Bagefter genindlæses det reelle
udtræk, så tabellen ender i AD-sandhed (dummy-fodaftrykket holdes under
størrelses-guardens grænse, ellers afbrydes FØR der skrives).

Kørsel (fra etl-roden, fx i dev code-server-containeren)
--------------------------------------------------------
::

    python -m infrastructure.security.verify_sor_expansion --yes

Kræver samme miljøvariabler som loaderen (``MSSQL_TARGET_STAGING_*``) samt et
frisk audit-JSONL fra seneste extract (``LDAP_HP_OUTPUT_JSONL``).
KUN til dev/QA: afbryder hvis miljøet ligner prod. Skriver i
``sec.RBOrganisation`` (``sec.RolleDomaene`` røres ikke).

Exit codes: 0 = all checks passed, 1 = a check failed, 2 = missing config,
3 = the tool itself broke (e.g. a schema error). 3 is split out from 1 so a
schema error cannot hide as a check failure.
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback
from collections.abc import Iterator
from pathlib import Path

# Same helper as the loader — the verification must not have its own copy.
from core.environment import is_prod_environment, prod_environment_reason
from DSA.ConceptMap.utils import find_all_descendants

try:
    from infrastructure.security import load_healthpipe_ad_to_mssql as loader
except ImportError:
    sys.exit(
        "Kør som modul fra etl-roden: "
        "python -m infrastructure.security.verify_sor_expansion --yes"
    )

_DUMMY_EXPAND_USER = "zzdummy1"
_DUMMY_NOEXPAND_USER = "zzdummy2"

_DESCENDANTS_CTE_SQL = """
    WITH d AS (
        SELECT CAST(%s AS NVARCHAR(100)) AS code
        UNION ALL
        SELECT h.child_code
        FROM [codesystem].[SORHierarchy] h
        INNER JOIN d ON h.parent_code = d.code
    )
    SELECT DISTINCT o.[dw_ek_Organisation], o.[kode]
    FROM (SELECT DISTINCT code FROM d) dd
    INNER JOIN [dim].[Organisation] o ON o.[kode] = dd.code
    OPTION (MAXRECURSION 1000);
"""

# Root code alone: candidate selection + expected result for zzdummy2.
_DIM_CODE_SQL = "SELECT [dw_ek_Organisation], [kode] FROM [dim].[Organisation] WHERE [kode] = %s"


def _iter_candidate_codes(
    granted_codes: list[str],
    children: dict[str, list[str]],
    max_subtree: int,
) -> Iterator[tuple[str, str]]:
    """Yield (kode, barn)-kandidater med 2..max_subtree koder i udpakningen."""
    seen: set[str] = set()
    for granted in granted_codes:
        for code in sorted(find_all_descendants(granted, children)):
            if code in seen:
                continue
            seen.add(code)
            kids = children.get(code)
            if not kids:
                continue
            if 2 <= len(find_all_descendants(code, children)) <= max_subtree:
                yield code, sorted(kids)[0]


def _fetch_pairs(cur, sql: str, params: tuple = ()) -> set[tuple[int, str]]:
    cur.execute(sql, params)
    return {(row[0], row[1]) for row in cur.fetchall()}


def _actual_pairs(cur, bruger: str) -> set[tuple[int, str]]:
    return _fetch_pairs(
        cur,
        "SELECT DW_EK_RBOrganisation, SOR_kode FROM [sec].[RBOrganisation] WHERE bruger = %s",
        (bruger,),
    )


def _check(label: str, ok: bool, failures: list[str]) -> None:
    print(f"  {'OK  ' if ok else 'FEJL'} {label}")
    if not ok:
        failures.append(label)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verificér SOR-hierarki-udpakning i sec.RBOrganisation med dummy-brugere (dev)"
    )
    parser.add_argument("--yes", action="store_true", help="Bekræft skrivning til sec (dev/QA)")
    parser.add_argument(
        "--max-subtree", type=int, default=100, help="Max koder i dummy-subtræet (default 100)"
    )
    parser.add_argument(
        "--keep", action="store_true", help="Spring genindlæsning af AD-sandhed over"
    )
    args = parser.parse_args()

    loader._load_dotenv_repo_root()

    host = (os.environ.get("MSSQL_TARGET_STAGING_HOST") or "mssql").strip()
    if is_prod_environment():
        print(f"FEJL: ligner prod ({prod_environment_reason()}) — afbryder.")
        return 2
    if not args.yes:
        print("FEJL: kræver --yes (udskifter indholdet af sec.RBOrganisation på dev/QA).")
        return 2

    port = int((os.environ.get("MSSQL_TARGET_STAGING_PORT") or "1433").strip())
    db = (os.environ.get("MSSQL_TARGET_STAGING_DB") or "staging").strip()
    user = (os.environ.get("MSSQL_TARGET_STAGING_USER") or "").strip()
    password = (os.environ.get("MSSQL_TARGET_STAGING_PASSWORD") or "").strip()
    if not user or not password:
        print("FEJL: MSSQL_TARGET_STAGING_USER og MSSQL_TARGET_STAGING_PASSWORD skal være sat.")
        return 2

    org_jsonl = Path(
        os.environ.get("LDAP_HP_OUTPUT_JSONL")
        or str(Path(loader.__file__).resolve().parent / "output" / "healthpipe_grupper.jsonl")
    )
    real_rows = loader._read_jsonl(org_jsonl)
    if not real_rows:
        print(f"FEJL: intet reelt AD-udtræk i {org_jsonl} — kør extract først.")
        return 2

    min_ratio = loader._min_count_ratio()
    print(f"MSSQL: {host}:{port}/{db}")
    print(f"Reelt udtræk: {len(real_rows)} org-rækker fra {org_jsonl}")

    conn = loader._connect(host, port, db, user, password)
    failures: list[str] = []
    wrote_dummy = False
    tool_error = False
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM [sec].[RBOrganisation]")
        pre_total = cur.fetchone()[0]
        if pre_total == 0:
            print("FEJL: sec.RBOrganisation er tom — indlæs det reelle udtræk først (bootstrap).")
            return 2

        children = loader._fetch_sor_hierarchy(conn)
        granted = sorted({r["code_sor"] for r in real_rows})

        code = child = None
        expected_pairs: set[tuple[int, str]] = set()
        expected_root_pairs: set[tuple[int, str]] = set()
        for cand_code, cand_child in _iter_candidate_codes(granted, children, args.max_subtree):
            expected_pairs = _fetch_pairs(cur, _DESCENDANTS_CTE_SQL, (cand_code,))
            if not expected_pairs:
                continue
            # The root code must itself be in dim, not merely have descendants
            # there — otherwise the zzdummy2 check fails on correct behaviour.
            root_pairs = _fetch_pairs(cur, _DIM_CODE_SQL, (cand_code,))
            if not root_pairs:
                continue
            code, child, expected_root_pairs = cand_code, cand_child, root_pairs
            break
        if code is None:
            print(
                "FEJL: ingen egnet verifikationskode fundet (kræver rod i dim.Organisation "
                "— prøv større --max-subtree)."
            )
            return 2

        # Genindlæsningen af AD-sandhed skal kunne passere størrelses-guarden:
        # dummy-fodaftrykket må højst være (1-ratio)/ratio af det eksisterende.
        dummy_estimate = len(expected_pairs) + 1
        if dummy_estimate > pre_total * (1 - min_ratio) / min_ratio:
            print(
                f"FEJL: dummy-fodaftryk ({dummy_estimate}) for stort ift. guard "
                f"({pre_total} eksisterende, ratio {min_ratio}) — sænk --max-subtree."
            )
            return 2

        print(
            f"Verifikationskode: {code} (subtræ ∩ dim = {len(expected_pairs)} org), "
            f"overlap-barn: {child}"
        )

        dummy_rows = [
            {"bruger": _DUMMY_EXPAND_USER, "code_sor": code},
            {"bruger": _DUMMY_EXPAND_USER, "code_sor": child},
            {
                "bruger": _DUMMY_NOEXPAND_USER,
                "code_sor": code,
                "giv_adgang_til_underorganisationer": False,
            },
        ]
        wrote_dummy = True
        loader._load_rb_organisation(
            conn, real_rows + dummy_rows, dry_run=False, min_ratio=min_ratio
        )

        print("Checks:")
        actual1 = _actual_pairs(cur, _DUMMY_EXPAND_USER)
        _check(
            f"{_DUMMY_EXPAND_USER}: udpakning == uafhængig SQL-traversering "
            f"({len(expected_pairs)} rækker, overlap dedup'et)",
            actual1 == expected_pairs,
            failures,
        )
        actual2 = _actual_pairs(cur, _DUMMY_NOEXPAND_USER)
        # expected_root_pairs is guaranteed non-empty by candidate selection.
        _check(
            f"{_DUMMY_NOEXPAND_USER}: flag=false giver præcis koden selv "
            f"({len(expected_root_pairs)} rækker, ingen udpakning)",
            actual2 == expected_root_pairs,
            failures,
        )
    except Exception as e:
        # Tool failure, not an invariant failure: its own exit code.
        print(f"VÆRKTØJSFEJL: {type(e).__name__}: {e}")
        traceback.print_exc()
        tool_error = True
    finally:
        if wrote_dummy and not args.keep:
            try:
                loader._load_rb_organisation(conn, real_rows, dry_run=False, min_ratio=min_ratio)
                cur = conn.cursor()
                cur.execute(
                    "SELECT COUNT(*) FROM [sec].[RBOrganisation] WHERE bruger LIKE '%zzdummy%'"
                )
                leftover = cur.fetchone()[0]
                _check(
                    "genindlæsning: AD-sandhed genskabt uden dummy-rækker", leftover == 0, failures
                )
            except Exception as e:
                print(f"FEJL under genindlæsning: {type(e).__name__}: {e}")
                failures.append("restore")
        conn.close()

    print("-" * 60)
    if tool_error:
        print("AFBRUDT: værktøjet brød sammen — checks er IKKE afgjort (se traceback ovenfor).")
        return 3
    if failures:
        print(f"FEJLET: {len(failures)} check(s): {failures}")
        return 1
    print("ALLE CHECKS BESTÅET")
    return 0


if __name__ == "__main__":
    sys.exit(main())

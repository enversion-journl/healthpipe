#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Indlæs SydBI AD-gruppedata fra JSONL til sec.RolleDomaene og sec.RBOrganisation.

Formål
------
Læser JSONL-output fra ``export_sydbi_ad_groups.py`` og genindlæser
sec-tabellerne fuldt pr. kørsel (så brugere der er forsvundet fra AD-grupperne
ikke længere har en (stale) række):

- ``sec.RolleDomaene``    — bruger → domæne + rolle + populationsansvar.
  TRUNCATE+INSERT.
- ``sec.RBOrganisation``  — bruger → SOR-organisation; hver tildelt SOR-kode
  udpakkes til sig selv + alle underliggende koder via
  ``codesystem.SORHierarchy`` inden joinet til ``dim.Organisation``.
  Indlæses via SWAP (se nedenfor) — den levende tabel trunkeres ALDRIG.

``bruger`` gemmes som bart sAMAccountName (fx ``vah5ep``, uden
domænepræfiks); DPA-funktionerne stripper selv domænet fra ``SUSER_SNAME()``.

Swap-load af sec.RBOrganisation
-------------------------------
For at minimere nedetid roteres indholdet gennem tre permanente, strukturelt
identiske tabeller (metadata-only ``ALTER TABLE ... SWITCH`` i én transaktion):

1. ``sec.RBOrganisation_New`` trunkeres og fyldes (udpakning + dim-join + dedup).
2. Rækkeantal + distinkte brugere for gammel (levende) og ny tabel logges til
   ``sec.RBOrganisationLoadLog`` — også når guarden blokerer — som grundlag
   for en senere manuel acceptgate på de daglige kørsler.
3. ``sec.RBOrganisation_Old`` trunkeres; levende → _Old; _New → levende.

Forrige generation står altid i ``sec.RBOrganisation_Old``.

Workflow
--------
::

    # 1. Eksportér AD-data til JSONL
    python infrastructure/security/export_sydbi_ad_groups.py

    # 2. Indlæs JSONL i MSSQL
    python infrastructure/security/load_sydbi_ad_to_mssql.py

    # Dry-run (viser data og SQL uden at eksekvere)
    python infrastructure/security/load_sydbi_ad_to_mssql.py --dry-run

Miljøvariabler
--------------
Databaseforbindelse (fra ``.env``) — skriver til STAGING (sandhedskilden); den
nattlige Dagster-sync (dso_sync_security) propagerer staging→DSO:

- ``MSSQL_TARGET_STAGING_HOST``     (default: ``mssql``)
- ``MSSQL_TARGET_STAGING_PORT``     (default: ``1433``)
- ``MSSQL_TARGET_STAGING_USER``     / ``MSSQL_TARGET_STAGING_PASSWORD``
- ``MSSQL_TARGET_STAGING_DB``       (default: ``staging``)

Input-filer:

- ``LDAP_SYDBI_OUTPUT_ROLE_JSONL``   → ``sydbi_roller.jsonl``
- ``LDAP_SYDBI_OUTPUT_BOPAEL_JSONL`` → ``sydbi_bopael.jsonl``
- ``LDAP_SYDBI_OUTPUT_JSONL``        → ``sydbi_grupper.jsonl``

Størrelses-guard (task 28852)
-----------------------------
Et fejlbehæftet eller ufuldstændigt AD-udtræk må ikke stille og roligt tømme
sikkerhedsdata. Antallet af nye rækker valideres mod det eksisterende antal,
FØR indholdet udskiftes:

- ``AD_LOAD_MIN_COUNT_RATIO`` (default: ``0.8``) — udtrækket skal mindst
  udgøre denne andel af det nuværende antal rækker. Skal være et positivt
  tal; ugyldig værdi giver exit 2. Guard'en springes over når tabellen er
  tom (bootstrap/første load).

For ``sec.RBOrganisation`` valideres det FAKTISK indsatte antal i
``sec.RBOrganisation_New`` (efter hierarki-udpakning, dim.Organisation-join og
dedup), så et kollapset dim-join også fanges. Blokerer guarden, forbliver den
levende tabel urørt (swap springes over, exit 3).

The guard does NOT cover the hierarchy: at bootstrap the table is empty so the
guard is skipped, and a stale hierarchy hardly moves the row count enough to
trip the ratio. An empty hierarchy therefore fails separately (exit 4);
freshness is enforced by ``deps`` on ``cs_sor``.

Exit codes
----------
- **0** — indlæsning gennemført
- **1** — databasefejl
- **2** — manglende eller ugyldig konfiguration, eller manglende input-filer
- **3** — udtrækket er væsentligt mindre end sidste load; den ramte tabel er
  ikke ændret (en tidligere tabel i kørslen kan dog allerede være genindlæst)
- **4** — ``codesystem.SORHierarchy`` is empty; run ``cs_sor`` first
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Shared with the other SOR modules — one implementation of the expansion.
from DSA.ConceptMap.utils import find_all_descendants, rows_to_hierarchy_dict


def _load_dotenv_repo_root() -> None:
    # infrastructure/security/<this file> -> repo root is 4 parents up.
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    for env_file in (
        project_root / ".env",
        project_root / "tests" / "config" / "test.env",
    ):
        if env_file.is_file():
            load_dotenv(env_file, override=True)
            return
    load_dotenv(override=True)


def _strip_bruger_domain(bruger: str) -> str:
    """Bart sAMAccountName: alt til og med sidste backslash fjernes."""
    return bruger.rsplit("\\", 1)[-1]


def _read_jsonl(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    rows: list[dict] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _connect(host: str, port: int, database: str, user: str, password: str):
    import pymssql

    return pymssql.connect(
        server=host,
        port=port,
        database=database,
        user=user,
        password=password,
        tds_version="7.3",
        autocommit=False,
    )


class ExtractTooSmallError(RuntimeError):
    """AD-udtrækket er væsentligt mindre end det eksisterende indhold."""


class EmptySorHierarchyError(RuntimeError):
    """codesystem.SORHierarchy is empty — expansion would silently under-grant."""


def _validate_extract_size(
    table: str, existing_count: int, incoming_count: int, min_ratio: float
) -> None:
    """Guard mod at et for lille/ufuldstændigt udtræk tømmer måltabellen.

    Springes over når tabellen er tom (bootstrap). Skal kaldes FØR indholdet
    udskiftes (TRUNCATE hhv. swap).
    """
    if existing_count <= 0:
        return
    if incoming_count < existing_count * min_ratio:
        raise ExtractTooSmallError(
            f"{table}: udtræk er væsentligt mindre end sidste load "
            f"({incoming_count} nye vs {existing_count} eksisterende, "
            f"kræver mindst {min_ratio:.0%}). Tabellen er IKKE ændret."
        )


_DEFAULT_MIN_COUNT_RATIO = 0.8


def _min_count_ratio() -> float:
    """Læs AD_LOAD_MIN_COUNT_RATIO (default 0.8) — brugt af både main() og populate_security-asset.

    Fejler LUKKET: en ugyldig værdi (ikke-numerisk eller <= 0) giver ValueError
    i stedet for stille at deaktivere størrelses-guarden.
    """
    raw = (os.environ.get("AD_LOAD_MIN_COUNT_RATIO") or "").strip()
    if not raw:
        return _DEFAULT_MIN_COUNT_RATIO
    try:
        ratio = float(raw)
    except ValueError:
        raise ValueError(
            f"AD_LOAD_MIN_COUNT_RATIO er ugyldig: {raw!r} (skal være et positivt tal, fx 0.8)"
        ) from None
    if ratio <= 0:
        raise ValueError(
            f"AD_LOAD_MIN_COUNT_RATIO er ugyldig: {raw!r} (skal være et positivt tal, fx 0.8)"
        )
    return ratio


# ---------------------------------------------------------------------------
# sec.RolleDomaene — TRUNCATE+INSERT from roller + bopæl JSONL
# ---------------------------------------------------------------------------

_TRUNCATE_ROLLE_DOMAENE_SQL = "TRUNCATE TABLE [sec].[RolleDomaene];"

_INSERT_ROLLE_DOMAENE_SQL = """
    INSERT INTO [sec].[RolleDomaene]
        ([bruger], [Sikkerhedsdomaene], [Rolle],
         [Populationsansvar], [dw_timestamp_update], [dw_is_deleted])
    SELECT [bruger], [Sikkerhedsdomaene], [Rolle],
           [Populationsansvar], GETDATE(), 0
    FROM #SrcRolleDomaene;
"""


def _build_rolle_domaene_rows(
    rolle_rows: list[dict],
    bopael_rows: list[dict],
) -> list[tuple[str, str, str, str]]:
    """Combine rolle + bopæl JSONL into (bruger, Sikkerhedsdomaene, Rolle, Populationsansvar).

    Bopæl hospital is mapped to Populationsansvar for every domain the user has.
    ``bruger`` gemmes som bart sAMAccountName (via ``_strip_bruger_domain``).

    Dublet-regler (task 28852): ved flere bopæle vælges den alfabetisk første;
    ved flere roller for samme (bruger, Sikkerhedsdomaene) vælges K over U.
    """
    bopael_map: dict[str, str] = {}
    for b in bopael_rows:
        bruger = _strip_bruger_domain(b["bruger"]).lower()
        hospital = b["hospital"]
        if bruger not in bopael_map or hospital < bopael_map[bruger]:
            bopael_map[bruger] = hospital

    chosen: dict[tuple[str, str], tuple[str, str, str, str]] = {}
    for r in rolle_rows:
        bruger = _strip_bruger_domain(r["bruger"])
        domain_name = r["domain"]
        rolle = "U" if r.get("ubegraenset") else "K"
        populationsansvar = bopael_map.get(bruger.lower(), "")

        key = (bruger.lower(), domain_name.lower())
        if key not in chosen:
            chosen[key] = (bruger, domain_name, rolle, populationsansvar)
        elif rolle == "K" and chosen[key][2] == "U":
            chosen[key] = (bruger, domain_name, "K", populationsansvar)

    return list(chosen.values())


def _load_rolle_domaene(
    conn,
    rolle_rows: list[dict],
    bopael_rows: list[dict],
    dry_run: bool,
    min_ratio: float,
) -> int:
    """TRUNCATE+INSERT rows into sec.RolleDomaene. Returns inserted row count."""
    rows = _build_rolle_domaene_rows(rolle_rows, bopael_rows)

    if not rows:
        # Fail-safe med vilje: et tomt udtræk må IKKE truncate'e tabellen —
        # forældede rækker overlever hellere end at sikkerhedsdata tømmes.
        print("  sec.RolleDomaene: ingen rækker at indlæse.")
        return 0

    if dry_run:
        print("  [DRY-RUN] sec.RolleDomaene")
        print(f"  Rækker: {len(rows)}")
        for bruger, domain, rolle, pop in rows[:10]:
            pop_label = f", Populationsansvar={pop}" if pop else ""
            print(f"    {bruger} → {domain} ({rolle}){pop_label}")
        if len(rows) > 10:
            print(f"    ... og {len(rows) - 10} flere")
        print("  TRUNCATE SQL:")
        print(_TRUNCATE_ROLLE_DOMAENE_SQL)
        print("  INSERT SQL:")
        print(_INSERT_ROLLE_DOMAENE_SQL)
        return 0

    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE #SrcRolleDomaene (
            [bruger]             NVARCHAR(50)  COLLATE DATABASE_DEFAULT NOT NULL,
            [Sikkerhedsdomaene]  NVARCHAR(50)  COLLATE DATABASE_DEFAULT NOT NULL,
            [Rolle]              CHAR(1)       COLLATE DATABASE_DEFAULT NOT NULL,
            [Populationsansvar]  NVARCHAR(50)  COLLATE DATABASE_DEFAULT NOT NULL
        );
    """
    )

    for bruger, domain, rolle, pop in rows:
        cur.execute(
            "INSERT INTO #SrcRolleDomaene "
            "([bruger], [Sikkerhedsdomaene], [Rolle], [Populationsansvar]) "
            "VALUES (%s, %s, %s, %s)",
            (bruger, domain, rolle, pop),
        )

    # Guard'en ligger med vilje umiddelbart op ad TRUNCATE, i samme
    # transaktion, så det validerede count ikke kan drifte fra det der slettes.
    cur.execute("SELECT COUNT(*) FROM [sec].[RolleDomaene]")
    existing_count = cur.fetchone()[0]
    _validate_extract_size("sec.RolleDomaene", existing_count, len(rows), min_ratio)

    cur.execute(_TRUNCATE_ROLLE_DOMAENE_SQL)
    cur.execute(_INSERT_ROLLE_DOMAENE_SQL)
    inserted = cur.rowcount

    cur.execute("DROP TABLE #SrcRolleDomaene")
    conn.commit()

    print(f"  sec.RolleDomaene: {inserted} rækker indsat (truncate+insert)")
    return inserted


# ---------------------------------------------------------------------------
# SOR-hierarki — udpakning af tildelte SOR-koder til alle underliggende koder
# ---------------------------------------------------------------------------

_FETCH_SOR_HIERARCHY_SQL = "SELECT [parent_code], [child_code] FROM [codesystem].[SORHierarchy];"


def _fetch_sor_hierarchy(conn) -> dict[str, list[str]]:
    """parent_code -> [child_code, ...] fra codesystem.SORHierarchy (staging).

    Raises ``EmptySorHierarchyError`` on an empty table: otherwise every grant
    silently collapses to the code itself (under-grants), and the guard does
    not catch it at bootstrap.
    """
    cur = conn.cursor()
    cur.execute(_FETCH_SOR_HIERARCHY_SQL)
    children = rows_to_hierarchy_dict(cur.fetchall())
    if not children:
        raise EmptySorHierarchyError(
            "codesystem.SORHierarchy is empty - run cs_sor before the security load."
        )
    return children


def _build_rb_organisation_rows(
    org_rows: list[dict],
    children: dict[str, list[str]],
) -> list[tuple[str, str]]:
    """(bruger, SOR_kode)-par til staging: domæne-strip, hierarki-udpakning, dedup.

    Hver tildelt SOR-kode udpakkes til sig selv + alle underliggende koder via
    ``find_all_descendants`` (giv_adgang_til_underorganisationer=False slår
    udpakning fra; manglende felt udpakker). Dedup på (bruger.lower(), kode);
    deterministisk output-orden.

    ``org_rows`` is one row per user x group, so the expansion is memoised per
    distinct code.
    """
    seen: set[tuple[str, str]] = set()
    rows: list[tuple[str, str]] = []
    expanded: dict[str, list[str]] = {}
    for r in org_rows:
        bruger = _strip_bruger_domain(r["bruger"])
        if r.get("giv_adgang_til_underorganisationer", True):
            granted = r["code_sor"]
            codes = expanded.get(granted)
            if codes is None:
                codes = sorted(find_all_descendants(granted, children))
                expanded[granted] = codes
        else:
            codes = [r["code_sor"]]
        for code in codes:
            key = (bruger.lower(), code)
            if key not in seen:
                seen.add(key)
                rows.append((bruger, code))
    return rows


_STAGE_RB_ORGANISATION_BATCH_SIZE = 500


def _stage_rb_organisation_rows(cur, rows: list[tuple[str, str]]) -> None:
    """Multi-row VALUES-batches ind i #SrcRBOrganisation (udpakning kan give mange rækker)."""
    for i in range(0, len(rows), _STAGE_RB_ORGANISATION_BATCH_SIZE):
        chunk = rows[i : i + _STAGE_RB_ORGANISATION_BATCH_SIZE]
        placeholders = ", ".join(["(%s, %s)"] * len(chunk))
        cur.execute(
            "INSERT INTO #SrcRBOrganisation ([bruger], [SOR_kode]) VALUES " + placeholders,
            tuple(v for row in chunk for v in row),
        )


# ---------------------------------------------------------------------------
# sec.RBOrganisation — swap-load (_New fyldes, roteres atomisk til levende)
# ---------------------------------------------------------------------------

_TRUNCATE_RB_ORGANISATION_NEW_SQL = "TRUNCATE TABLE [sec].[RBOrganisation_New];"

_INSERT_RB_ORGANISATION_NEW_SQL = """
    INSERT INTO [sec].[RBOrganisation_New]
        ([bruger], [DW_EK_RBOrganisation], [SOR_kode], [dw_timestamp_update])
    SELECT [bruger], [DW_EK_RBOrganisation], [SOR_kode], GETDATE()
    FROM (
        SELECT
            src.[bruger],
            org.[dw_ek_Organisation] AS [DW_EK_RBOrganisation],
            src.[SOR_kode],
            ROW_NUMBER() OVER (
                PARTITION BY src.[bruger], org.[dw_ek_Organisation]
                ORDER BY src.[SOR_kode]
            ) AS rn
        FROM #SrcRBOrganisation AS src
        INNER JOIN [dim].[Organisation] AS org
            ON org.[kode] = src.[SOR_kode]
    ) AS d
    WHERE d.rn = 1;
"""

# Metadata-only rotation i én transaktion: læsere ser enten gammelt eller nyt
# indhold — aldrig en tom/halvfyldt tabel. Forrige generation står i _Old.
# NB: staging only. The DSO sync is truncate_insert — see sikkerhedsmodel.md.
_SWAP_RB_ORGANISATION_SQL = """
    TRUNCATE TABLE [sec].[RBOrganisation_Old];
    ALTER TABLE [sec].[RBOrganisation] SWITCH TO [sec].[RBOrganisation_Old];
    ALTER TABLE [sec].[RBOrganisation_New] SWITCH TO [sec].[RBOrganisation];
"""

_COUNT_RB_ORGANISATION_LIVE_SQL = (
    "SELECT COUNT(*), COUNT(DISTINCT [bruger]) FROM [sec].[RBOrganisation];"
)

_COUNT_RB_ORGANISATION_NEW_SQL = (
    "SELECT COUNT(*), COUNT(DISTINCT [bruger]) FROM [sec].[RBOrganisation_New];"
)

_INSERT_RB_ORGANISATION_LOAD_LOG_SQL = (
    "INSERT INTO [sec].[RBOrganisationLoadLog] "
    "([RaekkerGammel], [RaekkerNy], [BrugereGammel], [BrugereNy], [SwapUdfoert]) "
    "VALUES (%d, %d, %d, %d, %d)"
)


def _load_rb_organisation(
    conn,
    org_rows: list[dict],
    dry_run: bool,
    min_ratio: float,
) -> dict:
    """Swap-load af sec.RBOrganisation. Returnerer load-metrikker.

    ``bruger`` gemmes som bart sAMAccountName. Hver tildelt SOR-kode udpakkes
    til sig selv + alle underliggende koder via ``codesystem.SORHierarchy``
    (``giv_adgang_til_underorganisationer=False`` slår udpakning fra). Dedup
    happens in two steps: Python-side on (bruger, SOR_kode) after expansion,
    and again in SQL on the business key (bruger, DW_EK_RBOrganisation) AFTER
    the dim.Organisation join — because overlapping SOR-hierarki grants fan out
    to the same organisation ("Fjern dupletter efter udpakning af hierarki").

    Rækker indsættes i ``sec.RBOrganisation_New``; gammel/ny-metrikker logges
    til ``sec.RBOrganisationLoadLog`` (også når guarden blokerer), og indholdet
    roteres derefter atomisk. Den levende tabel trunkeres aldrig.
    """
    metrics = {
        "inserted": 0,
        "rows_old": 0,
        "rows_new": 0,
        "users_old": 0,
        "users_new": 0,
        "swapped": False,
    }

    if not org_rows:
        # Fail-safe med vilje: et tomt udtræk må IKKE udskifte indholdet —
        # forældede rækker overlever hellere end at sikkerhedsdata tømmes.
        print("  sec.RBOrganisation: ingen rækker at indlæse.")
        return metrics

    if dry_run:
        # Hierarki-udpakning kræver DB-opslag (codesystem.SORHierarchy) og
        # vises derfor ikke i dry-run; kun de tildelte par vises.
        rows = _build_rb_organisation_rows(org_rows, {})
        print("  [DRY-RUN] sec.RBOrganisation")
        print(f"  Rækker før hierarki-udpakning (udpakning kræver DB-opslag): {len(rows)}")
        for bruger, code in rows[:10]:
            print(f"    {bruger} → SOR {code}")
        if len(rows) > 10:
            print(f"    ... og {len(rows) - 10} flere")
        print("  INSERT SQL (join til dim.Organisation + dedup efter hierarki-udpakning):")
        print(_INSERT_RB_ORGANISATION_NEW_SQL)
        print("  SWAP SQL:")
        print(_SWAP_RB_ORGANISATION_SQL)
        return metrics

    children = _fetch_sor_hierarchy(conn)
    rows = _build_rb_organisation_rows(org_rows, children)

    cur = conn.cursor()

    cur.execute(_TRUNCATE_RB_ORGANISATION_NEW_SQL)
    cur.execute(
        """
        CREATE TABLE #SrcRBOrganisation (
            [bruger]    NVARCHAR(50)  COLLATE DATABASE_DEFAULT NOT NULL,
            [SOR_kode]  VARCHAR(50)   COLLATE DATABASE_DEFAULT NOT NULL
        );
    """
    )
    _stage_rb_organisation_rows(cur, rows)
    cur.execute(_INSERT_RB_ORGANISATION_NEW_SQL)
    metrics["inserted"] = cur.rowcount
    cur.execute("DROP TABLE #SrcRBOrganisation")

    cur.execute(_COUNT_RB_ORGANISATION_LIVE_SQL)
    metrics["rows_old"], metrics["users_old"] = cur.fetchone()
    cur.execute(_COUNT_RB_ORGANISATION_NEW_SQL)
    metrics["rows_new"], metrics["users_new"] = cur.fetchone()

    def _log(swapped: bool) -> None:
        cur.execute(
            _INSERT_RB_ORGANISATION_LOAD_LOG_SQL,
            (
                metrics["rows_old"],
                metrics["rows_new"],
                metrics["users_old"],
                metrics["users_new"],
                1 if swapped else 0,
            ),
        )

    try:
        _validate_extract_size(
            "sec.RBOrganisation", metrics["rows_old"], metrics["rows_new"], min_ratio
        )
    except ExtractTooSmallError:
        # Log også blokerede loads (grundlag for acceptgate-regler), commit
        # loggen og lad den levende tabel stå urørt.
        _log(swapped=False)
        conn.commit()
        raise

    cur.execute(_SWAP_RB_ORGANISATION_SQL)
    metrics["swapped"] = True
    _log(swapped=True)
    conn.commit()

    print(
        f"  sec.RBOrganisation: {metrics['rows_new']} rækker swappet ind "
        f"(før: {metrics['rows_old']} rækker/{metrics['users_old']} brugere, "
        f"efter: {metrics['rows_new']} rækker/{metrics['users_new']} brugere; "
        f"forrige generation i sec.RBOrganisation_Old)"
    )
    return metrics


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Indlæs SydBI AD JSONL-data i sec.RolleDomaene og sec.RBOrganisation"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Vis data og SQL uden at eksekvere mod databasen",
    )
    args = parser.parse_args()

    _load_dotenv_repo_root()

    # Skriv til STAGING (sandhedskilden). Den nattlige Dagster-sync
    # (dso_sync_sec_rolledomaene / dso_sync_sec_rborganisation) propagerer
    # staging->DSO. dim.Organisation (til RBOrganisation-join) findes på staging.
    host = (os.environ.get("MSSQL_TARGET_STAGING_HOST") or "mssql").strip()
    port = int((os.environ.get("MSSQL_TARGET_STAGING_PORT") or "1433").strip())
    db = (os.environ.get("MSSQL_TARGET_STAGING_DB") or "staging").strip()
    user = (os.environ.get("MSSQL_TARGET_STAGING_USER") or "").strip()
    password = (os.environ.get("MSSQL_TARGET_STAGING_PASSWORD") or "").strip()

    if not user or not password:
        print("FEJL: MSSQL_TARGET_STAGING_USER og MSSQL_TARGET_STAGING_PASSWORD skal være sat.")
        return 2

    try:
        min_ratio = _min_count_ratio()
    except ValueError as e:
        print(f"FEJL: {e}")
        return 2

    script_dir = Path(__file__).resolve().parent
    default_output_dir = script_dir / "output"

    rolle_jsonl = Path(
        os.environ.get("LDAP_SYDBI_OUTPUT_ROLE_JSONL")
        or str(default_output_dir / "sydbi_roller.jsonl")
    )
    bopael_jsonl = Path(
        os.environ.get("LDAP_SYDBI_OUTPUT_BOPAEL_JSONL")
        or str(default_output_dir / "sydbi_bopael.jsonl")
    )
    org_jsonl = Path(
        os.environ.get("LDAP_SYDBI_OUTPUT_JSONL") or str(default_output_dir / "sydbi_grupper.jsonl")
    )

    print(f"MSSQL: {host}:{port}/{db}")
    print(f"Input Rolle JSONL:  {rolle_jsonl}")
    print(f"Input Bopæl JSONL:  {bopael_jsonl}")
    print(f"Input Org JSONL:    {org_jsonl}")
    if args.dry_run:
        print("Mode: DRY-RUN (ingen ændringer)")
    print("-" * 60)

    rolle_rows = _read_jsonl(rolle_jsonl)
    bopael_rows = _read_jsonl(bopael_jsonl)
    org_rows = _read_jsonl(org_jsonl)

    print(f"Læst {len(rolle_rows)} rolle-rækker fra {rolle_jsonl.name}")
    print(f"Læst {len(bopael_rows)} bopæl-rækker fra {bopael_jsonl.name}")
    print(f"Læst {len(org_rows)} org-rækker fra {org_jsonl.name}")

    if not rolle_rows and not org_rows:
        print("Ingen data at indlæse — afslutter.")
        return 0

    if args.dry_run:
        # Guard'en kræver en åben forbindelse (COUNT(*) mod måltabellen) og
        # køres derfor IKKE i dry-run (min_ratio er valideret, men ubrugt her).
        _load_rolle_domaene(None, rolle_rows, bopael_rows, dry_run=True, min_ratio=min_ratio)
        _load_rb_organisation(None, org_rows, dry_run=True, min_ratio=min_ratio)
        print("-" * 60)
        print("DRY-RUN færdig — ingen ændringer foretaget.")
        return 0

    try:
        conn = _connect(host, port, db, user, password)
    except Exception as e:
        print(f"FEJL: Kunne ikke forbinde til MSSQL ({type(e).__name__}): {e}")
        return 1

    try:
        rd_ins = _load_rolle_domaene(
            conn, rolle_rows, bopael_rows, dry_run=False, min_ratio=min_ratio
        )
        rb_metrics = _load_rb_organisation(conn, org_rows, dry_run=False, min_ratio=min_ratio)
    except ExtractTooSmallError as e:
        print(f"FEJL: {e}")
        try:
            conn.close()
        except Exception:
            pass
        return 3
    except EmptySorHierarchyError as e:
        print(f"FEJL: {e}")
        try:
            conn.close()
        except Exception:
            pass
        return 4
    except Exception as e:
        print(f"FEJL: Indlæsning fejlede ({type(e).__name__}): {e}")
        try:
            conn.close()
        except Exception:
            pass
        return 1

    print("-" * 60)
    print(f"RolleDomaene:    {rd_ins} rækker indsat")
    print(
        f"RBOrganisation:  {rb_metrics['rows_new']} rækker "
        f"({rb_metrics['users_new']} brugere) swappet ind"
    )

    try:
        conn.close()
    except Exception:
        pass

    return 0


if __name__ == "__main__":
    sys.exit(main())

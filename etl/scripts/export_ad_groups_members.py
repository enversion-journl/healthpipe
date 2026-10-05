#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Eksportér AD-grupper og deres direkte medlemmer under en given OU.

Formål
------
Finder alle grupper (``objectCategory=group``) under en angivet OU i Active
Directory via en paged SUBTREE-søgning, og opløser hvert ``member``-DN til
et ``sAMAccountName`` via BASE-opslag. Resultatet skrives til stdout i JSONL-
eller tekst-format.

Hvis den angivne OU-DN ikke findes, foreslår scriptet kandidat-DN'er baseret
på OU-navnet (søgning under nærmeste matchende base).

Forudsætninger
--------------
- VPN aktivt (ldaps.example.org er intern)
- Python-pakker: ``ldap3``, ``pycryptodome``, ``python-dotenv``
- LDAP_* miljøvariabler sat i ``.env`` (repo-root)
- ``LDAP_GROUP_OU_DN`` kan sættes; default er ``OU=SOC,OU=Service Areas,DC=example,DC=org``

Miljøvariabler
--------------
=============================  ==========================  =========================================
Variabel                       Default                     beskrivelse
=============================  ==========================  =========================================
LDAP_GROUP_OU_DN               OU=SOC,OU=Service Areas,...  OU-DN at søge grupper under
LDAP_GROUP_FILTER              (objectCategory=group)      LDAP-filter for grupper
LDAP_GROUP_PAGE_SIZE           200                         RFC 2696 paged result-størrelse (grupper)
LDAP_MEMBER_PAGE_SIZE          200                         RFC 2696 paged result-størrelse (members)
LDAP_OUTPUT_JSONL              scripts/output/ad_grupper.jsonl  Sti til JSONL-outputfil
LDAP_OUTPUT_TXT                scripts/output/ad_grupper.txt    Sti til tekst-outputfil
LDAP_SERVER                    ldaps.example.org           LDAP-serverens hostname
LDAP_PORT                      636                         TCP-port (636 = LDAPS)
LDAP_BIND_USER                 EXAMPLE\\svc-ldap      Bind-brugernavn (DOMAIN\\user for NTLM)
LDAP_PASSWORD                  *(påkrævet)*                Bind-password
LDAP_USE_SSL                   true                        Brug SSL/TLS fra start
LDAP_AUTH                      NTLM                        Autentificeringsmetode (NTLM | SIMPLE)
=============================  ==========================  =========================================

Kørsel
------
Fra ``etl/``::

    # Med defaults (SOC-OU, output til scripts/output/ad_grupper.jsonl + .txt):
    python scripts/export_ad_groups_members.py

    # Anden OU:
    LDAP_GROUP_OU_DN="OU=Organisational,OU=REG,OU=Groups,OU=Identity,DC=example,DC=org" \\
        python scripts/export_ad_groups_members.py

    # Custom output-stier:
    LDAP_OUTPUT_JSONL="/tmp/grupper.jsonl" LDAP_OUTPUT_TXT="/tmp/grupper.txt" \\
        python scripts/export_ad_groups_members.py

Output
------
Scriptet skriver altid **to filer** (JSONL + tekst) og printer stier til terminalen.

**JSONL** (``ad_grupper.jsonl``) — én JSON-linje pr. gruppe::

    {"group_dn": "CN=DBA-Admins,OU=...", "group_name": "DBA-Admins",
     "group_sAMAccountName": "DBA-Admins",
     "member_sAMAccountName": ["ahansen", "blarsen"], "member_count_direct": 2}

Felter:

- ``group_dn`` — gruppens fulde Distinguished Name
- ``group_name`` — gruppens ``name``/``cn``-attribut
- ``group_sAMAccountName`` — gruppens korte login-navn
- ``member_sAMAccountName`` — liste af direkte medlemmers sAMAccountName
- ``member_count_direct`` — antal direkte ``member``-DN'er på gruppen

**Tekst** (``ad_grupper.txt``)::

    Fandt 42 grupper under OU=SOC,...

    Gruppe: DBA-Admins
      - ahansen
      - blarsen

    Gruppe: AndenGruppe
      (ingen direkte medlemmer)

Exit codes
----------
- **0** — eksport gennemført
- **1** — forbindelses-, bind- eller søgefejl
- **2** — manglende konfiguration eller OU-DN ikke fundet

Kendte begrænsninger
--------------------
- ``member``-attributten hentes som den er fra AD (max ~1500 DN'er).
  Ranged retrieval (``member;range=0-1499``) er endnu ikke implementeret,
  så meget store grupper kan mangle medlemmer.
- Kun **direkte** medlemmer opløses; nestede gruppemedlemskaber følges ikke.

Se også
-------
- ``test_ldap_connection.py`` — smoke-test af LDAP-forbindelse
- ``scan_ou_inventory.py`` — find OU'er med grupper/brugere
"""

from __future__ import annotations

import json
import os
import ssl
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from ldap3 import ALL, BASE, NTLM, SIMPLE, SUBTREE, Connection, Server, Tls
from ldap3.core.exceptions import (
    LDAPBindError,
    LDAPException,
    LDAPInvalidServerError,
    LDAPSocketOpenError,
    LDAPStartTLSError,
)
from ldap3.extend.standard.PagedSearch import paged_search_generator


def _int_env(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _load_dotenv_repo_root() -> None:
    project_root = Path(__file__).resolve().parent.parent.parent
    for env_file in (
        project_root / ".env",
        project_root / "tests" / "config" / "test.env",
    ):
        if env_file.is_file():
            load_dotenv(env_file, override=True)
            return
    load_dotenv(override=True)


def _env_required(name: str) -> str:
    val = (os.environ.get(name) or "").strip()
    if not val:
        raise ValueError(f"{name} er ikke sat")
    return val


def _connect() -> tuple[Server, Connection]:
    ldap_server = os.environ.get("LDAP_SERVER", "ldaps.example.org")
    ldap_port = int(os.environ.get("LDAP_PORT", "636"))
    ldap_bind_user = os.environ.get("LDAP_BIND_USER", r"EXAMPLE\svc-ldap")
    ldap_password = os.environ.get("LDAP_PASSWORD", "")
    ldap_use_ssl = os.environ.get("LDAP_USE_SSL", "true").lower() == "true"
    ldap_auth = os.environ.get("LDAP_AUTH", "NTLM").upper()

    if not ldap_password:
        raise ValueError("LDAP_PASSWORD er ikke sat")

    tls = Tls(validate=ssl.CERT_NONE)
    auth_method = NTLM if ldap_auth == "NTLM" else SIMPLE

    server = Server(
        ldap_server,
        port=ldap_port,
        use_ssl=ldap_use_ssl,
        get_info=ALL,
        tls=tls,
    )
    # auto_referrals=False: keep output predictable; do not chase referrals to other hosts.
    conn = Connection(
        server,
        user=ldap_bind_user,
        password=ldap_password,
        authentication=auth_method,
        auto_bind=True,
        auto_referrals=False,
    )
    return server, conn


def _probe_base_object(conn: Connection, dn: str) -> dict[str, Any]:
    ok = conn.search(
        search_base=dn,
        search_filter="(objectClass=*)",
        search_scope=BASE,
        attributes=["distinguishedName", "objectClass", "name", "ou", "cn"],
        size_limit=1,
    )
    if not ok or not conn.entries:
        return {
            "found": False,
            "ok": bool(ok),
            "result": conn.result,
            "last_error": conn.last_error,
        }
    e = conn.entries[0]
    obj = {
        "found": True,
        "ok": bool(ok),
        "dn": str(getattr(e, "distinguishedName", "")) or dn,
        "name": str(getattr(e, "name", "")) if getattr(e, "name", None) else None,
        "ou": str(getattr(e, "ou", "")) if getattr(e, "ou", None) else None,
        "cn": str(getattr(e, "cn", "")) if getattr(e, "cn", None) else None,
        "objectClass": list(getattr(e, "objectClass", []) or []),
    }
    return obj


def _extract_first_ou_name(dn: str) -> str | None:
    s = dn.strip()
    if not s.lower().startswith("ou="):
        return None
    first = s.split(",", 1)[0]
    if "=" not in first:
        return None
    _, _, value = first.partition("=")
    value = value.strip()
    return value or None


def _find_ou_dns_by_name(conn: Connection, base_dn: str, ou_name: str, limit: int = 5) -> list[str]:
    found: list[str] = []
    for item in paged_search_generator(
        conn,
        search_base=base_dn,
        search_filter=f"(&(objectClass=organizationalUnit)(ou={ou_name}))",
        search_scope=SUBTREE,
        attributes=["distinguishedName", "ou"],
        paged_size=200,
    ):
        if item.get("type") != "searchResEntry":
            continue
        attrs = item.get("attributes") or {}
        dn = str(item.get("dn") or attrs.get("distinguishedName") or "")
        if dn:
            found.append(dn)
        if len(found) >= limit:
            break
    return found


def _member_samaccountname(conn: Connection, dn: str, cache: dict[str, str | None]) -> str | None:
    if dn in cache:
        return cache[dn]

    try:
        ok = conn.search(
            search_base=dn,
            search_filter="(objectClass=*)",
            search_scope=BASE,
            attributes=["sAMAccountName"],
            size_limit=1,
        )
        if not ok or not conn.entries:
            cache[dn] = None
            return None
        entry = conn.entries[0]
        value = getattr(entry, "sAMAccountName", None)
        sam = str(value) if value else None
        cache[dn] = sam
        return sam
    except LDAPException:
        cache[dn] = None
        return None


def _iter_groups(
    conn: Connection,
    ou_dn: str,
    group_filter: str,
    page_size: int,
) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    for item in paged_search_generator(
        conn,
        search_base=ou_dn,
        search_filter=group_filter,
        search_scope=SUBTREE,
        attributes=["cn", "name", "sAMAccountName", "distinguishedName", "member"],
        paged_size=page_size,
    ):
        if item.get("type") != "searchResEntry":
            continue
        attrs = item.get("attributes") or {}
        dn = str(item.get("dn") or attrs.get("distinguishedName") or "")
        groups.append(
            {
                "dn": dn,
                "name": (
                    (attrs.get("name") or attrs.get("cn") or [""])[0]
                    if isinstance(attrs.get("name") or attrs.get("cn"), list)
                    else (attrs.get("name") or attrs.get("cn") or "")
                ),
                "sAMAccountName": (
                    (attrs.get("sAMAccountName") or [""])[0]
                    if isinstance(attrs.get("sAMAccountName"), list)
                    else (attrs.get("sAMAccountName") or "")
                ),
                "members_dn": list(attrs.get("member") or []),
            }
        )
    return groups


def main() -> int:
    _load_dotenv_repo_root()

    _DEFAULT_GROUP_OU_DN = "OU=SOC,OU=Service Areas,DC=example,DC=org"
    ou_dn = (os.environ.get("LDAP_GROUP_OU_DN") or "").strip() or _DEFAULT_GROUP_OU_DN

    group_filter = (os.environ.get("LDAP_GROUP_FILTER") or "(objectCategory=group)").strip()
    group_page_size = max(1, _int_env("LDAP_GROUP_PAGE_SIZE", 200))

    _script_dir = Path(__file__).resolve().parent
    _default_output_dir = _script_dir / "output"
    output_jsonl = Path(
        os.environ.get("LDAP_OUTPUT_JSONL") or str(_default_output_dir / "ad_grupper.jsonl")
    )
    output_txt = Path(
        os.environ.get("LDAP_OUTPUT_TXT") or str(_default_output_dir / "ad_grupper.txt")
    )

    print(f"OU: {ou_dn}")
    print(f"Group filter: {group_filter}")
    print(f"Group page size: {group_page_size}")
    print(f"Output JSONL: {output_jsonl}")
    print(f"Output TXT:   {output_txt}")
    print("-" * 50)

    try:
        _server, conn = _connect()
    except (
        ValueError,
        LDAPSocketOpenError,
        LDAPStartTLSError,
        LDAPBindError,
        LDAPInvalidServerError,
    ) as e:
        print("FEJL: Oprettelse af forbindelse eller brugerautentificering (bind) fejlede.")
        print(f"Fejlbesked ({type(e).__name__}): {e}")
        return 1
    except LDAPException as e:
        print("FEJL: Oprettelse af forbindelse eller brugerautentificering (bind) fejlede.")
        print(f"Fejlbesked ({type(e).__name__}): {e}")
        return 1

    # Probe: does the base DN exist and what is it?
    probe = _probe_base_object(conn, ou_dn)
    if not probe.get("found"):
        # Provide a clearer, actionable error than "0 groups" when base DN doesn't exist.
        best_match = None
        result = probe.get("result")
        if isinstance(result, dict):
            best_match = result.get("dn") or None
        print("FEJL: OU/base DN blev ikke fundet i AD (noSuchObject).")
        if best_match:
            print(f"Best match: {best_match}")
        if probe.get("last_error"):
            print(f"Detalje: {probe['last_error']}")

        ou_name = _extract_first_ou_name(ou_dn)
        candidates: list[str] = []
        if best_match and ou_name:
            try:
                candidates = _find_ou_dns_by_name(conn, best_match, ou_name, limit=5)
            except LDAPException:
                candidates = []
        if candidates:
            print("\nMulige DN'er for OU (fundet ved søgning på OU-navn):")
            for dn in candidates:
                print(f"  - {dn}")
        elif best_match and ou_name:
            print("\nIngen OU'er med det navn fundet under 'Best match'.")

        try:
            conn.unbind()
        except LDAPException:
            pass
        return 2

    try:
        groups = _iter_groups(
            conn, ou_dn=ou_dn, group_filter=group_filter, page_size=group_page_size
        )
    except LDAPException as e:
        print("FEJL: Group search fejlede.")
        print(f"Fejlbesked ({type(e).__name__}): {e}")
        try:
            conn.unbind()
        except LDAPException:
            pass
        return 1

    member_cache: dict[str, str | None] = {}

    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    output_txt.parent.mkdir(parents=True, exist_ok=True)

    sorted_groups = sorted(groups, key=lambda x: str(x.get("dn", "")).casefold())

    with (
        open(output_jsonl, "w", encoding="utf-8") as fj,
        open(output_txt, "w", encoding="utf-8") as ft,
    ):
        ft.write(f"Fandt {len(groups)} grupper under {ou_dn}\n")
        for g in sorted_groups:
            members_dn = [str(d) for d in (g.get("members_dn") or [])]
            members_sam = [
                sam
                for sam in (_member_samaccountname(conn, dn, member_cache) for dn in members_dn)
                if sam
            ]
            group_label = g.get("name") or g.get("sAMAccountName") or g.get("dn") or "<ukendt>"

            fj.write(
                json.dumps(
                    {
                        "group_dn": g.get("dn"),
                        "group_name": g.get("name"),
                        "group_sAMAccountName": g.get("sAMAccountName"),
                        "member_sAMAccountName": members_sam,
                        "member_count_direct": len(members_dn),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )

            ft.write(f"\nGruppe: {group_label}\n")
            if not members_dn:
                ft.write("  (ingen direkte medlemmer)\n")
            else:
                for sam in members_sam:
                    ft.write(f"  - {sam}\n")
                unresolved = len(members_dn) - len(members_sam)
                if unresolved > 0:
                    ft.write(f"  ({unresolved} medlemmer kunne ikke opløses)\n")

    print(f"Fandt {len(groups)} grupper.")
    print(f"Skrevet JSONL: {output_jsonl}")
    print(f"Skrevet TXT:   {output_txt}")

    try:
        conn.unbind()
    except LDAPException as e:
        print("FEJL: Afslutning af forbindelsen (unbind) fejlede.")
        print(f"Fejlbesked ({type(e).__name__}): {e}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

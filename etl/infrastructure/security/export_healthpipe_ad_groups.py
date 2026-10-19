#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Eksportér HealthPipe AD-grupper (LSSAPP/GSCAPP) og map til sec.RBOrganisation, sec.RolleDomaene og de historiserede sec-tabeller.

Formål
------
Finder alle LSSAPP/GSCAPP-grupper under HealthPipe-OU'en i Active Directory,
parser SOR-koder og domæneroller fra gruppenavnene, opløser medlemmer til
``sAMAccountName``, og producerer output klar til indlæsning i DW.

Understøttede gruppetyper
-------------------------
1. **Org-grupper** → ``sec.RBOrganisation`` (via ``load_healthpipe_ad_to_mssql.py``)::

       LSSAPP_HP_Org_<SOR-kode>   (Local Security — parent)
       GSCAPP_HP_Org_<SOR-kode>   (Global Security — child, tilmeldt via IdM)

   Brugere opløses fra GSCAPP (foretrukket) eller LSSAPP (fallback).
   "Org" indikerer ``GivAdgangTilUnderorganisationer = 1``.

2. **Domaenerolle-grupper** → ``sec.RolleDomaene`` (via ``load_healthpipe_ad_to_mssql.py``)::

       LSSAPP_HP_Domaenerolle_<DPA-domæne>_<rolle>
       LSSAPP_HP_Domaenerolle_<DPA-domæne>_<sygehus>_P

   Rolle: ``U`` = Ubegrænset, ``K`` = Kolonnebegrænset, ``P`` = Kolonnebegrænset med
   populationsansvar for sygehuset (``SHS``, ``EGS``, ``SLB``, ``OUH``) i domænet.
   En P-gruppe med et ukendt sygehus springes over med en advarsel.

Gruppestruktur i AD
-------------------
Alle grupper ligger under::

    OU=HealthPipe,OU=Application,OU=Local Security,
    OU=Cross-Org,OU=Groups,OU=ORG,
    OU=Service Areas,DC=example,DC=org

Forudsætninger
--------------
- VPN aktivt (ldaps.example.org er intern)
- Python-pakker: ``ldap3``, ``pycryptodome``, ``python-dotenv``
- LDAP_* miljøvariabler sat i ``.env`` (repo-root)

Kørsel
------
Fra ``etl/``::

    python infrastructure/security/export_healthpipe_ad_groups.py

Output
------
**Org-grupper:**

- ``healthpipe_grupper.jsonl`` — én linje pr. (bruger, SOR-kode)
- ``healthpipe_grupper.txt`` — menneskelæselig oversigt

**Domaenerolle-grupper:**

- ``healthpipe_roller.jsonl`` — én linje pr. (bruger, domæne, rolle, populationsansvar)

**Fejlede medlemsopslag** (læses af ``load_healthpipe_ad_to_mssql.py``):

- ``healthpipe_fejlede_opslag.jsonl`` — én linje pr. medlems-DN, hvis opslag fejlede; loaderen
  lukker ingen rækker i historiktabellerne, når filen ikke er tom

Brug ``load_healthpipe_ad_to_mssql.py`` til at indlæse JSONL-data i MSSQL.

Exit codes
----------
- **0** — eksport gennemført
- **1** — forbindelses-, bind- eller søgefejl
- **2** — manglende konfiguration eller OU-DN ikke fundet

Se også
-------
- ``etl/scripts/export_ad_groups_members.py`` — generisk v1-eksport (vilkårlig OU)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import ssl
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from DSA.SQL.ref.kommune_to_populationsansvar import POPULATIONSANSVAR_BY_KOMMUNEKODE
from ldap3 import ALL, BASE, NTLM, SIMPLE, SUBTREE, Connection, Server, Tls
from ldap3.core.exceptions import (
    LDAPBindError,
    LDAPException,
    LDAPInvalidServerError,
    LDAPSocketOpenError,
    LDAPStartTLSError,
)
from ldap3.extend.standard.PagedSearch import paged_search_generator

_DEFAULT_OU_DN = (
    "OU=HealthPipe,OU=Application,OU=Local Security,"
    "OU=Cross-Org,OU=Groups,OU=ORG,"
    "OU=Service Areas,DC=example,DC=org"
)

_ORG_GROUP_RE = re.compile(r"^(?P<prefix>LSSAPP|GSCAPP)_HP_(?P<type>Org)_(?P<sor_code>\d+)$")

_ROLE_GROUP_RE = re.compile(
    r"^(?P<prefix>LSSAPP|GSCAPP)_HP_Domaenerolle_(?P<domain>DPA_\w+?)"
    r"(?:_(?P<hospital>[^\W_]+)(?=_P$))?_(?P<role>[UKP])$"
)

_HOSPITALS = frozenset(POPULATIONSANSVAR_BY_KOMMUNEKODE.values())


@dataclass
class ParsedOrgGroup:
    """A HealthPipe Org AD group with parsed metadata."""

    dn: str
    name: str
    sam_account_name: str
    prefix: str  # LSSAPP or GSCAPP
    access_type: str  # "Org"
    sor_code: str
    members_dn: list[str] = field(default_factory=list)


@dataclass
class ParsedRoleGroup:
    """A HealthPipe Domaenerolle AD group with parsed metadata."""

    dn: str
    name: str
    sam_account_name: str
    prefix: str  # LSSAPP or GSCAPP
    domain: str  # e.g. "DPA_TIP", "DPA_Socialdata"
    role: str  # "U" (Ubegrænset), "K" (Kolonnebegrænset) or "P" (populationsansvar)
    populationsansvar: str  # hospital for "P", else ""
    members_dn: list[str] = field(default_factory=list)


@dataclass
class SorGroupPair:
    """Matched LSSAPP + GSCAPP pair for a single SOR code."""

    sor_code: str
    access_type: str
    lssapp: ParsedOrgGroup | None = None
    gscapp: ParsedOrgGroup | None = None


# ---------------------------------------------------------------------------
# Environment helpers (shared with v1)
# ---------------------------------------------------------------------------


def _int_env(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


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


# ---------------------------------------------------------------------------
# LDAP connection (shared with v1)
# ---------------------------------------------------------------------------


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
    conn = Connection(
        server,
        user=ldap_bind_user,
        password=ldap_password,
        authentication=auth_method,
        auto_bind=True,
        auto_referrals=False,
    )
    return server, conn


# ---------------------------------------------------------------------------
# LDAP search helpers (shared with v1)
# ---------------------------------------------------------------------------


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
    return {
        "found": True,
        "ok": bool(ok),
        "dn": str(getattr(e, "distinguishedName", "")) or dn,
        "name": str(getattr(e, "name", "")) if getattr(e, "name", None) else None,
        "ou": str(getattr(e, "ou", "")) if getattr(e, "ou", None) else None,
        "cn": str(getattr(e, "cn", "")) if getattr(e, "cn", None) else None,
        "objectClass": list(getattr(e, "objectClass", []) or []),
    }


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


@dataclass
class ResolvedMember:
    sam_account_name: str
    is_group: bool
    members_dn: list[str] = field(default_factory=list)


class MemberCache(dict[str, ResolvedMember | None]):
    """DN -> resolved member; ``failed`` holds the DNs whose LDAP search failed."""

    def __init__(self) -> None:
        super().__init__()
        self.failed: set[str] = set()


def _resolve_member(conn: Connection, dn: str, cache: MemberCache) -> ResolvedMember | None:
    if dn in cache:
        return cache[dn]
    try:
        ok = conn.search(
            search_base=dn,
            search_filter="(objectClass=*)",
            search_scope=BASE,
            attributes=["sAMAccountName", "objectCategory", "member"],
            size_limit=1,
        )
        if not ok or not conn.entries:
            if not ok and (conn.result or {}).get("description") not in (
                "noSuchObject",
                "referral",
            ):
                cache.failed.add(dn)
            cache[dn] = None
            return None
        entry = conn.entries[0]
        sam_val = getattr(entry, "sAMAccountName", None)
        sam = str(sam_val) if sam_val else None
        if not sam:
            cache[dn] = None
            return None
        cat_val = str(getattr(entry, "objectCategory", "") or "").lower()
        is_group = "group" in cat_val
        members_dn = list(getattr(entry, "member", []) or [])
        result = ResolvedMember(sam_account_name=sam, is_group=is_group, members_dn=members_dn)
        cache[dn] = result
        return result
    except LDAPException:
        cache.failed.add(dn)
        cache[dn] = None
        return None


_MAX_NEST_DEPTH = 5


def _resolve_users_recursive(
    conn: Connection,
    member_dns: list[str],
    cache: MemberCache,
    warnings: list[str],
    context: str,
    _depth: int = 0,
) -> list[str]:
    """Resolve a list of member DNs to sAMAccountNames, recursing into nested groups.

    Returns a list of user sAMAccountNames (no duplicates within a single call).
    """
    if _depth > _MAX_NEST_DEPTH:
        warnings.append(f"{context}: max nested group depth ({_MAX_NEST_DEPTH}) exceeded")
        return []

    users: list[str] = []
    seen: set[str] = set()

    for member_dn in member_dns:
        resolved = _resolve_member(conn, member_dn, cache)
        if not resolved:
            warnings.append(f"{context}: kunne ikke opløse member DN: {member_dn}")
            continue
        if not resolved.is_group:
            if resolved.sam_account_name.lower() not in seen:
                seen.add(resolved.sam_account_name.lower())
                users.append(resolved.sam_account_name)
        elif resolved.members_dn:
            nested = _resolve_users_recursive(
                conn, resolved.members_dn, cache, warnings, context, _depth + 1
            )
            for u in nested:
                if u.lower() not in seen:
                    seen.add(u.lower())
                    users.append(u)

    return users


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


# ---------------------------------------------------------------------------
# v2-specific: parse, pair, resolve
# ---------------------------------------------------------------------------


def _parse_groups(
    raw_groups: list[dict[str, Any]],
) -> tuple[list[ParsedOrgGroup], list[ParsedRoleGroup], list[str], list[str]]:
    """Classify all raw groups into Org, Role or skipped, plus warnings for skipped P groups."""
    org_groups: list[ParsedOrgGroup] = []
    role_groups: list[ParsedRoleGroup] = []
    skipped: list[str] = []
    warnings: list[str] = []

    for raw in raw_groups:
        name = raw.get("sAMAccountName") or raw.get("name") or ""
        members = list(raw.get("members_dn") or [])

        m_org = _ORG_GROUP_RE.match(name)
        if m_org:
            org_groups.append(
                ParsedOrgGroup(
                    dn=raw["dn"],
                    name=raw.get("name") or name,
                    sam_account_name=name,
                    prefix=m_org.group("prefix"),
                    access_type=m_org.group("type"),
                    sor_code=m_org.group("sor_code"),
                    members_dn=members,
                )
            )
            continue

        m_role = _ROLE_GROUP_RE.match(name)
        if m_role:
            hospital = m_role.group("hospital") or ""
            if m_role.group("role") == "P" and hospital not in _HOSPITALS:
                warnings.append(f"{name}: ukendt sygehus {hospital!r}, gruppen springes over")
                skipped.append(name)
                continue
            role_groups.append(
                ParsedRoleGroup(
                    dn=raw["dn"],
                    name=raw.get("name") or name,
                    sam_account_name=name,
                    prefix=m_role.group("prefix"),
                    domain=m_role.group("domain"),
                    role=m_role.group("role"),
                    populationsansvar=hospital,
                    members_dn=members,
                )
            )
            continue

        skipped.append(name or raw.get("dn", ""))

    return org_groups, role_groups, skipped, warnings


def _pair_org_groups(parsed: list[ParsedOrgGroup]) -> dict[str, SorGroupPair]:
    """Group LSSAPP/GSCAPP by SOR code into matched pairs."""
    pairs: dict[str, SorGroupPair] = {}
    for g in parsed:
        key = g.sor_code
        if key not in pairs:
            pairs[key] = SorGroupPair(sor_code=g.sor_code, access_type=g.access_type)
        pair = pairs[key]
        if g.prefix == "LSSAPP":
            pair.lssapp = g
        elif g.prefix == "GSCAPP":
            pair.gscapp = g
    return pairs


@dataclass
class UserOrgMapping:
    """One row destined for sec.RBOrganisation (via load_healthpipe_ad_to_mssql.py)."""

    bruger: str
    code_sor: str
    giv_adgang_til_underorganisationer: bool
    beskrivelse: str
    source_lssapp_group: str
    source_gscapp_group: str
    member_source: str  # "GSCAPP" or "LSSAPP (fallback)"


@dataclass
class UserRoleMapping:
    """One row destined for sec.RolleDomaene (via load_healthpipe_ad_to_mssql.py)."""

    bruger: str
    domain: str  # e.g. "DPA_TIP"
    rolle: str
    populationsansvar: str
    source_group: str


def _resolve_org_pairs(
    conn: Connection,
    pairs: dict[str, SorGroupPair],
    cache: MemberCache,
    verbose: bool = False,
) -> tuple[list[UserOrgMapping], list[str]]:
    """Resolve members to sAMAccountName and build UserOrgMapping rows.

    Prefers GSCAPP members; falls back to LSSAPP direct members.
    Recursively resolves nested groups to find actual user accounts.
    """
    mappings: list[UserOrgMapping] = []
    warnings: list[str] = []

    for sor_code in sorted(pairs):
        pair = pairs[sor_code]
        lssapp_name = pair.lssapp.sam_account_name if pair.lssapp else ""
        gives_sub_orgs = pair.access_type.lower() == "org"

        if verbose:
            lssapp_members = len(pair.lssapp.members_dn) if pair.lssapp else 0
            gscapp_members = len(pair.gscapp.members_dn) if pair.gscapp else 0
            print(
                f"  [VERBOSE] SOR {sor_code}: "
                f"LSSAPP={'ja' if pair.lssapp else 'nej'} "
                f"({lssapp_members} member DNs), "
                f"GSCAPP={'ja' if pair.gscapp else 'nej'} "
                f"({gscapp_members} member DNs)"
            )

        if pair.gscapp:
            source_group = pair.gscapp
            source_label = "GSCAPP"
            gscapp_name = pair.gscapp.sam_account_name
        elif pair.lssapp and pair.lssapp.members_dn:
            source_group = pair.lssapp
            source_label = "LSSAPP (fallback)"
            gscapp_name = ""
        else:
            if not pair.lssapp:
                warnings.append(f"SOR {sor_code}: GSCAPP findes men ingen matchende LSSAPP")
            else:
                warnings.append(
                    f"SOR {sor_code}: kun LSSAPP, ingen GSCAPP, ingen direkte medlemmer"
                )
            continue

        if not pair.lssapp:
            warnings.append(f"SOR {sor_code}: GSCAPP findes men ingen matchende LSSAPP")

        users = _resolve_users_recursive(
            conn, source_group.members_dn, cache, warnings, f"SOR {sor_code}"
        )
        if verbose and not users:
            print(
                f"  [VERBOSE] SOR {sor_code}: 0 brugere resolved "
                f"fra {source_label} ({len(source_group.members_dn)} member DNs)"
            )
            for mdn in source_group.members_dn[:3]:
                resolved = cache.get(mdn)
                if resolved is None:
                    print(f"    member DN: {mdn} → (ukendt/fejl)")
                elif resolved.is_group:
                    print(
                        f"    member DN: {mdn} → gruppe "
                        f"'{resolved.sam_account_name}' "
                        f"({len(resolved.members_dn)} sub-members)"
                    )
                else:
                    print(f"    member DN: {mdn} → bruger " f"'{resolved.sam_account_name}'")
        for bruger in users:
            mappings.append(
                UserOrgMapping(
                    bruger=bruger,
                    code_sor=sor_code,
                    giv_adgang_til_underorganisationer=gives_sub_orgs,
                    beskrivelse=lssapp_name or gscapp_name,
                    source_lssapp_group=lssapp_name,
                    source_gscapp_group=gscapp_name,
                    member_source=source_label,
                )
            )

    return mappings, warnings


def _resolve_role_groups(
    conn: Connection,
    role_groups: list[ParsedRoleGroup],
    cache: MemberCache,
    verbose: bool = False,
) -> tuple[list[UserRoleMapping], list[str]]:
    """Resolve Domaenerolle group members into UserRoleMapping rows.

    Recursively resolves nested groups to find actual user accounts.
    """
    mappings: list[UserRoleMapping] = []
    warnings: list[str] = []

    for rg in sorted(role_groups, key=lambda g: (g.domain, g.role, g.populationsansvar)):
        label = "_".join(filter(None, (rg.domain, rg.populationsansvar, rg.role)))
        if verbose:
            print(f"  [VERBOSE] Rolle {label} ({rg.prefix}): {len(rg.members_dn)} member DNs")
        if not rg.members_dn:
            continue
        ctx = f"Rolle {label}"
        users = _resolve_users_recursive(conn, rg.members_dn, cache, warnings, ctx)
        if verbose:
            print(f"    → {len(users)} brugere resolved")
        for bruger in users:
            mappings.append(
                UserRoleMapping(
                    bruger=bruger,
                    domain=rg.domain,
                    rolle=rg.role,
                    populationsansvar=rg.populationsansvar,
                    source_group=rg.sam_account_name,
                )
            )

    return mappings, warnings


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------


def _write_org_jsonl(path: Path, mappings: list[UserOrgMapping]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for m in mappings:
            f.write(
                json.dumps(
                    {
                        "bruger": m.bruger,
                        "code_sor": m.code_sor,
                        "giv_adgang_til_underorganisationer": m.giv_adgang_til_underorganisationer,
                        "beskrivelse": m.beskrivelse,
                        "source_lssapp_group": m.source_lssapp_group,
                        "source_gscapp_group": m.source_gscapp_group,
                        "member_source": m.member_source,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )


def _write_role_jsonl(path: Path, mappings: list[UserRoleMapping]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for m in mappings:
            f.write(
                json.dumps(
                    {
                        "bruger": m.bruger,
                        "domain": m.domain,
                        "rolle": m.rolle,
                        "populationsansvar": m.populationsansvar,
                        "source_group": m.source_group,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )


def _write_rows_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _write_txt(
    path: Path,
    pairs: dict[str, SorGroupPair],
    org_mappings: list[UserOrgMapping],
    role_groups: list[ParsedRoleGroup],
    role_mappings: list[UserRoleMapping],
    warnings: list[str],
    ou_dn: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    by_sor: dict[str, list[tuple[str, str]]] = {}
    for m in org_mappings:
        by_sor.setdefault(m.code_sor, []).append((m.bruger, m.member_source))

    with open(path, "w", encoding="utf-8") as f:
        f.write(f"HealthPipe AD-gruppeeksport — {date.today().isoformat()}\n")
        f.write(f"OU: {ou_dn}\n")
        f.write(f"antal SOR-koder (Org): {len(pairs)}\n")
        f.write(f"antal unikke (bruger, SOR) mappings: {len(org_mappings)}\n")
        f.write(f"antal Domaenerolle-grupper: {len(role_groups)}\n")
        f.write(f"antal unikke (bruger, domæne) roller: {len(role_mappings)}\n")

        # --- Org-grupper ---
        f.write("\n" + "=" * 70 + "\n")
        f.write("ORG-GRUPPER (sec.RBOrganisation)\n")
        f.write("=" * 70 + "\n")

        for sor_code in sorted(pairs):
            pair = pairs[sor_code]
            lssapp_label = pair.lssapp.sam_account_name if pair.lssapp else "(mangler)"
            gscapp_label = pair.gscapp.sam_account_name if pair.gscapp else "(mangler)"
            entries = by_sor.get(sor_code, [])
            users = sorted(entries, key=lambda x: x[0].casefold())

            f.write(f"\nSOR: {sor_code}\n")
            f.write(f"  LSSAPP: {lssapp_label}\n")
            f.write(f"  GSCAPP: {gscapp_label}\n")
            f.write(f"  Adgangstype: {pair.access_type}")
            if pair.access_type.lower() == "org":
                f.write(" (inkl. underorganisationer)")
            f.write("\n")
            f.write(f"  Medlemmer ({len(users)}):\n")
            if users:
                for u, src in users:
                    suffix = f"  [{src}]" if src != "GSCAPP" else ""
                    f.write(f"    - {u}{suffix}\n")
            else:
                f.write("    (ingen medlemmer)\n")

        # --- Domaenerolle-grupper ---
        f.write("\n" + "=" * 70 + "\n")
        f.write("DOMAENEROLLE-GRUPPER (sec.RolleDomaene)\n")
        f.write("=" * 70 + "\n")

        by_domain_role: dict[tuple[str, str, str], list[str]] = {}
        for m in role_mappings:
            by_domain_role.setdefault((m.domain, m.rolle, m.populationsansvar), []).append(m.bruger)

        role_labels = {"U": "Ubegrænset", "K": "Kolonnebegrænset", "P": "Populationsansvar"}
        for domain, role, pop in sorted(by_domain_role):
            users = sorted(by_domain_role[(domain, role, pop)], key=str.casefold)
            label = " ".join(filter(None, (role_labels.get(role, role), pop)))
            f.write(f"\n{domain} — {label} ({role})\n")
            f.write(f"  Medlemmer ({len(users)}):\n")
            for u in users:
                f.write(f"    - {u}\n")

        # Vis roller uden medlemmer
        seen_domain_roles = set(by_domain_role.keys())
        empty_roles = [
            rg
            for rg in role_groups
            if (rg.domain, rg.role, rg.populationsansvar) not in seen_domain_roles
        ]
        if empty_roles:
            for rg in sorted(empty_roles, key=lambda g: (g.domain, g.role, g.populationsansvar)):
                label = " ".join(
                    filter(None, (role_labels.get(rg.role, rg.role), rg.populationsansvar))
                )
                f.write(f"\n{rg.domain} — {label} ({rg.role})\n")
                f.write("  Medlemmer (0):\n")
                f.write("    (ingen medlemmer)\n")

        if warnings:
            f.write("\n" + "=" * 70 + "\n")
            f.write(f"Advarsler ({len(warnings)}):\n")
            for w in warnings:
                f.write(f"  ! {w}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _read_rows(path: Path) -> list[dict]:
    rows: list[dict] = []
    if not path.is_file():
        return rows
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def run_export(verbose: bool = False) -> dict:
    _load_dotenv_repo_root()

    ou_dn = (os.environ.get("LDAP_HP_OU_DN") or "").strip() or _DEFAULT_OU_DN
    group_filter = (os.environ.get("LDAP_GROUP_FILTER") or "(objectCategory=group)").strip()
    group_page_size = max(1, _int_env("LDAP_GROUP_PAGE_SIZE", 200))

    script_dir = Path(__file__).resolve().parent
    default_output_dir = script_dir / "output"
    output_jsonl = Path(
        os.environ.get("LDAP_HP_OUTPUT_JSONL")
        or str(default_output_dir / "healthpipe_grupper.jsonl")
    )
    output_txt = Path(
        os.environ.get("LDAP_HP_OUTPUT_TXT") or str(default_output_dir / "healthpipe_grupper.txt")
    )

    output_role_jsonl = Path(
        os.environ.get("LDAP_HP_OUTPUT_ROLE_JSONL")
        or str(default_output_dir / "healthpipe_roller.jsonl")
    )

    output_fejlede_jsonl = Path(
        os.environ.get("LDAP_HP_OUTPUT_FEJLEDE_OPSLAG_JSONL")
        or str(default_output_dir / "healthpipe_fejlede_opslag.jsonl")
    )

    print(f"OU: {ou_dn}")
    print(f"Group filter: {group_filter}")
    print(f"Group page size: {group_page_size}")
    print(f"Output Org JSONL:    {output_jsonl}")
    print(f"Output Role JSONL:   {output_role_jsonl}")
    print(f"Output Fejlede opslag JSONL: {output_fejlede_jsonl}")
    print(f"Output TXT:          {output_txt}")
    if verbose:
        print("Mode: VERBOSE")
    print("-" * 60)

    # -- Connect ---------------------------------------------------------------
    try:
        _server, conn = _connect()
    except (
        ValueError,
        LDAPSocketOpenError,
        LDAPStartTLSError,
        LDAPBindError,
        LDAPInvalidServerError,
    ) as e:
        print("FEJL: Forbindelse eller bind fejlede.")
        print(f"  ({type(e).__name__}): {e}")
        raise RuntimeError("Forbindelse eller bind fejlede.") from e
    except LDAPException as e:
        print("FEJL: Forbindelse eller bind fejlede.")
        print(f"  ({type(e).__name__}): {e}")
        raise RuntimeError("Forbindelse eller bind fejlede.") from e

    # -- Probe base DN ---------------------------------------------------------
    probe = _probe_base_object(conn, ou_dn)
    if not probe.get("found"):
        best_match = None
        result = probe.get("result")
        if isinstance(result, dict):
            best_match = result.get("dn") or None
        print("FEJL: OU/base DN blev ikke fundet i AD (noSuchObject).")
        if best_match:
            print(f"  Best match: {best_match}")
        if probe.get("last_error"):
            print(f"  Detalje: {probe['last_error']}")

        ou_name = _extract_first_ou_name(ou_dn)
        candidates: list[str] = []
        if best_match and ou_name:
            try:
                candidates = _find_ou_dns_by_name(conn, best_match, ou_name, limit=5)
            except LDAPException:
                candidates = []
        if candidates:
            print("\nMulige DN'er for OU:")
            for dn in candidates:
                print(f"  - {dn}")

        try:
            conn.unbind()
        except LDAPException:
            pass
        raise RuntimeError("OU/base DN blev ikke fundet i AD (noSuchObject).")

    # -- Search groups ---------------------------------------------------------
    print("Søger grupper...")
    try:
        raw_groups = _iter_groups(
            conn, ou_dn=ou_dn, group_filter=group_filter, page_size=group_page_size
        )
    except LDAPException as e:
        print("FEJL: Group search fejlede.")
        print(f"  ({type(e).__name__}): {e}")
        try:
            conn.unbind()
        except LDAPException:
            pass
        raise RuntimeError("Group search fejlede.") from e

    print(f"Fandt {len(raw_groups)} grupper totalt under OU.")

    # -- Parse & classify ------------------------------------------------------
    org_groups, role_groups, skipped, parse_warnings = _parse_groups(raw_groups)

    org_lssapp = sum(1 for g in org_groups if g.prefix == "LSSAPP")
    org_gscapp = sum(1 for g in org_groups if g.prefix == "GSCAPP")
    print(f"  Org-grupper:          LSSAPP={org_lssapp}, GSCAPP={org_gscapp}")
    print(f"  Domaenerolle-grupper: {len(role_groups)}")
    if skipped:
        print(f"  Skippet:             {len(skipped)}")
        for s in skipped[:5]:
            print(f"    (skippet: {s})")
        if len(skipped) > 5:
            print(f"    ... og {len(skipped) - 5} flere")

    # -- Pair Org LSSAPP/GSCAPP by SOR code ------------------------------------
    pairs = _pair_org_groups(org_groups)
    print(f"Unikke SOR-koder: {len(pairs)}")

    # -- Resolve members -------------------------------------------------------
    member_cache: MemberCache = MemberCache()

    print("Opløser Org-gruppemedlemmer (GSCAPP foretrukket, LSSAPP fallback)...")
    org_mappings, org_warnings = _resolve_org_pairs(conn, pairs, member_cache, verbose=verbose)
    fallback_count = sum(1 for m in org_mappings if m.member_source != "GSCAPP")
    print(f"  Org-mappings (bruger → SOR): {len(org_mappings)}")
    if fallback_count:
        print(f"    heraf {fallback_count} fra LSSAPP (fallback)")

    print("Opløser Domaenerolle-medlemmer...")
    role_mappings, role_warnings = _resolve_role_groups(
        conn, role_groups, member_cache, verbose=verbose
    )
    print(f"  Rolle-mappings (bruger → domæne): {len(role_mappings)}")

    if member_cache.failed:
        print(f"  ADVARSEL: {len(member_cache.failed)} medlemsopslag fejlede")

    all_warnings = parse_warnings + org_warnings + role_warnings

    # -- Write output ----------------------------------------------------------
    _write_org_jsonl(output_jsonl, org_mappings)
    _write_role_jsonl(output_role_jsonl, role_mappings)
    _write_rows_jsonl(output_fejlede_jsonl, [{"dn": dn} for dn in sorted(member_cache.failed)])
    _write_txt(
        output_txt,
        pairs,
        org_mappings,
        role_groups,
        role_mappings,
        all_warnings,
        ou_dn,
    )

    print("-" * 60)
    print(f"Skrevet Org JSONL:    {output_jsonl}")
    print(f"Skrevet Role JSONL:   {output_role_jsonl}")
    print(f"Skrevet Fejlede opslag JSONL: {output_fejlede_jsonl}")
    print(f"Skrevet TXT:          {output_txt}")

    if all_warnings:
        print(f"\nAdvarsler ({len(all_warnings)}):")
        for w in all_warnings:
            print(f"  ! {w}")

    # -- Disconnect ------------------------------------------------------------
    try:
        conn.unbind()
    except LDAPException as e:
        print(f"FEJL: Unbind fejlede ({type(e).__name__}): {e}")
        raise RuntimeError(f"Unbind fejlede ({type(e).__name__}): {e}") from e

    return {
        "org": _read_rows(output_jsonl),
        "roller": _read_rows(output_role_jsonl),
        "fejlede_opslag": [r["dn"] for r in _read_rows(output_fejlede_jsonl)],
        "advarsler": parse_warnings,
        "source": {"user": conn.user, "host": conn.server.host, "table": ou_dn},
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Eksportér HealthPipe AD-grupper til JSONL/TXT/SQL"
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Vis detaljeret member-resolution info for debugging",
    )
    args = parser.parse_args()
    try:
        run_export(verbose=args.verbose)
    except Exception as e:  # noqa: BLE001
        print(f"FEJL: {e}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

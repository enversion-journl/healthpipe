<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# LDAP & Active Directory — Guide til scripts

## 1. Hvad er Active Directory og LDAP?

### Active Directory (AD)

Active Directory er Microsofts katalog-tjeneste til Windows-domæner. Det fungerer
som en central, hierarkisk database over organisationens brugere, grupper, computere,
printere m.m. Tænk på det som en kæmpestor telefonbog for hele infrastrukturen.

### LDAP (Lightweight Directory Access Protocol)

LDAP er den **protokol** man bruger til at tale med AD (og andre katalog-servere).
Den er standardiseret i RFC 4511 og kører over TCP/IP.

LDAP definerer et lille sæt operationer:

| Operation  | beskrivelse                                              |
|------------|----------------------------------------------------------|
| **Bind**   | Autentificering (log ind)                                |
| **Search** | Søg entries med filter og scope under en base-DN         |
| **Add**    | Opret ny entry                                           |
| **Modify** | Ændr attributter på en entry                             |
| **Delete** | Slet en entry                                            |
| **Unbind** | Luk forbindelsen                                         |

Vores scripts bruger kun **Bind**, **Search** og **Unbind**.

### Træstrukturen (DIT — Directory Information Tree)

Data i AD er organiseret som et træ. Hvert punkt i træet er en **entry** med
en unik **Distinguished Name (DN)**:

```
DC=Rsyd,DC=net                               ← domæne-rod
├── OU=Users                                  ← organisatorisk enhed
│   ├── OU=SydID
│   │   ├── OU=REG
│   │   │   └── CN=Anders Hansen             ← bruger
│   │   └── OU=OUH
│   └── OU=SydID (TestOU)
├── OU=Groups
│   └── OU=SydID
│       ├── OU=Organisational
│       │   └── CN=DBA-Admins                ← gruppe
│       └── OU=Cross-Org
├── OU=Service Areas
│   ├── OU=SOC                               ← grupper + brugere
│   └── OU=REG
└── OU=Server Administration
    └── OU=Standard
        └── OU=SQL
            └── CN=SQLPROD01                  ← computer
```

### Vigtige begreber

| Begreb              | Forkortelse | Eksempel                                       | Forklaring                                            |
|---------------------|-------------|-------------------------------------------------|-------------------------------------------------------|
| Domain Component    | DC          | `DC=Rsyd,DC=net`                                | Domæne-delen af DN                                    |
| Organizational Unit | OU          | `OU=Users,OU=SydID,DC=Rsyd,DC=net`              | Mappe/container i træet                               |
| Common Name         | CN          | `CN=Anders Hansen`                              | Navn på en leaf-entry (bruger, gruppe, computer)      |
| Distinguished Name  | DN          | `CN=Anders Hansen,OU=REG,OU=Users,...`           | Fuld, unik sti i træet                                |
| sAMAccountName      | —           | `ahansen`                                       | Kort Windows-loginnavn (AD-specifikt)                 |
| objectClass         | —           | `user`, `group`, `organizationalUnit`           | Bestemmer hvilke attributter en entry kan/skal have    |
| objectCategory      | —           | `person`, `group`                               | AD-specifik single-valued indekseret klassifikation    |
| member / memberOf   | —           | Liste af DN'er                                  | Gruppemedlemskab (member på gruppe, memberOf på bruger)|

### Søgning (Search)

En LDAP-søgning har fire parametre:

1. **Base DN** — Hvor i træet søgningen starter
2. **Scope** — Hvor dybt der søges:
   - `BASE`: Kun selve base-DN'en
   - `ONE` (LEVEL): Kun direkte børn
   - `SUBTREE`: Alt under base (rekursivt) — det vores scripts bruger
3. **Filter** — LDAP-filter i præfiks-notation:
   - `(objectCategory=group)` — alle grupper
   - `(&(objectCategory=person)(objectClass=user))` — alle brugere
   - `(objectClass=organizationalUnit)` — alle OU'er
4. **Attributter** — Hvilke felter der returneres (fx `["sAMAccountName", "member"]`)

### Transport og sikkerhed

| Variant  | Port | beskrivelse                    |
|----------|------|--------------------------------|
| LDAP     | 389  | Klar tekst (eller STARTTLS)    |
| **LDAPS**| **636** | **TLS fra start** (det vi bruger) |

### Autentificering

| Metode    | beskrivelse                                                           |
|-----------|-----------------------------------------------------------------------|
| **NTLM**  | Windows challenge/response; password sendes aldrig direkte. Kræver `pycryptodome` for MD4-hash på Python 3.12+. Format: `DOMAIN\brugernavn` |
| Simple    | DN + password (skal være over TLS)                                    |
| Kerberos  | Ticket-baseret; kræver KDC-infrastruktur (ikke tilgængeligt fra container) |

Vores scripts bruger **NTLM** som default.

### Paging (RFC 2696)

AD begrænser som standard resultater til ~1000 entries. For at hente flere
bruger man **paged results control**, hvor man beder om N entries ad gangen
og får en cookie til næste side. Alle vores scripts bruger `paged_search_generator`
fra `ldap3` til dette.

### Referrals

Når man søger bredere end sit domæne, kan AD svare med en **referral** — en
redirect til en anden server. Vores scripts bruger `auto_referrals=False` for
at undgå at følge disse, da det kan give forvirrende fejl i test-scenarier.

---

## 2. Forudsætninger

### Netværk

- **VPN** skal være aktivt — `ldaps.example.org` er en intern server
- Bemærk: Kører du i en **Docker-container/devcontainer**, deles VPN fra
  værtsmaskinen ikke automatisk. Test evt. direkte på værten.

### Python-pakker

```
ldap3
pycryptodome
python-dotenv
```

Disse er allerede i projektets `pyproject.toml`.

### Miljøvariabler (.env)

Alle scripts læser credentials fra `.env` i repo-roden. Påkrævede variabler:

```env
LDAP_SERVER=ldaps.example.org
LDAP_PORT=636
LDAP_BIND_USER=regionsyd\svc-sasldap
LDAP_PASSWORD=<hemmeligt>
LDAP_USE_SSL=true
LDAP_AUTH=NTLM
```

---

## 3. Script 1: `test_ldap_connection.py`

**Placering:** `etl/scripts/test_ldap_connection.py`

### Formål

Smoke-test af LDAP-forbindelsen: binder til AD, og laver en paged
SUBTREE-søgning efter `organizationalUnit`-objekter for at verificere at
netværk, TLS og credentials fungerer. Printer en liste af OU-navne.

### Kørsel

```bash
cd etl
python scripts/test_ldap_connection.py
```

### Script-specifikke miljøvariabler

| Variabel           | Default              | beskrivelse                              |
|--------------------|----------------------|------------------------------------------|
| LDAP_SEARCH_BASE   | *(auto fra DSE)*     | Search base DN; fallback `DC=Rsyd,DC=net`|
| LDAP_OU_LIST_LIMIT | 50                   | antal OU'er vist i output                |
| LDAP_OU_PAGE_SIZE  | 200                  | RFC 2696 paged result-størrelse          |

### Eksempel: success

```
Server:  ldaps.example.org:636
SSL:     True
Auth:    NTLM
bruger:  regionsyd\svc-sasldap
--------------------------------------------------
SUCCESS: LDAP forbindelse OK
Server info: ...
Søgning: base=DC=Rsyd,DC=net (SUBTREE, paged size=200, viser op til 50 OU'er)

antal OU'er (under DC=Rsyd,DC=net): 1234
Første 50 af 1234 (sorteret efter distinguishedName):
  - Exchange
  - Global
  - HR - Personale
  - ...
```

### Eksempel: fejl

```
FEJL: LDAP_PASSWORD er ikke sat. Tjek .env filen.
```

```
FEJL: Oprettelse af forbindelse eller brugerautentificering (bind) fejlede.
Fejlbesked (LDAPSocketOpenError): socket connection error while opening ...
```

### Exit codes

| Code | Betydning                                              |
|------|--------------------------------------------------------|
| 0    | Forbindelse og søgning OK                              |
| 1    | Fejl (manglende password, bind-fejl, søge-fejl)        |

---

## 4. Script 2: `export_ad_groups_members.py`

**Placering:** `etl/scripts/export_ad_groups_members.py`

### Formål

Eksporterer alle AD-grupper under en angivet OU og opløser hvert direkte
`member`-DN til et `sAMAccountName`. Output skrives til stdout som JSONL
(default) eller tekst.

### Kørsel

```bash
cd etl

# JSONL-format (default) — gem til fil:
LDAP_GROUP_OU_DN="OU=SOC,OU=Service Areas,DC=Rsyd,DC=net" \
    python scripts/export_ad_groups_members.py > grupper.jsonl

# Tekst-format:
LDAP_GROUP_OU_DN="OU=SOC,OU=Service Areas,DC=Rsyd,DC=net" \
    LDAP_OUTPUT_FORMAT=text \
    python scripts/export_ad_groups_members.py > grupper.txt
```

### Script-specifikke miljøvariabler

| Variabel              | Default                  | beskrivelse                              |
|-----------------------|--------------------------|------------------------------------------|
| LDAP_GROUP_OU_DN      | *(påkrævet)*             | OU-DN at søge grupper under              |
| LDAP_GROUP_FILTER     | `(objectCategory=group)` | LDAP-filter for grupper                  |
| LDAP_GROUP_PAGE_SIZE  | 200                      | Paged result-størrelse (grupper)         |
| LDAP_MEMBER_PAGE_SIZE | 200                      | Paged result-størrelse (member-opslag)   |
| LDAP_OUTPUT_FORMAT    | jsonl                    | `jsonl` eller `text`                     |

### Output — JSONL

Én JSON-linje pr. gruppe:

```json
{
  "group_dn": "CN=DBA-Admins,OU=SOC,OU=Service Areas,DC=Rsyd,DC=net",
  "group_name": "DBA-Admins",
  "group_sAMAccountName": "DBA-Admins",
  "member_sAMAccountName": ["ahansen", "blarsen"],
  "member_count_direct": 2
}
```

| Felt                    | Type       | beskrivelse                              |
|-------------------------|------------|------------------------------------------|
| `group_dn`              | string     | Gruppens fulde Distinguished Name        |
| `group_name`            | string     | Gruppens `name`/`cn`-attribut            |
| `group_sAMAccountName`  | string     | Gruppens korte login-navn                |
| `member_sAMAccountName` | string[]   | Liste af direkte medlemmers sAMAccountName |
| `member_count_direct`   | int        | antal direkte `member`-DN'er på gruppen  |

### Output — text

```
Fandt 42 grupper.

Gruppe: DBA-Admins
  - ahansen
  - blarsen

Gruppe: Readonly-Users
  (ingen direkte medlemmer)
```

### Sådan finder du den rigtige OU-DN

1. Kør `test_ldap_connection.py` for at se OU-navne i domænet
2. Kør `scan_ou_inventory.py` for at se hvilke OU'er der indeholder grupper
   og/eller brugere (se afsnit 6)
3. Brug den fulde DN fra scan-output som `LDAP_GROUP_OU_DN`

### Exit codes

| Code | Betydning                                              |
|------|--------------------------------------------------------|
| 0    | Eksport gennemført                                     |
| 1    | Forbindelses-, bind- eller søgefejl                    |
| 2    | Manglende konfiguration eller OU-DN ikke fundet        |

### Kendte begrænsninger

- **Ingen ranged member-retrieval:** AD returnerer max ~1500 `member`-DN'er
  pr. gruppe. For meget store grupper kan medlemmer mangle. Ranged retrieval
  (`member;range=0-1499`) er endnu ikke implementeret.
- **Kun direkte medlemmer:** Nestede gruppemedlemskaber følges ikke.

---

## 5. Script 3: `export_sydbi_ad_groups.py`

**Placering:** `etl/infrastructure/security/export_sydbi_ad_groups.py` (flyttet fra `scripts/` så modulet shipper med app-pakken)

### Formål

Specialiseret eksport af SydBI-sikkerhedsgrupper (LSSAPP/GSCAPP) med
automatisk parsing af SOR-koder fra gruppenavne og output klar til
indlæsning i `sec.RolleDomaene` og `sec.RBOrganisation` via
`load_sydbi_ad_to_mssql.py` (TRUNCATE+INSERT).

### Gruppestruktur i AD

Grupperne ligger under:

```
OU=SydBI,OU=Application,OU=Local Security,
OU=Cross-Org,OU=Groups,OU=RSD,
OU=Service Areas,DC=Rsyd,DC=net
```

Navnekonvention:

```
LSSAPP_RSD_SydBI_Org_<SOR-kode>   ← Local Security (parent)
GSCAPP_RSD_SydBI_Org_<SOR-kode>   ← Global Security (child, SydID-tilmelding)
```

Brugere tilmeldes GSCAPP-grupper via SydID og arver automatisk
LSSAPP-medlemskab. Scriptet opløser GSCAPP-medlemmer til `sAMAccountName`
og mapper dem til SOR-koder.

### Kørsel

```bash
cd etl

# Med defaults (SydBI-OU, output til infrastructure/security/output/sydbi_*.jsonl/.txt):
python infrastructure/security/export_sydbi_ad_groups.py

# Anden OU:
LDAP_SYDBI_OU_DN="OU=SydBI,OU=Test,DC=Rsyd,DC=net" \
    python infrastructure/security/export_sydbi_ad_groups.py
```

### Script-specifikke miljøvariabler

| Variabel                  | Default                          | beskrivelse                              |
|---------------------------|----------------------------------|------------------------------------------|
| LDAP_SYDBI_OU_DN          | `OU=SydBI,OU=Application,...`    | OU-DN at søge grupper under              |
| LDAP_GROUP_FILTER         | `(objectCategory=group)`         | LDAP-filter for grupper                  |
| LDAP_GROUP_PAGE_SIZE      | 200                              | Paged result-størrelse                   |
| LDAP_SYDBI_OUTPUT_JSONL   | `infrastructure/security/output/sydbi_grupper.jsonl` | Org JSONL output     |
| LDAP_SYDBI_OUTPUT_TXT     | `infrastructure/security/output/sydbi_grupper.txt`   | Tekst output         |
| LDAP_SYDBI_OUTPUT_ROLE_JSONL | `infrastructure/security/output/sydbi_roller.jsonl` | Domænerolle JSONL output |
| LDAP_SYDBI_OUTPUT_BOPAEL_JSONL | `infrastructure/security/output/sydbi_bopael.jsonl` | Bopæl JSONL output |

### Output — JSONL

Én JSON-linje pr. (bruger, SOR-kode) mapping:

```json
{
  "bruger": "ahansen",
  "code_sor": "246871000016002",
  "giv_adgang_til_underorganisationer": true,
  "beskrivelse": "LSSAPP_RSD_SydBI_Org_246871000016002",
  "source_lssapp_group": "LSSAPP_RSD_SydBI_Org_246871000016002",
  "source_gscapp_group": "GSCAPP_RSD_SydBI_Org_246871000016002"
}
```

| Felt                                  | Type   | beskrivelse                              |
|---------------------------------------|--------|------------------------------------------|
| `bruger`                              | string | Brugerens `sAMAccountName`               |
| `code_sor`                            | string | SOR-kode parseret fra gruppenavn         |
| `giv_adgang_til_underorganisationer`  | bool   | `true` for `_Org_`-grupper               |
| `beskrivelse`                         | string | Kilde-LSSAPP-gruppens navn               |
| `source_lssapp_group`                 | string | LSSAPP-gruppens `sAMAccountName`         |
| `source_gscapp_group`                 | string | GSCAPP-gruppens `sAMAccountName`         |

### Output — TXT

```
SydBI AD-gruppeeksport — 2026-06-29
OU: OU=SydBI,...
antal SOR-koder: 42
antal unikke (bruger, SOR) mappings: 156
======================================================================

SOR: 246871000016002
  LSSAPP: LSSAPP_RSD_SydBI_Org_246871000016002
  GSCAPP: GSCAPP_RSD_SydBI_Org_246871000016002
  Adgangstype: Org (inkl. underorganisationer)
  Medlemmer (3):
    - ahansen
    - blarsen
    - cjensen
```

### Indlæsning i MSSQL

Scriptet genererer ikke længere SQL-output. Indlæsning sker med
`infrastructure/security/load_sydbi_ad_to_mssql.py`, der læser JSONL-filerne
og kører TRUNCATE+INSERT (fuld genindlæsning) mod `sec.RolleDomaene` og
`sec.RBOrganisation` på staging. Se
[sikkerhedsmodel.md](./sikkerhedsmodel.md#drift-dagster) for loaderens
dublet-regler og størrelses-guard.

### Exit codes

| Code | Betydning                                              |
|------|--------------------------------------------------------|
| 0    | Eksport gennemført                                     |
| 1    | Forbindelses-, bind- eller søgefejl                    |
| 2    | Manglende konfiguration eller OU-DN ikke fundet        |

### Forskel fra v1 (`export_ad_groups_members.py`)

| Egenskab         | v1 (generisk)         | v2 (SydBI)                                     |
|------------------|-----------------------|------------------------------------------------|
| Scope            | Vilkårlig OU          | SydBI-OU specifikt                             |
| Gruppenavn       | Rå eksport            | Parsed (LSSAPP/GSCAPP, SOR-kode)              |
| Nesting          | Ignoreret             | GSCAPP→LSSAPP par-opløsning                   |
| Output           | JSONL + TXT           | JSONL (org/roller/bopæl) + TXT                 |
| Target-tabel     | Ingen                 | `sec.RolleDomaene` / `sec.RBOrganisation` (via `load_sydbi_ad_to_mssql.py`) |

---

## 6. Typiske fejl og løsninger

### `FEJL: LDAP_PASSWORD er ikke sat`

**Årsag:** `.env` mangler `LDAP_PASSWORD`, eller filen ikke fundet.

**Løsning:** Tjek at `.env` ligger i repo-roden og indeholder `LDAP_PASSWORD=<password>`.

---

### `unsupported hash type MD4`

**Årsag:** Python 3.12+ fjernede MD4 fra `hashlib`. NTLM-autentificering i
`ldap3` bruger MD4 internt.

**Løsning:** Installér `pycryptodome`:

```bash
pip install pycryptodome
```

---

### `invalid server address` / timeout

**Årsag:** DNS kan ikke opløse `ldaps.example.org`, eller der er ingen
netværksrute til serveren.

**Løsning:**
- Tjek at VPN er aktivt
- Kører du i Docker/devcontainer? VPN på værtsmaskinen deles ikke automatisk.
  Test direkte på værten eller konfigurér container-netværk.

---

### `noSuchObject` (OU-DN ikke fundet)

**Årsag:** Den angivne `LDAP_GROUP_OU_DN` eksisterer ikke i AD. DN'er i AD
er ofte dybere end man forventer, fx:

```
OU=SQL,OU=Servers,OU=SentrySuite Nuklear,OU=Standard,OU=Server Administration,DC=Rsyd,DC=net
```

**Løsning:**
- Scriptet foreslår automatisk kandidat-DN'er baseret på OU-navnet
- Brug `test_ldap_connection.py` til at se OU-navne
- Brug `scan_ou_inventory.py` til at finde præcise DN'er

---

### `Fandt 0 grupper`

**Årsag:** Søgningen lykkedes, men OU'en indeholder ingen grupper. Mange OU'er
(fx under Server Administration) har kun computere, ikke grupper.

**Løsning:** Brug `scan_ou_inventory.py` til at finde OU'er der faktisk
indeholder grupper. De mest lovende er typisk under `Service Areas` eller
`Groups/SydID`.

---

## 7. Hjælpescript: `scan_ou_inventory.py`

**Placering:** `etl/scripts/scan_ou_inventory.py`

### Formål

Scanner AD under en domæne-base for en liste af OU-navne, finder matchende
OU-DN'er, og tæller antal grupper (`objectCategory=group`) og brugere
(`objectCategory=person` + `objectClass=user`) under hver DN.

Bruges til at besvare: *"Hvilke OU'er indeholder grupper og/eller brugere?"*

### Kørsel

```bash
cd etl
python scripts/scan_ou_inventory.py
```

### Script-specifikke miljøvariabler

| Variabel                   | Default          | beskrivelse                              |
|----------------------------|------------------|------------------------------------------|
| LDAP_SEARCH_BASE           | `DC=Rsyd,DC=net` | Search base for OU-lokalisering          |
| LDAP_SCAN_OU_NAMES         | *(built-in liste)*| Newline-separerede OU-navne at scanne   |
| LDAP_SCAN_MAX_DN_PER_NAME  | 15               | Maks antal OU-DN-kandidater pr. navn     |

### Output

```
Søgebase: DC=Rsyd,DC=net
antal OU-navne (unikke): 47
Maks OU-DN pr. navn: 15
----------------------------------------------------------------------

=== Resultater (OU-navn | grupper | brugere | DN) ===

SOC     grupper=2178    brugere=777     OU=SOC,OU=Service Areas,DC=Rsyd,DC=net
Organisational  grupper=903     brugere=0       OU=Organisational,...,DC=Rsyd,DC=net
...

--- OU'er med både grupper>0 og brugere>0 ---
SOC     grupper=2178    brugere=777     OU=SOC,OU=Service Areas,DC=Rsyd,DC=net
...
```

| Værdi i output | Betydning                                          |
|----------------|----------------------------------------------------|
| `grupper=N`    | antal group-objekter under den OU (subtree)        |
| `brugere=N`    | antal person/user-objekter under den OU (subtree)  |
| `grupper=0`    | Søgningen lykkedes; ingen grupper fundet            |
| `grupper=-1`   | Søgningen fejlede (typisk exception under tælling) |

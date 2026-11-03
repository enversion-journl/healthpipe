<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# OS2mo-integration (Magenta)

Basismodellens kodesystemer — i dag **SOR** og **Okosted**, flere på vej — afleveres
hver dag til Magentas OS2mo som full-load **CSV-filer**, uploadet gennem OS2mos
GraphQL-mutation `upload_file`. Magentas import læser filerne derfra. OS2mos
organisation læses desuden tilbage i basismodellen. Begge retninger går gennem den
samme pinnede GraphQL-transport.

**Fase 1** var den første, manuelle aflevering: Example Regions SOR-organisation
som én CSV-fil, **fase-1-filen**, som Magentas import blev bygget til. Den daglige
leverance bruger dens form, generaliseret til alle kodesystemer.

## Indhold

- [Kontrakten](#kontrakten)
- [Sådan hænger det sammen](#sådan-hænger-det-sammen)
- [Konfiguration](#konfiguration)
- [Filformatet](#filformatet)
- [Kodesystemerne](#kodesystemerne)
- [Et nyt kodesystem](#et-nyt-kodesystem)
- [Drift](#drift)
- [Verifikation](#verifikation)

## Kontrakten

| Emne | Aftale |
| :--- | :--- |
| Endpoint | `<OS2MO_BASE_URL>/graphql/v30` — **altid pinnet**, aldrig uversioneret |
| Auth | Keycloak client credentials |
| Leverance | Én CSV-fil pr. kodesystem, uploadet med `upload_file` til `INSERTS`-file-store |
| Retning | Full-load hver dag. Vi skriver **intet** andet i OS2mo end filerne |
| Delta | Magentas import håndterer selv delta og koder, der falder ud af sættet |
| Format | Fase-1 SOR-formatet, generaliseret til Example Regions egen navnestandard — 11 faste kolonner — se [Filformatet](#filformatet) |

Den uversionerede `/graphql` følger den version, OS2mo tilfældigvis kører, og ville
ændre kontrakten under os. `build_graphql_url` afviser derfor alt, der ikke er et
eksplicit `vNN` — pinnet kan ikke omgås fra kode.

## Sådan hænger det sammen

```
codesystem.<Navn>*    ──►  os2mo_<navn>_export  ──►  os2mo_<navn>_upload  ──►  OS2mo
                            export.CodesystemToOs2mo    upload_file              (INSERTS)
                            + rsyd_<navn>_<YYYYMMDD_HHMMSS>.csv

        arc.os2mo_org_unit  ◄──  ext.os2mo_org_unit  ◄──  ext_os2mo_org_unit  ◄──  OS2mo
```

Hvert kodesystem er en `CodesystemExportSpec` i `DSO/export/os2mo/codesystems.py`.
Én fabrik bygger to assets pr. spec:

- **`os2mo_<navn>_export`** læser kodesystemet, bygger sættet, erstatter
  kodesystemets rækker i `export.CodesystemToOs2mo` og skriver CSV'en lokalt.
  Kræver ingen OS2mo-adgang.
- **`os2mo_<navn>_upload`** uploader præcis de bytes. Uploaden forbruger export-assettets
  output, så tabellen, den lokale fil og bytes på tråden er bevisligt samme sæt.

Kørselshistorikken ligger i `meta.DFMRunLog`:

| `schema_name` | `table_name` | Hvad den fortæller |
| :--- | :--- | :--- |
| `export` | `<Navn>ToOs2mo` | at sættet blev bygget, hvor mange rækker, og hvor længe det tog |
| `os2mo` | `<Navn>File` | at filen blev **uploadet**, og om det gik godt |

Begge er log-only: hverken `export` eller `os2mo` står i
`CHECKPOINT_OWNER_SCHEMAS`, fordi et full-load ikke har en delta-position at holde.
En `dry_run` åbner **ingen** DFM-kørsel — en generalprøve er ikke en leverance.

| Lag | Sti | Ansvar |
| :--- | :--- | :--- |
| Transport | `core/os2mo/client.py`, `OS2moResource` i `core/resources.py` | Token, pinnet endpoint, retry, multipart-upload, paginering |
| Model | `utils/os2mo/{codesystem_export,csv_format,org_units}.py` | Uden database- og netværks-I/O: eksportmodel, CSV-format (`write_csv` skriver den lokale kopi), læsning af org units |
| Ud | `DSO/export/os2mo/` | Specs, SQL, asset-fabrikken |
| Ind | `DSI/os2mo/` | `ext_os2mo_org_unit`, `arc_os2mo_org_unit` |

Returstrækket er uafhængigt af uploaden: Magentas import kører på deres egen tidsplan,
så læsningen fanger OS2mo, som det står — ikke effekten af nattens filer. Den læser
alle enheder OS2mo nogensinde har haft (`from_date`/`to_date: null` + `validities`),
så en lukket enhed arkiveres som lukket i stedet for at blive soft-deleted.
`user_key` lander renset for fase-1-indpakningen (`="<kode>"`), rå i `user_key_raw`.

> **Bemærk:** `DSI/os2mo/` (GraphQL) er ikke det samme som `DSI/csv_files/conceptmap_csv.py`
> (håndholdte ConceptMap-override-filer). De to har bevidst hver sin asset-gruppe —
> `os2mo_import` og `conceptmap_csv_extract` — så det daglige job ikke trækker de
> fire `ext_csv_*`-assets med sig.

## Konfiguration

Credentials håndteres som databaseforbindelserne: **Nomad-variabler** i produktion,
`.env` i devcontaineren. Ingen secrets i repoet.

| Variabel | Kode-mønster | Beskrivelse |
| :--- | :--- | :--- |
| `OS2MO_ENABLED` | `env_bool` | Dræberkontakt for al trafik til OS2mo: upload og read-back fejler, og sensoren springer over. En `dry_run` virker stadig. `false` i devcontainer og CI |
| `OS2MO_BASE_URL` | `os.getenv` | OS2mo-rod **uden** `/graphql` |
| `OS2MO_GRAPHQL_VERSION` | `os.getenv` | `v30` |
| `OS2MO_KEYCLOAK_REALM_URL` | `os.getenv` | Keycloak **realm-rod**, fx `https://<host>/auth/realms/mo` |
| `OS2MO_CLIENT_ID` | `os.getenv` | Keycloak-client med ret til `upload_file` |
| `OS2MO_CLIENT_SECRET` | `dg.EnvVar` | Secret — forbliver en Dagster-reference til kørselstid |
| `OS2MO_TIMEOUT_SEC` | `env_int` | Valgfri, default 120 |
| `OS2MO_MAX_RETRIES` | `env_int` | Valgfri, default 3 |

Tre steder skal holdes i sync:

1. `.env.template` — tomme defaults, `OS2MO_ENABLED=false`.
2. `.gitlab-ci.yml` (`.prod-db-env`) — `placeholder`-værdier, så
   `dagster definitions validate` kan indlæse code location'en.
3. **`healthpipe-infrastructure`** (søster-repo) — nøglerne i Nomad-jobbets env-blok, og
   værdierne seedet som Nomad-variabler (samme flow som `seed-watcher-nomad-var.sh`).
   Secrets kommer fra Bitwarden.

En manglende variabel giver en `OS2moConfigurationError`, der navngiver variablen —
ikke et uforklarligt 401.

### Token-endpointet udledes

Vi konfigurerer **realm'et**, ikke token-stien. Klienten læser `token_endpoint` fra
`<realm>/.well-known/openid-configuration` ved første token-hentning og cacher det.

Det er bevidst: netop den sti er flyttet. Keycloak 17 droppede `/auth`-præfikset, så
`<host>/auth/realms/mo/protocol/openid-connect/token` og
`<host>/realms/mo/protocol/openid-connect/token` er begge rigtige — afhængigt af
version. Serveren kan svare på det selv.

Verificér realm-URL'en med:

```bash
curl -s "$OS2MO_KEYCLOAK_REALM_URL/.well-known/openid-configuration" | jq -r .token_endpoint
```

Svarer den ikke med et `token_endpoint`, peger URL'en sandsynligvis på Keycloaks
server-rod frem for på et realm. Klienten siger netop det.

Der er bevidst **ingen** måde at konfigurere token-stien direkte. To veje til samme
indstilling betyder, at den ene kan skygge for den anden i stilhed — og kommandoen
ovenfor ville så ikke afsløre noget. Skulle et miljø vise sig at annoncere et
endpoint, vi ikke kan nå, hører det til som en ny ændring med en kendt begrundelse.

## Filformatet

Fase-1 SOR-filen er grundformen, fordi det er den, OS2mos import er bygget op om.
Kolonnerne er generaliseret og omdøbt til Example Regions egen navnestandard,
som aftalt med Magenta, i stedet for OS2mos eget vokabular. De kolonner, der
svarer til et felt på `OrganisationUnitCreateInput` (v30, bekræftet ved
introspektion), bærer mappingen videre til Magentas import; de øvrige er vores
egne (se tabellen nedenfor, hvor de er markeret med `—`). Kodesystemet fremgår
af filnavnet, ikke af headeren, så én import kan læse dem alle. Headeren er
**fast**: altid de samme 11 kolonner, uanset data — der er ingen dynamisk
bredde længere.

`kode_parent` er filens **eneste** hierarkioplysning. Fase-1-filens
`Level`- og `Niv`-kolonner er udeladt: en integration skal bygge træet ud fra
forælderkoden, ikke ud fra navne, og en forfaders omdøbning eller flytning ville
ellers ændre rækker, hvis egne data er uændrede.

```
kode;kode_parent;gyldig_fra;gyldig_til;navn;type;niveau;aktiv;dw_is_deleted;dw_timestamp_insert;dw_timestamp_update
```

| Kolonne | OS2mo | Indhold |
| :--- | :--- | :--- |
| `kode` | `user_key` | Koden |
| `kode_parent` | `parent` | Forælderens kode i det **leverede** træ. OS2mos `parent` tager et UUID, som kun OS2mo kender, så filen navngiver forælderen ved dens `kode`. En rod har tom værdi — også hvis kodesystemet selv giver den en forælder uden for det leverede sæt: filen må ikke pege på en kode, den ikke indeholder |
| `gyldig_fra` / `gyldig_til` | `validity { from to }` | ISO-datoer (`YYYY-MM-DD`). Tom `gyldig_til` = åben, ligesom `null` i OS2mo. En kildeslutdato i år 9000 eller senere er en "slutter aldrig"-sentinel og bliver også tom |
| `navn` | `name` | Kodens display |
| `type` | `org_unit_type` | Klassens user_key: SOR's `entityType`-værdier (fx `region`, `hospital`, `klinisk enhed`), Okosteds `enhedstype`-værdier (`juridisk enhed` / `virksomhedsenhed` / `afdeling` / `omkostningssted` / `Ukendt`). En kode uden typeværdi renderes tom og tælles i asset-metadataens `codes_without_type` |
| `niveau` | — | Kodens dybde i det **leverede** træ: en rod er `1`, et barn er forælderens niveau plus 1. Beregnes af den bredde-først-gennemgang, der også ordner filen — ikke læst fra kilden — så den altid matcher det træ, filen rent faktisk afleverer |
| `aktiv` / `dw_is_deleted` | — | Kodens statusflag, `true`/`false`, eller tom, hvor kodesystemet ikke har flaget |
| `dw_timestamp_insert` / `dw_timestamp_update` | — | Basismodellens egne tidsstempler (ISO 8601, lokal tid, sekunder) — se nedenfor |

`dw_timestamp_insert` er, hvornår koden kom ind i `codesystem.<Navn>Concept`.
`dw_timestamp_update` er, hvornår kodens række i filen sidst ændrede sig: dens
concept-række, en af dens egenskaber eller dens forælder. De gør det muligt for
Magentas integration kun at behandle de rækker, hvor en af disse kolonner er
ændret siden sidst.

Tidsstemplerne betyder kun "ændret", fordi begge kodesystemer merges med
`update_only_on_change=True` og sletter forældede egenskaber og forælderkanter. En
sletning flytter concept-rækkens `dw_timestamp_update`. Ændringsvagten sammenligner
binært og med `DATALENGTH`, så en rettelse, der kun består i store/små bogstaver
eller efterstillede mellemrum, også registreres.

Filen bærer ingen mapninger — de vedligeholdes manuelt i OS2mo, nøglet på de nye
kolonnenavne.

Filen er `;`-separeret med CRLF-linjeskift. **Alle felter er i anførselstegn**, og
et anførselstegn i en værdi fordobles (RFC 4180), så et navn med `;`, `"` eller
linjeskift aldrig kan forskyde en kolonne.

Tegnsættet er **UTF-8**, som bærer Æ, Ø og Å — og alle andre tegn — uændret. Filen
har BOM, så den også åbner korrekt i Excel; et program, der læser den, skal dekode
som `utf-8-sig`, så BOM'en ikke havner i den første kolonneoverskrift. Filen skrives
som bytes, så ingen platform kan oversætte linjeskiftene. Rækkefølgen er
bredde-først fra rod(erne), søskende sorteret på kode — samme data giver
byte-identiske filer.

### Filnavnet

Hver leverance har sit eget navn: `rsyd_<kodesystem>_<YYYYMMDD_HHMMSS>.csv`, fx
`rsyd_okosted_20261006_013012.csv`. Tidspunktet er, hvornår **sættet blev bygget**,
ikke hvornår det blev uploadet, så en genkørt upload af samme sæt sender samme navn.
Magentas integration kan dermed se, hvilke leverancer den allerede har behandlet, og
en fil ændrer sig aldrig, mens den læses. Til gengæld vokser deres file store, så
oprydning af gamle filer hører hjemme hos Magenta.

Den lokale kopi under `DATA_DIR/os2mo/` får samme navn og samme bytes, og vokser
derfor også med én fil pr. kodesystem pr. nat.

`tests/fixtures/os2mo/sor_fase1_subtree.csv` er et udsnit af fase-1-filen; testen
oversætter den kolonne for kolonne og viser, at SOR-sættet stadig reproducerer
hver værdi.

`export.CodesystemToOs2mo` spejler filens kolonnenavne — undtagen de tre, der
kolliderer med basismodellens egne audit-kolonnenavne, som får `_codesystem`-postfix:
`dw_is_deleted_codesystem`, `dw_timestamp_insert_codesystem`,
`dw_timestamp_update_codesystem`. Tabellens eget byggestempel beholder navnet
`dw_timestamp_insert`. `kode`/`kode_parent` er `NVARCHAR(120)` (Okosteds
composite-koder); datoerne er `DATE`, `niveau` er `INT`, flagene `BIT`, og
tabellen har desuden `codesystem`. Den bærer ingen mapninger længere.

## Kodesystemerne

| | SOR | Okosted |
| :--- | :--- | :--- |
| Kilde | `codesystem.SOR*` (`cs_sor`) | `codesystem.Okosted*` (`cs_okosted`) |
| Afgrænsning | `rsdOrganization = true` (filtreret i SQL) — se nedenfor | **Hele kodesystemet** |
| Rod | `6101000016001` (Example Region) | Alle koder uden forælder (niveau-1-knuderne, de 9 toppe) |
| `gyldig_fra` / `gyldig_til` | `openingDate` / `closingDate` | `startsdate` / `enddate` |
| `type` | `entityType` | `enhedstype` (udfyldt) |
| `aktiv` / `dw_is_deleted` | `activeOwnOrganization` / tom | SydAX' `Aktiv` / `Slettet` |
| Mapninger | ingen — vedligeholdes manuelt i OS2mo | ingen — vedligeholdes manuelt i OS2mo |
| Fil | `rsyd_sor_<YYYYMMDD_HHMMSS>.csv` | `rsyd_okosted_<YYYYMMDD_HHMMSS>.csv` |

Begge kodesystemer og `cm_okosted_to_sor` bygges af `organisation_cs_job`.
`os2mo_delivery_sensor` starter leverancen, når det job er lykkedes.

SOR-filen indeholder alle enheder, `cs_sor` markerer som `rsdOrganization`:
enheder, der kan nås fra roden `6101000016001`, uanset om de er åbne, lukkede
eller endnu ikke åbnet. `aktiv` er `activeOwnOrganization`: `true`, når enheden
er i kraft — `openingDate` er nået, og der er ingen `closingDate` på eller før
i dag (dansk tid) — og kan nås fra roden gennem enheder, der også er i kraft. En lukket
enhed leveres derfor med `aktiv = false` og udfyldt `gyldig_til`, en kommende
med `aktiv = false` og `gyldig_fra` i fremtiden. En åben enhed under en lukket
forælder er også `aktiv = false`.

Okosteds `kode` er en sammensat identitet: en knude er parret `(nummer,
niveau)`, og koden skrives `<nummer>_NIVEAU_<niveau>`, fx `RSD_NIVEAU_1` eller
`SHD.SHS_NIVEAU_4`. Samme SydAX-nummer kan derfor stå for flere knuder — én
pr. niveau, det optræder på i kildens stitabel. Hver knude bærer sin egen
`enhedstype`-property (`juridisk enhed` / `virksomhedsenhed` / `afdeling` /
`omkostningssted`, afledt af hvilken typet id-kolonne der er udfyldt; `Ukendt`
hvis ingen er), som leveres som filens `type`-kolonne, og en `nummer`-property
med det rå SydAX-nummer, til rapporter og opslag. En kant går altid fra niveau
N til N−1 i samme kilderække, så hierarkiet er 1:1 med kildens stitabel og
acyklisk pr. konstruktion — og filens `niveau`-kolonne, som beregnes generisk
for alle kodesystemer ved en bredde-først-gennemgang, matcher derfor kodens
eget `_NIVEAU_<n>`-suffiks i et intakt hierarki; en kode, hvis forælder er
forsvundet fra kilden (`codes_gone`, se nedenfor), leveres i stedet som rod
med `niveau = 1`, uanset dens eget suffiks.

Okosteds datoer er altid 1900-01-01 → 9999-12-31 og siger intet om, hvorvidt et
omkostningssted er i brug. Derfor leveres hele kodesystemet, og status står i
`aktiv`/`dw_is_deleted`. Det er Magentas integration, der afgør, hvad den importerer.

Flagene gælder knuden selv og kommer fra dens **egne** rækker i SydAX — de
rækker, hvor knudens celle er den dybeste udfyldte i rækken. Har den flere,
vinder en række, der ikke er slettet. Samleniveauer — knuder, der i ingen
kilderække er det dybeste udfyldte niveau — optræder kun som forældre i andre
knuders rækker og har derfor ingen egen række og dermed ingen status i SydAX;
for dem er `aktiv` og `dw_is_deleted` tomme. Tom betyder "ingen status", ikke
"inaktiv", så hierarkiet skal bygges over hele filen, før der filtreres på
status.

Koder, der er forsvundet fra SydAX, har ingen egenskaber tilbage i kodesystemet og
udelades (`codes_gone` i metadata). Datoer og flag, der findes men ikke kan læses,
bliver tomme i filen og tælles i `unreadable_values` og en advarsel i loggen. En
kode uden typeværdi (`type` tom) tælles tilsvarende i `codes_without_type` og
logger en advarsel.

Okosted-sættet bygges ved en bredde-først-gennemgang fra de 9 niveau-1-rødder.
Fordi en kant altid går fra niveau N til N−1, er træet acyklisk pr.
konstruktion, så eksportens generelle cyklusværn — fælles for alle
kodesystemer, og stadig det, der tæller `codes_unreachable` i
export-assettets metadata og logger en advarsel — bør aldrig udelade en
Okosted-kode: `codes_unreachable = 0` er acceptkriteriet efter
niveau-identiteten. Datakvalitetssignaler fra selve kilden — en knude med
uenige forældre-numre på tværs af rækker, eller en knude uden forælder midt i
træet — tælles i stedet som `parent_conflicts` og `path_gaps` i `cs_okosted`s
egen metadata, og en knude uden typet id-kolonne tælles som
`unknown_enhedstype`.

## Et nyt kodesystem

1. Tilføj en `CodesystemExportSpec` i `DSO/export/os2mo/codesystems.py` og læg den
   i `CODESYSTEM_EXPORTS`. Det er det hele: assets, jobbets udvælgelse, sensorens
   `dry_run: false` og eksporttabellen følger af tuplen.
2. `source_deps` skal navngive de assets, der bygger kodesystemet, og de skal
   ligge i `organisation_cs_job`, som sensoren venter på.
3. Mapninger til andre kodesystemer er ikke en del af et nyt kodesystems
   opsætning: filen bærer ingen mapninger, og de oprettes manuelt i OS2mo efter
   leverancen, nøglet på de nye kolonnenavne.
4. Aftal med Magenta, at deres import kender filnavnet
   (`rsyd_<navn>_<YYYYMMDD_HHMMSS>.csv`).

Tabel- og egenskabsnavne fra spec'en havner i SQL og skal derfor være rene
identifikatorer; spec'en afviser alt andet.

## Drift

### dry_run

`os2mo_<navn>_upload` har `dry_run: true` som default: at nå et eksternt system skal
nogen bede om eksplicit. En `dry_run` bygger og tjekker alt, sender intet og kræver
ingen OS2mo-konfiguration — den kører også med resourcen helt ukonfigureret, som på
et feature-branch-deployment.

### Sensoren

`os2mo_delivery_sensor` starter `os2mo_codesystem_export_job`, når
`organisation_cs_job` er lykkedes — aldrig midt i en genopbygning, og en nat, hvor
genopbygningen fejlede, leveres intet. Den sætter `dry_run: false` for **hver**
upload. Med `OS2MO_ENABLED=false` springer den over: et deployment uden OS2mo-mål
har intet at levere til.

### Config

```yaml
ops:
  os2mo_okosted_export:
    config:
      output_dir: null        # kun placeringen af den lokale kopi
  os2mo_okosted_upload:
    config:
      dry_run: true
      file_store: INSERTS
      force: true             # overskriv et tidligere forsøg på SAMME sæt
```

Hvert sæt har sit eget navn, så `force: true` kun overskriver et tidligere forsøg
på det samme sæt. Det er det, der gør uploaden sikker at gentage: en upload, der
timede ud men nåede frem, erstattes af de samme bytes i stedet for at fejle som
"filen findes". Med `force: false` forsøges en upload kun én gang.

### Byg filerne lokalt

`etl/scripts/os2mo_render_csv.py` bygger filerne med samme kode som
export-assettet og skriver dem til en lokal mappe, uden Dagster. Den læser
staging via `MSSQL_TARGET_STAGING_*` (en `.env` i repo-roden læses automatisk),
skriver intet til databasen og uploader intet. Brug den til at se format og
data, eller til at sende en fil til Magenta manuelt.

```bash
python etl/scripts/os2mo_render_csv.py --out ~/os2mo
python etl/scripts/os2mo_render_csv.py --codesystem okosted --out ~/os2mo
```

## Verifikation

1. `cd etl && pytest tests/unit/ -k os2mo -v`
2. `dagster definitions validate`
3. Materialisér `os2mo_sor_export` og `os2mo_okosted_export`. Tjek
   `export.CodesystemToOs2mo` pr. `codesystem`, og at CSV'erne under
   `DATA_DIR/os2mo/` har forventet header og rækketal. Kør dem igen: rækketallet
   er det samme og `dw_timestamp_insert` er flyttet.
4. Materialisér uploads med `dry_run=true` — kræver ingen OS2mo-adgang.
5. `dry_run=false` mod **mo-test**. Bekræft med Magenta, at filerne ligger i
   `INSERTS`, og at deres import kan læse dem.
6. Kør `ext_os2mo_org_unit` og `arc_os2mo_org_unit` for returstrækket.

<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Okosted: Beslutningsnotat om niveau-baseret kodeidentitet

Status: IMPLEMENTERET 2026-10-14 — grundlag: måleserie mod Dev-staging
(`arc.sydax_omkostningssted_hierarki`, `map`/`dim`/`fct`) og mod kildens
`Finans.OmkostningsstedHierarki`. Alle tal i notatet kan genskabes med
queriene i §Målinger.

## Baggrund

SydAX' `Finans.OmkostningsstedHierarki` er sti-tabel: hver række er én sti
fra toppen (niveau 1) til et blad, og enheden på hvert niveau identificeres
af en **typet id-kolonne** — `jeId1` (juridisk enhed), `veIdN`
(virksomhedsenhed), `afIdN` (afdeling), `osIdN` (omkostningssted) — mens
`nummerN`/`tekstN` kun er visningsværdier. Samme nummer-tekst kan derfor stå
for **forskellige enheder på forskellige niveauer** (fx `FER` som juridisk
enhed på niveau 1, jeId 102105358334, og `FER` som afdeling på niveau 3,
afId 2724).

`cs_okosted` nøgler i dag concepts på nummer-strengen alene, og tre
uafhængige mekanismer afgør så, hvad hierarkiet bliver til:

1. **Ordensafhængigt forældrevalg**: en kodes forælder tages fra dens først
   mødte optræden på niveau ≥ 2 — rækkefølgen af kilderækkerne afgør valget.
2. **Tavst selv-kant-filter**: `merge_codesystem_tables`
   (`DSA/CodeSystem/utils.py`) frafiltrerer kanter hvor forælder = barn. Det
   redder tilfældigt `RSD` og `HOS` (deres genbrug ligger lige over dem
   selv, så kanten bliver en selv-kant og droppes → de forbliver rødder).
3. **Eksportens cyklusværn**: koder, der ikke kan nås fra en rod, udelades
   af OS2mo-leverancen og tælles som `codes_unreachable`.

Konsekvens målt på Dev: Okosted har 9 toppe (`MED, SYD, HSM, HSS, HSF, FER,
RSD, FYN, HOS`). Syv af dem fanges i 2-cykler (fx `FER ↔ FER.FON`), udløst
af én slettet pladsholderrække pr. hierarki, og **2.483 af 28.430 koder er
derfor udeladt af OS2mo-leverancen**. `stg_Okosted`/`dim.Okosted` beregner
samtidig forkerte niveauer for de berørte grene (forældrekæden løber i
ring), og enhedstypen (je/ve/af/os) repræsenteres slet ikke i warehouse.

## Målinger

Unpivot af de 20 niveauer (nummer + typet id pr. celle), nyeste SCD-version
pr. `OmkostningsstedHierarkiId`:

1. **Dubletkoder i kilden er præcis de 9 toppe**, hver på netop 2 niveauer,
   og hver dyb variant har præcis 1 distinkt forælder. `RSD` står på niveau
   1 *og* 2 i samtlige 18.717 RSD-rækker (levende: virksomhedsenheden under
   den juridiske enhed bærer hele RSD-træet); `HOS` står på niveau 5 i 29
   levende rækker; de 7 øvrige dybe varianter er hver én **slettet**
   pladsholderrække (`Slettet = 1`, pladsholdertekst).
2. **Økonomien refererer slettede hierarkirækker**: 4.618.029 posteringer
   (4.469 koder), 2.665.580 kladdelinjer (3.531 koder) og 746.233
   budgetrækker (2.541 koder) rammer rækker med `Slettet = 1`. At filtrere
   slettede rækker fra er dermed udelukket.
3. **14.653 af 28.430 koder findes kun i slettede rækker** og skal fortsat
   bygges og leveres; deres status bæres af `aktiv`/`slettet`-properties
   som i dag.
4. **Fact-koblingen er id-baseret indtil sidste led**: fact-rækkens
   `FinanskontoId` → `arc.sydax_finanskonto_omkostningssted`
   (`Omkostningsstedid`) → hierarkirækken via `max_OmkostningsstedId` → det
   **dybeste udfyldte nummer** i rækken (`COALESCE(nummer20..nummer1)`) →
   `map.Okosted.clean_code_Okosted`. Bladet er altid et ægte
   omkostningssted (osId), aldrig en af de 9 dubletkoder.
   `omkostningssted_niveau` (det dybeste niveau) beregnes allerede i
   `pre_stg_*`.

## Beslutning

**En Okosted-knude er `(nummer, niveau)`, og dens kode skrives
`<nummer>_NIVEAU_<niveau>` — for alle koder, uden undtagelser.**

1. **Kanter går altid fra niveau N til N−1 i samme række**: forælderen til
   `<nummerN>_NIVEAU_N` er `<nummerN-1>_NIVEAU_N-1`. Træet er dermed
   acyklisk pr. konstruktion, selv-kanter kan ikke opstå, og rødderne er
   niveau-1-knuderne (de 9 toppe). Selv-kant-filteret i merge'en mister sin
   bærende rolle og bliver rent dataværn.
2. **Attributter pr. knude** (alle udledt af knudens egne celler):
   - `display`: `tekstN` i knudens celle; ved flere rækker vinder nyeste
     række (eksisterende nyeste-række-regel, nu pr. knude).
   - `enhedstype` (ny property): `juridisk enhed` / `virksomhedsenhed` /
     `afdeling` / `omkostningssted` efter hvilken typet id-kolonne der er
     udfyldt i cellen (`jeId1` / `veIdN` / `afIdN` / `osIdN`). Målt:
     cellerne har altid præcis ét typet id. En celle uden typet id får
     `enhedstype = Ukendt` og tælles i asset-metadata — typen skal være
     udfyldt i OS2mo-leverancen, så `Ukendt` holder kolonnen udfyldt,
     mens tælleren er vagtposten, der afslører hullet i kilden.
   - `nummer` (ny property): det rå SydAX-nummer, så rapporter og
     opslag fortsat kan vise/finde kildens nummer.
   - `aktiv`/`slettet`: uændret own-row-regel (rækker hvor knudens celle er
     det dybeste udfyldte niveau), nu pr. knude.
   - `startsdate`/`enddate`/`kildesystem`/`OmkostningsstedHierarkiId`:
     som i dag, pr. knude.
3. **Slettede rækker bygges med** (jf. måling 2 og 3); status bæres af
   `aktiv`/`slettet`. Bugens syv pladsholder-varianter (fx `FER_NIVEAU_3`)
   bliver almindelige knuder med `slettet = true`, børn af deres
   niveau-2-forælder — cyklussen kan ikke længere opstå.
4. **Deterministisk valg ved flertydig forælder**: har samme `(nummer,
   niveau)` forskellige forældre-numre på tværs af rækker, vinder nyeste
   række (samme princip som display), og antallet af uenigheder udstilles i
   asset-metadata. Målt 0 for dubletkoderne; omfanget for øvrige knuder
   måles ved implementeringen.

### Accepteret pris

- Flytter en enhed niveau ved en reorganisering, skifter dens kode →
  nyt `dw_ek` i map/dim, delete+create i OS2mo, og fact-historik samles
  ikke på tværs af flytningen. Valgt bevidst til fordel for et hierarki,
  der er 1:1 med kilden.
- Alle koder i OS2mo-leverancen skifter `user_key` i forhold til det første
  manuelle dump (som manglede 2.483 koder og er kasseret). Kommunikeres til
  Example Region/Magenta sammen med næste dump; de manuelle
  SOR-mapninger i OS2mo oprettes med de nye nøgler.

## Migrering og berørte led

**Migreringsstrategi: truncate + fuld omkørsel.** Ingen in-place-omdøbning
af nøgler: en idempotent, tracket migrations-SQL (precedens:
`20260625_reset_sydax_delta_checkpoints.sql`) tømmer de berørte tabeller og
nulstiller eventuel DFM-state for de berørte pipelines, hvorefter kæden
omkøres fuldt i rækkefølgen `cs_okosted` →
`stg_Okosted`/`map_Okosted`/`dim_Okosted` → `cm_okosted_to_sor` → de tre
fact-kæder (mode=full_load) → DSO-sync.
`dw_ek_Okosted`-værdierne får dermed ny betydning, hvilket er sikkert, fordi
**alle** bærere af nøglen tømmes og genopbygges i samme ombæring
(inventeret: `map.Okosted`, `dim.Okosted`, `fct.Postering`, `fct.Budget`,
`fct.Finanskladdelinje`, `conceptmap`-lagets dw_ek-opløsning og DSO-synkens
kopier). `codesystem.Okosted*` truncates ligeledes — konsekvensen er, at
`created`/`updated` (dw_timestamp-kolonnerne, som OS2mo-filen afspejler)
starter forfra, hvilket er konsistent med, at alle koder er nye identiteter.

| Led | Ændring |
|---|---|
| `cs_okosted` (`build_okosted_hierarchy`) | Knude pr. `(nummer, niveau)`; kode `<nummer>_NIVEAU_<n>`; forælder altid fra niveau N−1 i samme række; `enhedstype`- og `nummer`-properties; metadata for forælder-uenigheder. |
| `codesystem.Okosted*` | Truncates og genopbygges fuldt af `cs_okosted`. |
| `map.Okosted` | Truncates og genopbygges fra det nye codesystem (nye `dw_ek`'er). |
| `dim.Okosted` | Truncates og genopbygges; `code_okosted` = ny kode; ny kolonne med det rå nummer til rapportering; `level` bliver korrekt (= knudens niveau). |
| Facts (`pre_stg_Postering/Budget/Finanskladdelinje` → `stg_fct_*` → `fct_*`) | Join-nøglen bygges som `leaf-nummer + '_NIVEAU_' + leaf-niveau` (begge beregnes allerede i `pre_stg`); `fct`-tabellerne truncates og genindlæses fuldt fra arc (pre_stg/stg genopbygges i forvejen fuldt hver kørsel). |
| `DSI/csv_files/data/okosted_to_sor.csv` + `conceptmap.OkostedToSOR` | `source_code` opdateres til de nye koder (manuel mapping-fil, få rækker); conceptmap genopbygges, og dw_ek-opløsningen følger den nye map. |
| DSO-sync (`DSO/load/sync_configs.py`) | dim/fct resynces fuldt efter genopbygningen. |
| OS2mo-leverancen | Følger automatisk codesystem (kode, forælder, type fra `enhedstype`); selve filformatet (kolonnenavne, niveau-kolonne) ligger på [internal ticket]-sporet. |

## Vagtposter og acceptkriterier

1. Ingen kode i `codesystem.OkostedHierarchy` er sin egen forfader
   (acyklisk pr. konstruktion — testes alligevel).
2. Rødderne er præcis niveau-1-knuderne, og alle koder kan nås fra en rod:
   `codes_unreachable = 0` på `os2mo_okosted_export`.
3. Kollisionsvagt: intet råt SydAX-nummer indeholder `_NIVEAU_` (build
   fejler ellers).
4. Fact-dækning: `missing_okosted_fk` i fact-diagnostikken må ikke stige i
   forhold til baseline før migreringen.
5. `dim.Okosted` verificeres for de berørte grene (de 9 toppe, inkl.
   FER-undertræet og `RSD_NIVEAU_2` som bærende knude).

## Uden for scope

- Kolonnenavne og format i OS2mo-filen samt `export.CodesystemToOs2mo`
  ([internal ticket], egen branch oven på denne).
- Øvrige kodesystemer: SOR og de andre kodesystemer er uberørte.

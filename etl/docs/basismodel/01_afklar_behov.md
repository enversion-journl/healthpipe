<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# 01 — Afklar behovet

Før du skriver kode, skal du vide hvilken slags opgave du har. Det afgør både hvor
meget arbejde der er, hvem du skal tale med, og hvilket kapitel du fortsætter i.

Dette kapitel er en tjekliste til den afklaring.

## Fire slags opgaver

| Opgave | Kendetegn | Omfang |
|---|---|---|
| **A. Ny datakilde** | Et kildesystem, vi ikke henter fra i dag. Kræver adgang, forbindelse, DDL-hjem, konfiguration, job og datakvalitetstjek. | Dage til uger |
| **B. Tilføjelse til eksisterende datakilde** | Ny tabel eller nye kolonner fra et kildesystem, vi allerede henter fra. | Timer til dage |
| **C. Fejlretning i en datakilde** | Data findes, men er forkerte, mangler eller er forældede. | Timer til dage |
| **D. Andet** | Nyt view, ny nøgle, performanceproblem, nyt datakvalitetstjek, dokumentation. | Varierer |

## A. Ny datakilde

### Det du skal have afklaret, før du koder

| Spørgsmål | Hvorfor det betyder noget |
|---|---|
| Hvilket forretningsbehov skal dækkes? | Afgør hvilke tabeller vi skal hente — ikke "alt hvad der findes". |
| Hvilken slags kildesystem er det? (on-prem MSSQL, cloud MSSQL/Synapse, Oracle, fildrop) | Afgør connection-bibliotek og connector |
| Findes der en read-only servicekonto? | Vi henter **altid** med en læsekonto. |
| Er der netværksadgang fra Dagster-miljøet? | Skal verificeres, ikke antages. |
| Er data personhenførbare? | Afgør hvor data må ligge, og om DSO skal adgangsstyres. |
| Hvor ofte leveres/ændres data? | Afgør tidsplan og om der er brug for delta-hentning. |
| Er data beskrevet i HL7 FHIR? | Afgør sporvalget nedenfor. |

### Sporvalg

Når kilden er inde i arkivet, skal du vælge, hvordan den modelleres:

```
Er data kliniske og dækket af en FHIR-ressource?
├── Ja  → FHIR-sporet         → kapitel 03a (+ 03a1)
└── Nej
    ├── Er det en færdigberegnet leverance udefra
    │   (aggregerede tal per periode/organisation/kategori,
    │    fulde snapshots, fx SDS/SSI)?
    │   └── Ja → dm-sporet     → kapitel 03c
    └── Ellers: transaktionelle data der skal modelleres
        dimensionelt          → kapitel 03b
```

Er du i tvivl mellem 03b og 03c: har rækkerne en transaktions-id, og kan man
drille ned i den enkelte hændelse? Så er det 03b. Er rækken selv tallet, og er
summen lavet af leverandøren, er det 03c.

### Det afklaringen skal ende med

Før du går videre til kapitel 02, skal du have:

- **Bekræftet adgang og netværksforbindelse** — verificeret, ikke lovet.
- **Et valgt spor** (FHIR, dimensionelt eller dm).
- **Et skemaoverblik over kildesystemet** i `etl/docs/source_systems/{kilde}/`:
  hvilke tabeller findes, hvordan hænger de sammen, og hvad ved vi om systemets
  adfærd. Se [05 — Udfyld dokumentation](05_udfyld_dokumentation.md).

Fortsæt i [02 — DSI: fra kilde til arkiv](02_DSI_kilde_til_arkiv.md).

## B. Tilføjelse til eksisterende datakilde

Kildesystemet kører allerede. Det, der stadig skal afklares, er den nye tabel.
Seks spørgsmål, som alle skal være besvaret, før du kan skrive konfigurationen:

| Spørgsmål | Sådan tjekker du |
|---|---|
| Hvad er grain? Én række per hvad? | `SELECT COUNT(*), COUNT(DISTINCT <kandidat>) FROM <tabel>` — tallene skal være ens |
| Findes der en stabil primærnøgle? | Kildens DDL. Bekræft at nøglen ikke genbruges efter en sletning |
| Findes der et modification timestamp? | Kig efter `modified`, `sysdato`, `*_MODIFICATION_DATETIME`, audit-heltal |
| Opdateres tidsstemplet ved **enhver** ændring? | Spørg systemejeren, og stikprøv rækker du ved er rettet |
| Hvordan repræsenteres sletninger? | Sletteflag, deletion log-tabel, eller hard delete |
| Hvor stor er tabellen, og hvor meget ændrer sig dagligt? | `SELECT COUNT(*)` plus ændringsvolumen |

Vær særligt opmærksom på fjerde spørgsmål: et timestamp, der kun opdateres ved
insert og ikke ved rettelser, får delta-hentningen til at se fejlfri ud, samtidig
med at den taber alle opdateringer.

Svarene bliver til konfiguration: grain og primærnøgle bliver `merge_keys`,
delta-kolonnen bliver `delta_column`, og sletteadfærden bliver `detect_deletes` /
`source_delete_column`. Konfigurationen er dermed den varige registrering af
valget. Fandt du undervejs noget om **kildens** adfærd, som en fremtidig læser
ikke kan se af kildens eget skema — fx at et modification timestamp ikke opdateres
ved rettelser — hører den iagttagelse til i `etl/docs/source_systems/{kilde}/`.

Fortsæt i [02 — DSI: fra kilde til arkiv](02_DSI_kilde_til_arkiv.md), afsnittet om
en ny tabel — og derefter det spor, kilden i forvejen ligger på.

## C. Fejlretning i en datakilde

Find laget først — hvert lag har sin egen type fejl.

| Symptom | Sandsynligt lag | Første tjek |
|---|---|---|
| Data mangler helt for en periode | DSI (extract) | Er checkpointet løbet forbi? `SELECT * FROM meta.DFMState WHERE table_name = '<tabel>'` |
| Data findes i `arc.*`, men ikke længere fremme | DSA | Er den seneste kørsel af staging/transform lykkedes? |
| Data findes i DSA, men ikke i DSO | DSO (sync) | Watermark og `counts_match` i asset-metadata |
| Tal er for høje (dobbelttælling) | DSA | Matcher `merge_keys` det faktiske grain? Er der flere aggregeringsniveauer i samme tabel? |
| En kolonne er altid NULL | DSI | Mangler kolonnen i `non_key_columns` på arkivets konfiguration? |
| Nøgler falder tilbage til `-1` | DSA (map) | Bliver CleanCode'en dannet ens i pre_stg og i map-opslaget? |
| Rækker der burde være slettet, er stadig med | DSI eller view | Er delete detection slået til? Filtrerer viewet på `dw_is_deleted = 0`? |

Tre regler for fejlretning:

1. **Reproducér før du retter.** Skriv den SQL, der viser fejlen, og gem den — den
   bliver din verifikation bagefter, og ofte et nyt DQA-tjek.
2. **Ret upstream, ikke downstream.** En fejl i arkivet skal rettes i arkivet, ikke
   dækkes af et filter i et view. Et filter downstream skjuler fejlen for det næste
   spor, der læser samme arkiv.
3. **Replay flytter ikke checkpointet.** Brug DFM's `period`- eller `ids`-mode
   til at hente de berørte data igen. Se
   [dfm_operations_guide.md](../implementation/dfm_operations_guide.md).

Ender fejlen med en beslutning om, *hvordan* data skal fortolkes (og ikke bare en
programmeringsfejl), hører beslutningen til i beslutningsloggen — se
[05 — Udfyld dokumentation](05_udfyld_dokumentation.md).

## D. Andet

| Opgave | Hvor |
|---|---|
| Nyt view i DSO | [04 — DSO](04_DSO_sync_til_DSO.md) |
| Ny fælles dimension | [03b](03b_DSA_arkiv_til_dimensionel_model.md) + [shared_dimensions.md](../shared_dimensions.md) |
| Nyt datakvalitetstjek | [dqa/README.md](../architecture/dqa/README.md) |
| Ny CodeSystem/ConceptMap (organisationshierarkier) | [codesystem_conceptmap_guide.md](../implementation/codesystem_conceptmap_guide.md) |
| Performanceproblem i map/dim/fct | [03b](03b_DSA_arkiv_til_dimensionel_model.md), afsnittet om performance |
| Adgangsstyring på DSO | [sikkerhedsmodel.md](../implementation/sikkerhedsmodel.md) |
| Ændring i navngivning | [navngivningsstandard.md](../implementation/navngivningsstandard.md) |

## Konventioner i repoet

Uanset opgavetype:

1. **Lav en branch efter konventionen:** `feature/*`, `bugfix/*`, `hotfix/*`,
   `release/*`.
2. **Hold merge request-beskrivelsen kort og læservendt:** hvad ændrer sig, og
   hvorfor. Udviklingshistorik og fejlsøgningsforløb hører ikke til i beskrivelsen.
3. **Dokumentation er en del af opgaven, ikke en efterfølgende oprydning.** Se
   [05 — Udfyld dokumentation](05_udfyld_dokumentation.md).

VS Code-serveren, feature-branch-flowet og hvordan du får din egen Dagster code
location på Dev: [Kom i gang](../../../docs/onboarding.md).

## Tjekliste før du går videre

- [ ] Opgavetypen er identificeret (A, B, C eller D)
- [ ] Forretningsbehovet er formuleret i én sætning
- [ ] For ny kilde: adgang, forbindelse og persondatavurdering er på plads
- [ ] For ny tabel: alle seks spørgsmål om tabellen er besvaret
- [ ] Sporet er valgt (FHIR / dimensionel / dm)

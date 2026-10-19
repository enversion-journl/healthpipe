<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# 05 — Udfyld dokumentationen

Dokumentation er en del af opgaven, ikke en oprydning bagefter. Grunden er praktisk:
det, der står uden for repoet — i en chattråd, en mail eller et opgavesystem —
følger ikke med koden. Repoet gør. Ligger en beslutning kun uden for repoet, er den
i praksis tabt.

Dette kapitel beskriver de fire dokumentationsartefakter, en ændring kan udløse, og
hvornår hver af dem er påkrævet.

| Artefakt | Påkrævet når | Hvor |
|---|---|---|
| **Lineage-YAML** | Tabellen synkroniseres til DSO (`dim`, `fct`, `dm`, `fhir`, `bridge`) | `etl/data_lineage/<skema>/` |
| **Kildesystem-beskrivelse** | Ny datakilde, eller ny viden om et kildesystems adfærd | `etl/docs/source_systems/{kilde}/` |
| **Beslutningslog** | Du har truffet et fortolkningsvalg, der ikke kan læses ud af koden | `etl/docs/contracts/` |
| **DQA-konfiguration** | Ny tabel i et lag, der har tjek på sig (stort set alle) | `etl/dqa/config/` |

---

## 1. Lineage-YAML — hvor kommer data fra?

Lineage-filerne dokumenterer, hvor hver kolonne stammer fra, sporet helt tilbage til
kildesystem og kildetabel. De er ikke kun dokumentation: en CI-kontrol validerer
dem mod DDL'en, DQA udleder referentielle tjek af dem, og de driver den interaktive
visualisering af stjerneskemaet.

**Én fil per tabel**, i mappen der svarer til SQL-skemaet:

```
etl/data_lineage/
├── bridge/   # bro-tabeller (mange-til-mange)
├── dim/      # dimensioner
├── dm/       # datamart-tabeller (fakta + Egenskaber)
├── fct/      # facts
└── fhir/     # flade FHIR-tabeller
```

### Skabelon

```yaml
dso_lineage:
  TableName: dim.MinTabel          # påkrævet — udledes IKKE af filnavnet
  Type: dimension                  # dimension | fact | fhir | bridge
  Domain: [ERP]                    # fx [ERP], [FHIR], [Shared]
  Description: "Kort beskrivelse på dansk"
  Grain: "Én række pr. ..."        # på dansk

  SourceSystems:
    - sydax: [Kreditor.Kreditor]   # system: [skema.tabel, ...]

  Columns:
    dw_ek_MinTabel:      { Type: PK, DataType: INT }         # entity key → ingen Source
    dw_ek_Organisation:  { Type: FK, DataType: INT }
    navn:                { DataType: NVARCHAR(200), Source: [sydax.Kreditor.Kreditor.navn] }
    beloeb:              { Type: Measure, DataType: DECIMAL(18,4), Source: [...], Expr: "sum af ..." }
    dw_timestamp_insert: { Type: Audit, DataType: DATETIME2 }
```

### De fire regler, CI håndhæver

1. **`Source` skal pege på et kildesystem — aldrig på en DWH-beregnet tabel.**
   `arc.*`, `stg.*`, `pre_stg.*`, `map.*`, `dim.*`, `fct.*` er *transformationstrin*,
   ikke kilder. Skal en kolonne spores, følg transformationen hele vejen tilbage til
   kildesystemets kolonne, og beskriv afledningen i `Expr`.
2. **Entity keys har ingen `Source`.** `dw_ek_*` og `dw_sk_*` tildeles i map-laget
   og har ingen oprindelse i et kildesystem. Skriv dem helt uden `Source` — ikke med
   `map.*` som kilde.
3. **Referencer skal kunne slås op case-sensitivt** til `system.skema.tabel.kolonne`, hvor
   `system` og `skema.tabel` er deklareret i filens `SourceSystems`.
4. **Hver relationsformet kolonne skal erklære sin hensigt.** En FHIR-`*_id` eller en
   `dw_ek_*`, der ikke er PK, skal have præcis én af: `Type: PK`, `Type: FK` (evt. med
   `FK-Target`), eller `FK-None: true`. Ellers fejler CI.

Fuld feltreference, FK-konventioner og eksempler:
[data_lineage_guide.md](../implementation/data_lineage_guide.md) (på dansk).

### Hvornår skal filen opdateres?

- Ny DSO-synkroniseret tabel
- En kolonnemapning ændres i DSA/DSO-SQL eller i FHIR-udfladningen
- Kildesystemer, grain eller FK-mål ændres

---

## 2. Kildesystem-beskrivelse

En mappe per kildesystem: `etl/docs/source_systems/{kilde}/`. Mappen er et
**skemakatalog over kildesystemet** — hvilke tabeller findes, hvordan hænger de
sammen, og hvordan opfører systemet sig. Den beskriver kilden, ikke vores
indlæsning, og dækker derfor gerne også tabeller, vi endnu ikke henter.

Vores egne valg (full load eller delta, `merge_keys`, delete detection) hører
**ikke** til her — de står i extract- og arkivkonfigurationen, som er den varige
registrering af dem. Det, der hører til her, er iagttagelser om kilden, som en
fremtidig læser ikke kan slutte sig til af kildens eget skema.

### Skabelon

```markdown
# {Kildesystem}

## Om systemet
Hvad er det for et system, hvem ejer det, hvad indeholder det, hvor ofte opdateres det.

## Adgang
Type (on-prem MSSQL / Synapse / Oracle / fildrop), forbindelsesmåde,
navn på servicekonto (aldrig credentials), hvem der giver adgang.

## Særlige forhold
Tegnsæt, tidszoner, kendte datakvalitetsproblemer, hvordan systemet
opfører sig ved fejl.

## Tabeller
Tabeloversigt med grain og primærnøgler, og relationerne mellem tabellerne.

## Det man ikke kan se af skemaet
Modification timestamps der ikke opdateres ved rettelser. Hvordan sletninger
repræsenteres. Kodelister. Felter der betyder noget andet, end navnet antyder.
Felter der står tomme. Tabeller der ser brugbare ud, men ikke er det.
```

Eksempler på niveauet, der har vist sig brugbart:
[`source_systems/ifdb/ifdb_schema.md`](../source_systems/ifdb/ifdb_schema.md) og
[`source_systems/bom_ifdb/bom_ifdb.md`](../source_systems/bom_ifdb/bom_ifdb.md).

Læg gerne det rå kildeskema med (`.sql`-dump) — flere kilder har det allerede.

---

## 3. Beslutningsloggen

Beslutningsloggen dokumenterer **fortolkningsvalg**: alt det, man *ikke* kan læse ud
af et lineage-diagram eller en tabeldefinition. Hvorfor en kode blev læst fra det
ene felt og ikke det andet. Hvorfor en række blev bevaret frem for frasorteret. Hvad
der bevidst ikke er implementeret. Hvilke antagelser der er verificeret mod
virkelige data.

**Tilføj en beslutning, når:**

- du valgte mellem to lige rimelige fortolkninger af kildens data
- du bevarede noget, det gamle system frasorterede (eller omvendt)
- du bevidst udelod noget
- du målte dig frem til et svar, der modsagde forventningen

**Tilføj ikke beslutninger om** navngivning, filstruktur, eller ting koden allerede
siger tydeligt.

Loggen ligger i [`etl/docs/contracts/`](../contracts/), opdelt per domæne, med
[beslutningslog_principper.md](../contracts/beslutningslog_principper.md) som
indgang og de bærende principper.

### Skabelon for en beslutning

```markdown
**Kort konstatering af beslutningen, i fed.**

Begrundelse: hvorfor dette valg og ikke alternativet. Hvis der ligger en måling
bag, står tallet her.

Aftager: hvad den, der læser data, skal gøre anderledes på grund af beslutningen.

Status: Fastlagt / Foreløbig / Kendt begrænsning indtil ...
```

For principielle valg bruges den udvidede form fra
[beslutningslog_principper.md](../contracts/beslutningslog_principper.md):
*Valgt* / *Alternativ* / *Begrundelse* / *Betydning for aftager* / *Status*.

---

## 4. DQA — dokumentation, der kører

Et DQA-subject er en formel beskrivelse af, hvad der skal være sandt om en tabel:
dens grain, dens nøgler, og hvad den skal stemme overens med. Det er derfor både et
tjek og en dokumentationsartefakt — og i modsætning til prosa forældes det ikke uden
at nogen opdager det.

Tilføj som minimum:

| Lag | Tjek |
|---|---|
| `arc.*` | `archive_reconciliation` — alle aktuelle kildenøgler findes i arkivet, og sletninger er propageret |
| `dim.*` / `fct.*` | `uniqueness` på grain, `referential_integrity` (udledes af lineage-YAML'en), `sentinel_rate` på `-1`-fremmednøgler |
| `fhir.*` | `source_reconciliation` mod arkivet, `fhir_server_reconciliation` |
| `dm.*` | rækkeparitet fakta↔arc og fakta↔Egenskaber, kontroltotaler, freshness på leverancer |
| DSO | `dso_reconciliation` — DSA og DSO stemmer række for række |

Konfigurationen ligger i `etl/dqa/config/`. Tag altid nøglekolonner **fra
konfigurationsobjektet** (fx `ArchiveMergeConfig.merge_keys`) frem for at skrive dem
af — det er dét, der holder tjekket i takt med tabellens faktiske grain.

Se [dqa/README.md](../architecture/dqa/README.md) (på engelsk).

---

## 5. Hold indekserne opdaterede

| Fil | Hvornår |
|---|---|
| [`etl/docs/README.md`](../README.md) | Når du tilføjer eller fjerner et dokument under `etl/docs/` |
| Dette kapitelsæt (`etl/docs/basismodel/`) | Når en regel eller en arbejdsgang her ændrer sig — primærdokumentationen skal være opdateret og korrekt |
| `etl/docs/source_systems/{kilde}/` | Når vi lærer noget nyt om kildesystemets skema eller adfærd |

Er et dokument blevet historisk (en plan, der er gennemført; en analyse, der beskrev
en tilstand), flyt det til `etl/docs/archive/` frem for at lade det stå og se
gyldigt ud.

---

## Tjekliste før merge request

- [ ] Lineage-YAML oprettet eller opdateret for hver DSO-synkroniseret tabel, jeg har rørt
- [ ] `Source`-referencer peger på kildesystemer, ikke på DWH-lag
- [ ] Iagttagelser om kildens adfærd, der ikke fremgår af dens skema, er skrevet ned i `source_systems/`
- [ ] Fortolkningsvalg er skrevet i beslutningsloggen — med begrundelse og aftagerkonsekvens
- [ ] DQA-subject tilføjet eller udvidet
- [ ] Dokumentationsindekset er opdateret, hvis jeg har tilføjet eller flyttet et dokument
- [ ] MR-beskrivelsen er kort og læservendt: hvad ændrer sig, og hvorfor

<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# dw-nøgler på de flade fhir-tabeller: valg af løsning for trin 2

**Status:** Besluttet: mulighed B
**Dato:** 2026-10-09, opdateret 2026-10-15

## Beslutning

**Vi har valgt mulighed B:** `dw_ek_Borger`, `dw_sk_Borger` og `dw_ek_Organisation` lægges
direkte på de flade `fhir.*`-tabeller. Én factory laver ét asset pr. tabel, som slår nøglerne
op i `dim.Borger` (via `fhir_Patient_id` og rækkens tidspunkt) og i `dim.Organisation` (via
`fhir_Organization_id`). Det opdaterer kun rækker, hvor en nøgle ændres.

Nøglerne findes i dag på `fhir.EpisodeOfCare` (borger og organisation) og `fhir.Location`
(kun organisation). Andre tabeller får dem ved at blive tilføjet til `DW_KEYS_CONFIG`.

Begrebet **trin 2** betyder her det nye skridt mellem SoF-indlæsningen (trin 1) og DSO-sync
(trin 3), der sætter dw-nøglerne på de flade tabeller.

Fortolkningsvalgene, som en aftager skal kende, står i beslutningsloggen
(`etl/docs/contracts/beslutningslog_tvaergaaende.md`, "Flade tabeller").

## De to muligheder

**A: view + persisteret kopi.** Et view over `fhir.<R>` slår nøglerne op. Viewet MERGE's ind i
en persisteret kopi med dw-kolonnerne, og DSO-sync kører fra kopien. Den rå SoF-tabel røres ikke.

**B: dw-kolonner på `fhir.<R>`.** Kolonnerne lægges på SoF-tabellen med DEFAULT -1. Et UPDATE
efter SoF-indlæsningen sætter dem. DSO-sync er uændret, fordi den samme DDL køres på staging og DSO.

A og B giver de samme nøgler. A er hurtigere ved første fyldning og har en enklere delta, men
lagrer tabellen to gange og kræver en ny kildetabel i DSO-synken. B lægger kun få procent til
tabellen og lader synken være, som den er.

## Opslaget

`dw_ek_Borger` er borgerens identitet og slås op uden tidspunkt og uden sletteflag.
`dw_sk_Borger` er den levende version, der gælder på rækkens tidspunkt:

```sql
OUTER APPLY (
    SELECT TOP (1) d.dw_ek_Borger
    FROM dim.Borger AS d
    WHERE d.fhir_Patient_id = f.patient_id
    ORDER BY d.dw_is_deleted, d.dw_valid_from DESC
) AS borger_entity
OUTER APPLY (
    SELECT TOP (1) d.dw_sk_Borger
    FROM dim.Borger AS d
    WHERE d.fhir_Patient_id = f.patient_id
      AND d.dw_is_deleted = 0
      AND f.<tidspunkt> >= d.dw_valid_from
      AND f.<tidspunkt> <  d.dw_valid_to
    ORDER BY d.dw_valid_from DESC, d.dw_sk_Borger DESC
) AS borger_version
```

`<tidspunkt>` er rækkens hændelsestidspunkt som DATETIME2(0), for EpisodeOfCare
`period_start`. Det er klokkeslættet, som IFDB leverer det (dansk lokaltid); et eventuelt
offset fjernes ved SoF-indlæsningen uden omregning. Det er ikke `dw_timestamp_update`.

Versionsopslagets sortering og filter på `dw_is_deleted` følger `dbo.GetBorgerSK_TVF`. Det er
nødvendigt, fordi soft-deletede versioner i `dim.Borger` kan overlappe en levende version.

`dw_ek_Organisation` er `dim.Organisation.dw_ek_Organisation`, hvor `fhir_Organization_id`
svarer til rækkens organisationsreference.

**-1 betyder "ingen nøgle":** referencen mangler, den findes ikke i dimensionen, eller (kun
`dw_sk_Borger`) rækken har intet tidspunkt eller ingen levende version på tidspunktet.
`dim.Organisation` har ingen -2-række, så -2 bruges ikke.

**Sletteflaget på fhir-rækken indgår ikke.** Slettemarkerede rækker indlæses, genberegnes og
opdateres som levende rækker. En række, hvis borger kun findes som slettet, får
`dw_ek_Borger`, men `dw_sk_Borger` = -1. Der fryses ingen nøgler.

**Fremtidige tidspunkter** får borgerens aktuelle version, fordi den gælder til 9999-12-31.

## Mulighed B i detaljer

### Ét asset pr. tabel fra én fælles factory

En factory laver ét asset pr. fhir-tabel ud fra en konfiguration, på samme måde som
`create_sof_dfm_asset` laver ét SoF-asset pr. view:

```python
DW_KEYS_CONFIG = {
    "episode_of_care": _config(
        "episode_of_care_view",                     # tabel og primærnøgle fra SoF-konfigurationen
        "episode_of_care",
        patient_column="patient_id",
        time_column="period_start",                 # DATETIME2(0), normaliseret af SoF
        organisation_column="managing_organization_id",  # den ansvarlige organisation
    ),
    "location": _config(
        "location_view",
        "location",
        organisation_column="managing_organization_id",  # ingen borger-kolonner
    ),
}
```

Hvert asset afhænger af sit SoF-asset og af `dim_Organisation`, og af `dim_Borger`, når tabellen
har borger-nøgler. `fhir_dw_keys_episode_of_care` afhænger fx af `sof_episode_of_care_dfm`,
`dim_Borger` og `dim_Organisation`, og `dso_sync_fhir_episode_of_care_dfm` afhænger af det.
Hver tabel har altså sit eget asset, sin egen kørsel og sine egne checkpoints, så en fejl på
én tabel ikke stopper de andre.

### Checkpoints: tre pr. tabel

Der er ikke ét fælles checkpoint. Hvert asset har ét DFM-checkpoint pr. kilde til ændringer,
fordi kilderne har hver sit ur. En tabel uden borger-nøgler, fx Location, har ikke
borger-checkpointet.

| Checkpoint | Læser | Sættes til |
|---|---|---|
| `<R>_DWKeys` | `fhir.<R>.dw_timestamp_update` | `MAX(dw_timestamp_update)` i tabellen efter assettets egne skrivninger |
| `<R>_DWKeys_Borger` | `dim.Borger.dw_timestamp_update` | Kørslens start, holdt tilbage til starten af en `dim_Borger`-kørsel, der stadig er i gang (`DfmManager.safe_reader_checkpoint`) |
| `<R>_DWKeys_Organisation` | `dim.Organisation.dw_timestamp_update` | Kørslens start minus én time |

Mangler et af checkpointene, eller kører assettet i `full_load`, gennemgås hele tabellen.

Kandidaterne findes med `dw_timestamp_update >= checkpoint`, ikke `>`. Tidsstemplerne i
`fhir.<R>` og `dim.Borger` og kørslens start er afrundet til hele sekunder, så en skrivning i
samme sekund som checkpointet får samme værdi. Med `>` ville den blive sprunget over. En række,
der genberegnes en ekstra gang, skrives ikke, når nøglerne er uændrede.

**fhir-checkpointet sættes efter egne skrivninger.** Assettet skriver selv i SoF's tabel og
flytter `dw_timestamp_update`. Med et checkpoint fra før kørslen ville næste kørsel se alle de
rækker, den selv lige har opdateret. Det kræver, at SoF ikke skriver i tabellen, mens trin 2
kører. Det sikres med en applikationslås pr. tabel
(`core.mssql_application_lock.application_lock`). Alle SoF-assets fra
`create_sof_dfm_asset` tager låsen for deres tabel, også dem uden dw-nøgler endnu, og trin 2
tager den for sin tabel. `sof_organization` bygges ikke af factoryen og tager ikke låsen, så
`fhir.Organization` kræver låsen, før den kan få dw-nøgler. To kørsler, der vil skrive i samme
tabel, venter derfor på hinanden i op til 5 minutter, hvorefter den sidste fejler.

**borger-checkpointet holdes tilbage.** `dim_Borger` stempler `dw_timestamp_update`, når dens
MERGE starter, men rækkerne bliver først synlige ved commit. Uden tilbageholdelse kunne trin 2
springe en borgerændring over, som var undervejs, mens trin 2 kørte.

**organisations-checkpointet ligger en time før kørslens start.** `dim_Organisation` skriver
ingen DFM-kørselslog og committer først til sidst, så en kørsel, der er i gang, kan ikke ses.
Marginen dækker den; rækker, der derved genberegnes igen, skrives ikke, når nøglerne er
uændrede.

### Hvilke rækker genberegnes, og hvilke skrives

En række **genberegnes**, når fhir-rækken er ændret siden fhir-checkpointet, når dens borger
er ændret i `dim.Borger` siden borger-checkpointet, eller når dens organisation er ændret i
`dim.Organisation` siden organisations-checkpointet. Den **skrives** kun, hvis mindst én
af `dw_ek_Borger`, `dw_sk_Borger` eller `dw_ek_Organisation` får en ny værdi. Kun skrevne
rækker får `dw_timestamp_update` flyttet, og kun de kommer med i næste DSO-delta.

Eksempel: En borger har 14 forløb. CPR-registret leverer en flytning pr. 1. marts 2026, så
`dim.Borger` får en ny version fra den dato, og `dim_Borger` stempler alle borgerens versioner.

1. Næste kørsel af trin 2 finder borgeren blandt de ændrede og genberegner alle 14 rækker.
2. 3 forløb starter efter 1. marts og får den nye `dw_sk_Borger`. De skrives og får nyt
   `dw_timestamp_update`.
3. De 11 andre giver samme nøgler som før og skrives ikke.
4. DSO-sync overfører de 3 rækker.

En navneændring (type 1) stempler også alle borgerens versioner. Alle 14 rækker genberegnes,
men ingen skrives, fordi nøglerne er uændrede.

### Ingen genforsøg af -1

Hver kørsel tæller -1 pr. nøgle blandt de rækker, den genberegner, og logger en advarsel, når
der er nogen. Rækker med -1 genforsøges ikke; de fanges, når en af kilderne ændrer sig:

* **Ukendt patient:** Når borgeren senere oprettes i `dim.Borger`, får rækken et nyt
  `dw_timestamp_update`, og borger-deltaet finder den. `dim_Borger` stempler alle versioner af
  en borger, den berører, også ved soft-delete og ved type 1-ændringer.
* **Manglende tidspunkt:** Rækken får `dw_sk_Borger` = -1, og det ændrer sig ikke, før SoF
  leverer et tidspunkt. Så er rækken ændret og fanges af fhir-deltaet.

Derfor er der heller ikke brug for et filtreret indeks på `dw_sk_Borger = -1`.

## Indeks

| Tabel | Indeks | Formål |
|---|---|---|
| `dim.Borger` | `(fhir_Patient_id, dw_valid_from, dw_valid_to) INCLUDE (dw_ek_Borger, dw_sk_Borger, dw_is_deleted)` | Opslaget; uden det scanner hvert opslag hele `dim.Borger` |
| `dim.Borger` | `(dw_timestamp_update) INCLUDE (fhir_Patient_id)` | Delta: ændrede borgere |
| `dim.Organisation` | `(fhir_Organization_id)` | Organisationsopslaget |
| `fhir.<R>` | `(patient_id)` | Find rækkerne for ændrede borgere; findes allerede |
| `fhir.<R>` | `(dw_timestamp_update, dw_timestamp_insert)` | Find ændrede fhir-rækker; findes allerede som watermark-indeks |

Indekset på `fhir.<R>(patient_id)` får ingen INCLUDE. Kandidatforespørgslen læser kun
`patient_id` og primærnøglen, og genberegningen læser og skriver alligevel selve rækken via
primærnøglen. Nøglekolonner i INCLUDE ville bare skulle opdateres ved hver skrivning.

**Genopbygning efter første fulde gennemgang.** Kolonnerne tilføjes med en standardværdi, og
den første gennemgang skriver nøglerne ind i hver række, så siderne deler sig, og tabellen
vokser markant. Det sker kun én gang pr. tabel; bagefter opdateres nøglerne på stedet.
`fhir_dw_keys_index_maintenance` kører efter DSO-synken i samme job og genopbygger
de indeks på dw-nøgletabellerne på staging og DSO, der har mindst 1.000 sider og mere end
30 % fragmentering, med fill factor 90.

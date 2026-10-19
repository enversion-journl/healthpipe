<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# SDS LPR3 Grunddata — kilde og ingestion

Beskriver SDS' regionsleverance LPR3 Grunddata, og hvordan den indlæses i ext og arc.
Den styrende specifikation er [[internal ticket]](https://enversion.visualstudio.com/healthpipe/_workitems/edit/27627);
ingestion er [user story [internal ticket]](https://enversion.visualstudio.com/healthpipe/_workitems/edit/29891).
Nøglevalg og deres begrundelse står i [nøglekontrakten](../../contracts/lpr3_ingest_noegler.md).

## Begreber

| Begreb | Betydning |
|---|---|
| U / M | Ugentlig (U) og månedlig (M) leverance. Begge er komplette snapshots; M erstatter ugens U |
| Cutoff | Dataafskæringsdatoen i medlemmernes filnavne (`_yyyyMMdd`); leverancen indeholder data til og med den |
| `DW_SK_*` | SDS' versionsnøgle. Gælder kun inden for én leverance og gemmes aldrig i arc |
| `DW_EK_*` | SDS' entitetsnøgle. Stabil mellem leverancer, men kan omnummereres ved en fuld SDS-genindlæsning |
| `LPR_Entity_ID` | LPR3-indberetningens UUID, tildelt af det indberettende system |
| Dim/fct-par | SDS fordeler ni entiteters identitet (Dim) og attributter (fct) på to tabeller med samme rækkeantal |
| Funktionel afhængighed | Målingen af, at et sæt kolonner entydigt bestemmer en række; grundlaget for hver arkivnøgle |
| Featurebeslutninger B1–B16 | Truffet på [internal ticket] den 2026-09-20. Dette dokument henviser til B2 (merge-nøglen er `LPR_Entity_ID`), B3 (afledte entiteter nøgles på UUID-relationer), B4 (et Dim/fct-par bliver én arkivtabel) og B6 (DimBorger og DimAdresse arkiveres komplet) |
| Nøglebeslutninger D1–D6 | De målte undtagelser fra B2/B3; se [nøglekontrakten](../../contracts/lpr3_ingest_noegler.md) |

## Leverance

| Egenskab | Kontrakt |
|---|---|
| Kilde | SDS' regionsleverance LPR3 Grunddata, region 1083; SDS' eget stjerneskema |
| Placering | `LPR3_BASE_DIR`, forventet `/mnt/ftproot/SDS_NGC_Sundhedsdata_LPR_Grunddata/Fra-SDS/` |
| Adgang | Read-only CIFS-mount; mount og `LPR3_BASE_DIR` skal begge findes i kodeserverens allokering |
| Pakke | `Region1083_LPRGrunddata_<U\|M>_<yyyyMMddHHmmss>.tar.bz2` |
| Indhold | 47 datafiler, 47 `Metadata_*.csv`, én `SDP_ExportLog`; 26 regionale og 21 nationale tabeller |
| Rækkefølge | Alle datafiler ligger før Metadata og ExportLog i tar-filen |
| Format | cp1252, semikolon, quoted felter; datetime med op til syv decimaler, decimal-komma, binær-id som hex op til Metadata-bredden, UUID med bindestreger |
| Cutoff | Datosuffiks på medlemmerne; ingen LEVERANCEINFO-fil |
| Volumen | Ca. 22 GB komprimeret, 146 GB CSV og 733–738 mio. rækker pr. leverance (september 2026) |

En manglende eller ulæselig mappe er en fejl. Koden bruger ingen fallback-sti og
fortolker ikke et manglende mount som en tom leverance. Leverancerne streames uden at
skrive udpakkede datafiler til disk. Parallelle pas betaler hver for en fuld
bz2-dekomprimering; oversprungne medlemmer sparer CSV-afkodning, ikke dekomprimering.

Metadata angiver `VARBINARY(16)` for de binære id'er, men `DimResultatIndberetning.ResultatIndberetning_ID`
indeholder ét id på 6 byte. Formatkontrollen følger derfor den deklarerede maksimalbredde.
Ulige antal hextegn, andre tegn, `0x`-præfiks og for lange værdier afvises; værdien
bevares uden padding.

## Det man ikke kan se af skemaet

Indberetningsobjekterne ændres på plads hos SDS. Både forretningsindhold og fejlflag kan
ændre sig uden ny `DW_SK`, så arkivet upserter med indholdsbaseret ændringsdetektion.
`DW_IsDeleted` og `DW_IsInferred` er 0 i alle profilerede rækker. Sletninger findes ved
at sammenligne stabile nøgler mellem komplette snapshots.

`DW_SK_*` og `DW_EK_*` indgår kun i nøgler i de undtagelser, nøglekontrakten
godkender. `Kontakt_ID` og de tilsvarende binære id'er er ikke indberetningens UUID;
den hedder `LPR_Entity_ID`.

Type 2-stamdata har forretningsgyldighed. En fact-reference bevarer den refererede
version via naturlig nøgle og `DW_ValidFrom`; et opslag på kun den aktuelle række er
forkert. `DW_IsCurrent = 1` er ikke det samme som åben `DW_ValidTo`.

Nationale bridges og regionale entiteter har kendte manglende referencer. De opløses med
LEFT JOIN og tælles; de forsvinder ikke ved INNER JOIN. En uløst relation er ikke i sig
selv en entydig arkivnøgle: flere rækker samles aldrig på samme NULL eller sentinel.

## Arkivkontrakt

Alle 47 kildefiler bevares i ext. De ni Dim/fct-par samles til én arkivtabel pr. entitet
(B4), i alt 38 arkivtabeller. Det er en kildespecifik afvigelse fra
[DSI-kontraktens regel 4](../../basismodel/02_DSI_kilde_til_arkiv.md#dsi-kontrakten--fem-regler),
fordi SDS har fordelt samme entitets identitet og attributter på to tabeller.
Sammenlægningen og opløsningen af referencer giver en arkividentitet, der tåler SDS'
omnummerering.

Paritetskontrollen beviser entydighed og samme nøglemængde på begge sider før merge;
samme rækkeantal alene beviser ikke et 1:1-join. Arc bærer kildens indhold og fejlflag,
opløste referencer, leveranceoplysninger og repoets audit-kolonner. `DW_SK_*`,
`DW_IsDeleted` og `DW_IsInferred` udelades; `DW_EK_*` og SDS' audit-id'er beholdes som
attributter, der ikke indgår i ændringsdetektionen. Vores `dw_is_deleted` er arkivets
snapshot-diff.

| Klasse | Merge-identitet |
|---|---|
| Indberetningsobjekter (kontakt, forløbselement, procedure-, markør- og resultatindberetning) | `LPR_Entity_ID` |
| KontaktLokation og Betalingsoplysning | Kildens binære id (D2) |
| Helbredsforløb | Rod-forløbselementets UUID, ellers den entydige synlige begyndelse eller hash af begyndelsernes sæt |
| Sygehusophold | Første kontakts UUID, ellers den entydige kontakt eller hash af kontakternes sæt |
| Diagnose- og proceduregruppe | SHA2-256 over det sorterede sæt af kodeversioner; teknisk nøgle for uløste og kolliderende grupper (D1, D3) |
| Resultat | Indberetnings-UUID + `Resultat_Nr` |
| Type 2-stamdata | Naturlig nøgle + `DW_ValidFrom`; CPR hashes i nøgler; DW_EK kun for tvetydige entiteter (D5) |
| Bridges | Hash af de opløste relationer; teknisk reference for uløste nationale referencer (D4) |

Gruppehashens form følger `_gruppe_hash` i `DSA/SQL/staging/drg_bridge_sql.py`:
SHA2-256 over `STRING_AGG` af et distinkt, sorteret sæt af faste medlemstokens med `|`
som separator. Arc gemmer digesten som `BINARY(32)`. Samme hashform er ikke bevis for
identiske medlemmer på tværs af modellerne: DRG's proceduregrupper medtager producerende
organisation, mens SDS' kodegruppe består af kode, type og parent.

## Leverancedetektion og vagter

Én DFM-enhed, `lpr3_leverance_1083`, dækker U og M. Identiteten er pakkens tidsstempel
som 19-tegns ISO-datetime via `DSI/connectors/delivery_detection.py`. Den nyeste pakke
efter checkpointet vælges; cutoff kontrolleres særskilt, så en ældre dataleverance ikke
kan overskrive nyere data.

`delivery_on_or_before` vælger i stedet den nyeste pakke på eller før datoen uanset
checkpointet og flytter aldrig checkpointet. Den bruges til genkørsel og indhentning.
Datoen er pakkens egen dato; en tidligere dato vælger en ældre pakke, som afvises på cutoff.

`ext.lpr3_leverance_manifest` afspejler den aktuelle kørsel. Uden ny leverance er
manifestet tomt, og ext og arc berøres ikke. Gate-assetet flytter først checkpointet,
når alle 47 kilder er arkiveret af den aktuelle kørsel.

| Vagt | Adfærd |
|---|---|
| Metadata- og header-drift | Afvises før ext-skrivning; navn, rækkefølge og SQL-type valideres mod datasæt-kontrakten |
| ExportLog | Eksakt rækkeantal og antal CSV-bytes pr. fil |
| Formater og encoding | Dato, decimal-komma, hex, UUID og mojibake; første fejl stopper med tabel, kolonne og rækkenummer, aldrig værdier |
| Manglende eller tom kilde | En manglende kildefil, eller en påkrævet `Dim*`-fil med 0 rækker, springer alle afhængige merges over. Ingen FK sættes til NULL, intet soft-slettes, og checkpointet flyttes ikke |
| Paritet og merge-nøgle | Afvises før merge ved manglende 1:1-paritet eller dubletter |
| Cutoff | En ældre cutoff end den senest arkiverede afvises før skrivninger |
| Soft-delete | 0,5 % af de levende rækker, 3 % for markører og betalingsoplysninger; under 1.000 levende rækker gælder 1.000. Alle tabeller med identitetsnøgle tjekkes i `lpr3_archive_noegler`, før første merge, og hver tabel igen før sin egen merge |
| Genindlæsning | Et fald i min eller antal distinkte `DW_ID_Audit_Insert` pr. tabel giver en advarsel, ikke en fejl |
| Alder | `lpr3_leverance_alder` fejler, når den nyeste pakke på sharet er over 12 dage gammel |

Soft-delete-grænserne bygger på U 2026-09-07 → M 2026-09-15: kontakter 0,06 %,
procedurer 0,03 %, markører 1,33 % og betalingsoplysninger 0,23 % forsvundet.
Fra M 2026-08-12 til M 2026-09-15 forsvandt 3,69 % af markørerne. En overgang mellem to
månedsleverancer uden de mellemliggende ugeleverancer overskrider derfor markørgrænsen;
se [Drift og genopretning](#drift-og-genopretning).

## Drift og genopretning

`lpr3_ingest_job` vælges af `lpr3_ingest_daily_schedule` kl. 02:30. Schedulet springer
over, mens et tidligere `lpr3_ingest_job`-run er aktivt. Pakkealderen kontrolleres af
`lpr3_freshness_job` kl. 07:00, uafhængigt af om ingest lykkes. Begge schedules er
registreret som stoppede og aktiveres manuelt.

**Ingen genoptagelse.** Landing, manifest og gate er bundet til det run, der startede
leverancen. "Re-execute from failure" afvises derfor altid. Et run, der dør, fx når
kodeserverens allokering redeployes, markeres som annulleret i Dagster; derefter
genindlæser et nyt run hele leverancen. Et annulleret eller fejlet run efterlader arkivet
konsistent pr. tabel, og checkpointet flytter sig ikke.

**Afvist leverance.** En leverance, der afvises af en vagt, er fortsat den nyeste efter
checkpointet. Den daglige kørsel prøver den igen hver nat, indtil årsagen er løst, eller
SDS leverer en nyere pakke, som så vælges i stedet. Stop schedulet, mens årsagen undersøges.

**Pause i kørslerne.** Andelen af forsvundne rækker vokser med afstanden mellem arkivet
og den valgte leverance. Efter en pause kan soft-delete-vagten derfor afvise den nyeste
leverance, selv om den er korrekt. Der er to veje:

1. Indhent de mellemliggende leverancer, der stadig ligger på sharet, med en pinnet
   kørsel pr. leverance (`ops.lpr3_leverance_plan.config.delivery_on_or_before`). Den
   efterfølgende normale kørsel vælger den nyeste leverance og flytter checkpointet.
2. Kør med `ops.arc_lpr3_<tabel>.config.soft_delete_threshold` sat over det forventede
   antal for de berørte tabeller, efter at antallet er forklaret. Værdien gælder kun det
   run og læses også af forhåndstjekket i `lpr3_archive_noegler`.

## Implementering

`lpr3_datasets.py` læser alle 1.042 kildekolonner fra `lpr3_metadata.json`, herunder
precision, scale og bytebredder. De 47 ext-tabeller beholder Dim/fct i navnet, så de ni
par ikke kolliderer. `ARCHIVE_SOURCES` angiver de 38 arkivmål. `scripts/lpr3_schema.py`
høster Metadata og ExportLog fra en leverance, når SDS ændrer skemaet;
`scripts/lpr3_generate_contract.py` genererer arkiv-DDL fra kontrakten.

`lpr3.py` læser først kontrolfilerne, fordi SDS lægger Metadata og ExportLog efter
datafilerne. Derefter streamer fire disjunkte grupper hver deres tabeller med native bulk
copy; procedure-dimension og -fact har en gruppe for sig. Manifestet offentliggøres først
efter alle grupper. Manglende kildefiler fjernes fra ext og får ingen manifest-række.
Dagster-metadata registrerer kontrolpassets sekunder, hver workers sekunder samt rækker,
bytes og rækker/s pr. fil. Arkivets metadata registrerer merge-tid, nøglemetoder,
uopløste referencer og LPR3's reserverede byte i ext og arc.

LPR3-ext oprettes med PAGE-kompression før bulk copy med TABLOCK; arkivernes clustered
primærnøgler bruger også PAGE-kompression. De 38 merges kører sekventielt, så der kun
findes én typet kildekopi ad gangen.

`_lpr3_run.py` beskytter de fælles ext-tabeller mellem kørsler: en vedvarende run-ejer
beskytter mellemrummene mellem assets, og SQL-applikationslåse tillader samtidige workers
fra samme kørsel. En anden kørsel kan først overtage fra et afsluttet Dagster-run, når
ingen af dets SQL-workers stadig arbejder. En ukendt ejer afvises.

`meta.LPR3Leverance` (`tables/lpr3/01_meta.sql`) har én række pr. kildetabel og
leverance med kontroltotaler, run-id, merge-tællinger, merge-metode, audit-min og -antal,
fallback- og referenceoptællinger og status. `raekker_arc` er antallet af aktive
arkivrækker efter merge. Et run, der fejler, nedgraderer ikke en tidligere arkiveret
leverances audit eller cutoff.

## Persondata

DimBorger og DimAdresse arkiveres komplet (B6). CPR findes også på borger- og
klynge-bridgen, og ResultatVaerdi kan indeholde personoplysninger. Data forbliver i
ext og arc; logs, exceptions, testfixtures, nøgleeksempler og dokumentation indeholder
ingen virkelige persondata. CPR hashes, hvor det indgår i en nøgle. Fejlbeskeder
indeholder kun tabel- og kolonnenavne og antal.

## Målte nøgleegenskaber

Målt på M 2026-09-15 og U 2026-09-07, medmindre andet er angivet. Beslutningerne, der
bygger på målingerne, står i nøglekontrakten.

- `DimVaerdiSKS`: 6.746 aktuelle rækker har 6.678 forskellige `Vaerdi_Kode`, men 6.746
  forskellige (`Vaerdi_Kode`, `Vaerdi_SKS_Type`). SKS-typen er nødvendig i nøglen; med
  `DW_ValidFrom` er nøglen entydig i alle 7.598 versioner.
- Sygehusophold: 22.159.007 ophold, ingen med flere første kontakter, og ingen første
  kontakt delt af flere ophold. Første kontakts UUID dækker 99,94 %.
- Helbredsforløb: relationsbridgen har én rod pr. forløb, hvor den findes, men 506.146
  forløb har ingen rod i leverancen. Rod suppleret med første forløbselement dækker
  99,63 %. Med den entydige synlige begyndelse eller sæt-hash dækker nøglen alle forløb
  uden kollisioner.
- Diagnose- og proceduregrupper: 25.248.617 diagnosegrupper og 46 proceduregrupper har
  ingen bridge-rækker og dermed intet kodesæt (D1). 38.452 diagnose- og 48.979
  proceduregrupper deler komplet kodesæt med en anden gruppe (D3, D6).
- Nationale bridges: 1.175.461 proceduregrupper i bridgen findes ikke i dimensionen (D4).
- Borger og Organisation: 312 CPR-par og to organisationsgrupper (2.396 entiteter) har
  samme naturlige identitet og `DW_ValidFrom` (D5).
- Resultat: (indberetnings-UUID, `Resultat_Nr`) er entydig og dækkende for 14.429.227 rækker.
- KontaktLokation og Betalingsoplysning: de binære id'er er entydige i U og M, og ingen
  id skifter `DW_EK` eller omvendt (D2).

## Driftsmålinger

Målt på `healthpipe_staging` på healthpipe-dev i september 2026 med fire extract-workers.

| Kørsel | Kontrolfiler og plan | Extract til manifest | Identitetstabeller | Arkiv-merges | I alt |
|---|---:|---:|---:|---:|---:|
| Første fulde load (U, bulk) | 0:36 | 3:18 | 0:39 | 2:13 | 6:10 |
| Ugentligt delta (U → U, MERGE) | 0:36 | 2:51–2:55 | 0:39 | 1:58–2:06 | 5:34–5:36 |
| Genkørsel af samme leverance | 0:37 | 2:53 | 0:39 | 2:02 | 5:35 |
| Ingen ny leverance | | | | | 0:03 |

Extract-tiden regnes fra kørslens start og inkluderer kontrolfilerne. En genkørsel af samme leverance gav 0 indsatte,
opdaterede og soft-slettede rækker i alle 38 tabeller og flyttede ikke checkpointet.
En kørsel uden ny leverance ændrede intet i ext eller arc ud over at tømme manifestet.

**Plads.** I hvile efter et delta fylder LPR3 150,3 GB i arc og 96,6 GB i ext, i alt
246,9 GB. Under første fulde load reserverede ext og identitetstabellerne 294,5 GB, før
arkivet blev fyldt.

**Ændringer pr. ugeleverance.** To U → U-deltaer med 6–8 dages afstand gav 1,7–3,3 mio.
indsatte, 0,2–0,3 mio. opdaterede og 0,6–1,0 mio. soft-slettede rækker af 457 mio.
aktive. Kontakt fik i det seneste delta 106.906 nye og 17.023 forsvundne rækker. MarkoerRegistrering
soft-slettede 1,55–2,37 % og Betalingsoplysning op til 0,47 %; alle øvrige tabeller under
0,17 %.

**UUID-genfindelse.** Målt mod aktive arkivrækker med samme cutoff i LPR3 og udredningsret.
Med forskellig cutoff falder genfindelsen, fordi den nyeste kilde bærer UUID'er, den anden
endnu ikke har leveret.

| Kilde og kolonne | LPR3-mål | Distinkte værdier | Genfundet |
|---|---|---:|---:|
| DRG T24 `KONTAKT_VAERDI` | kontakt eller procedureregistrering | 13.516.690 | 99,55 % |
| DRG T26 `KONTAKT_VAERDI` | kontakt eller procedureregistrering | 12.299.319 | 99,80 % |
| DRG T24 `PROCEDURE_VAERDI` | procedureregistrering | 43.956.804 | 99,80 % |
| DRG T26 `PROCEDURE_VAERDI` | procedureregistrering | 42.029.024 | 99,85 % |
| Udredningsret `C_ADIAG_KONTAKT` | kontakt | 1.775.843 | 99,88 % |
| Udredningsret `OBJEKTID_FORLOBELEMENT` | forløbselement | 2.674.445 | 99,65 % |
| Udredningsret `OBJEKTID_FORLOBSMARKOR` | markørregistrering | 8.251.488 | 99,77 % |
| Udredningsret `DW_EK_HelbredsForloeb` | helbredsforløb | 2.253.303 | 99,95 % |

DRG's `KONTAKT_VAERDI` er for forløbsprocedurer procedurens UUID, derfor de to mål. Det
laveste DRG-aktivitetsår er T24/2024 med 99,52 %. `kontakt_uuid` er lig `LPR_Entity_ID`
for alle kontakter.

**Uopløste referencer.** Antal NULL i hver af de 148 opløste referencer i arc er lig
antallet af sentinel-værdier (`-1` eller tom) plus kildeværdier uden match i den
refererede dimension. For de to grupperede bridges gælder det vægtet med
`antal_kilderaekker`. Helbredsforløb har én række mere end sin fact: SDS'
ukendt-række, hvor alle referencer er NULL. Uopløste referencer pr. leverance:

| Reference | Uopløste kildeværdier |
|---|---:|
| Sygehusophold → SygehusopholdEgenskaber (dimensionen leveres ikke) | 22,3 mio. (alle) |
| Procedure-bridge → Proceduregruppe (national bridge, D4) | 5,2 mio. |
| Forløbsrelation-bridge → Helbredsforløb | 0,9 mio. |
| Procedure-bridge → Procedure / parent | 556.000 / 243.000 |
| Helbredsforløb → sidste / første forløbselement | 89.000 / 33.000 |
| Forløbsrelation-bridge → forrige forløbselement | 69.000 |
| Sygehusophold → første / sidste kontakt / primærindlæggelse | 13.000 / 11.000 / 3.000 |

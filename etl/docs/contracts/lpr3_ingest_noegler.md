<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# LPR3 ingestion — målte nøgleafvigelser og beslutninger

Gælder ingestion af SDS LPR3 Grunddata.
Begreber, kildebeskrivelse og målinger: [LPR3](../source_systems/lpr3/lpr3_kilde.md).

Featurebeslutning B2 gør `LPR_Entity_ID` til merge-nøgle, og B3 nøgler de afledte
entiteter på deres UUID-relationer. D1–D6 er de målte undtagelser fra B2/B3: D1–D2 er
besluttet 2026-09-20 og D3–D6 2026-09-21. Kildepopulation, fejlflag, samlingen af
Dim/fct-par (B4) og øvrige krav består.

## D1 — uløste diagnose- og proceduregrupper

**Godkendt:** En dokumenteret teknisk nøgle er tilladt for uløste grupper.

Den komplette kontrol af M 2026-09-15 fandt 25.248.617 ikke-sentinel-diagnosegrupper
og 46 proceduregrupper uden bridge. Deres kildekolonner er udelukkende DW-/audit-felter;
der findes ikke et kodesæt at hashe. Fravær af en bridge må ikke fortolkes som et
kendt tomt sæt eller få kilderækkerne til at kollapse.

Fallback skal have et særskilt navnerum og en eksplicit afledningsmetode. Den bruger
gruppens tekniske kildeidentitet, bevarer hver kilderække og tælles ved hver leverance.
Et fuldt kodesæt bruger fortsat den afledte kodebaserede nøgle, når den er entydig.
Konflikter i kodebaserede nøgler skal måles, ikke skjules med vilkårlig deduplikering.

Begrænsning: en teknisk kildeidentitet kan skifte ved SDS-genindlæsning. Fallback giver
ikke et løfte om stabilitet under en sådan omnummerering. Genindlæsningsalarm
og kontroltotaler består og må ikke sættes ud af kraft for at få et
load igennem. Antal uopløste grupper og ændringer i afledningsmetode skal fremgå af
driftsmålingerne.

## D2 — KontaktLokation og Betalingsoplysning

**Godkendt betinget af måling:** Undersøg og dokumentér brug af henholdsvis
`Opholdadresse_ID` og `Betalingsoplysning_ID` som nøgle. Begge tabeller mangler den
`LPR_Entity_ID`, som featurebeskrivelsen forudsatte.

Den eksakte kontrol af hele U 2026-09-07 og M 2026-09-15
opfyldte betingelsen for det målte interval:

| Kontrol | KontaktLokation | Betalingsoplysning |
|---|---:|---:|
| U-rækker | 27.111.181 | 37.868.054 |
| M-rækker | 27.165.781 | 37.948.270 |
| Dublerede kilde-id'er i U eller M | 0 | 0 |
| Samme DW_EK, ændret kilde-id | 0 | 0 |
| Samme kilde-id, ændret DW_EK | 0 | 0 |

Id'erne blev valideret som præcis 16 byte og sammenlignet som normaliserede byteværdier,
så store og små hextegn ikke skabte falske forskelle.

De to id'er kan dermed bruges i den afgrænsede undtagelse. De betegnes binære kilde-id'er,
ikke LPR-indberetnings-UUID'er; målingen dokumenterer otte dage, ikke stabilitet under
enhver fremtidig SDS-genindlæsning. De fem tabeller, der faktisk har LPR_Entity_ID,
beholder den UUID-baserede politik. DW_EK og audit-id'er forbliver attributter uden for
change-detect.

## D3 — komplette grupper med kolliderende kodesæt

**Godkendt 2026-09-21, præciseret af D6 nedenfor:** Hver dimensionsgruppe i et kolliderende komplet kode/type/parent-sæt
bevares med hash af gruppens DW_EK i et særskilt `collision-source-id:v1`-navnerum.
Metoden er `technical_collision`; et entydigt komplet sæt beholder `code_set`.
Kun komplette grupper med en faktisk dimensionsrække deltager i kollisionskontrollen.

Den fulde M-måling fandt 38.452 ekstra diagnosegrupper og 48.979 ekstra proceduregrupper
med samme kode/type/parent-sæt. Selv alle kliniske forretningsfelter efterlader
11.225 henholdsvis 31.597 ekstra grupper. Disse tidligere målinger er ikke målinger
af D6's versions- og payloadkontrol. En kollision alene tillader ikke deduplikering;
D6 beskriver den snævre undtagelse for dokumenteret identiske versioner.
Metodeskift, når en kollision opstår eller forsvinder, kan ændre nøglen; en
ændret nøgle er en soft-delete plus en ny række.

## D4 — nationale bridges med uløste referencer

**Godkendt 2026-09-21:** En ikke-sentinel-reference uden dimensionsmatch må bruge det
numerisk normaliserede kilde-SK i **bridge-rækkens identitet alene**. Det indrammes med
referencekolonnens rolle og et særskilt navnerum; øvrige relationer bruger deres opløste
identitet. Metoden er `technical_reference`, ellers `resolved_relations`.
Den kliniske FK og dens versionsdel forbliver NULL. Sentinel -1 er stadig NULL og
udløser ikke teknisk fallback. Ingen rå DW_SK gemmes som arkivkolonne.

M indeholder 1.175.461 proceduregrupper med bridge-rækker uden dimensionsgruppe og
27.967 procedureversioner uden kode-dimensionsmatch. Forskellige uløste kilde-id'er
må ikke samles på samme NULL-baserede rækkenøgle. Referencer tælles særskilt fra antallet
af berørte rækker; nøglemetoder og deres rækketal fremgår af Dagster-metadata.

## D5 — Borger og Organisation med tvetydige naturlige identiteter

**Godkendt 2026-09-21:** DW_EK må indgå i nøglens hash alene for tvetydige entiteter.
312 Borger-par og to organisationsgrupper med tilsammen 2.396 entiteter har samme
naturlige identitet og ValidFrom; alle leverede forretningsfelter er ens inden for
grupperne. Distinkte DW_EK med samme naturlige nøgle **og ValidFrom** udløser `technical_collision`.
Alle leverede historiske versioner af de berørte DW_EK bruger samme tekniske identitet;
ValidFrom er fortsat anden del af merge-nøglen. Flere versioner af samme DW_EK er
ikke i sig selv en kollision. Ikke-tvetydige entiteter beholder den naturlige nøgle
og metoden `natural`, også ved ren omnummerering af deres DW_EK.

D3–D5 garanterer ikke identitetsstabilitet ved SDS-genindlæsning eller ved ændret
referenceopløsning/kollisionsstatus mellem leverancer. Hver metode tælles separat,
kontroltotaler og entydighed kontrolleres fortsat.

## D6 — identiske dimensionsversioner med bevarede kildehenvisninger

**Godkendt 2026-09-21:** KontaktEgenskaber må samles efter alle 24 forretningsfelter
og ValidFrom. Sammenlægning kræver, at samtlige øvrige kildefelter bortset fra
DW_SK/DW_EK også er byte-identiske, herunder ValidTo, flags og audit. Afvigelser
stopper load. Det er samme versionsindhold, der samles; forskellige versioner
bevares som forskellige nøgler.

Den private ext-identitetsmappe beholder præcis én række pr. kilde-SK, også når
flere SK'er peger på samme arkivnøgle. Dermed bevarer alle refererende rækker deres
entydige opslag uden join-multiplikation. Arkivet indeholder én repræsentant og
`antal_kilderaekker`; ved flere kilder indeholder `kildeidentiteter` en sorteret
JSON-liste over alle DW_EK. Ingen DW_SK gemmes i arkivet. JSON er NULL ved én kilde,
og den almindelige DW_EK-kolonne stammer fra repræsentanten. DW_EK og JSON opdateres
uden indholdsændring ved ren omnummerering; antal er omfattet af change-detect.

På U giver 1.907 kilde-SK'er 1.778 arkivrækker; 129 sammenlagte par har 258 bevarede
kildeidentiteter. Alle 32.496.015 fctKontakt-rækker har præcis ét opslag og nul
uopløste KontaktEgenskaber-referencer. Ingen af U's kontakter refererer til de 129
par; SQL-testene dækker derfor også tilfældet, hvor begge id'er i et par anvendes.
M's feltsammenligning fandt samme 129 par og uændrede forretningsfelter.

Mønstret anvendes også på **komplette** Diagnosegruppe/Proceduregruppe:

- Gruppens forretningsnøgle dannes af det sorterede sæt af opløste kodeversioner,
  deres parent-versioner og AntalRaekker. Kodeversionen omfatter den kliniske
  identitet og ValidFrom. Ingen kilde-SK indgår i denne hash (`v2`).
- Sammenlægning kræver ens medlemsmultiplicitet, bridge-audit samt dimensionens
  gyldighed, flags og audit. Forskellige payloads i samme sæt bruger fortsat D3.
- Ufuldstændige grupper og grupper uden bridge bruger fortsat D1. Et ukendt
  medlemskab bliver aldrig behandlet som et fælles tomt sæt.
- Identiske opløste diagnose-/procedure-bridge-rækker samles med summeret
  `antal_kilderaekker`. Forskellige payloads beholdes til entydighedskontrollen;
  den vælger aldrig vilkårligt en række. Kontroltotaler og NULL-afstemning vægtes
  med kildeantallet for begge bridges og de tre kanoniske dimensioner.

På U samler komplethedskontrollen 2.567 diagnose- og 11.952 procedurekohorter:
30.141.245 diagnosegrupper bliver 30.138.445 arkivrækker og 2.011.759 proceduregrupper
1.999.707.

**D5 udvides ikke til deduplikering:** U's 311 Borger-par (622 versioner) har alle
ErCPRnummerGyldigt = »Erstatnings CPR«, og 51 par har forskellig audit. Samme
erstatningsnummer er ikke dokumentation for samme person. Alle 2.396 rækker i de
to organisationskohorter har sentinelværdier i den primære identitetskode, og
begge kohorter har auditforskelle. De kan derfor ikke dokumenteres som samme
forretningsentitet. D5's adskilte tekniske identiteter bevares.

## Andre målte nøglevalg

- `Tidspunkt`: den nullable `Tidspunkt`-kolonne kan ikke være primærnøgle, fordi
  SDS leverer en ukendt-række med NULL. Den eksisterende forretningskode `MinutID`
  bruges i stedet som `tidspunkt_minut_id` (`SMALLINT`); rolleopdelte referencer
  har samme type. U har 1.441 entydige koder: 1.440 klokkeslæt følger HHMM, og
  ukendt-rækken har kode -1. Midnat har kode 0 og forbliver adskilt fra ukendt.
  Det oprindelige nullable klokkeslæt bevares som attribut. Referencer med kilde-SK
  -1 bliver fortsat NULL; ingen SDS-teknisk nøgle indgår i denne arkivnøgle.
  Den fulde U/M-kontrol er bestået: begge leverancer har samme 1.441 entydige
  koder, nul HHMM-afvigelser og nul ændringer af nøgle eller forretningsindhold
  blandt fælles identiteter i det målte leveranceinterval.
- Små indholdsdimensioner (U, 2026-09-21): `ForloebEgenskaber` har 3.234
  kode/type/ValidFrom-kollisionsgrupper (13.941 ekstra rækker), og `Markoer` har 17
  (17 ekstra rækker). Alle leverede forretningsfelter, herunder indberettere og
  tekster, plus ValidFrom adskiller samtlige henholdsvis 18.482 og 888 rækker.
  Deres hash omfatter derfor alle felter uden `DW_`-præfiks; ValidFrom er fortsat
  særskilt merge-komponent. Tekstændringer kan ændre nøglen; en ændret nøgle er
  en soft-delete plus en ny række (se D3). Der bruges ingen teknisk kildeidentitet i disse nøgler.
  I M har de 18.515 ForloebEgenskaber- og 888 Markoer-rækker
  ingen kollisioner. Ingen af de henholdsvis 18.482 og 888 fælles EK/versioner
  har ændret forretningsnøgle fra U til M. Målingen gælder dette leveranceinterval.
- `KontaktEgenskaber`: kode/type/ValidFrom gav 133 kollisionsgrupper (135 ekstra
  rækker). Alle forretningsfelter efterlader 129 identiske par; disse håndteres
  efter D6 ovenfor.
- Helbredsforløb: rod suppleret med første forløb dækker kun 99,630524 %. En supplerende
  kandidat bruger den entydige synlige begyndelse eller hash af det komplette sorterede
  sæt af synlige begyndelser, aldrig DW_EK/SK. Den komplette U/M-kontrol fandt
  ingen manglende nøgler og ingen kollisioner i nogen af leverancerne. Direkte UUID dækker
  99,986615 % af M; de resterende 1.198 bruger sæt-hash. Fem af 8.934.680 fælles entiteter
  skifter kandidat (tre også afledningsmetode). Disse er reelle ændringer i afledningen,
  ikke et løfte om evig identitet; de tælles.
- Sygehusophold: første kontakt dækker 99,942556 %. Første kontakt suppleret med en
  entydig, ikke-delt kontakt eller komplet kontakt-UUID-sæt er valideret på hele U/M:
  nul manglende eller kolliderende nøgler. Direkte UUID dækker 99,983867 % i M; 3.575
  ophold bruger sæt-hash. Fire af 22.104.126 fælles entiteter skifter nøgle.
- Diagnose og Procedure: type-teksten adskiller de berørte kode/type-kollisioner
  og er uændret mellem de berørte entiteters historiske versioner. Den fulde M-kontrol
  på 549.888 diagnoseversioner og 324.513 procedureversioner fandt nul aktuelle
  nøglekollisioner, nul dublerede (nøgle, ValidFrom) og nul manglende nøgler.
  På tværs af hele historikken har 24 andre diagnoseentiteter forskellige type-tekster
  mellem versioner; ingen procedureentiteter har nøgleændringer. De 24 historiske
  ændringer bevares som versioner. Målingen beviser ikke i sig selv, at samme version
  har samme nøgle i alle fremtidige leverancer.
- VaerdiSKS kræver både Vaerdi_Kode og Vaerdi_SKS_Type; sammen med DW_ValidFrom er nøglen
  entydig i alle 7.598 versioner. Resultatnøglen (indberetnings-UUID, Resultat_Nr) er
  entydig og fuldt dækkende for 14.429.227 rækker.

## Markørtab ved månedsovergange

M 2026-08-12 til M 2026-09-15: 785.234 af 21.303.274 tidligere markør-UUID'er
forsvinder (3,685978 %), og 1.043.546 kommer til. Kun otte bevarede UUID'er har nyt
DW_EK; de forsvundne UUID'er kan derfor ikke forklares som ren omnummerering.
Begge leverancer har entydige og udfyldte markør-UUID'er.

En overgang mellem to månedsleverancer uden de mellemliggende ugeleverancer soft-sletter
derfor mange markører.

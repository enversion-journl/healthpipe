<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Beslutningslog, del 2 — Diagnoser

Se [hovedloggen](beslutningslog_principper.md) for principper og læsevejledning.

## Fem veje i én tabel — bevidst, og de kan skilles

Diagnosetabellen fyldes af **fem** veje. Samlingen er et bevidst valg. Hver
vej har sit eget identifier-system, så de kan skilles uden at gætte på
indholdet. *Aftager:* **gruppér altid på kildesystem / identifier-system.**
Tæl aldrig tabellen uden det.

| # | Vej | Identifier-system | Nøgle | Rolle (aktions-/bidiagnose) |
|---|---|---|---|---|
| 1 | Kontaktdiagnose fra det patientadministrative system | `urn:source:ifdb:pas_diagnose` | `PAS_DIAGNOSE.OBJEKT_ID` | Kildens `DIAGNOSETYPE` bæres som udvidelse |
| 2 | Øvrige diagnoseregistreringer i journalens diagnoseværktøj | `urn:source:ifdb:diagnose` | `DIAGNOSE.OBJEKT_ID` | Ingen — kilden har ikke rollen på denne tabel |
| 3 | SFI-registreringens typekode (`SKS.a.dia*`) | `urn:source:ifdb:aktivitetsresultat_condition` | `AKTIVITETSRESULTAT.OBJEKT_ID` | Ingen — typekoden er koden, ikke rollen |
| 4 | Diagnosekode-felt i SFI-skemaets XML | `urn:source:ifdb:aktivitetsresultat_xml_condition` | `{OBJEKT_ID}/{elementnummer}` | **Ingen** — elementet har ingen aktions-/bi-markør |
| 5 | Henvisningsdiagnose fra billeddiagnostik | `urn:source:ifdb:ris_ydelse:diagnose` | én pr. RIS-række | Henvisningskategori; eventuel type fra XML hvis den findes |

Alle fem går gennem den samme kanonisering til SKS-D-form. Er værdien ikke
en genkendelig SKS-diagnosekode, dannes der ingen kode — ressourcen kan
stadig postes med fritekst (billeddiagnostik). *Aftager:* kodekolonnen er
SKS-D, eller tom.

En henvisningsdiagnose er ikke det samme kliniske faktum som en kontaktdiagnose:
den er begrundelsen for at bestille en undersøgelse og kan være en generisk kode
frem for den diagnose, lægen satte på kontakten. Om der skal findes et
standardview, hvor billeddiagnostikkens diagnoser er frasorteret, er **ikke
besluttet** — det er et åbent spørgsmål til aftagerne.

**Forholdet mellem de to journalkilder er målt.** Tabellen med øvrige
diagnoseregistreringer stammer fra journalens diagnoseværktøj, kontaktdiagnoserne
fra det patientadministrative system; det gamle flow brugte alene
kontaktdiagnoserne. Målingen forklarer hvorfor: **91,7 % af diagnoseværktøjets
patient+kode-par findes også blandt kontaktdiagnoserne**. Kun 8,3 % findes alene
i diagnoseværktøjet. *Aftager:* de 8,3 % **er med**. De
har ingen rollemarkering, så et slutview kan sortere dem frem på
identifier-systemet `urn:source:ifdb:diagnose` uden at blande dem med
kontaktdiagnoser. Tæller man tabellen uden at gruppere på kildesystem,
dobbelttælles overlappet.

**XML-diagnoser (vej 4) er med, uden rolle.** Et felt der hedder `Diagnosekode`
i et SFI-skema bliver en Condition. Kilden siger ikke, om det er en aktions-
eller bidiagnose — kun at en kliniker har udfyldt en diagnosekode i skemaet.
*Aftager:* de kan sorteres frem på
`urn:source:ifdb:aktivitetsresultat_xml_condition`. Forvent tom rolle. En
registrering, der både har typekode-diagnose og strukturerede XML-elementer,
giver **kun** XML-rækkerne (undertrykkelsesreglen nedenfor).

**Ingen kliniske eller administrative filtre.** Ingen LPR-filtrering, intet
statusfilter, intet datofilter. Kun delta-vinduet afgrænser. *Aftager:*
populationen er bredere end det gamle LPR-baserede udtræk.

**Slettede diagnoser bevares som gravsten.** En tilbagekaldt diagnose postes med
fejlregistrerings-status og et sletteflag frem for at forsvinde. *Aftager:*
filtrér på både sletteflag og verifikationsstatus. Rækketallet matcher arkivet
1:1, ikke antallet af gyldige diagnoser.

## Hvad der bliver en diagnose

**Fra skemaernes typekode:** rækker med diagnosepræfiks, styret af en
konfigurationsfil i repoet. Brede mønstre på nøgne koder er bevidst ikke slået
til, fordi de også ville ramme interne navnerum i kildesystemet.

**Fra skemaernes indhold:** kun ét elementnavn er en port — "Diagnosekode".
Andre stavemåder giver ingen diagnose, uanset om værdien er en gyldig kode. Målt:
porten rammer ikke forbi — de udelukkede navne bærer nul gyldige koder.

**Undertrykkelse: XML-diagnoser vinder over den grove kode.** Bærer en
diagnosekodet række strukturerede diagnose-elementer, undertrykkes den grovere
diagnose på registreringsniveau. *Aftager:* for en given registrering får du
**enten** én diagnose fra typekoden **eller** flere fra indholdet — ikke begge.

**En operationskodet række kan alligevel give diagnoser**, hvis dens XML
indeholder diagnose-elementer. *Aftager:* man kan ikke slutte fra aktivitetstypen
til, om rækken bidrog til diagnosetabellen.

**Epikrisernes diagnoser modelleres ikke.** Epikrisen bliver et dokument med
metadata; teksten og dens diagnoser omsættes ikke. *Aftager:* diagnoser nævnt i
en epikrisetekst findes ikke i tabellen.

## Kodefortolkning

**Én kanonisk regel for hele kodebasen.** Alle diagnosekoder føres til dansk
SKS-D-form: præfiks strippes, og er koden ren ICD-10, præfikses den med `D`.
Reglen er idempotent. Før den fandtes, gav samme kildekode forskellige resultater
i forskellige pipelines. *Aftager:* kodekolonnen er altid SKS-D. Vil man have
nøgen ICD-10, skal det ledende `D` fjernes.

**Tillægskoder kanoniseres ikke.** De beholder deres eget kodesystem og får kun
præfikset strippet — mange af dem er slet ikke ICD-10, blandt andet
ATC-lægemiddelkoder. *Aftager:* **skel på kodesystem, ikke kun på kode.**

**Diagnoser i skemaernes indhold læses fra originalkode-feltet** — kodefeltet
er kun et suffiks (fælles regel, del 5). Ændringen vækker undertrykkelsesreglen
ovenfor, så registreringer, der før gav én diagnose, kan give flere andre.
**Omfanget måles ved genkørslen og forelægges fagligt** — det er sporets
største populationsændring.

**En kode fra en anden SKS-familie afvises.** Et operationsnummer formet
bogstav+ciffer ville ellers kanonisere til en diagnose, der ikke findes.

**Værdier der ikke kan fortolkes, giver ingen kode.** Skemaindekser og fritekst
kastes væk frem for at blive falske diagnoser. To konsekvenser: en
billeddiagnostisk diagnose med ukendt kode postes alligevel — med den rå værdi
som fritekst og **ingen** kode, og fritekstværdien projiceres ikke til den flade
tabel. Og en række med ukendt hovedkode kan ende med **kun** tillægskoder: tom
kodekolonne, men rækker i tillægskodetabellen. Det sidste er en afledt
konsekvens, ikke et dokumenteret valg — bør bekræftes fagligt.

**Tillægskoder ligger i egen tabel, aldrig i hovedtabellens kodekolonne.**
ATC-lægemiddelkoder projiceres slet ikke til datawarehouset — de bliver på
ressourcen til en fremtidig allergimodel. *Aftager:* lægemiddelallergi-
information er ikke tabt, men findes ikke som diagnose.

## Diagnosetyper og kategori

**Diagnosetypen bevares råt** i kildens værdirum (aktions-, bi- og
henvisningsdiagnose). Ingen oversættelse til den gamle LPR2-arvede værdi, fordi
der ikke er hjemmel for den i den danske profil. *Aftager:* forvent præcis
kildens tre værdier plus tom.

**Henvisningsdiagnoser kræver to kriterier.** Alle kontaktdiagnoser får samme
kategori; kun billeddiagnostikkens får en særlig henvisningskategori. *Aftager:*
vil du have **alle** henvisningsdiagnoser, skal du bruge både diagnosetypen og
kategorien i en ELLER — kategorien alene er ikke nok.

**Diagnosetypen ligger i sit eget felt** for at give den flade projektion en
kort vej. *Aftager:* kolonnen er kun udfyldt for ressourcer skrevet efter
indførelsen — lukkes af genkørslen.

## Status

**Slettet giver fejlregistrering og ingen klinisk status.** Det gør det muligt at
skelne en tilbagekaldt registrering fra en almindelig inaktiv diagnose.
*Aftager:* tom klinisk status betyder tilbagekaldt eller modbevist — ikke ukendt.

**Modbevist giver sin egen status.** Sletning har forrang over modbevisning.

**Afslutningstidspunkt giver "afsluttet".** Det betyder "kilden har sat et
afslutningstidspunkt" — ikke en klinisk vurdering af helbredelse.

**Billeddiagnostikkens diagnoser er ubekræftede** og har ingen klinisk status.
*Aftager:* filtrerer du på bekræftet status, forsvinder **alle** diagnoser fra
billeddiagnostik.

**En slettet rekvisition markeres, men diagnosen forbliver ubekræftet** — det er
rekvisitionen, der er slettet, ikke mistanken afkræftet.

**Kladde giver "provisional" på SFI-diagnoser.** Familiens kladdebeslutning
gælder også her: en diagnose fra en ufærdig skemaudfyldning må ikke tælle som
gældende. Sletning slår kladde. *Aftager:* filtrér på bekræftelsesstatus, når
kun færdige registreringer skal med — kladder står som "provisional".

**SFI-diagnoser bærer modtidspunkt, forløb og LPR-mærket som de øvrige spor.**
Modtidspunktet sættes som samme datetime-felt som de øvrige veje, så kolonnen
ikke er tom for SFI. Forløbet og LPR-mærket spejler `fhir.Procedure`; kun
SFI-diagnosesporene bærer forløbet — de øvrige veje lander tomme. *Aftager:*
rækker fra før kolonnerne er tomme, indtil genkørslen.

## Identitet og grain

**Fem identifier-systemer**, ét pr. vej i tabellen ovenfor, hver med sit
unikke indeks. Forretningsnøglen er kildesystem plus kildeværdi.

**Diagnoser fra skemaernes indhold identificeres ved forælder plus
elementnummer.** *Aftager:* elementnummeret er en del af nøglen — flyttes et
element i skemaet, er det en ny diagnose.

**Én række pr. diagnose; koder udfoldes ikke til egne rækker.** Udfoldning
ville gøre lægemiddel-tillægskoder uskelnelige fra diagnoser. *Aftager:* tæl
diagnoser på rækker, koder i tillægskodetabellen.

**Unikhed på forretningsnøglen er kun en advarsel**, fordi nøglen er sammensat
over to arkiventiteter, som den automatiske afhjælpning ikke kan udtrykke.
*Aftager:* dubletter på kildenøglen stoppes ikke af en gate — verificér selv,
hvis din model kræver unikhed.

**Fuldstændighed overvåges kun for to af fem veje.** Billeddiagnostikkens og
skemaernes diagnoser er ikke afstemt mod arkivet. Kendt hul.

## Drift med semantisk betydning

**Felter tilføjet efter første indlæsning er først komplette efter en
genkørsel af historikken** — nye felter kommer ikke af sig selv på urørte
ressourcer. Genkørsler af den karakter køres kun efter eksplicit godkendelse.

**Slette-detektion er slået fra for de tre store børnetabeller** (kontakt-,
tillægskode- og henvisningsreferencer, tilsammen ~34 mio. rækker), fordi
alternativet var at streame ~149 mio. rækker hver nat. Prisen er dokumenteret: en
børnerække, der fjernes i kilden **uden** at forælderen røres, bliver liggende.
Grænsen er sat ved ca. 1 mio. rækker. Den ugentlige arkivafstemning afslører det.

**Arkivet deduplikerer kilden**, fordi kilden ikke har en primærnøgle — grainen er
noget vi påstår, ikke noget kilden håndhæver. En tilsvarende påstand er tidligere
modbevist i praksis. *Aftager:* er der ægte dubletter med forskelligt indhold,
taber deduplikeringen indhold. Forudsætningstjekket er endnu ikke kørt.

**En miljøvariabel kan slå diagnoser fra skemaindholdet helt af.** Køres
pipelinen i hurtig tilstand, produceres de parsede rækker ikke, og
XML-diagnoserne mangler. Tilstanden er ikke standard.

**Validering sker på stikprøve**, ikke på hver ressource. En enkeltstående
ugyldig diagnose kan slippe igennem; serverens egen validering er sidste værn.

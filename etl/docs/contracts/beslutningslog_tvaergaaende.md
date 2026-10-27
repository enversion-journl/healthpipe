<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Beslutningslog, del 5 — Tværgående valg

Se [hovedloggen](beslutningslog_principper.md) for principper og læsevejledning.
Dette er de valg, der gælder på tværs af procedurer, diagnoser, målinger og
spørgeskemasvar.

## Kodenormalisering

**Én central afstribning af kildepræfikser.** Præfikserne er tilføjet af
kildesystemet og er ikke en del af koden; de skal væk før validering eller
opslag. *Aftager:* kodekolonnerne indeholder rene SKS-koder. Kildens rå strengform
findes ikke i de flade tabeller.

**Præfikslistens rækkefølge er semantisk betydende.** Listen er ordnet
specifik-før-generisk, og den generiske står **sidst** — ellers ville den æde
familiedelen af koden. *Aftager:* en ny præfiksfamilie kan ikke tilføjes vilkårligt
i listen; placeringen ændrer resultatet.

**Numeriske præfikser er ægte præfikser, ikke ledende nuller.** Verificeret mod
registret: ingen kode starter med nul. *Aftager:* værdier har ingen ledende
nuller — join mod et register, der padder, kræver normalisering hos aftageren.

**Kildens hierarki bevares som otte niveauer**, i paritet med det eksisterende
datawarehouse. Niveauerne bæres som oplysninger på ressourcen, ikke som kolonner.

## XML-parseren

**Parseren er en portering af den eksisterende regionale C#-logik** til to flade
strukturer: én række pr. element og én pr. registrering med kontekstoplysninger.
*Aftager:* feltnavne og semantik følger den model, der allerede kendes.

**Skrald efter rod-endetagget trimmes bort.** Kildens tekstfelter indeholder
lejlighedsvis rester efter det afsluttende tag; den gamle platforms XML-læser
ignorerede det stille, Pythons gør ikke. *Aftager:* registreringer, som ville have
fejlet, kommer med. Ingen population tabes til denne kildeartefakt.

**Klynge- og gentagelseshierarkiet bevares som separerede strenge.** Nødvendigt
for klyngebindingen — uden den ville tillægskoder hænge på den forkerte operation.
*Aftager:* strengene er lister og skal **splittes på separatoren**; delvis
tekstsammenligning er forkert.

**Wrapper-noder er transparente.** Kildesystemet pakker klyngebørn i beholdere,
som grupperer uden selv at være svar; de gennemløbes med uændret dybde.
*Aftager:* ressourcer skrevet før reglen kan mangle disse elementer, indtil
registreringen genskrives — lukkes af genkørslen.

**Personalerollen emitteres ikke som eget datapunkt** — den foldes ind i
personrækkens ledetekst. *Aftager:* rollen findes ikke som selvstændigt datapunkt.

**Kontekstroller: første forekomst vinder.** Oprettet, ændret og besluttet parses
med bruger, person, rolle, enhed og tidspunkt. *Aftager:* felterne bæres som
oplysninger på målinger og er **ikke** kolonner. Oprettet og ændret er efterspurgt
som kolonner — endnu ikke bygget.

**Ubrugelig XML giver stadig en række, med en statuskode.** Tre udfald: parsefejl
eller tom, parsbar men uden brugbare elementer, og normal. *Aftager:* filtrér på
statuskoden, hvis kun brugbare datapunkter ønskes. En registrering med ubrugelig
XML fremgår som eksisterende, men uden indhold.

**Fejl i datoparsing sluges; feltet bliver tomt.** *Aftager:* et tomt
kontekst-tidspunkt kan betyde "ikke registreret" **eller** "uparsbar
kildeværdi" — de to kan ikke skelnes.

**Værdifeltet bliver tom streng, ikke tom.** *Aftager:* "ingen værdi registreret"
og "tom værdi" kan ikke skelnes på feltet; brug flaget for "har værdi".

**Hurtig tilstand er en bevidst, fravalgt projektion.** Den udtrækker kun
navn/værdi-par og udelader **al** struktur: rodoplysninger, kontekstblokken,
element-oplysninger og hele hierarki-bogføringen. *Aftager:* i hurtig tilstand
findes **ingen** tillægskoder, deloperationer, XML-diagnoser, knivtid,
operationspersonale eller klyngekontekst — og ingen pensionering af XML-børn.
Tilstanden er slået fra som standard, og i normal drift tvinges den tabsfrie
parser uanset flaget.

## Codings-opbygning

**Kildeorden bevares; hovedkoden står først.** Rækkefølgen blandt tillægskoder er
kildens, men den er **ikke** til at slå op i de flade tabeller.

**Dedup på system og kode; hovedkoden vinder.** Multiplicitet tabes bevidst, fordi
tillægskodetabellernes nøgle ikke kan bære to identiske koder. *Aftager:* tæl ikke
multiplicitet ud af tabellerne.

**Hovedkoden vinder også på tværs af kodesystem-splittet.** Uden det ville
diagnosekoden optræde som sin egen tillægskode.

**Hovedkode-markøren sættes kun positivt.** Tillægskoder har feltet **fraværende**,
ikke sat til falsk — derfor skal børnenes filter skrives som "fraværende eller
falsk". *Aftager:* ressourcer skrevet før markøren blev indført har ingen —
hovedkode-kolonnen er tom, og hovedkoden ligger i tillægskodetabellen, indtil
rækken genskrives. Lukkes af genkørslen.

## Sletning og lossless

**Intet frasorteres i indlæsningen.** Det gamle flows filtre (slettede,
ikke-indberetningspligtige, resultattype, kodegyldighed) er alle erstattet af
markering. Fagligt bekræftet: gyldighedstrinnet frasorterede heller ikke i det
gamle flow. *Aftager:* **enhver opgørelse skal filtrere aktivt** — uden en let
filtreringsvej bliver princippet en fælde i stedet for en styrke.

**Én fælles slette-oplysning på alle ressourcetyper**, så aftagere kan filtrere
med ét udtryk. Den er projiceret til de flade tabeller for Procedure, Condition
og QuestionnaireResponse. Observation har den stadig kun på ressourcen.

**Ingen fysisk sletning nogen steder.** Alle sletninger er markeringer, og soft
delete er reversibel. *Aftager:* tabellerne vokser monotont; aftagerens filtrering
er den eneste beskyttelse mod at tælle historik og tilbagekaldte rækker med.

**Kladdeflaget læses i hele familien.** Slettet slår kladde, kladde slår
færdig. Procedurer og spørgeskemasvar får status "i gang"; målinger bliver
foreløbige. *Aftager:* tæl ikke kladder som gennemførte ydelser. Rækkerne
forsvinder ikke — de kan medtages ved også at læse status "i gang".

**Arkivet læser slettede rækker med**, så sletningen kan propagere. Ville man
filtrere dem væk i arkivet, ville kildens sletning stille aldrig nå frem.

## Identitet og referencer

**Ét identifier-system pr. kilde og pr. afledningsvej**, hver med sit unikke
indeks, så to afledninger fra samme kilderække ikke kolliderer.

**Ressource-id genbruges via en cache; manglende opslag danner et nyt.**
*Aftager:* id'et er **ikke** en funktion af kildenøglen og kan ikke genberegnes.
Join altid via kildenøglen. Deterministiske id'er blev prøvet og forkastet.

**Inden for én kørsel danner samme kildenøgle kun ét id** — men to af
opslagsvejene har ikke den beskyttelse.

**Børns nøgler slås op i databasen, før ressourcerne bygges.** Uden opslaget
danner hver kørsel en ny identitet for samme kilde, og ændringer skrives aldrig,
mens kørslen forbliver grøn.

**En nøglekollision behandles konservativt.** Den tælles kun som succes, hvis
databasen kan bevise, at præcis én eksisterende række matcher. Ellers er det en
ægte fejl. Opslaget kører i egen deltransaktion, så en fejl ikke vælter partiet.

**Referencer falder tilbage til kildenøglen; de droppes aldrig.** *Aftager:* tomme
reference-kolonner betyder, at opslaget fejlede — kildeoplysningen findes stadig
på ressourcen. Manglende kontakt **og** forløb betyder derfor enten en
registreringsfejl i kilden eller et fejlet opslag, og de to kan ikke skelnes.

**Cache-fejl maskeres ikke** — de kastes videre med diagnostisk kontekst, så et
nedbrud giver en fejlet kørsel frem for stille duplikering.

## Flade tabeller

**Fletningen sletter aldrig selv.** Extractet er delta-afgrænset og repræsenterer
ikke hele tilstanden; en slette-klausul på en delvis kilde ville markere gyldige
rækker som slettede. *Aftager:* uden en særskilt oprydning bliver rækker liggende.

**En uændret række udløser ingen opdatering.** Revisionskolonnerne indgår ikke i
sammenligningen. *Aftager:* opdateringstidsstemplet er et pålideligt
ændringssignal for forretningsindhold.

**Nøgler: ressource-id på forældre, sammensat på børn.** Diagnosetabellens nøgle
blev **indsnævret**, fordi den tidligere udfoldning af hver kode gav ~40.000
falske kvalitetsbrud, hvor lægemiddel-tillægskoder ikke kunne skelnes fra
diagnoser. Læren: børnetabeller i eget grain, aldrig rækker i forælderens.

**Nøglebærende kolonner får en erstatningsværdi frem for tom.** *Aftager:* en tom
streng i disse kolonner betyder "ikke til stede i ressourcen", ikke "tom værdi".
Uparsbare datoværdier kollapser på én nøgle — advarslen logges, fordi det ellers
stille kunne skjule en rigtig periode.

**Deduplikering med tiebreak — men uens dubletter er en hård fejl.** Identiske
dubletter kollapses og logges; har to rækker med samme nøgle forskelligt indhold,
**stopper kørslen** frem for at skjule data.

**Genoplivning af soft-slettede rækker er opt-in.** En soft-slettet række optager
stadig nøglen, så når kilden leverer den igen, er det et match, ikke en ny række —
uden genoplivning ville den blive opdateret men forblive usynlig for enhver, der
filtrerer på flaget. Og fordi indholdet typisk er identisk efter en sletning, skal
flaget selv tælle som en forskel, ellers udløses opdateringen aldrig.

**Børnerækker markeres slettet før fletningen, kun for ændrede forældre.** Uden
det akkumulerede tabellerne rækker, der havde forladt forælderen. *Aftager:*
**filtrér på sletteflaget** i tillægskode- og svar-elementtabellerne.

**Sletteflaget er ikke-nullbart med standardværdi nul** — bevidst, så den
naturlige filtrering ikke stille kan droppe levende rækker (på en nullbar kolonne
er sammenligning med tom ukendt, og rækken forsvinder). *Aftager:* på de ældre
nullbare flag skal man håndtere tom eksplicit.

**Et tomt extract kombineret med oprydning er en hård fejl** — en fejlet
forbindelse må ikke kunne tømme tabellen.

**Ingen sortering i extractet.** Målt: sortering ville tvinge hele resultatet
materialiseret før første række — femten minutter mod under et sekund. *Aftager:*
rækkeordenen i de flade tabeller er ikke meningsfuld.

**Ikke projiceret til de flade tabeller:** personale, udførende organisation,
kodernes rækkefølge, kontekstblokken og hierarkiniveauerne — de findes kun i
ressourcens JSON. Kolonner bygges, når en opgørelse er navngivet; hvad der
allerede er kolonner, står i lineage. *Aftager:* **sig til, hvilke opgørelser
der skal kunne køre uden at læse JSON.**

**dw-nøgler på de flade tabeller.** `dw_ek_Borger`, `dw_sk_Borger` og `dw_ek_Organisation`
lægges direkte på de flade tabeller (i dag `fhir.EpisodeOfCare` og `fhir.Location`) og slås op
i `dim.Borger` og `dim.Organisation` på FHIR-id. Begrundelse og mekanik står i
`etl/docs/architecture/fhir_dw_keys_trin2.md`.

- **`dw_ek_Borger` findes også for en borger, der kun findes som slettet.** Entitetsnøglen er
  borgerens identitet, ikke en gyldig version. *Aftager:* en nøgle her siger ikke, at borgeren
  er aktiv.
- **`dw_sk_Borger` er den levende version, der gælder på rækkens tidspunkt** (for
  EpisodeOfCare `period_start`), med samme regel som `dbo.GetBorgerSK_TVF`. Soft-deletede
  versioner tæller ikke.
- **-1 betyder "ingen nøgle"** — referencen mangler, findes ikke i dimensionen, eller (kun
  `dw_sk_Borger`) rækken har intet tidspunkt eller ingen levende version på tidspunktet. -2
  bruges ikke. *Aftager:* et join til dimensionen rammer UNKNOWN-rækken; filtrér på -1 for at
  finde rækker uden nøgle.
- **Sletteflaget på fhir-rækken indgår ikke.** Slettemarkerede rækker får nøgler efter samme
  regel som levende rækker.
- **Rækker med -1 genforsøges ikke**, men genberegnes, når fhir-rækken, borgeren eller
  organisationen ændres. Hver kørsel logger antallet af -1.
- **Kun rækker, hvor en nøgle ændres, skrives og får nyt `dw_timestamp_update`.** *Aftager:*
  en ændret nøgle kommer med i DSO-deltaet som enhver anden ændring.

**`EpisodeOfCare.period_start`/`period_end` er `DATETIME2(0)` med klokkeslættet, som IFDB
leverer det.** IFDB leverer dansk lokaltid, men FHIR-værdien bærer `+00:00` uden at være
omregnet. Offset fjernes ved indlæsningen uden omregning, og sekunddele afkortes.
*Aftager:* værdien er dansk lokaltid uden offset; den er ikke UTC.

## Delta og ændringsopdagelse

**Arkivlaget er et obligatorisk mellemled.** Et delta-extract er per definition
ufuldstændigt; arkivet giver det referencepunkt, der gør det muligt at skelne
"slettet i kilden" fra "ikke i vinduet". *Aftager:* arkivet er den fulde historik
over, hvad der er hentet; FHIR-lageret er en projektion af arkivet.

**Ændringsmarkøren varierer pr. kildetabel**, fordi kolonnerne ikke findes ens i
kilden. *Aftager:* hvilken kildehandling der udløser en genskrivning, afhænger af
tabellen.

**Ingen overlappende tilbagekig; i stedet et fælles loft.** Alle udtræk i samme
kørsel bruger kildesystemets egen jobafslutning som øvre grænse, så rækker, som
kildens jobs ikke er færdige med, udelades. *Aftager:* alle tabeller ser samme
snit; en række, der ikke er klar, venter til næste nat.

**Kildens eget slettemærke propageres; automatisk slettedetektion er slået fra**
for de store tabeller. Prisen er dokumenteret: en børnerække, der fjernes i kilden
uden at forælderen røres, bliver liggende. Grænsen er sat ved ca. en million
rækker. Den svageste er en tabel på 85 mio. rækker, hvor børnerækker aldrig
retireres — den skal ryddes med en fuldkørsel.

**Forsvundne kilderækker fanges ugentligt, men ryddes kun på ordre.** Kilden
beholder logisk slettede rækker, så en nøgle, der genuint forsvinder, er sjælden —
et stort tal er systemisk. Oprydningen er aldrig skemalagt. *Aftager:* en række,
der forsvinder fysisk fra kilden, forbliver aktiv i både arkiv og FHIR-lager,
indtil en operatør rydder op.

**På FHIR-siden findes detektion, men ingen oprydning for de fire ressourcetyper**,
fordi de mangler sletteflag-kolonnen. *Aftager:* en ressource, der slettes hårdt i
lageret, kan efterlade en forældet række, som **intet** retirerer.

**Om et skift af kontakt eller forløb overhovedet opdages er uafklaret.** Delta
hakker kun rækker, hvis kildens tidsstempel er nyere. Om kilden opdaterer det, når
**kun** tilknytningen skifter, har vi ikke kunnet afgøre. Erfaring fra en anden
region: det sker ofte. Verificeret om vores model: vi denormaliserer ikke
procedurer ud på kontakten, så deres præcise fejl kan ikke opstå — men to
eksponeringer består. **Advarsel til aftagere, der aggregerer pr. kontakt
inkrementelt:** feedet markerer proceduren som ændret, men **ikke** den kontakt,
der mistede den. Kildens egne kortlægningstabeller over kontekstændringer
refereres ikke ét sted i kodebasen.

**Historiske huller i datapunkterne lukker ikke sig selv.** Fremadrettet er
tilstanden korrekt, men huller fra før spejlingen lukker kun, når registreringen
ændres. Kræver en engangs-opsamlingskørsel.

## Pensionering af XML-børn

**Mekanismen:** efter posting slås levende ressourcer op pr. forælder, og de, der
ikke blev postet denne gang, markeres som fejlregistrering med sletteflag. Der
slettes aldrig fysisk.

**Fire familier — spørgeskemasvar er ikke iblandt dem.** *Aftager:* en besvarelse,
der forsvinder fra kildens XML, retireres af intet lag, mens dens elementtabel har
fuld oprydning. Asymmetrien er ikke begrundet.

**Tre værn.** En byggefejl i partiet afbryder hele oprydningen, fordi en sprunget
række ser berettiget ud men postede ingen børn. En parsefejl på ikke-tom XML
holder forælderen ude, så et nedbrud ikke tømmer levende børn. En tom parse-liste
tolkes som et nedbrud i arbejderen, aldrig som "ingen børn".

**Undtagelsen:** en tømt tekstværdi i kilden pensionerer bevidst **alle** børn —
det er den korrekte fortolkning af, at registreringen er tømt.

**Oprydningen sker kun for hentede forældre.** *Aftager:* historik, som daglig
kørsel aldrig rører, bliver liggende. Efterslæbet lukker ikke af sig selv.

**Sikkerhedsnet:** allerede pensionerede børn udelades af opslaget, og over 10.000
markeringer logges en advarsel — men kørslen fortsætter. Tærsklens karakter
(advarsel, ikke fejl) er ikke begrundet.

**Den flade side følger med** via oprydning på børnegrain — men **migrationen skal
være udrullet**, før oprydningen kan køre.

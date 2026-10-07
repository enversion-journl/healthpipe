<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Beslutningslog, del 3 — Observationer (målinger)

Se [hovedloggen](beslutningslog_principper.md) for principper og læsevejledning.

## Hvad bliver en måling

**Observation kræver en godkendt rute; alt andet bliver spørgeskemasvar.** En
registrering bliver kun en måling, hvis dens aktivitetstype står på en godkendt
liste i databasen — routing er data, ikke kode; formgætteri på XML'en er
fravalgt. *Aftager:* **Observation er en kurateret, snæver population.**
Fravær af data betyder normalt ikke, at kilden mangler — den ligger som
spørgeskemasvar. Brug datapunkt-fladen, som samler begge.

**Listen er i praksis meget lille:** 11 aktive ruter. Fem foreslåede ruter for de
generiske vitalparametre (blodtryk, puls, saturation, respirationsfrekvens,
temperatur) står stadig til gennemsyn og router derfor til spørgeskemasvar.
*Aftager:* **antag ikke, at "vitalparametre = måling".** Det er let at læse
konfigurationsfilen og tro det modsatte.

**Ruten slås op på aktivitetstypen alene.** Skemanavn og version tages imod men
bruges ikke. To skemaversioner routes ens.

**Rutens gyldighed vurderes mod registreringens starttidspunkt** — så historik
routes efter den regel, der var gyldig klinisk. Kan tidspunktet ikke læses,
bruges **kørselsdagens** dato, hvilket ikke er reproducerbart over tid.
Udokumenteret hjørne.

**En konfigurationsfejl koster aldrig data.** Overlappende ruter, manglende rute
eller manglende aktivitetstype giver alle spørgeskemasvar, med årsagen skrevet i
kørselsloggen. *Aftager:* en konfigurationsfejl viser sig som et *skifte* fra
måling til spørgeskemasvar — ikke som huller.

**Manglende routing-tabel er en hård fejl**, ikke en stille tilbagefaldsvej til
konfigurationsfilen, så ruteafgørelser altid kan rekonstrueres fra databasen. En
**tom** tabel er lovlig og sender alt til spørgeskemasvar — det ser ikke ud som en
fejl i loggen.

**Kun bladelementer og valg bliver datapunkter.** Klynger og gentagelser er
beholdere. Rækker med parsefejl eller uden elementer frasorteres. *Aftager:*
gruppeoverskrifter optræder ikke som selvstændige målinger.

**Tomme værdier bliver ikke målinger** — også ikke tomme valg. På
spørgeskemasiden bevares de derimod. *Aftager:* "spørgsmål stillet men ikke
besvaret" kan kun tælles via spørgeskemasvar, ikke via målinger.

**Én producent i dag.** Den gamle hændelsesbaserede målingspipeline er
deaktiveret og slettet, men en kontrakttest holder deaktiveringen på plads, fordi
sletningen alene ikke forhindrer, at assets med de gamle navne registreres igen.

**Den gamle beholder-model er ikke standard.** Standard er én måling pr.
datapunkt; den oprindelige model med hele XML'en i komponenter findes kun bag en
miljøvariabel. *Aftager:* komponentbaserede målinger forekommer ikke i
produktionsdata — men noget lineage-dokumentation beskriver stadig den gamle
model.

## Kodedannelse

**Koden kvalificeres med skemanavnet, ikke med versionen.** Samme element-id
under to skemaer bliver to begreber; versioner af samme skema deler ét begreb.
Formen er skemaet, ikke versionen af skemaet. *Aftager:* koden er stabil på tværs
af skemaversioner; ændrer skemaet navn, får man en ny kode.

**Manglende element-id giver en reservekode markeret til gennemsyn**, så den
dukker op i styringen frem for stille at slå begreber fra forskellige skemaer
sammen. *Aftager:* koder med reservepræfiks eller gennemsyns-status er ikke fuldt
styrede — behandl dem med forbehold.

**Valg koder på klyngenavnet**, fordi klynger ikke har element-id. *Aftager:* et
valg identificeres ved spørgsmålets navn, ikke ved svarmulighedens kode.

**Målingens kode indeholder kun skemakoden** — ikke registreringens
aktivitetstype. *Aftager:* aktivitetstypen er **ikke** tilgængelig i den flade
målingstabel. Vejen tilbage går via kildenøglen.

**Kategorien kommer fra rutens konfiguration, ikke fra data**, og er uvalideret
fritekst i både fil og database. *Aftager:* validér ikke blindt mod
standardværdisættet.

**XML'ens kodefelter er oplysninger, ikke koder.** Kodefeltet er for
klassificeringselementer kun et suffiks. *Aftager:* brug originalkode-feltet og
verificér præfikset.

## Værdihåndtering

**Datatypen udledes af kildens typeangivelse; ellers gættes den af værdien.**
*Aftager:* **samme kode kan skifte datatype mellem forekomster**, hvis kildens
typeangivelse mangler. Konflikter opsamles og logges, men blokerer ikke.

**Decimaler bliver mængder — også uden enhed**, fordi standarden ikke har et
decimalfelt på målinger. *Aftager:* decimalkolonnen i den flade tabel er derfor
**altid tom**; værdien ligger i mængdekolonnen. Datapunkt-fladen skelner bagefter
i SQL.

**Heltal mister enheden.** *Aftager:* for heltalsbaserede målinger som puls og
saturation er enheden ikke på ressourcen. Den findes kun som konceptegenskab i
kataloget — værdier bør ikke tolkes uden opslag.

**Dansk decimalkomma håndteres eksplicit.** Alle numeriske værdier er
punktum-separerede i output. Men de står som **tekst** i den flade tabel og skal
castes.

**Booleske værdier accepterer dansk ja/nej.** Andre varianter bliver hverken
boolsk eller tekst — elementet droppes.

**Dato og tid videreføres uændret som tekst.** *Aftager:* værdien kan være både
dato og fuldt tidsstempel, uden tidszone. Håndtér begge former.

**Kodede svar får kun en fritekst, ingen kode.** *Aftager:* kolonnerne for
kodesystem, kode og visningstekst er derfor **altid tomme** for skemamålinger, og
der findes **ingen** kolonne for fritekstværdien. Følgen er, at flaget for
"har værdi" bliver falsk for kodede svar, og at svarteksten kun kan læses i
ressourcens JSON. For valg findes teksten dog også i en egen kolonne. Dette er en
kodeobservation, ikke et dokumenteret valg — bør verificeres fagligt.

**En typekonflikt koster kun det enkelte element**, ikke hele registreringen.
Antallet ses kun i kørselsloggen.

**Enheder gættes heuristisk** ud fra tekstfragmenter i visningsteksten
(blodtryk → mmHg, puls → bpm, saturation → %, temperatur, vægt, højde).
*Aftager:* enheder er **udledt, ikke registreret**, og de er ikke kodede efter
standard. Brug dem ikke til automatisk konvertering.

**Intervaller og forhold er ikke implementeret.** En kildeværdi som "120/80"
bliver én tekst. Al opsplitning er aftagerens ansvar.

## Struktur og identitet

**Ét datapunkt = én selvstændig måling. Ingen komponenter.** *Aftager:*
**blodtryk kommer som to separate målinger**, ikke som én med to komponenter.
Sammenstilling sker ved at gruppere på registreringen og eventuelt klyngen.
Standard-klienter, der forventer panelmønsteret, genkender ikke sammenhængen.

**Ingen relationsfelter til andre ressourcer.** Al sammenkobling mellem måling,
procedure, diagnose og spørgeskemasvar for samme registrering går via
**kildenøglen**. Der findes ingen reference at følge.

**Gentagelser dedupliceres ikke.** Optræder samme element flere gange, bliver
hver forekomst sin egen måling. Begrundelsen er skarp: gentagne elementer med
samme kode *kan* være rigtige data, og på en ny platform før en flerårig
indlæsning ville et stille kollaps skjule legitime målinger og gøre datamodellen
løgnagtig. *Aftager:* forvent flere rækker med samme kode inden for samme
registrering; vælg selv, hvis du kun vil have én.

**Hierarkiet ligger i kolonner, ikke i ressourcestrukturen** — dels fordi det var
overlappende med kataloget, dels fordi serveren ikke kunne materialisere de
nødvendige udtryk. Alle 14 dele er projiceret til navngivne kolonner.

**Forretningsnøglen er registrering plus elementnummer.** *Aftager:*
elementnummeret er en **position**, ikke en semantisk nøgle. Indsættes et nyt felt
midt i skemaet, forskydes numrene for alle efterfølgende datapunkter: de gamle
nøgler pensioneres, og nye ressourcer oprettes for uændrede kliniske værdier.
**Byg tidsserier på registrering plus kode** — ikke på ressource-id eller nøgle —
hvis de skal overleve skemaændringer.

**Deterministiske id'er er fravalgt.** *Aftager:* ressource-id kan ikke
genberegnes fra kildenøglen.

**Dublet-id er en hård fejl.** På en regionskritisk fuldindlæsning ville et
stille kollaps være umuligt at skelne fra korrekt output.

## Status, sletning og aftagerflade

**Slettet giver fejlregistrering; kladde giver foreløbig.** *Aftager:*
kladderegistreringer **kan** identificeres på målinger — modsat procedurer.

**Sletning markeres dobbelt** (status og et fælles sletteflag på ressourcen), så
aftagere kan filtrere med ét udtryk. Men **den flade målingstabel har ingen
sletteflag-kolonne** — status er den eneste indikator, og der findes ingen
automatisk oprydning.

**XML-børn pensioneres**, når de forsvinder fra kilden, med de samme værn som for
procedurer. *Aftager:* en måling kan skifte til fejlregistrering, uden at
kilderegistreringen er slettet — den er blot ikke længere i XML'en.

**Referencer falder tilbage til kildenøglen ved manglende opslag.** *Aftager:*
det giver tomme kolonner i den flade projektion — og fordi patientkolonnen ikke
må være tom, kan sådanne rækker ikke lande i tabellen. Antallet ses kun i loggen.

**Tidspunkt: er både start og slut sat, projiceres datoen ikke.** Kun det simple
tidspunkt kommer med. *Aftager:* målinger med en periode får **tom** dato — og
dermed tomt tidspunkt i datapunkt-fladen. Hvor ofte det sker, er **ikke målt**.

**Per-datapunkt-målingen bærer ikke kontekst-, kilde- eller specialeoplysninger.**
De findes kun på den gamle beholder-model. *Aftager:* speciale,
fortrolighedsniveau, aktivitetshierarki og oprettet/ændret-oplysninger er **ikke
tilgængelige nogen steder** i det flade lag. Åbent punkt.

**Skemakataloget bygges som sideeffekt** af samme XML-gennemløb, uden ekstra
forespørgsler. Det er kilden til enheder, datatyper og klyngestier i
datapunkt-fladen. Konflikter — samme kode med forskellig datatype, enhed eller
tekst på tværs af versioner — markeres og logges. *Aftager:* konceptoplysninger
er **seneste version**, ikke versionen ved registreringen.

**Aftagerfladen er en materialiseret datapunkt-tabel**, ikke et live opslag.
*Aftager:* der er en opdateringsforsinkelse, og målinger uden skemakode
forsvinder helt fra fladen. Enkelte felter tabes på vejen og findes kun i
målingstabellen.

**Kolonnebredden for skemakoden er ikke ens** i de to tabeller (255 mod 450
tegn). Om nogen kode reelt afkortes, er **ikke undersøgt**.

**En rute kan ikke afmeldes ved at slette en linje** i konfigurationsfilen —
fletningen sletter ikke. Afmelding kræver, at rutens status sættes inaktiv eller
gives en slutdato. Fælde, der bør stå i driftsvejledningen.

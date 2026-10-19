<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Beslutningslog, del 1 — Procedure

Se [hovedloggen](beslutningslog_principper.md) for principper og læsevejledning.

## Kilder og population

**De tre kilder, fire fortolkningsveje.** Procedure dannes fra sundhedsfagligt
indhold (hvor både registreringens typekode og indholdet i skemaets XML er
kilder — to adskilte veje i samme tabel), administrative procedurer og
billeddiagnostiske ydelser. Samme fire veje som det gamle flow. *Aftager:*
samme kliniske ydelse kan optræde flere gange — kilderne skelnes kun på
kildesystem-kolonnen. Tæl pr. kilde.

**Populationen styres af en fil, ikke af kode.** Hvilke koder der bliver en
procedure, står i en konfigurationsfil i repoet. Udelukkelser vinder over alle
positive match. *Aftager:* populationen kan ændres uden kodeændring — hold øje
med filens historik. Rækker der ikke matcher, findes stadig som måling eller
spørgeskemasvar; de er ikke tabt.

**Brede kodemønstre er bevidst ikke slået til.** Et mønster på nøgne K- og
B-koder ville også ramme to interne navnerum i kildesystemet, som ikke er
SKS-procedurekoder; begge er desuden eksplicit udelukket. *Aftager:* en
operationskode uden SKS-præfiks i kilden bliver ikke en procedure. Populationen
defineres af præfikset, ikke af kodens form.

**Aktivitetsfamilier danner Procedure; resten gør ikke.** Linjen er: en
SKS-familie, der beskriver noget, *der blev gjort*, danner Procedure. Familier
der beskriver et fund, et stof, en organisation eller en modifikator gør ikke
— de har enten deres eget spor eller er ikke en handling. Konkret: `opr`,
`pro`, hele `und` (ikke kun U-stammen), `adm` og enhver *ukendt* familie
danner Procedure. Undtaget er `dia` (eget diagnosespor), `til` og
`SKS_Vaerdier` (tillægskoder på den procedure, de hører til), `atc`
(lægemiddelstof), `afd`/`sgh` (organisation) og `spc` (speciale). *Aftager:*
populationen er bredere end de tidligere tre præfikser. En ny familie tælles
med som standard, så den ikke forsvinder lydløst.

**Ingen særregel på skematype.** Udvælgelsen ser kun på aktivitetstypen.
En række med `SKS.a.adm` bliver Procedure, også når skematypen er et besøg.
Det er samme akse som referencen: skematypen er hverken inklusion eller
udelukkelse. *Aftager:* vil man holde besøg ude, filtrerer man selv — som
man gjorde på referencen.

**En tillægskode eller en talværdi alene bliver ikke en procedure.** En
registrering, hvis *typekode* er `SKS.a.til*` eller `SKS_Vaerdier.*`, danner
ikke en selvstændig Procedure. Familierne mangler ikke — de ligger på de
procedurer, de hører til.

**RESULTATTYPE er ikke en akse.** En række uden SKS-typekode bliver ikke
procedure, selv om skematypen er en operations- eller procedurebeskrivelse.

**Dobbelttælling mellem SFI-undersøgelser og billeddiagnostik slås ikke
sammen.** Samme ydelse kan komme fra begge kilder. *Aftager:* tæl pr. kilde.
En fælles hændelsesnøgle indføres kun, hvis der findes et *eksakt* match —
ikke fuzzy. Indtil da er sporene adskilte.

**Proceduregruppe hører ikke til i dette lag.** En sammensat gruppestreng
(hovedkode + deloperationer + tillægskoder pr. procedure, kontakt eller
forløb) kan beregnes nedstrøms. *Aftager:* FHIR-sporet leverer de
normaliserede rækker; gruppenøglen bygges dér, hvor den skal bruges.

**Alle administrative procedurer medtages** — også slettede og
ikke-indberetningspligtige, som det gamle warehouse smed væk.

**Billeddiagnostik: kun endelige svar.** Bestillinger, foreløbige svar og afviste
rekvisitioner springes over. Møder kørslen en ukendt tilstand, **stopper den med
fejl** frem for at gætte. Én ydelse giver 1..N procedurer — én pr. undersøgelse i
svaret. *Aftager:* tæl pr. undersøgelse, ikke pr. svardokument; bestillingen
findes som en henvisning.

## Kodefortolkning

**Hovedkoden kommer fra typekoden, ikke fra XML'en.** Verificeret: XML'ens
operationskode er identisk med typekoden — XML'en bekræfter blot. Hovedkoden er
derfor stabil og uafhængig af XML-kvalitet.

**Den fulde kode står i originalkode-feltet, aldrig i kodefeltet.** Verificeret:
kodefeltet er **kun et suffiks**, observeret ned til ét tegn. *Aftager:* koderne
i tillægskodetabellen er fulde SKS-koder. Spørgeskemasporet bruger stadig
suffikset — se del 4.

**Ingen filtrering på kodefamilie.** Tillægskodefeltet bærer alle SKS-familier:
sideangivelser, operations-, undersøgelses-, lægemiddel- og administrative koder.
Fagligt bekræftet: er noget registreret som tillægskode, *er* det en tillægskode
— og en operationskode som tillægskode er **ikke** en deloperation.

**Beregnede talværdi-tillægskoder.** Fire ud af ti procedureregistreringer har
tillægskoder, der kun står som et tal uden kode. Tallet konverteres efter faste
regler for fortegn, længde, decimalseparator og foranstillede nuller — dansk
decimalkomma. Formatet er valideret mod produktionsdata fra en uafhængig kilde,
hvor kildesystemet selv leverer færdigberegnede koder af samme form. Det gamle
SAS-udtræk tog kun heltal med en bestemt ledetekst og lagde alle koder i én spand
uden den konkrete værdi; kommatal blev aldrig implementeret. *Aftager:* forvent
den konkrete kode, ikke en spand.

**Grænseværdier: heltal klampes, kommatal droppes.** Inkonsistensen er bevidst
men uafklaret; den nationale models facit er at trunkere. *Aftager:*
kommatalsværdier lige under 1000 giver ingen tillægskode — man kan ikke se, at
der stod noget. Ingen af 500 stikprøver ramte grænsen.

**Første kode er hovedkoden, markeret som valgt.** Markøren findes kun på
ressourcer skrevet efter 5. august 2026 — se del 5 om efterslæbet.

**Samme kode skrives kun én gang pr. procedure.** Hovedkoden vinder ved
kollision. Multiplicitet tabes bevidst, fordi tillægskodetabellens nøgle ikke kan
bære to identiske koder. *Aftager:* tæl ikke multiplicitet ud af tabellen.

**Side, kontrast og personalekategori er felter, ikke koder.** Fagligt bekræftet:
værdierne *var* tillægskoder før LPR3 og indberettes efter LPR3 som særlige
felter — og det gamle warehouse konverterede dem heller ikke. Numerisk marginalt
(side udfyldt på 229 af 304.000 rækker, kontrast på 1).

## Deloperationer

**Klyngen er den eneste pålidelige rollemarkør.** Primær operation og
deloperation deler både elementnavn og element-id; ledeteksten varierer
usystematisk. Kun klyngenavnet afgør rollen. Verificeret på tværs af specialer.

**Hver deloperation er sin egen procedure med reference til hovedoperationen.**
Én registrering giver 1+N ressourcer. To uafhængige argumenter: den nationale
models granularitet siger det samme, og en beviscase i egne data viser, at
alternativet taber data — en bilateral operation, hvor samme operationskode
optræder som både hoved- og deloperation med hver sin sideangivelse, ville
kollapse til én række. Fagligt bekræftet mod LPR3. *Aftager:* **tæl 1+N.**

**Tillægskoder er klyngekorrekte.** En tillægskode lander kun på den operation,
den deler klynge med. *Aftager:* slå tillægskoder op på den rigtige procedure —
saml ikke alle koder fra dokumentet.

**Deloperationens nøgle er en position, ikke en identitet.** Slettes den første
deloperation, overtager den næste nummeret. Den nationale models tilsvarende felt
har samme egenskab. Kilden udstiller intet id pr. deloperation, hvis stabilitet på
tværs af redigeringer er bekræftet — XML'ens metadata-id er en kandidat, men
afklaring udestår hos kildesystemet. *Aftager:* følg ikke en enkelt deloperation over tid via
nummeret.

**Brug ikke oprettelsestidspunkt på deloperations-niveau.** Fordi nøglen er en
position, fortæller rækkens tekniske oprettelsestidspunkt, hvornår *pladsen*
først blev set — ikke hvornår deloperationen blev registreret. Rykker indholdet
en plads ved en sletning midt i listen, beholder rækken sit gamle tidsstempel,
og slettemarkeringen rammer den sidste plads, selvom det klinisk var en
tidligere, der udgik. Nu-billedet er altid korrekt; det er kun historikken pr.
deloperation, der ikke kan læses af nøglen. *Aftager:* kliniske tidspunkter
læses fra registreringens egne tidsfelter; tekniske indsættelsestidspunkter er
kun meningsfulde på registreringsniveau (hovedoperationen).

**Deloperationen arver forældrens kontekst, tid og status.** *Aftager:* et opslag
på patient og tid ser både hoved og dele — filtrér, hvis du kun vil have én.

## Knivtid og personale

**Knivtid ligger i kildeoplysningerne, én pr. registrering** — og som kolonner
på den flade tabel: rigtige tidspunkter, ikke tekst, med varigheden beregnet i
byggelaget. Start uden slut tolereres (kildedata, målt i promille). *Aftager:*
varigheden er tom, hvis et af tidspunkterne mangler, ikke kan læses eller går
baglæns — den fabrikeres ikke.

**Knivtid er ikke det samme som procedurens udførelsesperiode.**
Udførelsesperioden kommer fra kilderækkens aktivitetsperiode og er
administrativ; knivtiden er det tidsrum, der reelt blev opereret. Til
operationsplanlægning er knivtiden den interessante. *Aftager:* vi overskriver
**ikke** udførelsesperioden med knivtid — begge har aftagere, og de skal ikke
forveksles. Bemærk også, at tidspunkterne er danske vægur-tidspunkter: sådan som
de blev registreret.

**Operationspersonale bliver udførende personer med rolle.** Rollen læses fra
personrækkens ledetekst — ikke fra klyngenavnet, som ville give alle samme
rolle. Både operationspersonale-klyngen og "Anlagt af/kompetence" medtages.
Person-id findes i ca. 2 % af rækkerne; ellers identificeres personen ved
navnetekst. *Aftager:*
personale ligger i ressourcens JSON, ikke som kolonner — og de to klynger kan
ikke skelnes maskinelt, så en opgørelse over operationshold kan tælle andet
personale med.

**Flere personer i samme klynge kollapser til én.** Produktionsdata viser én
person pr. klynge uden undtagelse, men brydes antagelsen i et ukendt skema,
forsvinder de øvrige lydløst fra proceduren. De findes stadig som
spørgeskemasvar.

## Identitet, status og livscyklus

**Ressource-id er serverteknisk; forretningsnøglen ligger i identifieren.**
*Aftager:* join på kildenøglen, aldrig på et udregnet id. Kildenøgle-kolonnen
virker for alle tre kilder og hører altid sammen med kildesystem-kolonnen: den
ene fortæller hvilken kilde, den anden nøglen i den kilde. Den ældre
nøglekolonne dækker fortsat kun sundhedsfagligt indhold — bevaret, fordi den
har aftagere.

**Status er binær: gennemført eller fejlregistrering.** Den flade tabel har ingen
sletteflag-kolonne — status er den eneste markør.

**Kladder tæller ikke som gennemførte.** En registrering, hvis dokumentation
ikke er færdiggjort, får status "i gang" — samme værdi som spørgeskemasvar.
Trappen er: slettet slår kladde, kladde slår færdig. Deloperationer arver
hovedoperationens status. *Aftager:* tæl ikke kladder som gennemførte; de kan
medtages ved også at læse status "i gang". Bemærk nuancen: "i gang" betyder i
standarden, at proceduren er i gang — ikke nødvendigvis at noten er et udkast;
indgrebet kan være foretaget.

**XML-afledte børn pensioneres, når de forsvinder fra kilden.** Forsvinder en
deloperation fra XML'en, markeres den som fejlregistrering ved næste kørsel af
forælderen. Der slettes ikke fysisk. Tre værn: en parsefejl holder forælderen
ude, en byggefejl afbryder oprydningen for kørslen, og en tømt XML fjerner
bevidst alle børn. *Aftager:* oprydningen sker kun for de forældre, en kørsel
faktisk hentede. Ældre forældreløse rækker kan findes, indtil forælderen ændres i
kildesystemet — og en måneds- eller fuldkørsel kan markere mange historiske børn
på én gang. Det er forventet oprydning, ikke en fejl.

**Hele kilderækker, der forsvinder, er ikke dækket.** Overvågningen omfatter kun
patienter, kontakter og forløb. *Aftager:* en procedure kan stå som aktiv, efter
at kilderækken er væk. Åbent punkt.

## Relationer og tilgængelighed

**Kontakt og forløb kan begge være sat.** Vi kopierer kilden; den nationale model
valgte én. *Aftager:* dobbeltudfyldning er **ikke en fejl**. Begge tomme betyder
enten manglende tilknytning i kilden eller et fejlet opslag — de to kan ikke
skelnes i den flade tabel.

**Billeddiagnostiske procedurer peger på deres bestilling**, så kæden kan følges.
Ikke en kolonne. **Ingen henvisningsdiagnose i procedurekoden:** koder under
diagnose-elementet springes over — de er årsagen, ikke den udførte ydelse.

**Hoved- og deloperationer skilles på pegepinden:** tom pegepind er en
hovedoperation. *Aftager:* tæl aldrig "operationer" uden at vælge niveau.

**Ikke tilgængeligt uden at læse JSON:** operationspersonale med rolle,
udførende afdeling, og rækkefølgen blandt tillægskoder. Bevidst valg: kolonner
bygges, når en opgørelse er navngivet — hvad der allerede er kolonner, står i
lineage. **Sig til, hvis en opgørelse skal kunne køre uden at læse JSON.**

## Organisation og kodetekster på aftagerfladen

**Organisationen hentes gennem referencen; den kopieres ikke.** Udførende enhed
og stamafdeling står som performer med hver sin funktionskode, og navn, SOR-kode,
type og overordnet enhed hentes i organisationstabellen. Alternativet — at skrive
dem ned på hver procedure — blev fravalgt, fordi de to steder kan nå at pege hver
sin vej, og så er der ingen at tro på. *Aftager:* join til organisationstabellen
på `performer_organization_id` og `stamafdeling_organization_id`.

**Kildesystemets egne afdelingskoder bliver derimod stående som kolonner.**
Grænsen mellem »hent ved opslag« og »behold som kolonne« er målt, ikke skønnet:
de seks koder findes hverken i SOR eller i SHAK — nul træffere for alle seks — så
de kan ikke oversættes til en organisation. De er kildens egne fakta, ikke en
kopi af opslagsdata. *Aftager:* de seks kolonner er kildesystemets interne koder
og kan ikke joines til organisationstabellen.

**Seks kodetekster er flyttet til kodesystemet; resten bliver stående rå.**
Kravet er entydighed: teksten skal være entydigt bestemt af koden, og koden skal
være fuldt kvalificeret, så den bærer sin egen liste. Seks felter opfyldte det;
statuskode og statustekst gjorde ikke og står derfor som to rå kolonner.
*Aftager:* teksten joines på i aftagerviewet under sit oprindelige navn —
opslaget er ikke synligt for den, der læser viewet.

**Kildens statuskode leveres ved siden af FHIR-statussen.** Kolonnen `status` er
en FHIR-oversættelse af sletteflaget og skelner kun mellem gennemført og
tilbagekaldt registrering; den er ikke kildens egen statuskode. Den står i
`status_code` med sin tekst i `status_text`. *Aftager:* brug `status_code`, hvis
du vil vide, hvad kildesystemet selv kaldte tilstanden.

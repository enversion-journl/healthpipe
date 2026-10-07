<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Beslutningslog, del 4 — Spørgeskemasvar og routing

Se [hovedloggen](beslutningslog_principper.md) for principper og læsevejledning.

## Grundmodellen

**Skemakoden er den fælles nøgle på tværs af ressourcetyper.** Hvert element i et
skema får sin egen repræsentation, og koden binder måling, skabelon og svar
sammen — materialiseret som et katalog. Den tidligere model var én stor beholder
pr. registrering med hele XML'en i komponenter og syv grupper af tilføjelser.
*Aftager:* et datapunkt er en ressource eller et svar-element, ikke en komponent i
en beholder.

**Spørgeskemasvar er standardvejen.** En registrering bliver kun en måling, hvis
dens aktivitetstype står på en godkendt liste; alt andet — inklusive **ukendte og
nye skemaer** — bliver spørgeskemasvar, fordi den form bevarer vilkårlig
skemastruktur uden at opfinde heuristikker. *Aftager:* forvent at det store
flertal af skemaindhold ligger som spørgeskemasvar. Nye skemaer forsvinder ikke —
de kommer altid ind som svar, aldrig som måling, indtil en rute er godkendt.

**En rute i gennemsyn betyder "endnu ikke måling", ikke "fejl".** Der findes ikke
længere en driftstilstand, hvor manglende kuratering stopper pipelinen. *Aftager:*
**data skifter ressourcetype, når en rute godkendes.** En opgørelse, der kun
læser målinger, ændrer sig dermed uden at kilden har ændret sig.

**Ruteafgørelsen revideres pr. registrering** med måltype, kildeangivelse,
sikkerhed og en fritekst-begrundelse. *Aftager:* man kan for hver registrering slå
op, hvorfor den blev det ene og ikke det andet. To forbehold: der er kun **én
række pr. registrering**, så når en registrering giver flere afgørelser (fx både
en procedure og en bevaring som svar), vinder den sidst skrevne — og en af
kildeangivelsernes navne er historisk og betyder i dag blot "ikke på listen".

## Sameksistens mellem ressourcetyper

**En procedurekode giver altid en procedure — parallelt med resten.** Derefter
routes skemaets indhold normalt. *Aftager:* samme registrering optræder som både
procedure og måling/svar. Tæl ikke på tværs uden at deduplikere på kildenøglen.

**Procedure- eller diagnoserækker uden indhold i skemaet giver hverken måling
eller svar.** *Aftager:* en rent kodet operation findes **kun** som procedure —
søg den ikke i svarene eller i datapunkt-fladen.

**Numerisk/ikke-numerisk split: samme række giver både måling og svar.** For en
kodet række på den godkendte liste bliver kun de numeriske datapunkter målinger;
resten bevares som svar. Splittet er disjunkt — der dobbelttælles ikke.
*Aftager:* én kilderegistrering kan legitimt have datapunkter i **begge** ben af
datapunkt-fladen.

**Gav den numeriske filtrering ingen målinger, bevares hele rækken som svar.**
*Aftager:* aktivitetstyper på den godkendte liste kan alligevel dukke op som svar
— ressourcetypen er ikke en funktion af aktivitetstypen alene.

**Ingen undertrykkelse på element-niveau.** Et diagnose-element eller en
deloperation, der bliver sin egen ressource, fjernes **ikke** fra svarets
elementliste. *Aftager:* samme kliniske indhold findes to steder — som
struktureret ressource og som datapunkt. Tæller man diagnoser via
datapunkt-fladen, tæller man noget andet end via diagnosetabellen. Denne
asymmetri er ikke begrundet nogen steder.

## Identitet: spørgsmål, svar og gentagelser

**Skemakoden er spørgsmålets nøgle; et element-id er forekomstens.** Skabelon og
svar deler begge. *Aftager:* join på **både** kode og forekomstnøgle — ikke på
koden alene.

**Forekomstnøglen er en position inden for ét dokument**, ikke en stabil
identitet for et spørgsmål. Ændrer XML'ens rækkefølge sig mellem to versioner af
samme skema, peger samme nøgle på noget andet. Findes den primære nøgle ikke,
dannes en hash — som blandt andet indeholder **værdien**, så nøglen skifter, når
svaret ændres. Stabilitetsgrænsen er ikke dokumenteret andre steder.

**Svar dedupliceres ikke.** Hver svarbærende række giver præcis ét element.
*Aftager:* forvent **ikke** unikhed på registrering plus kode; aggregér eksplicit.

**Skabelonens elementer sammenflettes på den sammensatte nøgle** — forekomst-
nøglen alene ville lade to begreber på samme position kollidere og stille tabe
det ene. *Aftager:* skabelonen er **ikke** en unik spørgsmålsliste.

**Flad elementstruktur.** Ingen indlejrede grupper — dels overlappede de
kataloget, dels kunne serveren ikke materialisere de nødvendige udtryk.
Klyngestruktur rekonstrueres fra kolonner, ikke fra ressourcens træstruktur.

**Svarindeks er en konstant.** Der sættes højst ét svar pr. element, og
indekskolonnen bærer ingen information ud over "har svar / har ikke svar".
*Aftager:* får et skema flere svar pr. element, kolliderer nøglen. Uudtalt
begrænsning.

**Elementer uden svar bevares.** Et tomt felt er data: "spørgsmålet blev stillet,
ikke besvaret". *Aftager:* brug flaget for "har værdi" til at skelne.

**Obligatoriskhed sættes aldrig.** Kolonnen er altid tom — det kan ikke læses af
leverancen.

**Parser-sentinels udelukkes.** En registrering med ugyldig XML giver en **tom
svarskal** — en ressource uden elementer. Fordi statusfeltet kun bæres på
elementniveau, kan årsagen ikke ses på selve svaret.

## Kodning af svar

**Svarkoden bruger kodefeltet, ikke originalkode-feltet.** For strukturerede
kodeelementer er kodefeltet kun et **afkortet suffiks** — samme feltforveksling,
som diagnosesporet har rettet. **Spørgeskemasporet er ikke rettet.** *Aftager:*
læs **ikke** svarets kodekolonne som en kodeværdi — brug originalkode-kolonnen.
Åbent punkt.

**Et valgs svarkode er klyngenavnet** — altså spørgsmålet — mens det valgte svar
kun står i visningsteksten. *Aftager:* for klyngevalg skal svaret læses fra
visningsteksten eller den dedikerede kolonne, aldrig fra kodekolonnen. Koden er
konstant for alle svar på samme spørgsmål. Ingen begrundelse findes; fremstår
utilsigtet.

**Kodesystemet sættes altid til skemasystemet**, uanset hvor svarets kode kommer
fra. *Aftager:* systemet identificerer **ikke** svarets egentlige kodesystem.

**Præfiks strippes af visningstekster** på formen "tal: tekst". *Aftager:*
indeholder svarteksten selv et kolon, afkortes den — og den rå værdi bevares ikke
andetsteds for kodede svar.

**Værdimapningen er bevidst forskellig for målinger og svar.** Samme kliniske
værdi ser forskellig ud i de to ben; for kodede svar mister målingsbenet koden
helt. **Tomme værdier dropper målinger, men bevares som svar** — hvilket gør
flaget for "har værdi" systematisk skævt mod svar-benet.

**En typekonflikt koster ingenting i svar-benet:** værdien falder tilbage til
tekst, så kilden aldrig tabes. *Aftager:* et felt deklareret som tal kan dukke op
som tekst — sammenlign værditypen med den forventede for at finde dem.

## Skemamodellen

**Ét skema pr. navn og version.** Én ny skemaversion i kilden giver én ny
ressource. **Svar er versionspinnet**, og en manglende version er en hård fejl:
et svar uden pinnet skema er ikke analyserbart. *Aftager:* enhver besvarelse kan
opløses til præcis den skemaversion, der gjaldt.

**Skabelonen er et akkumulerende katalog, ikke et snapshot.** En senere kørsel
kan se en delmængde af skemaets elementer, så eksisterende elementer bevares ved
genskrivning — ellers ville tidligere svars nøgler forsvinde. *Aftager:*
skabelonen er **strukturelt tilladt at være ufuldstændig** og må ikke bruges som
autoritativ liste over et skemas spørgsmål. Den afstemmes ikke: rækker forsvinder
ikke, selv om et element fjernes fra skemaet.

**Nye skemaer kræver ingen konfiguration** — de opdages af data. Men de kommer
altid ind som svar, ikke som måling.

**Metadata der bevidst ikke sættes:** status er hårdkodet, dato og udgiver sættes
aldrig, og skemakoden har intet kodesystem. *Aftager:* de felter bærer ingen
information og kan ikke joines mod et kodesystem.

**Kontekstoplysninger indlejres ikke i svaret** — sporbarheden bliver på
kilderækken. Registreringens overskrift findes på skabelonen, ikke på svaret.

## Datapunkt-fladen

**Datapunkter er de to ben forenet:** målinger og svar-elementer, beriget med
kataloget og med kontaktens organisation som reserve. En kolonne viser hvilket
ben. *Aftager:* ét opslag dækker begge routingudfald — filtrér på benet for at
forstå værdisemantikken.

**Svar gates ikke på skabelonen** — et manglende skabelon-match må aldrig koste
et klinisk svar. *Aftager:* ingen svar tabes på grund af en ufuldstændig
skabelon; til gengæld kan konceptoplysninger være tomme, hvis koden endnu ikke
er i kataloget.

**Forkontrollen er hård på det, der kan koste et svar,** og rådgivende på resten:
kørslen afbrydes ved svar uden kode og ved uløselige skemareferencer, men ikke
ved manglende skabelon-match. *Aftager:* en fejlet opdatering betyder en reel
kildedefekt.

**Forkontrollen er sikker som standard** — pipelinen skal være korrekt ud fra
standardværdier, og netop den historiske indlæsningsvej er den, værnet mest af
alt skal dække. *Aftager:* verificér den faktiske indstilling i det miljø, du
læser data fra.

**Opdatering sker som slet-og-indsæt pr. ændret ressource**, i én transaktion,
med vandmærket rykket efter commit. Ændringsdetektionen medtager også
svar-elementer, fordi et fjernet svar ændrer elementrækken men ikke
hovedressourcen. *Aftager:* fjernes et svar, forsvinder datapunktet ved næste
opdatering. Datapunkter er "sådan som XML'en ser ud nu", ikke en historik.

**Nul datapunkter for en besvarelse er et overvågningssignal, ikke en gate** —
legitime nuller findes (besvarelser uden skemaelementer).

**Sletteflaget filtreres ikke i datapunkt-fladen.** Svar-elementer fik et
sletteflag, men hverken opbygningen eller opdateringen filtrerer på det.
*Aftager:* forældede svar kan flyde ind i fladen. Filtrér selv på flaget ved
læsning af elementtabellen. Åbent punkt.

**Besvarelser pensioneres ikke.** De fire familier af XML-børn omfatter ikke
spørgeskemasvar. En besvarelse, der forsvinder fra kilden, retireres af intet lag
— mens dens elementtabel har fuld oprydning. Asymmetrien er ikke begrundet.

**Sletmarkeringen ligger på både forælder og barn.** Skemasvarets ressource
bærer `urn:fhir:isDeleted` fra kildens `SLETTET`, og den flade tabel
`fhir.QuestionnaireResponse` projicerer den som `dw_is_deleted` — samme
mønster som Procedure og Condition. Barnetabellen `QuestionnaireResponse_item`
har haft kolonnen hele tiden. *Aftager:* filtrér `dw_is_deleted = 0` på
forælderen, som på de øvrige fhir-tabeller. Datapunkt-fladen filtrerer
stadig ikke selv på flaget.

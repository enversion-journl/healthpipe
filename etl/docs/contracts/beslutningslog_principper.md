<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Beslutningslog, del 0 — Formål og bærende principper

**Formål:** Dokumentere de **fortolkningsvalg**, der ligger mellem kildesystemet
og de data, en aftager læser. Alt her er noget, man *ikke* kan læse ud af et
data lineage-diagram eller en tabeldefinition: hvorfor en kode blev læst fra det
ene felt og ikke det andet, hvorfor en række blev bevaret frem for frasorteret,
hvad der bevidst ikke er implementeret, og hvilke antagelser der er verificeret
mod virkelige data.

**Dækning:** IFDB (Cosmic EPJ) → FHIR (Aidbox) → flade SQL-on-FHIR-tabeller, for
ressourcerne Procedure, Condition, Observation og QuestionnaireResponse samt de
tværgående mekanismer. Perioden er februar 2026 og frem.

**Sådan læses en post:** Principperne her i del 0 føres som *Valgt* /
*Alternativ* / *Begrundelse* / *Betydning for aftager* / *Status*. Delrapporterne
(del 1–5) fører hver beslutning som en fed konstatering med begrundelse og en
*Aftager:*-anvisning, samlet under emneafsnit.

**Vedligeholdelse:** Denne fil er den varige hukommelse. Opgavebeskrivelser og
kommentartråde i opgavestyringssystemet flytter ikke med til en ny platform —
denne fil gør, fordi den ligger i repoet. Føres en beslutning kun i en
opgavetråd, hører den også her.

## Bærende principper

Seks principper går igen i næsten alle enkeltbeslutninger. Kender man dem, kan
man forudsige de fleste valg længere nede.

## Lossless: markér frem for at frasortere

**Valgt:** Ingen kilderække smides væk i indlæsningen. Slettede registreringer
bevares med fejlregistrerings-status og et sletteflag; registreringer der ikke
skal indberettes til Landspatientregisteret bevares med et flag; koder skrives
som de blev registreret, uden gyldighedsfilter.

**Alternativ:** Det gamle datawarehouse frasorterede alle tre kategorier i
indlæsningen.

**Begrundelse:** Aftagerne skal selv kunne vælge population. En frasortering i
indlæsningen kan ikke fortrydes nedstrøms; en markering kan altid filtreres.

**Betydning for aftager:** **Enhver opgørelse skal filtrere aktivt.** Man kan
ikke slutte fra "findes i tabellen" til "er en gældende registrering". Til
gengæld kan man ikke komme til at mangle data, man ikke vidste var væk.

**Status:** Fastlagt. Fagligt bekræftet, at det gamle flows gyldighedstrin
heller ikke frasorterede — begge muligheder blev bevaret dér.

## Ingen kliniske kendsgerninger inline på en anden ressource

**Valgt:** Diagnoser, procedurekoder og tillægskoder er selvstændige rækker i
domænetabeller. Andre ressourcer peger på dem med referencer eller via
child-tabeller. En henvisningsdiagnose bliver altså en diagnose, ikke et felt på
henvisningen.

**Betydning for aftager:** Man skal joine — men til gengæld findes hver klinisk
kendsgerning ét sted, uanset hvilken ressource der refererer til den.

**Status:** Fastlagt (august 2026).

## `userSelected` markerer kildens primære kode

**Valgt:** Den kode, kilden har valgt som hovedkode, markeres med
`userSelected = true`. Tillægskoder markeres ikke. De flade tabellers
hovedkode-kolonner vælger via markøren; tillægskode-tabellerne tager
komplementet.

**Alternativ:** Positionsbaseret konvention ("første kode er hovedkoden") — mere
skrøbelig, fordi rækkefølgen kan ændre sig.

**Faldgrube, der er værd at kende:** child-tabellernes filter skal skrives som
"markøren er fraværende **eller** falsk" — ikke som "forskellig fra sand". Det
sidste giver en tom mængde, når feltet mangler, og dermed tomme child-tabeller.

**Betydning for aftager:** Markøren findes kun på ressourcer skrevet efter
indførelsen. På ældre, urørte ressourcer kan hovedkode-kolonnen være tom, og
hovedkoden ligge i tillægskode-tabellen — lukkes af genkørslen.

**Status:** Fastlagt. Konsekvensen for historiske ressourcer er en kendt,
accepteret begrænsning indtil genkørslen.

## Diagnosekoder kanoniseres til SKS-D; alt andet gør ikke

**Valgt:** Primære diagnosekoder føres til dansk SKS-D-form: præfiks strippes,
og er koden ren ICD-10 (`J44`), præfikses den med `D` (`DJ44`). Reglen er
idempotent og bruges af **alle** diagnose-producenter. Tillægskoder,
procedurekoder og ydelseskoder kanoniseres **ikke** — de får kun præfikset
strippet og beholder deres eget kodesystem.

**Begrundelse:** Kravet kommer fra den danske FHIR-kerneprofil. Tidligere gav
samme kildekode forskellige resultater i forskellige pipelines (`DI10` i den ene,
`I10` i den anden). Tillægskoder kan ikke kanoniseres, fordi mange af dem slet
ikke er ICD-10 — blandt dem ATC-lægemiddelkoder.

**Betydning for aftager:** **Skel altid på kodesystem, ikke kun på kode.** En
diagnosekode og en tillægskode kan se ens ud og betyde noget forskelligt. Og:
opslag af diagnosekoder via FHIR-serverens indbyggede terminologi virker ikke
efter skiftet — det er en bevidst platformsafvigelse; brug
terminologiserveren/SKS-registret direkte.

**Status:** Fastlagt; gælder alle stier.

## Værdier der ikke kan fortolkes, kastes væk frem for at blive falske koder

**Valgt:** En værdi, der ikke matcher en gyldig kodeform (skemaindekser som
`D`, `9`, `Z`, fritekst) bliver ikke en kode. For nogle kilder postes ressourcen
alligevel med den rå værdi som fritekst og uden kode.

**Betydning for aftager:** Man får ikke skrald i kodefelterne — men der findes
rækker med tom kode. For RIS-diagnoser med ukendt kode ligger den rå værdi kun i
ressourcens JSON, ikke i den flade tabel.

**Status:** Fastlagt.

## Antagelser verificeres mod produktionsdata, ikke mod intuition

Flere beslutninger i loggen er truffet efter en måling, og målingen står i
posten. Tre eksempler på, at det var nødvendigt:

- Antagelsen om, at tillægskoder kun er "til"-familien, var forkert:
  tillægskodefeltet bærer **alle** SKS-familier, inklusive operations-, ATC- og
  undersøgelseskoder.
- Antagelsen om, at deloperationer var sjældne (0,2 %), byggede på en måling der
  målte det forkerte; den rigtige akse gav knap 10 % af operationsregistreringerne.
- Antagelsen om, at talværdi-tillægskoder var en detalje, byggede på queries der
  **principielt ikke kunne se dem** (de søgte i kodefelter; kategorien har ingen
  kode). Den udgør fire ud af ti procedureregistreringer.

**Lære, der gælder fremad:** en negativ konklusion kræver en måling, der faktisk
kan observere fænomenet. Ellers bekræfter den kun sit eget blinde punkt.


---

## Indhold — de fem delrapporter

| Del | Fil | Dækker |
|---|---|---|
| 1 | [Procedure](beslutningslog_procedure.md) | De tre procedurekilder, tillægskoder, deloperationer, knivtid, personale |
| 2 | [Diagnoser](beslutningslog_condition.md) | Fem diagnoseveje med identifier-system, kodekanonisering, diagnosetyper, statusmapping |
| 3 | [Observationer](beslutningslog_observation.md) | Hvad der bliver en måling, værdihåndtering, enheder, struktur |
| 4 | [Spørgeskemasvar og routing](beslutningslog_questionnaire.md) | Hvilken ressource en registrering bliver, skemamodellen, datapunkterne |
| 5 | [Tværgående](beslutningslog_tvaergaaende.md) | Kodenormalisering, XML-parseren, sletning, identitet, flade tabeller, delta |

## Hurtig orientering — de otte ting, der oftest overrasker

1. **Filtrér altid aktivt.** Tabellerne indeholder slettede, tilbagekaldte og
   ikke-indberetningspligtige registreringer, fordi intet frasorteres i
   indlæsningen.
2. **Antal ressourcer > antal kilderækker.** Én registrering kan blive både en
   procedure og en måling eller et spørgeskemasvar, og en operation med
   deloperationer bliver 1+N procedurer.
3. **Gruppér altid på kildesystem.** Diagnosetabellen blander fem veje,
   proceduretabellen tre. Henvisningsdiagnoser fra billeddiagnostik er ikke
   kontaktdiagnoser. XML-diagnoser har ingen aktions-/bi-rolle.
4. **Skel på kodesystem, ikke kun på kode.** Diagnosekoder og tillægskoder kan
   se ens ud og betyde forskelligt.
5. **Ressource-id er ikke en kildenøgle.** Join på kildenøglen, aldrig på et
   udregnet id.
6. **Løbenumre er positioner, ikke identiteter.** En deloperations nummer kan
   skifte betydning, hvis en tidligere deloperation slettes.
7. **Noget ligger kun i ressourcens JSON.** Personale og udførende afdeling er
   ikke kolonner i de flade tabeller. Knivtid og LPR-flaget blev kolonner i
   august 2026.
8. **Måling slår intuition.** Flere af de største fund i dette spor var
   antagelser, der viste sig forkerte, fordi den første måling ikke kunne se
   fænomenet. Spørg efter tallet.

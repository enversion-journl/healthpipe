<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Procedure-populationen: forskelle mod det gamle DWH-flow

> **Målgruppe:** aftagere af `fhir.Procedure` og `fhir.Procedure_tillaegskode`.
> **Senest opdateret:** 2026-09-01 (SFI-sporet: operationskoder & delkoder).

FHIR-sporet er bevidst **lossless**: registreringer frasorteres ikke i
indlæsningen, men markeres, så aftagere selv vælger population. Det giver
tre forskelle mod det gamle SAS/LPR-flow — plus tre udvidelser af, hvad
Procedure-data indeholder.

## Bevidste populationsforskelle

| # | Forskel | Gammelt flow | FHIR-sporet | Aftager-håndtering |
|---|---|---|---|---|
| 1 | Slettede registreringer | Frasorteret | Beholdes med `status = entered-in-error` + isDeleted-extension | Filtrér på status/flag |
| 2 | LPR3-ignorerede | Frasorteret (`LPR3_IGNORED = 0`) | Beholdes. Alle tre veje bærer det søgbare mærke, projiceret som `fhir.Procedure.lpr3_ignored` (0 eller 1) | Filtrér på kolonnen, hvis LPR-paritet ønskes |
| 3 | SKS-gyldighedsvalidering | Koder blev holdt op mod gyldighedsperioder | Koder skrives som registreret (`userSelected = true`); gyldighed og kodetekst er terminologi-opslag ved forbrug. `cs_sks` indeholder kun den nyeste version — tidskorrekt opslag er ikke muligt | Slå op i kodedimension/terminologiserver; forvent ikke historisk gyldighed |
| 4 | RESULTATTYPE som akse | referencen medtager også rækker uden SKS-typekode, hvis skematypen er en operations-/procedurebeskrivelse | RESULTATTYPE er **ikke** en akse. Procedure dannes kun, når typekoden er en SKS-aktivitetsfamilie — også når skematypen er et besøg | Tæl Procedure-rækker, ikke kilderækker. Besøg med `SKS.a.adm` følger med; filtrér selv hvis de skal ud |
| 5 | `til*` / `SKS_Vaerdier` som hovedkode | referencen tæller `SKS.a.tilZP` og `SKS_Vaerdier.a.*` som procedurepopulation | Danner **ikke** Procedure-ressource som hovedkode (familierne indgår som tillægskoder) | Brug tillægskode-tabellen for disse koder |

## Udvidelser (2026-08)

**6. Hele undersøgelsesfamilien (`und*`) danner procedurer.** Kliniske
undersøgelser (UL-skanninger, ekkokardiografi, EMG, osteodensitometri m.fl.)
indgår — ikke kun U-stammen. Administrative typekoder (`adm*`) indgår også,
uden særregel på skematype. OBS: billeddiagnostiske ydelseskoder
(UXU*/UXR*) kan forekomme BÅDE som SFI-registrering og som RIS-ydelse for
samme undersøgelse — kilderne adskilles på identifier-system. Deduplikering
på tværs er ikke indført; tæl pr. kilde. En fælles hændelsesnøgle kræver
eksakt match.

**7. Deloperationer er selvstændige Procedure-ressourcer.** En
operationsregistrering med deloperationer producerer 1 + N ressourcer:
hovedoperationen plus én pr. deloperation, forbundet med `partOf`.
Deloperationens identifier er `{objekt_id}-delop-{n}` i samme
identifier-system som hovedproceduren. Antal Procedure-rækker overstiger
derfor antal kilderækker. Tillægskoder er **klyngekorrekte**: en sideangivelse
på en deloperation lander på deloperationen — ikke på hovedoperationen.

**8. Tillægskoder er komplette.** `Procedure.code.coding` (og dermed
`fhir.Procedure_tillaegskode`) indeholder nu også: (a) XML-tillægskoderne
(alle SKS-familier — også operations-, undersøgelses-, administrative og
ATC-koder), og (b) de beregnede talværdi-tillægskoder VPH/VNH/VPK/VNK
(ca. 24 mio. registreringer bærer dem). Første coding er hovedkoden
(`userSelected = true`); resten er tillægskoder.

**Desuden:** knivtid (start, slut og varighed i minutter) er kolonner på
`fhir.Procedure`. Operationspersonale med rolle ligger som `performer` på
hovedproceduren og er **ikke** projiceret.

## Ikke medtaget (bevidst)

- **Epikrisernes operationskoder** — fravalgt (2026-08-18): de er en
  gengivelse af operationer, der allerede er registreret, og ville
  dobbelttælle.
- **Aflyste operationer** — fravalgt (2026-08-18): aflysninger dækkes af
  booking-sporet (FHIR Appointment med status cancelled/noshow og
  aflysningsårsag fra BOM-bookinghistorikken).
- **Side/kontrast/personalekategori som koder** — føres som strukturerede
  felter (anatomisk placering + kilde-metadata), ikke i kodelisten. Samme
  valg som det gamle DWH: værdierne var tillægskoder FØR LPR3, indberettes
  efter LPR3 som særlige felter og blev aldrig konverteret. Evt. konvertering
  er en klinisk beslutning og numerisk marginal (promiller).

## Hvad der ikke er query-bart i de flade tabeller

Følgende findes i FHIR-ressourcen (Aidbox JSONB), men **ikke** som kolonner i
`fhir.Procedure` / `fhir.Procedure_tillaegskode`:

- operationspersonale / `performer` (rolle + navn)
- udførende organisation (SOR på performer)
- ElementNummer-rækkefølge blandt tillægskoder
- AdminProc-/RIS-kildenøgle: kolonnen `ifdb_aktivitet_id` læser kun
  identifier-systemet `urn:source:ifdb:aktivitetsresultat_procedure`

Knivtid, LPR-flaget, kildenøglen og pegepinden fra deloperation til
hovedoperation (`part_of_id`) **er** kolonner — rækker fra før kolonnerne er
tomme, indtil genkørslen.

Kontakt vs. forløb: `encounter_id ≠ NULL` = kontakt, `episode_of_care_id ≠ NULL`
= forløb, begge NULL = LPR-fejl *eller* ureløselig reference (uskelneligt).
Begge felter kan være sat samtidig; referencens "kontakt vinder"-regel skal
aftager selv anvende.

## Kendte begrænsninger

Reduceres antallet af deloperationer eller
RIS-services i en registrering, bliver de overskydende ressourcer nu ryddet op:
børneressourcer afledt af forælderens XML (deloperationer, RIS-services,
SFI-observationer, XML-diagnoser) markeres som fejlregistrering med sletteflag,
når de ikke længere står i XML'en. Der slettes ikke fysisk. Tre forhold skal
aftagere kende:

1. **Oprydningen sker kun for de forældre, en kørsel faktisk henter.** Ældre
   forældreløse rækker kan derfor findes, indtil deres forælder opdateres i
   kildesystemet — og en måneds- eller fuldkørsel kan markere mange historiske
   børn på én gang. Det er forventet oprydning, ikke en fejl.
2. **Efterslæb:** rækker skrevet før oprydningen fandtes står som aktive,
   indtil deres forælder ændres. Det lukker ikke af sig selv.
3. **Forsvinder hele kilderækken** (ikke blot en klynge i XML'en), er den ikke
   dækket: overvågningen for forsvundne kilderækker omfatter i dag kun patient,
   kontakt og forløb — ikke procedurefamilien.

**Procedurer skrevet før hovedkode-markøren blev indført** kan mangle den. På
urørte gamle ressourcer kan hovedkode-kolonnen derfor være tom, og hovedkoden
ligge i tillægskode-tabellen — lukkes af genkørslen.

Talværdi-tillægskoder VPK/VNK: grænserækkerne `±999,99` giver `None` (ingen
kode). Kravspec-PDF'ens facit er trunkering til `VPK999K9` / `VNK999K9`.
Heltalsgrenen klampes (`99999` → `VPH9999`).

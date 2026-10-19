<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# IFDB: tabeller, kolonner og joins

Denne vejledning beskriver den aktuelle kodekontrakt. Tilgængelighed i et bestemt
miljø kræver de tilhørende migrationer, views og indlæsninger. Historiske dev-målinger
er ikke en garanti for, at QA er opdateret.

`vw_fhir` er aftagerfladen på DSO. De tilsvarende fysiske tabeller ligger i `fhir`.
Brug `LEFT JOIN` til valgfrie relationer, så en manglende reference ikke skjuler en
procedure eller henvisning. Brug `dw_is_deleted = 0`, når kun levende rækker ønskes;
på valgfrie joinmål placeres filteret i `ON`.

## Procedurer og organisationer

| Oplysning | Tabel og kolonne / join |
| --- | --- |
| Udførende organisation | `vw_fhir.Procedure.performer_organization_id` → `vw_fhir.Organization.organization_id` |
| Stamafdeling på RIS-procedure | `vw_fhir.Procedure.stamafdeling_organization_id` → `vw_fhir.Organization.organization_id` |
| Organisationsnavn, SOR-kode, type, overordnet organisation | `Organization.name`, `identifier_value_sor`, `type_code`/`type_display`, `part_of_id` |
| Registreret medarbejder og rolle | `Procedure.practitioner_id`, `practitioner_display`, `practitioner_role_code`, `practitioner_role_display` |
| Kildens statustekst | `Procedure.status_text` |
| Oprettet/ændret i EPJ og ændret i IFDB | `Procedure.epji_created_at`, `epji_modified_at`, `ifdb_modified_at` |
| Kontrast og personalekategori | `Procedure.used_contrast_code`, `used_contrast_text`, `staff_category_code`, `staff_category_text` |
| Tillægskode og rækkefølge | `vw_fhir.Procedure_SupplementaryCode`, join på `procedure_id`, rækkefølge i `idx` |

Organisationens attributter kopieres ikke til Procedure. Kildens afdelingskoder,
orgkoder, navne og SOR-koder bevares i IFDB-arkivet og FHIR-extensions til sporbarhed,
men projiceres ikke som parallelle Procedure-kolonner. Organization stiller aktuelt
SOR-koden til rådighed; kildens øvrige org-/afdelingskoder kan ikke loves genskabt
med dette join. Manglende organisationsreference er en mangel, der skal afstemmes;
den erstattes ikke af et kopieret navn.

```sql
SELECT p.procedure_id,
       udfoerende.name AS udfoerende_navn,
       udfoerende.identifier_value_sor AS udfoerende_sor,
       stam.name AS stamafdeling_navn,
       stam.identifier_value_sor AS stamafdeling_sor
FROM vw_fhir.[Procedure] AS p
LEFT JOIN vw_fhir.Organization AS udfoerende
  ON udfoerende.organization_id = p.performer_organization_id
 AND udfoerende.dw_is_deleted = 0
LEFT JOIN vw_fhir.Organization AS stam
  ON stam.organization_id = p.stamafdeling_organization_id
 AND stam.dw_is_deleted = 0
WHERE p.dw_is_deleted = 0;
```

Navnene i SELECT er kun visningsaliaser for joinresultatet, ikke ekstra lagrede
kolonner. `Organization.part_of_id` kan tilsvarende joines til Organization igen.

## Henvisninger og diagnoser

Henvisninger findes i `vw_fhir.ServiceRequest`. Afgræns PAS-henvisninger med
`identifier_system = 'urn:source:ifdb:henvisning'`, fordi tabellen også har andre kilder.

| Oplysning | ServiceRequest-kolonne |
| --- | --- |
| FHIR-status og kildens status | `status`, `status_kode`, `status_kodetekst` |
| Kildens prioritet | `prioritet_kode`, `prioritet_kodetekst` |
| Henvisningsmåde og formål | `henvisningsmaade_kode`, `henvisningsmaade_kodetekst`, `referral_purpose` |
| Modtagelse, afsendelse og administrativ lukning | `henvisningsdato`, `modtaget`, `afsendelsesdato`, `slutdato` |
| Afslutningsårsag | `afslutningsaarsag_kode`, `afslutningsaarsag_kodetekst` |
| Henviser og modtager som organisationsreferencer | `requester_id`, `performer_id` → `Organization.organization_id` |
| Henvisningens direkte diagnosereference | `reason_reference_id` → `Condition.condition_id` |

Henviserreferencen er byggerens valgte requester; den må ikke automatisk fortolkes
som alle kildens afsender-, henvist-fra- og behandlerroller på én gang.

Diagnoseoplysninger ligger i `vw_fhir.Condition`: `code`, `code_system`,
`code_display`, `diagnosetype`, `clinical_status` og `verification_status`.
`ServiceRequest.code` er bestillingens kode, ikke en kopi af diagnosen.

`vw_fhir.Condition_henvisning` er en koblingstabel, ikke en ekstra diagnosetabel:

- `condition_id` → `Condition.condition_id` giver diagnoseoplysningerne.
- `henvisning_id` er kildens henvisnings-id. Det joines til
  `ServiceRequest.identifier_value` med det korrekte `identifier_system`.
- `henvisning_id` må **ikke** joines til `ServiceRequest.service_request_id`.

En diagnose kan have flere henvisninger. Joinet kan derfor give flere rækker pr.
diagnose; optælling af unikke diagnoser skal ske på `condition_id`.

```sql
SELECT c.condition_id, c.code, c.code_display, c.diagnosetype,
       sr.service_request_id
FROM vw_fhir.Condition_henvisning AS ch
JOIN vw_fhir.Condition AS c
  ON c.condition_id = ch.condition_id
 AND c.source_system = N'urn:source:ifdb:pas_diagnose'
 AND c.dw_is_deleted = 0
LEFT JOIN vw_fhir.ServiceRequest AS sr
  ON sr.identifier_value = ch.henvisning_id
 AND sr.identifier_system = N'urn:source:ifdb:henvisning'
 AND sr.dw_is_deleted = 0
WHERE ch.dw_is_deleted = 0;
```

## SFI-skemaer og svar fra aktivitetsresultater

Her er der tale om SFI fra `IFDB.AKTIVITETSRESULTAT`, ikke henvisningsskemaer.
Registreringernes XML leverer svarene; SFI-modeloplysningerne beskriver skema og felter.
Routingen kan danne Observation eller QuestionnaireResponse. `fhir.SFI_datapoint`
samler datapunkter fra begge spor og er fortsat den fælles indgang til SFI-data.

De underliggende tabeller kan anvendes, når skemadefinition og konkrete svar skal
adskilles:

| Indhold | Tabel og centrale kolonner |
| --- | --- |
| Skemadefinition | `vw_fhir.Questionnaire`: `questionnaire_id`, `url`, `name`, `version` |
| Spørgsmål/felter | `vw_fhir.Questionnaire_item`: `questionnaire_id`, `link_id`, `text`, `item_type` |
| En skemaudfyldelse | `vw_fhir.QuestionnaireResponse`: `qr_id`, `source_aktivitet_id`, `questionnaire_canonical`, `questionnaire_version`, `source_model_name`, `source_model_version` |
| Svarværdier | `vw_fhir.QuestionnaireResponse_item`: `qr_id`, `link_id`, `value_string`, `value_int`, `value_dec`, `value_datetime`, `value_bool`, `value_coding_code` m.fl. |

Join svar til udfyldelsen på `qr_id`. Join udfyldelsen til den præcise skemaversion
med `QuestionnaireResponse.questionnaire_canonical = Questionnaire.url` **og**
`questionnaire_version = Questionnaire.version`. Join spørgsmål til skemadefinitionen
på `questionnaire_id`; et bestemt svarfelt forbindes desuden med `link_id` inden
for den pågældende skemaversion. Gentagne svar skal bevare deres eget grain.

`source_model_name` og `source_model_version` på besvarelsen udfyldes fra den
tilhørende Questionnaire. `Questionnaire_item.required` er fjernet, fordi den
nuværende kilde/bygger ikke leverer oplysningen; tomt må ikke fortolkes som valgfrit.

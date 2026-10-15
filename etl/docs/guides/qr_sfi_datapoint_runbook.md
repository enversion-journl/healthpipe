<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Runbook: QR → SFI join-key og datapunkt-kæde

Operationel runbook for QR → SFI-datapunkt-kæden (`fhir.QuestionnaireResponse`
→ `fhir.SFI_datapoint_build` → `fhir.SFI_datapoint_store`): DQ preflight,
reconciliation efter schema-/view-/key-ændringer, verifikationsqueries og
rollback. Designbaggrunden ligger i den arkiverede plan
[qr_sfi_join_key_hardening_plan.md](../archive/qr_sfi_join_key_hardening_plan.md)
(migration `20260608_qr_sfi_join_key_hardening.sql` + preflight i
`refresh_sfi_datapoint.py`); dette dokument er det autoritative sted for
driftsprocedurerne.

## DQ preflight (`refresh_sfi_datapoint.py`)

Preflighten måler joinbarheden i QR → SFI-kæden før delete/insert og styres af
env-varen `SFI_DATAPOINT_QR_PREFLIGHT_MODE`:

| Mode | Effekt |
|------|--------|
| `skip` (default) | Ingen preflight. Preflighten kostede ~5,5 min pr. kørsel, og build-SQL'ens INNER JOIN forhindrer i forvejen, at ugyldige rækker skrives. |
| `warn` | Kør preflight, emit DQ-metadata og warnings, men fortsæt. Diagnoseindstilling. |
| `fail` | Kør preflight og stop før delete/insert ved brud (fail-fast, fx efter Questionnaire-reload). |

**Bemærk:** `skip` opdager IKKE silent under-emission fra en ufuldstændig
`Questionnaire_item` — slå `warn` til ved mistanke.

I fail-mode stoppes run ved:

- `qr_missing_canonical > 0` for rækker med `questionnaire IS NOT NULL`.
- `qr_missing_version_when_reference_is_versioned > 0`.
- QR-referencer, der ikke findes i `fhir.Questionnaire`.

## Reconciliation efter schema-/view-/key-ændringer

Efter migration, view update og DQ preflight:

1. Kør infra/migrations.
2. Kør SOF extract/merge for Questionnaire og Questionnaire_item.
3. Kør SOF extract/merge for QuestionnaireResponse og
   QuestionnaireResponse_item.
4. Kør `dso_infra_views_fhir`.
5. Kør DQ preflight uden at skrive til store.
6. Kør `dso_reconcile_sfi_datapoint_store` for batchvis fuld reconciliation.
7. Kør daily incremental bagefter.
8. Sammenlign DQ metadata før/efter.
9. Hvis en diagnosekørsel midlertidigt har brugt warn-mode, fjern override igen,
   så default er aktiv.

Det er reconciliation, der reparerer historisk tomt/manglende QR-output. Daily
incremental må ikke forventes at rette alle historiske problemer alene.

## Verifikationsqueries

### 1. Persisted QR key health

```sql
SELECT
    COUNT(*) AS qr_total,
    SUM(CASE WHEN questionnaire IS NOT NULL THEN 1 ELSE 0 END) AS qr_with_questionnaire,
    SUM(CASE
        WHEN questionnaire IS NOT NULL AND questionnaire_canonical IS NULL THEN 1
        ELSE 0
    END) AS missing_canonical,
    SUM(CASE
        WHEN questionnaire IS NOT NULL
         AND CHARINDEX(N'|', questionnaire) > 0
         AND questionnaire_version IS NULL THEN 1
        ELSE 0
    END) AS missing_version_when_reference_is_versioned,
    SUM(CASE
        WHEN questionnaire IS NOT NULL
         AND CHARINDEX(N'|', questionnaire) = 0
         AND questionnaire_version IS NULL THEN 1
        ELSE 0
    END) AS valid_unversioned_references
FROM fhir.QuestionnaireResponse;
```

### 2. QR references uden matching Questionnaire

```sql
WITH qr_ref AS (
    SELECT DISTINCT
        questionnaire_canonical,
        questionnaire_version
    FROM fhir.QuestionnaireResponse
    WHERE questionnaire_canonical IS NOT NULL
)
SELECT COUNT(*) AS qr_references_without_questionnaire
FROM qr_ref AS qr
WHERE NOT EXISTS (
    SELECT 1
    FROM fhir.Questionnaire AS q
    WHERE q.url = qr.questionnaire_canonical
      AND (
          q.version = qr.questionnaire_version
          OR (q.version IS NULL AND qr.questionnaire_version IS NULL)
      )
);
```

### 3. QR-items der kan joine til SFI Questionnaire_item

```sql
SELECT
    COUNT_BIG(*) AS qri_total,
    SUM(CASE WHEN qri.link_id IS NULL THEN 1 ELSE 0 END) AS qri_without_link_id,
    SUM(CASE WHEN qi.link_id IS NOT NULL THEN 1 ELSE 0 END) AS qri_joinable_to_questionnaire_item,
    SUM(CASE WHEN qi.element_system = N'urn:dk:ifdb:sfi' THEN 1 ELSE 0 END) AS qri_sfi_joinable
FROM fhir.QuestionnaireResponse_item AS qri
INNER JOIN fhir.QuestionnaireResponse AS qr
    ON qr.qr_id = qri.qr_id
LEFT JOIN fhir.Questionnaire_item AS qi
    ON qi.questionnaire_url = qr.questionnaire_canonical
   AND (
       qi.questionnaire_version = qr.questionnaire_version
       OR (qi.questionnaire_version IS NULL AND qr.questionnaire_version IS NULL)
   )
   AND qi.link_id = qri.link_id;
```

### 4. SFI build/store QR-output

```sql
SELECT
    N'build' AS source,
    COUNT_BIG(*) AS rows,
    COUNT(DISTINCT source_resource_id) AS qr_resources
FROM fhir.SFI_datapoint_build
WHERE source_resource = N'QuestionnaireResponse'
UNION ALL
SELECT
    N'store' AS source,
    COUNT_BIG(*) AS rows,
    COUNT(DISTINCT source_resource_id) AS qr_resources
FROM fhir.SFI_datapoint_store
WHERE source_resource = N'QuestionnaireResponse';
```

### 5. SFI-relevante QR-items uden output

```sql
WITH sfi_qri AS (
    SELECT DISTINCT
        qri.qr_id,
        qri.link_id,
        qri.item_key,
        qri.answer_index
    FROM fhir.QuestionnaireResponse_item AS qri
    INNER JOIN fhir.QuestionnaireResponse AS qr
        ON qr.qr_id = qri.qr_id
    INNER JOIN fhir.Questionnaire_item AS qi
        ON qi.questionnaire_url = qr.questionnaire_canonical
       AND (
           qi.questionnaire_version = qr.questionnaire_version
           OR (qi.questionnaire_version IS NULL AND qr.questionnaire_version IS NULL)
       )
       AND qi.link_id = qri.link_id
       AND qi.element_system = N'urn:dk:ifdb:sfi'
),
build_qri AS (
    SELECT DISTINCT
        source_resource_id AS qr_id,
        source_link_id AS link_id,
        source_item_key AS item_key,
        source_answer_index AS answer_index
    FROM fhir.SFI_datapoint_build
    WHERE source_resource = N'QuestionnaireResponse'
)
SELECT COUNT(*) AS sfi_relevant_qri_without_build_output
FROM sfi_qri AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM build_qri AS b
    WHERE b.qr_id = s.qr_id
      AND b.link_id = s.link_id
      AND ISNULL(b.item_key, N'') = ISNULL(s.item_key, N'')
      AND ISNULL(b.answer_index, N'') = ISNULL(s.answer_index, N'')
);
```

## Forventede resultater (acceptkriterier)

- `missing_canonical = 0` for QR-rækker med `questionnaire IS NOT NULL`.
- `missing_version_when_reference_is_versioned = 0`.
- Uversionerede questionnaires accepteres som gyldige, når Questionnaire-siden
  også er uversioneret.
- `qr_references_without_questionnaire = 0` for SFI-relevante QR-referencer.
- `sfi_relevant_qri_without_build_output = 0`, medmindre der findes en
  dokumenteret og accepteret datakvalitetsundtagelse.
- `SFI_datapoint_build` og `SFI_datapoint_store` har QR-output i samme
  størrelsesorden efter reconciliation.
- Daily incremental ændrer kun changed resources, men kan både tilføje,
  opdatere og fjerne QR-datapunkter korrekt.
- Dagster metadata viser candidate counts, joinable counts, output counts og
  fallback usage.

## Rollback og sikker drift

Rollback må ikke slette fallbacken eller gøre output dårligere.

Hvis ny persistering eller DQ gate giver uventet effekt:

- behold fallback i `SFI_datapoint_build`.
- slå DQ gate midlertidigt over i warn-only, hvis drift skal fortsætte.
- revert kun den konkrete persisterings-/preflight-ændring.
- kør reconciliation igen, når fejlen er rettet.
- sammenlign build/store QR-output før og efter.

## Backfill-vinduer

Storen er en clustered columnstore, månedspartitioneret på `occurred_at_dt`, med to
smalle B-træer (datapoint-nøgle og patient). Store vinduer (mindst 1 mio. ændrede
ressourcer) indlæses via arbejdstabellen `fhir.SFI_datapoint_store_backfill` og skiftes ind
med SWITCH (partitioner med højst 100.000 efterslæbere tømmes til
`fhir.SFI_datapoint_store_efterslaeb` først og får rækkerne tilbage bagefter); genkørsler af
allerede indlæste måneder bruger indeks-frakobling. Hver kørsel
slutter med REORGANIZE af columnstoren. Det daglige delta kører uændret. Run-tags til
målekørsler:

| Run-tag | Værdier |
|---|---|
| `sfi_datapoint/backfill_index_mode` | `auto` (default), `always`, `never` |
| `sfi_datapoint/backfill_min_resources` | heltal, default 1.000.000 |
| `sfi_datapoint/backfill_strategy` | `auto` (default), `switch`, `disable_indexes`, `none` |
| `sof/backfill_mode`, `sof/merge_batch_rows`, `sof/index_disable` | SOF-MERGE'n: `auto`/`always`/`never`, portionsstørrelse (0 = ét statement), `on`/`off` |
| `dso_sync/index_disable_min_rows`, `dso_sync/index_disable_min_ratio` | DSO-synkens indeks-frakobling |

Status: `dso_sfi_store_partition_status`. Målebænke: `sfi_store_layout_benchmark_job` og
`sof_merge_benchmark_job` (kopier, der droppes igen).

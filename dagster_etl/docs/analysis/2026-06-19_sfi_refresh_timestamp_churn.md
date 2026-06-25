<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# SFI Refresh: Timestamp Churn

**Dato**: 2026-06-19
**Run**: `4350edd8`
**Asset**: `dso_refresh_sfi_datapoint_daily`
**Watermark**: `2026-06-18T15:03:32`

## Konklusion

SFI-refresh slettede/indsatte mange rækker, fordi upstream staging/DSO markerede gamle `QuestionnaireResponse`-ressourcer som ændrede via tekniske update-timestamps.

SFI er downstream-effekten: den reagerer korrekt på et oppustet changed-set. Rodårsagen er timestamp churn fra MERGE/upsert uden payload-diff-check.

## Verificeret Evidens

Changed resources i SFI:

| Kilde | Antal |
|---|---:|
| `Observation` SFI | 81.907 |
| `QuestionnaireResponse` | 560.758 |
| **Total** | **642.665** |

`QuestionnaireResponse`:

| Type | Antal |
|---|---:|
| Inserts efter watermark | 115.981 |
| Updates efter watermark | 444.777 |
| **Total** | **560.758** |

Update-bølger:

| Tidspunkt | Rows |
|---|---:|
| `2026-06-19 06:20` | 277.185 |
| `2026-06-19 07:23` | 167.592 |
| **Total** | **444.777** |

Verificeret kæde for `07:23`-bølgen:

| Led | Værdi |
|---|---|
| Aidbox/SoF `last_updated` | `2026-06-18 04:18-04:24` |
| Staging `DW_Timestamp_Update` | `2026-06-19 07:23` |
| DSO `DW_Timestamp_Update` | `2026-06-19 07:23:04` |
| Rows | ca. 167.592 |

## Årsagskæde

```text
SQL-on-FHIR/staging touch
-> STOP HER: staging må kun flytte DW_Timestamp_Update ved reel payloadændring
-> DSO sync læser reelt ændrede rows via DW_Timestamp_Update
-> DSO MERGE toucher target rows
-> SFI ser DW_Timestamp_Effective som changed
-> SFI delete/insert rebuild pr. changed resource
```

## Kodeområder

| Område | Fil | Problem |
|---|---|---|
| SFI changed-set | `dagster_etl/DSO/load/refresh_sfi_datapoint.py:156` | Bruger `DW_Timestamp_Effective` fra `Observation`, `QuestionnaireResponse`, `QuestionnaireResponse_item` |
| SFI apply | `dagster_etl/DSO/load/refresh_sfi_datapoint.py:630` | Deletes/inserts alle datapoints for hver changed resource |
| SQL-on-FHIR extract | `dagster_etl/DSA/FHIR/utils/sql_on_fhir/sof_utils.py:460` | Bruger Aidbox `r.ts` som `last_updated` / delta-signal |
| SQL-on-FHIR MERGE | `dagster_etl/DSA/FHIR/utils/sql_on_fhir/sof_utils.py:1007` | `WHEN MATCHED THEN UPDATE` uden diff-check |
| DSO delta MERGE | `dagster_etl/DSO/load/sync_utils.py:973` | Delta filter bruger `DW_Timestamp_Update` |
| DSO update | `dagster_etl/DSO/load/sync_utils.py:982` | `WHEN MATCHED THEN UPDATE` sætter nyt `DW_Timestamp_Update` uden diff-check |
| Aidbox upsert | `dagster_etl/core/fhir/direct_upsert.py:607` | `ON CONFLICT DO UPDATE` sætter `ts = CURRENT_TIMESTAMP` uden diff-check |

## Implementeret Fix

Der er lavet én primær ændring: SQL-on-FHIR MERGE er gjort diff-aware i `dagster_etl/DSA/FHIR/utils/sql_on_fhir/sof_utils.py`.

Det betyder: `DW_Timestamp_Update` i staging må kun flyttes, hvis forretningskolonnerne faktisk ændrer sig. Derefter kan DSO og SFI fortsat stole på downstream timestamps.

Diff-aware betyder: opdater kun matched rows, hvis forretningskolonnerne faktisk er ændret. Audit-kolonner som `DW_Timestamp_Insert`, `DW_Timestamp_Update` og tekniske timestamps må ikke indgå i diff-grundlaget.

For SQL Server MERGE bør det implementeres som `WHEN MATCHED AND <payload differs> THEN UPDATE`, helst via row-hash over forretningskolonner.

## Ikke Først

Ændr ikke SFI delete/insert-strategien først. Ændr heller ikke DSO/Aidbox først for denne incident. Den verificerede fejl starter ved staging `DW_Timestamp_Update`, og den mest præcise fix er derfor at stoppe no-op timestamp churn lige efter SQL-on-FHIR.

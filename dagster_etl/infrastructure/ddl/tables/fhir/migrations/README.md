<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# FHIR Schema Migrations

Inkrementelle skemaaendringer til `[fhir].*` tabeller i MSSQL.

Foelger etableret moenster fra `erp/migrations/` og `organization/migrations/`.

## Konventioner

- En fil per aendring: `NN_beskrivelse.sql`
- Nummerering er sekventiel (01, 02, 03, ...)
- Alle udsagn SKAL vaere **idempotente** (`IF NOT EXISTS`, `IF COL_LENGTH`, etc.)
- Baseline DDL i `01_tables.sql` opdateres OGSAA til at afspejle den nye desired state
- Filen refereres i PR-beskrivelse og work item

## Eksempler

```sql
-- 01_add_procedure_org_performer.sql
IF COL_LENGTH('[fhir].[Procedure]', 'org_performer_sor') IS NULL
BEGIN
    ALTER TABLE [fhir].[Procedure]
        ADD [org_performer_sor] NVARCHAR(255) NULL;
END
GO
```

## Koesel

Migrations koeres automatisk af `staging_infra_fhir` Dagster-asset
efter baseline-DDL (`01_tables.sql`).

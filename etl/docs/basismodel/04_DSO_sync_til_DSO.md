<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# 04 — DSO: synkronisering til DSO

DSO (Data Staging Out) er sidste lag i basismodellen. Opgaven er at flytte de
færdige tabeller fra staging-databasen til rapporteringsdatabasen og udstille dem
gennem views, som `datamarter`-repoet og rapportværktøjerne læser.

DSO **ændrer ikke data**. Hører en beregning til et sted, er det i DSA.

Forudsætter et af 03-kapitlerne.

## Hvorfor to databaser?

Staging-databasen er et arbejdsområde: den bliver skrevet til hele natten, den
indeholder mellemresultater, arkiver og personhenførbare nøgler, og den er ikke
noget, aftagerne skal kunne se. Rapporteringsdatabasen er læsemiljøet: kun de
færdige tabeller, med adgangsstyring på.

Adskillelsen er derfor både en performancegrænse og en sikkerhedsgrænse.

## Sådan flyttes data

```
Staging-database (DSA)                    Rapporteringsdatabase (DSO/DPA)
  map.*                                     map.*
  dim.*      ──── STAGING_LINK (MERGE) ───▶  dim.*
  fct.*                                     fct.*
  dm.*                                      dm.*
  fhir.*                                    fhir.*
                                                │
                                                ▼
                                          vw_dim.* / vw_fct.* / vw_dm.*
                                          (præsentation + sikkerhed)
```

**Den foretrukne vej er linked server.** MERGE'en eksekveres helt på DSO-serveren
via 4-delt navngivning (`[STAGING_LINK].[DW].[skema].[tabel]`), så ingen data
passerer gennem Python.

**Python-fallback** findes som nødløsning: hvis linked serveren ikke er
tilgængelig, streames rækkerne gennem Python i chunks. Den bruger altid TRUNCATE +
INSERT uanset konfigureret mode. Fallback logges på **ERROR**-niveau med præfikset
`DSO_SYNC_FALLBACK_ACTIVE`, og asset-metadata sætter `sync_engine=python_fallback`
plus `fallback_reason`.

## Tre sync-modes

| Mode | SQL | Brug til |
|---|---|---|
| `merge` | Fuld MERGE af alle rækker | Map-tabeller og små lookup-tabeller, hvor en fuld scanning er billig |
| `watermark` | Delta-MERGE filtreret på et timestamp | Dimensioner og facts, der vokser over tid |
| `truncate_insert` | TRUNCATE + INSERT | Type 2-dimensioner med IDENTITY-nøgle, og sikkerhedstabeller der altid fuldopdateres |

Rækkefølgen er bundet i Dagster: **map → dim → fct**, hvor map findes. Hvert
sync-asset afhænger af sit upstream staging-asset.

## Watermark-reglerne

En delta-synkronisering kan tabe rækker uden at fejle. Reglerne nedenfor er derfor
eksplicitte, og de er implementeret i `build_delta_merge_statement` og `DfmManager`
(`scope='dso'`).

**Filterudtrykket:**
`COALESCE([watermark_kolonne], [dw_timestamp_insert]) > checkpoint`

- `COALESCE` sikrer, at rækker uden `dw_timestamp_update` alligevel kommer med via
  indsættelsestidspunktet.
- Grænsen er **eksklusiv (`>`)**. Rækker med timestamp præcis lig checkpointet blev
  hentet sidste gang. Var grænsen inklusiv, ville hele det sidste sekund blive
  gen-MERGE'et hver nat — og fordi MERGE'en på DSO stempler
  `dw_timestamp_update = SYSDATETIME()`, ville det aldrig stoppe.

**Checkpointet:**

- Før MERGE'en måles `max_now = MAX(COALESCE(watermark, insert))` på staging.
  Al læsning er derefter afgrænset til `> checkpoint AND <= max_now`, så rækker, der
  indsættes midt i synkroniseringen, ikke kan flytte checkpointet forbi noget ulæst.
- Efter en succesfuld kørsel med rækker sættes checkpointet til det **forudmålte**
  `max_now` — aldrig til et `MAX` målt efter MERGE'en, og aldrig til DSO's
  `SYSDATETIME()`.
- **Nul-række-guard:** checkpointet flyttes ikke, når `rows_affected == 0`.

**Automatisk genopretning:** efter hver delta-kørsel sammenlignes `COUNT(*)` på
staging og DSO. Er der **færre** rækker på DSO, kører en fuld MERGE som
genopretning. Er der **flere** på DSO (rester eller forældreløse rækker), logges en
advarsel, og der genoprettes ikke — MERGE sletter ikke den slags.

**Hvor tilstanden ligger:** `[meta].[DFMState]` på **staging** med `scope='dso'`.
Historikken ligger i `[meta].[DFMRunLog]` med samme scope.

```sql
-- se DSO-sync-checkpoints
SELECT schema_name, table_name, checkpoint_value, last_mode, updated_at
FROM [meta].[DFMState] WHERE scope = 'dso' ORDER BY schema_name, table_name;
```

Reset til en fuld re-sync sker med `meta.usp_dfm_reset` med
`@scope = 'dso'` — se
[dfm_operations_guide.md §10](../implementation/dfm_operations_guide.md).

> `[meta].[SyncWatermarks]` på DSO er **udgået** for tabel-sync. Den bruges kun
> stadig til refresh af SFI-datapunkter. Brug ikke den tabel til at resette en
> tabel-sync.

## Sådan tilføjer du en tabel

### 1. Konfigurationen

`DSO/load/sync_configs.py` er **kilden til sandhed** for, hvilke tabeller der
synkroniseres. Tilføj en `SyncTableConfig`:

```python
"dim_nytabel": SyncTableConfig(
    schema="dim",
    table="NyTabel",
    primary_key="dw_ek_NyTabel",
    mode="watermark",
    watermark_column="dw_timestamp_update",
),
```

Udvalgte felter — den fulde reference står i `DSO/load/sync_utils.py`:

| Felt | Betydning |
|---|---|
| `primary_key` | Kolonne(r) i MERGE'ens ON-udtryk |
| `mode` | `merge`, `watermark` eller `truncate_insert` |
| `watermark_column` | Krævet ved `mode="watermark"`. Typisk `dw_timestamp_update`; `dw_timestamp_insert` for append-only tabeller |
| `exclude_columns` | Kolonner der ikke synkroniseres, fx IDENTITY-kolonner på map-tabeller |
| `insert_only_columns` | Kolonner der kun sættes ved INSERT (standard: `dw_timestamp_insert`) |
| `identity_insert` | Slå `SET IDENTITY_INSERT ON` til, når primærnøglen er IDENTITY |
| `chunk_size` | Rækker per chunk i Python-fallback (standard 5000) |

### 2. Assettet

Tilføj det i `DSO/load/sync_dim.py`, `sync_fct.py` eller `sync_fhir.py` afhængigt af
lag. Kopiér et naboasset — mønstret er altid: hent konfigurationen, kald
`sync_table_via_linked_server(...)`, kald `compare_row_counts(...)`, og returnér
begge dele som metadata.

Navngivning: `dso_sync_{skema}_{tabel}`, group `{domæne}_dso_sync`.
Afhængigheder: det upstream staging-asset, plus map-sync'en hvis domænet
synkroniserer map.

### 3. Registrér

Læg assettet i den relevante `all_*_dso_sync_assets`-liste i samme fil, og eksportér
fra `DSO/load/__init__.py`.

### 4. DDL på DSO

Måltabellen skal findes på DSO med **samme kolonner** som på staging.
Schema validation kører automatisk (`validate_schemas=True`) og fejler ved forskelle.
Tilføj DDL'en under `infrastructure/ddl/tables/{domæne}/` og materialisér det
tilhørende `dso_infra_*`-asset.

### 5. Viewet

Views ligger som DDL under `infrastructure/ddl/views/{domæne}/` og bruger
`CREATE OR ALTER VIEW`.

| Viewtype | Navngivning |
|---|---|
| Dimensionsview | `vw_dim_<Tabel>` |
| Factview | `vw_fct_<Tabel>` |
| dm-view | `vw_dm.<Tabel>` (eget skema) |

View-laget har fire opgaver:

1. **Slå `dw_sk` op** for type 2-dimensioner ved hjælp af `dw_haendelsestidspunkt`
   på fact-rækken.
2. **Slå komplekse relationer op**, der bevidst blev efterladt som CleanCodes i DSA
   (fx fact-til-fact-relationer, der kan ændre sig over tid).
3. **Filtrere.** `dw_is_deleted = 0` og gyldighedsfiltre. Den sikre læsning skal
   være standarden.
4. **Adgangsstyring.** Se [sikkerhedsmodel.md](../implementation/sikkerhedsmodel.md):
   roller og rækkebaseret sikkerhed lægges på i view-laget, drevet af AD-grupper og
   tabellerne `sec.RolleDomaene` / `sec.RBOrganisation`.

## Verificér

Hvert sync-asset returnerer metadata, du kan læse direkte i Dagster:

| Nøgle | Hvad du bruger den til |
|---|---|
| `sync_engine` | Skal være `linked_server` i normal drift. `python_fallback` er en alarm. |
| `sync_mode` | Skelner mellem første kørsel (`watermark_full`) og delta (`watermark_delta`) |
| `rows_affected` | Antal rækker indsat eller opdateret |
| `staging_count` / `dso_count` / `counts_match` | Rækkeparitet efter synkroniseringen |
| `sync_duration_seconds` / `rows_per_second` | Gennemløb — Dagster plotter dem som tidsserier |

Konkret at tjekke ved en ny tabel:

1. **Første kørsel** giver `watermark_full` og fuldt rækketal.
2. **Anden kørsel** giver `watermark_delta` med 0 eller få rækker, og checkpointet er
   flyttet.
3. `counts_match` er `true`.
4. **Viewet** returnerer det forventede — og filtrerer slettede rækker.
5. DQA-tjekket `dso_reconciliation` er konfigureret for tabellen. Det afstemmer DSA
   mod DSO løbende og er den langsigtede sikring.

## Faldgruber

| Symptom | Årsag og løsning |
|---|---|
| `Linked server 'STAGING_LINK' does not exist` | Kør infrastruktur-assettet `dso_infra_linked_server`. Tjek med `SELECT * FROM sys.servers WHERE name = 'STAGING_LINK'`. |
| `SchemaValidationError: Column mismatch` | Staging-skemaet er ændret. Opdatér DSO-tabellens DDL, kør `dso_infra_*`, kør synkroniseringen igen. DSO må aldrig ændre DSA's skema. |
| `sync_engine=python_fallback` | Linked serveren var utilgængelig. Undersøg netværk og credentials — behandl det som en hændelse, ikke som normal drift. |
| `counts_match: false` med lille forskel | Rækker indsat mellem MERGE og optælling. Løser sig selv ved næste kørsel. |
| `counts_match: false` med stor forskel | Enten rester på DSO (MERGE sletter dem ikke) eller en fejlet fallback-overførsel. Overvej at resette checkpointet og køre en fuld re-sync. |
| Første watermark-kørsel MERGE'er alt | Forventet: `scope='dso'`-checkpointet var tomt. |

## Videre herfra

[05 — Udfyld dokumentation](05_udfyld_dokumentation.md). En DSO-synkroniseret tabel
**skal** have lineage-dokumentation, før merge requesten kan lukkes.

Uddybende (endnu ikke oversat):
[dso_sync_guide.md](../implementation/dso_sync_guide.md) — den engelske
originaludgave med performancetuning og fuld feltreference;
[dso_delta_load_strategy.md](../architecture/dso_delta_load_strategy.md) —
beslutningen om delta-strategi og de alternativer, der blev forkastet.

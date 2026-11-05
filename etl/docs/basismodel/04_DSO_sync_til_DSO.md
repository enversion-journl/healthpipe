<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# 04 — DSO: synkronisering til DSO

DSO (Data Staging Out) er sidste lag i basismodellen. Opgaven er at flytte de
færdige tabeller fra staging-databasen til DSO-databasen på datamartserveren. Dér
er de til rådighed for datamart-teamet, som bygger datamarter oven på dem i
sikkerhedsdomænerne. Basismodellen laver selv et view oven på en tabel, når der er
en grund til det — se afsnit 5.

DSO **ændrer ikke data**. Hører en beregning til et sted, er det i DSA.

Forudsætter et af 03-kapitlerne.

## Hvorfor to databaser?

Staging-databasen er et arbejdsområde: den bliver skrevet til hele natten, den
indeholder mellemresultater, arkiver og personhenførbare nøgler, og den er ikke
noget, aftagerne skal kunne se. DSO-databasen er det, der bygges videre på: kun
de færdige tabeller, med adgangsstyring på.

Adskillelsen er derfor både en performancegrænse og en sikkerhedsgrænse.

## Sådan flyttes data

```
Staging-database (DSA)                    DSO-database (datamartserveren)
  map.*
  dim.*      ──── STAGING_LINK (MERGE) ───▶  dim.*
  fct.*                                     fct.*
  dm.*                                      dm.*
  fhir.*                                    fhir.*
                                                │
                                                ▼
                                          vw_*  (kun når der er en grund)
```

**Synken går altid via linked server** — en forbindelse fra DSO-serveren direkte
til staging-databasen. MERGE'en eksekveres helt på DSO-serveren via 4-delt
navngivning (`[STAGING_LINK].[DW].[skema].[tabel]`), så ingen data
passerer gennem Python.

Findes linked serveren ikke, eller svarer den ikke, fejler synken med en fejl, der
nævner linked serveren og tabellen. Checkpointet flytter sig ikke, så den næste
kørsel efter reparationen indhenter rækkerne.

## Tre sync-modes

| Mode | SQL | Brug til |
|---|---|---|
| `merge` | Fuld MERGE af alle rækker | Små lookup-tabeller, hvor en fuld scanning er billig |
| `watermark` | Delta-MERGE filtreret på et timestamp | Dimensioner og facts, der vokser over tid |
| `truncate_insert` | TRUNCATE + INSERT | Type 2-dimensioner med IDENTITY-nøgle, og sikkerhedstabeller der altid fuldopdateres |

Rækkefølgen er bundet i Dagster: **dim → fct**. Hvert
sync-asset afhænger af sit upstream staging-asset.

**DSO stempler selv.** Alle tre modes sætter `dw_timestamp_insert` og
`dw_timestamp_update` til DSO's skrivetidspunkt i UTC (`SYSUTCDATETIME()`), ved både
INSERT og UPDATE. Stagings stempler kopieres ikke med. Et stempel på DSO angiver derfor,
hvornår DSO skrev rækken, og alle stempler går efter samme ur, som kun går fremad. En
række, der når DSO sent, får et sent stempel. Den, der læser ændringer på DSO, skal
derfor kun bruge DSO's egne stempler.

## Checkpoint-reglerne

Sync'en holder styr på, hvor langt den er nået, med et checkpoint — ligesom resten
af DFM. I koden hedder mode'en `watermark`, og kolonnen, der sammenlignes med
checkpointet, hedder `watermark_column`; begrebet er det samme.

En delta-synkronisering kan miste rækker, uden at der kommer en fejlbesked —
rækkerne mangler bare på DSO. Reglerne nedenfor er derfor eksplicitte, og de er
implementeret i `build_delta_merge_statement` og `DfmManager` (`scope='dso'`).

**Filterudtrykket:**
`COALESCE([watermark_kolonne], [dw_timestamp_insert]) > checkpoint`

- `COALESCE` sikrer, at rækker uden `dw_timestamp_update` alligevel kommer med via
  indsættelsestidspunktet.
- Grænsen er **eksklusiv (`>`)**. Rækker med timestamp præcis lig checkpointet blev
  hentet sidste gang. Var grænsen inklusiv, ville hele det sidste sekund blive
  gen-MERGE'et hver nat — og fordi MERGE'en på DSO stempler sin egen skrivetid i
  `dw_timestamp_update`, ville det aldrig stoppe.

**Checkpointet:**

- Før MERGE'en måles `max_now = MAX(COALESCE(watermark, insert))` på staging.
  Al læsning er derefter afgrænset til `> checkpoint AND <= max_now`, så rækker, der
  indsættes midt i synkroniseringen, ikke kan flytte checkpointet forbi noget ulæst.
- Efter en succesfuld kørsel med rækker sættes checkpointet til det **forudmålte**
  `max_now` — aldrig til et `MAX` målt efter MERGE'en, og aldrig til DSO's eget ur.
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

> Ingen sync eller refresh læser `[meta].[SyncWatermarks]` på DSO. SFI-datapunktstoren har sit
> checkpoint i `[meta].[DFMState]` med `scope='sfi_store'`
> (`fhir.SFI_datapoint_store`). Storen læser ændringer på DSO's egne stempler og
> holder checkpointet tilbage til starten af en DSO-sync af sine kilder, der stadig kører.

## Sådan tilføjer du en tabel

### 1. Konfigurationen

Det er i `DSO/load/sync_configs.py`, det bestemmes, hvilke tabeller der
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
| `exclude_columns` | Kolonner der ikke synkroniseres, fx IDENTITY-kolonner |
| `insert_only_columns` | Kolonner der kun sættes ved INSERT (standard: `dw_timestamp_insert`) |
| `identity_insert` | Slå `SET IDENTITY_INSERT ON` til, når primærnøglen er IDENTITY |

### 2. Assettet

Tilføj det i `DSO/load/sync_dim.py`, `sync_fct.py` eller `sync_fhir.py` afhængigt af
lag. Kopiér et naboasset — mønstret er altid: hent konfigurationen, kald
`sync_table_via_linked_server(...)`, kald `compare_row_counts(...)`, og returnér
begge dele som metadata.

Navngivningen står i
[navngivningsstandard_basismodel.md](../implementation/navngivningsstandard_basismodel.md#assets).
Afhængigheder: det upstream staging-asset. Map-tabellerne bliver på staging.

### 3. Registrér

Læg assettet i den relevante liste i samme fil. For en ny fil: importér dens liste i
`DSO/load/__init__.py`, og læg den i `all_dso_sync_assets`. Listerne i `sync_fhir.py`
importeres direkte i `definitions.py`.

### 4. DDL på DSO

Måltabellen skal findes på DSO med **samme kolonner** som på staging.
Schema validation kører automatisk (`validate_schemas=True`) og fejler ved forskelle.
Tilføj DDL'en under `infrastructure/ddl/tables/{domæne}/` og materialisér det
tilhørende `dso_infra_*`-asset. Har assettet en eksplicit filliste (fx DRG, HAIBA og
udredningsret), så tilføj din fil til listen. Et nyt domæne får altid en filliste — se
[infrastrukturguiden](../implementation/infrastructure_guide.md#4-staging-and-dso).

### 5. Viewet — kun når der er en grund

En DSO-tabel får ikke automatisk et view. Datamart-teamet bygger selv datamarter
oven på basismodellens tabeller, så basismodellen laver kun et view, når der er en
grund til det. Grundene er i dag:

| Grund | Eksempel |
|---|---|
| **Den sikre læsning skal være standarden.** Tabellen samler soft deleted rækker op, og den, der læser tabellen direkte, får dem med | `vw_dm.HAIBA_*` og `vw_fct.Lagerbeholdning` filtrerer `dw_is_deleted = 0` |
| **Et opslag skal laves for den, der læser tabellen** — fx en relation, der bevidst blev efterladt som CleanCode i DSA, eller en organisation, der ikke er gemt på fact'en | `vw_fct.*` i ERP slår SOR-organisationen op via `conceptmap.*` |
| **Persondata skal maskeres** | `vw_dim.Borger` er altid maskeret |

Gælder ingen af dem, laver du ikke et view. De pass-through-views, der findes i
dag (fx `vw_fhir.*`), bliver stående. Adgangsstyringen efter sikkerhedsmodellen
lægges på i DPA-views — se [sikkerhedsmodel.md](../implementation/sikkerhedsmodel.md).

Skal der et view til:

- DDL'en ligger under `infrastructure/ddl/views/{domæne}/` og bruger
  `CREATE OR ALTER VIEW`.
- Viewet hedder det samme som tabellen, i skemaet `vw_` plus tabellens skema:
  `vw_dm.HAIBA_CasesInfektion` ligger oven på `dm.HAIBA_CasesInfektion`.
- Det køres af domænets `dso_infra_views_{domæne}`-asset, som er registreret i
  `infrastructure/__init__.py`. Har domænet ikke et endnu, skal du lave det.
- Lineage-filen beskriver, hvad viewet filtrerer på, så den, der læser tabellen,
  kan se forskellen. Se [05 — Udfyld dokumentation](05_udfyld_dokumentation.md).

Uanset om der er et view eller ej, leverer basismodellen det, der skal til for at
læse tabellen rigtigt: `dw_ek` og `dw_haendelsestidspunkt` på facts (se
[03b](03b_DSA_arkiv_til_dimensionel_model.md#dw_sk--versionsnøglen)),
`dw_is_deleted` på tabeller, der samler slettede rækker op, og sikkerhedstabellerne
`sec.RolleDomaene` og `sec.RBOrganisation`, som indlæses fra AD og synkroniseres
til DSO.

## Verificér

Hvert sync-asset returnerer metadata, du kan læse direkte i Dagster:

| Nøgle | Hvad du bruger den til |
|---|---|
| `sync_engine` | Skal være `linked_server` i normal drift. |
| `sync_mode` | Skelner mellem første kørsel (`watermark_full`) og delta (`watermark_delta`) |
| `rows_affected` | Antal rækker indsat eller opdateret |
| `staging_count` / `dso_count` / `counts_match` | Rækkeparitet efter synkroniseringen |
| `sync_duration_seconds` / `rows_per_second` | Gennemløb — Dagster plotter dem som tidsserier |

Konkret at tjekke ved en ny tabel:

1. **Første kørsel** giver `watermark_full` og fuldt rækketal.
2. **Anden kørsel** giver `watermark_delta` med 0 eller få rækker, og checkpointet er
   flyttet.
3. `counts_match` er `true`.
4. **Tabellen på DSO** har de samme rækker som på staging — også de slettede
   (`dw_is_deleted = 1`). Har tabellen et view, filtrerer viewet dem fra.
5. DQA-tjekket `dso_reconciliation` er konfigureret for tabellen. Det afstemmer DSA
   mod DSO løbende og er den langsigtede sikring.

## Faldgruber

| Symptom | Årsag og løsning |
|---|---|
| `Linked server 'STAGING_LINK' does not exist` | Kør infrastruktur-assettet `dso_infra_linked_server`. Tjek med `SELECT * FROM sys.servers WHERE name = 'STAGING_LINK'`. |
| `SchemaValidationError: Column mismatch` | Staging-skemaet er ændret. Opdatér DSO-tabellens DDL, kør `dso_infra_*`, kør synkroniseringen igen. DSO må aldrig ændre DSA's skema. |
| `Cannot sync ...: linked server 'STAGING_LINK' is unavailable` | Linked serveren findes ikke eller svarer ikke. Undersøg netværk og credentials, og kør synken igen. |
| Synken fejler med `ROW_COUNT_MISMATCH` | DSO har færre rækker end staging: rækker indsat mellem MERGE og optælling. Kør synken igen. Fejler den igen, så reset checkpointet og kør en fuld re-sync. |
| `counts_match: false` med flere rækker på DSO | Rester på DSO (MERGE sletter dem ikke). Kørslen forbliver grøn. |
| Første kørsel med `mode="watermark"` MERGE'er alt | Forventet: `scope='dso'`-checkpointet var tomt. |

## Videre herfra

[05 — Udfyld dokumentation](05_udfyld_dokumentation.md). En DSO-synkroniseret tabel
**skal** have lineage-dokumentation, før merge requesten kan lukkes.

Uddybende (på engelsk):
[dso_sync_guide.md](../implementation/dso_sync_guide.md) — den engelske
originaludgave med performancetuning og fuld feltreference;
[dso_delta_load_strategy.md](../architecture/dso_delta_load_strategy.md) —
beslutningen om delta-strategi og de alternativer, der blev forkastet.

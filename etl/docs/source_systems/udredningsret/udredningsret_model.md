<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# SDS Udredningsret — målmodel, staging-regler og beslutninger (US-3 → US-6)

Feature [internal ticket] (SDSUDRED v1.0). Dette notat er den autoritative specifikation for lagene
efter arkivet: `pre_stg` → `stg` → `map` → `dim`/`fct` → DSO/DQA ([internal ticket], [internal ticket],
[internal ticket], [internal ticket]). Kilden og arkivet (US-2/US-7) er beskrevet i
[udredningsret_kilde.md](udredningsret_kilde.md); beslutningerne B1–B10 står på featuren og på
review-siden "Udredningsret i basismodellen".

Alle tal er målt på healthpipe-dev 2026-09-12 mod `arc.udredningsret_mont` med U-leverancen 2026-09-07
(8.169.899 rækker, 2.233.064 helbredsforløb, 1.927.694 F_ID, 1.168.808 markørrækker uden F_ID).

## 1. Lagene og filerne

```
arc.udredningsret_mont ──► stg.pre_Helbredsforloeb ──► map.Helbredsforloeb
        │                          │ (scope)
        ├──► stg.pre_Udredningsforloeb ──► stg.fct_Udredningsforloeb ──► map.Udredningsforloeb ──► fct.Udredningsforloeb
        │                          └────► stg.dim_UdredningsretEgenskaber ─┬► map.UdredningsretKlassifikation ──► dim.UdredningsretKlassifikation
        │                                                                  └► dim.UdredningsretEgenskaber (delt PK)
        ├──► stg.dim_Forloebsmarkoer ──► map.Forloebsmarkoer ──► dim.Forloebsmarkoer
        └──► stg.fct_UdredningsretMarkoer ──► map.UdredningsretMarkoer ──► fct.UdredningsretMarkoer
```

| Lag | Fil | Asset (key) | Gruppe |
|---|---|---|---|
| kolonner/SQL | `DSA/SQL/staging/udredningsret_kolonner.py`, `udredningsret_staging_sql.py` | — | — |
| fælles opslag | `DSA/SQL/common/dimension_lookup_temp_tables.py` (`create_borger_cpr_lookup_tables_sql`, `create_organisation_sor_lookup_table_sql`, flyttet fra `drg_staging_sql.py`, som re-eksporterer dem) | — | — |
| pre_stg | `pre_stg_Helbredsforloeb.py`, `pre_stg_Udredningsforloeb.py` | `["udredningsret","pre_stg_Helbredsforloeb"]`, `["udredningsret","pre_stg_Udredningsforloeb"]` | `udredningsret_pre_staging` |
| stg | `stg_fct_Udredningsforloeb.py`, `stg_dim_UdredningsretEgenskaber.py`, `stg_fct_UdredningsretMarkoer.py`, `stg_dim_Forloebsmarkoer.py` | `["udredningsret","stg_..."]` | `udredningsret_staging` |
| borger | `DSA/FHIR/Patient/Patient_Udredningsret.py` (`fhir_patient_udredningsret`) | `fhir_patient_udredningsret` | `udredningsret_borger` (i `udredningsret_ingest_job`) |
| map | `map_Helbredsforloeb.py`, `map_Udredningsforloeb.py`, `map_UdredningsretKlassifikation.py`, `map_Forloebsmarkoer.py` (US-4); `map_UdredningsretMarkoer.py` (US-5) | `map_<Tabel>` | `udredningsret_map` |
| dim | `dim_UdredningsretKlassifikation.py`, `dim_UdredningsretEgenskaber.py`, `dim_Forloebsmarkoer.py` (US-4) | `dim_<Tabel>` | `udredningsret_dimension` |
| fct | `fct_Udredningsforloeb.py` (US-4); `fct_UdredningsretMarkoer.py` (US-5) | `fct_<Tabel>` | `udredningsret_fact` |
| lineage | `data_lineage/dim/dim_udredningsretklassifikation.yml`, `dim_udredningsretegenskaber.yml`, `dim_forloebsmarkoer.yml`, `data_lineage/fct/fct_udredningsforloeb.yml` (US-4); `data_lineage/fct/fct_udredningsretmarkoer.yml` (US-5) | — | — |
| DQA | `dqa/config/dwh_subjects.py` (`_FACTS`-post pr. fact, US-4/US-5), `dqa/config/udredningsret_subjects.py` (§9, US-6, registreret i `dqa/config/__init__.py`'s `ALL_SUBJECTS`); sensor-specen `dqa_udredningsret_after_publish` i `dqa/sensors.py` (US-6) | — | — |
| DSO | `DSO/load/sync_udredningsret.py`, `UDREDNINGSRET_SYNC_CONFIGS` i `sync_configs.py` (også i `ALL_SYNC_CONFIGS`, som `dqa/dso.py` udleder `dso_reconciliation` af) | `dso_sync_<tabel i små bogstaver>`, fx `dso_sync_fct_udredningsretmarkoer` | `udredningsret_dso_sync` |
| DDL | `infrastructure/ddl/tables/udredningsret/02_maps.sql`, `03_dims.sql`, `04_facts.sql`, `05_seeds.sql` | `staging_infra_udredningsret` (findes), `dso_infra_udredningsret` | infrastructure |
| sensor | `sensors/udredningsret_publish_sensor.py` (US-6), bygget af `build_udredningsret_publish_sensor` i `definitions.py` efter branch-postfixing | `udredningsret_publish_sensor` | — |
| jobs | `jobs/udredningsret_jobs.py`: `udredningsret_ingest_job` (+ `udredningsret_borger`), `udredningsret_publish_job` | — | — |
| tests | `tests/unit/test_udredningsret_dwh.py`, `test_udredningsret_markoer.py` (US-4/US-5), `test_udredningsret_dso.py` (US-6: sync-configs, assets, views, sensor, subjects) | — | — |

Asset-form, DFM-håndtering og metadata følger DRG 1:1 (`pre_stg_DRGKontakt.py`, `stg_fct_DRGKontakt.py`,
`map_DRGKontakt.py`, `dim_DRGKontaktKlassifikation.py`, `dim_DRGKontaktEgenskaber.py`, `fct_DRGKontakt.py`).
Prosa i kode er engelsk (review F21 på MR !94); identifikatorer, tests og docs er danske.

## 2. Forretningsnøgler og clean_codes

Arkivet leverer nøglerne; staging danner `clean_code_*` af dem. CPR indgår aldrig i en clean_code
(den ligger hashet i `helbredsforloeb_noegle`). Alle clean_codes er ASCII og typet `VARCHAR`.

| clean_code | Udtryk | Type |
|---|---|---|
| `clean_code_Helbredsforloeb` | `'UDREDHF_' + helbredsforloeb_noegle` | `VARCHAR(80)` (72 tegn) |
| `clean_code_Udredningsforloeb` | `'UDREDFORL_' + helbredsforloeb_noegle + '_' + REPLACE(startdato,'-','') + '_' + loebenummer` | `VARCHAR(100)` (≤ 90 tegn) |
| `clean_code_UdredningsretMarkoer` | `'UDREDMRK_' + markoer_noegle + '_' + arc.loebenummer` | `VARCHAR(100)` (≤ 80 tegn) |
| `clean_code_UdredningsretKlassifikation` | `'UDREDKLASS_' + SHA2_256 (hex, 64) over de otte klassifikationsattributter, længde-præfikset som DRG (`klassifikation_clean_code_sql`) | `NVARCHAR(100)` (som DRG) |
| `clean_code_Forloebsmarkoer` | `'FORLOEBSMARKOER_' + UPPER(LTRIM(RTRIM(FORLOEBSMARKOER)))` | `VARCHAR(50)` |

Læsbar sammenkædning frem for hash overalt, hvor den holder sig under 255 tegn (DRG-læring:
`clean_code_DRGKontakt` `VARCHAR(255)`); kun klassifikationen hashes (læsbarheden bæres af dimensionsrækken).

**Løbenummer på udredningsforløb** bevares fra `map.Udredningsforloeb` og
`dim.UdredningsretEgenskaber`, også for retirerede forløb. Et eksisterende forløb genfindes inden for
samme `(helbredsforløb, startdato)` på første forløbselement-UUID og startmarkør-UUID. Hvis
forløbselementet er entydigt i både historik og aktuel leverance, kan en genudstedt startmarkør-UUID
også genfindes. NULL-markør-id matches NULL eksplicit; det gælder også historik uden begge UUID’er.
Tvetydige matches fejler før dimensioner og facts overskrives.

Kun nye forløb nummereres med `ROW_NUMBER`, i orden efter første forløbselement-UUID, slutdato og
F_ID, **over det højeste allerede reserverede løbenummer**. Et slettet forløbs plads genbruges ikke,
og nye eller fjernede parallelle forløb flytter derfor ikke de eksisterendes nøgler. Formatet på
clean_code og de eksisterende `dw_ek` bevares; der kræves ingen ændring af tabelstrukturen.

## 3. Grain og regler i pre_stg

### 3.1 Scope og delta-vindue (`stg.pre_Helbredsforloeb` er scope-tabellen)

- Checkpoint = MIN over de downstream ejere `("dim","UdredningsretEgenskaber")`,
  `("dim","UdredningsretKlassifikation")`, `("dim","Forloebsmarkoer")`, `("fct","Udredningsforloeb")`,
  `("fct","UdredningsretMarkoer")` (som `pre_stg_DRGKontakt`) — men **en ejer uden checkpoint tvinger fuld load** (U11,
  afviger fra DRG, som springer `None` over). `mode=full_load` ⇒ intet checkpoint.
- Delta-scope = helbredsforløb (`helbredsforloeb_noegle`) med **mindst én arc-række** (levende eller
  soft-slettet) med `dw_timestamp_update > checkpoint` **∪** helbredsforløb, hvis levende
  `fct.Udredningsforloeb`-rækker står med `dw_ek_Borger = -1` (så en borger, som natkæden har mintet
  siden sidst, opløses uden fuld genkørsel — "borger-genopretning i delta").
- `stg.pre_Helbredsforloeb` indeholder præcis scope'ets helbredsforløb: alle øvrige staging-tabeller
  begrænses til `EXISTS (SELECT 1 FROM stg.pre_Helbredsforloeb h WHERE h.helbredsforloeb_noegle = a.helbredsforloeb_noegle)`.
- Et helbredsforløb i scope, hvor **alle** markører er soft-slettede, staged'es med `dw_is_deleted = 1`
  og 0 forløb — det er sådan facten opdager, at forløbene er væk (§6).

### 3.2 `stg.pre_Helbredsforloeb` — én række pr. `helbredsforloeb_noegle` i scope

| Kolonne | Type | Regel |
|---|---|---|
| `clean_code_Helbredsforloeb` | `VARCHAR(80)` | §2 |
| `helbredsforloeb_noegle` | `CHAR(64)` | arc |
| `helbredsforloeb_id` | `NVARCHAR(50)` | `MAX(DW_EK_HelbredsForloeb)` over levende rækker (SDS-surrogat, egenskab) |
| `dw_ek_Borger` | `INT` | CPR → `#borger_current_cpr`/`#borger_previous_cpr` (`dim.Borger.cpr_nummer`, `tidligere_cpr`); blank → -2, ukendt → -1. CPR forlader aldrig pre_stg |
| `borger_opslag` | `NVARCHAR(20)` | `cpr` / `tidligere_cpr` / `ukendt` / `tom` |
| `foerste_markoer_kode`, `foerste_markoer_dato`, `foerste_enhed_sor`, `foerste_forloebselement_uuid` | `NVARCHAR(20)`, `NVARCHAR(10)`, `NVARCHAR(25)`, `NVARCHAR(36)` | Første levende markør: `ORDER BY FORLOEBSMARKOER_DATO, FORLOEBSMARKOER, ANSVARLIG_ENHED_FORLOB, LOWER(OBJEKTID_FORLOBELEMENT)` (samme orden som arkivets nøgleberegning) |
| `antal_markoerer`, `antal_forloebselementer`, `antal_udredningsforloeb` | `INT` | Levende rækker / distinkte `OBJEKTID_FORLOBELEMENT` / distinkte ikke-blanke `F_ID` |
| `dw_is_deleted` | `BIT` | 1 når 0 levende rækker |
| `dw_timestamp_update` | `DATETIME2` | `MAX(arc.dw_timestamp_update)` over alle rækker |

Unikt clustered index på `clean_code_Helbredsforloeb`; nonclustered på `helbredsforloeb_noegle`.

### 3.3 `stg.pre_Udredningsforloeb` — én række pr. `(helbredsforloeb_noegle, F_ID)` over levende rækker, F_ID ikke blank, hf i scope

Markørgrupper (SDS bilag 1, samme regler som profileringsscriptets `MARKER_GROUPS`; én CASE i
`udredningsret_kolonner.markoer_gruppe_sql(alias)`), i prioriteret rækkefølge:

| gruppe | koder (T-SQL) | gruppe_tekst |
|---|---|---|
| `henvist_udredning` | `IN ('AFV01A1','AFV01A1A','AFA01A','AFD01A')` eller `LIKE 'AFB[0-9][0-9]A'` | Henvist til udredning |
| `udredning_start` | `= 'AFV01B'` | Udredning start |
| `endeligt_udredt` | `LIKE 'AFX01A%'` eller `= 'AFX01C'` | Endeligt udredt |
| `udredningsplan` | `LIKE 'AFX01D1%'` eller `IN ('AFX01D2','AFX01D3','AFX01D4','AFX01D9')` | Udredningsplan |
| `afbrudt` | `IN ('AFV01X1','AFV01X2','AFV01X4')` | Afbrudt |
| `kraeftpakke_slut_beslutning` | `IN ('AFA01X2','AFA01X9','AFD01X2','AFD01X9','AFD01D')`, `LIKE 'AFB[0-9][0-9]C%'`, `LIKE 'AFB[0-9][0-9]X[29]'` | Kræftpakke slut/beslutning |
| `behandling_start` | `= 'AFV01F'` | Behandling start |
| `oevrige` | resten | Øvrige |

Regler pr. forløb (F_ID-gruppe):

- **Startmarkør** = markøren med tidligste `FORLOEBSMARKOER_DATO` i gruppen `henvist_udredning`
  (ties: laveste kode, laveste `LOWER(OBJEKTID_FORLOBSMARKOR)`); findes ingen henvist-markør (142.461
  forløb): forløbets tidligste markør. `startdato` = dens dato.
- **Slutmarkør** = markøren med `MRK_H = '1'`, der ikke er i `henvist_udredning` (SDS' indikator-slut);
  findes ingen: seneste markør i gruppen `endeligt_udredt`; findes heller ingen: **ingen slutmarkør**
  (`slutdato` NULL ⇒ BLANK-sentinel i dato-nøglen, T3.2's AC). Ties: seneste dato, laveste kode, laveste UUID.
- **Attributmarkør** = slutmarkøren, ellers forløbets seneste markør (`FORLOEBSMARKOER_DATO DESC`,
  kode, UUID). Herfra læses: `C_ADIAG`, `C_ADIAG_KONTAKT`, `ANSVARLIG_ENHED_KONTAKT`,
  `ANSVARLIG_ENHED_FORLOB`, `Speciale_ansvarlig_enhed_kontakt`, `ALDER`, `C_BOREGION_F`, `C_BOREGION`,
  `C_SGHREGION`, `PSYKIATRI`, `MRK_C`, `H`, `MRK_ST_H`, `MRK_ST_L`, `MRK_ST_N`, `MRK_L_N`, `OPGPER`,
  `OPGMAANED`, `FORLOEBSLABEL`, `PK`, `OBJEKTID_FORLOBSMARKOR`.
- **`indgaar_i_indikator`** = 1 når gruppen har en henvist-markør med `MRK_H='1'` **og** en ikke-henvist
  markør med `MRK_H='1'` (1.681.973 forløb; de 11 forløb med kun én MRK_H-markør indgår ikke, som i kilden).
- Tællere: `antal_markoerer` (levende rækker i gruppen), `antal_udredningsplaner` (gruppe
  `udredningsplan`), `antal_afbrydelser` (gruppe `afbrudt`) — dannes her (T5.3 opfyldes i pre_stg, så
  forløbsfacten aldrig læser markørfacten).
- Indikatorværdier tages som leveret (B5): ingen egen beregning af H, overholdelse eller kvartal.

| Kolonne | Type | Kilde |
|---|---|---|
| `clean_code_Udredningsforloeb`, `clean_code_Helbredsforloeb` | `VARCHAR(100)`, `VARCHAR(80)` | §2 |
| `helbredsforloeb_noegle` | `CHAR(64)` | arc |
| `f_id`, `pk` | `NVARCHAR(500)`, `NVARCHAR(20)` | `F_ID` (gruppen), attributmarkørens `PK` |
| `loebenummer` | `INT` | §2 |
| `startdato`, `start_markoer_kode`, `start_markoer_uuid` | `NVARCHAR(10)`, `NVARCHAR(20)`, `NVARCHAR(36)` | startmarkør |
| `slutdato`, `slut_markoer_kode`, `slut_markoer_uuid` | do. (NULL uden slutmarkør) | slutmarkør |
| `seneste_markoer_dato` | `NVARCHAR(10)` | `MAX(FORLOEBSMARKOER_DATO)` |
| `foerste_forloebselement_uuid` | `NVARCHAR(36)` | `MIN(LOWER(OBJEKTID_FORLOBELEMENT))` i gruppen |
| `helbredsforloeb_id` | `NVARCHAR(50)` | `MAX(DW_EK_HelbredsForloeb)` |
| `aktionsdiagnose_kode` `NVARCHAR(20)`, `diagnosekontakt_uuid` `NVARCHAR(36)`, `kontakt_enhed_sor` `NVARCHAR(25)`, `forloeb_enhed_sor` `NVARCHAR(25)`, `speciale` `NVARCHAR(100)`, `alder_raa` `NVARCHAR(10)`, `bopaelsregion_forloeb` `NVARCHAR(10)`, `bopaelsregion` `NVARCHAR(10)`, `sygehusregion` `NVARCHAR(10)`, `psykiatri` `NVARCHAR(5)`, `hjerteomraade` `NVARCHAR(5)`, `udredningstid_raa` `NVARCHAR(10)`, `inden_30_dage` `NVARCHAR(5)`, `overholdt` `NVARCHAR(5)`, `aarsag` `NVARCHAR(5)`, `mrk_l_n` `NVARCHAR(5)`, `opgoerelseskvartal` `NVARCHAR(10)`, `opgoerelsesmaaned` `NVARCHAR(10)`, `forloebslabel` `NVARCHAR(20)` | | attributmarkør (rå, trimmet; blank → NULL) |
| `indgaar_i_indikator` | `BIT` | ovenfor |
| `antal_markoerer`, `antal_udredningsplaner`, `antal_afbrydelser` | `INT` | ovenfor |
| `dw_is_deleted` | `BIT` | altid 0 (kun levende rækker; forsvundne forløb håndteres via scope, §6) |
| `dw_timestamp_update` | `DATETIME2` | `MAX` over gruppen |

Unikt clustered index på `clean_code_Udredningsforloeb` (grain hard-fail via indekset og en eksplicit
COUNT/COUNT DISTINCT-kontrol i assettet); nonclustered på `(helbredsforloeb_noegle, f_id)`.

### 3.4 `stg.dim_Forloebsmarkoer` — én række pr. distinkt `FORLOEBSMARKOER` i arc (levende rækker; ikke delta-scoped, 433 koder)

`clean_code_Forloebsmarkoer`, `kode NVARCHAR(20)`, `gruppe NVARCHAR(40)`, `gruppe_tekst NVARCHAR(100)`,
`tekst NVARCHAR(500)` = `codesystem.SKSConcept.display` (LEFT JOIN på `code`; alle 433 koder findes på dev),
`tekst_kilde NVARCHAR(30)` = `'SKS'` / `'ukendt'` (`tekst` = `'Ukendt'`). Ukendte koder mintes altid
(gruppe `oevrige`) — aldrig hard-fail. Deps: `arc_udredningsret_mont`, `cs_sks`.

### 3.5 `stg.fct_UdredningsretMarkoer` — én række pr. arc-række (levende **og** soft-slettede) med hf i scope

| Kolonne | Type | Kilde |
|---|---|---|
| `clean_code_UdredningsretMarkoer` | `VARCHAR(100)` | §2 |
| `clean_code_Udredningsforloeb` | `VARCHAR(100)` NULL | `stg.pre_Udredningsforloeb` på `(helbredsforloeb_noegle, F_ID)`; NULL når F_ID er blank eller gruppen ikke findes (markøren soft-slettet med forældet F_ID) |
| `clean_code_Helbredsforloeb`, `clean_code_Forloebsmarkoer` | | §2 |
| `dw_ek_Borger`, `borger_opslag` | `INT`, `NVARCHAR(20)` | `stg.pre_Helbredsforloeb` (samme borger for hele helbredsforløbet) |
| `forloeb_enhed_sor`, `dw_ek_Organisation_forloeb` | `NVARCHAR(25)`, `INT` | `ANSVARLIG_ENHED_FORLOB` via `#organisation_sor` (blank og uden match → -1; rå kode beholdes) |
| `markoer_kode`, `markoer_dato`, `dw_ek_Dato_markoer` | `NVARCHAR(20)`, `NVARCHAR(10)`, `INT` | `FORLOEBSMARKOER`, `FORLOEBSMARKOER_DATO`, `dato_key()` |
| `helbredsforloeb_id`, `forloebselement_uuid`, `markoer_uuid`, `f_id`, `pk` | `NVARCHAR(50)`, `NVARCHAR(36)`, `NVARCHAR(36)`, `NVARCHAR(500)`, `NVARCHAR(20)` | degenererede egenskaber (B9), "senest leverede" |
| `indgaar_i_indikator_H`, `indgaar_i_indikator_L_N` | `BIT` | `MRK_H='1'`, `MRK_L_N='1'` |
| `sygehusregion`, `bopaelsregion` | `NVARCHAR(10)` | `C_SGHREGION`, `C_BOREGION` |
| `antal_kilderaekker` | `INT` | arc |
| `dw_haendelsestidspunkt` | `DATETIME2` | `TRY_CONVERT(DATETIME2, FORLOEBSMARKOER_DATO)` |
| `dw_is_deleted`, `dw_timestamp_update` | | arc |

Unikt clustered index på `clean_code_UdredningsretMarkoer`.

### 3.6 `stg.fct_Udredningsforloeb` og `stg.dim_UdredningsretEgenskaber` (fra `stg.pre_Udredningsforloeb`, samme grain)

`stg.fct_Udredningsforloeb`: `clean_code_Udredningsforloeb`, `clean_code_Helbredsforloeb`,
`clean_code_UdredningsretKlassifikation`, `dw_ek_Borger` + `borger_opslag` (fra `stg.pre_Helbredsforloeb`),
`forloeb_enhed_sor` + `dw_ek_Organisation_forloeb`, `kontakt_enhed_sor` + `dw_ek_Organisation_kontakt`
(`#organisation_sor`; blank/ukendt → -1), `dw_ek_Dato_start` = `dato_key(startdato)`, `dw_ek_Dato_slut` =
`dato_key(ISNULL(slutdato, ''))` (BLANK 99999998 uden slutmarkør), `dw_ek_Dato_opgoerelse` =
`dato_key(opgoerelse_dato_sql(opgoerelseskvartal))` hvor `'2K26'` → `'2026-04-01'` (kvartalets første dag;
mønster `[1-4]K[0-9][0-9]`, århundrede 20; andet → `''` ⇒ BLANK), `udredningstid_dage` =
`TRY_CONVERT(INT, udredningstid_raa)` kun når `indgaar_i_indikator = 1` (ellers NULL), `indgaar_i_indikator BIT`,
`overholdt BIT` (`MRK_ST_L`: '1'→1, '0'→0, blank→NULL), `inden_30_dage BIT` (do.), `alder_ved_henvisning SMALLINT`,
`aktionsdiagnose_kode`, `diagnosekontakt_uuid`, `helbredsforloeb_id`, `antal_markoerer`, `antal_udredningsplaner`,
`antal_afbrydelser`, `dw_haendelsestidspunkt` = `TRY_CONVERT(DATETIME2, startdato)`, `dw_is_deleted`, `dw_timestamp_update`.

`stg.dim_UdredningsretEgenskaber`: `clean_code_Udredningsforloeb`, `clean_code_UdredningsretKlassifikation` og
attributterne `f_id`, `pk`, `start_markoer_kode`, `start_markoer_uuid`, `slut_markoer_kode`, `slut_markoer_uuid`,
`seneste_markoer_dato`, `foerste_forloebselement_uuid`, `helbredsforloeb_id`, `forloeb_enhed_sor`,
`kontakt_enhed_sor`, `aktionsdiagnose_kode`, `diagnosekontakt_uuid`, `opgoerelseskvartal`, `opgoerelsesmaaned`,
`sygehusregion`, `bopaelsregion`, `speciale`, `forloebslabel`, `mrk_l_n`, `dw_is_deleted`, `dw_timestamp_update`.
Attributlisten er ÉN tuple i `udredningsret_kolonner.py` (`EGENSKABER_ATTRIBUTTER`), som dim-SQL og DDL-test afledes af.

**Klassifikationens otte attributter** (`KLASSIFIKATION_ATTRIBUTTER`, i denne rækkefølge): `overholdt`,
`aarsag`, `inden_30_dage`, `psykiatri`, `hjerteomraade`, `forloebslabel`, `bopaelsregion_forloeb`, `speciale`.
Målt på dev 2026-09-13: **2.948** kombinationer i `stg.dim_UdredningsretEgenskaber` efter en fuld kørsel (< 10.000). Profileringens 3.105 var talt direkte på slutmarkørerne og tæller derfor også kombinationer fra markører, der ikke danner et forløb.

## 4. map, dim og fct (US-4/US-5)

### 4.1 DDL-konventioner

Som DRG (`tables/drg/02_maps.sql` … `05_seeds.sql`): map = `dw_ek_<Tabel> INT IDENTITY(1,1)` + `clean_code_<Tabel>` PK
clustered + `IX_Map_<Tabel>_DW_EK`; dims og facts PK på `dw_ek_*`; FK'er `INT NOT NULL CONSTRAINT [DF_<Tabel>_<kolonne>] DEFAULT -1`;
sentinel-seeds -1 `'UNKNOWN'` for map og dim (aldrig for fct); constraint-navne `PK_Map_*`, `PK_Dim_*`, `PK_Fct_*`, `IX_*`, `DF_*`
(søskende-konventionen fra DRG vinder over KONTEKST-blokkens `PK_udredningsret_*`, som allerede er brudt af `01_meta.sql`).
Audit `dw_timestamp_insert/update DATETIME2 NOT NULL DEFAULT SYSDATETIME()`. Ingen migration nødvendig: tabellerne er nye.

### 4.2 Tabeller

**map**: `map.Helbredsforloeb` (`clean_code_Helbredsforloeb VARCHAR(80)`), `map.Udredningsforloeb` (`VARCHAR(100)`),
`map.UdredningsretKlassifikation` (`NVARCHAR(100)`), `map.Forloebsmarkoer` (`VARCHAR(50)`), `map.UdredningsretMarkoer` (`VARCHAR(100)`).
Insert-only: ét `INSERT … SELECT DISTINCT … WHERE NOT EXISTS` pr. map (hver har præcis én kilde, så DRG's `UNION ALL` over
flere kilder er ikke nødvendig), aldrig MERGE/UPDATE/DELETE. Kilder: hf ← `stg.pre_Helbredsforloeb`;
forløb + klassifikation ← `stg.dim_UdredningsretEgenskaber`; markør ← `stg.fct_UdredningsretMarkoer`; forløbsmarkør ← `stg.dim_Forloebsmarkoer`.
`map.UdredningsretMarkoer` minter nøgler for **både levende og soft-slettede** stagede markører: en retireret markør skal
beholde sit `dw_ek`, så den genoplives på den samme fact-række, når leverancen bringer den tilbage (§6). Sentinel -1 seedes
som for de øvrige maps, men facten opløser sin egen nøgle med en INNER JOIN og skriver den derfor aldrig.

**dim.UdredningsretKlassifikation** (Type 1, aggregeret pr. clean_code som DRG): `dw_ek_UdredningsretKlassifikation`,
`clean_code_UdredningsretKlassifikation`, `overholdt NVARCHAR(5)`, `overholdt_tekst NVARCHAR(50)`, `aarsag NVARCHAR(5)`,
`aarsag_tekst NVARCHAR(200)`, `inden_30_dage NVARCHAR(5)`, `inden_30_dage_tekst NVARCHAR(50)`, `psykiatri NVARCHAR(5)`,
`psykiatri_tekst NVARCHAR(50)`, `hjerteomraade NVARCHAR(5)`, `hjerteomraade_tekst NVARCHAR(50)`, `forloebslabel NVARCHAR(20)`,
`forloebslabel_tekst NVARCHAR(500)`, `bopaelsregion_forloeb NVARCHAR(10)`, `bopaelsregion_forloeb_tekst NVARCHAR(50)`, `speciale NVARCHAR(100)`.
Kolonnetuplen afledes i dim-modulet af `KLASSIFIKATION_ATTRIBUTTER` (rå værdi efterfulgt af `<attribut>_tekst`, undtagen
`speciale`, som er fritekst fra leverancen og forklarer sig selv); en unit-test holder den statiske DDL til samme tuple.

**Kilden til de otte rå værdier (U12):** Kun `forloebslabel` og `speciale` ligger på `stg.dim_UdredningsretEgenskaber`. Medlemmet
grupperes derfor på den stagede `clean_code_UdredningsretKlassifikation`, mens værdierne hentes fra `stg.pre_Udredningsforloeb`,
joinet på det `clean_code_Udredningsforloeb`, de to stagingtabeller deler (samme grain, én række hver), med `MAX()` pr. attribut.
Værdierne er konstante inden for gruppen, fordi hashen er dannet af netop de otte værdier, så `MAX()` opfinder ingen orden
(samme idiom som DRG's klassifikation).

Tekster (én `CASE` pr. tekstkolonne, renderet af `tekst_case_sql` ud fra dictene i `udredningsret_kolonner.py` — aldrig skrevet
i hånden i dim-modulet; opslagsnøglen er `ISNULL(LTRIM(RTRIM(<attribut>)), '')`, så NULL og blank falder på dictens tomme nøgle):
`overholdt_tekst` = Overholdt / Ikke overholdt / Ikke i opgørelsen (blank); `aarsag_tekst` = §7-tabellen
(`AARSAG_TEKSTER` i `udredningsret_kolonner.py`); `inden_30_dage_tekst` = Inden for 30 dage / Over 30 dage / Ikke i opgørelsen;
`psykiatri_tekst` = Psykiatri ('1') / Somatik ('0') / Ukendt; `hjerteomraade_tekst` = Hjerteområdet ('1') / Uden for hjerteområdet ('0') / Ukendt;
`bopaelsregion_forloeb_tekst` = 1081 Region Nordjylland, 1082 Region Midtjylland, 1083 Example Region, 1084 Region Hovedstaden,
1085 Region Sjælland (Danmarks Statistiks regionskoder), andet → koden selv, blank → 'Ukendt'. En værdi, dictet ikke kender,
giver 'Ukendt' (`UKENDT_TEKST`) på de øvrige tekstkolonner.

`forloebslabel_tekst` er den eneste tekst, der ikke kommer fra et dict: den slås op i `codesystem.SKSConcept.display` på
`s.[code] = UPPER(LTRIM(RTRIM(forloebslabel)))` (LEFT JOIN, så kolonnen står NULL, indtil kataloget har koden). Efter hver load
kører desuden en global, idempotent refresh over ALLE medlemmer undtagen -1, som kun rører rækker, hvor teksten faktisk afviger
(som DRG's `drg_beskrivelse`) — uden den ville en katalogrettelse aldrig nå medlemmer, hvis forløb ikke var med i delta-batchen.
Derfor har dimensionen også en dep på `cs_sks`.

Sentinel -1 bærer `overholdt_tekst = 'Ukendt'` og `aarsag_tekst = 'Ukendt'`, så en rapport på tekstkolonnerne aldrig viser en tom
celle for en uopløst nøgle.

**dim.UdredningsretEgenskaber** (transaktionsdimension, delt PK `dw_ek_Udredningsforloeb` med facten via `map.Udredningsforloeb`, Type 1):
PK + `clean_code_Udredningsforloeb` + `EGENSKABER_ATTRIBUTTER` (§3.6) + audit. Præcis én række pr. fact-række; ingen CPR.

**dim.Forloebsmarkoer** (fælles, Type 1): `dw_ek_Forloebsmarkoer`, `clean_code_Forloebsmarkoer`, `kode`, `tekst`, `gruppe`,
`gruppe_tekst`, `tekst_kilde` + audit. En ukendt kode er et normalt medlem, aldrig hard-fail: staging minter den allerede med
`tekst = 'Ukendt'`, `tekst_kilde = 'ukendt'` og `gruppe = 'oevrige'` (U7). Sentinel -1 bærer `tekst = 'Ukendt'`.

**fct.Udredningsforloeb** (DDL-orden): `dw_ek_Udredningsforloeb INT` PK, `clean_code_Udredningsforloeb VARCHAR(100) NULL`,
`dw_ek_Helbredsforloeb`, `dw_ek_Borger`, `dw_ek_Organisation_forloeb`, `dw_ek_Organisation_kontakt`, `dw_ek_UdredningsretKlassifikation`,
`dw_ek_Dato_start`, `dw_ek_Dato_slut`, `dw_ek_Dato_opgoerelse` (alle `INT NOT NULL DEFAULT -1`), `udredningstid_dage INT NULL`,
`indgaar_i_indikator BIT NOT NULL DEFAULT 0`, `overholdt BIT NULL`, `inden_30_dage BIT NULL`, `alder_ved_henvisning SMALLINT NULL`,
`aktionsdiagnose_kode NVARCHAR(20) NULL`, `diagnosekontakt_uuid NVARCHAR(36) NULL`, `helbredsforloeb_id NVARCHAR(50) NULL`,
`antal_markoerer INT NULL`, `antal_udredningsplaner INT NULL`, `antal_afbrydelser INT NULL`, `dw_haendelsestidspunkt DATETIME2 NULL`,
`dw_is_deleted BIT NOT NULL DEFAULT 0`, audit. **Ingen** `dw_ek_UdredningsretEgenskaber`-kolonne: egenskabsdimensionen deler PK
(som `fct.DRGKontakt`/`dim.DRGKontaktEgenskaber`; T4.5's kolonneliste rettes tilsvarende). Indexes: unikt filtreret på clean_code,
Borger, Helbredsforloeb, Klassifikation, Organisation_Forloeb, Dato_Start, Dato_Opgoerelse, Watermark (`dw_timestamp_update`).

**fct.UdredningsretMarkoer**: `dw_ek_UdredningsretMarkoer INT` PK, `clean_code_UdredningsretMarkoer VARCHAR(100) NULL`,
`dw_ek_Udredningsforloeb`, `dw_ek_Helbredsforloeb`, `dw_ek_Forloebsmarkoer`, `dw_ek_Borger`, `dw_ek_Organisation_forloeb`,
`dw_ek_Dato_markoer` (`INT NOT NULL DEFAULT -1`), `helbredsforloeb_id NVARCHAR(50)`, `forloebselement_uuid NVARCHAR(36)`,
`markoer_uuid NVARCHAR(36)`, `indgaar_i_indikator_H BIT NOT NULL DEFAULT 0`, `indgaar_i_indikator_L_N BIT NOT NULL DEFAULT 0`,
`sygehusregion NVARCHAR(10)`, `bopaelsregion NVARCHAR(10)`, `antal_kilderaekker INT NOT NULL DEFAULT 1`,
`dw_haendelsestidspunkt DATETIME2 NULL`, `dw_is_deleted`, audit. `dw_ek_Udredningsforloeb = -1` for markører uden F_ID
(1.168.808 rækker på leverancen 2026-09-07 — designmæssigt, ikke et opslagsfald; DQA-cap med luft, §9. T5.1's acceptkriterium
skriver 1.169.293; det tal kommer fra profileringsscriptet, mens 1.168.808 er målt på arkivet af US-3-kørslen).
`markoer_kode` og `markoer_dato` er staget, men bæres bevidst **ikke** med: koden er `dim.Forloebsmarkoer` via
`dw_ek_Forloebsmarkoer`, og datoen er `dw_ek_Dato_markoer` plus `dw_haendelsestidspunkt` — ingen degenererede kopier. Facten
bærer heller ikke stagingens `borger_opslag`, rå SOR-kode, `f_id`, `pk` eller `dw_timestamp_update` (diagnostik og
staging-bogholderi; auditstemplerne sættes af loaden). Indexes: unikt filtreret på clean_code,
Udredningsforloeb, Helbredsforloeb, Forloebsmarkoer, Borger, Dato_Markoer, Watermark.

### 4.3 Load-regler

- UNMAPPED-guard før hver dim/fct (som DRG). `fct.Udredningsforloeb` tæller tre gange i ét kald — staged forløb uden
  `map.Udredningsforloeb`, helbredsforløb uden `map.Helbredsforloeb`, klassifikation uden `map.UdredningsretKlassifikation` —
  fordi alle tre opslag er INNER JOINs i resolve-sættet, og en umappet clean_code derfor ville tabe rækken i stilhed frem for
  at lande på -1. `fct.UdredningsretMarkoer` tæller **fire** gange i ét kald: markørens egen
  `clean_code_UdredningsretMarkoer` og `clean_code_Helbredsforloeb`, som er INNER JOINs af samme grund, samt
  `clean_code_Udredningsforloeb` og `clean_code_Forloebsmarkoer`, som er LEFT JOINs. De to sidste tællinger er begrænset til
  **ikke-NULL** clean_codes: NULL er dér den lovlige sentinel-situation (ingen F_ID, U5; blank markørkode), mens en staged
  ikke-NULL nøgle uden map-match er en fejl ("0 forældreløse"), aldrig stille -1. NULL → -1. Fejlbeskeden navngiver den map,
  der skal køres først, og siger for forløbet eksplicit, at en staged forælder SKAL findes.
  Tællingen på `clean_code_Forloebsmarkoer` er derudover begrænset til de **levende** stagede rækker, fordi det er hele den
  population, `map.Forloebsmarkoer` kan dække: map'en mintes fra `stg.dim_Forloebsmarkoer`, der bygges over arkivets levende
  rækker alene (§3.4), mens markørstagingen bærer levende **og** soft-slettede rækker (§3.5). En kode, der på
  kørselstidspunktet kun findes på soft-slettede rækker, kan derfor aldrig mintes, og uden grænsen ville værnet hard-faile på
  en tilstand, ingen map-kørsel kan rette (map'en er append-only). Den retirerede markør lander i stedet på -1 — rækken er
  alligevel slettet — og værnet gælder fuldt ud igen, så snart koden leveres levende. De tre andre tællinger har ikke og skal
  ikke have grænsen: markørens egen map fyldes fra selve markørstagingen, `map.Helbredsforloeb` fra `stg.pre_Helbredsforloeb`
  (som også stager et helbredsforløb helt uden levende rækker), og forløbets clean_code læses fra
  `stg.pre_Udredningsforloeb` — netop den tabel, `map.Udredningsforloeb` mintes fra.
- Tomt mål ⇒ `INSERT … WITH (TABLOCK)`; ellers MERGE med `EXCEPT`-change-detection; `WHEN MATCHED` sætter `dw_is_deleted` fra kilden (genopliver).
  "Tomt" betyder for dims: kun -1-sentinelrækken (map-nøgler er IDENTITY ≥ 1 og kolliderer aldrig med den); ingen af de to
  facts har en sentinel, så enhver række dér vælger MERGE-stien.
- Hvert resolve-sæt (`#Temp`) får en **clustered nøgle på sin `dw_ek_*`, før MERGE'n kører** — både de tre dims og begge facts.
  MERGE'n matcher mod målets clustered PK på samme kolonne, så uden nøglen skal kilden sorteres eller hashes ved hver
  kørsel; det betyder mest for `dim.UdredningsretEgenskaber` og de to facts, der bærer samme kardinalitet som kilden.
  Nøglen er bevidst **ikke** unik, så en dublet nås af tællingen nedenfor i stedet for at dø på en indeksbygning.
- Grain-validering på hver facts resolve-sæt (COUNT vs COUNT DISTINCT på `clean_code_Udredningsforloeb` hhv.
  `clean_code_UdredningsretMarkoer`), hard-fail.
- Retirering (§6) i `fct.Udredningsforloeb` kører efter loaden på **begge** stier: bulk-INSERT'et rammer kun et tomt mål,
  hvor der intet er at retirere, mens en genopbygning over et fyldt mål tager MERGE-stien og stadig skal retirere de forløb,
  der ikke længere findes. Scopet er de helbredsforløb, kørslen har staget (`stg.pre_Helbredsforloeb` →
  `map.Helbredsforloeb`), så et delta aldrig rører forløb uden for sit eget vindue.
- `fct.UdredningsretMarkoer` har **ingen** retireringsrutine (U14). Markøren findes 1:1 i arkivet, staging bærer de
  soft-slettede rækker med i sit scope (§3.5), og arkivet hard-sletter aldrig, så en markør kan kun retireres gennem sin egen
  stagede `dw_is_deleted` — den, MERGE'ns `WHEN MATCHED` skriver fra kilden. Markøren retireres altså, når leverancen dropper
  den, og genoplives, når leverancen bringer den tilbage.
- **Kontroltotal (hard-fail, hele tabellen mod hele arkivet, holder på tværs af delta-batches):**
  `fct.Udredningsforloeb`: levende rækker = antal `(helbredsforloeb_noegle, F_ID)`-par over levende arc-rækker med ikke-blank
  `F_ID` — præcis den grain, `stg.pre_Udredningsforloeb` danner (§3.3), ikke `COUNT(DISTINCT F_ID)` (U13);
  `SUM(indgaar_i_indikator)` = antal af de par, der både har en henvist- og en ikke-henvist markør med `MRK_H='1'`
  (henvist-prædikatet er `henvist_predicate_sql`, samme udtryk som pre_stg, hvor en NULL-kode falder i ELSE-grenen og tæller
  som ikke-henvist). `fct.UdredningsretMarkoer`: levende rækker = levende arc-rækker; `SUM(antal_kilderaekker)` = arkivets sum
  over levende rækker — grainet er 1:1 med arkivrækken, så rækketallet kræver ingen gruppering, og `antal_kilderaekker`
  gendanner leverancens eget rækketal før dubletreduktionen. Summerne castes til `BIGINT`, fordi `SUM` over `INT` løber over
  ved 2^31. Begge facts har derfor en dep på `arc_udredningsret_mont`, og en afvigelse fælder kørslen som `dg.Failure`
  ("kontroltotalen holder ikke") med de fire tal i metadata. Markørfactens total forudsætter, at facten er bootstrappet af en
  fuld staging-kørsel; udrulningsrækkefølgen sikrer det.
- FK-diagnostik pr. rolle i metadata (som `FK_DIAGNOSTIK_SQL`): -1 pr. rolle, `dw_ek_Borger = -2` (strukturelt fraværende CPR)
  talt for sig, og BLANK-tællinger (99999998) for `dw_ek_Dato_slut` og `dw_ek_Dato_opgoerelse`, som lovligt lander dér (U2).
  Kun levende rækker. Der logges og rapporteres udelukkende tællinger. `fct.UdredningsretMarkoer` tæller -1 for sine seks
  roller plus `dw_ek_Borger = -2`; den tæller ingen BLANK-sentinel, fordi den kun har én datorolle (`dw_ek_Dato_markoer`), og
  den lovligt blanke slutdato hører til forløbsfacten. `dw_ek_Udredningsforloeb = -1` rapporteres desuden under sit eget
  metadatanavn `markoerer_uden_forloeb`, fordi det er design (U5) og ikke en uopløst nøgle.
- Tællerkontrol på `fct.UdredningsretMarkoer` (metadata, **aldrig** hard-fail): `SUM(antal_markoerer)` over levende
  `fct.Udredningsforloeb`-rækker holdes op mod levende markørrækker med `dw_ek_Udredningsforloeb <> -1`, og begge tal plus
  differencen rapporteres. De to tal er bygget fra hver sin kilde med vilje (U6): `antal_markoerer` tælles i pre_stg direkte
  fra arkivet, så forløbsfacten aldrig læser markørfacten. Enigheden er beviset for, at de to læsninger af "et forløbs
  markører" beskriver de samme rækker; E2E'en asserter lighed på syntetiske data, og US-6 gør den til et DQA-subject.
  Fordi tællerkontrollen læser `fct.Udredningsforloeb`, har `fct.UdredningsretMarkoer` en dep på forløbsfacten: begge facts
  ligger i gruppen `udredningsret_fact`, og uden kanten er de sidestillede i grafen, så Dagster måtte bygge markørfacten
  først og sammenligne mod forrige kørsels `antal_markoerer`. Der opstår ingen cyklus — forløbsfacten læser stg, pre_stg,
  sine maps, arc og sit eget mål, aldrig markørfacten.

## 5. Borger og organisation

- CPR opløses i pre_stg via `dim.Borger` (`cpr_nummer`, fallback `tidligere_cpr`) og forlader aldrig staging. Målt: 889.770 distinkte
  CPR, 8.115 (0,91 %) uden `dim.Borger`-række før stubs.
- `fhir_patient_udredningsret` (`DSA/FHIR/Patient/Patient_Udredningsret.py`, kopi af `Patient_DRG.py` med kilde `arc.udredningsret_mont`
  (levende rækker) og `meta.source = "SDSUDRED"`) opretter stubs; gruppe `udredningsret_borger` i `udredningsret_ingest_job`.
- SOR: `ANSVARLIG_ENHED_FORLOB` og `ANSVARLIG_ENHED_KONTAKT` direkte mod `dim.Organisation.kode` (B8). Målt: 738/738 og 1.360/1.360 opløst.
  Blank og uden match → -1 (skelnes i metadata, ikke i nøglen — som DRG).

## 6. Soft-delete, genoplivning og forsvundne forløb

- Markører: arc's `dw_is_deleted` staged'es igennem; facten retirerer/genopliver via MERGE. Aldrig hard delete.
  `fct.UdredningsretMarkoer` har ingen selvstændig retireringsrutine — den har ikke brug for en (U14, §4.3).
- Udredningsforløb: kun levende markører danner forløb. Et forløb, der forsvinder (alle markører soft-slettede, eller markørerne har
  fået en anden start), retireres af `fct_Udredningsforloeb`: for hvert helbredsforløb i scope (`stg.pre_Helbredsforloeb` →
  `map.Helbredsforloeb`) sættes `dw_is_deleted = 1` på levende fact-rækker, hvis `dw_ek_Udredningsforloeb` ikke findes i den stagede
  batch. Genopliver, når clean_code'en dukker op igen.
- Et helbredsforløb, hvis første markør ændres (0,01 %/md), får ny `helbredsforloeb_noegle` i arkivet: dets gamle markører
  soft-slettes, de nye indsættes ⇒ nye `dw_ek` og retirering af de gamle. Arkivet måler genfindelsen (`helbredsforloeb_genfindelse`),
  men har ingen grænse, der stopper kørslen.

## 7. Årsagskoder (MRK_ST_N) — tekster og oprindelse

SDS' udredningsplan-koder (`AFX01D*`) forklarer årsagskoden. Målt 2026-09-12 på slutmarkører (`MRK_H='1'`, ikke henvist) mod
forløbets udredningsplan-markør med `MRK_L_N='1'`:

| `MRK_ST_N` | `MRK_ST_L` | Dominerende plan-kode | Tekst (`AARSAG_TEKSTER`) |
|---|---|---|---|
| 1 | 1 | ingen (761.091) / D4 / D2 / D1 / D3 | Udredt inden for 30 dage |
| 2 | 1 | AFX01D1, AFX01D1A (74.294) | Udredningsplan inden for 30 dage pga. faglige årsager |
| 3 | 1 | AFX01D2 (267.981) | Udredningsplan inden for 30 dage pga. patientens afslag om udredning i anden enhed |
| 4 | 1 | AFX01D3 (48.731) | Udredningsplan inden for 30 dage pga. patientens ønske om senere udredning |
| 5 | 0 | AFX01D4 (399.709) | Ikke overholdt: udredningsplan pga. manglende kapacitet |
| 6 | 0 | AFX01D9 (406) | Ikke overholdt: udredningsplan pga. andre årsager |
| 7 | 0 | blandet (10.090) | Ikke overholdt: udredningsplan først efter 30 dage |
| 8 | 0 | ingen (47.463) | Ikke overholdt: ingen udredningsplan |
| 0 | 0 | ingen (3) | Uoplyst |
| blank | blank | — | Ikke i opgørelsen |

Teksterne er formuleret af os ud fra SDS' SKS-tekster og datamart-beskrivelsens kategorier; SDS' egen ordlyd i
`Datamart_udredningsret.pdf` (bilag på featuren) bør erstatte dem 1:1, når den er verificeret. `tekst_kilde` på
`dim.Forloebsmarkoer` og kommentaren ved `AARSAG_TEKSTER` angiver oprindelsen.

## 8. Orkestrering

- `udredningsret_ingest_job` udvides med gruppen `udredningsret_borger` (stubs oprettes på ingest, som DRG/HAIBA).
- `udredningsret_publish_job` = grupperne `udredningsret_pre_staging` og `udredningsret_staging` (US-3),
  `udredningsret_map`, `udredningsret_dimension` og `udredningsret_fact`, som er lagt i selektionen med US-4 (US-5's markør-map
  og -fact hører til de samme to grupper og krævede derfor ingen ændring), samt `udredningsret_dso_sync`, som US-6 har lagt i.
  Grupperne tilføjes til selektionen i den US, der definerer dem (`test_asset_selection_groups_exist` afviser udefinerede
  grupper). Kører delta som standard. Jobbet publicerer altså DSA-laget **og** flytter det til DSO i én kørsel; det rører
  ikke de delte dimensioner på DSO (`dso_sync_dim_borger`/`dim_organisation` er deps for RI-orden, ikke en del af selektionen).

### 8.1 DSO-laget (US-6)

- **Sync.** `DSO/load/sync_udredningsret.py` bygger fem assets i gruppen `udredningsret_dso_sync`, ét pr. config i
  `UDREDNINGSRET_SYNC_CONFIGS`. Alle fem er `mode="watermark"` på `dw_timestamp_update` — hver tabel vedligeholdes
  inkrementelt i DSA (facterne med MERGE + soft-delete, dimensionerne som Type 1-upserts), så deltaet flytter præcis de
  ændrede rækker. Rækkefølgen er FK-orden på DSO: de tre dimensioner først, så `fct.Udredningsforloeb`, og
  `fct.UdredningsretMarkoer` sidst, fordi den peger på forløbsfacten. Hvert asset venter desuden på sit DSA-asset og på
  `dso_infra_udredningsret`. Synkroniseringen flytter **også** soft-slettede rækker (§6), så DSO bærer hele tilbagetrækningssporet.
- **Hvad der ikke synkroniseres.** De fem maps (nøgleminting med IDENTITY, staging-internt), `arc.udredningsret_mont`
  (som bærer den rå CPR), `ext.udredningsret*` og `meta.UdredningsretLeverance`. Ingen af dem findes på DSO.
  Det synlige fravær er `map.Helbredsforloeb`: facternes `dw_ek_Helbredsforloeb` opløser derfor ingen dimension på DSO
  (den er `FK-None` i lineage og får kun sentinel-rate, intet referentielt tjek). Den læsbare identifikator rejser i
  stedet med som `helbredsforloeb_id` på begge facter.
  Én **kolonne** holdes også tilbage: `clean_code_Udredningsforloeb` på `fct.Udredningsforloeb` og
  `dim.UdredningsretEgenskaber` bærer arkivets CPR-hash ordret og er derfor i `exclude_columns` på begge configs
  (**U18**); kolonnen findes på DSO-tabellen, men står NULL.
- **DDL.** `dso_infra_udredningsret` kører kun `03_dims.sql` og `04_facts.sql` på DSO — aldrig `02_maps.sql`
  eller `05_seeds.sql`; -1-sentinellerne ankommer med synkroniseringen. **Dette repo bygger ingen views over
  udredningsretten** (**U19**): præsentations- og DPA-laget ejes af security-views-repoet, som for
  `fct.DRGKontakt`. Aftagere læser indtil da basistabellerne på DSO og filtrerer selv `dw_is_deleted = 0` —
  begge facter akkumulerer retirerede rækker, så en ufiltreret læsning over-tæller mere for hver leverance.
- **Indeksflaget.** Begge facter kører med `chunk_size=20_000` og `disable_target_indexes_during_sync=True` (U17).
- `udredningsret_publish_sensor` (US-6, `sensors/udredningsret_publish_sensor.py` efter `haiba_publish_sensor.py`,
  bygget af `build_udredningsret_publish_sensor` i `definitions.py` efter branch-postfixing og registreret i
  `postfixed_sensors`): på `ifdb_dfm_daily_job` SUCCESS læses to tal fra **staging-databasen** — aldrig fra
  Dagsters event-log — i én forbindelse:
  1. **Leverancesignaturen**, `MAX(leverance_tidsstempel)` pr. `kildetabel` i `meta.UdredningsretLeverance` for hver
     arkiveret kildetabel (i dag én, `udredningsret_mont`), formateret `"<kildetabel>=<tidsstempel>"`. Mangler
     registret eller en kildetabel, skippes tick'et frem for at gætte.
  2. **Uafklarede forløb**, `COUNT(*)` på `fct.Udredningsforloeb` med `dw_ek_Borger = -1` **og** `dw_is_deleted = 0`.
     Kun levende rækker: en retireret række på -1 bliver aldrig repareret, fordi U9's genopretningsgren ikke samler den
     op, så den ville plateau'e gaten over nul for evigt. Blank CPR står på -2 og tælles derfor slet ikke med.

  **Gate 1** læser det seneste publish-forsøg for signaturen (tag
  `udredningsret/delivery_signature`). Ingen tidligere kørsel giver første publicering. En fejlet eller
  annulleret kørsel forsøges igen, også hvis en ældre kørsel lykkedes, og DSA allerede har afklaret alle
  borgere: DSO kan stadig mangle den delvist gennemførte publicering. Det fejlede forsøgs udgangstal
  bevares på genforsøget. Et ventende eller kørende forsøg får lov at afslutte uden en ekstra publicering.
  `run_key` bærer den udløsende kørsels id, så hver nattekørsel giver højst ét forsøg.
  En manglende `fct.Udredningsforloeb` betyder ukendt dækning, ikke nul uafklarede. Efter en tidligere
  succes skippes med en konkret besked om at genskabe infrastrukturen og køre `mode=full_load`;
  sensorens normale publish-selektion omfatter ikke infrastruktur-assets.
  **Gate 2** er genopretning: er signaturen uændret, men står der stadig uafklarede forløb, republiceres der, så længe
  tallet **falder** — hver genopretningskørsel tagges med sit udgangstal (`udredningsret/unresolved_at_request`), og
  næste tick sammenligner. Uændret eller stigende tal = plateau, og sensoren tier, indtil en ny leverance åbner gate 1.
  Bunden er nødvendig: en CPR, som FHIR-serveren kun kender som `previous_ssn`, når aldrig `ref.PatientToBorger` og kan
  derfor ikke nå nul. Delta-scope'ets borger-genopretning (§3.1, U9) gør genopretningskørslen billig — sensoren og den
  scope-gren tæller med vilje de samme rækker. En databasefejl giver `SkipReason`, aldrig et fejlet tick.
- DQA: `dqa_udredningsret_after_publish` i `dqa/sensors.py` (monitorerer `udredningsret_publish_job` →
  `dqa_dwh_daily_job`/`dqa_dwh_weekly_job`), subjects i `dqa/config/udredningsret_subjects.py` (§9). Sensoren er
  registreret med `default_status = STOPPED` som `dqa_drg_after_publish`: §9-tællingerne har brug for én publicering i
  `dqa.Results`, før deres baselines betyder noget, og det er **samme** job, DRG's parkerede sensor anmoder — en
  kørende udredningsret-sensor ville derfor stille starte DRG-tjekkene igen. Tænd dem sammen, når baselines er læst af.
  `_FACTS`-posterne i `dwh_subjects.py` følger derimod den US, der bygger facten:
  `fct.Udredningsforloeb` registreres i US-4 (`dwh_udredningsforloeb`, `sentinel_rate` over de otte
  FK-roller plus ugentlig `referential_integrity` for de syv, der har en dimension — `dw_ek_Helbredsforloeb` er `FK-None`
  og får kun sentinel-raten), `fct.UdredningsretMarkoer` i US-5 (`dwh_udredningsretmarkoer`, §9).
  DSO-reconciliation **udledes** af `UDREDNINGSRET_SYNC_CONFIGS` i `dqa/dso.py` (domænet `udredningsret`, `Track.DWH`):
  de fem `dso_reconciliation`-checks opstår alene ved, at configene er med i `ALL_SYNC_CONFIGS` — der er intet at
  tilføje i `dqa/dso.py` ud over domæne-posten. Fodaftrykket stopper dog ikke dér: domænet skal også have sin egen
  **lørdags-cron** i `dqa/schedules.py` (`_DSO_WEEKLY_CRONS["udredningsret"] = "30 11 * * 6"`, 30 min efter DRG).
  `build_dso_weekly_schedules` springer tavst et domæne uden cron over, og ingen sensor anmoder et `dqa_dso_*`-job for
  udredningsret — uden posten ville `dqa_dso_udredningsret_job` blive bygget, men aldrig udløst. Samme argument som
  HAIBA's: en leverancedrevet publicering kan ikke ride på en "jobbet kørte en lørdag"-gate. Bevidst parkering hører
  til i `_ALWAYS_STOPPED_DOMAINS` (som ERP), ikke i en manglende cron;
  `tests/unit/test_dqa_scheduling.py` vogter, at hvert DSO-domæne undtagen `fhir` har en.

## 9. DQA-subjects (US-6, præfiks `udredningsret_`, alle `custom_sql`/inline, kun tællinger — aldrig CPR)

Implementeret i `dqa/config/udredningsret_subjects.py` og registreret i `dqa/config/__init__.py`'s `ALL_SUBJECTS`.
Ti subjects, tolv checks (subject 1 og 8 har to hver). Alle ligger på `Track.DWH` med `source=None`, så de lander i
`dqa_dwh_daily_job`/`dqa_dwh_weekly_job` sammen med de to `_FACTS`-subjects for de samme tabeller — HAIBA's source-split
ville her sprede tjekkene på `fct.Udredningsforloeb` over to jobs uden gevinst.

| # | Subject (`udredningsret_`…) | Definition som implementeret | Sværhedsgrad | Målt 2026-09-07 |
|---|---|---|---|---|
| 1 | `dobbeltregistrerede_markoerer` | `fct.UdredningsretMarkoer` levende med `antal_kilderaekker > 1`. **To checks:** `…_raekker` (antal fact-rækker, der står for mere end én kilderække) og `…_reducerede_kilderaekker` (`SUM(antal_kilderaekker) - COUNT(*)`, castet til BIGINT) — spec'ens definition og dens målte tal er to forskellige tal | INFO | 1.705 kilderækker reduceret |
| 2 | `markoernoegle_dubletter` | `arc.udredningsret_mont` levende med `loebenummer > 1`. Tælles på arkivet, ikke på facten: facten er 1:1 med arkivrækken, så egenskaben hører hjemme i arkivet | INFO | 10.531 |
| 3 | `flere_endeligt_udredt` | markører i gruppen `endeligt_udredt` pr. forløb > 1, `dw_ek_Udredningsforloeb <> -1`. Subject-tabellen er forløbsfacten, selv om evidensen læses på markørfacten, så raten læses som andel af 1,9 M forløb og ikke af 8,2 M markører | INFO | 31.057 |
| 4 | `afslutning_og_afbrydelse` | `endeligt_udredt` og `afbrudt` i samme forløb (`MAX(CASE …)` på begge), `dw_ek_Udredningsforloeb <> -1` | INFO | 43.730 |
| 5 | `lang_udredningstid` | `fct.Udredningsforloeb.udredningstid_dage > 730` på levende rækker. Nævneren er kun rækker med `udredningstid_dage IS NOT NULL` (dvs. `indgaar_i_indikator`, U2), så raten følger udfaldene og ikke andelen af åbne forløb | INFO | 4.788 |
| 6 | `aendret_indikatorvaerdi` | levende fact-rækker med `dw_timestamp_update > dw_timestamp_insert` **og** `dw_timestamp_update >= MAX(leverance_tidsstempel)` fra `meta.UdredningsretLeverance`. **Definitionen er ændret for at kunne udtrykkes** — se noten nedenfor | INFO | ~5.000/md forventet |
| 7 | `soft_slettede_forloeb` | `fct.Udredningsforloeb` med `dw_is_deleted = 1`; nævneren er **hele** facten (levende + retirerede), så tallet er en andel af facten og ikke af det, der overlevede | INFO | 0 (første leverance) |
| 8 | `sor_uden_opslag` | `dw_ek_Organisation_* = -1` samtidig med at den rå kode i `dim.UdredningsretEgenskaber` er ikke-blank (blank står lovligt på -1 og tælles ikke). **To checks**, ét pr. rolle (`forloeb_enhed_sor`, `kontakt_enhed_sor`), fordi rollerne fejler af hver sin grund | WARN | 0 og 0 |
| 9 | `skiftet_foerste_markoer` | helbredsforløb med en **retireret** fact-række, hvor der findes en levende række under et **andet** `dw_ek_Helbredsforloeb` med samme `dw_ek_Borger` og samme `foerste_forloebselement_uuid`. **Præciseret** — se noten nedenfor | INFO | 0 (første leverance) |
| 10 | `forloeb_pr_helbredsforloeb` | `FULL OUTER JOIN` på `clean_code_Helbredsforloeb` mellem arkivets `(helbredsforloeb_noegle, ikke-blankt F_ID)`-grupper og levende fact-rækker opløst gennem `map.Helbredsforloeb`; tæller helbredsforløb, hvor antallet afviger. Den årsagsuafhængige bagstopper for U13's net-globale kontroltotal og for U15 | ERROR | forventet 0 |

**Kadence.** Alle tolv checks er `Cadence.DAILY`. Tabellerne ændrer sig kun, når `udredningsret_publish_job` kører, og
det er leverancedrevet (sensor-anmodet, §8) — "dagligt" betyder her reelt "efter hver publicering", som er datas egen
kadence. En WEEKLY-tier ville være værre end ingenting: `dqa_dwh_weekly_job` har ingen cron, så et ugentligt check ville
kun køre, hvis en publicering tilfældigt landede på en lørdag. De tunge subjects (3, 4, 9, 10 scanner 8,2 M-populationen)
betaler derfor prisen én gang pr. leverance, ikke én gang om dagen.

Den pris er dog **betinget**: `dqa_dwh_daily_job` er delt, og `dqa_erp_daily_after_dfm` anmoder det samme job efter det
**natlige** `erp_daily_dfm_job`. "Én gang pr. leverance" gælder altså, så længe den ERP-sensor er `STOPPED` (hvilket den
er i dag, som `dqa_drg_after_publish` og `dqa_udredningsret_after_publish`). Tændes den, scanner subject 3, 4, 9 og 10
8,2 M rækker hver nat, uden at nogen har rørt udredningsret. Bliver det en reel omkostning, er svaret et eget
source-split som HAIBA's (`dqa_dwh_haiba_daily_job`) — ikke en WEEKLY-tier, som stadig ville mangle sin cron.

**Sværhedsgrad — tre tiers, fordi tallene ikke betyder det samme.** INFO (1–7 og 9) er egenskaber ved SDS' egne data, som
forventes at være ikke-nul og endnu ikke har en aftalt baseline; INFO består altid, så tallet trendes i `dqa.Results` i
stedet for at alarmere ved hver publicering. De løftes til WARN med en cap, efterhånden som deres baselines er læst af
i `dqa.Results` — samme mekanik som `_SENTINEL_MAX_RATES` bruger for facterne. WARN (8) er en opslagsregression, ikke en kildeegenskab: 0 ud af 738 og 0 ud af 1.360
er målt (§5), så ethvert tal er nyt. ERROR (10) betyder, at facten ikke reproducerer arkivet — en modelfejl. Ingen af
dem er BLOCKING: DQA afdækker, den gater aldrig ETL'en (factens egen kontroltotal fejler allerede kørslen på totalen).

**Kun tællinger, aldrig eksempler.** Intet check har `examples_sql`. Det er ikke kun GDPR-hygiejne: domænets egen
forretningsnøgle (`clean_code_Helbredsforloeb` = `UDREDHF_` + SHA2_256 over CPR og første markør) er et **stabilt
pseudonym**, så en eksempelnøgle i `dqa.Results` ville lægge en personhenførbar værdi i en tabel, der læses på tværs af
miljøer. Hvert check rapporterer et antal og en nævner (`checked_sql`) og intet andet; den, der skal bruge de konkrete
nøgler til triage, kører SQL'en inde i warehouset.

**Subject 6 — definitionen er ændret for at kunne udtrykkes.** Spec'ens oprindelige formulering var "levende fact-rækker,
hvor klassifikationen eller `udredningstid_dage` afviger". Det kan facten ikke svare på: den er Type 1, så den forrige
værdi er væk i samme øjeblik MERGE'n har skrevet den nye. Til gengæld rører load'ens MERGE kun en række, når dens
`EXCEPT`-ændringsdetektion faktisk finder en forskel, så en **opdateret** række *er* en række, hvis værdier ændrede sig i
denne leverance. Checket tæller derfor `dw_timestamp_update > dw_timestamp_insert` inden for den nyeste registrerede
leveranceperiode. Det er bevidst en **overmængde** af "indikatoren ændrede sig": en ændret organisation eller
aktionsdiagnose tælles med. Er leveranceregistret tomt, bliver grænsen `9999-12-31`, og vinduet er tomt — en bevidst
fail-safe retning, for uden den ville et manglende register gøre checket til "hver række, der nogensinde er opdateret".

**Subject 9 — præciseret.** Spec'ens "samme borger og forløbselement" er implementeret som `dw_ek_Borger > 0` plus
ikke-blank `foerste_forloebselement_uuid`. Kravet om et **rigtigt** borgermedlem er nødvendigt: -1 (CPR endnu ikke i
`dim.Borger`) og -2 (strukturelt fraværende CPR) deles af tusindvis af urelaterede rækker, så et match på dem ville
fabrikere efterfølgere. Et helbredsforløb tæller med, hvis **mindst ét** af dets retirerede forløb har en efterfølger —
bevidst bredere end "alle dets forløb blev retireret", fordi en delvis omgruppering er det samme signal.

**Subject 10 — detaljer.** Afstemningen er pr. nøgle på **antallet** af forløb, så udligning på tværs af helbredsforløb
er umulig — men en omgruppering, der bevarer gruppeantallet inden for samme helbredsforløb (en markør flytter fra `F_ID`
A til B, og begge grupper overlever), er stadig U15-detektorens område alene. Verificeret på SQL Server med checkets
egne udtryk: split + merge i samme vindue giver grøn kontroltotal og subject 10 = 2, mens et rent medlemskabsbytte giver
grøn kontroltotal og subject 10 = 0. `subject 10 = 0` betyder altså "ingen nøgle har forkert **antal** forløb", ikke
"facten reproducerer arkivet gruppe for gruppe".
Nøglen dannes på begge sider med staging-kædens egen builder (`clean_code_helbredsforloeb_sql`),
så præfikset aldrig staves om her. En fact-række på -1-sentinellen joiner map'ens `UNKNOWN`-medlem, som arkivsiden aldrig
producerer, og rapporteres dermed som en forskel — hvilket er præcis rigtigt, eftersom load'ens UNMAPPED-guard betyder, at
det slet ikke bør kunne ske. Blankt `F_ID` er ekskluderet på arkivsiden (U5: de markører hører til intet forløb, og facten
har ingen række for dem). Nævneren er **alle** levende helbredsforløb i arkivet (2.233.064 målt), hvilket er bredere end
den sammenlignede population med netop de nøgler, hvis markører alle mangler `F_ID` — det gør nævneren til en stabil
reference frem for et tal, der flytter sig med overtrædelsesmængden.

Profileringens 12.236 "nøgledubletter" og 31.110 "flere afslutninger" talte rækker hhv. med en bredere kodegruppe; DQA'ens
definitioner ovenfor er dem, der kan udtrykkes på modellens tabeller, og tallene er de målte. Gruppenavnene i checkene
(`endeligt_udredt`, `afbrudt`) valideres mod stagingens egen `MARKOER_GRUPPER` ved import, så en omdøbning dér fejler med
det samme i stedet for at efterlade checks, der tæller nul for evigt.

**`_FACTS`-posten fra US-5: `("dwh_udredningsretmarkoer", "fct.UdredningsretMarkoer")`.** Ud over tabellen ovenfor registrerer
US-5 markørfacten i `dqa/config/dwh_subjects.py` ved siden af `dwh_udredningsforloeb` (§8). Begge checkfamilier udledes af
lineage-YAML'en: daglig `sentinel_rate` over factens seks FK-roller og ugentlig `referential_integrity` for de fem, der har et
`FK-Target` — `dw_ek_Helbredsforloeb` er `FK-None` som på forløbsfacten og får kun sentinel-raten, mens
`dw_ek_Udredningsforloeb` peger på forældrefacten `fct.Udredningsforloeb` (DQA udleder forældrenøglen af målets navn).
`_SENTINEL_MAX_RATES` får `"fct.UdredningsretMarkoer": {"dw_ek_Udredningsforloeb": 0.20}`: markører uden F_ID står lovligt på
-1 (U5), den målte andel er 0,143 på leverancen 2026-09-07, og cappen er sat med luft, så checket overvåger andelen frem for
eksistensen. Cappen løfter samtidig hele factens `sentinel_rate` fra INFO til WARN. `dw_ek_Forloebsmarkoer` bærer også en lille
lovlig -1-andel (blank markørkode), men får bevidst ingen cap: dictet capper pr. kolonne, så rollen forbliver ren observation.

## 10. Beslutninger truffet under implementeringen (ud over B1–B10)

Ført efter [beslutningslog_principper.md](../../contracts/beslutningslog_principper.md).

**U1. `F_ID` og `PK` holdes aktuelle i arkivet med en smal opdatering efter mergen — uden at tælle som ændring.**
Valgt: `udredningsret_archive` kører efter `execute_archive_merge` én `UPDATE` af `PK`/`F_ID` på levende rækker, hvor værdien afviger
fra leverancens (`refresh_surrogate_ids`), uden at røre `dw_timestamp_update` eller `leverance_dato`; springes over ved `backfill_historical`.
Alternativ: (a) lade `F_ID` være forældet på uændrede rækker (status før US-3); (b) tage `F_ID` med i change-detect igen.
Begrundelse: staging grupperer markører til udredningsforløb på `F_ID` **inden for leverancen** (B3/B4). Med (a) ville et
helbredsforløb, hvis `F_ID` er omnummereret (31 %/md), stå med gamle `F_ID` på uændrede rækker og nye på ændrede — samme forløb
splittet i to grupper, dvs. fantomforløb med forkert startdato og splittede indikatormarkører. (b) ville genindføre 1,5 M
tidsstempel-opdateringer om ugen og re-stage hele arkivet downstream (beslutning 2026-09-08 imod). U1 bevarer beslutningens
hensigt (renummerering er ingen ændring) og gør staging korrekt.
Aftager: `dim.UdredningsretEgenskaber.f_id`/`pk` er "senest leverede" for forløb, hvis markører er blevet re-staget; et forløb uden
nogen indholdsændring beholder den `F_ID`, det havde ved sidste staging (kun en egenskab — nøglen er `dw_ek_Udredningsforloeb`).
Status: Fastlagt 2026-09-12.
Kendt begrænsning (review 2026-09-12, lukket 2026-09-13 med U15): en **omgruppering** af markører mellem F_ID'er uden anden
indholdsændring (SDS splitter eller samler udredningsforløb, eller en markørs F_ID bliver blank) skrives af U1 til arc uden
tidsstempel, og helbredsforløbet kommer derfor ikke i delta-scope igen af sig selv. DQA-subject 6 og 9 fanger **ikke** det
tilfælde — subject 6 tæller kun fact-rækker, der allerede ER re-staget (`dw_timestamp_update > dw_timestamp_insert`), og subject 9
tæller helbredsforløb, hvis **første markør** skiftede; en ren omgruppering rører ingen af delene. Antagelsen holdes derfor af
omgrupperingsdetektoren i arkivet (**U15**), som bumper `dw_timestamp_update` på netop de helbredsforløb, hvis F_ID-gruppering
ikke er en bijektion — eller hvor en markør krydser blank-grænsen og dermed forlader eller tilslutter sig et forløb — og af
kontroltotalen (U13) som sidste værn — en tripwire for hele populationen, ikke en afstemning pr.
helbredsforløb (se U13). En `mode=full_load`-publish genopretter altid. Dokumenteret i `udredningsret_kilde.md` og i
builder-docstringen.

**U2. Slutdato er slutmarkørens dato; åbne og afbrudte forløb får BLANK-sentinel.** Valgt: `dw_ek_Dato_slut` = datoen for
`MRK_H='1'`-slutmarkøren, ellers seneste `endeligt_udredt`-markør, ellers BLANK (99999998). `seneste_markoer_dato` bæres som egenskab.
Alternativ: T3.1's "ellers seneste markør" som slutdato. Begrundelse: T3.2's acceptkriterium og review-siden kræver BLANK for åbne
forløb, og "seneste markør" er ikke en afslutning. Aftager: udredningstid må kun regnes på `indgaar_i_indikator = 1`
(`udredningstid_dage` er NULL ellers). Status: Fastlagt 2026-09-12.

**U3. Attributter læses fra slutmarkøren, ellers forløbets seneste markør.** Målt: 167.904 forløb har mindst én attribut, der
varierer mellem markørerne (enhed, label eller bopælsregion) — én regel, ét svar. Status: Fastlagt (T3.1).

**U4. Startdato uden henvist-markør = forløbets tidligste markør** (142.461 forløb, T3.1). 184.003 forløb har flere henvist-markører;
den tidligste vinder. Status: Fastlagt.

**U5. Markører uden F_ID peger på sentinel -1, ikke -2.** Begrundelse: repoets map-baserede dimensioner har kun -1-seeds, og
DQA's referentielle tjek ignorerer kun -1; -2 ville kræve et ekstra seedmedlem i `dim.UdredningsretEgenskaber`. Andelen (14 %)
er design, og DQA-cappen for rollen sættes med luft (0,20). Status: Fastlagt.

**U6. Tællerne `antal_markoerer/antal_udredningsplaner/antal_afbrydelser` dannes i pre_stg fra arkivet, ikke fra markørfacten.**
Samme rækker (arkivet efter dubletreduktion), ingen afhængighed fra forløbsfacten til markørfacten, og US-5 verificerer i stedet, at
`SUM(antal_markoerer)` = markørfactens rækker med forløb. Status: Fastlagt.

**U7. `dim.Forloebsmarkoer`'s tekster kommer fra `codesystem.SKSConcept` (Å3 afklaret).** Alle 433 koder findes på dev med
tekst; ukendte koder mintes med tekst 'Ukendt' og `tekst_kilde = 'ukendt'`. Status: Fastlagt.

**U8. `aarsag_tekst` er vores formulering, afledt af SKS-teksterne og målt sammenhæng (§7)** — erstattes af SDS' ordlyd, når PDF'en er
verificeret. Status: Foreløbig.

**U9. Borger-genopretning i delta-scope (§3.1)** frem for fuld genkørsel: helbredsforløb med `dw_ek_Borger = -1` i facten re-stages hver
kørsel, indtil natkæden har mintet borgeren. Status: Fastlagt.

**U11. En downstream-ejer uden checkpoint tvinger pre_stg til fuld load.** Valgt: `pre_stg_Helbredsforloeb` sætter
checkpoint = `None`, hvis blot én af de fem ejere (§3.1) mangler checkpoint; ellers MIN. Alternativ: DRG's regel (spring `None` over,
MIN over resten). Begrundelse: efter en delvis fejl (fx markørfacten fejler på første fulde kørsel) ville DRG-reglen krympe
delta-vinduet til de øvrige ejeres checkpoint, så den haltende ejer aldrig fik sine rækker, og dens kontroltotal mod hele arkivet
ville fejle ved hver kørsel, indtil nogen kørte `full_load` manuelt. Én fuld load efter en delvis fejl er den billigere fejltilstand;
i normal drift har alle ejere checkpoint, og reglen koster intet. Aftager: ingen. Status: Fastlagt 2026-09-12 (review-fund).

**U10. Fælles opslags-SQL (`DSA/SQL/common/dimension_lookup_temp_tables.py`).** `create_borger_cpr_lookup_tables_sql`/`create_organisation_sor_lookup_table_sql` flyttes uændret fra
`drg_staging_sql.py` (som re-eksporterer dem), så DRG og udredningsret deler ét udtryk (review-reglen om delte udtryk i `DSA/SQL/common`).
Status: Fastlagt.

**U12. Klassifikationens rå attributter læses fra `stg.pre_Udredningsforloeb`, ikke fra `stg.dim_UdredningsretEgenskaber`.**
Valgt: `dim_UdredningsretKlassifikation` grupperer på den stagede `clean_code_UdredningsretKlassifikation` og henter de otte
værdier med `MAX()` fra `stg.pre_Udredningsforloeb`, joinet på det `clean_code_Udredningsforloeb`, de to stagingtabeller deler.
Alternativ: udvide `EGENSKABER_ATTRIBUTTER` med de seks indikatorflag, så alt kunne læses fra egenskabstabellen alene.
Begrundelse: kun `forloebslabel` og `speciale` af de otte ligger på `stg.dim_UdredningsretEgenskaber`. At lægge de øvrige seks
derind ville duplikere indikatorværdierne i transaktionsdimensionen — samme værdi to steder og dermed to sandheder ved en
rettelse — og kræve en ændring af US-3-staging, der allerede er reviewet og målt. Join'et er 1:1 på samme grain, og hashen
garanterer, at værdierne er konstante inden for gruppen, så `MAX()` ikke opfinder en orden (samme idiom som DRG).
Aftager: dimensionen har en dep på `pre_stg_Udredningsforloeb` ud over `stg_dim_UdredningsretEgenskaber` og `cs_sks`; en
fremtidig ændring af de otte attributter skal rettes ét sted (`KLASSIFIKATION_ATTRIBUTTER`), men i to stagingtabeller.
Status: Fastlagt 2026-09-12.

**U13. Kontroltotalens grain er `(helbredsforloeb_noegle, F_ID)`-par, ikke `COUNT(DISTINCT F_ID)`.**
Valgt: `fct_Udredningsforloeb` sammenligner levende fact-rækker med antallet af `(helbredsforloeb_noegle, F_ID)`-grupper over
levende arc-rækker med ikke-blank `F_ID`, og `SUM(indgaar_i_indikator)` med antallet af de grupper, der har både en henvist- og
en ikke-henvist markør med `MRK_H='1'`. Alternativ: §4.3's oprindelige formulering `COUNT(DISTINCT F_ID)`.
Begrundelse: `F_ID` er kun unik inden for helbredsforløbet — SDS omnummererer den pr. leverance (U1) — så et `F_ID`, der optræder
under to helbredsforløb, ville tælle som ét forløb i arkivet og to i facten. Kørslen ville da fejle på en korrekt fact.
Par-grainen er præcis den, `stg.pre_Udredningsforloeb` danner (§3.3), så de to tal er definitorisk sammenlignelige.
Aftager: kontroltotalen kører uændret på tværs af delta-batches, fordi den måler hele facten mod hele arkivet — men den
sammenligner to **skalarer**: ét net-globalt tal for facten mod ét for arkivet. En splitning og en sammenlægning i samme vindue
ophæver derfor hinanden, og kørslen bliver **grøn med begge helbredsforløb forkerte**. Kontroltotalen er en tripwire for
populationen som helhed, ikke en afstemning pr. helbredsforløb; en ægte afstemning kræver et `FULL OUTER JOIN` på
`helbredsforloeb_noegle` mellem facten og arkivets `(helbredsforloeb_noegle, F_ID)`-grupper og er rejst som DQA-subject 10 i US-6
(§9). Afviger totalen, er den hyppigste årsag en omgruppering (U15), og afhjælpningen er `mode=full_load`.
Status: Fastlagt 2026-09-12; Aftager præciseret 2026-09-13 (review).

**U14. Markørfacten har ingen retireringsrutine.** Valgt: `fct_UdredningsretMarkoer` retirerer og genopliver udelukkende gennem
den stagede `dw_is_deleted`, som MERGE'ns `WHEN MATCHED` skriver fra kilden; der findes ingen `UPDATE`, der sætter
`dw_is_deleted = 1` uden for MERGE'n. Alternativ: kopiere forløbsfactens retireringstrin (§4.3), som sætter `dw_is_deleted = 1`
på levende fact-rækker, der ikke findes i den stagede batch, inden for kørslens scope.
Begrundelse: et udredningsforløb er en **gruppering**, der kan forsvinde, uden at nogen arkivrække forsvinder (markørerne
omgrupperes under en anden start), og derfor skal forløbsfacten retirere det, der ikke længere stages. En markør har ikke den
frihed: hver markørrække findes 1:1 i `arc.udredningsret_mont` via arkivets egen mergenøgle (§2), staging bærer de
soft-slettede rækker med i sit scope (§3.5), og arkivet hard-sletter aldrig. Et retireringstrin ville derfor enten være dødt
kode eller — hvis det tog scopet fra helbredsforløbene som forløbsfacten — retirere markører, som leverancen stadig sender,
i det øjeblik et delta-vindue ikke indeholdt dem.
Aftager: kontroltotalen (§4.3) er det, der fanger en tabt markør, ikke et retireringstrin; en unit-test holder invarianten ved
at afvise enhver `UPDATE`, der sætter `dw_is_deleted = 1` uden for MERGE'n.
Status: Fastlagt 2026-09-13.

**U15. Omgruppering af `F_ID` opdages i arkivet og bumper `dw_timestamp_update` — omnummerering gør stadig ikke.**
Valgt: `refresh_surrogate_ids` kører tre trin på én cursor og ét commit, i denne rækkefølge: (1) detektion, (2) U1's `PK`/`F_ID`-opdatering,
(3) bump. Detektionen sammenligner pr. helbredsforløb arkivets gamle `F_ID`-gruppering med leverancens (`#omgruppering`,
`build_regroup_detection_sql`) og markerer de helbredsforløb, hvor omdøbningen **ikke** er en bijektion på grupper: er R mængden af
`(gammel, ny)`-par for helbredsforløbets matchede markører, er `|R| = |gammel| = |ny|` præcis "1:1 pr. gruppe", og alt andet er en
omgruppering — splitning, sammenlægning eller en markør, der skifter gruppe. **Blank er ikke en gruppe, men en grænse, og kræver
sit eget led:** `'#BLANK'` er en etiket på begge sider, men `stg.pre_Udredningsforloeb` danner kun forløb pr.
`(helbredsforløb, ikke-blankt F_ID)`, og kontroltotalen (U13) tæller netop de grupper. En markør, der krydser blank-grænsen,
tilføjer eller fjerner derfor en hel gruppe i arkivet, *mens etiketteringen stadig er en bijektion* — en hel gruppe, der bliver
blank, giver `|R| = |gammel| = |ny|`, og det samme gør en blank markør, der får et `F_ID` (målt på SQL Server med detektorens egne
udtryk). Derfor markerer et tredje `HAVING`-led ethvert helbredsforløb med et par, der er blankt på præcis **én** side. Det giver
ingen nye falske positive: et sådant par kan kun opstå på rækker, mergen ikke rørte, og det ændrer altid gruppepopulationen. Med
1,17 M markører (14 %), der hører til intet forløb, er den drift ind og ud af medlemskab helt almindelig her.
De markerede helbredsforløb får bagefter `dw_timestamp_update` bumpet (`build_regroup_bump_sql`) — aldrig
`leverance_dato`, som fortsat betyder "den leverance, rækken sidst ændrede sig i". Rækker, som mergen selv har ændret i **denne
kørsel**, prunes fra detektionen, så et halvt opdateret helbredsforløb ikke ligner en omgruppering; målet er
derfor "helbredsforløb, der mangler et bump", ikke "helbredsforløb, der er omgrupperet". Grænsen for prunet er serverens eget ur
læst **før** mergen (`read_merge_start`), ikke leverancedatoen: intet forbyder to leverancer pakket samme dato (retningsguarden
afviser kun en strengt ældre, og DFM-identiteten er pakketidspunktet på sekundet), og et dato-prune ville så også fjerne rækker,
som en *tidligere* kørsel ændrede — og dermed evidensen for en omgruppering, der aldrig ville blive bumpet. Mergen og bumpet
stempler begge `CAST(SYSDATETIME() AS datetime2(0))`, så hver række, kørslen selv skrev, ligger på eller efter grænsen. Bumpet har ingen grænse for antallet. Springes over
ved `backfill_historical` som resten af U1. Antallet rapporteres som `omgrupperinger` i assetets metadata.
Alternativ: (a) tage `F_ID` med i change-detect igen (beslutning 2026-09-08 imod: ~1,5 M tidsstempel-opdateringer om ugen
og re-staging af hele arkivet hver uge); (b) en tredje gren i delta-scopet (`_scope_sql`), der sammenligner gruppeantal ved hver
publish-kørsel; (c) snævre kontroltotalen ind eller nedgradere den til en advarsel.
Begrundelse: rækkefølgen er hele pointen — detektionen SKAL læse arc før id-opdateringen, for bagefter er `arc.F_ID` lig
leverancens, og nøgletabellen droppes kort efter; det er det eneste øjeblik, hvor begge grupperinger findes. Uden bumpet kommer
helbredsforløbet aldrig i delta-scope igen (hverken tidsstempel-grenen eller U9's borger-gren), staging beholder den gamle
gruppering, og U13's kontroltotal fejler hele natkæden kørsel efter kørsel, fordi fejlen rejses **før** `set_checkpoint` — ét
helbredsforløb ud af 2,23 M stopper dims, markørfact, DQA og DSO-sync, mens ingest kører videre. (b) er dyrt ved hver kørsel og
overser netop medlemskabsbyttet, som ethvert tælle-baseret tjek misser; (c) ville ødelægge det eneste hele-fact-mod-hele-arkiv-tjek.
Detektionen er et `HAVING` med to `DISTINCT` og et `MAX(CASE ...)` over ca. én række pr. gruppe (kilden er allerede `DISTINCT` på `(hf, gammel, ny)`) —
ikke `COUNT(DISTINCT)` over 8,2 M rækker — og bumpet seeker det eksisterende nonclustered index `(helbredsforloeb_noegle, dw_is_deleted)`.
Aftager: et omgrupperet helbredsforløb re-stages ved næste publish-kørsel i stedet for at stå med gammel grain, indtil nogen kørte
`full_load`. Detektoren er årsagsspecifik; den cause-agnostiske bagstopper er DQA-subject 10 (§9, US-6).
Status: Fastlagt 2026-09-13 (review-fund, verificeret ende til ende).

**U19. Dette repo bygger ingen views over udredningsretten; præsentationslaget ejes af security-repoet.**
Valgt: hverken `vw_dim.*` eller `vw_fct.*`. `dso_infra_udredningsret` opretter kun tabellerne på DSO, og der findes
ingen `views/udredningsret/`-mappe; en unit-test håndhæver fraværet.
Alternativ: (a) pass-through-views for de tre dimensioner og begge facter, som T6.1 beskriver (valget indtil
2026-09-14); (b) kun dimensionsviews, som DRG har.
Begrundelse: beslutning 2026-09-14 efter review. `fct.Udredningsforloeb` er personhenførbar — den bærer
`dw_ek_Borger` og `alder_ved_henvisning` — og `fct.DRGKontakt` har af præcis den grund bevidst intet view i dette repo
(`tests/unit/test_drg_dso_sync.py` forbyder det aktivt). Egenskabsdimensionen deler PK med facten og er lige så tæt på
personen. Frem for at udstille to af fem objekter usikret og lade resten vente på security-repoet, ejer det repo hele
fladen for dette domæne.
Aftager: en aftager uden DPA-view læser basistabellen på DSO og skal selv filtrere `dw_is_deleted = 0` — begge facter
akkumulerer retirerede rækker (§6), så en ufiltreret `SELECT` over-tæller mere for hver leverance og fejler stille i
retningen "flere forløb, længere udredningstider". Det er den ene ting, et view ellers ville have lukket, og det står
derfor her og i §8.1. Revisionssporet er uændret læsbart på `fct.*`.
Status: Fastlagt 2026-09-14 (erstatter U16, som handlede om fact-viewenes filtrering).


**U18. `clean_code_Udredningsforloeb` synkroniseres ikke til DSO.**
Valgt: `exclude_columns=["clean_code_Udredningsforloeb"]` på både `fct_Udredningsforloeb` og
`dim_UdredningsretEgenskaber` i `UDREDNINGSRET_SYNC_CONFIGS`. Kolonnen findes fortsat på DSO-tabellen (samme DDL som
DSA, og `validate_table_schemas_match` kræver identiske skemaer), men står NULL; `dw_ek_Udredningsforloeb` er
join-nøglen dér.
Alternativ: (a) tage kolonnen med og skrive en persondatavurdering, der accepterer eksponeringen; (b) gøre clean_code'en
ikke-genberegnelig ved at hashe hele tuplen (`HASHBYTES` over nøgle + startdato + løbenummer) i stedet for at
konkatenere hf-digestet ind.
Begrundelse: `clean_code_Udredningsforloeb` = `'UDREDFORL_' + helbredsforloeb_noegle + '_' + startdato + '_' +
loebenummer` (§2), og `helbredsforloeb_noegle` er arkivets `SHA2_256` over CPR. De 64 hex-tegn står altså i klartekst i
kolonnen — et **genberegneligt, stabilt pseudonym** over CPR, ikke en surrogatnøgle. Det er præcis den kategori, §9
holder ude af `dqa.Results` ("en eksempelnøgle ... ville lægge en personhenførbar værdi i en tabel, der læses på tværs
af miljøer"), og rapporteringsdatabasen har langt flere læsere end `dqa.Results`. Forskellen fra `dw_ek_Borger` er
afgørende: surrogatet kan kun opløses gennem det DPA-sikrede `dim.Borger`, mens clean_code'en kan genberegnes af
enhver, der kender hash-inputtene. DRG's søskende har ikke problemet: `clean_code_DRGKontakt` bygges af kildens
kontakt-UUID, ikke af CPR — og det gør `clean_code_UdredningsretMarkoer` heller ikke, fordi `markoer_noegle` er en **ny**
hash AF helbredsforløbsnøglen. (a) ville kræve en persondatavurdering afklaret med ejeren af security-repoet, og indtil
den findes, er den billige og reversible vej at lade være med at eksponere værdien. (b) ville ændre nøglen i
`map.Udredningsforloeb` og dermed omminte hver eneste `dw_ek_Udredningsforloeb` — en langt større indgriben i en
allerede reviewet og målt US-3/US-4-kæde.
Aftager: DSO har ingen læsbar forretningsnøgle på forløbet. En aftager, der skal binde en DSO-række tilbage til DSA,
bruger `dw_ek_Udredningsforloeb`, som er stabil på tværs af leverancer. `dso_reconciliation` er upåvirket: den
sammenligner PK-mængder og sletteflag, ikke kolonneværdier, og eksempelnøglerne er selve PK'en. Det unikke filtrerede
indeks på kolonnen står tomt på DSO (`WHERE ... IS NOT NULL` matcher ingen rækker), men bliver stående, fordi
DDL-filerne er fælles for DSA og DSO. Omgøres beslutningen, kræver det (a)'s persondatavurdering.
Status: Fastlagt 2026-09-14 (review-fund på MR !94).

**U17. Begge facter slår måltabellens indekser fra under sync — ikke kun markørfacten.**
Valgt: `fct.UdredningsretMarkoer` **og** `fct.Udredningsforloeb` får `chunk_size=20_000` og
`disable_target_indexes_during_sync=True` i `UDREDNINGSRET_SYNC_CONFIGS`.
Alternativ: kun markørfacten, og lade forløbsfacten køre med indekser intakt.
Begrundelse: flaget slår kun til på **bulk**-stien (`truncate_insert` eller et tomt mål), hvilket her betyder den første
indlæsning i den tabel, `dso_infra_udredningsret` opretter og aldrig seeder. Det natlige watermark-delta lander på et
befolket mål og tager MERGE-stien, hvor indekserne står urørt — at genopbygge hvert indeks over hele tabellen for at
optage et lille delta ville koste langt mere end vedligehold pr. række. Markørfacten er 8,2 M rækker med syv
nonclustered indekser oven på den klyngede PK (~57 M tilfældige indeksskrivninger ved første load) og er dermed
`fct.DRGKontakt`'s tilfælde direkte. Forløbsfacten er 1,9 M rækker med otte indekser (~15 M skrivninger) — en
størrelsesorden fra DRG's 35,6 M × 11, som hang på `PAGEIOLATCH_EX` i timevis på dev, men det er samme fejlform, flaget
er inert ved hver kørsel efter den første, og én regel for begge facter slår to regler for samme problem.
Aftager: ingen. En unit-test binder flaget til DDL'ens faktiske indeksantal, så en tilføjet eller fjernet indeks ikke
kan gøre begrundelsen forældet i stilhed.
Status: Fastlagt 2026-09-14. Endnu ikke målt på dev — første fulde DSO-load verificerer den.


### Review: F_ID og genforsøg

`F_ID` bevares som `NVARCHAR(500)` fra arkiv til staging og egenskabsdimension på DSA/DSO.
Migrationen `20260917_udredningsret_f_id_width.sql` udvider eksisterende dimensioner uden at
ændre værdier eller nøgler. Kør de normale staging-/DSO-migrationer før publicering af denne version.
Det sammensatte opslag på helbredsforløb og F_ID er nonclustered, så også 500 tegn holder sig
inden for SQL Servers indeksgrænse på 1.700 byte.

Borger-plateau er ikke et sikkert stopkriterium for
fejlede publish-kørsler: DSA kan allerede have committet borgernøgler, mens DSO-sync stadig mangler.
Fejlede publish-kørsler forbliver derfor genprøvelige ved næste natlige succes.

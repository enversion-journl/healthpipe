<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# SDS Udredningsret (LPR_Udredningsret) — kildebeskrivelse og nøglebeslutning

Feature SDSUDRED, ingestion og leverancedetektion.
Ingestion: `etl/DSI/csv_files/udredningsret*.py` (tar-streamingen deles med DRG og LPR3 via
`etl/DSI/csv_files/_sds_tar.py`), DDL: `etl/infrastructure/ddl/tables/udredningsret/`,
job: `udredningsret_ingest_job`, schedule: `udredningsret_ingest_daily_schedule`.

Tallene nedenfor er målt på healthpipe-dev 2026-09-07/09 mod leverancerne 2026-08-31 og 2026-09-07
(detaljer og tællinger som kommentarer).

## Leverancen

| | |
|---|---|
| Kilde | SDS' datamart "Monitorering af udredningsretten" (LPR_Udredningsret), region 1083 |
| Placering | `UDREDNINGSRET_BASE_DIR` (`/mnt/ftproot/Udredningsret`, read-only CIFS-mount injiceret pr. allokering). Ingen default i kode og ingen værdi i `.env.template`: manglende variabel er hård fejl |
| Filnavn | `Region_1083_LPR_Udredningsret_<U|M>_<yyyyMMddHHmmss>.tar.bz2` |
| Kadence | `U` hver mandag aften, `M` en gang om måneden. Samme spor pakket på forskellige tidspunkter, så den nyeste brugbare fil er autoritativ uanset kadence (B6/B7) |
| Retention på sharet | ~3 måneder. Ældre `Arkiv/`-formater (2021–22) matches ikke |
| Indhold | Ét FULDT snapshot fra 2018-07. Delta-semantik findes ikke i kilden |
| Medlemmer i tar | `Metadata_UdredningsretMont_1083_<dato>.csv` (SDS' kolonneskema), `SDP_ExportLog_<dato>.csv` (rækkeantal pr. fil), `UdredningsretMont_1083_<dato>.csv` (data), `UdredningsretMont_LEVERANCEINFO_1083_<dato>.csv` (`DATAOMRAADE`, `LPROPDATERINGSDATO`) — i den rækkefølge |
| Encoding / separator | cp1252 (`UDREDNINGSRET_FILE_ENCODING`), `;` |
| Volumen | ~8,17 M rækker, ~2,5 GB udpakket; 2,23 M helbredsforløb |
| Eksportlog | Kontroltotal pr. fil. Hård fejl ved afvigelse (extract) |
| Datoformat | `FORLOEBSMARKOER_DATO` og `LPROPDATERINGSDATO` SKAL være ISO `YYYY-MM-DD`; extracten afviser leverancen ellers (se "Datoer" nedenfor) |

## Datasæt `UdredningsretMont` (28 kolonner, uændret i alle 14 leverancer 2026-06-08 → 2026-09-07)

Kildens kolonneorden og stavning bevares 1:1 i `ext.udredningsret_mont` og `arc.udredningsret_mont`
(`DW_EK_HelbredsForloeb`, `OBJEKTID_FORLOBELEMENT`, `OBJEKTID_FORLOBSMARKOR`, `PK`, `F_ID`, `CPR`,
`C_SGHREGION`, `ANSVARLIG_ENHED_FORLOB`, `FORLOEBSLABEL`, `C_ADIAG`, `C_ADIAG_KONTAKT`,
`ANSVARLIG_ENHED_KONTAKT`, `FORLOEBSMARKOER`, `FORLOEBSMARKOER_DATO`, `MRK_C`, `H`, `MRK_ST_H`,
`MRK_H`, `MRK_ST_L`, `MRK_ST_N`, `MRK_L_N`, `C_BOREGION`, `C_BOREGION_F`, `PSYKIATRI`, `ALDER`,
`OPGPER`, `OPGMAANED`, `Speciale_ansvarlig_enhed_kontakt`). Alle værdikolonner er `NVARCHAR(500)`
i både ext og arc; SDS' egne max_length er ≤ 255 tegn. Arkivet typer ikke.

Grain i kilden: én række pr. forløbsmarkør (kode + dato) på et helbredsforløb.
Identisk med 2022-arkivets skema bortset fra `SYGDOMSFORLOB_ID` → `DW_EK_HelbredsForloeb`.

**Person-data:** `CPR` står råt i ext/arc (som DRG) og hashet i `helbredsforloeb_noegle`. CPR
optræder aldrig i log, fejlbeskeder, metadata eller DSO. Extract tæller kun afvigende CPR-format.

### Datoer

Nøgleberegningen ("helbredsforløbets **tidligste** markør") og retningsguarden
(`LPROPDATERINGSDATO` mod `meta.UdredningsretLeverance.lpropdateringsdato NVARCHAR(10)`) sorterer og
sammenligner datoer **som strenge**. Det er kun kronologisk for ISO `YYYY-MM-DD`. Extracten
håndhæver derfor formatet pr. række (`UdredningsretDataset.iso_date_columns`): en ikke-ISO værdi i
`FORLOEBSMARKOER_DATO` eller `LPROPDATERINGSDATO` afviser leverancen med kolonnenavn og antal
(aldrig værdien). Blanke datoer tælles til metadata (`blanke_datoer`) men fejler ikke; målt: 0.
Kilden leverer ISO i alle 14 leverancer på sharet.

## Hvorfor kildens egne id'er ikke er merge-nøgler (B3)

| Kolonne | Målt adfærd | Konsekvens |
|---|---|---|
| `F_ID` | Omnummereres for hele helbredsforløb ad gangen: 31 % af markørerne på 27 dage | Ubrugelig som nøgle |
| `PK` (= `F_ID_løbenr`) | Følger `F_ID` | Ubrugelig som nøgle |
| `OBJEKTID_FORLOBSMARKOR` | Genudstedes for 3,8 % af markørerne pr. måned; 98,5 % af de "fjernede" UUID'er genfindes med samme (helbredsforløb, kode, dato) | Egenskab, ikke nøgle. Holdes aktuel i arc (markørens LPR3-UUID, B9) |
| `DW_EK_HelbredsForloeb` | SDS-surrogat, omnummereret december 2021 | Gruppering inden for én leverance, ikke nøgle på tværs |

98,8 % af 1,5 M ugentlige arc-opdateringer var ren omnummerering af `PK`/`F_ID`, indtil de to
kolonner blev taget ud af change-detect (`SURROGATE_ID_COLUMNS`). En ren omnummerering tæller
stadig ikke som ændring (`dw_timestamp_update` og `leverance_dato` røres ikke), men værdierne
holdes aktuelle: mergen skriver dem sammen med en reel indholdsændring, og bagefter kopierer
`refresh_surrogate_ids` leverancens `PK`/`F_ID` over på de levende rækker, mergen ikke rørte
(beslutning U1 i `udredningsret_model.md` §10 — staging grupperer markører til udredningsforløb på
`F_ID` inden for leverancen, så et forældet `F_ID` på uændrede rækker ville splitte et omnummereret
helbredsforløb i to).

## Forretningsnøgler beregnet i `ext.udredningsret_mont_noegler`

Beregnes i SQL af `udredningsret_archive.build_source_table_sql` som fysisk tabel (tre
vinduesfunktioner over 8,2 M rækker læses flere gange af guards og merge; derfor `_noegler`-tabel,
ikke `v_`-view). Navngivet `*_noegle` som DRG's afledte arc-kolonner; `clean_code_*` dannes først i
pre_stg (US-3). **Tabellen droppes igen efter en succesfuld merge** — mellem kørsler ville den være
en tredje ~5 GB kopi af datasættet uden læser (ext-heapen ~5 GB og arc ~5 GB bliver). Fejler
kørslen, bliver den liggende til post mortem og genopbygges ved næste kørsel. Spidsforbrug under
en kørsel: ~15 GB (ext + nøgletabel + arc); dev/QA/prod-staging er dimensioneret til DRG's ~16 GB
ext alene, så det er inden for rammen.

| Kolonne | Definition | Målt |
|---|---|---|
| `helbredsforloeb_noegle` `CHAR(64)` | SHA2-256 over `CPR` + helbredsforløbets FØRSTE markør (tidligste `FORLOEBSMARKOER_DATO`, laveste `FORLOEBSMARKOER`, `ANSVARLIG_ENHED_FORLOB`, `LOWER(OBJEKTID_FORLOBELEMENT)`) inden for leverancen | 100 % unik over 2.233.064 helbredsforløb; første markør skiftede for 218 (0,01 %) på 27 dage |
| `markoer_noegle` `CHAR(64)` | SHA2-256 over `helbredsforloeb_noegle` + `FORLOEBSMARKOER` + `FORLOEBSMARKOER_DATO` | 12.236 dubletter af 8,17 M |
| `loebenummer` `INT` | `ROW_NUMBER` over `markoer_noegle`, sorteret på `loebenummer_hash` = SHA2-256 over indholdet UDEN `PK`, `OBJEKTID_FORLOBSMARKOR`, `F_ID` og `DW_EK_HelbredsForloeb` (`LOEBENUMMER_HASH_EXCLUDED`); `OBJEKTID`, `PK` kun som sidste tie-break | Gør (markør, løbenummer) unik og stabil på tværs af leverancer |
| `antal_kilderaekker` `INT` | Fuldt identiske rækker (samme indhold, kun andet `PK`/`OBJEKTID`) reduceret til én | 1.705 rækker (2026-09-07) |

**Hvorfor løbenummeret ikke rangeres på `OBJEKTID`/`PK`:** begge omnummereres mellem leverancer
(3,8 %/md hhv. 31 %/md). For de 12.236 markører, der deler `markoer_noegle`, kunne rangeringen
vende, så indholdet byttede plads mellem `(markoer_noegle, 1)` og `(markoer_noegle, 2)` — uden at
nogen guard fangede det, og et downstream `dw_ek` ville sidde på den forkerte markør. Hashen af det
stabile indhold er en total orden over de rækker, der overlever dubletreduktionen (de har pr.
konstruktion forskelligt indhold), og den ændrer sig kun, når indholdet gør. **Restrisiko:** to
overlevende rækker, der kun adskiller sig i de udeladte id'er (fx samme markør på to
udredningsforløb med forskelligt `F_ID`), rangeres på `OBJEKTID`/`PK` og kan bytte plads ved
omnummerering. Tilfældet er ikke observeret; det er dokumenteret her frem for guardet.

Merge-nøgle: (`markoer_noegle`, `loebenummer`). PK `PK_arc_udredningsret_mont`.

## Merge- og guard-semantik

- `core.archive_merge` med `strategy="upsert"`, `update_only_on_change=True`, `detect_deletes=True`
  over hele tabellen (`upstream_is_full_load=True`), `enable_bulk_insert_on_first_load=True`
  (forudsætter det unikke clustered index på kildetabellen, som verificeres før merge).
- Change-detect udelader `leverance_dato`, `PK`, `F_ID`.
- **Opdatering af `PK`/`F_ID` efter mergen (U1, `udredningsret_model.md` §10):** når mergen er
  lykkedes, kører `refresh_surrogate_ids` én smal `UPDATE` fra nøgletabellen på levende arc-rækker
  (`dw_is_deleted = 0`), hvor `PK` eller `F_ID` afviger fra leverancens. Den `UPDATE` skriver kun de
  to kolonner — aldrig `dw_timestamp_update` eller `leverance_dato`, så en omnummerering forbliver
  en ikke-ændring (en *omgruppering* bumpes derimod af trinnet i næste punkt, U15). Kører før nøgletabellen droppes (den læser fra den) og før
  `meta.UdredningsretLeverance` registreres. **Springes over ved `backfill_historical`** (en ældre
  leverance må ikke skubbe ældre id'er hen over arkivets nyere) — logges som WARNING. Antallet
  rapporteres i assetets metadata som `surrogat_id_opdateret`; det indgår ikke i `rows_updated`.
- **Omgrupperingsdetektion sammen med den opdatering (U15, `udredningsret_model.md` §10):** U1
  behandler et ændret `F_ID` som ren omnummerering (1:1 for hele helbredsforløbet). Skulle SDS
  *omgruppere* markører (én F_ID-gruppe splittes i to, to samles, en markør skifter gruppe, eller en
  markørs F_ID bliver blank) uden at nogen anden kolonne ændres, ville helbredsforløbet ikke komme i
  delta-scope (scope styres af `dw_timestamp_update` og U9's borger-gren), og `fct.Udredningsforloeb`
  ville beholde den gamle grain, indtil en reel ændring rørte helbredsforløbet eller nogen kørte
  `full_load` — mens kontroltotalen fejlede hele natkæden ved hver kørsel i mellemtiden. Derfor kører
  `refresh_surrogate_ids` **detektion → id-opdatering → bump** på én cursor og ét commit:
  detektionen (`build_regroup_detection_sql`) sammenligner arkivets gamle gruppering med leverancens,
  mens begge endnu findes, og markerer de helbredsforløb, hvor omdøbningen ikke er en bijektion på
  grupper — plus dem, hvor en markør krydser **blank-grænsen** (et tredje `HAVING`-led): blank er en
  etiket på begge sider, men ikke en gruppe nedstrøms, så en hel gruppe, der bliver blank — eller en
  blank markør, der får et `F_ID` — er en bijektion på etiketter, samtidig med at arkivet mister
  eller får en gruppe. Rækker, som kørslens egen merge har ændret, prunes fra sammenligningen på
  serverens ur læst før mergen (`read_merge_start`), ikke på leverancedatoen (to leverancer kan være
  pakket samme dato). De markerede helbredsforløb får bagefter `dw_timestamp_update` bumpet
  (`build_regroup_bump_sql`) — aldrig `leverance_dato`. Antallet
  rapporteres som `omgrupperinger` i assetets metadata. **DQA-subject 6/9 dækker det ikke** — subject 6
  tæller kun rækker, der allerede ER re-staget, og subject 9 handler om skiftende første markør.
  Kontroltotalen (U13) er sidste værn, men den er **net-global**: en splitning og en sammenlægning i
  samme vindue ophæver hinanden og giver en grøn kørsel med begge helbredsforløb forkerte. Den ægte
  afstemning pr. helbredsforløb (`FULL OUTER JOIN` på `helbredsforloeb_noegle`) er rejst som
  DQA-subject 10 i US-6.
- **`leverance_dato` på arc betyder "den leverance, hvor rækken sidst ændrede sig"** — ikke "den
  leverance, rækken kom fra". Kolonnen er uden for change-detect og skrives derfor kun sammen med en
  reel ændring (alternativet er at omskrive 8,2 M rækker hver uge). "Hvilken leverance står arkivet
  på" besvares af `meta.UdredningsretLeverance` (revisionslog) og af DFM-checkpointet (se
  leverancedetektion), aldrig af `arc.leverance_dato`. Samme fælde som HAIBA's arkiv dokumenterer.
- Guards i rækkefølge: leveranceretning (`LPROPDATERINGSDATO` mod `meta.UdredningsretLeverance`,
  `backfill_historical` som eksplicit override), rækkeparitet (`SUM(antal_kilderaekker)` = ext),
  nøgleunikhed, unikt index. Arkivet har ingen soft-delete-loft og ingen grænse for genfindelse: kilden er ét
  fuldt snapshot, så leverancen afgør, hvad der slettes. Genfindelsen (andelen af arkivets levende
  helbredsforløbsnøgler, der findes i leverancen) måles og registreres, men stopper ikke kørslen.
- Målt: U 2026-08-31 fra tomt arkiv: 8.146.224 ext = eksportlog, 8.144.530 arc, fast load 33 s,
  nøglebygning ~6 min. U 2026-09-07: 26.884 indsat / 143.308 opdateret / 1.515 soft-slettet,
  genfindelse 99,98 %, ~8 min. Kilden sletter reelt ~4.500 markører/md (0,06 %).

## Leverancedetektion (niveau 1) og manifest

Følger DRG's beslutningsnotat (`etl/docs/source_systems/drg/drg_leverancedetektion_beslutningsnotat.md`): **en kørsel uden ny fil læser intet.** Udredningsret har ét spor (U og M er samme
snapshot), så der er én checkpoint-enhed og ingen niveau 2 pr. skive — retningsguarden på
`LPROPDATERINGSDATO` udfylder rollen som værn mod at merge ældre oven i nyere.

| | |
|---|---|
| Watermark | `meta.DFMState` (`scope=staging`, `schema_name=dfm`, `table_name=udredningsret_leverance_1083`), eksisterende `DfmManager.get_checkpoint` / `set_checkpoint` |
| Identitet | Filnavnets 14-cifrede pakketidsstempel som ISO, fx `2026-09-07T19:35:44`. Strengorden = kronologisk orden |
| Udvælgelse | `select_candidates`: kun filer med identitet **strengt nyere** end checkpointet (alle ved første kørsel); blandt dem den nyeste, der passerer pre-flight (`is_usable`). Ingen tar åbnes for filer under checkpointet |
| No-op | Ingen kandidater → extract skriver et **tomt** `ext.udredningsret_leverance_manifest`, rører ikke ext (forrige leverance bevares som debug-flade) og rapporterer SUCCESS. Arkiv-assetet kortslutter på det tomme manifest FØR nøgletabel, guards og merge; gate-assetet lader checkpointet stå |
| Defekt ny fil | Kandidater findes, men ingen passerer pre-flight → **fejl** (ikke no-op): en ny leverance er landet og kan ikke læses |
| Manifest | Én række pr. indlæst leverance: `filnavn`, `kadence`, `leverance_tidsstempel`, `leverance_dato`, `lpropdateringsdato`, `checkpoint_enhed`, `checkpoint_identitet`, `ryk_checkpoint`. Committes sammen med data; arkivet læser leverancen herfra (ikke fra `MAX()` over ext) |
| Gate | `udredningsret_leverance_checkpoint` (deps på `arc_udredningsret_mont`) stempler `meta.DFMState` med manifestets identitet, når mergen er lykkedes. Rykker ALDRIG ved pin (`ryk_checkpoint = 0`) |
| Pin / backfill | `delivery_on_or_before` omgår checkpointet (vælger blandt filer på/før datoen) og rykker det ikke. `backfill_historical` bruges sammen med pin. Ud over disse og et eksplicit checkpoint-reset (`EXEC meta.usp_dfm_reset @schema_name='dfm', @table_name='udredningsret_leverance_1083', ...`) læses ingen fil to gange |
| Forældelse | `udredningsret_leverance_alder` (eget asset i extract-gruppen, ingen deps): fejler når nyeste fil på sharet er > 10 dage gammel. Kigger kun på filnavne og påvirkes ikke af checkpoint eller pin. Ikke en afhængighed for loadet — et forældet share blokerer aldrig indlæsning af det, der er kommet |

Verificeret E2E (devcontainer): første leverance → checkpoint stemples; ny leverance → kun den er
kandidat; samme filer igen → tomt manifest, ext uændret, arkiv og gate no-op; efter checkpoint-reset
re-merges samme fil som 0/0/0; pin på ældre leverance afvises af retningsguarden og rykker ikke
checkpointet.

## Orkestrering

`udredningsret_ingest_job` (grupperne `udredningsret_extract`, `udredningsret_archive`) kører
dagligt kl. 02:00 CET via `udredningsret_ingest_daily_schedule` (auto-enabled på
development/prod/release-*, ellers STOPPED som repoets øvrige schedules). Dagligt frem for ugentligt,
fordi en dag uden ny fil er en no-op med kun en mappelistning og et checkpoint-opslag, mens en
forsinket kadence risikerer sharets ~3 måneders retention. Forudsætning på miljøerne:
`UDREDNINGSRET_BASE_DIR` i jobbets miljø (T2.5 hos DDK); indtil da fejler jobbet i
`udredningsret_dir()` med en tydelig besked. Til manuelle kørsler på en branch-allokering, hvor variablen endnu ikke er
registreret, findes run-config-overriden `base_dir` (på `ext_udredningsret` og `udredningsret_leverance_alder`): eksplicit
pr. kørsel, valideres som en mappe, logges som WARNING, aldrig en default. Ingen DQA-subjects i ingest (fil-kilde, som
DRG/HAIBA); kontroltotaler og guards kører inline. Staging/dim/fct (US-3–US-5) og DSO (US-6)
følger i egne jobs. `HENVIST_MARKOERER` (`AFV01A1`, `AFV01A1A`, `AFA01A`, `AFD01A`, bilag 1 i
SDS' datamart-beskrivelse) bruges af staging til startdato.

DFM bruger kun den eksisterende checkpoint-funktionalitet. Leverancens nøgle og
filnavnets tidsstempel defineres i udredningsret-kilden; DFM har ingen viden om
kildesystemer eller leveranceformater. [internal ticket] kræver ingen DFM-udvidelse.

<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# DRG-leverancedetektion: beslutningsnotat

Status: BESLUTTET 2026-09-11.
Ingen kode i denne task — notatet fastlægger mekanisme, watermark-placering
og detektionsnøgle, så DFM, extract, schedule og HAIBA kan implementeres deterministisk.
Grundlag: koden på `development` pr. 2026-09-11, leveranceinventaret på
`/mnt/ftproot/SDS_NGC_Sundhedsdata_DRG_GrupperetLPR/Fra-SDS/` samt
målingerne i `drg_noegler_beslutningsnotat.md` og
`drg_profileringsrapport.md` (samme mappe).

## Baggrund

`ext_drg`/`ext_drgpsyk` vælger i dag den nyeste **brugbare** fil pr.
takstsystem (`select_newest`, `DSI/csv_files/drg.py`) og streamer den ind i
ext ved HVER kørsel. Nedstrøms er det idempotent — `update_only_on_change`
med `leverance_dato` uden for `change_detect_columns` — men det er ikke
gratis: 2,5-4 GB bz2 pr. somatik-takstsystem dekomprimeres, ~16 GB skrives
til ext, og 16 arkiv-merges re-prober deres nøgler. Det er prisen for, at
extracten ikke ved, om filen allerede er indlæst.

Leverancerne kommer i to kadencer pr. takstsystem: **U** (ugentlig, to
aktivitetsår) og **M** (månedlig, tre aktivitetsår). Filnavnets timestamp er
*pakketidspunktet* — ikke datacap'en. M-filer pakkes fx d. 18., men bærer
data opgjort tidligere i måneden. Rækkefølgen mellem en U og en M kan derfor
ikke afgøres ud fra filnavnene, og det er kernen i det, detektionen skal
løse uden at åbne arkiverne.

**Observationen der former designet:** en NY M-fil kan aldrig springes over.
M er den eneste kadence, der bærer tre aktivitetsår, så dens t-2-skive findes
ikke i nogen U. Det eneste tilfælde, hvor en M må udelades, er når to M-filer
er ventende samtidig — kun realistisk ved første load eller efter mere end en
måneds stilstand — og der er svaret "tag den nyeste". Detektionens værdi
ligger derfor ikke i at springe nye filer over, men i at **en kørsel uden ny
fil ikke læser noget**: på månedens øvrige ~27 dage skal M-filen ikke
genlæses.

---

## Beslutning 1 — Tre jobs, ét takstsystem pr. kørsel

**Besluttet:** indlæsningen deles pr. kadence OG pr. leverancetype — et
somatik-månedsjob, et somatik-ugejob og et psyk-månedsjob — og hver kørsel
behandler ÉT takstsystem. Automatiseringen er **én serialiseret kø pr.
leverancetype**, bygget af et dagligt schedule og ført videre af én
`run_status_sensor`; somatik-U ligger efter somatik-M i køen og nås derfor
kun, hvis M-kørslerne lykkes. Se "Hvordan de tre jobs automatiseres"
nedenfor.

Hvorfor delt: M og U for samme takstsystem kolliderer i ext. Ext-tabellerne
droppes og genskabes én gang pr. kørsel, og alle valgte arkiver streamer ind
i samme writers — en M og en U for T26 i samme ext producerer samme
`KONTAKT_VAERDI` med forskellige værdier i de overlappende aktivitetsår, og
`guard_key_uniqueness` klassificerer det (korrekt) som konfliktende
dubletter og fejler kørslen. Adskilte jobs giver flere ext→arc-cyklusser pr.
døgn, så **M og U kan begge landes samme nat** i stedet for at en restance
skal afvikles over to dage.

Hvad delingen ellers rydder op i:

- **Kadence-bucketen bliver strukturel.** Jobbet *er* bucketen. Filer uden
  kadencebogstav (4 stk. T24, 4 stk. T25) håndteres af månedsjobbets
  filfilter i stedet for af en `None`-nøgle, der ellers ville bide ved
  historisk load.
- **M→U-rækkefølgen bliver en egenskab ved køen** i stedet for noget, der
  skal udledes af filnavne. Det var hele problemet.
- **Mønstret findes i repoet.** `haiba_publish_sensor` er en
  `run_status_sensor`, der starter et downstream-job på SUCCESS. (Indtil
  2026-09 var `build_dfm_base_job_success_sensor` det nærmeste forbillede,
  komplet med FAILURE-tvilling, men den blev fjernet, da IFDB→FHIR blev ét
  job.)

Fastlagte konsekvenser:

- **En kørsel uden ny fil rapporterer SUCCESS.** Det er forudsætningen for,
  at køen kan komme videre til næste takstsystem og derefter til
  U-entriene. Et "intet nyt"-resultat er et gyldigt, grønt udfald med
  besluttet/sprunget-over i asset-metadata — ikke en skip-tilstand.
- **En fejlende M-kørsel stopper køen og dermed U.** Det er tilsigtet: U skal
  ikke lægges oven på en brudt M. Konsekvensen er, at en vedvarende M-fejl
  også stopper U-indlæsningen, og at FAILURE-sensoren derfor er den eneste
  alarm på begge spor.
- **Psyk har sit eget månedsjob** (kun M-leverancer) og er uafhængigt af
  somatik i både kørsel, gate-asset og checkpoint.
- **Tidspunkt: 20:00 CET.** Kriteriet er, at en leverancedag skal kunne nå at
  være færdig før `ifdb_dfm_daily_schedule` kl. 06:00, fordi ingest-jobbene
  også opretter stub-Patients (`drg_borger`), der først bliver en rigtig
  `dw_ek_Borger` når den natlige DFM-kæde har mintet dem. Den oprindelige
  anbefaling var 01:00 CET afhængigt af en *målt* load-tid for en fuld
  M-leverance — hele køen, ikke én kørsel. Den måling blev aldrig lavet, og
  køen bliver længere for hvert nyt takstår, så vinduet blev i stedet udvidet
  fra 5 til 10 timer. Misses det, er konsekvensen ét døgns forsinkelse på
  `drg_publish_job`, ikke en datafejl: `drg_publish_sensor` venter på kæden og
  gen-opløser det, der stadig står på -1, en senere nat.
- **Ugejobbet har intet selvstændigt schedule.** Det ville kunne overlappe
  en somatisk månedskørsel og ødelægge dens ext midt i et load — somatik-M
  og somatik-U deler ext-tabeller. U-kørsler bestilles derfor kun af
  somatik-køen.

### Sådan vælges kadencen i extracten — og hvor `null` defaultes til M

De to somatik-jobs materialiserer de samme assets (`ext_drg` + de 9
somatiske `arc_*`); psyk-jobbet sit eget sæt. Kadencen og takstsystemet er
*udvælgelses*-parametre, ikke nye kilder, så de hører i config:
`DrgExtractConfig` udvides med `kadence`, `takstsystemer` findes allerede,
og jobbene/`RunRequest`s sætter dem via `dg.RunConfig`. Alternativet —
separate `ext_drg_m`/`ext_drg_u`-assets — ville give to asset-keys, der
skriver de samme fysiske tabeller, og dermed misvisende lineage.

**Defaultingen af kadenceløse filer skal skrives eksplicit** — jobdelingen
fjerner den ikke, den flytter den. `parse_archive_name` returnerer
`kadence: str | None`, og filtret i `discover_archives`/`select_newest` er:

| job | accepterede filer |
|---|---|
| månedsjob | `kadence in ("M", None)` |
| ugejob | `kadence == "U"` |

Det er her de 8 historiske filer uden kadencebogstav (4 × T24, 4 × T25)
bliver M-leverancer, og filtret er load-bearing: uden `None` i månedsjobbets
prædikat ville de aldrig blive indlæst af noget job. Ugejobbet skal omvendt
afvise dem strengt, så de ikke også ender i U-bucketen.

Det jobdelingen faktisk fjerner, er `None` som **nøgleværdi**: tidligere blev
checkpoint-nøglens kadence udledt af filnavnet, så `None` var en mulig tredje
bucket i `meta.DFMState`. Nu kommer nøglens kadence fra jobbet, så
nøglerummet er præcis {M, U} pr. takstsystem, uanset hvad filnavnene siger.

Bemærk at `ARCHIVE_RE` kun matcher `[MU]`: skulle SDS en dag indføre et
tredje kadencebogstav, bliver filen uparsebar og dermed usynlig for
`discover_archives`. Det degraderer sikkert — forældelses-assetet
(beslutning 4) vil alarmere, fordi de kontrollerede takstsystemer så mangler
en frisk fil — men regexet er stedet at opdatere.

Valget af config-form er at effektuere; filtret ovenfor er
et krav, ikke en mulighed.

### Én kørsel pr. (leverancetype, takstsystem)

**Besluttet:** ét takstsystem ad gangen — fil → ext → arkiv-merges med
guards → checkpoint — og så næste takstsystem. Og **somatik og psyk er helt
adskilt**: de kommer fra forskellige filer, skriver til forskellige
ext-tabeller (`ext.drg_*` hhv. `ext.drgpsyk_*`), har hver deres
checkpoint-enheder og hver deres jobs.

Den nuværende deling af ext er ikke en optimering, men en arv fra tiden før
leverancedetektion: når hver kørsel alligevel genlæste nyeste fil for alle
takstsystemer, fandtes der intet at committe pr. takstsystem, og koblingen
kostede ikke noget. Med et checkpoint pr. takstsystem koster den, at et
takstsystem ikke kan lykkes mens et andet fejler, at commit bliver pr.
kørsel i stedet for pr. takstsystem, og at hvert opslag i ext skal grupperes
på takstsystem. Alle tre forsvinder, når kørslen er enheden — og
karantæne-reglen for nye takstsystemer bliver overflødig.

#### Jobs

| job | kadence | omfang pr. kørsel |
|---|---|---|
| `drg_ingest_somatik_maaned_job` | somatik-køens M-entries (dagligt schedule) | ét takstsystem, M-filer |
| `drg_ingest_somatik_uge_job` | nås af somatik-køen efter alle M-entries | ét takstsystem, U-filer |
| `drg_ingest_psyk_maaned_job` | psyk-køen (eget dagligt schedule) | ét takstsystem, M-filer (psyk har ingen U) |

**Psyk blokerer aldrig somatik og omvendt.** Det var ikke opfyldt i et
fælles job: fejlede psyk, blev kørslen FAILED, og en `run_status_sensor` på
success ville dermed aldrig starte den somatiske U-kørsel — psyk ville
blokere somatik gennem run-statussen, selvom asset-grafen holdt dem adskilt.
Adskilte jobs løser det ved roden.

Hvert job har sit eget gate-asset, der stempler netop sin leverancetypes
checkpoint-enhed: `("dfm", "drg_leverance_T26_M")` og
`("dfm", "drgpsyk_leverance_T26_M")` er forskellige rækker og rykkes
uafhængigt.

#### Hvordan de tre jobs automatiseres

Der er to *forskellige* sekvenseringsbehov, og de løses af samme mekanisme:

1. **Inden for en leverancetype** må to kørsler aldrig overlappe — somatik-M
   og somatik-U skriver til de SAMME `ext.drg_*`-tabeller, som droppes og
   genskabes pr. kørsel. Det gælder både på tværs af takstsystemer og på
   tværs af kadencer.
2. **Somatik-U skal først køre, når somatik-M for samme cyklus er lykkedes.**

Begge falder ud af **én serialiseret kø pr. leverancetype**. Somatik har én
kø (begge kadencer), psyk har sin egen; de to køer er uafhængige og kører
parallelt, fordi de ikke deler ext-tabeller.

**Køen bygges af schedulet** og bæres i run-config:

```
drg_somatik_ingest_schedule (dagligt)
  lister sharet (os.listdir + ARCHIVE_RE — ingen tar, ingen database)
  → kø = [(T24,M), (T25,M), (T26,M), (T26,U)]
  → ÉN RunRequest på køens hoved: job=somatik_maaned,
    config={takstsystemer:[T24], kadence:M, resterende:[(T25,M),(T26,M),(T26,U)]}
```

**Sensoren er ren rørlægning.** Én `run_status_sensor` overvåger alle tre
jobs. På SUCCESS læser den `resterende` fra den afsluttede kørsels config,
tager det nye hoved og udsteder en `RunRequest` mod det job, hovedets
(leverancetype, kadence) peger på — med resten som ny `resterende`. Den
læser hverken share eller checkpoint.

Dermed:

- **Takstsystemerne kører i sekvens** inden for leverancetypen, fordi næste
  kørsel først bestilles, når den forrige er afsluttet. Ingen
  `concurrency_key`, ingen instans-konfiguration.
- **U-entries ligger efter M-entries i køen**, så somatik-U kører først, når
  alle somatik-M-kørsler er lykkedes. Gatingen er altså en egenskab ved
  køen, ikke en separat M→U-sensor. Fejler en M-kørsel, stopper køen, og
  U-entriet nås aldrig.
- **Psyk-køen er upåvirket** af en somatik-fejl og omvendt.

**Ved fejl** stopper køen dér, og næste dags tick bygger køen forfra. De
takstsystemer, der allerede er indlæst, er no-ops i kraft af niveau 1 — kæden
er derfor **selvhelende**, og et tabt sensor-hop koster højst et døgn.

**Hvad `drg_queue_failure_sensor` dækker.** Den fejlende kørsel er synlig i
sig selv — rød, med exception og logs. Det der IKKE er synligt, er
konsekvensen: kæden rykker kun på SUCCESS, så hver enhed der stod i kø
bagefter bliver aldrig bestilt, og for somatik gælder det også U-entriene, der
ligger efter alle M-entries. Én rød kørsel plus fire kørsler der aldrig skete
er et *fravær*, og fravær er det svære at bemærke. Sensoren læser den
resterende kø af den fejlede kørsels tag og siger højt, hvad der ikke blev
behandlet. Mekanismen er en Dagster-native FAILURE-sensor uden
ekstern transport — den afhænger altså ikke af den ADO-transport,
`nightly_failure_sensor` venter på. Den retter intet og
beder derfor ikke om et job.

**To værn mod overlappende køer:** `RunRequest`en får en `run_key` pr.
(leverancetype, kadence, takstsystem, dato), så samme entry ikke kan
bestilles to gange samme dag, og schedulet springer sit tick over, hvis der
allerede er en kørsel in-flight for den leverancetype (`get_run_records`
filtreret på status). Det sidste dækker det tilfælde, hvor et M-load løber
ud over et døgn.

**No-op-kørsler skal være billige, for der er mange af dem.** Køen indeholder
hvert takstsystem hver nat, også de pensionerede, så på en stille nat er alle
4 somatik-kørsler no-ops. Det er kun acceptabelt, fordi et **tomt manifest er
en erklæret no-op** (jf. manifest-afsnittet): arkiv-assetsene skal
kortslutte på et tomt manifest FØR de opretter views, kører guards eller
merger. Uden den kortslutning ville hver no-op-kørsel koste ~16
`CREATE OR ALTER VIEW` plus guard-scanninger.

Alternativet — `tags={"concurrency_key": ...}` og alle `RunRequest`s udstedt
på én gang — sparer sensoren, men giver ingen gating: U-kørslen ville stå i
kø uafhængigt og køre selv om M-kørslen fejlede. Det er korrekthedsmæssigt
harmløst (niveau 2 gør rækkefølgen irrelevant), men det er ikke den adfærd,
der er besluttet her. Desuden kræver det en `tag_concurrency_limits`-post i
instans-konfigurationen uden for dette repo.

#### Hvad det koster

| post | effekt |
|---|---|
| Dagster-kørsler | N× fixed overhead pr. leverancetype (proces, ressourcer, asset-steps). De fleste er no-ops. Målt mod et timelangt M-load: forsvindende |
| ext-DDL, guards, MERGE | ~neutralt. Arbejdet er proportionalt med data, og data deles blot pr. takstsystem. MERGE'ens ON er `(takstsystem, <UUID>)`, så hvert takstsystem rammer et disjunkt nøgleinterval |
| væg-ur | **uændret.** Løkken over takstsystemer er allerede sekventiel inde i én kørsel i dag |
| `drg_borger` | **den eneste reelle omkostning** — se nedenfor |

`drg_borger` (`DSA/FHIR/Patient/Patient_DRG.py`) laver i dag
`SELECT DISTINCT [CPR] FROM [arc].[drg_kontakt]` UNION psyk-pendanten over
HELE arkivet, uden takstsystem-filter, og har `deps` på begge
kontakt-arkiver. Uændret ville den scanne ~20M rækker én gang pr. kørsel.

**Krav: del den i to (somatik/psyk) og scop CPR-opslaget til kørslens
takstsystem.** Delingen følger af, at leverancetyperne har hver sit job —
en fælles `drg_borger` ville genindføre koblingen, den netop skal undgå.
Scopingen er både korrekt og hurtigere end i dag: en kørsel tilføjer kun
rækker for sit eget takstsystem, så CPR'er, der kun findes i T24, er
håndteret i T24's egen kørsel — og fordi `takstsystem` er den LEDENDE
kolonne i `PK_arc_drg_kontakt`, bliver `WHERE [takstsystem] = ?` et
range-seek i stedet for den fulde clustered scan. Flere smalle seeks er
billigere end én bred scan.

## Beslutning 2 — Watermark: `meta.DFMState`, ikke `meta.DRGLeverance`

**Besluttet: leverance-watermarket forankres i DFM** som rækker i
`meta.DFMState` med

| felt | værdi |
|---|---|
| `scope` | `staging` |
| `target_type` | `archive` |
| `schema_name` | `dfm` |
| `table_name` | `<leverancetype>_leverance_<takstsystem>_<kadence>` |
| `checkpoint_value` | leverancens pakke-timestamp, ISO (`2026-08-18T11:45:49`) |

Eksempel: `("dfm", "drg_leverance_T26_M")` → `2026-08-18T11:45:49`.
Maks. 9 rækker (3 takstsystemer × {M, U}, psyk × {M}).

`target_type` er `archive` og ikke det `dfm`, `schema_name` ellers ville
udlede. Rækken rykkes først, når leverancen **er** landet i `arc.*` — det er
det, checkpointet lover, og det er det, man har brug for at kunne se i
`meta.DFMState`. `schema_name` bliver ved med at være `dfm`, fordi det er
markøren for, at rækken er syntetisk og ikke har en tabel bag sig. Begge felter
indgår i DFMState's primærnøgle, så læser og skriver skal være enige om dem —
derfor står `DELIVERY_TARGET_TYPE` ét sted, i `DSI/connectors/delivery_detection.py`.

Leverancetypen **er** præfikset (`drg_leverance_…`, `drgpsyk_leverance_…`) i
stedet for at stå både som præfiks og inde i navnet. Samme information, ét
sted, og det flugter med, hvordan de to feeds i forvejen skelnes overalt ellers
i kilden (`arc.drg_kontakt` vs. `arc.drgpsyk_kontakt`). Bemærk at `_` er et
LIKE-jokertegn: et prædikat over enhederne skal escape præfikset som
`drg[_]leverance[_]%`, ellers rammer det hverken psyk-rækkerne eller kun
somatiks egne.

`<kadence>` er **jobbets** kadence, ikke filnavnets bogstav: de to
somatik-jobs skriver til hvert sit sæt rækker, og nøglerummet er derfor
præcis {M, U} pr. takstsystem. En fil uden kadencebogstav lander i M-bucketen, fordi
månedsjobbets filfilter er det, der accepterer den — se filtertabellen under
beslutning 1; defaultingen er et eksplicit kodekrav, ikke en følge af
jobdelingen.

**Hvorfor ikke `meta.DRGLeverance` som sandhedskilde** (alternativet i
task-beskrivelsen): tabellen er en *revisionslog* med PK
`(kildetabel, takstsystem, leverance_dato)` — én række pr. arkivtabel pr.
leverance (9 for en somatik-leverance, 7 for en psyk-), skrevet af
`record_delivery()` EFTER
merge. Den svarer på "hvad afspejlede arkivet historisk", ikke "må jeg
springe denne fil over". Dens grain mangler både kadence og aktivitetsår, og
retningsværnet læser den allerede — at lægge skip-beslutningen oveni ville
koble værn og gate sammen i samme tabel. `meta.DRGLeverance` **bevares som
revisionslog** (men udvides, se manifest-afsnittet).

**Hvorfor DFM:** repoets standard for nyt arbejde, og det giver
`get_checkpoint`/`set_checkpoint`, `meta.DFMRunLog`, reset-assetet
(`usp_dfm_reset`, `DfmResetConfig`) og RunModeConfig-vokabularet gratis.

**Hvorfor enheden ligger i nøglen og ikke i værdien:** `checkpoint_value` er
`NVARCHAR(50)`, og `meta.DFMState`s PK er
`(scope, target_type, schema_name, table_name)`. Et kort pr. enhed kan ikke
ligge i værdien, og en bredere kolonne ville ramme alle DFM-forbrugere for én
kildes skyld. `schema_name = "dfm"` med et *syntetisk* `table_name` er
etableret mønster: `flow_start_key=("dfm", "<tabel>_flow_start")` bruges af
SYDAX-, MySupply- og IFDB-arkiverne. **Konsekvens: [internal ticket] kræver ingen ny
tabel og ingen DDL for selve watermarket.**

**Hvem sætter checkpointet:** **ét gate-asset pr. leverancetype** i gruppen
`drg_archive` — ét med `deps` på de 9 somatik-`arc_*` og ét på de 7
psyk-tabeller. Hvert af dem stempler den ene checkpoint-enhed, kørslen
faktisk indlæste, ud fra manifestet.

Dagster-grafen er det eneste sted, hvor "alle leverancetypens merges
lykkedes i denne kørsel" kan læses: `execute_archive_merge` sætter kun det
enkelte `("arc", <tabel>)`-checkpoint, og et arkivtabel-checkpoint kan ikke
bære enheds-granularitet (én række pr. tabel, ikke pr. takstsystem ×
kadence). Adskillelsen i to gates er ikke kosmetisk — ét fælles gate ville
lade en somatik-fejl blokere psyk's checkpoint og dermed genindføre den
kobling, beslutning 1 fjerner.

**Commit-granulariteten følger kørslen, og kørslen er ét
(leverancetype, takstsystem)** — så nøglen giver både udvælgelse OG
recovery: fejler en merge for somatik-T26, rykker kun den enhed ikke, mens
T25's kørsel og psyk-T26's kørsel er upåvirkede. Inden for kørslen er det
fortsat alt-eller-intet på tværs af leverancetypens 9 hhv. 7 arkivtabeller:
fejler én tabels merge, kører gate-assetet ikke, og hele leverancen
genlæses næste gang. Ext understøtter samme semantik — ét
`conn.commit()` efter loadet, mens DDL'en committes separat før, hvilket er
grunden til at en no-op-kørsel skal springe DDL'en over (jf.
manifest-afsnittet). Bemærk at `_ExtWriter.close()` kalder `flush()` i et
`finally`, så en fejl der kan maskere den oprindelige undtagelse — værd at
rette, når extracten alligevel røres.

**Rettet.** Skriveren bor nu i `DSI/csv_files/_sds_tar.py` (fælles med
udredningsret og den kommende LPR3-kilde) og har en `discard()`, som udredningsret
allerede havde. `run_writers()` kasserer alle buffere på enhver undtagelse, før
`close()` kaldes, så den oprindelige årsag — mojibake, eksportlog-afvigelse,
skemadrift — overlever i stedet for at blive erstattet af en driver-fejl fra det
afsluttende `executemany`. Rækker nåede alligevel aldrig at blive committet på
fejlstien; ændringen er, at de ikke længere forsøges indsat på vej ud.

Afvist alternativ: lade extracten udlede gaten af en
fuldstændighedsforespørgsel mod `meta.DRGLeverance` ("har alle
leverancetypens kildetabeller en række for denne fil?"). Selvhelende uden et
nyt asset, men
hardkoder det forventede tabelantal og blander revisionslog med
styringstilstand. Bekræft eller omstød.

**Pin må aldrig rykke checkpointet.** `delivery_on_or_before` indlæser bevidst
en ældre leverance og følger samme konvention som RunModeConfigs
`period`/`ids`: `advance_checkpoint` = False. Ud over pin og et eksplicit
checkpoint-reset må ingen fil læses mere end én gang.

**`backfill_historical` er fjernet for DRG** (implementering).
Værktøjet merger en ældre leverance ind over en nyere med delete-detection og
revisionslog slået fra, og dets egen instruks er "kør nyeste leverance
bagefter". Den instruks kan ikke opfyldes med niveau 2: registret står stadig
på den nyere cap, så genindlæsningen af den nyeste bliver afvist, og arkivet
ender med tilbagerullede værdier *og* en påstand om den nyeste cap. Da
DRG-arkivet kan tømmes og genindlæses fra sharet, er den rigtige måde at
genskabe en tidligere tilstand "tøm + `delivery_on_or_before`" — den holder
registret konsistent med arkivets faktiske indhold. HAIBA's eget
backfill-flag er urørt.

---

## Beslutning 3 — Detektionsnøgle: to niveauer

"Skal denne fil indlæses?" og "må denne leverance opdatere arkivet?" er to
forskellige spørgsmål med adgang til forskellig information. De adskilles.

### Niveau 1 — filidentitet, før udpakning

Nøgle: enheden fra beslutning 2 → **filnavnets 14-cifrede pakke-timestamp**.
`ARCHIVE_RE` parser det allerede. Hvert job vælger den **nyeste** brugbare
fil med timestamp **strengt nyere** end enhedens checkpoint.

- Ren identitet: "har jeg indlæst præcis denne fil?" Ingen cap-semantik,
  ingen antagelse om SDS' pakkekonvention.
- Billigt: `os.listdir` + regex. Det er dette niveau, der sparer de ~16 GB.
- **Fuldt timestamp, ikke kun dato** — to filer kan falde på samme dato, og
  timestamppet total-ordner gratis.
- Er flere filer ventende for samme enhed (første load, eller stilstand over
  en måned), tages kun den nyeste. De mellemliggende springes over og
  rapporteres som sådan.
- `is_usable()`-pre-flighten kører fortsat, men kun på kandidater, der har
  passeret niveau 1.

### Niveau 2 — reel datacap pr. årsskive, mellem ext og arc

Nøgle: `(takstsystem, aktivitetsår)` → **`LPROPDATERINGSDATO`** fra
leverancens `LEVERANCEINFO`-medlem. Efter ext-loadet sammenholdes
leverancens faktiske cap med den cap, arkivet afspejler for hver skive, og
**kun skiver med strengt nyere cap merges**.

Dette er værnet mod at overskrive nyere data med ældre, og det gør
**rækkefølgen mellem M og U irrelevant for korrektheden**: uanset om U
(cap 17/8) eller M (cap 10/8) kommer først, ender arkivet i samme tilstand,
fordi cap-sammenligningen afviser de skiver, der ikke er nyere. Netop derfor
er den strukturelle M→U-rækkefølge fra beslutning 1 en driftsforenkling og
ikke en korrekthedsforudsætning.

**`LEVERANCEINFO` bærer én cap pr. fil, ikke én pr. år.** Sammenligningen
"pr. aktivitetsår" er altså: filens ene cap mod den cap, **arkivet har
registreret pr. skive**. Den per-skive-registrerede cap er den eneste
egentlig nye tilstand, designet kræver — se manifest-afsnittet.

### Hvor cap'en hentes

Med én kørsel pr. takstsystem (beslutning 1) rummer ext højst én leverance
pr. leverancetype, så opslaget er trivielt:

```sql
SELECT [takstsystem], [LPROPDATERINGSDATO] FROM [ext].[drg_leveranceinfo]
```

`takstsystem` stemples alligevel på hver ext-række
(`TAKSTSYSTEM_COLUMN` i `_stream_data_member`), så prædikatet er en
billig sikkerhedssele frem for en nødvendighed — og det bevarer korrektheden,
hvis nogen senere kører med `takstsystemer` sat til flere værdier.

To egenskaber ved kilden er værd at kende:

- **Én LEVERANCEINFO-række pr. leverance** (`drg_noegler_beslutningsnotat.md`:
  "1 række pr. leverance"; profileren læser kun første række). Filens cap er
  altså én værdi, ikke en mængde.
- **Kun én cap pr. FIL** — ikke én pr. aktivitetsår. Sammenligningen er
  derfor filens ene cap mod arkivets registrerede cap for hver af de
  (takstsystem, aktivitetsår)-skiver, filen bærer.

### Cap'ens kilder: reel, ellers afledt af filnavnet

Der er **altid** en cap. Rækkefølgen er:

1. `LPROPDATERINGSDATO` fra leverancens `LEVERANCEINFO` — den reelle cap.
2. Mangler den, bruges **filnavnets timestamp** som cap, markeret som
   *afledt*.

Sammenligningen pr. (takstsystem, aktivitetsår)-skive er derefter én regel:
**merge kun, hvis leverancens cap er strengt nyere end den cap, arkivet har
registreret for skiven** — og har arkivet ingen registreret cap (første load
af et takstsystem), merges der altid.

Begrundelsen for fallbacken: uden en cap kan en leverance aldrig opdatere
arkivet igen, og så er en manglende `LEVERANCEINFO` en blindgyde. En
rækkefølgeindikation, der er omtrentlig, er bedre end ingen.

**Den kendte skævhed, og hvorfor den er acceptabel.** For U-filer er
filnavnets dato dokumenteret som datacap'en, så den afledte cap er korrekt.
For M-filer overdriver den med ~8 dage (pakket d. 18., data opgjort d. 10.).
En cap-løs M ville derfor registrere 18/8 for sine skiver, og en
efterfølgende U med reel cap 17/8 ville blive sprunget over. Konsekvensen er
**afgrænset og selvhelende**: M er et FULDT snapshot af de samme
aktivitetsår, så arkivet står med M's 10/8-værdier — ikke med et hul — og
næste U (24/8) slår 18/8 og merges. Værste tilfælde er ét ekstra døgns
alder på t/t-1-skiverne i én kadence-cyklus.

**Cap'ens proveniens registreres sammen med værdien** (`reel` / `afledt`) i
per-skive-registret. To grunde: sammenligningen kan revideres i drift uden
at gætte på, hvor tallene kom fra, og en afledt cap er et signal om, at
leverancen manglede `LEVERANCEINFO` — hvilket i sig selv bør undersøges.

**Tilstanden er tilladt af koden, men forekommer ikke.** `is_usable()`
undtager eksplicit `LEVERANCEINFO` fra 0-rækkers-checket
(`if max(counts) == 0 and prefix != "LEVERANCEINFO"`), og
profileringsrapportens §12 fastslår "Ingen leverancer med 0-rækkers datasæt
fundet i inventaret (113 arkiver)". Fallbacken er altså et defensivt design,
ikke en reaktion på observeret adfærd — og med den på plads er der ingen
grund til at fjerne undtagelsen i pre-flighten.

Fallbacken gælder **somatik**. Psyk-leverancerne har slet ingen
`LEVERANCEINFO`, og niveau 2 er ikke i brug for psyk i nogen variant — heller
ikke med en afledt cap. Se afsnittet om psyk nedenfor.

**Konsekvens: niveau 2 erstatter `guard_delivery_direction()`s
sammenligning af pakkedatoer.** Værnet sammenligner i dag ext'ens
`MAX(leverance_dato)` mod `meta.DRGLeverance` pr. takstsystem og fejler,
hvis ext er ældre. Med den faste M→U-rækkefølge er det direkte forkert:
ugejobbet præsenterer pakkedatoen 17/8, efter at månedsjobbet har registreret
18/8, og værnet ville rejse en fejl på en fuldstændig korrekt sekvens.
Værnet omskrives til at sammenligne **cap pr. skive** — samme beskyttelse,
rigtig størrelse. `leverance_dato` på ext er pakkeprovenance;
`LEVERANCEINFO` er det, der skal danne checket mod arkivet.

**Kun for somatik.** Psyk har hverken `LEVERANCEINFO` eller
`aktivitetsaar_column`, så dér er der ingen skiver at sammenligne, og
`guard_delivery_direction` **bevares uændret** på pakkedato pr. takstsystem.
Det er korrekt for psyk: kadencen er kun M, kørslen bærer ét takstsystem, og
en nyere leverance har altid en nyere pakkedato. Implementeringen får altså
to varianter af værnet, ikke én — det er et bevidst valg, ikke en
forglemmelse.

`leverance_dato` **bevares som pakkedato** på ext og arc, og cap'en lægges
*ikke* i den: `LEVERANCEINFO` sorterer alfabetisk EFTER `DIAGNOSER` og
`DRGKONTAKT`, så cap'en ikke er kendt, mens de store tabeller streames — at
stemple den ville kræve en efterfølgende `UPDATE` over 25,6 mio.
procedurerækker. To kolonner, to betydninger, ingen omskrivning.

### Hvorfor filnavnet kun er fallback, aldrig primærkilde

Konventionen "M-filer har cap d. 10." er dokumenteret, men er en antagelse om
SDS' pakkeadfærd. Bruges den som PRIMÆR cap, fejler en ændring i den
*stille*: M og U mis-ordnes systematisk ved hver eneste leverance, og
revisioner udskydes uden spor. `LEVERANCEINFO` indeholder sandheden og
indlæses alligevel hver kørsel, så der er ingen grund til at gætte, når
kilden er der.

Som **fallback** er filnavnet derimod det rigtige valg (se ovenfor):
skævheden rammer kun leverancer uden `LEVERANCEINFO` — en tilstand, der
aldrig er observeret — den er selvhelende, og alternativet er en leverance,
der aldrig kan opdatere arkivet. Forskellen mellem de to roller er hele
pointen: en systematisk skævhed på hver leverance er uacceptabel, en
afgrænset skævhed i et defensivt hjørne er det ikke.

**Og cap'en kan ikke læses billigt før loadet.** Medlemsrækkefølgen i tar'en
er alfabetisk (`Metadata_* < SDP_ExportLog < datafiler`), datafilerne hedder
`T26_<PREFIX>_1083_<dato>.csv`, og `T26_DIAGNOSER…` og `T26_DRGKONTAKT…`
sorterer derfor FØR `T26_LEVERANCEINFO…`. Det er de to største datasæt (8,6
mio. og 7,5 mio. rækker), og bz2 er en sekventiel strøm uden index —
`tarfile` kan ikke søge forbi dem. At nå `LEVERANCEINFO` betyder at
dekomprimere hovedparten af et 2,5-4 GB arkiv. Det er præcis derfor
`is_usable()` stopper ved `SDP_ExportLog`, som ligger *før* datafilerne.

**Til gengæld valideres konventionen gratis bagefter:**
`arc.drg_leveranceinfo` er en insert_only-log af LPROPDATERINGSDATO, så efter
hver leverance kendes den faktiske cap. Afviger den fra det, konventionen
ville forudsige, logges det som advarsel.

### Psyk har ingen `LEVERANCEINFO`

Kun somatik-leverancen har et `LEVERANCEINFO`-datasæt (`drg_datasets.py`);
psyk har det ikke, og ingen psyk-tabel har en `aktivitetsaar_column`. Niveau
2 findes derfor ikke for psyk — og behøver det ikke: psyk leveres kun M, så
der er ingen M/U-ordning at tage fejl af. For psyk gælder niveau 1 plus
retningsværnet på takstsystem-niveau.

### HAIBA-formatet

Samme niveau-1-form, andet identitetsudtryk: enhed `(HAI-type, kind)` →
`leverancedato + mtime` som ISO-streng. Ingen HAIBA-pendant til
`LEVERANCEINFO`, så niveau 2 udgår. HAIBA's arkiv-invariant er hårdere
(`detect_deletes=True` + `upstream_is_full_load=True`, uscoped): en partiel
ext soft-sletter de manglende HAI-typer. Kortslutningen skal derfor være
**alt-eller-intet på tværs af typer**. Formatet er ens-læsbart for begge
kilder (én sorterbar identitetsstreng pr. enhed), som [internal ticket] kræver.

---

## Beslutning 4 — Forældelses-alarmen bliver et selvstændigt asset

`check_delivery_age()` flyttes **ud** af `_run_extract` og bliver et
selvstændigt, genbrugbart check-asset over en kildes leverancemappe.

**Det retter en aktiv defekt.** Checket får i dag `selected`, dvs. den
nyeste fil pr. takstsystem *uanset kadence*. Dør U-sporet, mens M bliver
leveret, er den nyeste T26-fil en frisk M — og checket går grønt. **Et dødt
ugentligt spor er i dag usynligt.**

Ny form: **tre uafhængige checks**, afgrænset til **de to højeste
takstsystemer pr. leverancetype** — ikke til ét "aktivt" takstsystem:

| check | omfang | cap |
|---|---|---|
| DRG / M | pr. takstsystem, begge skal være friske | ~40 dage |
| DRG / U | aggregeret over de to — alarm kun hvis INGEN har en frisk U | ~10 dage (nuværende `DRG=10`) |
| DRGPSYK / M | pr. takstsystem, begge skal være friske | ~40 dage (nuværende `DRGPSYK=40`) |

Afgrænsningen er ikke et gæt — den følger leveranceinventaret
(`drg_profileringsrapport.md` §4), og den er samtidig det, der gør checket
robust over et takstårsskifte:

- **Flere takstsystemer leverer M samtidig.** T25 M og T26 M har begge
  seneste leverance 2026-08-18. "Kun det højeste takstsystem" ville derfor
  gøre et dødt T25-M-spor usynligt, mens T26 kørte videre. Derfor tjekkes de
  to højeste hver for sig.
- **Serien stopper først, når den er blevet tredje-højeste.** Somatik T24 M
  stoppede 2026-03-17, og T26 M startede 2025-12-14 — T24 var altså allerede
  ude af top-2, da den lukkede. Et lukket takstår forlader derfor
  check-mængden af sig selv, uden konfiguration, og et per-takstsystem-M-check
  falsk-alarmerer ikke på lukkede år.
- **U hører til det aktive takstår og skifter takstsystem ved
  årsskiftet — med overlap.** T26 U startede 2026-02-25, T25 U stoppede
  2026-03-09. I de ~2,5 måneder hvor T26 havde M men endnu ingen U, var
  U-serien fuldt levende på T25. Et per-takstsystem-U-check på det højeste
  takstsystem ville have alarmeret falsk i hele den periode. Aggregeret over
  de to højeste er svaret korrekt gennem hele overdragelsen — og en
  nådeperiode/"har nogensinde haft en U"-hukommelse er dermed unødvendig.
- **Udfasning sker pr. (leverancetype, takstsystem), ikke pr. takstsystem.**
  Psyk T24 leverede stadig 2026-08-12, mens somatik T24 stoppede
  2026-03-17. Check-mængden beregnes derfor pr. leverancetype.
- **Sen genlevering kan ikke maskere et dødt spor.** Med top-2-afgrænsningen
  er en efternølende T24-leverance uden for check-mængden og kan ikke gøre
  et dødt T26-spor grønt — hensynet bag det nuværende
  `max(discovered)`-design er dermed bevaret.

Øvrige krav, uændret:

- **Cap pr. kadence.** Den nuværende `DRG=10` passer til U. Et M-check med
  samme cap ville falsk-alarmere hver måned mellem d. 18. og næste d. 10.
- **Kun filnavne.** Ingen tar åbnes, ingen database læses, og checket må ikke
  påvirkes af checkpoint, `takstsystemer`-filter eller
  `delivery_on_or_before` — det er et check af *sharet*, ikke af
  indlæsningen. Top-2-reglen holder den egenskab: den udledes af selve
  fillistningen.
- **Uden for load-stien** afkobler det to fejltilstande: en forældet
  leverance fejler i dag extracten hårdt, så man ikke kan indlæse det, der
  faktisk ER kommet.

Assetet designes generisk (mappe + filnavnsmønster + et sæt
`(bucket, max-alder)`-regler), så HAIBA og fremtidige fil-leverancer kan
bruge det samme.

---

## Beslutning 5 — Partitionering forkastet; MERGE bevares

Forslaget var at partitionere de store arc-tabeller på
`(takstsystem, aktivitetsår)` og erstatte MERGE med truncate+insert pr.
partition — legitimt motiveret ved, at en fil altid indeholder ALT data for
de skiver, den dækker. **Forkastet.** Tre blokeringer, hvoraf den anden er
afgørende:

**1. Kolonnen kan ikke være nøglekolonne, som den ser ud.**
`AKTIVITETSAAR`/`Aktivitetsaar` er `NVARCHAR(500) NULL` i alle arc-tabeller.
SQL Server kræver partitioneringskolonnen i nøglen på ethvert unikt indeks,
og en klynget nøgle er begrænset til 900 bytes — NVARCHAR(500) er 1000 alene.
Værdierne er reelt 4-cifrede år, så en indsnævring til `NVARCHAR(4)`/`INT` er
mekanisk mulig (PK'erne har rigelig budget: `PK_arc_drg_diagnose` bruger ~138
af 900 bytes), men det kræver en migration over op til 25,6 mio. rækker og
gør en værdikolonne til nøglekolonne — imod reglen om, at kun nøglekolonner
er snævre, så extracten også skulle længdevalidere den.
Løsbar, men ikke gratis.

**2. En kontakts aktivitetsår revideres — derfor er året ikke i
merge-nøglen.** `drg_noegler_beslutningsnotat.md` fastslår det, og
profileringsrapportens §6.3 måler frekvensen: **3 kontakter skiftede
aktivitetsår** mellem U 2026-08-02 og U/M 2026-08-16. Begrundelsen står
eksplicit: *"med året i nøglen ville revisionen skabe en dublet i stedet for
en opdatering."* Truncate+insert pr. skive genindfører netop det:

> En kontakt ligger i 2024, som kun M dækker. Året revideres til 2025.
> U-leverancen bærer 2025, truncater den partition og indsætter kontakten
> der. Intet rører 2024-partitionen, for U bærer ikke 2024. Arkivet har nu
> samme `KONTAKT_VAERDI` i to årsskiver — og fact-grainen
> `(takstsystem, kontakt)` er brudt lydløst.

Dagens MERGE håndterer det korrekt: `ON (takstsystem, KONTAKT_VAERDI)`
matcher den eksisterende række og opdaterer `AKTIVITETSAAR` **på plads**, og
den årsskive-scopede delete-detection rører den ikke, fordi den gamle rækkes
år ligger uden for scopet. Den **globale** unikhed på (takstsystem, UUID)
udfører altså reelt arbejde, og partitionsjusteret unikhed opgiver den. Ved
~3 tilfælde pr. leverance ville det akkumulere i størrelsesordenen 150
permanente, lydløse dubletter om året.

Præmissen "en fil indeholder alt for en given (takstsystem, aktivitetsår)"
er sand. Den er blot ikke tilstrækkelig, fordi en **række kan flytte fra en
skive til en anden**, og filen beviser kun de skiver, den bærer.

**3. Det ville gøre nedstrøms langsommere — målt.** Truncate+insert stempler
hver række med nyt `dw_timestamp_update`, og DSA's delta-scope er defineret
på netop den kolonne (`IX_*_DW_Timestamp_Update` findes på hver arc-tabel for
at betjene den). Profileringsrapportens §6.2 måler den reelle ugedelta
(M/U 16/8 → U 23/8, 7.393.644 matchede kontakter):

| måling | resultat |
|---|---|
| reel attributdrift blandt matchede | **0,04 %** (DRG/totalpris: 2.920 / 2.922) |
| ind-/udtidspunkt ændret | 282 / 612 |
| nye kontakter i U | 97.884 |

MERGE med `update_only_on_change` stempler altså ~100.000 af 7,4 mio. rækker
— **ca. 1,4 %**. Truncate+insert ville stemple 100 %. Det er ~70× forstørrelse
af deltaet til pre_stg, staging og begge bridges, hver uge; for en månedlig M
ville ~12 mio. kontakter præsenteres som ændrede. Derudover mistes
soft-delete-historik (`dw_is_deleted`) og genoplivnings-semantik, og deltaet
bliver principielt *umåleligt* — nedstrøms kan ikke længere se forskel på
ændret og uændret.

**Bonus-regression:** MERGE'ens ON-klausul er `(takstsystem, <UUID>)` uden
år. På en tabel partitioneret efter `(takstsystem, aktivitetsår)` er det
klyngede indeks justeret, så et sådant seek kun kan eliminere på takstsystem
og må søge i HVER årspartition — N seeks i stedet for ét, på millioner af
probes. Et ikke-justeret unikt indeks er ingen udvej: det blokerer
`TRUNCATE ... WITH (PARTITIONS ...)` og `SWITCH`, som er hele formålet.

**Hvad der står i stedet:** `IX_*_Aarsskive` på
`(takstsystem, aktivitetsår, dw_is_deleted)` findes allerede på alle 7
somatik-tabeller, så delete-scope-scanningen er indekseret i dag. Den store
gevinst er **ikke at merge de skiver, der ikke er nyere** — niveau 2 — som
på hver månedlig M springer t- og t-1-mergen helt over. Det kræver ingen
skemaændring.

Ønskes partitionering senere, er den forsvarlige begrundelse
read-side-elimination for DSA og partitionsvis indeksvedligehold — ikke
write-side skiveudskiftning — og den kræver stadig året indsnævret og et svar
på dublet-problemet i blokering 2. (Instansens SQL Server-version/edition er
ikke verificeret; partitionering findes i alle editions fra 2016 SP1.)

**Selvstændig optimeringsmulighed — verificeret sikker (ikke en del af denne
task):** `enable_bulk_insert_on_first_load` er **slået fra** for alle
DRG-arkivtabeller (`build_merge_config`), mens IFDB- og SOR-arkiverne bruger
den. På et tomt arkiv erstatter den MERGE med én
`INSERT ... WITH (TABLOCK) SELECT` — direkte relevant for førstegangsloadet
af 46M+ rækker. Flagets docstring kræver, at ext-rækkerne er *garanteret*
unikke på merge-nøglerne, hvilket er en design-time-påstand; for DRG er
kontrollen stærkere end det:

- `guard_key_uniqueness` kører FØR mergen i `_run_archive` og fejler på
  konfliktende dubletter hhv. slår `dedup_source_on_keys` til for kørslen ved
  indholds-identiske. `_build_empty_target_fastload_sql` kalder **samme**
  `_build_source_expr` som MERGE-stien, så fast loadet arver dedup'en.
  Det dækker den målte ikke-unikke `drg_interne_koder` (2.898 grupper /
  25.996 overskydende rækker, alle byte-identiske) og T24-psyk-dubletten.
- Alle 16 merge-mål har `dw_is_deleted ... DEFAULT (0)`; fast loadet udelader
  kolonnen og forlader sig på defaulten.
- Fast stien vælges kun på et TOMT mål, så der er ingen interaktion med
  delete-detection (levende rækker i scope = 0).
- `drg_leveranceinfo` er `insert_only` og rammes ikke af flaget (kun
  upsert-tabeller).
- Fejlsemantikken er ren: rollback efterlader arc tom og ext urørt (rerun
  tager fast stien igen); commit-derefter-nedbrud giver et ikke-tomt mål, og
  rerun falder tilbage til MERGE, der re-upserter de stadig tilstedeværende
  ext-rækker idempotent.

**Uverificeret forudsætning for den fulde gevinst:** minimal logning kræver
staging-databasen i SIMPLE eller BULK_LOGGED recovery. Er den i FULL, vinder
man stadig OUTPUT-`$action`-sporingen, den per-række-change-detection og
per-række-indeksvedligeholdet (indeksene disables og rebuildes), men ikke
log-skrivningen. Det er en miljøforespørgsel, ikke et repo-faktum.

---

## Robusthed ved takstårsskifte (T26 → T27)

Skiftet er ikke en hændelse, der skal håndteres manuelt. Mønstret fra
inventaret (`drg_profileringsrapport.md` §4) er: en ny M-serie starter i
midten af december året før (T26 M: 2025-12-14), U-serien følger efter et par
måneder (T26 U: 2026-02-25) med overlap til den gamle (T25 U stoppede
2026-03-09), og den ældste serie stopper efter at være blevet tredje-højeste
(somatik T24 M: 2026-03-17). Retirerede filer bliver liggende på sharet —
`Arkiv/` rummer T23 og ældre T24-historik, men somatik-T24's egne 8
leverancer ligger fortsat i `Fra-SDS`, blot uden nye. **Fraværet af filer er
altså ikke retirement-signalet; fraværet af NYE filer er.**

### Holder af sig selv — ingen deployment ved et nyt takstsystem

| mekanisme | hvorfor det holder |
|---|---|
| Checkpoint-enheder | `set_checkpoint` upserter via MERGE, så `("dfm", "drg_leverance_T27_M")` opstår ved første kørsel. Ingen DDL, ingen migration, ingen konfiguration |
| Første load af T27 | `get_checkpoint` returnerer NULL for en manglende række → "alt er nyt" → nyeste T27-fil indlæses |
| Retningsværn | ingen arc-rækker for T27 → `guard_delivery_direction` springer takstsystemet over |
| Delete-scope | T24 uden nye filer giver ingen T24-rækker i ext → T24-skiverne ligger uden for `delete_scope_predicate` → arkivet beholder sin T24-historik. Det er præcis den egenskab, årsskive-scopingen blev indført for |
| Sen genlevering | et gammelt takstsystem, der får en efternølende fil, har sin egen tier-1-enhed og sine egne per-skive-caps og påvirker ikke det aktive år |
| Forældelses-checket | top-2-reglen (beslutning 4) flytter check-mængden af sig selv: T27 træder ind ved sin første fil, T25 træder ud |

**`optional_takstsystemer=("T24", "T25")` skal IKKE udvides ved årsskiftet.**
Feltet er den allow-list, der holder union-skemaet ærligt: `source_columns`
er unionen af SDS' `Metadata_*`-skemaer for T24/T25/T26, `optional_columns`
navngiver de kolonner, der ikke findes overalt
(`drg_kontakt`: `DW_EK_HelbredsForloeb`, `FORLOB_REFERENCE_VAERDI`,
`FORLOB_REFERENCE_TYPE`; `drg_sghforlob`: `DW_EK_HelbredsForloeb`), og
`optional_takstsystemer` siger HVOR de lovligt må mangle. Uden allow-listen
ville hver T24/T25-leverance fejle; uden takstsystem-begrænsningen ville en
kolonne, der forsvinder fra en T26-leverance, passere ubemærket.

Listen er altså et *historisk* faktum om hvilke takstsystemer der mangler
kolonnerne — ikke en liste over "gamle år". Tilføjes T27, koster det to
ting: skemadrift-detektionen forsvinder for det nyeste og mest brugte
takstår fra dag ét (leverer SDS T27 uden `FORLOB_REFERENCE_*`, lykkes
loadet, og T27's rækker bærer NULL hvor T25/T26 bærer værdier — en lydløs
funktionel regression for det aktive år), og når T27 først HAR arkiverede
rækker, ville en senere leverance uden kolonnen blive accepteret og NULLe
hele skiven ved merge — netop den regression, feltets kommentar advarer om.

Mangler T27 reelt en kolonne, som T26 har, er det rigtige svar derfor en
bevidst beslutning om `optional_columns`/DDL — ikke en udvidelse af
allow-listen. Indtil den beslutning er taget, fejler T27's kørsel hårdt, og
fordi kørslen er ét takstsystem, rammer det kun T27.

### Skemadrift på den første T27-leverance

`validate_member_schema` fejler hårdt på både ukendte og manglende kolonner,
og **det skal den blive ved med** — en kolonne, der forsvinder fra et nyere
takstsystem, ville ellers NULLe arkivet ved merge.

Med én kørsel pr. takstsystem er konsekvensen afgrænset af sig selv: ændrer
SDS skemaet i T27, fejler T27's kørsel, mens T25's og T26's kørsler er
uberørte. Den karantæne-regel, en delt kørsel ville have krævet, er dermed
overflødig — isolationen er strukturel i stedet for en undtagelse.

**Alarmen kommer KUN fra den røde kørsel.** Forældelses-assetet ser filer,
ikke indlæsninger: T27-filen ligger frisk på sharet, så checket går grønt,
selv om intet kan indlæses. Det er et bevidst skel — checket skal kunne
rapportere, uanset om noget loader — men det betyder, at FAILURE-sensoren er
den eneste alarm på "filerne kommer, men vi kan ikke læse dem". Den rigtige
reaktion er en bevidst opdatering af `drg_datasets.py` og arc-DDL — ikke en
udvidelse af `optional_takstsystemer`.

### En rollover-specifik grund til beslutning 4

Så længe forældelses-checket ligger inde i `_run_extract`, ser det kun de
takstsystemer, `takstsystemer`-filteret slipper igennem — og med én kørsel
pr. takstsystem er det filter nu sat på HVER kørsel. Checket ville altså se
præcis ét takstsystem ad gangen og udlede sin top-2-mængde af det. Som
selvstændigt asset uden den config ser det hele sharet. Beslutning 1 gør
derfor beslutning 4 til en forudsætning, ikke en forbedring.

---

## Leverance-manifestet: kontrakten mellem ext og arc

For at niveau 1 og 2 kan implementeres uden at gætte, skriver extracten én
lille tabel pr. kørsel — `ext.drg_leverance_manifest`. Med én kørsel pr.
(leverancetype, takstsystem) er det **højst én række**: enten indlæste
kørslen en leverance, eller også er manifestet tomt.

`(leverancetype, takstsystem, kadence, leverance_ts, filnavn, leverance_dato,
 lpropdateringsdato, cap_kilde)`

`cap_kilde` er `reel` eller `afledt`, jf. cap-afsnittet.

**Afvigelse ved implementeringen:** manifestet bærer IKKE
årsskiverne. Arkivet udleder dem i forvejen pr. tabel fra ext
(`read_ext_slices`), fordi skivemængden kan variere mellem tabellerne i samme
leverance; at føre en kopi i manifestet ville være to sandheder om det samme.
Manifestet bærer derfor leverancens identitet og cap, og skivemængden udledes
— men BEGGE scopes udledes fortsat af samme funktion
(`build_slice_predicate`), som er det, invarianten kræver.

Der er ét manifest pr. leverancetype (`ext.drg_leverance_manifest`,
`ext.drgpsyk_leverance_manifest`): de to extracts ejer hver sine ext-tabeller
og må ikke kunne rydde hinandens manifest.

Manifestet er kørslens **kontrakt**: det lister præcis de enheder og skiver,
arkivet må merge. Det er provenance, ikke forretningslogik, og hører i ext
(volatilt, genskabt pr. kørsel). Det løser fire ting på én gang:

1. `record_delivery()` kan persistere kadence og cap i `meta.DRGLeverance`
   uden at gætte (kolonnen `lpropdateringsdato` findes allerede i DDL'en og
   er aldrig blevet skrevet af koden).
2. Gate-assetet ved, hvilken enhed kørslen faktisk indlæste, og kan stemple
   dens checkpoint uden at gen-scanne mappen (en ny fil kan være landet midt
   i et timelangt load).
3. **Source scope og delete scope udledes af samme skivemængde.** Kritisk:
   `build_delete_scope_predicate()` udleder i dag delete-scopet fra *hvad
   der ligger i ext*. Filtrerer niveau 2 en skive ud af kilden, mens
   delete-scopet stadig dækker den, soft-sletter `WHEN NOT MATCHED BY SOURCE`
   hele skiven i arkivet — samme fejlklasse.
   `ArchiveMergeConfig` har i dag kun `delete_scope_predicate` og skal have
   et tilsvarende `source_scope_predicate`; begge skal komme fra manifestet.
4. **Tomt manifest = erklæret no-op.** Er intet nyt fundet, lades ext urørt
   (den nyeste leverance bevares som debug-flade), og hvert arc-asset er et
   deklareret no-op i stedet for at re-merge ext's gamle indhold. Jobbet
   rapporterer SUCCESS, jf. beslutning 1.

`meta.DRGLeverance` udvides tilsvarende med `kadence`, `leverance_ts` og
`aktivitetsaar` i grainen (plus `cap_kilde`), så cap'en kan slås op pr.
skive (niveau 2's "hvad afspejler arkivet?"). `kildetabel` **bliver i
grainen** — ellers ville en delvist lykket kørsel (fx 5 af somatikkens 9
tabeller merget) registrere en cap, de øvrige ikke afspejler.

**Registret genopbygges, det migreres ikke.** DRG er en selvstændig løsning,
og arkivet kan tømmes og genindlæses fra leverancerne på sharet. De
eksisterende rækker beskriver leverancer merget under den gamle logik og har
hverken aktivitetsår eller pakke-timestamp; at backfille dem ville opfinde en
historik, der alligevel overskrives ved genindlæsningen — og backfill +
`ALTER COLUMN NOT NULL` + nøgleombygning er netop den slags migration, der kan
fejle halvvejs. Migrationen dropper derfor tabellen, og base-DDL'en genskaber
den med den nye definition; base-DDL kører efter migrationerne (håndhævet af
`tests/unit/test_infra_migrations_ordering.py`). Ændringen ligger fortsat i
base-DDL **og** migration i samme commit, idempotent og uden CURSOR (jf.
migrations-checklisten i `sql_etl_guide.md`).

Første kørsel bagefter har ingen registreret cap for nogen skive og merger
alt — hvilket er den rigtige opførsel, når arkivet alligevel genindlæses.

---

## Konsekvenser for de øvrige tasks

**DFM:** **tasken er i praksis tom som formuleret.** Designet endte
med udelukkende at bruge eksisterende DFM-funktionalitet —
`get_checkpoint`/`set_checkpoint` mod `("dfm", "<enhed>")` — så der er hverken
ny tabel, ny kolonne eller nyt API at bygge. Det eneste, der manglede, var en
aftalt nøgle- og værdiformat, og det hører hos den extraction-maskineri, der
bruger det (`DSI/connectors/delivery_detection.py`), ikke i
checkpoint-frameworket. Taskens DDL/migration-krav angår i stedet
`meta.DRGLeverance`s udvidede grain, som er leveret.
Gate-asset-valget skal fortsat bekræftes.

**extract:** `kadence` i `DrgExtractConfig` og kadencefilteret
(`("M", None)` hhv. `"U"`); niveau 1 efter `select_newest`; manifestet;
`source_scope_predicate` + justeret delete-scope; cap-baseret retningsværn i
stedet for pakkedato-sammenligning; asset-metadata pr. enhed med
besluttet/sprunget-over/superseded + leverance-id; cap-fallback til
filnavnets timestamp med `cap_kilde`-markering; kortslutning af
arkiv-assetsene på tomt manifest FØR views/guards/merge; opdeling og
takstsystem-scoping af `drg_borger`s CPR-opslag. Arkiv-invarianten fra
[internal ticket] er den farlige kant: en enhed må kun springes over, når INTET nyt
skal merges for den.

**schedule: LEVERET.** Tre ingest-jobs
(`drg_ingest_somatik_maaned_job`, `drg_ingest_somatik_uge_job`,
`drg_ingest_psyk_maaned_job`), to daglige kø-schedules kl. 20:00 (ét pr.
leverancetype) der hver bygger køen fra sharet og udsteder ÉN `RunRequest` på
køens hoved med resten i et run-tag, samt `drg_queue_sensor`
(`run_status_sensor` på SUCCESS) der fører køen videre. `run_key` pr.
(leverancetype, kadence, takstsystem, dato) og et in-flight-tjek i schedulet
dækker et M-load der løber over midnat. Ingen instans-konfiguration —
kæden serialiserer. Forældelses-checket har sit eget job og schedule kl.
07:00, uden for load-stien.

Run-config'ens op-nøgle er det bare navn (`ext_drg`), også på et
feature-branch-deployment: postfixet sidder på asset-nøglen, ikke på op'en.

**Efterfølgende: `drg_publish_sensor`.** Orkestreringen dækkede kun ingest.
`drg_publish_job` havde hverken schedule eller sensor og kunne derfor kun
startes i hånden — rækkefølgen mod Borger-kæden var i praksis operatørens valg.
Sensoren (`sensors/drg_publish_sensor.py`) fyrer jobbet på `ifdb_dfm_daily_job`
SUCCESS. Det er en ren rækkefølge-mekanisme: `drg_publish_job` materialiserer
IKKE `dim_Borger` (dens selection er de seks `drg_*`-grupper), så afhængigheden
er en kryds-job-kant, Dagster ikke håndhæver på kørselstidspunktet — et ekstra
cron ville kun flytte kapløbet.

**Ingen gates — bevidst afvigelse fra `haiba_publish_sensor`.** Forbilledet har
to: leverance-watermark og en plateau-bundet recovery på uopløste nøgler. Ingen
af dem giver mening her, fordi omkostningsprofilerne er modsatte. HAIBAs
dm-lag er en FULD genopbygning (~8M rækker udledt, ~6,7M publiceret til DSO), så
en kørsel på en nat uden leverance koster timer. DRG-publiceringen er delta hele
vejen: `pre_stg_DRGKontakt` scoper på `dw_timestamp_update > checkpoint` (hver
arc-tabel har et `IX_arc_*_DW_Timestamp_Update`, så scopet er seeks), og alt
efter den læser `stg.pre_DRGKontakt`. En nat uden leverance fører en tom mængde
gennem kæden. Der er ikke noget dyrt udfald at gate imod.

Recovery-gaten er overflødig af samme grund — men det krævede én ændring i
staging. `pre_stg_DRGKontakt` kører delta på et timestamp-checkpoint, og en stub
der mintes i Aidbox ændrer INTET i arc: arkivets merge sætter
`update_only_on_change=True`, så kontaktens `dw_timestamp_update` rykker sig
ikke. En -1 ville derfor være permanent indtil en manuel `mode=full_load`.
`build_unresolved_borger_scope_sql` bærer nu de uopløste kontakter som deres
eget delta-scope (`#uoploest_borger`, seek på `IX_Fct_DRGKontakt_Borger`), så
den ALMINDELIGE natlige publicering gen-opløser dem. En kontakt der opløses til
-1 igen koster intet nedstrøms: `fct_DRGKontakt`s MERGE er værnet af `WHEN
MATCHED AND EXISTS (SELECT src EXCEPT SELECT tgt)`, så rækken ikke opdateres og
dens `dw_timestamp_update` ikke rykkes — og DSO-synken er et watermark-delta på
netop den kolonne. Permanent uopløselige CPR'er giver derfor hverken
fact-skrivninger eller DSO-trafik, og der er intet kredsløb for en
plateau-detektion at stoppe.

**Nedre grænse: 14 dage (`UOPLOEST_LOOKBACK_DAGE`).** Uden den vokser scopet
kun — hver CPR der aldrig kan opløses, ville blive gen-staged hver nat for
evigt, og den natlige omkostning ville stige monotont. Grænsen måles på
FACTENS `dw_timestamp_update`, og netop den kolonne er det rigtige ur *fordi*
MERGE-værnet ovenfor står stille for en uændret række: uret løber fra
sidste reelle skrivning, så en uopløselig kontakt ældes selv ud af scopet efter
to uger, mens en der faktisk opløses forlader scopet alligevel (den er ikke -1
længere). Begge prædikater er indekseret (`IX_Fct_DRGKontakt_Borger`,
`IX_Fct_DRGKontakt_Watermark`). Prisen er eksplicit: er Borger-kæden brudt i
mere end vinduet, ældes de berørte rækker ud og selvhelbreder ikke — de kræver
en bevidst `mode=full_load`.

`#uoploest_borger` bygges i sin EGEN batch og sin egen `execute()`. Med
mssql-python når en fejl fra en senere sætning i en batch ikke Python, når en
tidligere sætning har afgivet et row count (samme mekanisme som de lydløst
fejlende migrationer). Delta-scopet åbner med to `SELECT ... INTO`, så en INSERT
lagt bagefter kunne fejle ned i en tom temp-tabel — og gen-opløsningen ville
stille holde op med at virke. Alene er INSERT'en batchens første
row-count-producent, og fejlen kommer frem.

Alternativet — en "afterburn", altså en målrettet UPDATE direkte på
`fct.DRGKontakt` hvor `dw_ek_Borger = -1` — blev overvejet og fravalgt. Den er
billigere pr. nat, men fact-rækken bærer kun kontaktens `clean_code`, ikke CPR
(GDPR: CPR staged aldrig), så den ville alligevel skulle tilbage i
`arc.drg_kontakt` for at slå op — altså et fct-lags-asset der læser `arc` — og
den ville duplikere opslagsreglen (`COALESCE(b, bt, -1)`) et andet sted. Den bør
tages op igen, hvis -1-populationen viser sig stor; den er ikke målt endnu.

Bemærk at `nightly_failure_sensor` ifølge sin egen docstring **ikke
rapporterer i øjeblikket** — transporten skal porteres til GitLab-API'et
efter migreringen — og at den kun rapporterer runs med automation-lineage.
Fejl-alarmen skal verificeres, ikke antages.

**HAIBA:** genbrug niveau 1 og det generiske
forældelses-check-asset; alt-eller-intet-kortslutning pga. arkiv-invarianten.

## Åbne punkter

1. **Superseded U.** Bliver en U helt subsumeret af en nyere M, afvises alle
   dens skiver i niveau 2. Dens niveau-1-checkpoint SKAL alligevel rykkes
   (filen er evalueret og indlæst), og tilstanden skal vises som
   "superseded" i asset-metadata, så den ikke læses som et fastlåst
   checkpoint.
2. **Cron-tidspunkt: LUKKET.** Køen starter 20:00 CET. Målingen blev ikke
   lavet; vinduet før 06:00 blev i stedet udvidet fra 5 til 10 timer, så en
   fuld M-cyklus kan nå at blive færdig uden at kende load-tiden præcist. En
   *målt* load-tid er stadig værd at have — den afgør, hvor tæt på grænsen vi
   ligger, når takstårene lægger sig oven på hinanden.
3. **Kæde-sensoren** er den eneste nye orkestreringsdel uden direkte
   forbillede i repoet (repoets `run_status_sensor`-mønstre starter et andet
   job, ikke den næste kørsel af samme job). Viser den sig besværlig, er en
   `concurrency_key`-post i instans-konfigurationen en ren erstatning for
   kædens andet trin.
4. **Somatik og psyk kører parallelt** — de deler ingen ext-tabeller. Det er
   bevidst, men det bør verificeres, at de to familier ikke tilsammen
   overskrider `max_concurrent_runs`.
5. **`enable_bulk_insert_on_first_load`** for DRG-arkivet: verificeret sikker
   (beslutning 5), men egen opgave — og recovery-modellen
   på staging bør slås op, inden gevinsten estimeres.

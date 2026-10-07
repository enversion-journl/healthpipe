<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# DRG-grupperet LPR: Beslutningsnotat om merge-nøgler

Status: KOMPLET 2026-08-26 — alle målinger gennemført (7 leverancer
profileret, 6 krydsmålinger). Grundlag: profileringskørsler med `drg_snapshot_profiler.py` (samme mappe) mod
leverancerne på `/mnt/ftproot/SDS_NGC_Sundhedsdata_DRG_GrupperetLPR/Fra-SDS/`.
Alle tal kan genskabes med scriptets `profile`- og `compare`-kommandoer.

## Baggrund

SDS leverer DRG-grupperet LPR som fulde snapshots pr. (leverancetype,
takstsystem). De heltallige ID'er i leverancerne (`ID`, `ID_INT`,
`PsykiatriForloeb_ID`, `DRGSGHFORLOB_ID`, børnetabellernes `ID` og
`*_ID`-referencer) er **per-leverance surrogater**: målt på to på hinanden
følgende psyk-leverancer var `ID_INT` omnummereret for 206.316 af 206.317
matchede kontakter og `PsykiatriForloeb_ID` for alle. SDS' egne
`DW_EK_*`/`DW_SK_*`-nøgler var stabile i måleperioden, men SDS garanterer dem
ikke, og de kan omnummereres ved genkørsler hos SDS. Arkivets merge-nøgler skal
derfor bygges på leverancens LPR3-UUID'er (`*_VAERDI`-kolonnerne) — og hvor de
mangler, på veldefinerede afledninger, som dette notat fastlægger.

Fordi samme kontakt vurderes under flere takstsystemer (M-leverancer dækker
tre aktivitetsår; grain-beslutning på [internal ticket]: alle kombinationer
medtages), indgår **takstsystem** i alle merge-nøgler. Takstsystem kommer
fra leverancens filnavn/mappenavn og stemples på alle rækker ved extract.

**Fact-grain deklareres (takstsystem, aktivitetsår, kontakt)** (review
2026-08-26): aktivitetsåret er entydigt pr. (takstsystem, kontakt) i en
leverance, så det udvider ikke rækketallet, men det gøres eksplicit på
facten (kolonne + partition-completeness pr. årsskive) fordi års-skiverne
bærer både arkiv- og kadence-semantikken. **Aktivitetsår indgår IKKE i
merge-nøglen**: en kontakts aktivitetsår kan revideres mellem leverancer
(3 tilfælde målt) — med året i nøglen ville revisionen skabe en dublet i
stedet for en opdatering.

## Merge-nøgler pr. arkivtabel

| Arkivtabel | Merge-nøgle | Grundlag |
|---|---|---|
| arc.drg_kontakt | (takstsystem, KONTAKT_VAERDI) | UUID unik og udfyldt i alle 7.491.528 rækker (T26 U 20260823); stabilitet på tværs af leverancer/takstsystemer: se §Stabilitet |
| arc.drg_sghforlob | (takstsystem, ind-kontaktens KONTAKT_VAERDI) | Forløbstabellen har ingen egen UUID; nøglen udledes ved join `DRGKONTAKT_ID_IND` → kontakt (0 manglende referencer intra-leverance). Unik pr. leverance: 6.349.305/6.349.305 (T26 U 20260823) |
| arc.drg_diagnose | (takstsystem, kontakt-UUID, DIAGNOSE, ART, SIDEANGIVELSE) — se §Diagnoser | DIAGNOSE_VAERDI er tom i samtlige rækker; kompositten er UNIK i målt leverance (8.628.544/8.628.544, T26 U 20260823) og bekræftet unik ved fuld T26-ingestion 2026-08-27 (0 dubletter) |
| arc.drg_diagnose_tillaeg | følger diagnosens strategi | intra-leverance `DIAGNOSE_ID`-reference |
| arc.drg_procedure | (takstsystem, PROCEDURE_VAERDI) | UUID unik og udfyldt i alle 25.562.168 rækker (T26 U 20260823) |
| arc.drg_procedure_tillaeg | (takstsystem, PROCEDURE_VAERDI, TILLAEGSKODE_PROCEDURE) | tillæg hænger på procedure-registreringen via intra-leverance `PROCEDURE_ID`; unikhed verificeres i US-2 |
| arc.drg_interne_koder | (takstsystem, kontakt-UUID, INTERN_TYPE, INTERN_KODE) | kun arkiv i v1. **IKKE unik**: målt 2026-08-27 i drift — 2.898 grupper / 25.996 overskydende rækker (22,1 %), alle byte-identiske. Se §Kildedubletter |
| arc.drg_takstbeskrivelse | (takstsystem, DRG) | takstkatalog, 924 rækker |
| arc.drg_leveranceinfo | insert_only-log (takstsystem, leverance_dato) | 1 række pr. leverance |
| arc.drgpsyk_kontakt | (takstsystem, KONTAKT_VAERDI) | UUID unik og udfyldt i alle 228.928 rækker (T26 M 20260810); 100 % stabil over leverancer (206.317 matchede, 0 ændringer) |
| arc.drgpsyk_forloeb | (takstsystem, første kontakts KONTAKT_VAERDI) | udledes via `DW_EK_Kontakt_foerste` → kontakt-join intra-leverance; unik i begge målte leverancer, 0 manglende referencer |
| arc.drgpsyk_diagnose (+tillaeg) | (takstsystem, kontakt-UUID, Diagnose, ART, SIDEANGIVELSE) — som somatik | komposit målt UNIK i begge profilerede psyk-leverancer (556.849 og 874.218 rækker); ved fuld ingestion 2026-08-27 fandtes 1 byte-identisk dublet i T24 — se §Kildedubletter |
| arc.drgpsyk_procedure (+tillaeg) | (takstsystem, PROCEDURE_VAERDI) | PROCEDURE_VAERDI findes i psyk-skemaet; somatik-pendanten er målt unik på op til 46,6M rækker — psyk verificeres i US-2 |
| arc.drgpsyk_betalingsoplysninger | (takstsystem, kontakt-UUID, Bet_opl_starttidspunkt) | kun arkiv i v1; unikhed verificeres i US-2 |

**Generel regel (skærpet efter review 2026-08-26):** de leverance-interne
heltals-ID'er og SDS' `DW_EK_*`/`DW_SK_*`-nøgler **udelades helt af arc**.
Efter en merge ville arc indeholde rækker fra forskellige leverancer, og de
volatile ID'er ville være ude af sync på tværs af tabeller — en join på dem
ville give forkerte resultater. Alle relationer opløses derfor til UUID'er
ved extract (kontakt-UUID stemples på diagnose-/procedure-/forløbsrækker i
ext, målt 0 manglende referencer), og kun UUID-referencerne persisteres i
arc. ext beholder kilden 1:1 (inkl. alle ID'er) til fejlsøgning af den
seneste leverance. Én undtagelse: psyk-kontaktens/-forløbets
`DW_EK_Helbredsforloeb` beholdes som ANALYTISK ATTRIBUT (aldrig join-nøgle),
da psyk-skemaet ikke har FORLOB_VAERDI-UUID'en — forbeholdet dokumenteres på
kolonnen (77 af 206.317 ændrede værdi mellem to leverancer). Somatik bruger
FORLOB_VAERDI (UUID, 1:1 med DW_EK_HelbredsForloeb) i stedet.

## Stabilitet af KONTAKT_VAERDI (grundnøglen)

- Psyk, leverance → leverance (M 20260710 → M 20260810): 206.317 matchede
  UUID'er, **0 ændringer** i identitet; `DW_EK_Kontakt` og `DW_EK_Borger`
  uændrede for alle matchede. 315 kontakter udgik, 22.611 kom til, 56 fik
  revideret indtidspunkt — normalbilledet for revisioner.
- Somatik, leverance → leverance (M/U 16/8 → U 23/8): 7.393.644 matchede
  UUID'er, **0 identitetsændringer**; `ID` omnummereret for ALLE; reel
  attributdrift 0,04 % (DRG/pris-revisioner). Kun 97.884 nye, 4.539.910
  "forsvundne" = 2024-årgangen (U-indhold dækker kun 2 aktivitetsår — se
  §Kadence nedenfor).
- Somatik, på tværs af takstsystemer (T25-M vs. T26-M, samme LPR-kørsel):
  **9.143.287 fælles = eksakt 2024- + 2025-årgangene**; kun-T25 = eksakt
  2023-årgangen; kun-T26 = eksakt 2026-årgangen. 91 % af fælles kontakter
  har forskellig totalpris, 3,7 % skifter DRG → (takstsystem,
  KONTAKT_VAERDI) er BEKRÆFTET som arkivnøgle.
- Psyk, på tværs af takstsystemer: kun 1.053 fælles UUID'er — psyk-
  leverancer dækker i praksis disjunkte aktivitetsår; samme nøglemodel.

## Kadence og arkiv-enhed (vigtig konsekvens)

M-leverancer indeholder TRE aktivitetsår (t-2, t-1, t), U-leverancer kun TO
(t-1, t) — målt eksakt på årgangstallene. Forretningsreglen "nyeste fil pr.
(leverancetype, takstsystem) uanset kadence" kombineret med naiv
delete-detection ville derfor soft-slette det ældste aktivitetsår ved hver
U-leverance og genoplive det ved næste M.

**Arkiv-enheden er (takstsystem × aktivitetsår):** delete-detection scopes
til de aktivitetsår, leverancen faktisk indeholder — en U-leverance
opdaterer to årsskiver, en M tre. Implementeres i US-2 som
scoped NOT MATCHED BY SOURCE (eller tilsvarende) pr. årsskive; leverancens
aktivitetsår-fordeling kendes fra extract-checket.

## Forløbsidentitet

**Somatik:** `FORLOB_VAERDI` på kontakterne er IKKE en sygehusforløbs-nøgle —
den er **helbredsforløbets UUID**: 2.431.116 distinkte værdier og præcis 1:1
med `DW_EK_HelbredsForloeb` (2.431.116 distinkte par), målt på T26 U
20260823. Den grupperer altså på tværs af sygehusforløb (deraf de 6.486.124
(forløb, FORLOB_VAERDI)-par over 6.349.305 forløb) og gemmes i arc som den
stabile helbredsforløbs-identitet. Sygehusforløbets identitet defineres i
stedet som **ind-kontaktens KONTAKT_VAERDI**:

- `DRGKONTAKT_ID_IND`/`_UD` har perfekt intra-leverance-integritet
  (0 manglende referencer på 6.349.305 forløb).
- Unik i SAMTLIGE profilerede leverancer (op til 11.520.047 forløb, T25-M).
- Re-keying målt: 649 af 3.826.063 forsvundne forløb mellem to
  somatik-leverancer havde stadig ind-kontakten (0,01 %); psyk: 22 af 274.
  Randfænomen — håndteres som soft-delete + ny nøgle (korrekt adfærd).
- Nøglen holder på tværs af takstsystemer (7.711.784 fælles forløb T25↔T26).

**OBS — tungeste vs. udfaldsgivende:** flagene markerer hver præcis én
kontakt pr. forløb, men IKKE altid den samme: 744.996 kontakter (~5,9 % af
forløbene) har forskellig tungeste- og udfaldsgivende kontakt. Beslutningen
(forløbsmål på TUNGESTE kontakt) er entydig; udfaldsgivende-flaget bevares
som attribut i egenskabsdimensionen.

**Konsekvens for arkivet:** et forløb, hvis ind-kontakt ændres mellem to
leverancer, optræder som soft-delete af den gamle nøgle + insert af en ny.
Det er korrekt adfærd: forløbet ER redefineret af SDS' gruppering.
Re-keying-raten er målt til 0,01 % — et randfænomen uden behov for særskilt
håndtering.

**Psyk:** samme princip med `DW_EK_Kontakt_foerste` → første kontakts
KONTAKT_VAERDI. Bemærk at joinen bruger SDS' DW-nøgle INTRA-leverance (samme
leverance, konsistent univers) — det er tilladt; kun kryds-leverance-joins på
DW-nøgler er forbudt. Målt: unik i begge psyk-leverancer (218.825/218.825 og
353.498/353.498), 0 manglende referencer, re-keying 22 af 274.

**DW_EK_Helbredsforloeb** (begge leverancetyper) er IKKE sygehusforløbet:
32.636 distinkte værdier over 218.825 psyk-forløb (1:N, helbredsforløb samler
flere sygehusforløb). Den gemmes 1:1 i arc som attribut, indgår ikke i nøgler,
og kan senere bruges analytisk (fx forløbssammenkædning på tværs af sygehuse).

## Diagnoser: komposit-merge (anbefaling, revideret 2026-08-26)

`DIAGNOSE_VAERDI` er tom i samtlige 8.628.544 somatik-rækker, og kolonnen
findes slet ikke i psyk-skemaet. Men kompositten (kontakt, diagnosekode,
art, sideangivelse) er målt **unik** i den nyeste leverance
(8.628.544 af 8.628.544, T26 U 20260823).

**Anbefaling: komposit-merge** på (takstsystem, kontakt-UUID, DIAGNOSE, ART,
SIDEANGIVELSE):

1. Genbruger den eksisterende merge-motor uændret (upsert + delete-detection
   + change detection) og giver revisionsspor på rækkeniveau.
2. Kontakt-UUID'en opløses ved extract via leverancens interne
   `DRGKONTAKT_ID`-reference (0 NULL, 0 manglende referencer målt).
3. Værn: `dedup_source_on_keys=True` + et DQ-check der FEJLER hvis en
   leverance indeholder dubletter på kompositten — unikheden er en målt
   egenskab, ikke en garanti fra SDS. Observeres dubletter, falder vi
   tilbage til genopbygning pr. kontakt (scoped delete pr. kontakt +
   deterministisk registreringsindeks over en fast sortering).

Tillægskoder denormaliseres på registreringsrækken via intra-leverance
`DIAGNOSE_ID`-referencen (én-til-få).

Samme strategi for psyk-diagnoser — kompositten er målt unik i begge
psyk-leverancer. `arc.drg_interne_koder` og
`arc.drgpsyk_betalingsoplysninger` verificeres tilsvarende i US-2.

## Kildedubletter: målt i drift 2026-08-27

Første fulde ingestion på dev-miljøet afkræftede antagelsen om, at
kompositterne er unikke i alle leverancer. **SDS leverer samme række flere
gange** — byte for byte identisk, ikke konfliktende:

| Tabel | Leverance | Dublet-grupper | Overskydende rækker | Andel |
|---|---|---|---|---|
| `arc.drgpsyk_diagnose` | T24 psyk M 20260812 | 1 | 1 | 0,00004 % |
| `arc.drg_interne_koder` | T26 somatik U 20260824 | 2.898 | 25.996 | **22,1 %** |

`drg_interne_koder` er den vigtige: 117.765 ext-rækker blev til 91.769 i
arkivet. Dubletterne er ikke et randfænomen, men et strukturelt træk ved
datasættet — (kontakt, INTERN_TYPE, INTERN_KODE) er simpelthen ikke unik
hos SDS, fordi samme interne kode kan registreres flere gange på én kontakt
uden at registreringerne skelnes fra hinanden.

Profileringen kunne ikke se det: den målte unikhed pr. enkelt leverance på
de nøgler vi *forventede*, og `INTERNE_KODER` indgik ikke i nøgleanalysen,
fordi datasættet kun skal i arkivet i v1.

**Konsekvens for det oprindelige værn.** Notatet foreskrev
`dedup_source_on_keys=True` som fast indstilling. Det er nu erstattet af en
guard, der skelner — fast dedup ville have skjult en konfliktende dublet,
og en guard der fejlede på *enhver* dublet efterlod hele tabellen tom
(målt: `arc.drgpsyk_diagnose` fik 0 af 2,25 mio. rækker):

- **Konfliktende** (samme nøgle, forskellige værdier) → kørslen fejler.
  Fallback i dette notat aktiveres bevidst. Ingen observeret endnu.
- **Byte-identiske** → motorens deterministiske ROW_NUMBER-dedup slås til
  for den enkelte kørsel, efter guarden har bevist at der ikke er en
  konflikt at skjule. Antallet rapporteres i asset-metadata
  (`identiske_kildedubletter`) og logges som warning.

**Åbent spørgsmål til US-4/US-5:** hvis `INTERNE_KODER` senere skal bruges
nedstrøms (i dag kun arkiv, jf. spm. 10), er 22 % dubletter et
modelleringsspørgsmål, ikke kun et arkivspørgsmål — enten skal grain'en
udvides med en registreringsdiskriminator, eller også skal koderne
behandles som en mængde pr. kontakt.

## Procedurer: komposit-merge på UUID

`PROCEDURE_VAERDI` er unik og udfyldt (25.562.168/25.562.168 i T26 U
20260823) → almindelig upsert-merge på (takstsystem, PROCEDURE_VAERDI) med
change detection og delete detection, som kontakterne.

## Konsekvenser for US-2 (ArchiveMergeConfig)

- Kontakt/procedure/forløb/diagnoser: HAIBA-mønstret (upsert +
  detect_deletes + update_only_on_change) med nøglerne ovenfor. Værnet mod
  dubletter er IKKE fast `dedup_source_on_keys` som oprindeligt foreslået,
  men den skelnende guard fra [internal ticket] (se §Kildedubletter) — dedup slås
  kun til for den enkelte kørsel, og kun for byte-identiske dubletter.
- Fallback for diagnoser (kun hvis dubletter observeres): genopbygning pr.
  kontakt — kræver enten `core/archive_merge.py`-udvidelse (scoped replace)
  eller et dedikeret merge-mønster i drg_archive.py. Beslutning ligger i
  US-2.
- Delete-detection skal scopes pr. (takstsystem × AKTIVITETSÅR) — jf.
  §Kadence og arkiv-enhed ovenfor samt arkiv-invariant 1 på [internal ticket].
  Skemavariation: T25-somatik mangler DW_EK_HelbredsForloeb og
  FORLOB_REFERENCE_* — arc-DDL og extract-validering skal være pr.
  takstsystem-skema (Metadata_*-filerne er autoritative).
- **Delta-propagation på tværs af tabeller (review 2026-08-26):** ændres/
  tilføjes/slettes en diagnose- eller procedurerække, skal HELE kontakten
  (og forløbsmål-bæringen) med i staging-deltaet — børnenes
  `dw_timestamp_update` i arc driver ikke automatisk kontaktens. Delta-
  scope i pre_stg/stg og bridges
  defineres derfor som: kontakter hvor kontakten SELV, en af dens
  diagnoser/procedurer/tillæg ELLER dens forløb har ændret sig siden
  checkpoint — joinet på kontakt-UUID.
- **Bridges bygges fra børnetabellerne, ikke de fladgjorte DIA01-20/
  PROC01-40-kolonner** (review 2026-08-26): børnetabellerne bærer
  attributter, de fladgjorte kolonner mangler (diagnose: art +
  sideangivelse; procedure: side/handlingsspec/kontrast/tidspunkter/
  producerende enhed + UUID), tillægskoderne er kun koblet til deres
  konkrete diagnose/procedure via børnetabellernes referencer (TPROC/TDIA
  på kontakten kan ikke udtrykke koblingen — 5,3M procedurer har >1 tillæg,
  max 16), og de fladgjorte kolonner er kapacitetsbegrænsede (målt T26-M:
  104.027 kontakter har >40 procedurer, max 5.260; 303.791 kontakter har
  >10 proceduretillæg, max 1.506; 7 kontakter har >20 diagnoser). De
  fladgjorte kolonner bruges alene som konsistens-stikprøve —
  se rapportens §10.1.
- **Leverance-identifikation:** arc-rækker stemples med leverance_dato,
  `meta.DRGLeverance` logger hvilken fil arkivet afspejler pr.
  (leverancetype, takstsystem), og `arc.drg_leveranceinfo` er en
  insert_only-log af LPROPDATERINGSDATO. Fuld tilstands-historik pr.
  månedsleverance (tidsrejse) er bevidst ude af v1 — kan tilføjes senere
  som insert_only-snapshots, hvis behovet opstår.
- **dim.Proceduregruppe (RN's DW_SK_DRGProceduregruppe):** ikke nødvendig
  for at bevare grain — procedurer analyseres via bridgen. Leverancen har
  ingen eksplicit proceduregruppe-kolonne; RN's felt må være afledt (fx af
  den grupperingsdrivende procedure). Kan efterlignes senere som afledt
  attribut i klassifikationsdimensionen, hvis et konkret rapporteringsbehov
  opstår — kardinalitetseffekten kan måles med profileren.

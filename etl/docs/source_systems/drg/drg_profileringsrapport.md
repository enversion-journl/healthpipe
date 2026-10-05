<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Profileringsrapport: DRG-grupperet LPR fra SDS

[internal ticket] ([internal ticket] "SDS DRG-grupperet LPR (SDSDRG) version 1.0").
Første version 2026-08-26. Alle tal er målt med [`drg_snapshot_profiler.py`](drg_snapshot_profiler.py)
i samme mappe og kan genskabes — se §Metode. Merge-nøgle-konklusionerne er
samlet i [beslutningsnotatet](drg_noegler_beslutningsnotat.md).

## 1. Formål og omfang

Rapporten fastlægger faktagrundlaget for ingestion og
modellering af DRG-leverancerne:

1. Hvilke kolonner er stabile naturlige nøgler — og hvor stabile er de på
   tværs af leverancer, kadencer (M/U) og takstsystemer?
2. Hvad identificerer sygehusforløb og diagnoser, som ingen UUID har?
3. Hvad indeholder M- og U-kadencerne, og holder SDS' egne kontroltotaler
   (SDP_ExportLog)?
4. Hvilke kontakt-attributter er funktionelt afhængige af (takstsystem,
   DRG-kode) og dermed kandidater til `dim.DRG` frem for
   transaktionsdimensionen?

## 2. Metode og genkørsel

Alt er målt med det medfølgende stdlib-only script, der bevidst holder
hukommelsesforbruget under ~1 GB (disk-bucketede unikheds- og join-målinger),
så det kan køre på code-server-allokeringen. CPR-kolonner læses aldrig.

```bash
# 1) Leveranceinventar — læser kun metadata/eksportlog fra hvert tar-arkiv
python3 drg_snapshot_profiler.py inventory --base-dir "$DRG_BASE_DIR" --out inventory.json

# 2) Profilér én udpakket leverance (skriver nøgledumps + profile.json)
tar -xjf "$DRG_BASE_DIR/Region_1083_DRG_T26_U_20260824190546.tar.bz2" -C /tmp/u24/
python3 drg_snapshot_profiler.py profile --dir /tmp/u24/T26 --dump-dir /tmp/dumps/u24

# 3) Sammenlign to leverancer (stabilitet/overlap/re-keying)
python3 drg_snapshot_profiler.py compare --a /tmp/dumps/u17 --b /tmp/dumps/u24
```

Kørt på `dagster-code-server-development` (mount `/mnt/ftproot/...`, se
KONTEKST-blokken på [internal ticket]). OBS: allokeringens `/tmp` ryddes ved
redeploy — gem outputs løbende.

## 3. Leverancerne

Placering: `/mnt/ftproot/SDS_NGC_Sundhedsdata_DRG_GrupperetLPR/Fra-SDS/`
(read-only SMB; `Arkiv/` indeholder T23/T24-historik, ude af scope).

Filmønster: `Region_1083_<DRG|DRGPSYK>_<T24|T25|T26>[_<M|U>]_<timestamp>.tar.bz2`

- **DRG** (somatik): månedlig `_M` (~3,4–3,9 GB bz2; enkelt outlier 9,4 GB
  2026-03-17) og ugentlig `_U` (~1,7–3,9 GB). **DRGPSYK**: kun `_M`
  (~8 MB voksende til ~200 MB gennem takstårets levetid).
- Et takstsystems M-leverancer dækker TRE aktivitetsår (t-2, t-1, t: T26-M =
  2024+2025+2026), U-leverancer kun to (t-1, t: T26-U = 2025+2026) — målt i
  §5/§8. Kryds-takstsystem-overlappet følger deraf (§7).
  Takstsystemer udgår af leverancerne, når takståret lukkes — men IKKE
  synkront på tværs af leverancetyper: somatik-T24 fik sidste fil
  2026-03-17, mens psyk-T24 stadig leveres (seneste 2026-08-12).
- Besluttet forretningsregel: den gældende fil pr.
  (leverancetype, takstsystem) er den NYESTE uanset kadence.

Hver tar.bz2 indeholder én mappe (`T2x/`) med semikolonseparerede, fuldt
quotede CSV-filer i cp1252 med header, plus pr. datasæt en `Metadata_*.csv`
(SDS' kolonneskema: navn, type, nullability) og `SDP_ExportLog_*.csv` med
rækkeantal + bytes pr. fil samt `LEVERANCEINFO` (dataområde +
LPR-opdateringsdato).

## 4. Leveranceinventar

`inventory`-kommandoen skannede alle 113 leverancer i Fra-SDS-mappen
(eksportlog læst fra hvert arkiv uden fuld udpakning). Kondenseret:

| Serie | Leverancer | Periode | Rækker (første → sidste) |
|---|---|---|---|
| DRG T24 (uden kadence → M) | 4 + 4 | 2025-08-12 → 2026-03-17 | 106,0M → 106,1M (stabil — takståret lukket) |
| DRG T25 (uden kadence → M) | 4 + 9 | 2025-08-13 → 2026-08-18 | 96,2M → 111,3M |
| DRG T25 U | 26 | 2025-08-04 → 2026-03-09 | 59,1M → 74,8M (U-serien stoppede ved takstårsskiftet) |
| DRG T26 M | 11 | 2025-12-14 → 2026-08-18 | 69,5M → 97,5M |
| DRG T26 U | 21 | 2026-02-25 → 2026-08-24 | 42,9M → 61,1M |
| DRGPSYK T24 M | 13 | 2025-08-11 → **2026-08-12** | 3,57M → 3,58M |
| DRGPSYK T25 M | 13 | 2025-08-11 → 2026-08-12 | 2,18M → 3,61M |
| DRGPSYK T26 M | 8 | 2026-01-14 → 2026-08-12 | 0,19M → 2,28M |

Vigtige observationer:

- **Psyk T24 leveres STADIG** (seneste 2026-08-12) — det er kun *somatik*
  T24, der stoppede 2026-03-17. Arkivet skal altså håndtere, at
  leverancetyper udfaser takstsystemer på forskellige tidspunkter.
- Nye takstsystemer starter småt og vokser (psyk T26: 188K → 2,3M rækker
  over 7 måneder) — rækkeantal-drift-checks skal tage højde for det.
- U-serier hører til det aktive takstår og stopper ved takstårsskiftet.

## 5. Nøgleprofil pr. leverance

### 5.1 Somatik T26 U 20260823 (nyeste U ved målingen)

Eksportlog-verifikation: alle 9 datafiler matcher SDP_ExportLog EKSAKT
(CSV-bevidst gennemtælling). Kontroltotalerne er dermed anvendelige som
hårdt indlæsningsværn i [internal ticket].

| Måling | Resultat |
|---|---|
| DRGKONTAKT rækker | 7.491.528 |
| `ID` distinkte | 7.491.528 → unik (pr. leverance) |
| `KONTAKT_VAERDI` distinkte / tomme | 7.491.528 / 0 → **unik og udfyldt** |
| `FORLOB_VAERDI` tomme | 0 |
| `FORLOB_REFERENCE_VAERDI` tomme | 3.598.971 (48 %) |
| Aktivitetsår | 2025: 4.611.118 · 2026: 2.880.410 |
| `TUNGESTE_DRGKONTAKT` ≠ `UDFALDSGIVENDE_KONTAKT` | 744.996 rækker (~5,9 % af forløbene har forskellig tungeste- og udfaldsgivende kontakt — flagene har samme antal 1-taller, én pr. forløb, men peger ikke altid på samme kontakt) |
| `MDC` ≠ `DRG[0:2]` | 2.420.196 rækker (32 %) — MDC kan IKKE udledes af DRG-koden |
| DRGSGHFORLOB rækker / `ID` distinkte | 6.349.305 / 6.349.305 |
| Forløbsnøgle (ind-kontaktens UUID) distinkte | **6.349.305 af 6.349.305 → unik**; 0 manglende ind-/ud-referencer |
| (forløb, FORLOB_VAERDI)-par | 6.486.124 (≠ 6.349.305 → FORLOB_VAERDI er ikke forløbsnøgle) |
| `FORLOB_VAERDI` distinkte / (FORLOB_VAERDI, DW_EK_HelbredsForloeb)-par | 2.431.116 / 2.431.116 → **1:1 med helbredsforløbet** — FORLOB_VAERDI er LPR3-(helbreds)forløbets UUID |
| `FORLOB_REFERENCE_TYPE` | ALAA01: 2.233.952 · ALAA03: 1.158.310 · ALAA02: 465.705 · ALAA04: 43.424 · tom: 3.590.137 |
| DIAGNOSER rækker / `DIAGNOSE_VAERDI` tomme | 8.628.544 / 8.628.544 (**alle tomme**) |
| Diagnose-komposit (kontakt, kode, art, side) distinkte | **8.628.544 af 8.628.544 → UNIK** (ART: ALGA01 6.354.356 · ALGA02 2.274.188) |
| PROCEDURE rækker / `PROCEDURE_VAERDI` distinkte / tomme | 25.562.168 / 25.562.168 / 0 → **unik og udfyldt** |

### 5.2 Alle profilerede somatik-leverancer (nøglerne holder overalt)

| Leverance | Kontakter | Aktivitetsår | Forløb | Diagnoser | Procedurer |
|---|---|---|---|---|---|
| T26 U 20260823 | 7.491.528 | 2025: 4.611.118 · 2026: 2.880.410 | 6.349.305 | 8.628.544 | 25.562.168 |
| T26 U 20260810* | 7.239.387 | 2025: 4.611.111 · 2026: 2.628.276 | 6.134.040 | 8.336.830 | 24.732.203 |
| T26 U 20260816* | 11.933.554 | 2024: 4.532.162 · 2025: 4.611.125 · 2026: 2.790.267 | 10.092.752 | 13.747.776 | 40.829.782 |
| T26 M 20260816 | 11.933.554 | (identisk med U 20260816 — samme LPR-kørsel) | 10.092.752 | 13.747.776 | 40.829.782 |
| T25 M 20260816 | 13.667.989 | 2023: 4.524.702 · 2024: 4.532.162 · 2025: 4.611.125 | 11.520.047 | 15.615.367 | 46.649.827 |

\* filnavnets timestamp er pakketidspunktet; LPROPDATERINGSDATO i parentes-datoerne her.
I ALLE leverancer: `ID`, `KONTAKT_VAERDI`, forløbsnøglen (ind-kontaktens UUID),
diagnose-kompositten og `PROCEDURE_VAERDI` er 100 % unikke; ingen tomme
UUID'er; 0 manglende intra-leverance-referencer. `TUNGESTE ≠ UDFALDSGIVENDE`
konsistent på ~10 % af kontakterne (720K-1,36M pr. leverance).
**Skemavariation:** T25-skemaet har 149 kolonner — `DW_EK_HelbredsForloeb`,
`FORLOB_REFERENCE_VAERDI` og `FORLOB_REFERENCE_TYPE` findes kun i T26 (152).
Extract skal validere pr. (takstsystem)-skema via leverancens Metadata_*-filer.

### 5.3 Psyk T26 M 20260810 (nyeste M ved målingen)

| Måling | Resultat |
|---|---|
| Kontakt rækker | 228.928 |
| `ID_INT`, `ID` (hex-GUID), `KONTAKT_VAERDI`, `DW_EK_Kontakt` | alle unikke (228.928 distinkte) |
| `KONTAKT_VAERDI` tomme | 0 |
| (CPR, indtidspunkt, enhed)-komposit | 228.555 distinkte → 373 dubletter (**dur ikke som nøgle**) |
| Forløb rækker / `PsykiatriForloeb_ID` distinkte | 218.825 / 218.825 |
| `DW_EK_Helbredsforloeb` distinkte | 32.636 (1:N — helbredsforløb ≠ sygehusforløb) |
| (CPR, indtid-samlet, sygehus, type)-komposit | 218.807 → 18 dubletter (dur ikke) |
| Forløbsnøgle (første kontakts UUID) | **218.825 af 218.825 → unik**; 0 manglende referencer |
| Diagnose-komposit (kontakt, kode, art, side) | **556.849 af 556.849 → UNIK** |
| Psyk T25 M 20260810 (kontrolleverance) | 369.147 kontakter / 353.498 forløb — samme unikhed hele vejen (diagnose-komposit 874.218/874.218) |

## 6. Stabilitet på tværs af leverancer

### 6.1 Psyk M 20260710 → M 20260810

| Måling | Resultat |
|---|---|
| Matchede `KONTAKT_VAERDI` | 206.317 |
| Kun i juli (udgået/revideret væk) | 315 |
| Kun i august (nye) | 22.611 |
| `ID_INT` ændret blandt matchede | **206.316 (99,9995 %)** — omnummereres hver leverance |
| `PsykiatriForloeb_ID` ændret | **206.317 (alle)** |
| `DW_EK_Kontakt` / `DW_EK_Borger` ændret | 0 / 0 (stabile i måleperioden — men uden garanti) |
| CPR ændret / indtidspunkt ændret | 0 / 56 |
| Forløb matchet på (CPR, indtid, sygehus, type) | 197.000; `PsykiatriForloeb_ID` ændret for alle; `DW_EK_Helbredsforloeb` ændret for 152 |

**Konklusion:** heltals-ID'erne er per-leverance surrogater; KONTAKT_VAERDI er
identitetsbærende og 100 % stabil.

### 6.2 Somatik M/U 20260816 → U 20260823 (nyeste leverancepar)

U 20260817-filen og M 20260818-filen er indholdsidentiske (samme LPR-kørsel,
16/8) — de to compares gav linje-for-linje samme resultat:

| Måling | Resultat |
|---|---|
| Matchede `KONTAKT_VAERDI` | 7.393.644 |
| Kun i 16/8-indholdet | 4.539.910 (≈ 2024-årgangen: U-indhold dækker kun 2 aktivitetsår, se §8) |
| Kun i U 23/8 (nye) | 97.884 |
| `ID` ændret blandt matchede | **7.393.644 (alle)** — somatik-ID'er omnummereres også hver leverance |
| DRG / totalpris ændret | 2.920 / 2.922 (0,04 % — reelle revisioner) |
| Ind-/udtidspunkt ændret | 282 / 612 |
| `DW_EK_HelbredsForloeb` ændret | 1.296 |
| Forløb matchet på ind-kontakt-UUID | 6.266.689; DRG/pris-drift 3,7K; antal-drift 2,1K |
| **Forløbs-re-keying** | **649 af 3.826.063 forsvundne forløb havde stadig ind-kontakten i B (0,01 %)** — re-keying er et randfænomen |

### 6.3 Somatik U 20260802 → U/M 20260816 (kontrolpar hen over "outlieren")

| Måling | Resultat |
|---|---|
| Matchede | 7.229.872 · kun i 2/8: 9.515 · kun i 16/8: 4.703.682 (2024-årgangen + ugens tilvækst) |
| `ID` ændret | alle · DRG/pris-drift: 3.874 · aktivitetsår ændret: 3 (kontakter KAN flytte aktivitetsår ved revision) |
| Forløbs-re-keying | 567 af 6.086 forsvundne |

Størrelses-"outlieren" U 20260817 er altså ikke en defekt: den indeholdt
månedsindholdet (3 aktivitetsår) — se §8.

## 7. På tværs af takstsystemer: T25 vs. T26

Compare af T25-M mod T26-M (begge LPR-kørsel 16/8) — aritmetikken går
eksakt op i aktivitetsårgange:

| Måling | Resultat |
|---|---|
| Matchede `KONTAKT_VAERDI` | **9.143.287 = 2024-årgangen (4.532.162) + 2025-årgangen (4.611.125), eksakt** |
| Kun i T25 | 4.524.702 = præcis 2023-årgangen |
| Kun i T26 | 2.790.267 = præcis 2026-årgangen |
| Totalpris ændret blandt fælles | **8.339.754 (91 %)** — samme kontakt, ny takst |
| DRG ændret blandt fælles | 335.049 (3,7 %) — grouper-ændringer mellem takstår flytter kontakter til andre DRG |
| Forløb fælles på ind-kontakt-UUID | 7.711.784 — forløbsnøglen holder også på tværs af takstsystemer |

**KONTAKT_VAERDI er 100 % stabil på tværs af takstsystemer**, og
(takstsystem, KONTAKT_VAERDI) er dermed bekræftet som arkivnøgle. Grain-
beslutningen (alle kombinationer) betyder i praksis op til TRE
takstsystem-vurderinger pr. kontakt (fx 2024-aktivitet i T24, T25 og T26);
fct.DRGKontakt får i størrelsesordenen ~40M rækker for T24-T26.

**Psyk afviger:** psyk-T25 og psyk-T26 deler kun 1.053 kontakt-UUID'er —
psyk-leverancerne dækker i praksis disjunkte aktivitetsår (T26 = 2026-
aktivitet alene; psyk-kontakten har ingen AKTIVITETSAAR-kolonne). Modellen
er den samme, men psyk-kontakter optræder typisk kun under ét takstsystem.

## 8. M- vs. U-kadencens indhold

Inventaret afgør spørgsmålet på filniveau (T26-serien, kontaktrækker):

- **M-leverancerne er systematisk større end U**: M ligger på 9,7–11,9M
  kontakter, U-serien vokser jævnt 5,25M → 7,49M uge for uge. U er altså
  IKKE det fulde M-indhold, men en delmængde/anden afgrænsning.
- **U 2026-08-17 = M 2026-08-18 på rækkeniveau** (11.933.554 kontakter,
  13.747.776 diagnoser, 40.829.782 procedurer — identisk på alle datasæt):
  den formodede "U-outlier" var månedsindholdet pakket som U-fil. Samme
  mønster ses ved U 2026-03-10 = M 2026-03-13 samt identiske
  gentagelses-leverancer (M dec 2025 = M jan 2026; M 17/2 = M 20/2):
  der er ét underliggende LPR-opdateringsspor, og kadencerne er
  pakketidspunkter — hvilket understøtter forretningsreglen "nyeste fil
  uanset kadence" identitetsmæssigt.
- **Forklaringen er aktivitetsår-dækningen** (målt i §5): M-leverancer
  indeholder TRE aktivitetsår (t-2, t-1, t), U-leverancer kun TO (t-1, t).
  Compare M/U-16/8 → U-23/8: 4.539.910 kontakter fandtes kun i M-indholdet
  ≈ 2024-årgangen (4.532.162) + ugens revisioner; kun 97.884 var nye i U.
- **Konsekvens for arkivet**: "nyeste fil uanset kadence"
  kombineret med naiv delete-detection ville soft-slette det ældste
  aktivitetsår ved hver U-leverance og genoplive det ved næste M.
  **Arkiv-enheden skal derfor være (takstsystem × aktivitetsår)**:
  delete-detection scopes til de aktivitetsår, leverancen faktisk
  indeholder — en U-leverance opdaterer to årsskiver, en M-leverance tre.
  Aktivitetsår-fordelingen pr. leverance rapporteres allerede af
  extract-checket og definerer skiverne.

## 9. Forløbsidentitet

Somatik-forløbstabellen (DRGSGHFORLOB) har ingen UUID. Tre kandidater er målt:

1. **`FORLOB_VAERDI`** (på kontakterne): udfyldt overalt, men IKKE en
   sygehusforløbsnøgle — 6.349.305 forløb ↔ 6.486.124 distinkte
   (forløb, FORLOB_VAERDI)-par (~2,2 % af forløb spænder over flere).
   Målingen afgør semantikken: 2.431.116 distinkte FORLOB_VAERDI og præcis
   2.431.116 distinkte (FORLOB_VAERDI, DW_EK_HelbredsForloeb)-par → kolonnen
   er **LPR3-(helbreds)forløbets UUID** (1:1 med SDS' DW_EK_HelbredsForloeb)
   og grupperer på tværs af sygehusforløb. **Forkastes som forløbsnøgle**,
   men gemmes i arc — den er den stabile identitet for helbredsforløbet.
2. **Composite af (CPR, tidspunkt, enhed)**: dubletter allerede i psyk
   (373/228.928). **Forkastes.**
3. **Ind-kontaktens `KONTAKT_VAERDI`** (via `DRGKONTAKT_ID_IND`-join inden for
   leverancen): referencerne har perfekt intra-leverance-integritet og
   nøglen er **unik i samtlige profilerede leverancer** (op til 11.520.047
   forløb i T25-M). Stabilitet målt: re-keying (forløb hvis ind-kontakt
   skifter) er et randfænomen — 649 af 6,3M (0,01 %) mellem to
   somatik-leverancer, 22 af 197K i psyk. Nøglen holder også på tværs af
   takstsystemer (7,7M fælles forløb T25↔T26). **Anbefales — bekræftet.**

Psyk: samme princip via `DW_EK_Kontakt_foerste` → første kontakts UUID
(intra-leverance-join på SDS' DW-nøgle er OK — kun kryds-leverance-joins på
DW-nøgler er forbudt). Målt: unik i begge psyk-leverancer (218.825/218.825
og 353.498/353.498), 0 manglende referencer, re-keying 22 af 274.

Se beslutningsnotatet for konsekvenserne (re-keyede forløb = soft-delete +
ny nøgle; det er korrekt adfærd, da forløbet ER redefineret af SDS).

## 10. Diagnoseidentitet

`DIAGNOSE_VAERDI` er tom i samtlige somatik-rækker og findes ikke i
psyk-skemaet. Kompositten (kontakt, diagnosekode, art, sideangivelse) er
imidlertid **unik** i den målte leverance (8.628.544 af 8.628.544).
Anbefaling (uddybet i beslutningsnotatet): **komposit-merge** på
(takstsystem, kontakt-UUID, diagnose, art, sideangivelse) med dedup-guard —
genopbygning pr. kontakt som fallback, hvis senere leverancer viser
dubletter. Kompositten er målt unik i ALLE profilerede leverancer — fire
somatik (op til 15.615.367 rækker i T25-M) og begge psyk (874.218 og
556.849).

### 10.1 Fladgjorte kolonner vs. børnetabeller (kapacitetstjek)

Review-spørgsmål: kan diagnoser/procedurer hentes fra kontakt-rækkens
fladgjorte kolonner (fordel: UUID'en står på rækken)? Målt på T26-M
(11,9M kontakter):

| Måling | Resultat | Fladgjort kapacitet |
|---|---|---|
| Diagnoser pr. kontakt | max 21; 7 kontakter > 20 | DIA01-20 |
| Procedurer pr. kontakt | **max 5.260; 104.027 kontakter (0,9 %) > 40** | PROC01-40 |
| Diagnosetillæg pr. kontakt | max 18; 6 kontakter > 14 | TDIA01-14 |
| Proceduretillæg pr. kontakt | **max 1.506; 303.791 kontakter > 10** | TPROC01-10 |
| Tillæg pr. enkelt procedure | 14,9M procedurer har tillæg; **5,3M har >1 (max 16)** | koblingen tillæg→procedure findes ikke fladt |

**Konklusion:** de fladgjorte kolonner er et top-N-udsnit og taber data
(0,9 % af kontakterne mister procedurer; tillægs-koblingen kan slet ikke
udtrykkes). Dertil mangler de attributter (diagnose: art/side; procedure:
side/handlingsspec/kontrast/tidspunkter/producerende enhed). Bridges
bygges derfor fra børnetabellerne; UUID-behovet dækkes af PROCEDURE_VAERDI
(på børnerækken) hhv. kontakt-UUID opløst ved extract (0 manglende
referencer målt). De fladgjorte kolonner bruges alene som
konsistens-stikprøve.

## 11. FD-analyse: kandidater til dim.DRG

Formål: `dim.DRG` (grain: takstsystem × DRG-kode) skal være den primære
analysedimension. Attributter, der er (næsten) funktionelt afhængige af
DRG-koden inden for et takstsystem, hører hjemme dér — ikke i den brede
transaktionsdimension. Målt pr. DRG-kode i én leverance (antal DRG-koder hvor
kolonnen har >1 værdi):

Målt på alle fire somatik-leverancer (911-938 DRG-koder i brug) med samme
resultat — her T26 U 20260823:

| Kolonne | DRG-koder med >1 værdi | Egnet til dim.DRG? |
|---|---|---|
| `TRIMPUNKT` | **0 af 911** | **JA** — funktionelt afhængig af (takstsystem, DRG) |
| `PRIS_DRG` | **0 af 911** | **JA** — standardtaksten pr. DRG; bliver også som additivt mål på facten |
| `DRG_GROUPER` | 1 af 911 | (nær-redundant med DRG — medtages ikke; grouper-DRG'en beholdes i egenskabsdimensionen) |
| `MDC` | 350 af 911 | **NEJ** — varierer pr. kontakt inden for samme DRG, og `MDC ≠ DRG[0:2]` på 32 % af rækkerne → bliver i egenskabsdimensionen |
| `LANGLIGGERPRIS` | 551 af 911 | NEJ — beregnet beløb (mål på facten) |

**FD-konklusion**: trimpunkt og standardtakst (PRIS_DRG) er funktionelt
afhængige af (takstsystem, DRG) og kan bæres af enhver dimension, hvis
grain indeholder DRG-koden — uden at koste kardinalitet. MDC kan IKKE
udledes af DRG-koden og er ikke konstant pr. DRG.

### 11.1 Udvidet analysedimension — kardinalitetsmåling (2026-08-26)

Beslutning på featuren (Jacob, 2026-08-26): dimensionen må gerne udvides ud
over de rent FD-afhængige attributter — map-tabellens clean_code udvides
blot, og øget udfald accepteres — så længe resultatet er én dimension, der
dækker 80-90 % af analyserne, så egenskabsdimensionen (1:1 med facten)
sjældent skal i spil. Kardinalitet målt på T26-M (11,9M kontakter, tre
aktivitetsår):

| Variant | Attributter | Distinkte kombinationer |
|---|---|---|
| V0 | DRG, MDC | 3.875 |
| V1 (gruppering) | + DRG_GROUPER, REGEL, RTC, D90UA05 | 5.946 |
| V2 (+ kontaktklassifikation) | + drgkontakttype, kontakttype, prioritet, kontaktårsag, afslutningsmåde, henvisningsmåde, henvisningsårsag | 75.363 |
| V3 (+ afregningsklassifikation) | + betalingsaftale, betaler, specialiseringsniveau, sygehustype | 131.523 |

Psyk-kontakternes tilsvarende klassifikationssæt giver 6-8 kombinationer —
en fælles dimension på tværs af leverancetyper koster intet ekstra.

**Anbefaling: V3.** ~132K rækker pr. takstsystem (~400K i alt over T24-T26)
er samme størrelsesorden som dim.Organisation (165K) og trivielt for en
dimension — mod factens ~40M. Dimensionen bærer desuden beskrivelse,
trimpunkt og standardtakst denormaliseret på DRG-delen (FD — ingen ekstra
kardinalitet). Grain: den distinkte kombination af attributterne; nøglen
mintes i map-laget på clean_code = alle attributværdier.

**BESLUTTET (Jacob, 2026-08-26): fuld V3 med navnet
`dim.DRGKontaktKlassifikation`** (parallelt med `dim.DRGKontaktEgenskaber`,
som beholder lav-frekvens- og fritekst-attributterne). Facten bærer
`dw_ek_DRGKontaktKlassifikation`; nøglen mintes i
`map.DRGKontaktKlassifikation`. Implementeres i [internal ticket].

## 12. Leverancedefekter og -observationer

- SMB-mounten (`Host is down`) var utilgængelig fra miljøet 2026-08-26
  formiddag — ingestion skal kunne skelne "mount nede" fra "ingen nye
  leverancer" (hard error ved manglende mount, jf. haiba.py-mønstret).
- **M-filen 2026-03-17 (T26) indeholder 217,8M rækker (32,2M kontakter) —
  ca. 2,7× det normale M-niveau** og er den eneste af sin slags. Alle tre
  takstsystemer fik en M-fil netop 17/3 (T24's sidste). Årsagen er ukendt
  (mulig dobbelt-/tripel-pakning); den er aldrig "nyeste gældende" længere,
  men rækkeantal-drift-værnet i [internal ticket] skal fange den slags.
- U-filen 2026-08-17 (T26) er ikke en defekt: indholdet er identisk med
  M 2026-08-18 (se §8).
- Identiske gentagelses-leverancer forekommer (samme indhold pakket flere
  gange) — arkivmergens change detection gør dem til no-ops.
- Ingen leverancer med 0-rækkers datasæt fundet i inventaret (113 arkiver).

## 13. Konklusioner

1. **Merge-nøgler**: (takstsystem, KONTAKT_VAERDI) for kontakter,
   (takstsystem, PROCEDURE_VAERDI) for procedurer, ind-/første kontakts UUID
   for forløb, komposit-merge (kontakt-UUID, kode, art, side) for diagnoser
   — fuldt specificeret i [beslutningsnotatet](drg_noegler_beslutningsnotat.md).
2. **Kontroltotaler**: SDP_ExportLog matcher eksakt (verificeret på to
   somatik- og tre psyk-leverancer, i alt ~80M talte rækker) og skal
   håndhæves som hard fail i extract.
2b. **Arkiv-enheden er (takstsystem × aktivitetsår)** — M-leverancer dækker
   tre aktivitetsår, U-leverancer to; delete-detection scopes til de
   årsskiver leverancen indeholder (§8).
3. **Grain-beslutningen bekræftet og kvantificeret**: samme kontakt vurderes
   under op til tre takstsystemer (91 % af fælles kontakter har forskellig
   totalpris; 3,7 % skifter DRG). fct.DRGKontakt bliver ~40M rækker for
   T24-T26 (somatik) + ~0,8M (psyk).
4. **Tungeste ≠ udfaldsgivende**: flagene markerer hver én kontakt pr.
   forløb, men IKKE altid den samme (744.996 rækker afviger). Beslutningen
   om at bære forløbsmålene på TUNGESTE kontakt er entydig at implementere;
   udfaldsgivende-flaget bevares som attribut.
5. **Én udvidet analysedimension anbefales** (V3 i §11.1, ~132K rækker pr.
   takstsystem): gruppering + kontakt- og afregningsklassifikation +
   beskrivelse/trimpunkt/standardtakst denormaliseret på DRG-delen —
   målet er at 80-90 % af analyser klares uden egenskabsdimensionen.
6. **KONTEKST-blok-rettelser**, indarbejdet 2026-08-26:
   (a) tungeste/udfaldsgivende falder IKKE altid sammen (744.996 rækker);
   (b) diagnose-kompositten ER unik i målt leverance → komposit-merge er
   primær strategi; (c) FORLOB_VAERDI er helbredsforløbets UUID (1:1 med
   DW_EK_HelbredsForloeb), ikke et forløbsmærke pr. kontakt.

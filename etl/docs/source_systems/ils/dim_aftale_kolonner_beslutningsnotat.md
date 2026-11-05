<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# dim.Aftale — kolonneudvidelse (beslutningslog)

- **User story:** "dim.Aftale — udvidelse med deflevdage, udbud og øvrige lafth-kolonner"
- **Kilde:** `dbo.lafth` (ILS) — ekstraktes allerede fuldt til `arc.ils_lafth`
- **Grain:** én række pr. (lnr, aftale) — ren berigelse, ingen grain-ændring
- **Besluttet:** 2026-09-30 (kildedata verificeret mod ILS samme dag)

## Resultat — nye kolonner i dim.Aftale

| Kildekolonne (lafth) | Dim-kolonne | Type |
|---|---|---|
| `deflevdage` | `standard_leveringsdage` | INT |
| `sysdato` | `dw_ek_Dato_sysdato` + `dato_tid_sysdato` | INT (FK dim.Dato) + DATETIME2 |
| `bruger` | `bruger` | NVARCHAR |
| `aktiv` | `aktiv` | BIT |
| `fragtbel` | `fragt_beloeb` | DECIMAL(17,2) |
| `sw_fragt` | `sw_fragt` | BIT |
| `gebyrbel` | `gebyr_beloeb` | DECIMAL(17,2) |
| `sw_gebyr` | `sw_gebyr` | BIT |
| `graensebel` | `graense_beloeb` | DECIMAL(17,2) |
| `fragtgraensebel` | `fragt_graense_beloeb` | DECIMAL(17,2) |

`vw_dim.Aftale` er `SELECT *` og følger med automatisk.

## Beslutninger pr. gruppe

### 1. Leveringsdage + ajourføring — medtages

- `deflevdage` → `standard_leveringsdage`: samme navn som dim.Lager bruger for
  `lgr.deflevdage` — maksimal konsistens på tværs af dims. Aftalens leveringsdage
  (ILS-view'ets "Aftale lev. dage"); må ikke forveksles med lagerets kolonne, som
  hører til sin egen story.
- `sysdato` → `dw_ek_Dato_sysdato` + `dato_tid_sysdato`: følger datokonventionen
  (jf. dim.ILSVare's `dw_ek_Dato_oprettet`/`dato_tid_oprettet`) — INT-nøgle til
  dim.Dato plus fuldt tidsstempel. Kildenavnet `sysdato` beholdes bevidst frem for
  en oversættelse til "ajourfoert".
- `bruger` → `bruger`: kildenavnet beholdes bevidst; samme navn findes allerede i
  dim.Vareudlevering.

### 2. Udbudsreference (`udbud`) — fravalgt

Strategisk Indkoeb har nævnt "udbudsaftale" som nødvendig, og `lafth.udbud` (varchar(24)) var
nærmeste kandidat — direkte relevant for "samlede indkøb pr. udbudsaftale"-dashboardet.
Kolonnen er dog **altid tom i kilden** og bærer derfor ingen information; at medtage
den ville kun skabe falsk tryghed om at udbudsbehovet var dækket.

**Åbent punkt (uden for story'en):** behovet for udbudsaftale er dermed IKKE dækket.
Det skal afklares med Strategisk Indkoeb hvor udbudsreferencen reelt findes (andet felt, andet
system).

### 3. Kildens aktiv-flag (`aktiv`) — medtages

Medtages uændret som `aktiv` (BIT) — følger præcedensen fra dim.Lager og dim.ILSVare,
der begge bærer kildens aktiv-flag under samme navn. Flagget kan afvige fra
start/slut-vinduet (fx en aftale deaktiveret før tid) og bærer dermed selvstændig
information. Dim'en har i øvrigt ingen delete-markering — udløbet aftale er blot en
aftale med `hoved_slut` i fortiden.

### 4. Kontakt og eksternt ansvar (`kontakt`, `eksansvar`) — fravalgt

`kontakt` er altid tom i kilden. `eksansvar` er oftest tom; skal man bruge den, slår
man op i ILS i stedet — vurderet uden analytisk værdi. Fravalget undgår samtidig
fritekst-/PII-eksponering i dim'en.

### 5. Bemærkning + alternativ leverandør (`bem`, `altlnr`) — fravalgt

Begge kolonner er altid tomme i kilden. (Alternative leverandører pr. aftale ligger
desuden i den separate tabel `lafthalt`, ikke på headeren.)

### 6. Fragt- og gebyrvilkår — medtages

Beløb og switches er beskrivende attributter på aftale-grain (vilkår, ikke målinger).
Alle seks medtages med beskrivende navne; switches beholder `sw_`-præcedensen fra
dim.Vareudlevering: `fragt_beloeb`, `sw_fragt`, `gebyr_beloeb`, `sw_gebyr`,
`graense_beloeb`, `fragt_graense_beloeb`.

### 7. Rabat- og bonustrapper — fravalgt

Progress-slotkolonnerne `belfra/beltil/rabkr/rabatpct ##1–3` (rabattrappe) og
`bonusgr/bonusbel/bonuspct ##1–3` (bonustrappe) er **0 på alle rækker** i kilden, og
`bonusbem` er tom streng på alle rækker. Trapperne er ikke i brug i ILS, så
spørgsmålet om form (flade kolonner vs. unpivot) bortfalder.

### Tekniske kolonner — medtages ikke

`prrowid`, `seqnr`, `Pro2SrcPDB`, `pro2created`, `pro2modified` er
Progress-/replikeringskolonner. De bæres i `arc.ils_lafth` men hører ikke hjemme i
dimensionen.

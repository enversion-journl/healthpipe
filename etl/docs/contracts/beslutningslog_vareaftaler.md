<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Beslutningslog — Vareaftaler (fct.AftaleVarePris og leverandørvare-kilderne)

Se [hovedloggen](beslutningslog_principper.md) for principper og
læsevejledning. Dækningen her er ERP-sporet ILS → dimensionel model, afgrænset til
leverandøraftalernes varer og priser (`fct.AftaleVarePris`,
`dim.Leverandoervare`, `bridge.LeverandoervareILSVare`). Kildesemantikken er
afklaret med og verificeret af forretningsekspert i Strategisk Indkøb
(oktober 2026).

## Kildernes rollefordeling

**`laftd` er rygraden; `laftdpris` beriger kun.** Hvilke varer der ligger på
en aftale, afgøres af `laftd` (nøgle: lnr + aftale + vnr). Prisrækker i
`laftdpris` uden en tilhørende aftalelinje medtages ikke; aftalelinjer uden
prisrækker medtages som syntetiske dækningsrækker med tom pris
(`har_prisversion = 0`). *Aftager:* en aftalelinje findes i fakten også før
den har fået sin første prisversion — filtrér på `har_prisversion = 1`, hvis
man kun vil se rigtige prisversioner (det giver paritet med ILS'
referencevisning, som ikke kender prisløse linjer).

**`laftd`'s egne pris-, pakstr- og lvnr-kolonner er historiske levn og
ignoreres overalt.** Dataprofilering viste, at `laftd.pris##1` kun matcher den
aktuelle pris i `laftdpris` på 0,3 % af linjerne, og `laftd.lvnr##2–3` kun er
udfyldt på ~50 linjer. Alle priser hentes fra `laftdpris`, alle
leverandørvarenumre fra `laftdstam` — det gælder både fakten og
leverandørvare-dimensionens/bridgens kildebillede, inklusive `laftd.lvnr##1`,
som tidligere var en selvstændig primærkilde i hub, dim og bridge.
*Aftager:* migrationen (`20261006_30055`) tømmer `dim.Leverandoervare` og
`bridge.LeverandoervareILSVare` eksplicit (UNKNOWN-rækken/-parret bevares),
og første ordinære kørsel genopbygger dem fuldt fra det aktuelle
kildebillede — entiteter og par, hvis eneste kilde var `laftd`'s
lvnr-kolonner, bliver dermed ikke restaget og udgår. `laftd.vnr` er derimod
IKKE et leverandørvarenummer og er fortsat autoritativ (faktens backbone og
ILSVare-aksen).

## Slot-semantik

**Slots er positionsvise forpakningsvarianter.** `laftdstam.lvnr##N`,
`laftdpris.pris##N` og `laftdpris.pakstr##N` hører sammen pr. position N:
slot N er én forpakningsvariant med eget leverandørvarenummer, egen pris og
egen pakkestørrelse. Samme lvnr går oftest igen på tværs af en rækkes slots,
men kan også skifte inden for rækken. *Aftager:* slot N's
`dw_ek_Leverandoervare_{N}` hører sammen med `pris{N}` og
`pakkestoerrelse{N}` — bland aldrig kolonner på tværs af slots.

**Slots ligger som kolonner, ikke som rækker.** En unpivot til én række pr.
lvnr blev implementeret og siden rullet tilbage: når lvnr##1 og lvnr##2 er
ens (det almindelige tilfælde), gav fan-out'en nærmest identiske "aktuelle"
rækker for samme vare på samme aftale, og koblingen slot-lvnr ↔ slot-pris gik
tabt, fordi alle fire prisslots lå på hver fan-out-række. *Aftager:* grain er
én række pr. prisversion (`bk_prrowid`); skal slots vendes ned på rækker,
gøres det i et view ovenpå fakten — bevidst udskudt, ikke glemt.

**Fakten bærer ingen rå lvnr-kolonner.** Leverandørvarenummeret nås via
`dw_ek_Leverandoervare_1–4` mod `dim.Leverandoervare`, som ejer nummeret.
Samme begrundelse som da `kreditorvarenummer1–3` udgik ved fan-out'en.
*Aftager:* join til dimensionen pr. slot; -1 betyder tomt slot eller
manglende kreditor-CVR i kilden.

## Faktaens entitet og kolonnevalg

**Entiteten bag `dw_ek_AftaleVarePris` er aftalelinjen.** CleanCode-formatet
er `AFTALE_{lnr}_{aftale}_VARE_{vnr}` uden lvnr-komponent — stabil på tværs
af prisversioner. *Aftager:* nøglen identificerer aftalelinjen, ikke en
bestemt forpakningsvariant.

**`maerke`, `linje_start` og `linje_slut` er udgået af fakten.** `maerke`
siger det samme som aftaletypen på `dim.Aftale`; linjens start/slut hører til
aftalelinjen, ikke prisversionen. Linjens start bruges fortsat internt som
`dw_valid_from` på syntetiske rækker. *Aftager:* aftale-attributter læses på
`dim.Aftale`, ikke på fakten.

**`kilder`-kolonnen (slot-provenans) er udgået med fan-out'en.** Behovet for
at vide, hvilken kildekolonne en række kom fra, fandtes kun så længe slots
blev vendt ned som rækker. *Aftager:* provenansen er nu strukturel —
kolonnenavnet ER slottet.

## Bridge-kilder og genopbygning

**Bridge-kildeprioriteterne er renummereret kontiguert.** Med `laftd` ude som
par-kilde er ordenen i `bridge_kilder`: laftdstam (1), lgrvare (2),
indkfak (3), pmdm (4), laftdstam_alt (5). *Aftager:* historiske
`bridge_kilder`-strenge med `laftd`/`laftd_alt` forsvinder ved genopbygning.

**Grain-skiftet blev deployet som tømning + fuld genopbygning, ikke
reconciliation.** Migrationen (`20261006_30055`) sletter alle rækker i
`fct.AftaleVarePris` og `map.AftaleVarePris`, tømmer `dim.Leverandoervare`
og `bridge.LeverandoervareILSVare` (UNKNOWN-rækken `dw_ek_Leverandoervare = -1`
hhv. UNKNOWN-parret `(-1, -1)` bevares) og nulstiller staging-checkpoints for
`dim.Leverandoervare`, `bridge.LeverandoervareILSVare` og
`fct.AftaleVarePris`, så første ordinære kørsel selv genopbygger
`dim.Leverandoervare`, `bridge.LeverandoervareILSVare` og
`fct.AftaleVarePris` fuldt fra det aktuelle kildebillede — laftd-sourcede
entiteter udgår dermed. `map.Leverandoervare` tømmes bevidst IKKE: dens
IDENTITY `dw_ek`-værdier refereres af andre fakta (`fct.Fakturalinje`), så at
holde den stabil betyder at genopstagede entiteter får deres gamle nøgler
tilbage; forældede map-rækker, der aldrig refereres igen, er harmløse. Valgt
frem for MERGE-reconciliation, fordi grain-skiftet gjorde alle eksisterende
rækker og map-identiteter forældede, og ERP-sporet alligevel stod foran en
samlet nulstilling. *Aftager:* `dw_sk`-/`dw_ek`-værdier fra før oktober 2026
kan ikke sammenlignes med værdier efter genopbygningen.

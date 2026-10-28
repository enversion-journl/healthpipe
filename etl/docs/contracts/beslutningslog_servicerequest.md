<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Beslutningslog, del 6 — Henvisninger og rekvisitioner

Se [hovedloggen](beslutningslog_principper.md) for principper og læsevejledning.

ServiceRequest dannes fra henvisninger, billeddiagnostiske rekvisitioner og
genoptræningsplaner. Denne del fører de fortolkningsvalg, der er truffet for
henvisningssiden.

## Status

**To kildestatusser lades bevidst stå som `unknown`.** Kildens »Afsluttet« og
»Fejlet« oversættes ikke. De FHIR-statusser, der ellers ville passe, betyder
noget ret præcist — `completed` siger, at anmodningen er udført, og `revoked`,
at den er trukket tilbage — og ingen af delene kan udledes af kildens ord alene.
»Afsluttet« kan dække både en gennemført og en administrativt lukket henvisning.
Kun »Annulleret« oversættes, til `revoked`. *Aftager:* `status` står som
`unknown` for hovedparten af bestanden. Brug `status_kode` og
`status_kodetekst`, indtil kildens betydning er afklaret med dataejeren.

## Datoer

**Henvisningsdatoen står i det native `authoredOn`; de tre øvrige datoer gør
ikke.** FHIR definerer `authoredOn` som tidspunktet, hvor anmodningen blev
oprettet, og det er præcis, hvad henvisningsdatoen er. Modtagelse, afsendelse og
administrativ lukning dokumenterer hverken oprettelse eller udførelse, og der
findes ikke et native felt, de kan stå i uden at blive misbrugt; de bevares
derfor som kildefelter under deres egne navne. `occurrencePeriod` sættes ikke
for henvisninger — en henvisning har ingen udførelsesperiode.

**Fallbacket fra henvisningsdato til afsendelsesdato er fjernet.** Feltet bar
tidligere henvisningsdatoen med afsendelsesdatoen som reserve. To forskellige
fakta i ét felt kan ingen aftager skelne, og en reserve, der udløses tavst, er
værre end en tom værdi. *Aftager:* `authored_on` er tom for de henvisninger, der
mangler en henvisningsdato. Afsendelsesdatoen står i `afsendelsesdato`.

## Historik

**Genåbning ophæver den foregående afslutning.** Vælges den seneste
historikpost, tæller en genåbning som en ophævelse af afslutningen før den, så
henvisningen ikke fremstår afsluttet, mens den i virkeligheden er åben igen.
*Aftager:* historikken viser den gældende tilstand, ikke den seneste hændelse.

**Ufuldstændig historik leverer ingen historik.** Kan den gældende tilstand ikke
afgøres af posterne — fx en genåbning uden en forudgående afslutning — udelades
historikken helt for den henvisning frem for at levere en tilstand, vi ikke kan
stå inde for. *Aftager:* `latestHistory` mangler for de henvisninger. Fraværet
betyder »kan ikke afgøres«, ikke »ingen historik findes«.

## Behandlere og institutioner

**Yder-navne og institutionskontakt leveres som kopierede kildefelter — som en
mellemstation.** Princippet i modellen er, at organisationens navn og kontakt
hentes gennem referencen og ikke gentages på faktarækken; det er gennemført for
procedurernes organisationer. For henvisninger kan det ikke lade sig gøre endnu:
ydernummeret er ikke med i organisationsudtrækket, så det kan ikke oversættes
til en organisation. Felterne leveres derfor, som kilden skrev dem, fordi
alternativet er ikke at levere dem. *Aftager:* `*_yder_navn` og
`*_institution_*` er kildefelter, ikke opslagsdata. De forventes afløst af en
organisationsreference, når ydernummeret kan opløses; byg ikke varige rapporter
direkte på dem.

**Behandlende afdeling opfindes ikke som rekvirent.** Står hverken henvist-fra
eller afsender med en organisationskode, sættes rekvirenten ikke — den
behandlende afdeling er ikke den, der har bedt om noget. *Aftager:* en tom
rekvirent betyder, at kilden ikke oplyste den.

## Genoptræningsplanens modtagere

**En modtager kendes på sit ydernummer, sin afdelingskode eller sin SOR-kode.**
Kilden har ingen nøgle på modtagerrækkerne. En modtager står med enten et
ydernummer eller en afdelingskode — aldrig begge — og ellers kun med en
SOR-kode, og de tre koder kommer fra hver sit kodesystem. Modtagernøglen
(`recipient_key`) er den første udfyldte af ydernummer, afdelingskode og
SOR-kode, i den rækkefølge, og den er entydig inden for en genoptræningsplan. Kilden fører
én række pr. modtager pr. plan, så afsendelsestidspunktet indgår ikke i
nøglen. Den tidligere nøgle — ydernummer, afdelingskode og
afsendelsestidspunkt — kunne ikke skelne modtagere, der kun har en SOR-kode, og
smed alle på nær én af dem væk, når de var sendt samtidig. *Aftager:* alle
kildens modtagere er med. `recipient_key` er ydernummeret, afdelingskoden eller
SOR-koden, ikke en sammensat streng; en afdeling med både afdelingskode og
SOR-kode har afdelingskoden som nøgle. En modtager med kun en SOR-kode har
`receiver_type` = `ukendt`; SOR-koden står i `organization_ref`.

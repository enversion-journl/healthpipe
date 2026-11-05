<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Beslutningslog, del 7 — Lokationer

Se [hovedloggen](beslutningslog_principper.md) for principper og læsevejledning.

Location med SOR-kode dannes fra to kilder med samme identifier-system:
SOR-arkivet `arc.bom_ifdb_orgenhed_sor` og IFDB's kontaktlokationer
`arc.ifdb_location`. Hver kilde har sin egen pipeline og finder kun de ændrede
SOR-koder i sit eget arkiv. Begge pipelines skriver gennem samme funktion
(`write_codes` i `DSA/FHIR/Location/location_service.py`). For hver ændret kode
læser den begge arkiver, fletter kildernes bidrag og poster hele ressourcen én
gang. Et bidrag er den del af ressourcen, som én kilde leverer. Ejerskabstabellen
angiver for hvert felt, hvilke kilder der må levere det, og i hvilken
rækkefølge: den første kilde, der har feltet, vinder det. Patient bygges på samme
måde.

## Ejerskab

**SOR står først på alle felter, som begge kilder leverer.** SOR er registeret
for enhederne og har den rigeste beskrivelse af dem, mens IFDB kun kender koden
fra en kontakt. Ejerskabstabellen står i
`DSA/FHIR/Location/location_ownership.py`. IFDB leverer kun SOR-koden,
`managingOrganization` og `status = active`. SOR leverer de samme felter og
derudover `type`, `partOf` og gyldighedsperioden. Har SOR koden, kommer hele
lokationen derfor fra SOR, uanset hvilken pipeline der skriver. En IFDB-ændring
fjerner aldrig SOR's `type`, `partOf` eller gyldighedsperiode og sætter aldrig en
inaktiv SOR-enhed til `active`. *Aftager:* en lokation med `type`, `partOf` eller
gyldighedsperiode kommer fra SOR. En lokation, hvis kode ikke findes i
SOR-arkivet, kommer fra IFDB og har altid `status = active`.

**SOR har koden, når SOR-arkivet har en række for den — også en slettet række.**
SOR-arkivet soft-sletter, så den sidste række for en enhed, som SOR har slettet,
står stadig i arkivet. Har koden både en levende og en slettet række, bruges den
levende. En slettet IFDB-række tæller ikke som et IFDB-bidrag.

## Slettede SOR-enheder

**En slettet SOR-række beholder sin arkiverede AKTIV-værdi.** Sletningen betyder,
at rækken ikke længere kommer med i SOR-udtrækket, ikke at SOR har erklæret
enheden inaktiv; at sætte `inactive` ville være en tolkning, som kilden ikke har
leveret. En sletning i SOR sætter derfor ikke `status = inactive`. *Aftager:*
`status = active` betyder ikke, at enheden findes i SOR i dag. Brug SOR-arkivets
sletteflag, hvis gyldigheden skal afgøres. Opslaget af overordnet enhed tæller også
slettede SOR-rækker, så `partOf` kan pege på en slettet forælder.

**En sletning skriver lokationen om.** Begge pipelines udvælger de ændrede koder
blandt alle rækker i deres arkiv, også de slettede. En soft-sletning i et af
arkiverne bygger derfor koden om ud fra de rækker, der står tilbage.

**Kendte begrænsninger.** Har en kode kun slettede IFDB-rækker og ingen SOR-række,
har den intet bidrag. Pipelinen logger en advarsel og skriver ikke lokationen om,
så den beholder sit IFDB-tag og `status = active`. Skifter et ophold i IFDB fra
kode A til kode B, overskriver arkivet værdien, og A bliver ikke bygget om. Har A
ingen andre ophold, beholder lokationen A sit IFDB-tag.

## Proveniens

**Lokationen bærer sine kilder i `meta`.** `meta.source` er byggerens URI, og der
er ét `meta.tag` pr. kilde, der bidrog (BOM_IFDB, IFDB). `fhir.Location` viser dem i
`meta_source`, `source_bom_ifdb` og `source_ifdb`. *Aftager:* en `source_*`-kolonne er 1,
når kilden bidrog til lokationen, og 0, når den ikke gjorde.

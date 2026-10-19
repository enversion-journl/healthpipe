<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Navngivningsstandarder

> Denne standard gælder repoet `datamarter`. Reglerne for basismodellen står i
> [navngivningsstandard_basismodel.md](navngivningsstandard_basismodel.md).

I det følgende beskrives navngivningsstandarder for alle elementer i datamarter – fra de scripts, der danner datamarten, til datamartens enkelte kolonner.

Navngivningen i datamarter følger i store træk de samme regler, som gælder for basismodellen. Da datamarternes struktur adskiller sig fra basismodellens struktur, vil der dog være enkelte afvigelser.

## Indhold

- [Forklaring af cases](#forklaring-af-cases)
- [Repository](#repository)
- [Struktur i repositoriet](#struktur-i-repositoriet)
- [Navngivning i repositoriet](#navngivning-i-repositoriet)
- [Databaser](#databaser)
- [Struktur i databaser](#struktur-i-databaser)
- [Navngivning i databaser](#navngivning-i-databaser)
- [Ordliste](#ordliste)


## Forklaring af cases

For alle elementer i datamarter gælder regler for, hvordan du skal bruge store og små bogstaver og adskille ord. Altså hvilken case du skal bruge, når du navngiver et element. I tabellen herunder kan du se en forklaring af de forskellige typer af cases.

### Casetyper


| Case | Forklaring | Eksempel |
| :--- | :--- | :--- |
| **lowercase** | - Små bogstaver hele vejen igennem<br>- Ingen mellemrum |`dettestårilowercase`|
| **UPPERCASE** | - Store bogstaver hele vejen igennem<br>- Ingen mellemrum | `DETTESTÅRIUPPERCASE` |
| **snake_case** | - Små bogstaver hele vejen igennem<br>- De enkelte ord adskilt af et `_` (underscore)<br>- Ingen mellemrum |  `dette_står_i_snake_case` |
| **kebab-case** | - Små bogstaver hele vejen igennem<br>- De enkelte ord adskilt af en `-` (bindestreg)<br>- Ingen mellemrum | `dette-står-i-kebab-case` |
| **camelCase** | - Lille begyndelsesbogstav i første ord<br>- Stort begyndelsesbogstav i alle efterfølgende ord<br>- Ingen mellemrum | `detteStårICamelCase` |
| **PascalCase** | - Stort begyndelsesbogstav i alle ord<br>- Ingen mellemrum | `DetteStårIPascalCase` |

Den generelle regel er, at tabelnavne skrives i PascalCase, mens alle øvrige elementer skrives i snake_case.

## Repository

Vores datamarter dannes på baggrund af data i basismodellen. Koden, som danner datamarterne, ligger i et GitLab-repository med navnet `datamarter`.

### Struktur i repositoriet

Repositoriet `datamarter` er opdelt i en række mapper, heriblandt mappen `etl`, som blandt andet indeholder undermappen `domains`.

Undermappen `domains` indeholder én undermappe for hvert sikkerhedsdomæne, fx `patientrettigheder`.

Hver af disse undermapper indeholder igen én undermappe for hver datamart i sikkerhedsdomænet, fx `kraeftpakker`.

Nye mapper i repositoriet må kun oprettes efter aftale med teamet Datamarter og rapporter i Dokumentation og ledelsesinformation.

Strukturen for repositoriet `datamarter` er vist på tegningen herunder.


```
datamarter
├── etl
│   ├── domains
│   │   ├── template        -- skabelonmappe til nyt domæne
│   │   │   ├── datamart
│   │   │   │   ├── sql
│   │   │   │   ├── __init__.py
│   │   │   │   ├── datamart.py
│   │   │   │   ├── datamart.yml
│   │   │   ├── README.md
│   │   ├── domain                 -- fx 'patientrettigheder'
│   │   │   ├── datamart           -- fx 'kraeftpakker'
│   │   │   │   ├── sql            -- mappe til alle SQL-scripts
│   │   │   │   ├── __init__.py
│   │   │   │   ├── datamart.py     -- fx 'kraeftpakker.py'
│   │   │   │   ├── datamart.yml    -- fx 'kraeftpakker.yml'
│   │   │   ├── README.md
│   ├── jobs
│   ├── resources
│   ├── schedules
│   ├── sources
│   ├── utils
│   ├── __init__.py
│   ├── definitions.py
│   ├── namespacing.py
├── docs        -- her ligger vores udviklingsprincipper
├── .gitignore
├── README.md
```

### Navngivning i repositoriet


Tabellen herunder viser, hvordan du skal navngive de enkelte elementer i repositoriet `datamarter`.

| Element | Regler | Eksempel |
| :--- | :--- | :--- |
| **Generelt** | - Ingen specialtegn og ingen Æ-Ø-Å<br>- Brug `ae` for æ, `oe` for ø og `aa` for å<br>- Brug danske ord til forretningsbegreber<br>- Brug engelske ord til tekniske infrastrukturbegreber | |
| **Repository** | - Brug kebab-case<br>- Tilstræb korte navne | `datamarter` |
| **Mapper** | - Brug snake_case<br>- Skriv dog akronymer i UPPERCASE<br>- Tilstræb korte navne og gerne kun ét ord | `domains`<br>`jobs`<br>`DSO`<br>`FHIR` |
| **SQL-scripts** | - Brug snake_case på selve scriptnavnet<br>- Brug dog PascalCase på den del af navnet, der måtte referere til et tabelnavn<br>- Brug `tabeltype_` i snake_case som præfiks: `dim_` for dimensioner, `fct_` for facts og `stg_` for stagingtabeller<br>- Brug et præfiks med 2 cifre til at angive eksekveringsrækkefølgen af scripts (kun hvis relevant), fx `01_`<br>- Giv filen samme navn som den tabel eller det view, som scriptet danner | `dim_Organisation.sql`<br>`fct_Markoerforloeb.sql`<br>`01_stg_Markoerforloeb.sql` |
| **Funktioner** | - Brug snake_case | `asset_factories.py` |
| **README-filer** | - Brug UPPERCASE | `README.md` |
| **yml-filer** | - Brug snake_case<br>- Brug `.yml` som filendelse (ikke .yaml)<br>- Giv filen samme navn som den datamart, som yml-filen tilhører | `patientrettigheder.yml` |
| **Øvrige filer** | - Brug snake_case i filnavnet<br>- Brug dog PascalCase på den del af filnavnet, der måtte referere til et tabelnavn | `01_foer_du_starter.md`<br>`kraeftpakker.py` |


## Databaser

Vores datamarter er grupperet i en række sikkerhedsdomæner, som hver har sin egen database på SQL-serveren, fx `DPA_Aktivitet` og `DPA_Patientrettigheder`.

### Struktur i databaser

Databaserne til de enkelte sikkerhedsdomæner følger en helt fast struktur, der ser ud som på tegningen herunder.

```
DPA_Sikkerhedsdomæne    -- fx 'DPA_Patientrettigheder'
├── Security
├── Query
├── dbo   -- skema til alle views
├── dim   -- skema til dimensionstabeller
├── dm    -- skema til datamart-tabeller (dm-pattern: fakta + Egenskaber-transaktionsdimension)
├── fct   -- skema til facttabeller
├── ref
├── stg   -- skema til alle stagingtabeller
```

Bemærk: `dm`-skemaet bærer dm-pattern (fx HAIBA) — slanke facts parret med
`Egenskaber`-transaktionsdimensioner, der deler faktaets `dw_ek`. `Egenskaber`
er i flertal (som de eksisterende `dim.*Egenskaber`), en vedtaget undtagelse
fra "dimensioner i ental".

Nye databaser må kun oprettes efter aftale med teamet Datamarter og rapporter i Dokumentation og ledelsesinformation.

### Navngivning i databaser

Tabellen herunder viser, hvordan du skal navngive de enkelte elementer i en database.

| Element | Regler | Eksempler |
| :--- | :--- | :--- |
| **Generelt** | - Ingen specialtegn og ingen Æ-Ø-Å<br>- Brug `ae` for æ, `oe` for ø og `aa` for å<br>- Brug danske ord til forretningsbegreber<br>- Brug engelske ord til tekniske infrastrukturbegreber | |
| **Database** | - Brug PascalCase på selve databasenavnet<br>- Brug `DPA_` som præfiks | `DPA_Aktivitet`<br>`DPA_Patientrettigheder` |
| **Skemaer** | - Brug lowercase<br>- Skemanavnet vil typisk være en forkortelse på 3-4 bogstaver | `dbo`<br>`dim` |
| **Tabeller** | - Brug PascalCase på selve tabelnavnet<br>- Skriv dog akronymer i UPPERCASE<br>- Brug `datamartnavn_` i snake_case som præfiks<br>- Navngiv facts i flertal og dimensioner i ental | `kraeftpakker_Organisation`<br>`kraeftpakker_Haendelser` |
| **Views** | - Brug PascalCase på selve viewnavnet<br>- Brug `datamartnavn_tabeltype_` i snake_case som præfiks<br>- Angiv tabeltypen som `dim_` for dimensioner, `fct_` for facts og `stg_` for stagingtabeller | `kraeftpakker_dim_Organisation`<br>`kraeftpakker_fct_Haendelser` |
| **Kolonner** | - Brug snake_case på selve kolonnenavnet<br>- Brug dog PascalCase på den del af et kolonnenavn, som måtte referere til et tabelnavn<br>- Skriv eventuelle forkortelser i lowercase, fx `epj`<br>- Brug kun alment kendte forkortelser og akronymer og undgå forkortelser som `dg` for `dage` eller `kd` for `kode`<br>- Brug `dw_sk_` som præfiks på surrogatnøgler og `dw_ek_` som præfiks på entitetsnøgler | `patient_id`<br>`posteringsbeloeb`<br>`dw_sk_Aftale`<br>`dw_ek_Organisation` |

## Ordliste

Herunder kan du læse forklaringer af nogle af de mest anvendte begreber og forkortelser.


| Begreb | Betydning |
|---|---|
| **Datamart** | En emneafgrænset delmængde af datavarehuset optimeret til rapportering og analyse inden for ét forretningsområde, fx `kraeftpakker` eller `maksimale_ventetider`. En datamart består af en række facts og dimensioner udstillet i views med adganggstyring. Typisk det lag slutbrugere og Power BI tilgår. |
| **Domæne** | Et synonym for `Sikkerhedsdomæne`. |
| **Sikkerhedsdomæne** | Et dataområde bestående af en række datamarter, fx `Patientrettigheder`, og relevante basismodeltabeller. Det er sikkerhedsdomænet, der gives adgang til, og som ud fra brugerens rolle styrer, hvilke data brugeren kan se i domænets tabeller og views. |
| **dim** | Forkortelse for *dimensionstabel* eller blot *dimension* – en tabel, der indeholder beskrivende attributter, fx `Borger` eller `Organisation`. Bruges både som navn på skemaer og som præfiks på scripts og views. |
| **fct** | Forkortelse for *facttabel* eller blot *fact*  – en tabel der indeholder målbare hændelser, fx `Forloeb` eller `Bookinger`. Bruges både som navn på skemaer og som præfiks på scripts og views. |
| **stg** | Forkortelse for *stagingtabel* eller blot *staging* – en tabel, hvor data mellemlander på vejen fra basismodel til datamart. Bruges både som navn på skemaer og som præfiks på scripts og views.  |
| **dw_ek** | Forkortelse for *DataWarehouse Entity Key* eller blot *entitetsnøgle*. En datavarehustildelt heltalsnøgle, der entydigt identificerer en entitet, fx en kreditor eller en kontoplan. Bruges som primærnøgle i facts og dimensioner, hvor entitetsnøgler fra dimensioner bruges som fremmednøgler i facts. |
| **dw_sk** | Forkortelse for *DataWarehouse Surrogate Key* eller blot *surrogatnøgle*. En versionsnøgle, der kun findes i type 2-dimensioner, fx dimensionen `Borger`. Hvor dw_ek peger på entiteten (borgeren), peger dw_sk på en bestemt version af entiteten inden for et gyldighedsinterval. |
| **yml-fil** | Konfigurationsfil i YAML-format (.yml). Bruges til at dokumentere datamartens enkelte dele. |

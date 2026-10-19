# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""CodeSystem: IFDB's egne administrative kodelister.

IFDB kvalificerer selv sine koder. «PASHenvisning_ks.ur.Henvisningsstatus.Afsluttet»
naevner listen foer den sidste prik og selve koden efter. Derfor daekker én tabel
alle listerne: den fulde kvalificerede kode er noeglen, og det er praecis den vaerdi,
faktatabellerne allerede gemmer, saa aftagerfladen kan joine uden at oversaette noget.

Teksten hoerer her og ikke paa hver faktaraekke. Kilden har ti statusvaerdier fordelt
paa 3,4 mio. henvisninger; teksten er den samme for alle raekker med samme kode.

Kontrakten er, at teksten er entydigt bestemt af koden. Byggeren efterproever det og
fejler, hvis en kode optraeder med flere tekster — saa er feltet ikke en kodeliste,
og teksten skal blive paa raekken, hvor den historiske ordlyd er bevaret.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.CodeSystem.utils import merge_codesystem_tables

# Noeglen i CODESYSTEM_PASCAL; bestemmer tabelnavnene codesystem.IFDB*.
CODESYSTEM_NAVN = "ifdb"

# (arkivtabel, kodekolonne, tekstkolonne). Feltet skal vaere maalt entydigt og
# fuldt kvalificeret i kilden, foer det tilfoejes her.
KODELISTE_FELTER: tuple[tuple[str, str, str], ...] = (
    ("arc.ifdb_henvisning", "STATUS_KODE", "STATUS_KODETEKST"),
    ("arc.ifdb_henvisning", "HENVISNINGSMAADE_KODE", "HENVISNINGSMAADE_KODETEKST"),
    ("arc.ifdb_henvisning", "INDKALDELSESPRIORIT_KODE", "INDKALDELSESPRIORIT_KODETEKST"),
    ("arc.ifdb_henvisning", "HENVIST_TIL_MUNI_KODE", "HENVIST_TIL_MUNI_KODETEKST"),
    (
        "arc.ifdb_administrative_procedure",
        "INDICATION_OF_SIDE_CODE",
        "INDICATION_OF_SIDE_TEXT",
    ),
    ("arc.ifdb_administrative_procedure", "STAFF_CATEGORY_CODE", "STAFF_CATEGORY_TEXT"),
)


def _kodeliste_query(tabel: str, kode: str, tekst: str) -> str:
    """Distinkte koder med tekst og antal forskellige tekster pr. kode."""
    return f"""
        SELECT
            LTRIM(RTRIM(CONVERT(NVARCHAR(450), [{kode}])))            AS code,
            MAX(LTRIM(RTRIM(CONVERT(NVARCHAR(500), [{tekst}]))))      AS display,
            COUNT(DISTINCT LTRIM(RTRIM(CONVERT(NVARCHAR(500), [{tekst}])))) AS tekstvarianter
        FROM {tabel}
        WHERE NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(450), [{kode}]))), N'') IS NOT NULL
        GROUP BY LTRIM(RTRIM(CONVERT(NVARCHAR(450), [{kode}])))
    """


def _kodeliste_navn(code: str) -> str:
    """«PASHenvisning_ks.ur.Henvisningsstatus.Afsluttet» -> listedelen foer sidste prik."""
    return code.rsplit(".", 1)[0] if "." in code else ""


def byg_ifdb_kodelister(
    rows_pr_felt: dict[str, list[tuple]],
) -> tuple[list[dict], list[dict], list[str]]:
    """Byg concepts og properties; returnér ogsaa de koder, der brød entydigheden."""
    concepts: dict[str, str | None] = {}
    kodelister: dict[str, str] = {}
    flertydige: list[str] = []
    for felt, rows in rows_pr_felt.items():
        for code, display, tekstvarianter in rows:
            if tekstvarianter and int(tekstvarianter) > 1:
                flertydige.append(f"{felt}")
                continue
            # Koderne er fuldt kvalificerede og deles derfor normalt ikke paa
            # tvaers af felter. Sker det alligevel med hver sin tekst, ville den
            # sidst indlaeste stille og roligt overskrive den foerste, og
            # opslaget i viewet ville give et andet svar end kilderaekken. Det
            # skal stoppe indlaesningen, ikke passere.
            if code in concepts and concepts[code] != display:
                flertydige.append(f"{felt} (koden findes med anden tekst i et andet felt)")
                continue
            concepts[code] = display
            liste = _kodeliste_navn(code)
            if liste:
                kodelister[code] = liste
    concept_rows = [{"CODE": c, "DISPLAY": d} for c, d in sorted(concepts.items())]
    property_rows = [
        {"CODE": c, "KEY": "kodeliste", "VALUE": v, "TYPE": "string"}
        for c, v in sorted(kodelister.items())
    ]
    return concept_rows, property_rows, sorted(set(flertydige))


@dg.asset(
    name="cs_ifdb_kodelister",
    group_name="codesystem",
    compute_kind="mssql",
    deps=[
        "staging_infra_codesystem",
        "arc_henvisning_ifdb",
        # To af de seks felter laeses fra administrative procedurer, ikke henvisninger.
        "arc_administrative_procedure_ifdb",
    ],
    tags={"domain": "codesystem", "layer": "codesystem", "module": "ifdb"},
    description=(
        "Byg codesystem.IFDBConcept/_Properties af IFDB's egne kvalificerede "
        "kodelister, saa teksten staar ét sted i stedet for paa hver faktaraekke."
    ),
)
def cs_ifdb_kodelister(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    rows_pr_felt: dict[str, list[tuple]] = {}
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for tabel, kode, tekst in KODELISTE_FELTER:
                cursor.execute(_kodeliste_query(tabel, kode, tekst))
                rows_pr_felt[f"{tabel}.{kode}"] = [tuple(r) for r in cursor.fetchall()]
            concepts, properties, flertydige = byg_ifdb_kodelister(rows_pr_felt)
            if flertydige:
                raise dg.Failure(
                    "Teksten er ikke entydigt bestemt af koden for: "
                    f"{', '.join(flertydige)}. Feltet er ikke en kodeliste; "
                    "teksten skal blive paa raekken."
                )
            resultat = merge_codesystem_tables(
                cursor,
                CODESYSTEM_NAVN,
                concepts,
                properties,
                [],
                code_max_length=450,
                delete_stale=True,
            )
            conn.commit()
        finally:
            cursor.close()
    return dg.MaterializeResult(
        metadata={
            "koder": dg.MetadataValue.int(len(concepts)),
            "kodelister": dg.MetadataValue.int(len({p["VALUE"] for p in properties})),
            "felter": dg.MetadataValue.int(len(KODELISTE_FELTER)),
            "koder_pr_felt": dg.MetadataValue.json(
                {felt: len(rows) for felt, rows in rows_pr_felt.items()}
            ),
            "merge": dg.MetadataValue.json(resultat),
        }
    )

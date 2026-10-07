# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""MIDLERTIDIGT verifikationsasset for genkørslens indlæste data.

Efterprøver grenens nye kontrakter direkte mod de indlæste rækker i staging
og DSO: kildenøgle-parret, knivtid, delop/partOf, LPR-flaget, de fem
diagnoseveje, XML-barne-formater, referentiel integritet på tværs af
familierne og afstemningen staging↔DSO inkl. SFI-lageret.

To arter af tjek:
  - KONTRAKT   brud SKAL være 0 — asset'et fejler til sidst, hvis ikke.
  - MÅLING     rapporteres med tal; grundlag for beslutninger og kalibrering
               (fx RIS-primærkoder med ciffer/plus til det åbne filterspørgsmål).

Alt rapporteres som aggregater — ingen rækkenøgler eller feltindhold når
loggen. Read-only: asset'et ændrer intet og kræver derfor ingen bekræftelse.

Fjernes igen, når genkørselssporet er lukket — deraf «midlertidigt».
"""

from dataclasses import dataclass

import dagster as dg
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_ADMINISTRATIVE_PROCEDURE,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION,
    SYSTEM_IFDB_DIAGNOSE,
    SYSTEM_IFDB_PAS_DIAGNOSE,
    SYSTEM_IFDB_RIS_YDELSE,
    SYSTEM_IFDB_RIS_YDELSE_DIAGNOSE,
)

_PROC_KILDER = (
    SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
    SYSTEM_IFDB_RIS_YDELSE,
    SYSTEM_IFDB_ADMINISTRATIVE_PROCEDURE,
)
_COND_VEJE = (
    SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION,
    SYSTEM_IFDB_DIAGNOSE,
    SYSTEM_IFDB_PAS_DIAGNOSE,
    SYSTEM_IFDB_RIS_YDELSE_DIAGNOSE,
)


def _in_liste(vaerdier):
    return ", ".join(f"N'{v}'" for v in vaerdier)


@dataclass(frozen=True)
class Tjek:
    navn: str
    art: str  # "kontrakt" | "maaling"
    sql: str  # returnerer én række: (antal, grundlag)
    hvorfor: str


# Alle tjek kører mod staging. Grundlaget (2. kolonne) viser, hvor meget der
# reelt blev efterprøvet — et PASS på 0 rækker er ikke et bevis.
TJEK: tuple[Tjek, ...] = (
    # ---- Procedure: grenens nye kolonner og kontrakter ----------------------
    Tjek(
        "proc_lpr3_aldrig_null",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure]) FROM [fhir].[Procedure] "
        "WHERE [lpr3_ignored] IS NULL",
        "Nyindlæste rækker skal have LPR-flaget 0/1 — det er backfill-beviset.",
    ),
    Tjek(
        "proc_kildepar_komplet",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure]) FROM [fhir].[Procedure] "
        "WHERE [source_system] IS NULL OR [source_identifier_value] IS NULL",
        "DQA joiner på (source_system, source_identifier_value) — parret må aldrig være halvt.",
    ),
    Tjek(
        "proc_kildesystem_kendt",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure]) FROM [fhir].[Procedure] "
        f"WHERE [source_system] NOT IN ({_in_liste(_PROC_KILDER)})",
        "Kun de tre procedurekilder må optræde.",
    ),
    Tjek(
        "proc_delop_partof_kobling",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure]) FROM [fhir].[Procedure] "
        "WHERE (CASE WHEN [ifdb_aktivitet_id] LIKE '%-delop-%' THEN 1 ELSE 0 END) "
        "<> (CASE WHEN [part_of_id] IS NOT NULL THEN 1 ELSE 0 END)",
        "Delop-identifier og partOf skal følges ad — begge eller ingen.",
    ),
    Tjek(
        "proc_delop_partof_ri",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure] WHERE [part_of_id] IS NOT NULL) "
        "FROM [fhir].[Procedure] d WHERE d.[part_of_id] IS NOT NULL AND NOT EXISTS "
        "(SELECT 1 FROM [fhir].[Procedure] h WHERE h.[procedure_id] = d.[part_of_id])",
        "Hver deloperation skal pege på en eksisterende hovedprocedure.",
    ),
    Tjek(
        "proc_delop_uden_knivtid",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure] WHERE [part_of_id] IS NOT NULL) "
        "FROM [fhir].[Procedure] WHERE [part_of_id] IS NOT NULL AND ([knivtid_start] IS NOT NULL "
        "OR [knivtid_slut] IS NOT NULL OR [knivtid_minutter] IS NOT NULL)",
        "Knivtid hører KUN til hovedproceduren — delops må ikke arve den.",
    ),
    Tjek(
        "proc_knivtid_varighed",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure] WHERE [knivtid_start] IS NOT NULL "
        "AND [knivtid_slut] IS NOT NULL) FROM [fhir].[Procedure] "
        "WHERE [knivtid_start] IS NOT NULL AND [knivtid_slut] IS NOT NULL "
        "AND [knivtid_minutter] <> DATEDIFF(MINUTE, [knivtid_start], [knivtid_slut])",
        "Varigheden beregnes i byggelaget — den skal stemme med tidspunkterne.",
    ),
    Tjek(
        "proc_knivtid_negativ_uden_varighed",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure] WHERE [knivtid_start] IS NOT NULL "
        "AND [knivtid_slut] IS NOT NULL AND [knivtid_slut] < [knivtid_start]) "
        "FROM [fhir].[Procedure] "
        "WHERE [knivtid_start] IS NOT NULL AND [knivtid_slut] IS NOT NULL "
        "AND [knivtid_slut] < [knivtid_start] AND [knivtid_minutter] IS NOT NULL",
        "Baglæns knivtid-par er kildedata (lossless) — men byggelaget må aldrig "
        "beregne en varighed af dem.",
    ),
    Tjek(
        "proc_knivtid_baglaens_par",
        "maaling",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure] WHERE [knivtid_start] IS NOT NULL "
        "AND [knivtid_slut] IS NOT NULL) FROM [fhir].[Procedure] "
        "WHERE [knivtid_start] IS NOT NULL AND [knivtid_slut] IS NOT NULL "
        "AND [knivtid_slut] < [knivtid_start]",
        "Kendt kildedata-kanttilfælde (slut tastet før start) — måles til "
        "WARN-til-ERROR-stramningen efter fuld genkørsel.",
    ),
    Tjek(
        "proc_hoved_har_kode",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure] WHERE [part_of_id] IS NULL) "
        "FROM [fhir].[Procedure] WHERE [part_of_id] IS NULL AND [code] IS NULL",
        "En hovedprocedure uden kode kan ikke bruges af aftageren.",
    ),
    Tjek(
        "proc_tillaeg_ri",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure_tillaegskode]) "
        "FROM [fhir].[Procedure_tillaegskode] t WHERE NOT EXISTS "
        "(SELECT 1 FROM [fhir].[Procedure] p WHERE p.[procedure_id] = t.[procedure_id])",
        "Tillægskoder uden forælder er forældreløse.",
    ),
    Tjek(
        "proc_encounter_ri",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure] WHERE [encounter_id] IS NOT NULL) "
        "FROM [fhir].[Procedure] pr WHERE pr.[encounter_id] IS NOT NULL AND NOT EXISTS "
        "(SELECT 1 FROM [fhir].[Encounter] e WHERE e.[encounter_id] = pr.[encounter_id])",
        "Kontakterne blev ikke tømt — alle opløste encounter-referencer skal ramme.",
    ),
    Tjek(
        "proc_patient_ri",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure] WHERE [patient_id] IS NOT NULL) "
        "FROM [fhir].[Procedure] pr WHERE pr.[patient_id] IS NOT NULL AND NOT EXISTS "
        "(SELECT 1 FROM [fhir].[Patient] p WHERE p.[patient_id] = pr.[patient_id])",
        "Patienterne blev ikke tømt — alle opløste patient-referencer skal ramme.",
    ),
    # ---- Condition: de fem veje og aftagerfladen ---------------------------
    Tjek(
        "cond_lpr3_aldrig_null",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Condition]) FROM [fhir].[Condition] "
        "WHERE [lpr3_ignored] IS NULL",
        "Samme backfill-bevis som på Procedure.",
    ),
    Tjek(
        "cond_kildepar_komplet",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Condition]) FROM [fhir].[Condition] "
        "WHERE [source_system] IS NULL OR [source_identifier_value] IS NULL",
        "Vejene skilles på identifier-system — parret må aldrig være halvt.",
    ),
    Tjek(
        "cond_veje_kendte",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Condition]) FROM [fhir].[Condition] "
        f"WHERE [source_system] NOT IN ({_in_liste(_COND_VEJE)})",
        "Kun de fem diagnoseveje må optræde.",
    ),
    Tjek(
        "cond_xml_identifier_format",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Condition] "
        f"WHERE [source_system] = N'{SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION}') "
        "FROM [fhir].[Condition] "
        f"WHERE [source_system] = N'{SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION}' "
        "AND [source_identifier_value] NOT LIKE '%/%'",
        "XML-diagnosens barne-id er {forælder}/{elementnr} — det nye formats kontrakt.",
    ),
    Tjek(
        "cond_tillaeg_ri",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Condition_tillaegskode]) "
        "FROM [fhir].[Condition_tillaegskode] t WHERE NOT EXISTS "
        "(SELECT 1 FROM [fhir].[Condition] c WHERE c.[condition_id] = t.[condition_id])",
        "Tillægskoder uden forælder er forældreløse.",
    ),
    # ---- QuestionnaireResponse ---------------------------------------------
    Tjek(
        "qr_dwisdel_domaene",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[QuestionnaireResponse]) "
        "FROM [fhir].[QuestionnaireResponse] "
        "WHERE [dw_is_deleted] IS NULL OR [dw_is_deleted] NOT IN (0, 1)",
        "Den flade slettemarkering er grenens QR-kontrakt: altid 0 eller 1.",
    ),
    Tjek(
        "qr_item_ri",
        "kontrakt",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[QuestionnaireResponse_item]) "
        "FROM [fhir].[QuestionnaireResponse_item] i WHERE NOT EXISTS "
        "(SELECT 1 FROM [fhir].[QuestionnaireResponse] q WHERE q.[qr_id] = i.[qr_id])",
        "Besvarelses-items uden besvarelse er forældreløse.",
    ),
    Tjek(
        "obs_uden_element_kode",
        "maaling",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Observation]) FROM [fhir].[Observation] "
        "WHERE [element_code] IS NULL",
        "Observation-fladen bærer SFI-elementkoden — dækningen skal kendes.",
    ),
    # ---- Målinger ----------------------------------------------------------
    Tjek(
        "ris_primaerkode_ciffer_eller_plus",
        "maaling",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure] "
        f"WHERE [source_system] = N'{SYSTEM_IFDB_RIS_YDELSE}') FROM [fhir].[Procedure] "
        f"WHERE [source_system] = N'{SYSTEM_IFDB_RIS_YDELSE}' "
        "AND ([code] LIKE '[0-9]%' OR [code] LIKE '%+%')",
        "Tallet bag det åbne RIS-filterspørgsmål — måles, før der eventuelt bygges.",
    ),
    Tjek(
        "proc_knivtid_start_uden_slut",
        "maaling",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure] WHERE [knivtid_start] IS NOT NULL) "
        "FROM [fhir].[Procedure] WHERE [knivtid_start] IS NOT NULL AND [knivtid_slut] IS NULL",
        "Kendt kildedata-kanttilfælde — tolereres indtil stramningen efter fuld genkørsel.",
    ),
    Tjek(
        "proc_delop_uden_kode",
        "maaling",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure] WHERE [part_of_id] IS NOT NULL) "
        "FROM [fhir].[Procedure] WHERE [part_of_id] IS NOT NULL AND [code] IS NULL",
        "Delops kan lovligt mangle kode (tom klynge) — omfanget skal bare kendes.",
    ),
    Tjek(
        "proc_periode_kronologi_brud",
        "maaling",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure] "
        "WHERE [performed_period_start] IS NOT NULL AND [performed_period_end] IS NOT NULL) "
        "FROM [fhir].[Procedure] WHERE [performed_period_start] IS NOT NULL "
        "AND [performed_period_end] IS NOT NULL AND [performed_period_end] < [performed_period_start]",
        "Kildens administrative perioder er lossless — baglæns perioder måles, ikke censureres.",
    ),
    Tjek(
        "proc_knivtid_over_et_doegn",
        "maaling",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Procedure] WHERE [knivtid_minutter] IS NOT NULL) "
        "FROM [fhir].[Procedure] WHERE [knivtid_minutter] > 1440",
        "Stikprøve-plausibilitet: knivtid over et døgn er påfaldende og skal kunne ses.",
    ),
    Tjek(
        "cond_eoc_daekning_pas",
        "maaling",
        "SELECT COUNT(*), (SELECT COUNT(*) FROM [fhir].[Condition] "
        f"WHERE [source_system] = N'{SYSTEM_IFDB_PAS_DIAGNOSE}') FROM [fhir].[Condition] "
        f"WHERE [source_system] = N'{SYSTEM_IFDB_PAS_DIAGNOSE}' AND [episode_of_care_id] IS NOT NULL",
        "Forløbsreferencens dækning på PAS-vejen — aftagerfladens nye kolonne i brug.",
    ),
)

# Fordelinger pr. kilde/vej — logges som målinger med ét tal pr. system.
FORDELINGER = (
    ("proc_fordeling", "[fhir].[Procedure]", _PROC_KILDER),
    ("cond_fordeling", "[fhir].[Condition]", _COND_VEJE),
)

# Afstemning staging↔DSO: rækketal pr. familietabel skal være ens lige efter
# kædens sync. (Kolonnen qr_id-nøglen m.v. er ligegyldig her — kun COUNT.)
AFSTEMNINGSTABELLER = (
    "Condition",
    "Condition_tillaegskode",
    "Observation",
    "Procedure",
    "Procedure_tillaegskode",
    "Questionnaire",
    "Questionnaire_item",
    "QuestionnaireResponse",
    "QuestionnaireResponse_item",
)


@dg.asset(
    name="sfi_genkoersel_verifikation",
    group_name="fhir_operations",
    compute_kind="mssql",
    required_resource_keys={"mssql_target_staging", "mssql_target_dso"},
    description=(
        "MIDLERTIDIG: efterprøv genkørslens indlæste data mod grenens kontrakter "
        "(kildepar, knivtid, delop/partOf, LPR, fem diagnoseveje, RI, staging↔DSO). "
        "Read-only; fejler ved kontraktbrud."
    ),
)
def sfi_genkoersel_verifikation(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    res = context.resources
    rapport: dict = {"kontrakter": {}, "maalinger": {}, "fordelinger": {}, "afstemning": {}}
    kontraktbrud: list[str] = []

    with res.mssql_target_staging.get_connection() as conn:
        cur = conn.cursor()
        for tjek in TJEK:
            cur.execute(tjek.sql)
            antal, grundlag = (int(v or 0) for v in cur.fetchone())
            if tjek.art == "kontrakt":
                status = "PASS" if antal == 0 else "FAIL"
                context.log.info(
                    f"KONTRAKT {status} {tjek.navn}: {antal} brud af {grundlag} — {tjek.hvorfor}"
                )
                rapport["kontrakter"][tjek.navn] = {"brud": antal, "grundlag": grundlag}
                if antal:
                    kontraktbrud.append(f"{tjek.navn} ({antal} brud af {grundlag})")
            else:
                context.log.info(f"MÅLING {tjek.navn}: {antal} af {grundlag} — {tjek.hvorfor}")
                rapport["maalinger"][tjek.navn] = {"antal": antal, "grundlag": grundlag}

        for navn, tabel, systemer in FORDELINGER:
            fordeling = {}
            for system in systemer:
                cur.execute(f"SELECT COUNT(*) FROM {tabel} WHERE [source_system] = N'{system}'")
                fordeling[system] = int(cur.fetchone()[0] or 0)
                context.log.info(f"MÅLING {navn}: {fordeling[system]} rækker med {system}")
            rapport["fordelinger"][navn] = fordeling

        staging_tal = {}
        for tabel in AFSTEMNINGSTABELLER:
            cur.execute(f"SELECT COUNT_BIG(*) FROM [fhir].[{tabel}]")
            staging_tal[tabel] = int(cur.fetchone()[0] or 0)

    with res.mssql_target_dso.get_connection() as conn:
        cur = conn.cursor()
        for tabel in AFSTEMNINGSTABELLER:
            cur.execute(f"SELECT COUNT_BIG(*) FROM [fhir].[{tabel}]")
            dso = int(cur.fetchone()[0] or 0)
            diff = dso - staging_tal[tabel]
            status = "PASS" if diff == 0 else "FAIL"
            context.log.info(
                f"KONTRAKT {status} afstemning fhir.{tabel}: staging {staging_tal[tabel]} vs DSO {dso}"
            )
            rapport["afstemning"][tabel] = {"staging": staging_tal[tabel], "dso": dso}
            if diff != 0:
                kontraktbrud.append(f"afstemning_{tabel} (diff {diff})")

        # SFI-lagerets korn: ét datapunkt pr. observation + ét pr. QR-item.
        cur.execute(
            "SELECT (SELECT COUNT_BIG(*) FROM [fhir].[SFI_datapoint_store]) - "
            "((SELECT COUNT_BIG(*) FROM [fhir].[Observation]) + "
            "(SELECT COUNT_BIG(*) FROM [fhir].[QuestionnaireResponse_item]))"
        )
        store_diff = int(cur.fetchone()[0] or 0)
        status = "PASS" if store_diff == 0 else "FAIL"
        context.log.info(f"KONTRAKT {status} sfi_store_korn: diff {store_diff} mod obs + QR-items")
        rapport["afstemning"]["SFI_datapoint_store_diff"] = store_diff
        if store_diff != 0:
            kontraktbrud.append(f"sfi_store_korn (diff {store_diff})")

    if kontraktbrud:
        raise dg.Failure(
            description="Verifikationen fandt kontraktbrud (alle tal står i loggen): "
            + "; ".join(kontraktbrud)
        )
    return dg.MaterializeResult(
        metadata={
            "status": dg.MetadataValue.text("alle kontrakter PASS"),
            "rapport": dg.MetadataValue.json(rapport),
        }
    )


sfi_genkoersel_verifikation_job = dg.define_asset_job(
    name="sfi_genkoersel_verifikation_job",
    selection=dg.AssetSelection.assets("sfi_genkoersel_verifikation"),
    description="MIDLERTIDIG: verificér genkørslens indlæste data mod grenens kontrakter.",
    tags={"domain": "fhir", "kind": "operations"},
)

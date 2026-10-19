# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the DRG staging SQL builders.

The builders are pure functions, so the SQL's structure — clean_code rules,
GDPR guarantees, delta propagation, sentinel handling, carrying-contact
logic — is asserted without a database.
"""

import pytest
from DSA.SQL.staging.drg_kontakt_kolonner import (
    PRE_COLUMN_NAMES,
    PRE_COLUMNS,
    SOR_ROLES,
    StgColumn,
    clean_code_sql,
)
from DSA.SQL.staging.drg_staging_sql import (
    UOPLOEST_LOOKBACK_DAGE,
    build_delta_scope_sql,
    build_forloeb_kontroltotal_sql,
    build_pre_stg_sql,
    build_sor_coverage_sql,
    build_stg_dim_sql,
    build_stg_fct_sql,
    build_unresolved_borger_scope_sql,
    create_borger_cpr_lookup_tables_sql,
    create_organisation_sor_lookup_table_sql,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# clean_code-regler
# ---------------------------------------------------------------------------


def test_clean_code_bygges_af_leverancetype_takstsystem_og_uuid_aldrig_cpr():
    for lt in ("DRG", "DRGPSYK"):
        code = clean_code_sql(lt)
        assert f"'DRGKONTAKT_{lt}_'" in code
        assert "takstsystem" in code
        assert "KONTAKT_VAERDI" in code
        assert "CPR" not in code.upper().replace("KONTAKT_VAERDI", "")


def test_clean_code_skelner_leverancetyper():
    """De to leverancetyper traekker KONTAKT_VAERDI fra samme nationale
    noeglerum, og disjunkthed paa tvaers er hverken maalt eller haandhaevet —
    uden leverancetype i koden ville et delt uuid dele dedup-partition, og
    den ene kontakt ville blive kasseret lydloest."""
    assert clean_code_sql("DRG") != clean_code_sql("DRGPSYK")
    sql = build_pre_stg_sql(None)
    assert "'DRGKONTAKT_DRG_'" in sql
    assert "'DRGKONTAKT_DRGPSYK_'" in sql


def test_clean_code_normaliserer_uuid():
    code = clean_code_sql("DRG")
    assert "UPPER(" in code and "LTRIM(RTRIM(" in code


# ---------------------------------------------------------------------------
# GDPR: ingen raa CPR i nogen stg-tabel
# ---------------------------------------------------------------------------


def test_ingen_cpr_kolonne_i_pre_kolonnesaettet():
    assert not [n for n in PRE_COLUMN_NAMES if "cpr" in n.lower()]


def test_pre_sql_resolver_borger_men_stager_aldrig_cpr():
    sql = build_pre_stg_sql(None)
    # CPR bruges KUN i join-/CASE-udtryk — aldrig som selvstaendig staged kolonne
    assert "AS [dw_ek_Borger]" in sql
    assert "AS [borger_opslag]" in sql
    assert "AS [CPR]" not in sql and "AS [cpr]" not in sql
    assert "AS [CPRnummer]" not in sql


def test_dim_sql_har_hverken_cpr_eller_borgernoegle():
    sql = build_stg_dim_sql()
    assert "cpr" not in sql.lower()
    assert "dw_ek_Borger" not in sql  # borgernoeglen hoerer til fct-siden


# ---------------------------------------------------------------------------
# Borger-opslag
# ---------------------------------------------------------------------------


def test_borger_opslag_bruger_tidligere_cpr_som_fallback():
    lookup = create_borger_cpr_lookup_tables_sql()
    assert "[cpr_nummer]" in lookup and "[tidligere_cpr]" in lookup
    assert "MIN([dw_ek_Borger])" in lookup  # deterministisk kollaps over T2-raekker
    sql = build_pre_stg_sql(None)
    # fallback-raekkefoelgen: primaer foer tidligere_cpr foer -1
    assert "COALESCE(b.[dw_ek_Borger], bt.[dw_ek_Borger], -1)" in sql


def test_borger_sentinel_tom_cpr_er_minus_2():
    sql = build_pre_stg_sql(None)
    assert "THEN -2" in sql  # N/A ved tom/NULL CPR
    assert "'tom'" in sql and "'ukendt'" in sql and "'tidligere_cpr'" in sql


# ---------------------------------------------------------------------------
# Delta-propagation paa tvaers af tabeller
# ---------------------------------------------------------------------------


def test_delta_scope_daekker_alle_boernetabeller_og_forloeb():
    sql = build_delta_scope_sql("2026-08-27T00:00:00")
    for table in (
        "drg_diagnose",
        "drg_diagnose_tillaeg",
        "drg_procedure",
        "drg_procedure_tillaeg",
        "drg_interne_koder",
        "drgpsyk_diagnose",
        "drgpsyk_diagnose_tillaeg",
        "drgpsyk_procedure",
        "drgpsyk_procedure_tillaeg",
        "drgpsyk_betalingsoplysninger",
        "drg_sghforlob",
        "drgpsyk_forloeb",
    ):
        assert f"[{table}]" in sql, f"{table} mangler i delta-scopet"


def test_delta_filter_rammer_kontakt_boern_og_forloeb():
    sql = build_pre_stg_sql("2026-08-27T00:00:00")
    assert "k.[dw_timestamp_update] > '2026-08-27T00:00:00'" in sql
    assert "#delta_kontakt" in sql and "#delta_forloeb" in sql
    # forloebs-delta joiner paa den rigtige noegle pr. leverancetype
    assert "df.[forloeb_noegle] = k.[sygehusforloeb_noegle]" in sql
    assert "df.[forloeb_noegle] = k.[forloeb_noegle]" in sql


def test_fuld_load_har_intet_delta_filter():
    assert build_delta_scope_sql(None) == ""
    sql = build_pre_stg_sql(None)
    assert "#delta_kontakt" not in sql
    # Fuld load laeser hele arc alligevel, saa -1-scopet er overfloedigt der.
    assert "#uoploest_borger" not in sql


# ---------------------------------------------------------------------------
# Gen-oploesning af dw_ek_Borger = -1
# ---------------------------------------------------------------------------


def test_uoploeste_borgere_er_deres_eget_delta_scope():
    """En stub-Patient mintes i Aidbox, ikke i arc, saa kontaktens
    dw_timestamp_update flytter sig ALDRIG (update_only_on_change=True).
    Uden dette scope ville -1 vaere permanent indtil en manuel full_load."""
    sql = build_unresolved_borger_scope_sql()

    assert "#uoploest_borger" in sql
    assert "[fct].[DRGKontakt]" in sql
    assert "[dw_ek_Borger] = -1" in sql
    # -2 (tom CPR) er uoploeselig pr. konstruktion og maa ikke gen-staged.
    assert "[dw_ek_Borger] = -2" not in sql
    assert "[dw_is_deleted] = 0" in sql


def test_uoploest_scope_har_en_nedre_graense_paa_to_uger():
    """Graensen maales paa FACTENS dw_timestamp_update: fct-MERGE'en skriver
    kun ved reel aendring, saa en uoploeselig kontakts timestamp staar stille
    og raekken aeldes selv ud af scopet."""
    sql = build_unresolved_borger_scope_sql()

    assert UOPLOEST_LOOKBACK_DAGE == 14
    assert f"DAY, -{UOPLOEST_LOOKBACK_DAGE}," in sql
    assert "[dw_timestamp_update] >= DATEADD(" in sql
    # Uret er factens skrivetidspunkt, ikke arkivets eller checkpointets.
    assert "SYSDATETIME()" in sql


def test_uoploest_scope_er_sin_egen_batch():
    """mssql-python taber en fejl efter en saetning med row count, og
    delta-scopet aabner med to SELECT ... INTO — derfor egen batch."""
    delta = build_delta_scope_sql("2026-08-27T00:00:00")

    assert "#uoploest_borger" not in delta
    assert "INSERT INTO #uoploest_borger" in build_unresolved_borger_scope_sql()


def test_uoploest_scope_taaler_at_facten_ikke_findes_endnu():
    """Paa en frisk database kan pre_stg koere foer fct.DRGKontakt er oprettet.
    Temp-tabellen oprettes derfor ubetinget, og INSERT'en gaar gennem EXEC —
    et IF forhindrer ikke T-SQL i at compile-validere grenen."""
    sql = build_unresolved_borger_scope_sql()

    assert "CREATE TABLE #uoploest_borger" in sql
    assert "IF OBJECT_ID(N'[fct].[DRGKontakt]', N'U') IS NOT NULL" in sql
    assert "EXEC(N'" in sql
    # Praedikatet skal kunne compile uanset om INSERT'en koerte.
    create_pos = sql.index("CREATE TABLE #uoploest_borger")
    insert_pos = sql.index("INSERT INTO #uoploest_borger")
    assert create_pos < insert_pos


def test_delta_filter_gen_stager_uoploeste_kontakter_i_begge_grene():
    """Baade somatik og psyk skal med — en psyk-kontakt paa -1 er lige saa
    permanent som en somatisk."""
    sql = build_pre_stg_sql("2026-08-27T00:00:00")

    assert sql.count("FROM #uoploest_borger ub") == 2
    # Joinet gaar paa forretningsnoeglen, ikke paa dw_ek: pre_stg kender kun
    # clean_code, og fct baerer den med et unikt indeks.
    assert "ub.[clean_code] =" in sql


def test_checkpoint_vaerdi_escapes():
    sql = build_delta_scope_sql("2026' OR 1=1 --")
    assert "2026' OR 1=1" not in sql
    assert "2026'' OR 1=1 --" in sql


# ---------------------------------------------------------------------------
# Dedup og grain
# ---------------------------------------------------------------------------


def test_dedup_partitionerer_paa_clean_code_med_deterministisk_orden():
    sql = build_pre_stg_sql(None)
    assert "PARTITION BY [clean_code_DRGKontakt]" in sql
    assert "ORDER BY [dw_timestamp_update] DESC, [kontakt_uuid] ASC" in sql
    assert "WHERE [rn] = 1" in sql
    assert "UNIQUE CLUSTERED INDEX" in sql  # grain haandhaeves fysisk


# ---------------------------------------------------------------------------
# SOR-opslag pr. rolle
# ---------------------------------------------------------------------------


def test_sor_roller_daekker_de_fire_besluttede():
    assert dict(SOR_ROLES) == {
        "sygehus_sor": "dw_ek_Organisation_sygehus",
        "kontaktansvar_sor": "dw_ek_Organisation_kontaktansvar",
        "forloebselement_sor": "dw_ek_Organisation_forloebselement",
        "henvisende_sor": "dw_ek_Organisation_henvisende",
    }


def test_sor_opslag_beholder_raa_kode_og_bruger_minus_1():
    sql = build_stg_fct_sql()
    for raw, key in SOR_ROLES:
        assert f"AS [{key}]" in sql
        assert f"p.[{raw}]" in sql  # raa kode staar ved siden af noeglen
    assert "COALESCE(o0.[dw_ek_Organisation], -1)" in sql
    assert "#organisation_sor" in create_organisation_sor_lookup_table_sql()


def test_sor_rapportering_skelner_tom_fra_udfyldt_uden_match():
    sql = build_sor_coverage_sql()
    assert "[tom_kode]" in sql and "[udfyldt_uden_match]" in sql and "[matchet]" in sql


# ---------------------------------------------------------------------------
# Forloebsmaal paa baerende kontakt
# ---------------------------------------------------------------------------


def test_forloebsmaal_kun_paa_tungeste_somatik_kontakt():
    sql = build_stg_fct_sql()
    assert "p.[tungeste_drgkontakt] = '1'" in sql
    assert "f.[ind_kontakt_uuid] = p.[forloeb_noegle]" in sql
    assert "AS [forloeb_totalpris]" in sql


def test_psyk_baering_er_forloebets_foerste_kontakt():
    sql = build_stg_fct_sql()
    assert "fp.[foerste_kontakt_uuid] = p.[kontakt_uuid]" in sql
    assert "AS [forloeb_pris_psykiatriforloeb]" in sql


def test_kontroltotal_daekker_begge_leverancetyper_og_begge_brud():
    sql = build_forloeb_kontroltotal_sql()
    assert "[forloeb_med_flere_baerende]" in sql
    assert "[afvigende_maal]" in sql
    assert "'DRG'" in sql and "'DRGPSYK'" in sql
    assert "TOTALPRIS_DRGSGHF" in sql and "PRIS_PsykiatriForloeb" in sql


# ---------------------------------------------------------------------------
# Faelles kolonnesaet somatik/psyk (én samlet fact)
# ---------------------------------------------------------------------------


def test_psyk_faar_null_for_somatik_kolonner_og_omvendt():
    henvisende = next(c for c in PRE_COLUMNS if c.name == "henvisende_sor")
    assert henvisende.psyk is None
    assert henvisende.select_for("DRGPSYK") == "CAST(NULL AS NVARCHAR(25)) AS [henvisende_sor]"
    afsluttet = next(c for c in PRE_COLUMNS if c.name == "kontakt_afsluttet")
    assert afsluttet.somatik is None


def test_begge_brancher_stager_praecis_samme_kolonnesaet():
    sql = build_pre_stg_sql(None)
    for name in PRE_COLUMN_NAMES:
        assert sql.count(f"AS [{name}]") == 2, f"{name} skal staa i begge UNION-grene"


def test_stg_fct_typer_maal_med_try_convert():
    sql = build_stg_fct_sql()
    assert "TRY_CONVERT(DECIMAL(19,4), p.[totalpris_drg_raa])" in sql
    assert "TRY_CONVERT(INT, p.[liggedage_raa])" in sql


def test_dato_noegler_for_ind_ud_og_henvisning():
    from DSA.SQL.common.dato_dimension import dato_key

    sql = build_stg_fct_sql()
    for key in ("dw_ek_Dato_ind", "dw_ek_Dato_ud", "dw_ek_Dato_henvisning"):
        assert f"AS [{key}]" in sql
    # datonoeglerne kommer direkte fra den delte dato_key (ingen lokal
    # wrapper) — dens blank-gren haandterer tom NVARCHAR-dato -> BLANK
    for col in ("p.[ind_tidspunkt]", "p.[ud_tidspunkt]", "p.[henvisnings_tidspunkt]"):
        assert dato_key(col) in sql


def test_stg_dim_deler_grain_og_clean_code_med_fct():
    dim = build_stg_dim_sql()
    fct = build_stg_fct_sql()
    assert "[clean_code_DRGKontakt]" in dim and "[clean_code_DRGKontakt]" in fct
    assert "UNIQUE CLUSTERED INDEX" in dim


def test_kolonnenavne_er_unikke_og_uden_aeoeaa():
    assert len(set(PRE_COLUMN_NAMES)) == len(PRE_COLUMN_NAMES)
    for name in PRE_COLUMN_NAMES:
        assert not set(name) & set("æøåÆØÅ"), name


def test_stgcolumn_select_udtryk_og_null_pr_leverancetype():
    col = StgColumn("x", "k.[A]", None, "INT")
    assert col.select_for("DRG") == "CAST(k.[A] AS INT) AS [x]"
    assert col.select_for("DRGPSYK") == "CAST(NULL AS INT) AS [x]"


# ---------------------------------------------------------------------------
# FHIR-oprettelse af ukendte CPR (to-pass)
# ---------------------------------------------------------------------------


def test_fhir_cpr_sql_daekker_begge_leverancetyper_og_ekskluderer_slettede():
    """CPR_SQL er en skabelon pr. leverancetype: de to kilder koerer i hvert
    sit job, saa en faelles forespoergsel ville genindfoere den kobling,
    jobsplittet fjerner."""
    from DSA.FHIR.Patient.Patient_DRG import CPR_SQL, SOURCES

    assert set(SOURCES) == {"DRG", "DRGPSYK"}
    assert SOURCES["DRG"] == ("drg_kontakt", "CPR")
    assert SOURCES["DRGPSYK"] == ("drgpsyk_kontakt", "CPRnummer")

    for leverancetype, (kontakt_table, cpr_column) in SOURCES.items():
        sql = CPR_SQL.format(kontakt_table=kontakt_table, cpr_column=cpr_column, takstsystemer="?")
        assert f"[arc].[{kontakt_table}]" in sql, leverancetype
        assert sql.count("[dw_is_deleted] = 0") == 1, leverancetype
        assert "DISTINCT" in sql  # set-baseret (volumen-kravet)
        # scopet til koerslens takstsystem: takstsystem er den LEDENDE
        # PK-kolonne, saa det er et range-seek frem for en fuld scan
        assert "[takstsystem] IN (" in sql, leverancetype


def test_fhir_stub_baerer_kun_cpr_og_kildetag():
    """Stubben skrives gennem Patient-indgangen: kun personnummer, og byggeren sætter
    proveniensen — meta.source er byggerens URI, kildetagget er SDSDRG."""
    from DSA.FHIR.Patient.field_ownership import SOURCE_DRG
    from DSA.FHIR.Patient.patient_builder import build
    from DSA.FHIR.Patient.patient_contributions import stub_contribution
    from utils.fhir.identifier_systems import SOURCE_PATIENT_BUILDER

    assert SOURCE_DRG == "SDSDRG"
    patient = build([stub_contribution("0101011111", SOURCE_DRG)])
    assert patient.meta.source == SOURCE_PATIENT_BUILDER
    assert [t.code for t in patient.meta.tag] == ["SDSDRG"]
    assert len(patient.identifier) == 1
    assert patient.identifier[0].value == "0101011111"
    # stub by design: intet gaettes
    assert patient.birthDate is None and patient.gender is None and patient.name is None


def test_fhir_tier_klassificering_mod_fake_conn():
    from DSA.FHIR.Patient.Patient_DRG import classify_cprs

    class _Cursor:
        def execute(self, sql, *p):
            pass

        def fetchall(self):
            return [("0101011111", "A"), ("0202022222", "B"), ("0303033333", "C")]

        def close(self):
            pass

    class _Conn:
        def cursor(self):
            return _Cursor()

    class _Res:
        def get_connection(self):
            from contextlib import contextmanager

            @contextmanager
            def cm():
                yield _Conn()

            return cm()

    tiers = classify_cprs(_Res(), "DRG", ["T26"])
    assert tiers == {"A": ["0101011111"], "B": ["0202022222"], "C": ["0303033333"]}


def test_fhir_tier_uden_takstsystemer_roerer_ikke_arkivet():
    """Tomt manifest = ingen nye CPR'er. Arkivet maa ikke scannes."""
    from DSA.FHIR.Patient.Patient_DRG import classify_cprs

    class _Res:
        def get_connection(self):
            raise AssertionError("maa ikke aabne en forbindelse")

    assert classify_cprs(_Res(), "DRG", []) == {"A": [], "B": [], "C": []}


def test_cpr_joines_uden_trunkerende_laengdecast():
    """CAST(cpr AS NVARCHAR(10)) ville stille trunkere en defekt 11+-tegns
    vaerdi og kunne fejl-matche en anden persons noegle. En for lang vaerdi
    skal bare ikke matche (-1)."""
    sql = build_pre_stg_sql(None)
    # leverance-siden af borger-joinet maa aldrig laengdecastes
    assert "b.[cpr] = LTRIM(RTRIM(" in sql and "bt.[cpr] = LTRIM(RTRIM(" in sql
    assert "b.[cpr] = CAST(" not in sql and "bt.[cpr] = CAST(" not in sql
    from DSA.FHIR.Patient.Patient_DRG import CPR_SQL, SOURCES

    for kontakt_table, cpr_column in SOURCES.values():
        rendered = CPR_SQL.format(
            kontakt_table=kontakt_table, cpr_column=cpr_column, takstsystemer="?"
        )
        assert f"CAST([{cpr_column}]" not in rendered


def test_kontroltotal_identificerer_baerere_af_egenskab_ikke_maal():
    """Et forloeb hvis maal alle er blanke (TRY_CONVERT -> NULL) skal stadig
    taelle i flere-baerende-checket — ellers slipper to tungeste-kontakter paa
    samme forloeb igennem, netop naar leverancen ogsaa mangler prisen."""
    sql = build_forloeb_kontroltotal_sql()
    flere_del = sql.split("[forloeb_med_flere_baerende]")[0]
    assert "[TUNGESTE_DRGKONTAKT] = '1'" in flere_del
    assert "forloeb_totalpris] IS NOT NULL" not in sql
    psyk_flere = sql.split("UNION ALL")[1].split("(SELECT COUNT(*)")[1]
    assert "[arc].[drgpsyk_forloeb]" in psyk_flere
    assert "forloeb_pris_psykiatriforloeb] IS NOT NULL" not in sql


def test_forloebsmaal_baeres_kun_fra_levende_forloebsraekker():
    """Et forloeb SDS har trukket tilbage soft-slettes i arc, men raekken
    bliver liggende — uden filteret ville dens oekonomi fortsat blive baaret
    paa kontakten. Fact-join og kontroltotal skal filtrere ens, ellers
    melder kontrollen falsk afvigelse."""
    fct = build_stg_fct_sql()
    assert fct.count("[dw_is_deleted] = 0") == 2  # begge forloebs-joins
    kontrol = build_forloeb_kontroltotal_sql()
    # Alle tre arc-forloebs-joins filtrerer levende raekker: de to maal-joins
    # (DRG + psyk) og psyk-baerer-taellingen. DRG-baerer-taellingen filtrerer
    # paa alias k ([arc].[drg_kontakt]), saa den taelles ikke her.
    assert kontrol.count("f.[dw_is_deleted] = 0") == 3


def test_baerer_check_maales_i_arc_paa_tvaers_af_batches():
    """En baerer-dublet kan opstaa paa tvaers af to delta-koersler — stg ser
    kun sin egen batch, men arc ser begge. Checket skal derfor taelle
    levende arc-kontakter, scoped til de stagede forloeb."""
    sql = build_forloeb_kontroltotal_sql()
    flere_del = sql.split("[forloeb_med_flere_baerende]")[0]
    assert "[arc].[drg_kontakt]" in flere_del
    assert "k.[dw_is_deleted] = 0" in flere_del


def test_org_opslag_medtager_kun_levende_organisationer():
    """Skemaet garanterer kun kode-unikhed for levende raekker: en udgaaet
    organisations kode kan genudstedes under ny noegle, og uden filteret ville
    det unikke index paa #organisation_sor faelde hele koerslen — og nye fakta kunne
    resolvere til den udgaaede organisation."""
    sql = create_organisation_sor_lookup_table_sql()
    assert "[dw_is_deleted] = 0" in sql


def test_kontroltotal_sammenligner_null_sikkert():
    """Maal-sammenligningen skal vaere NULL-sikker (EXCEPT) frem for en
    magisk sentinel-vaerdi, der selv kunne forekomme som aegte maal."""
    sql = build_forloeb_kontroltotal_sql()
    assert "EXCEPT" in sql
    assert "-999999999" not in sql


def test_pre_stg_stager_eksplicit_kolonneliste():
    """SELECT * ville stage hjaelpekolonner (rn) og goere tabellens form
    afhaengig af subquery-detaljer; kolonnelisten afledes af mappingen."""
    sql = build_pre_stg_sql(None)
    assert "ALTER TABLE" not in sql
    ydre = sql.split("INTO")[0]
    assert "[clean_code_DRGKontakt]" in ydre and "[rn]" not in ydre


def test_dim_kolonneliste_afledes_af_mappingens_flag():
    from DSA.SQL.staging.drg_kontakt_kolonner import DIM_ATTRIBUTE_NAMES, PRE_COLUMNS

    assert set(DIM_ATTRIBUTE_NAMES) <= {c.name for c in PRE_COLUMNS}
    assert "drg_kode" in DIM_ATTRIBUTE_NAMES and "koen" in DIM_ATTRIBUTE_NAMES
    # opslags-input og raa maal hoerer IKKE i dimensionen
    for name in ("sygehus_sor", "pris_drg_raa", "ind_tidspunkt", "kontakt_uuid"):
        assert name not in DIM_ATTRIBUTE_NAMES

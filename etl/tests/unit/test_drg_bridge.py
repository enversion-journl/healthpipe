# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the DRG bridge/group layer (staging, code dims, groups, fct FKs).

The group-key design: fct.DRGKontakt references a contact's whole set of
diagnoses/procedures with one FK each (dw_ek_DRG{Diagnose,Procedure}Gruppe,
minted in map.DRG*Gruppe); append-only group->code bridges expand a group into
its member codes. SQL builders and the static DDL are tested as text.
"""

import re
from pathlib import Path

import pytest
from DSA.SQL.bridge.bridge_DRGDiagnoseGruppe import INSERT_SQL as DIAG_GRP_INSERT
from DSA.SQL.bridge.bridge_DRGDiagnoseGruppe import UNRESOLVED_SQL as DIAG_GRP_UNRESOLVED
from DSA.SQL.bridge.bridge_DRGProcedureGruppe import INSERT_SQL as PROC_GRP_INSERT
from DSA.SQL.bridge.bridge_DRGProcedureGruppe import UNRESOLVED_SQL as PROC_GRP_UNRESOLVED
from DSA.SQL.fct.fct_DRGKontakt import FACT_COLUMNS
from DSA.SQL.fct.fct_DRGKontakt import RESOLVE_SQL as FCT_RESOLVE
from DSA.SQL.map.map_DRGDiagnose import INSERT_SQL as MAP_DIAG_SQL
from DSA.SQL.map.map_DRGDiagnoseGruppe import INSERT_SQL as MAP_DIAG_GRP_SQL
from DSA.SQL.map.map_DRGProcedure import INSERT_SQL as MAP_PROC_SQL
from DSA.SQL.map.map_DRGProcedureGruppe import INSERT_SQL as MAP_PROC_GRP_SQL
from DSA.SQL.staging.drg_bridge_sql import (
    BEGINNING_OF_TIME,
    DIAGNOSE_ATTRIBUTES,
    PROCEDURE_ATTRIBUTES,
    build_diagnose_gruppe_stg_sql,
    build_diagnose_scope_sql,
    build_diagnose_stg_sql,
    build_procedure_gruppe_stg_sql,
    build_procedure_stg_sql,
    diagnose_clean_code_sql,
    procedure_clean_code_sql,
)

pytestmark = pytest.mark.unit

_DDL_DIR = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl" / "tables" / "drg"


def _ddl_columns(ddl_file: str, table: str) -> list[str]:
    text = (_DDL_DIR / ddl_file).read_text()
    m = re.search(rf"CREATE TABLE \[\w+\]\.\[{table}\] ?\((.*?)\n\s*\) ON \[PRIMARY\]", text, re.S)
    assert m, f"CREATE TABLE {table} ikke fundet i {ddl_file}"
    return re.findall(r"^\s+\[(\w+)\]", m.group(1), re.M)


# ---------------------------------------------------------------------------
# code clean_codes (code dimensions)
# ---------------------------------------------------------------------------


def test_diagnose_clean_code_hasher_alle_attributter():
    sql = diagnose_clean_code_sql("a")
    assert sql.startswith("'DRGDIAG_'") and "HASHBYTES('SHA2_256'" in sql
    for col in DIAGNOSE_ATTRIBUTES:
        assert f"a.[{col}]" in sql


def test_procedure_clean_code_uden_tidspunkter():
    sql = procedure_clean_code_sql("a")
    assert sql.startswith("'DRGPROC_'")
    for col in PROCEDURE_ATTRIBUTES:
        assert f"a.[{col}]" in sql
    assert "STARTTIDSPUNKT" not in sql and "SLUTTIDSPUNKT" not in sql


def test_clean_codes_er_laengde_praefikset():
    assert diagnose_clean_code_sql("a").count("CONCAT(LEN(") == len(DIAGNOSE_ATTRIBUTES)
    assert procedure_clean_code_sql("a").count("CONCAT(LEN(") == len(PROCEDURE_ATTRIBUTES)


# ---------------------------------------------------------------------------
# code staging (per registration, scope-driven)
# ---------------------------------------------------------------------------


def test_diagnose_stg_unionerer_begge_leverancetyper_og_scopes():
    sql = build_diagnose_stg_sql()
    assert "[arc].[drg_diagnose]" in sql and "[arc].[drgpsyk_diagnose]" in sql
    assert "clean_code_DRGDiagnose" in sql and "clean_code_DRGKontakt" in sql
    assert "[stg].[bridge_DRGKontaktDiagnose_scope]" in sql


def test_procedure_stg_har_producerende_men_ingen_tidspunkter():
    sql = build_procedure_stg_sql()
    assert "producerende_sor" in sql
    assert "PROCEDURE_STARTTIDSPUNKT" not in sql and "PROCEDURE_SLUTTIDSPUNKT" not in sql


def test_scope_beginning_of_time_tager_hele_arkivet():
    assert f"dw_timestamp_update] > '{BEGINNING_OF_TIME}'" in build_diagnose_scope_sql(
        BEGINNING_OF_TIME
    )


# ---------------------------------------------------------------------------
# group staging: sorted, distinct set-hash per contact + group->member rows
# ---------------------------------------------------------------------------


def test_diagnose_gruppe_stg_hasher_sorteret_saet():
    sql = build_diagnose_gruppe_stg_sql()
    assert "[stg].[DRGKontaktDiagnoseGruppe]" in sql
    assert "[stg].[DRGDiagnoseGruppeMedlem]" in sql
    # sorted, delimited set over the DISTINCT members, hashed
    assert "STRING_AGG" in sql and "WITHIN GROUP (ORDER BY clean_code_DRGDiagnose)" in sql
    assert "'DRGDIAGGRP_'" in sql and "HASHBYTES('SHA2_256'" in sql
    assert "SELECT DISTINCT [clean_code_DRGKontakt], [clean_code_DRGDiagnose]" in sql


def test_procedure_gruppe_stg_inkluderer_producerende_enhed():
    sql = build_procedure_gruppe_stg_sql()
    assert "[stg].[DRGKontaktProcedureGruppe]" in sql
    assert "[stg].[DRGProcedureGruppeMedlem]" in sql
    assert "'DRGPROCGRP_'" in sql
    # producing unit is resolved (via #org) and part of the group's set
    assert "#org" in sql and "dw_ek_Organisation_producerende" in sql


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------


def test_dim_diagnose_ddl_matcher_attributterne():
    ddl = _ddl_columns("03_dims.sql", "DRGDiagnose")
    assert ddl == [
        "dw_ek_DRGDiagnose",
        "clean_code_DRGDiagnose",
        *DIAGNOSE_ATTRIBUTES,
        "dw_timestamp_insert",
        "dw_timestamp_update",
    ]


def test_dim_procedure_ddl_matcher_attributterne():
    ddl = _ddl_columns("03_dims.sql", "DRGProcedure")
    assert ddl == [
        "dw_ek_DRGProcedure",
        "clean_code_DRGProcedure",
        *PROCEDURE_ATTRIBUTES,
        "dw_timestamp_insert",
        "dw_timestamp_update",
    ]


def test_map_gruppe_ddl_er_intern_tabeller():
    for tbl, key in (
        ("DRGDiagnoseGruppe", "clean_code_DRGDiagnoseGruppe"),
        ("DRGProcedureGruppe", "clean_code_DRGProcedureGruppe"),
    ):
        ddl = _ddl_columns("02_maps.sql", tbl)
        assert ddl == [
            f"dw_ek_{tbl}",
            key,
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]


def test_diagnose_gruppe_bridge_er_ren_kobling():
    ddl = _ddl_columns("06_bridges.sql", "DRGDiagnoseGruppe")
    assert ddl == [
        "dw_ek_DRGDiagnoseGruppe",
        "dw_ek_DRGDiagnose",
        "dw_is_deleted",
        "dw_timestamp_insert",
        "dw_timestamp_update",
    ]


def test_procedure_gruppe_bridge_baerer_producerende_i_pk():
    text = (_DDL_DIR / "06_bridges.sql").read_text()
    pk = re.search(r"PK_Bridge_DRGProcedureGruppe\] PRIMARY KEY CLUSTERED \(([^)]*)\)", text).group(
        1
    )
    assert "dw_ek_DRGProcedureGruppe" in pk
    assert "dw_ek_DRGProcedure" in pk
    assert "dw_ek_Organisation_producerende" in pk


def test_fct_ddl_har_gruppe_fk_kolonner():
    ddl = _ddl_columns("04_facts.sql", "DRGKontakt")
    assert "dw_ek_DRGDiagnoseGruppe" in ddl
    assert "dw_ek_DRGProcedureGruppe" in ddl
    # ordered right after the klassifikation FK
    ki = ddl.index("dw_ek_DRGKontaktKlassifikation")
    assert ddl[ki + 1] == "dw_ek_DRGDiagnoseGruppe"
    assert ddl[ki + 2] == "dw_ek_DRGProcedureGruppe"


# ---------------------------------------------------------------------------
# maps + group bridges are append-only; fct resolves the group FKs
# ---------------------------------------------------------------------------


def test_alle_maps_er_append_only():
    for sql in (MAP_DIAG_SQL, MAP_PROC_SQL, MAP_DIAG_GRP_SQL, MAP_PROC_GRP_SQL):
        assert "NOT EXISTS" in sql
        assert not re.search(r"\bMERGE\b|\bUPDATE\b|\bDELETE\b", sql, re.I)


def test_gruppe_bridges_er_append_only_ingen_soft_delete():
    for sql in (DIAG_GRP_INSERT, PROC_GRP_INSERT):
        assert "INSERT INTO" in sql and "NOT EXISTS" in sql
        # append-only membership: no MERGE, no UPDATE/soft-delete of rows
        assert not re.search(r"\bMERGE\b", sql, re.I)
        assert "dw_is_deleted = 1" not in sql


def test_gruppe_bridges_fail_fast_paa_uoploeste():
    assert "[map].[DRGDiagnoseGruppe]" in DIAG_GRP_UNRESOLVED
    assert "[map].[DRGDiagnose]" in DIAG_GRP_UNRESOLVED
    assert "[map].[DRGProcedureGruppe]" in PROC_GRP_UNRESOLVED
    assert "[map].[DRGProcedure]" in PROC_GRP_UNRESOLVED


def test_procedure_gruppe_bridge_baerer_producerende():
    assert "dw_ek_Organisation_producerende" in PROC_GRP_INSERT
    assert "dw_ek_Organisation_producerende" not in DIAG_GRP_INSERT


def test_fct_har_gruppe_fk_i_kolonnesaet_og_resolver_dem():
    assert "dw_ek_DRGDiagnoseGruppe" in FACT_COLUMNS
    assert "dw_ek_DRGProcedureGruppe" in FACT_COLUMNS
    # resolved via the group staging + group map, empty set -> -1
    assert "[stg].[DRGKontaktDiagnoseGruppe]" in FCT_RESOLVE
    assert "[map].[DRGDiagnoseGruppe]" in FCT_RESOLVE
    assert "COALESCE(diag_grp_map.[dw_ek_DRGDiagnoseGruppe], -1)" in FCT_RESOLVE
    assert "COALESCE(proc_grp_map.[dw_ek_DRGProcedureGruppe], -1)" in FCT_RESOLVE


def test_fct_merge_udelader_gruppe_fk_men_insert_beholder_dem():
    # A cross-type delta (only one code type changes) must not let the MERGE
    # write the other type's group FK to -1: the group FKs are excluded from the
    # MERGE compare/update (owned by the refresh) but still set on INSERT.
    from DSA.SQL.fct.fct_DRGKontakt import GROUP_FK_COLUMNS
    from DSA.SQL.fct.fct_DRGKontakt import INSERT_SQL as FCT_INSERT
    from DSA.SQL.fct.fct_DRGKontakt import MERGE_SQL as FCT_MERGE

    for col in GROUP_FK_COLUMNS:
        assert f"tgt.[{col}] = src.[{col}]" not in FCT_MERGE  # not in UPDATE set
        assert f"src.[{col}]" not in FCT_MERGE.split("WHEN NOT MATCHED")[0]  # not in EXCEPT
        assert f"[{col}]" in FCT_INSERT  # but new rows carry it


def test_fct_fail_faster_paa_uoploeste_gruppe_hashes():
    from DSA.SQL.fct.fct_DRGKontakt import GROUP_UNMAPPED_SQL

    assert "[map].[DRGDiagnoseGruppe]" in GROUP_UNMAPPED_SQL
    assert "[map].[DRGProcedureGruppe]" in GROUP_UNMAPPED_SQL
    assert "IS NULL" in GROUP_UNMAPPED_SQL


# ---------------------------------------------------------------------------
# registration and DQA
# ---------------------------------------------------------------------------


def test_assets_registreret_med_lag_grupper():
    from DSA.SQL.bridge import all_assets as b
    from DSA.SQL.dim import all_assets as d
    from DSA.SQL.map import all_assets as m
    from DSA.SQL.staging import all_assets as s

    grupper = {}
    for a in (*b, *d, *m, *s):
        for spec in a.specs:
            grupper[spec.key.to_user_string()] = spec.group_name
    assert grupper["bridge_DRGDiagnoseGruppe"] == "drg_dimension"
    assert grupper["bridge_DRGProcedureGruppe"] == "drg_dimension"
    assert grupper["map_DRGDiagnoseGruppe"] == "drg_map"
    assert grupper["map_DRGProcedureGruppe"] == "drg_map"
    assert grupper["dim_DRGDiagnose"] == "drg_dimension"
    # staging assets live under staging/ (layer-directory convention)
    assert grupper["drg/stg_DRGDiagnoseGruppe"] == "drg_staging"
    assert grupper["drg/stg_bridge_DRGKontaktDiagnose"] == "drg_staging"


def test_gruppe_bridge_ri_edges_peger_kun_paa_eksisterende_tabeller():
    # The group key is the bridge's own composite-PK identity (minted in map,
    # no dimension), so it must NOT produce an RI edge to a non-existent
    # dim.DRG*Gruppe; only the real code/org edges resolve.
    from dqa.references.lineage import edges_for

    diag_parents = {e.parent_table for e in edges_for("bridge.DRGDiagnoseGruppe")}
    assert diag_parents == {"dim.DRGDiagnose"}
    proc_parents = {e.parent_table for e in edges_for("bridge.DRGProcedureGruppe")}
    assert proc_parents == {"dim.DRGProcedure", "dim.Organisation"}


def test_dqa_subjects_for_begge_gruppe_bridges():
    from dqa.config.dwh_subjects import DWH_SUBJECTS

    tabeller = {s.table for s in DWH_SUBJECTS}
    assert "bridge.DRGDiagnoseGruppe" in tabeller
    assert "bridge.DRGProcedureGruppe" in tabeller
    for t in ("bridge.DRGDiagnoseGruppe", "bridge.DRGProcedureGruppe"):
        subj = next(s for s in DWH_SUBJECTS if s.table == t)
        assert any(c.check_type == "referential_integrity" for c in subj.checks)

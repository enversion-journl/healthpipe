# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the DRG map/dim/fct layer.

SQL builders and constants are tested as text (no database); the static DDL
is held to the column mapping the same way the archive DDL is held to
drg_datasets.py, so mapping, staging, dimension SQL and DDL cannot drift.
"""

import re
from pathlib import Path

import pytest
from DSA.SQL.dim.dim_DRGKontaktEgenskaber import ATTRIBUTES as EGENSKABER_ATTRIBUTES
from DSA.SQL.dim.dim_DRGKontaktEgenskaber import MERGE_SQL as EGENSKABER_MERGE_SQL
from DSA.SQL.dim.dim_DRGKontaktKlassifikation import ATTRIBUTES as KLASS_DIM_ATTRIBUTES
from DSA.SQL.dim.dim_DRGKontaktKlassifikation import BESKRIVELSE_REFRESH_SQL, DENORMALISED
from DSA.SQL.dim.dim_DRGKontaktKlassifikation import MERGE_SQL as KLASS_MERGE_SQL
from DSA.SQL.dim.dim_DRGKontaktKlassifikation import RESOLVE_SQL as KLASS_RESOLVE_SQL
from DSA.SQL.fct.fct_DRGKontakt import (
    FACT_COLUMNS,
    FK_DIAGNOSTIK_SQL,
    FK_ROLES,
    KONTROLTOTAL_SQL,
    MERGE_SQL,
    RESOLVE_SQL,
)
from DSA.SQL.map.map_DRGKontakt import INSERT_SQL as MAP_KONTAKT_SQL
from DSA.SQL.map.map_DRGKontaktKlassifikation import INSERT_SQL as MAP_KLASS_SQL
from DSA.SQL.staging.drg_kontakt_kolonner import (
    KLASSIFIKATION_ATTRIBUTES,
    PRE_COLUMNS,
    klassifikation_clean_code_sql,
)

pytestmark = pytest.mark.unit

_DDL_DIR = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl" / "tables" / "drg"


def _ddl_columns(ddl_file: str, table: str) -> list[str]:
    """Column names of one CREATE TABLE block, in DDL order."""
    text = (_DDL_DIR / ddl_file).read_text()
    match = re.search(
        rf"CREATE TABLE \[\w+\]\.\[{table}\]\((.*?)\n    \) ON \[PRIMARY\]", text, re.S
    )
    assert match, f"CREATE TABLE {table} ikke fundet i {ddl_file}"
    return re.findall(r"^\s+\[(\w+)\]", match.group(1), re.M)


# ---------------------------------------------------------------------------
# clean_codes
# ---------------------------------------------------------------------------


def test_klassifikation_clean_code_hasher_alle_noegleattributter():
    sql = klassifikation_clean_code_sql("d")
    assert sql.startswith("'DRGKLASS_'")
    assert "HASHBYTES('SHA2_256'" in sql
    for col in ("leverancetype", "takstsystem", *KLASSIFIKATION_ATTRIBUTES):
        assert f"d.[{col}]" in sql, f"{col} mangler i hash-inputtet"


def test_klassifikation_clean_code_er_laengde_praefikset():
    """Uden laengde-praefiks ville ('AB','C') og ('A','BC') hashe ens —
    sammenkaedningen skal vaere injektiv."""
    sql = klassifikation_clean_code_sql("d")
    assert sql.count("CONCAT(LEN(") == 2 + len(KLASSIFIKATION_ATTRIBUTES)


# ---------------------------------------------------------------------------
# DDL holdes til mappingen
# ---------------------------------------------------------------------------


def test_dim_egenskaber_ddl_matcher_mappingen():
    ddl = _ddl_columns("03_dims.sql", "DRGKontaktEgenskaber")
    forventet = [
        "dw_ek_DRGKontakt",
        "clean_code_DRGKontakt",
        *EGENSKABER_ATTRIBUTES,
        "dw_timestamp_insert",
        "dw_timestamp_update",
    ]
    assert ddl == forventet


def test_dim_klassifikation_ddl_matcher_attributsaettet():
    ddl = _ddl_columns("03_dims.sql", "DRGKontaktKlassifikation")
    forventet = [
        "dw_ek_DRGKontaktKlassifikation",
        "clean_code_DRGKontaktKlassifikation",
        *KLASS_DIM_ATTRIBUTES,
        *DENORMALISED,
        "dw_timestamp_insert",
        "dw_timestamp_update",
    ]
    assert ddl == forventet


def test_fct_ddl_matcher_fact_kolonnerne():
    ddl = _ddl_columns("04_facts.sql", "DRGKontakt")
    forventet = [
        "dw_ek_DRGKontakt",
        *FACT_COLUMNS,
        "dw_timestamp_insert",
        "dw_timestamp_update",
    ]
    assert ddl == forventet


def test_ddl_typer_matcher_mappingens_attributtyper():
    text = (_DDL_DIR / "03_dims.sql").read_text()
    types = {c.name: c.sql_type for c in PRE_COLUMNS}
    for navn in EGENSKABER_ATTRIBUTES:
        if navn in ("leverancetype", "takstsystem"):
            continue
        assert re.search(
            rf"\[{navn}\] {re.escape(types[navn])} NULL", text
        ), f"{navn}: DDL-typen matcher ikke mappingens {types[navn]}"


# ---------------------------------------------------------------------------
# map er append-only
# ---------------------------------------------------------------------------


def test_maps_er_append_only():
    for sql in (MAP_KONTAKT_SQL, MAP_KLASS_SQL):
        assert "NOT EXISTS" in sql
        # ordgraenser: kolonnenavne som dw_is_deleted maa ikke udloese tjekket
        assert not re.search(r"\bMERGE\b|\bUPDATE\b|\bDELETE\b", sql, re.I)


# ---------------------------------------------------------------------------
# dims: Type 1 med NULL-sikker change detection
# ---------------------------------------------------------------------------


def test_dim_merges_bruger_except_change_detection():
    for sql in (EGENSKABER_MERGE_SQL, KLASS_MERGE_SQL):
        assert "WHEN MATCHED AND EXISTS (" in sql
        assert "EXCEPT" in sql
        assert "WHEN NOT MATCHED BY SOURCE" not in sql  # dims retirerer aldrig


def test_dim_egenskaber_except_daekker_alle_attributter():
    matched = EGENSKABER_MERGE_SQL.split("WHEN NOT MATCHED")[0]
    for col in EGENSKABER_ATTRIBUTES:
        assert f"src.[{col}]" in matched and f"tgt.[{col}]" in matched


def test_dim_klassifikation_aggregerer_og_denormaliserer():
    assert "GROUP BY ds.[clean_code_DRGKontaktKlassifikation]" in KLASS_RESOLVE_SQL
    assert "MAX(fs.[pris_drg]) AS [standardtakst]" in KLASS_RESOLVE_SQL
    assert "[arc].[drg_takstbeskrivelse]" in KLASS_RESOLVE_SQL
    assert "t.[dw_is_deleted] = 0" in KLASS_RESOLVE_SQL


def test_klassifikation_beskrivelse_refreshes_globalt():
    """drg_beskrivelse er katalog-sourced, saa en katalog-kun-revision skal
    naa ALLE medlemmer — ikke kun dem der er i den aktuelle delta-batch.
    Refresh'en rammer hele tabellen keyed paa (takstsystem, drg_kode)."""
    assert "UPDATE k" in BESKRIVELSE_REFRESH_SQL
    assert "[dim].[DRGKontaktKlassifikation] k" in BESKRIVELSE_REFRESH_SQL
    assert "[arc].[drg_takstbeskrivelse] t" in BESKRIVELSE_REFRESH_SQL
    # ingen binding til #temp/delta-batchen — det er en global opdatering
    assert "#DRGKontaktKlassifikation" not in BESKRIVELSE_REFRESH_SQL
    # kun aendrede beskrivelser roeres (idempotent)
    assert "ISNULL(k.[drg_beskrivelse]" in BESKRIVELSE_REFRESH_SQL


def test_pre_stg_delta_vindue_omfatter_alle_downstream_ejere():
    """Delta-vinduet skal vaere MIN over alle downstream checkpoint-ejere
    (begge dimensioner + facten), saa en bagud-haengende ejer faar sine
    raekker gen-staget indtil den indhenter."""
    kilde = (
        Path(__file__).resolve().parents[2] / "DSA" / "SQL" / "staging" / "pre_stg_DRGKontakt.py"
    ).read_text()
    for ejer in (
        'get_checkpoint("dim", "DRGKontaktEgenskaber")',
        'get_checkpoint("dim", "DRGKontaktKlassifikation")',
        'get_checkpoint("fct", "DRGKontakt")',
    ):
        assert ejer in kilde, f"{ejer} mangler i delta-vinduet"


# ---------------------------------------------------------------------------
# fct: resolve, grain, kontroltotal, diagnostik
# ---------------------------------------------------------------------------


def test_fct_resolve_falder_tilbage_til_sentinel():
    assert "COALESCE(klass_map.[dw_ek_DRGKontaktKlassifikation], -1)" in RESOLVE_SQL
    assert "INNER JOIN [map].[DRGKontakt]" in RESOLVE_SQL


def test_fct_merge_except_daekker_alle_kolonner_undtagen_gruppe_fk():
    from DSA.SQL.fct.fct_DRGKontakt import GROUP_FK_COLUMNS

    matched = MERGE_SQL.split("WHEN NOT MATCHED")[0]
    for col in FACT_COLUMNS:
        if col in GROUP_FK_COLUMNS:
            # group FKs are owned by the per-type refresh, not the MERGE, so a
            # cross-type delta never clobbers the other type's key to -1
            assert f"src.[{col}]" not in matched and f"tgt.[{col}]" not in matched
        else:
            assert f"src.[{col}]" in matched and f"tgt.[{col}]" in matched


def test_fct_kontroltotal_sammenligner_levende_skiver():
    assert KONTROLTOTAL_SQL.count("[dw_is_deleted] = 0") == 3  # fct + begge arkiver
    for tabel in ("[arc].[drg_kontakt]", "[arc].[drgpsyk_kontakt]", "[fct].[DRGKontakt]"):
        assert tabel in KONTROLTOTAL_SQL
    assert "FULL OUTER JOIN" in KONTROLTOTAL_SQL
    # psyk har ingen totalpris — begge sider skal sammenligne 0 mod 0
    assert KONTROLTOTAL_SQL.count("ISNULL(SUM(") == 2


def test_fct_diagnostik_daekker_alle_fk_roller():
    for col in FK_ROLES:
        assert f"[{col}] = -1" in FK_DIAGNOSTIK_SQL
    assert "[dw_ek_Borger] = -2" in FK_DIAGNOSTIK_SQL  # strukturelt tom CPR taelles separat


# ---------------------------------------------------------------------------
# registrering og DQA
# ---------------------------------------------------------------------------


def test_assets_er_registreret_med_drg_grupper():
    from DSA.SQL.dim import all_assets as dim_assets
    from DSA.SQL.fct import all_assets as fct_assets
    from DSA.SQL.map import all_assets as map_assets

    grupper = {}
    for a in (*map_assets, *dim_assets, *fct_assets):
        for spec in a.specs:
            grupper[spec.key.to_user_string()] = spec.group_name
    assert grupper["map_DRGKontakt"] == "drg_map"
    assert grupper["map_DRGKontaktKlassifikation"] == "drg_map"
    assert grupper["dim_DRGKontaktEgenskaber"] == "drg_dimension"
    assert grupper["dim_DRGKontaktKlassifikation"] == "drg_dimension"
    assert grupper["fct_DRGKontakt"] == "drg_fact"


def test_dqa_subject_genererer_checks_fra_lineage():
    from dqa.config.dwh_subjects import DWH_SUBJECTS

    subject = next(s for s in DWH_SUBJECTS if s.table == "fct.DRGKontakt")
    check_types = {c.check_type for c in subject.checks}
    assert "sentinel_rate" in check_types
    assert "referential_integrity" in check_types
    assert subject.identity_column == "dw_ek_DRGKontakt"

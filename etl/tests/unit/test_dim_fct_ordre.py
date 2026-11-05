# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the dim.Ordre / fct.Ordre chain.

The chain's contract is its column set: staging, the MERGEs, the DDL and the
lineage YAML must name the same columns.
"""

import re
import sys
from pathlib import Path

import DSA.SQL.dim.dim_Ordre  # noqa: F401
import DSA.SQL.fct.fct_Ordre  # noqa: F401
import DSA.SQL.staging.pre_stg_Ordre_ils  # noqa: F401
import DSA.SQL.staging.stg_dim_Ordre_ils  # noqa: F401
import DSA.SQL.staging.stg_fct_Ordre_ils  # noqa: F401
import pytest
import yaml
from DSA.SQL.common.cpr_form_sql import CPR_FORM_MARKER
from DSI.mssql.archive.ils_archive_configs import ils_dfm_archive_configs

pytestmark = [pytest.mark.unit, pytest.mark.erp]

_pre = sys.modules["DSA.SQL.staging.pre_stg_Ordre_ils"]
_stg_dim = sys.modules["DSA.SQL.staging.stg_dim_Ordre_ils"]
_stg_fct = sys.modules["DSA.SQL.staging.stg_fct_Ordre_ils"]
_dim = sys.modules["DSA.SQL.dim.dim_Ordre"]
_fct = sys.modules["DSA.SQL.fct.fct_Ordre"]

ETL = Path(__file__).resolve().parents[2]
DDL = ETL / "infrastructure" / "ddl" / "tables" / "erp"
LINEAGE = ETL / "data_lineage"


def _ddl_columns(file: str, schema: str, table: str) -> list[str]:
    ddl = (DDL / file).read_text(encoding="utf-8")
    body = re.search(rf"CREATE TABLE \[{schema}\]\.\[{table}\]\s*\((.*?)\n    \) ON", ddl, re.S)
    assert body, f"CREATE TABLE [{schema}].[{table}] not found in {file}"
    return re.findall(r"^\s+\[(\w+)\]", body.group(1), re.M)


def _selected_aliases(sql: str, into: str) -> list[str]:
    select = sql.split(f"INTO {into}")[0].rsplit("SELECT", 1)[1]
    return [
        re.search(r"(\w+)\s*$", line.strip().rstrip(",")).group(1)
        for line in select.strip().splitlines()
    ]


def _lineage_columns(path: str) -> list[str]:
    doc = yaml.safe_load((LINEAGE / path).read_text(encoding="utf-8"))
    return list(doc["dso_lineage"]["Columns"])


class TestDimColumnContract:
    def test_staging_and_dim_lookups_cover_the_dim_attributes(self):
        staged = _selected_aliases(_stg_dim.BUILD_SQL, "[stg].[dim_Ordre_ils]")
        resolved = _selected_aliases(_dim.RESOLVE_SQL, "#Ordre_Resolved")
        assert resolved == ["dw_ek_Ordre", *_dim.ATTRIBUTE_COLUMNS]
        # The dim layer turns the user's CleanCode into its key and adds the date key.
        lookups = {"dw_ek_ILSBruger_oprettet_af": "clean_code_ILSBruger_oprettet_af"}
        assert staged == [
            lookups.get(c, c) for c in _dim.ATTRIBUTE_COLUMNS if c != "dw_ek_Dato_ajourfoert"
        ]

    def test_dim_reads_staging_and_maps_only(self):
        assert "[dim]." not in _dim.RESOLVE_SQL

    def test_ddl_holds_key_attributes_and_audit(self):
        assert _ddl_columns("02_dims.sql", "dim", "Ordre") == [
            "dw_ek_Ordre",
            *_dim.ATTRIBUTE_COLUMNS,
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]

    def test_lineage_documents_every_non_audit_column(self):
        assert _lineage_columns("dim/dim_Ordre.yml") == ["dw_ek_Ordre", *_dim.ATTRIBUTE_COLUMNS]


class TestFctColumnContract:
    def test_staging_feeds_every_fact_column(self):
        staged = _selected_aliases(_stg_fct.BUILD_SQL, "[stg].[fct_Ordre_ils]")
        resolved = _selected_aliases(_fct.RESOLVE_SQL, "#Ordre_Facts")
        assert resolved == ["dw_ek_Ordre", *_fct.FACT_COLUMNS]
        lookups = {
            "dw_ek_Lager": "clean_code_Lager",
            "dw_ek_ILSLeverandoer": "clean_code_ILSLeverandoer",
        }
        assert staged == [lookups.get(c, c) for c in _fct.FACT_COLUMNS]

    def test_ddl_holds_key_columns_and_audit(self):
        assert _ddl_columns("03_facts.sql", "fct", "Ordre") == [
            "dw_ek_Ordre",
            *_fct.FACT_COLUMNS,
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]

    def test_lineage_documents_every_non_audit_column(self):
        assert set(_lineage_columns("fct/fct_Ordre.yml")) == {"dw_ek_Ordre", *_fct.FACT_COLUMNS}


class TestPreStaging:
    def test_reads_the_archive_not_ext(self):
        for delta in (False, True):
            sql = _pre.build_sql(delta)
            assert "[arc].[ils_ordh]" in sql
            assert "[ext]." not in sql

    def test_delta_binds_the_checkpoint_once_and_full_load_not_at_all(self):
        assert _pre.build_sql(delta=True).count("?") == 1
        assert "?" not in _pre.build_sql(delta=False)

    def test_staged_archive_columns_are_archived(self):
        cfg = next(c for c in ils_dfm_archive_configs if c.target_table == "ils_ordh")
        archived = {"prrowid", *cfg.non_key_columns, "dw_timestamp_update"}
        select = _pre.build_sql(False).split("INTO [stg].[pre_Ordre_ils]")[0].rsplit("SELECT", 1)[1]
        # The bare column lines; the CleanCode CASE expressions span several lines.
        bare = re.findall(r"^\s+(\w+),?$", select, re.M)
        raw = [c for c in bare if c != "CASE" and not c.startswith("clean_code_")]
        assert raw and set(raw) <= archived

    def test_home_patient_address_is_not_staged(self):
        sql = _pre.build_sql(False)
        for column in ("adr1", "adr2", "adr3", "adr4", "adr5"):
            assert column not in sql


class TestCprFormMasking:
    def test_creator_is_masked_in_staging(self):
        assert CPR_FORM_MARKER in _stg_dim.BUILD_SQL

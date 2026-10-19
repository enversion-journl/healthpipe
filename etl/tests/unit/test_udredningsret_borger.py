# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for udredningsrets Patient-stubs (DSA/FHIR/Patient/Patient_Udredningsret.py).

To-pas-designet er arvet fra HAIBA/DRG (spec §5): CPR_SQL'ens kilde
og filtre, registreringen i DSA.FHIR.Patient og placeringen i
udredningsret_ingest_job. Selve skrivningen går gennem Patient-indgangen og
testes sammen med de øvrige kilder i test_cpr_stub_asset.py.
"""

from contextlib import contextmanager

import pytest
from DSA.FHIR.Patient import all_assets
from DSA.FHIR.Patient.cpr_stub_asset import TIER_SQL, TIER_STATEMENTS
from DSA.FHIR.Patient.Patient_Udredningsret import (
    CPR_SQL,
    classify_cprs,
    fhir_patient_udredningsret,
)

pytestmark = pytest.mark.unit

ARC = "[arc].[udredningsret_mont]"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeMssql:
    """MSSQLResource-stand-in: returnerer faste (cpr, tier)-raekker."""

    def __init__(self, rows):
        self.rows = rows
        self.executed: list[str] = []

    def get_connection(self):
        res = self

        class _Cursor:
            def execute(self, sql, *p):
                res.executed.append(sql)

            def fetchall(self):
                return res.rows

            def close(self):
                pass

        class _Conn:
            def cursor(self):
                return _Cursor()

        @contextmanager
        def cm():
            yield _Conn()

        return cm()


# ---------------------------------------------------------------------------
# CPR_SQL
# ---------------------------------------------------------------------------


def test_cpr_sql_laeser_kun_levende_arkivraekker_fra_mont():
    """Kilden er arc.udredningsret_mont (aldrig ext), og en markoer der har
    forladt snapshottet maa ikke skabe en person."""
    assert ARC in CPR_SQL
    assert "[ext]." not in CPR_SQL
    assert "[dw_is_deleted] = 0" in CPR_SQL
    assert "[CPR] IS NOT NULL" in CPR_SQL
    assert "LTRIM(RTRIM([CPR])) <> ''" in CPR_SQL
    assert "DISTINCT" in CPR_SQL  # set-baseret: ~8M raekker, <1M personer


def test_cpr_sql_laengdecaster_aldrig_cpr():
    """CAST(cpr AS NVARCHAR(10)) ville stille trunkere en defekt 11+-tegns
    vaerdi og oprette en stub under en anden persons identitet. Samme
    beslutning som DRG: en for lang vaerdi matcher bare ikke."""
    uden_kommentarer = "\n".join(
        linje for linje in CPR_SQL.splitlines() if not linje.strip().startswith("--")
    )
    assert "CAST(" not in uden_kommentarer
    assert "NVARCHAR" not in uden_kommentarer
    assert "LTRIM(RTRIM([CPR])) AS cpr" in CPR_SQL


def test_tier_sql_deler_tier_a_b_c_som_haiba_og_drg():
    """Tier A afgoeres af ref.PatientToBorger (levende raekker), tier B af
    fhir.Patient — hvor et tidligere CPR taeller som kendt."""
    statements = "\n".join(TIER_STATEMENTS)
    assert "[ref].[PatientToBorger]" in statements
    assert "dw_is_deleted = 0 AND cpr IS NOT NULL" in statements
    assert "patient_ssn" in statements and "previous_ssn" in statements
    assert "THEN 'A'" in TIER_SQL and "THEN 'B'" in TIER_SQL and "ELSE 'C'" in TIER_SQL


def test_classify_cprs_koerer_tier_sql_og_fordeler_paa_tiers():
    mssql = _FakeMssql([("0101011111", "A"), ("0202022222", "B"), ("0303033333", "C")])

    tiers = classify_cprs(mssql)

    assert any(CPR_SQL in sql for sql in mssql.executed)
    assert mssql.executed[-1] == TIER_SQL
    assert tiers == {"A": ["0101011111"], "B": ["0202022222"], "C": ["0303033333"]}


# ---------------------------------------------------------------------------
# Registrering og job-placering
# ---------------------------------------------------------------------------


def _asset_by_name(name: str):
    for asset in all_assets:
        if asset.key.to_user_string() == name:
            return asset
    raise AssertionError(f"{name} er ikke registreret i DSA.FHIR.Patient.all_assets")


def test_assettet_er_registreret_i_patient_pakken_med_gruppe_udredningsret_borger():
    asset = _asset_by_name("fhir_patient_udredningsret")

    assert asset is fhir_patient_udredningsret
    assert asset.group_names_by_key[asset.key] == "udredningsret_borger"
    deps = {k.to_user_string() for k in asset.asset_deps[asset.key]}
    assert deps == {"arc_udredningsret_mont"}
    tags = asset.tags_by_key[asset.key]
    assert tags["domain"] == "udredningsret" and tags["source"] == "udredningsret"
    assert tags["layer"] == "dsa"


def test_ingest_job_ejer_stub_oprettelsen():
    """Stub-assettet SKAL ligge i ingest — ellers koerer det i intet job, og
    tier-C-CPR'er oprettes aldrig, saa forloebene staar paa dw_ek_Borger = -1
    for evigt. Publish er et separat job, fordi stubs foerst bliver en
    dw_ek_Borger efter natkaeden."""
    from jobs.udredningsret_jobs import udredningsret_ingest_job

    ingest = str(udredningsret_ingest_job.selection)
    for group in ("udredningsret_extract", "udredningsret_archive", "udredningsret_borger"):
        assert group in ingest, group


def test_ingest_job_oploeser_stub_assettet_i_asset_grafen():
    """AssetSelection.groups() oploeser en ukendt gruppe til tom selektion uden
    fejl — derfor tjekkes mod den rigtige graf, ikke kun mod strengen."""
    from definitions import defs
    from jobs.udredningsret_jobs import udredningsret_ingest_job

    graph = defs.resolve_asset_graph()
    valgt = {k.to_user_string() for k in udredningsret_ingest_job.selection.resolve(graph)}

    assert "fhir_patient_udredningsret" in valgt
    assert "arc_udredningsret_mont" in valgt

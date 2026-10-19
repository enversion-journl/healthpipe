# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Organisation og diagnose læses via nøgler; opslagsdata kopieres ikke.

Skillelinjen er, om oplysningen kan hentes gennem referencen. Organisationens navn
og SOR-kode kan, og må derfor ikke stå som kolonne på proceduren. Kildesystemets egne
afdelings- og organisationskoder kan ikke — de findes hverken i SOR eller SHAK (målt:
nul træffere) — og er derfor kildens egne fakta, ikke en kopi af opslagsdata.
"""

import json
import re
from pathlib import Path

import pytest
import yaml

ETL = Path(__file__).resolve().parents[2]
VIEWS = ETL / "DSA/FHIR/SqlOnFhir/view_definitions"
# Hentes gennem referencen til Organization og må derfor ikke stå på proceduren.
REMOVED = {
    "location_name",
    "location_sorcode",
    "stamafdeling_navn",
    "stamafdeling_sorcode",
    "udfoert_enhed_navn",
    "udfoert_enhed_sorcode",
}
# Kildens egne koder. Findes ikke i noget register, så de kan kun stå her.
KILDEKODER = {
    "location_code",
    "location_orgcode",
    "stamafdeling_kode",
    "stamafdeling_orgkode",
    "udfoert_enhed_kode",
    "udfoert_enhed_orgkode",
}
pytestmark = pytest.mark.unit


def _columns(name):
    view = json.loads((VIEWS / f"{name}_view.json").read_text())
    return {column["name"]: column["path"] for part in view["select"] for column in part["column"]}


def test_procedure_uses_organization_keys_across_view_ddl_and_lineage():
    view = _columns("procedure")
    ddl = (ETL / "infrastructure/ddl/tables/fhir/01_tables.sql").read_text()
    block = ddl.split("CREATE TABLE [fhir].[Procedure]", 1)[1].split("GO", 1)[0]
    lineage = yaml.safe_load((ETL / "data_lineage/fhir/fhir_procedure.yml").read_text())[
        "dso_lineage"
    ]["Columns"]
    for col in REMOVED:
        assert col not in view
        assert col not in lineage
        assert f"[{col}]" not in block
    for col in ("performer_organization_id", "stamafdeling_organization_id"):
        assert col in view and f"[{col}]" in block
        assert lineage[col]["FK-Target"] == "fhir.Organization"
        assert lineage[col]["FK-Key"] == "organization_id"
    # Kildens egne koder skal blive: de kan ikke hentes gennem referencen.
    for col in KILDEKODER:
        assert col in view, f"{col} kan ikke hentes ved opslag og skal projiceres"
        assert f"[{col}]" in block
        assert col in lineage
        assert lineage[col].get("FK-None") is True, f"{col} er ikke en reference"


def test_existing_schema_cleanup_only_drops_the_retired_attributes():
    migration = (
        ETL / "infrastructure/ddl/migrations/20260917_fhir_procedure_organization_joins.sql"
    ).read_text()
    guarded = re.findall(
        r"IF COL_LENGTH\('fhir\.Procedure', '(\w+)'\) IS NOT NULL\s+"
        r"ALTER TABLE \[fhir\]\.\[Procedure\] DROP COLUMN \[(\w+)\];",
        migration,
    )
    # Migrationen fjernede oprindeligt alle tolv; de seks kildekoder er siden
    # foert tilbage af en senere migration, fordi de ikke kan hentes ved opslag.
    assert {before for before, _ in guarded} == REMOVED | KILDEKODER
    senere = (
        ETL / "infrastructure/ddl/migrations/20260918_fhir_procedure_kildens_afdelingskoder.sql"
    ).read_text()
    tilbagefoert = set(re.findall(r"ADD \[(\w+)\]", senere))
    assert tilbagefoert == KILDEKODER
    assert all(before == after for before, after in guarded)
    assert "EXEC sys.sp_refreshview N'[vw_fhir].[Procedure]'" in migration
    assert "DROP TABLE" not in migration


def test_referral_link_projects_keys_and_condition_remains_authoritative():
    assert set(_columns("condition_henvisning")) == {
        "condition_id",
        "source_identifier_value",
        "henvisning_id",
    }
    assert {"code", "code_display", "diagnosetype"} <= _columns("condition").keys()
    assert _columns("servicerequest")["reason_reference_id"] == "reasonReference.id.first()"
    lineage = yaml.safe_load((ETL / "data_lineage/fhir/fhir_condition_henvisning.yml").read_text())[
        "dso_lineage"
    ]["Columns"]
    assert lineage["condition_id"]["FK-Target"] == "fhir.Condition"
    assert lineage["condition_id"]["FK-Key"] == "condition_id"


def test_lineage_consumerview_peger_paa_et_vw_fhir_view():
    """Et ConsumerView i lineage skal findes som vw_fhir-view.

    ConsumerView angiver aftagerviewet, naar det hedder noget andet end tabellen
    (fhir.SFI_datapoint_store laeses gennem vw_fhir.SFI_datapoint). Peger det paa et
    view, der ikke findes, lover lineage noget, aftageren ikke kan naa. En tabel uden
    ConsumerView behoever intet view.
    """
    vw = (ETL / "infrastructure/ddl/views/fhir/01_vw_fhir.sql").read_text(encoding="utf-8")
    views = {n.lower() for n in re.findall(r"CREATE OR ALTER VIEW \[vw_fhir\]\.\[(\w+)\]", vw)}
    erklaerede = set()
    for sti in sorted((ETL / "data_lineage/fhir").glob("*.yml")):
        tekst = sti.read_text(encoding="utf-8")
        view = re.search(r"^\s*ConsumerView:\s*vw_fhir\.(\w+)", tekst, re.M)
        if view:
            erklaerede.add(view.group(1))
    ukendte = sorted(v for v in erklaerede if v.lower() not in views)
    assert not ukendte, f"lineage peger paa ConsumerViews, der ikke findes: {ukendte}"

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Convention test: the ownership table covers patient_view, agrees with lineage, is well-formed."""

import json
from pathlib import Path

import pytest
import yaml
from DSA.FHIR.Patient.field_ownership import (
    ALL_SOURCES,
    BUILDER_PROJECTED_FIELDS,
    FIELD_OWNERSHIP,
    key_for_fhirpath,
    source_for_lineage_system,
)

pytestmark = pytest.mark.unit

ROD = Path(__file__).resolve().parents[2]
PROJEKTION = ROD / "DSA" / "FHIR" / "SqlOnFhir" / "view_definitions" / "patient_view.json"
LINEAGE = ROD / "data_lineage" / "fhir" / "fhir_patient.yml"


def _projektionens_kolonner() -> dict[str, str]:
    view = json.loads(PROJEKTION.read_text(encoding="utf-8"))
    return {k["name"]: k["path"] for sel in view["select"] for k in sel["column"]}


def _lineagens_kolonner() -> dict[str, dict]:
    return yaml.safe_load(LINEAGE.read_text(encoding="utf-8"))["dso_lineage"]["Columns"]


# --------------------------------------------------------------------- 1. coverage


def test_every_column_in_the_projection_has_an_ownership_entry():
    mangler = []
    for kolonne, sti in _projektionens_kolonner().items():
        noegle = key_for_fhirpath(sti)
        if noegle is None:
            continue  # the server's fields: id
        if noegle in BUILDER_PROJECTED_FIELDS:
            continue  # provenance: the builder's own, no source owns it
        if noegle not in FIELD_OWNERSHIP:
            mangler.append(f"{kolonne} ({noegle})")
    assert not mangler, "columns without an entry in the ownership table: " + ", ".join(mangler)


def test_provenance_fields_have_no_source_owner():
    """The builder's own fields must never get an owner — then a contribution could set them."""
    for noegle in BUILDER_PROJECTED_FIELDS:
        assert noegle not in FIELD_OWNERSHIP, f"{noegle} has been given a source owner"


def test_the_provenance_fields_are_projected():
    """Without the columns dim.Borger cannot see where a citizen comes from."""
    kolonner = set(_projektionens_kolonner())
    assert {
        "meta_source",
        "source_cpr",
        "source_ifdb",
        "source_haiba",
        "source_sdsdrg",
        "source_sdsudred",
    } <= kolonner


def test_the_projection_has_no_server_fields_besides_id():
    """Only id may map to None — otherwise a field hides that the table does not see."""
    uden_noegle = [
        k for k, sti in _projektionens_kolonner().items() if key_for_fhirpath(sti) is None
    ]
    assert uden_noegle == ["patient_id"]


# ------------------------------------------------------------ 2. lineage agrees


def _lineage_kilder_pr_noegle() -> dict[str, dict[str, set[str]]]:
    """{table key: {column: sources}} for the lineage columns the table knows."""
    pr_noegle: dict[str, dict[str, set[str]]] = {}
    for kolonne, spec in _lineagens_kolonner().items():
        sti, kilder = spec.get("FhirPath"), spec.get("Source")
        if not sti or not kilder:
            continue
        noegle = key_for_fhirpath(sti)
        if noegle is None or noegle not in FIELD_OWNERSHIP:
            continue
        pr_noegle.setdefault(noegle, {})[kolonne] = {
            source_for_lineage_system(str(k).split(".", 1)[0]) for k in kilder
        }
    return pr_noegle


def test_no_column_claims_a_source_the_table_forbids():
    """A column can read a sub-element (name_text, use='old') and have fewer sources
    than the field — but never more. That is what the Sundhedskort error looked like."""
    afvigelser = []
    for noegle, kolonner in _lineage_kilder_pr_noegle().items():
        owners = set(FIELD_OWNERSHIP[noegle])
        if not owners:
            continue  # no source supplies yet; lineage may point to the planned one
        for kolonne, kilder in kolonner.items():
            for_meget = kilder - owners
            if for_meget:
                afvigelser.append(
                    f"{kolonne}: lineage claims {sorted(for_meget)}, the table allows {sorted(owners)}"
                )
    assert not afvigelser, "\n".join(afvigelser)


def test_every_owner_in_the_table_is_named_by_lineage():
    """If a source is listed as owner, lineage must name it for at least one column on the field.
    Otherwise lineage does not document what the builder actually does."""
    afvigelser = []
    for noegle, kolonner in _lineage_kilder_pr_noegle().items():
        naevnt = set().union(*kolonner.values())
        glemt = set(FIELD_OWNERSHIP[noegle]) - naevnt
        if glemt:
            afvigelser.append(f"{noegle}: the table has {sorted(glemt)}, which lineage never names")
    assert not afvigelser, "\n".join(afvigelser)


def test_lineage_does_not_know_sundhedskort_as_a_source():
    """The CPR register's file extract has no general practitioner or Sundhedskort details."""
    tekst = LINEAGE.read_text(encoding="utf-8")
    kildelinjer = [linje for linje in tekst.splitlines() if "Source:" in linje]
    assert not any("Sundhedskort" in linje for linje in kildelinjer)


# ------------------------------------------------------------- 3. the table's shape


def test_all_owners_are_known_sources():
    for noegle, owners in FIELD_OWNERSHIP.items():
        ukendte = [e for e in owners if e not in ALL_SOURCES]
        assert not ukendte, f"{noegle}: unknown sources {ukendte}"


def test_no_source_is_listed_twice_on_the_same_field():
    for noegle, owners in FIELD_OWNERSHIP.items():
        assert len(owners) == len(set(owners)), f"{noegle}: {owners}"


def test_the_key_forms_are_known():
    for noegle in FIELD_OWNERSHIP:
        if ":" in noegle:
            slags = noegle.split(":", 1)[0]
            assert slags in ("identifier", "extension", "meta.tag"), noegle
        else:
            assert noegle.isidentifier(), noegle


def test_epj_id_and_general_practitioner_are_owned_only_by_epj():
    """The two fields the CPR register never supplies and the other EPJ producers depend on."""
    from utils.fhir.identifier_systems import SYSTEM_IFDB_PATIENT_ID

    assert FIELD_OWNERSHIP[f"identifier:{SYSTEM_IFDB_PATIENT_ID}"] == ("IFDB",)
    assert FIELD_OWNERSHIP["generalPractitioner"] == ("IFDB",)


@pytest.mark.parametrize(
    "sti,forventet",
    [
        ("id", None),
        ("identifier.where(system='urn:dk:cpr').value.first()", "identifier:urn:dk:cpr"),
        (
            "identifier.where(system='urn:dk:cpr' and use='old').value.first()",
            "identifier:urn:dk:cpr",
        ),
        ("deceasedBoolean", "deceased"),
        ("name.where(use='official').given.first()", "name"),
        ("generalPractitioner.display.first()", "generalPractitioner"),
        ("extension('urn:fhir:isDeleted').valueInteger.first()", "extension:urn:fhir:isDeleted"),
        ("meta.tag.where(code='test').exists()", "meta.tag:test"),
        (
            "meta.tag.where(system='http://example.org/fhir/CodeSystem/kilde' "
            "and code='CPR').exists()",
            "meta.tag:CPR",
        ),
        ("meta.source", "meta.source"),
    ],
)
def test_key_for_fhirpath(sti, forventet):
    assert key_for_fhirpath(sti) == forventet

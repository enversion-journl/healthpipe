# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests of the CPR layout table: the contract between parser, checks and archive."""

import pytest
from DSI.csv_files.cpr_layouts import FILMETADATA_RECORDTYPER, LAYOUTS

pytestmark = pytest.mark.unit


# ---------------------------------------------------- integrity of the layout table
#
# A layout that drifts from itself reads fields at wrong positions without failing.


@pytest.mark.parametrize("recordtype", sorted(LAYOUTS))
def test_layout_covers_the_record_length_exactly(recordtype):
    """The last field must end exactly at the documented record length."""
    layout = LAYOUTS[recordtype]
    if layout.recordlaengde is None:
        pytest.skip("the record type has no documented length")
    slut = max(f.position + f.laengde - 1 for f in layout.felter)
    assert (
        slut == layout.recordlaengde
    ), f"{recordtype}: the fields end at {slut}, the record length is {layout.recordlaengde}"


# CPR's documentation of record type 053 has positions that skip or overlap in four places;
# its field positions after 484 are unverified against data.
LAYOUTS_MED_KENDTE_POSITIONSFEJL = frozenset({"053"})


@pytest.mark.parametrize("recordtype", sorted(set(LAYOUTS) - LAYOUTS_MED_KENDTE_POSITIONSFEJL))
def test_fields_neither_overlap_nor_leave_gaps(recordtype):
    felter = sorted(LAYOUTS[recordtype].felter, key=lambda f: f.position)
    assert felter[0].position == 1, f"{recordtype}: the first field does not start at position 1"
    for foregaaende, naeste in zip(felter, felter[1:], strict=False):
        forventet = foregaaende.position + foregaaende.laengde
        assert naeste.position == forventet, (
            f"{recordtype}: {foregaaende.navn} ends before {naeste.position}, "
            f"but {naeste.navn} starts at {naeste.position}"
        )


def test_only_the_known_record_types_have_position_errors():
    """The allowlist must not grow unnoticed."""
    med_fejl = set()
    for recordtype, layout in LAYOUTS.items():
        felter = sorted(layout.felter, key=lambda f: f.position)
        for foregaaende, naeste in zip(felter, felter[1:], strict=False):
            if naeste.position != foregaaende.position + foregaaende.laengde:
                med_fejl.add(recordtype)
    assert med_fejl == LAYOUTS_MED_KENDTE_POSITIONSFEJL


@pytest.mark.parametrize("recordtype", sorted(LAYOUTS))
def test_the_record_type_field_comes_first(recordtype):
    """The parser dispatches on the first three characters; the layout must back that up."""
    foerste = min(LAYOUTS[recordtype].felter, key=lambda f: f.position)
    assert foerste.navn == "RECORDTYPE"
    assert (foerste.position, foerste.laengde) == (1, 3)


@pytest.mark.parametrize("grund,variant", [("002", "002B"), ("023", "023B")])
def test_b_variant_is_a_pure_extension(grund, variant):
    """The variant's shared fields sit at the base variant's positions, so a wrong choice only loses the tail."""
    a = {f.navn: (f.position, f.laengde) for f in LAYOUTS[grund].felter}
    b = {f.navn: (f.position, f.laengde) for f in LAYOUTS[variant].felter}
    assert set(a) < set(b), f"{variant} must contain all fields from {grund}"
    for navn, placering in a.items():
        assert b[navn] == placering, f"{navn} has moved between {grund} and {variant}"
    assert LAYOUTS[variant].recordlaengde > LAYOUTS[grund].recordlaengde


def test_record_lengths_are_unique_per_record_type_code():
    """Two layouts with the same code differ in length, since the length is all that tells them apart."""
    pr_kode: dict[str, set[int]] = {}
    for recordtype, layout in LAYOUTS.items():
        if layout.recordlaengde is None:
            continue
        kode = recordtype.rstrip("B")
        pr_kode.setdefault(kode, set())
        assert (
            layout.recordlaengde not in pr_kode[kode]
        ), f"code {kode}: two variants have length {layout.recordlaengde} — cannot be told apart"
        pr_kode[kode].add(layout.recordlaengde)


def test_file_metadata_record_types_carry_no_personal_number():
    """Start and end records are reproduced verbatim, so they must be free of PNR."""
    for recordtype in FILMETADATA_RECORDTYPER:
        navne = {f.navn for f in LAYOUTS[recordtype].felter}
        assert not any("PNR" in n for n in navne), f"{recordtype} contains a PNR field"

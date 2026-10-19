# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Status code reads alike with and without the EPJ prefix; transform sets active and deceased."""

from unittest.mock import MagicMock

import pytest
from DSA.FHIR.Patient.patient_ifdb_contribution import _transform_patient_rows_to_fhir
from utils.fhir.cpr_status import (
    CPR_STATUS_ACTIVE,
    CPR_STATUS_DECEASED,
    CPR_STATUS_INACTIVE,
    CPR_STATUS_KNOWN,
    CPR_STATUS_MEANING,
    is_active,
    is_deceased,
    normalize_cpr_status,
)

pytestmark = pytest.mark.unit

PRAEFIKS = "PAS_ks.ur.CPRStatus."


def _epj_raekke(**overstyr) -> dict:
    """An EPJ row with the fields the transform reads; the status code in its production form."""
    raekke = {
        "patient_id": "999001",
        "identifier_cpr": "0000000001",
        "name_given": "Test Person",
        "name_family": "Testesen",
        "gender_code": "K",
        "birth_date": "1980-01-01",
        "address_street": "Testvej 1",
        "address_postal": "5000",
        "address_city": "Odense C",
        "address_municipality": "0461",
        "address_protection": "0",
        "person_status_code": PRAEFIKS + "01",
        "doctor_name": None,
        "doctor_number": None,
        "modtidspunkt": "2026-01-02 03:04:05",
        "district_code": None,
        "district_text": None,
        "social_district_code": None,
        "social_district_text": None,
        "is_deleted": "0",
    }
    raekke.update(overstyr)
    return raekke


# ------------------------------------------------------------- normalisation


@pytest.mark.parametrize(
    "vaerdi,forventet",
    [
        (PRAEFIKS + "90", "90"),  # EPJ's form
        ("90", "90"),  # the register's form
        (" 01 ", "01"),
        (1, "01"),  # a number from a source without a leading zero
        (None, None),
        ("", None),
        ("   ", None),
        ("99", None),  # outside CPR's list
        (PRAEFIKS + "99", None),
        ("PAS_ks.ur.CPRStatus.", None),
        ("død", None),
    ],
)
def test_normalisation_strips_prefix_and_rejects_unknown(vaerdi, forventet):
    assert normalize_cpr_status(vaerdi) == forventet


# ----------------------------------------------------------- interpretation


def test_interpretation_follows_cprs_own_labelling():
    """CPR calls 01-07 «aktiv» and 20-90 «inaktiv» — the table and the sets must agree."""
    for kode, betydning in CPR_STATUS_MEANING.items():
        assert betydning.startswith(("aktiv", "inaktiv")), kode
        assert (kode in CPR_STATUS_ACTIVE) == betydning.startswith("aktiv"), kode
    assert CPR_STATUS_DECEASED <= CPR_STATUS_INACTIVE


@pytest.mark.parametrize("kode", sorted(CPR_STATUS_KNOWN))
def test_active_and_deceased_are_the_same_with_and_without_prefix(kode):
    assert is_active(PRAEFIKS + kode) == is_active(kode) == (kode in CPR_STATUS_ACTIVE)
    assert is_deceased(PRAEFIKS + kode) == is_deceased(kode) == (kode == "90")


@pytest.mark.parametrize("vaerdi", [None, "", "99", PRAEFIKS + "99"])
def test_no_or_unknown_code_is_no_information(vaerdi):
    """A patient without a usable code is active and not dead."""
    assert is_active(vaerdi) is True
    assert is_deceased(vaerdi) is False


# ---------------------------------------------------- effect in the transform


def _patient(**overstyr):
    return _transform_patient_rows_to_fhir([_epj_raekke(**overstyr)], MagicMock())[0]


def test_deceased_in_epj_becomes_deceased_and_inactive():
    p = _patient(person_status_code=PRAEFIKS + "90")
    assert p.deceasedBoolean is True
    assert p.active is False


@pytest.mark.parametrize("kode", ["20", "30", "50", "60", "70", "80"])
def test_inactive_in_epj_becomes_inactive_but_not_deceased(kode):
    p = _patient(person_status_code=PRAEFIKS + kode)
    assert p.active is False
    assert p.deceasedBoolean is None  # the transform omits the field when it is false


@pytest.mark.parametrize("kode", ["01", "03", "05", "07"])
def test_active_in_epj_becomes_active(kode):
    p = _patient(person_status_code=PRAEFIKS + kode)
    assert p.active is True
    assert p.deceasedBoolean is None


def test_the_registers_form_without_prefix_reads_the_same():
    """The integration setup and the register's contribution deliver the code without prefix."""
    assert _patient(person_status_code="90").deceasedBoolean is True
    assert _patient(person_status_code="80").active is False


def test_unknown_code_is_counted_and_logged_without_failing_the_run():
    context = MagicMock()
    patients = _transform_patient_rows_to_fhir(
        [
            _epj_raekke(person_status_code=PRAEFIKS + "99"),
            _epj_raekke(patient_id="999002", identifier_cpr="0000000002", person_status_code=None),
        ],
        context,
    )
    assert [p.active for p in patients] == [True, True]
    assert context.log.warning.call_count == 1
    besked = context.log.warning.call_args.args
    assert besked[1] == 1 and besked[2] == [(PRAEFIKS + "99", 1)]


def test_empty_status_code_is_a_missing_code_not_an_unknown_one():
    context = MagicMock()
    patients = _transform_patient_rows_to_fhir([_epj_raekke(person_status_code="  ")], context)
    assert patients[0].active is True
    assert context.log.warning.call_count == 0

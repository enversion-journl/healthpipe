# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""CPR register person status codes (record type 001) and their interpretation in FHIR Patient.

EPJ carries the same codes with a prefix (``PAS_ks.ur.CPRStatus.90``), so the value is normalised
here before it is interpreted. 01-07 are active, 20-90 inactive, and 90 sets ``deceased``. An
unknown code is interpreted as no information; the calling transform counts them.
"""

# CPR's list, with the register's own designation. The first word decides the interpretation.
CPR_STATUS_MEANING: dict[str, str] = {
    "01": "aktiv, bopæl i dansk folkeregister",
    "03": "aktiv, speciel vejkode i dansk folkeregister",
    "05": "aktiv, bopæl i grønlandsk folkeregister",
    "07": "aktiv, speciel vejkode i grønlandsk folkeregister",
    "20": "inaktiv, uden bopæl, administrativt personnummer",
    "30": "inaktiv, annulleret personnummer",
    "50": "inaktiv, slettet personnummer ved dobbeltnummer",
    "60": "inaktiv, ændret personnummer ved ændring af fødselsdato eller køn",
    "70": "inaktiv, forsvundet",
    "80": "inaktiv, udrejst",
    "90": "inaktiv, død",
}

CPR_STATUS_KNOWN = frozenset(CPR_STATUS_MEANING)
CPR_STATUS_ACTIVE = frozenset(k for k, v in CPR_STATUS_MEANING.items() if v.startswith("aktiv"))
CPR_STATUS_INACTIVE = CPR_STATUS_KNOWN - CPR_STATUS_ACTIVE
CPR_STATUS_DECEASED = frozenset({"90"})


def normalize_cpr_status(value: object) -> str | None:
    """The code's two digits, prefixed or not; None for empty values and codes outside CPR's list."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    code = text.rsplit(".", 1)[-1].strip()
    if code.isdigit():
        code = code.zfill(2)
    return code if code in CPR_STATUS_KNOWN else None


def is_deceased(value: object) -> bool:
    """Dead according to the register's status code (90)."""
    return normalize_cpr_status(value) in CPR_STATUS_DECEASED


def is_active(value: object) -> bool:
    """Active unless CPR calls the code inactive. No code or an unknown code counts as active."""
    code = normalize_cpr_status(value)
    return code is None or code in CPR_STATUS_ACTIVE

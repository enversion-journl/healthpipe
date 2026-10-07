# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Code normalization utilities for healthcare codes.

This module provides functions to clean and normalize various healthcare code formats,
particularly SKS (Sundhedsvæsenets Klassifikations System) codes from different sources.

SKS codes may come with various prefixes from source systems that need to be stripped
before validation or translation via the Terminology Server.
"""

import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

# Known SKS prefixes that should be stripped from codes in IFDB
# These prefixes are added by source systems but not part of the actual code
# IMPORTANT: Order matters! More specific prefixes must come before generic ones
SKS_PREFIXES = [
    # Organization/Location prefixes
    "SKS.a.afd",  # Afdeling (department): "SKS.a.afd4202011" → "4202011"
    "SKS.a.sgh",  # Sygehus (hospital): "SKS.a.sgh1234" → "1234"
    # Numeric prefixes (check before generic SKS.a.)
    "SKS.a.00",  # Double-zero prefix
    "SKS.a.0",  # Single-zero prefix
    # Classification system prefixes
    "SKS.a.adm",  # Administrative codes
    "SKS.a.atc",  # ATC (Anatomical Therapeutic Chemical) codes
    "SKS.a.dia",  # Diagnose codes
    "SKS.a.opr",  # Operation/procedure codes
    "SKS.a.pro",  # Procedure codes
    "SKS.a.spc",  # Specialty codes
    "SKS.a.til",  # Tillægskoder (supplementary codes)
    "SKS.a.uly",  # Ulykke (accident) codes
    "SKS.a.und",  # Undersøgelse (examination) codes
    "SKS.a.res",  # Result codes
    # Alternative format with underscore
    "SKS_Vaerdier.a.til",  # value supplementary codes
    # Generic fallback (must be last!)
    "SKS.a.",  # Generic SKS administrative prefix
]


def strip_sks_prefix(code: str | None) -> str | None:
    """
    Strip known SKS prefixes from a code.

    Source systems like IFDB may store codes with SKS classification prefixes
    that need to be removed before validation or translation via the
    Terminology Server.

    Args:
        code: The raw code value, possibly with an SKS prefix

    Returns:
        The code with any known SKS prefix removed, or None if input is None/empty

    Examples:
        >>> strip_sks_prefix("SKS.a.afd4202011")
        '4202011'
        >>> strip_sks_prefix("4202011")
        '4202011'
        >>> strip_sks_prefix("SKS.a.syg123456")
        '123456'
        >>> strip_sks_prefix(None)
        None
    """
    if not code:
        return None

    code = str(code).strip()

    if not code:
        return None

    # Check each known prefix and strip if found
    for prefix in SKS_PREFIXES:
        if code.startswith(prefix):
            stripped = code[len(prefix) :]
            return stripped.strip() if stripped else None

    # No prefix found, return as-is
    return code


def normalize_sks_coding_code(code: str | None) -> str | None:
    """Normalize an SKS ``Coding.code`` value by removing source prefixes."""
    return strip_sks_prefix(code)


_PROCEDURE_RULES_PATH = (
    Path(__file__).resolve().parents[3]
    / "infrastructure"
    / "seed"
    / "ifdb_procedure_code_rules.yml"
)

_DIAGNOSIS_RULES_PATH = (
    Path(__file__).resolve().parents[3]
    / "infrastructure"
    / "seed"
    / "ifdb_diagnosis_code_rules.yml"
)

_DEFAULT_PROCEDURE_CODE_RULES: dict[str, list[str]] = {
    "exact": [],
    # Alle SKS-typekoder danner Procedure; tillaegskodefamilierne undtaget,
    # fordi en tillaegskode ikke er en procedure i sig selv. Skal matche
    # infrastructure/seed/ifdb_procedure_code_rules.yml.
    "prefixes": ["SKS.a."],
    "bare_regex": [],
    "exclude_exact": [],
    "exclude_prefixes": [
        "KPYdelser.",
        "BomKS.",
        "SKS.a.dia",
        "SKS.a.til",
        "SKS_Vaerdier.",
        "SKS.a.atc",
        "SKS.a.afd",
        "SKS.a.sgh",
        "SKS.a.spc",
    ],
}

_DEFAULT_DIAGNOSIS_CODE_RULES: dict[str, list[str]] = {
    "exact": [],
    "prefixes": ["SKS.a.dia"],
    "bare_regex": [],
    "exclude_exact": [],
    "exclude_prefixes": ["KPYdelser.", "BomKS."],
}


def _as_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"Expected list, got {type(value).__name__}")
    return [str(item).strip() for item in value if str(item).strip()]


def _load_ifdb_code_rules(
    *,
    rules_path: Path,
    rules_key: str,
    default_rules: dict[str, list[str]],
) -> dict[str, list[str]]:
    if not rules_path.exists():
        return {key: list(value) for key, value in default_rules.items()}

    with open(rules_path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    raw_rules = data.get(rules_key, data)
    if not isinstance(raw_rules, dict):
        raise ValueError(f"Code rules file must contain a mapping: {rules_path}")

    rules = {key: list(value) for key, value in default_rules.items()}
    for key in rules:
        if key in raw_rules:
            rules[key] = _as_text_list(raw_rules[key])
    return rules


@lru_cache(maxsize=8)
def load_ifdb_procedure_code_rules(
    path: str | Path | None = None,
) -> dict[str, list[str]]:
    """Load IFDB procedure-code classification rules from the repo seed file."""
    rules_path = Path(path) if path is not None else _PROCEDURE_RULES_PATH
    return _load_ifdb_code_rules(
        rules_path=rules_path,
        rules_key="procedure_code_rules",
        default_rules=_DEFAULT_PROCEDURE_CODE_RULES,
    )


@lru_cache(maxsize=8)
def load_ifdb_diagnosis_code_rules(
    path: str | Path | None = None,
) -> dict[str, list[str]]:
    """Load IFDB diagnosis-code classification rules from the repo seed file."""
    rules_path = Path(path) if path is not None else _DIAGNOSIS_RULES_PATH
    return _load_ifdb_code_rules(
        rules_path=rules_path,
        rules_key="diagnosis_code_rules",
        default_rules=_DEFAULT_DIAGNOSIS_CODE_RULES,
    )


def _matches_any_prefix(value: str, prefixes: list[str]) -> bool:
    return any(value.startswith(prefix) for prefix in prefixes)


def _matches_any_regex(value: str, patterns: list[str]) -> bool:
    return any(re.match(pattern, value) for pattern in patterns)


def _matches_ifdb_code_rules(
    code: str | None,
    *,
    rules: dict[str, list[str]],
) -> bool:
    if not code:
        return False

    code_str = str(code).strip()
    if not code_str:
        return False

    bare = strip_sks_prefix(code_str) or code_str

    if code_str in rules["exclude_exact"] or bare in rules["exclude_exact"]:
        return False
    if _matches_any_prefix(code_str, rules["exclude_prefixes"]):
        return False
    if _matches_any_prefix(bare, rules["exclude_prefixes"]):
        return False

    if code_str in rules["exact"] or bare in rules["exact"]:
        return True

    if _matches_any_prefix(code_str, rules["prefixes"]):
        return True
    if _matches_any_regex(bare, rules["bare_regex"]):
        return True

    return False


def is_sks_procedure_code(
    code: str | None,
    *,
    rules_path: str | Path | None = None,
) -> bool:
    """Classify whether an IFDB AKTIVITETTYPE_KODE represents a Procedure.

    Used to determine if an AKTIVITETSRESULTAT row should additionally produce
    a FHIR Procedure resource. The rules are repo-controlled in
    ``infrastructure/seed/ifdb_procedure_code_rules.yml``.

    Default classification rules:
    - ``SKS.a.`` prefix -> activity family (opr/pro/und/adm and unknown families)
    - ``dia`` / ``til`` / ``atc`` / organisation / speciale are excluded
    - RESULTATTYPE is not an axis (neither inclusion nor exclusion)
    - Bare ``K``/``B`` prefixes are not enabled by default because they also
      match IFDB families such as ``KPYdelser`` and ``BomKS``.

    Args:
        code: Raw AKTIVITETTYPE_KODE value, with or without SKS prefix.

    Returns:
        True if the code is a procedure code.

    Examples:
        >>> is_sks_procedure_code("SKS.a.oprKABC10")
        True
        >>> is_sks_procedure_code("SKS.a.proZXY01")
        True
        >>> is_sks_procedure_code("SKS.a.undUXUC80")
        True
        >>> is_sks_procedure_code("SKS.a.admAAF22")
        True
        >>> is_sks_procedure_code("KABC10")
        False
        >>> is_sks_procedure_code("KPYdelser.Afdelingsydelser.X")
        False
        >>> is_sks_procedure_code("SKS.a.diaDA01")
        False
        >>> is_sks_procedure_code("ZZ3170")
        False
        >>> is_sks_procedure_code(None)
        False
    """
    return _matches_ifdb_code_rules(code, rules=load_ifdb_procedure_code_rules(rules_path))


def is_sks_diagnosis_code(
    code: str | None,
    *,
    rules_path: str | Path | None = None,
) -> bool:
    """Classify whether an IFDB AKTIVITETTYPE_KODE represents a diagnosis.

    Used to determine if an AKTIVITETSRESULTAT row should additionally produce
    a FHIR Condition resource. The rules are repo-controlled in
    ``infrastructure/seed/ifdb_diagnosis_code_rules.yml``.

    Default classification rules:
    - ``SKS.a.dia`` prefix -> diagnosis

    Args:
        code: Raw AKTIVITETTYPE_KODE value, with or without SKS prefix.

    Returns:
        True if the code is a diagnosis code.

    Examples:
        >>> is_sks_diagnosis_code("SKS.a.diaDA01")
        True
        >>> is_sks_diagnosis_code("SKS.a.oprKABC10")
        False
        >>> is_sks_diagnosis_code("KPYdelser.Afdelingsydelser.X")
        False
        >>> is_sks_diagnosis_code(None)
        False
    """
    rules = load_ifdb_diagnosis_code_rules(rules_path)
    return _matches_ifdb_code_rules(code, rules=rules)


_XML_DIAGNOSIS_ELEMENT_NAMES = frozenset({"diagnosekode"})

# Bare WHO ICD-10 form: one letter + a digit + rest, e.g. J44, I10, D50, F41.9.
_ICD10_DIAGNOSIS_RE = re.compile(r"^[A-Z][0-9][A-Z0-9.]*$")
# Danish SKS-D diagnosis form: a leading 'D' + ICD-10, e.g. DJ44, DA01, DD50.
_SKS_D_DIAGNOSIS_RE = re.compile(r"^D[A-Z][0-9][A-Z0-9.]*$")


def to_sks_diagnosis_code(code: str | None) -> str | None:
    """Canonicalise a diagnosis code to Danish SKS-D form, or None if it is not a
    recognisable SKS diagnosis code.

    This is the single, codebase-wide rule for emitting SKS diagnosis codes
    (primary codings carry the SKS-D system urn:oid:1.2.208.176.2.4.12): every
    diagnosis ``Condition.code`` must pass through here so the value space is
    uniform. Source data carries the code either
    already in SKS-D form (``DA01``) or as bare WHO ICD-10 (``I10``); SKS diagnosis
    codes are always the ICD-10 code with a leading ``D``. Any SKS prefix is
    stripped first, then:

    - already SKS-D form (``DA01``, ``DD50``) -> kept unchanged
    - bare ICD-10 (``J44``, ``I10``, ``D50``) -> ``D`` prefixed -> ``DJ44``, ``DI10``, ``DD50``
    - anything else (schema indices ``D``/``9``/``Z``, free text) -> ``None``

    Idempotent: ``to_sks_diagnosis_code("J44") == to_sks_diagnosis_code("DJ44") == "DJ44"``.

    Examples:
        >>> to_sks_diagnosis_code("J44")
        'DJ44'
        >>> to_sks_diagnosis_code("I10")
        'DI10'
        >>> to_sks_diagnosis_code("D50")
        'DD50'
        >>> to_sks_diagnosis_code("DA01")
        'DA01'
        >>> to_sks_diagnosis_code("SKS.a.diaI10")
        'DI10'
        >>> to_sks_diagnosis_code("D")
        >>> to_sks_diagnosis_code(None)
    """
    if code is None:
        return None
    bare = strip_sks_prefix(str(code).strip())
    if not bare:
        return None
    bare = bare.strip().upper()
    if _SKS_D_DIAGNOSIS_RE.match(bare):
        return bare
    if _ICD10_DIAGNOSIS_RE.match(bare):
        return "D" + bare
    return None


def is_xml_diagnosis_element_name(element_navn: str | None) -> bool:
    """Return True when an XML element name represents a structured diagnosis code field."""
    if not element_navn:
        return False
    return element_navn.strip().lower() in _XML_DIAGNOSIS_ELEMENT_NAMES


def _diagnosis_candidate_from_original(original: str | None) -> str | None:
    """The ``element_original_kode`` value, when its SKS family is diagnoses.

    A prefixed code from another family must not be considered: an operation
    code shaped letter+digit (``SKS.a.oprD1234``) would otherwise canonicalise
    into a diagnosis that does not exist. Unprefixed values are passed through
    and validated by :func:`to_sks_diagnosis_code` on shape alone.
    """
    if not original:
        return None
    text = str(original).strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered.startswith("sks.") and not lowered.startswith("sks.a.dia"):
        return None
    return text


def extract_xml_clinical_diagnosis_code(
    *,
    element_navn: str | None,
    element_kode: str | None,
    element_vaerdi: str | None,
    element_original_kode: str | None = None,
) -> str | None:
    """Extract an SKS diagnosis code from a parsed XML Element row, if present.

    Returns the code in canonical SKS-D form (see :func:`to_sks_diagnosis_code`),
    so the XML ``DiagnoseKode`` path is SKS-gated identically to the
    ``AKTIVITETTYPE_KODE`` path. Values that are not recognisable SKS diagnosis
    codes yield ``None`` and therefore emit no Condition.

    ``element_original_kode`` is tried first, because that is the field IFDB's
    SFI schemas put the finished code in -- the same field the operation path
    reads. A structured ``Diagnosekode`` element carries the SKS code there
    (``SKS.a.diaDC509``), the trailing digit in ``element_kode`` (``9``) and the
    Danish diagnosis text in ``element_vaerdi`` (``Brystkraeft UNS``), so
    reading only the latter two yields no code at all.
    """
    if not is_xml_diagnosis_element_name(element_navn):
        return None
    for candidate in (
        _diagnosis_candidate_from_original(element_original_kode),
        element_kode,
        element_vaerdi,
    ):
        if not candidate:
            continue
        sks_code = to_sks_diagnosis_code(candidate)
        if sks_code:
            return sks_code
    return None


def split_aktivitettype_kode(kode: str | None) -> list[str | None]:
    """Split AKTIVITETTYPE_KODE by '.' into up to 8 hierarchy levels.

    Matches Example Region DWH LEVEL1-8 logic from Extract_BEHANDL_AKTIVITETSRESULTAT.
    Returns a list of exactly 8 elements, padded with None.

    Examples:
        >>> split_aktivitettype_kode("DI10.1.2")
        ['DI10', '1', '2', None, None, None, None, None]
        >>> split_aktivitettype_kode("ZZ3170")
        ['ZZ3170', None, None, None, None, None, None, None]
        >>> split_aktivitettype_kode(None)
        [None, None, None, None, None, None, None, None]
    """
    if not kode:
        return [None] * 8
    parts = str(kode).split(".")
    return [parts[i] if i < len(parts) else None for i in range(8)]


def normalize_shak_code(code: str | None) -> str | None:
    """
    Normalize a SHAK code by stripping SKS prefixes and cleaning whitespace.

    This is the main entry point for SHAK code normalization.
    Future enhancements (e.g., padding, format validation) can be added here.

    Args:
        code: The raw SHAK code value

    Returns:
        The normalized SHAK code, or None if input is None/empty/invalid

    Examples:
        >>> normalize_shak_code("SKS.a.afd4202011")
        '4202011'
        >>> normalize_shak_code("  4202011  ")
        '4202011'
        >>> normalize_shak_code("")
        None
    """
    return strip_sks_prefix(code)


def shak_identifier_value(code: str | None) -> str | None:
    """Normalize a SHAK code to a FHIR identifier value (system ``urn:dk:shak``).

    Produces the value written onto a FHIR resource: strips the known SKS
    classification prefixes via ``strip_sks_prefix`` (including the ``SKS.a.00`` /
    ``SKS.a.0`` numeric prefixes), then drops all separators (dots, commas,
    hyphens, whitespace) and upper-cases. Alphanumerics are kept, so an alphabetic
    afsnit suffix is preserved (e.g. ``420243F``). No padding or truncation.
    Returns None when nothing remains.

    Verified against the SHAK master register: sgh codes are 4 digits, afd codes
    6-7 digits (optionally + trailing letter); no code starts with ``0`` — so the
    ``SKS.a.00`` prefix is a real prefix, e.g. ``SKS.a.00500810`` -> ``500810``
    (a valid afd), not ``00500810`` (which does not exist).

    Examples:
        >>> shak_identifier_value("6620.010")
        '6620010'
        >>> shak_identifier_value("SKS.a.afd5000108")
        '5000108'
        >>> shak_identifier_value("SKS.a.00500810")
        '500810'
        >>> shak_identifier_value("SKS.a.sgh6013")
        '6013'
        >>> shak_identifier_value("SKS.a.afd420243f")
        '420243F'
        >>> shak_identifier_value("")
        None
    """
    cleaned = re.sub(r"[^0-9A-Za-z]", "", strip_sks_prefix(code) or "")
    return cleaned.upper() or None


def normalize_sor_code(code: str | None) -> str | None:
    """
    Normalize a SOR code by cleaning whitespace and validating format.

    SOR codes should be numeric strings (typically 15-18 digits).

    Args:
        code: The raw SOR code value

    Returns:
        The normalized SOR code, or None if input is None/empty

    Examples:
        >>> normalize_sor_code("  215811000016008  ")
        '215811000016008'
        >>> normalize_sor_code("")
        None
    """
    if not code:
        return None

    code = str(code).strip()

    if not code:
        return None

    return code


def extract_shak_parent_from_suffix(code: str | None) -> str | None:
    """
    Extract numeric parent from a SHAK code that has a trailing alphabetic suffix.

    Some SHAK codes have suffixes like "1234A", "600835S", "600835K". In these cases,
    if the code is not found in the terminology server, the parent should be the
    numeric portion (e.g., "1234", "600835").

    This function checks if a code ends with one or more letters (after digits),
    and if so, returns the numeric prefix as the parent.

    Args:
        code: The SHAK code to analyze (e.g., "600835S", "1234A", "600831A")

    Returns:
        The numeric parent code if pattern matches, or None otherwise

    Examples:
        >>> extract_shak_parent_from_suffix("1234A")
        '1234'
        >>> extract_shak_parent_from_suffix("600835S")
        '600835'
        >>> extract_shak_parent_from_suffix("600835K")
        '600835'
        >>> extract_shak_parent_from_suffix("4202011")
        None  # No suffix, returns None
        >>> extract_shak_parent_from_suffix("ABCD")
        None  # No numeric prefix
        >>> extract_shak_parent_from_suffix(None)
        None
    """
    if not code:
        return None

    code = str(code).strip()

    if not code:
        return None

    # Pattern: one or more digits followed by one or more letters at the end
    # Examples: "1234A" → "1234", "600835SK" → "600835"
    match = re.match(r"^(\d+)[A-Za-z]+$", code)

    if match:
        return match.group(1)

    return None

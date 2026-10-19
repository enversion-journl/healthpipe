# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Every RSD extension URL a ViewDefinition reads must be one we write.

FHIRPath returns empty for an unknown URL, so a typo is silent at runtime.
"""

import json
import re
from pathlib import Path

import pytest
from utils.fhir import fhir_extensions, fhir_extensions_planned

pytestmark = pytest.mark.unit

_VIEW_DEFINITIONS_DIR = (
    Path(__file__).resolve().parents[2] / "DSA" / "FHIR" / "SqlOnFhir" / "view_definitions"
)
_LOCAL_URL = re.compile(r"http://example\.org/fhir/StructureDefinition/[A-Za-z0-9._-]+")

# URLs a ViewDefinition reads on purpose even though no transform writes them yet.
# Adding an entry here is a deliberate act: it records "the column is expected to
# be NULL until someone starts producing this extension". Removing a URL from a
# transform without removing it here would go unnoticed, so keep the reason
# specific enough that the next person can tell whether it is still true.
_NOT_PRODUCED_YET: dict[str, str] = {
    "http://example.org/fhir/StructureDefinition/discretionFlag": (
        "OENSKER_DISKRETION is mapped in PATIENT_COLUMN_MAPPING but patient_ifdb_contribution.py "
        "does not build the extension. Owned by IFDB in DSA/FHIR/Patient/field_ownership.py."
    ),
}


def _written_urls() -> set[str]:
    """Every RSD StructureDefinition URL exported as a constant we post."""
    return {
        value
        for name, value in vars(fhir_extensions).items()
        if name.startswith("EXTENSION_") and isinstance(value, str) and _LOCAL_URL.fullmatch(value)
    }


def _read_urls() -> dict[str, list[tuple[str, str]]]:
    """RSD URLs referenced by ViewDefinition paths -> [(file, column), ...]."""
    found: dict[str, list[tuple[str, str]]] = {}
    for path in sorted(_VIEW_DEFINITIONS_DIR.glob("*.json")):
        definition = json.loads(path.read_text(encoding="utf-8"))
        for select in definition.get("select", []):
            for column in select.get("column", []):
                for url in _LOCAL_URL.findall(column.get("path", "")):
                    found.setdefault(url, []).append((path.name, column.get("name", "?")))
    return found


def test_view_definitions_only_read_urls_we_write():
    """A ViewDefinition reading a URL no transform writes yields silent NULLs."""
    written = _written_urls()
    unknown = {
        url: refs
        for url, refs in _read_urls().items()
        if url not in written and url not in _NOT_PRODUCED_YET
    }

    assert not unknown, (
        "ViewDefinition(s) read RSD extension URL(s) that no EXTENSION_* constant writes. "
        "FHIRPath returns empty for an unknown URL, so these columns would be silently "
        "NULL forever. Fix the path, or add the URL to _NOT_PRODUCED_YET with a reason:\n"
        + "\n".join(f"  {url} <- {refs}" for url, refs in sorted(unknown.items()))
    )


def test_address_protection_url_matches_the_transform():
    """Pin the one that caused the incident: consumers rely on this flag to
    suppress address-protected citizens, and a mismatch fails open, not closed."""
    refs = _read_urls().get(fhir_extensions.EXTENSION_ADDRESS_PROTECTION, [])
    assert ("patient_view.json", "address_protection") in refs, (
        "patient_view.address_protection must read "
        f"{fhir_extensions.EXTENSION_ADDRESS_PROTECTION!r} -- the URL transforms.py writes."
    )


def test_not_produced_yet_entries_are_still_unproduced():
    """Stop the allowlist from going stale: once a transform starts writing one
    of these, the entry must go, or the guard silently stops guarding it."""
    written = _written_urls()
    stale = sorted(url for url in _NOT_PRODUCED_YET if url in written)
    assert not stale, (
        "These URLs are now written by an EXTENSION_* constant and must be removed "
        f"from _NOT_PRODUCED_YET: {stale}"
    )


def _planned_rsd_urls() -> set[str]:
    """RSD URLs declared as *planned* -- decided and owned, but written by no transform yet."""
    return {
        value
        for name, value in vars(fhir_extensions_planned).items()
        if name.startswith("EXTENSION_") and isinstance(value, str) and _LOCAL_URL.fullmatch(value)
    }


def test_planned_constants_are_all_in_the_allowlist():
    """A planned constant is a promise, not production. Every RSD URL in
    fhir_extensions_planned must still be listed as not produced -- the day it moves
    to fhir_extensions, test_not_produced_yet_entries_are_still_unproduced forces the
    allowlist entry out. Together the two keep 'planned' from silently becoming
    'believed produced'."""
    missing = sorted(_planned_rsd_urls() - set(_NOT_PRODUCED_YET))
    assert not missing, (
        "Planned extension URL(s) are not in _NOT_PRODUCED_YET; either add them with a "
        f"reason, or -- if a transform now writes them -- move them to fhir_extensions: {missing}"
    )


def test_planned_and_written_are_disjoint():
    """The same URL cannot be both 'we post this' and 'nobody posts this yet'."""
    both = sorted(_planned_rsd_urls() & _written_urls())
    assert not both, f"URL(s) declared in both fhir_extensions and fhir_extensions_planned: {both}"

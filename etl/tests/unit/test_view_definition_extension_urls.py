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
from utils.fhir import fhir_extensions

pytestmark = pytest.mark.unit

_VIEW_DEFINITIONS_DIR = (
    Path(__file__).resolve().parents[2] / "DSA" / "FHIR" / "SqlOnFhir" / "view_definitions"
)
_RSD_URL = re.compile(r"http://regionsyddanmark\.dk/fhir/StructureDefinition/[A-Za-z0-9._-]+")

# URLs a ViewDefinition reads on purpose even though no transform writes them yet.
# Adding an entry here is a deliberate act: it records "the column is expected to
# be NULL until someone starts producing this extension". Removing a URL from a
# transform without removing it here would go unnoticed, so keep the reason
# specific enough that the next person can tell whether it is still true.
_NOT_PRODUCED_YET: dict[str, str] = {
    "http://regionsyddanmark.dk/fhir/StructureDefinition/motherIdentifier": (
        "No source column in PATIENT_COLUMN_MAPPING; mother CPR is not delivered by IFDB."
    ),
    "http://regionsyddanmark.dk/fhir/StructureDefinition/fatherIdentifier": (
        "No source column in PATIENT_COLUMN_MAPPING; father CPR is not delivered by IFDB."
    ),
    "http://regionsyddanmark.dk/fhir/StructureDefinition/discretionFlag": (
        "OENSKER_DISKRETION is mapped in PATIENT_COLUMN_MAPPING but transforms.py "
        "does not build the extension."
    ),
    "http://regionsyddanmark.dk/fhir/StructureDefinition/addressKvhx": (
        "No source column for KVHX; transforms.py does not build the extension."
    ),
}


def _written_urls() -> set[str]:
    """Every RSD StructureDefinition URL exported as a constant we post."""
    return {
        value
        for name, value in vars(fhir_extensions).items()
        if name.startswith("EXTENSION_") and isinstance(value, str) and _RSD_URL.fullmatch(value)
    }


def _read_urls() -> dict[str, list[tuple[str, str]]]:
    """RSD URLs referenced by ViewDefinition paths -> [(file, column), ...]."""
    found: dict[str, list[tuple[str, str]]] = {}
    for path in sorted(_VIEW_DEFINITIONS_DIR.glob("*.json")):
        definition = json.loads(path.read_text(encoding="utf-8"))
        for select in definition.get("select", []):
            for column in select.get("column", []):
                for url in _RSD_URL.findall(column.get("path", "")):
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

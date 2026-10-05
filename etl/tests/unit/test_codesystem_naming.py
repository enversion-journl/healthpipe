# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The codesystem table-name helper builds PascalCase names (acronyms UPPER)."""

import pytest
from DSA.CodeSystem.utils import CODESYSTEM_PASCAL, codesystem_table_name

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "name,kind,expected",
    [
        ("sks", "concept", "SKSConcept"),
        ("sks", "properties", "SKSProperties"),
        ("sks", "hierarchy", "SKSHierarchy"),
        ("sor", "concept", "SORConcept"),
        ("sfi", "properties", "SFIProperties"),
        ("ean", "hierarchy", "EANHierarchy"),
        ("rekvirent", "concept", "RekvirentConcept"),
        ("lager", "properties", "LagerProperties"),
        ("okosted", "hierarchy", "OkostedHierarchy"),
        ("shak", "concept", "SHAKConcept"),
        ("shak", "properties", "SHAKProperties"),
        ("shak", "hierarchy", "SHAKHierarchy"),
    ],
)
def test_codesystem_table_name(name, kind, expected):
    assert codesystem_table_name(name, kind) == expected


def test_map_covers_all_codesystems():
    assert set(CODESYSTEM_PASCAL) == {
        "sks",
        "sor",
        "sfi",
        "ean",
        "shak",
        "rekvirent",
        "lager",
        "okosted",
    }


def test_unknown_name_raises():
    with pytest.raises(KeyError):
        codesystem_table_name("bogus", "concept")


def test_unknown_kind_raises():
    with pytest.raises(ValueError):
        codesystem_table_name("sks", "bogus")

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Tests that lineage YAML files:
  1. Do not contain any retired %name placeholder patterns.
  2. Every system='...' URI matches a constant value in identifier_systems.py.
"""

import re
from pathlib import Path

import pytest
from utils.fhir import identifier_systems

LINEAGE_ROOT = Path(__file__).resolve().parents[2] / "data_lineage"
YAML_GLOB = "**/*.yml"

PERCENT_PLACEHOLDER = re.compile(r"%[a-z_]+")
SYSTEM_URI = re.compile(r"system='([^']+)'")


def _all_lineage_ymls() -> list[Path]:
    return sorted(LINEAGE_ROOT.glob(YAML_GLOB))


def _all_identifier_uris() -> set[str]:
    """Collect every string constant value from identifier_systems.py."""
    uris: set[str] = set()
    for name in dir(identifier_systems):
        if name.startswith("SYSTEM_"):
            val = getattr(identifier_systems, name)
            if isinstance(val, str):
                uris.add(val)
    return uris


class TestNoPercentPlaceholders:
    """Retired %name convention must not appear in lineage YAML."""

    @pytest.mark.parametrize(
        "yml_path", _all_lineage_ymls(), ids=lambda p: str(p.relative_to(LINEAGE_ROOT))
    )
    def test_no_percent_placeholders(self, yml_path: Path):
        content = yml_path.read_text(encoding="utf-8")
        matches = PERCENT_PLACEHOLDER.findall(content)
        assert not matches, (
            f"{yml_path.relative_to(LINEAGE_ROOT)} still contains retired "
            f"%name placeholder(s): {matches}"
        )


class TestSystemUrisMatchIdentifierSystems:
    """Every system='...' URI in lineage YAML must exist in identifier_systems.py."""

    @pytest.mark.parametrize(
        "yml_path", _all_lineage_ymls(), ids=lambda p: str(p.relative_to(LINEAGE_ROOT))
    )
    def test_system_uris_exist(self, yml_path: Path):
        content = yml_path.read_text(encoding="utf-8")
        uris_in_file = set(SYSTEM_URI.findall(content))
        if not uris_in_file:
            return

        known_uris = _all_identifier_uris()
        unknown = uris_in_file - known_uris
        assert not unknown, (
            f"{yml_path.relative_to(LINEAGE_ROOT)} references unknown identifier "
            f"system URI(s) not in identifier_systems.py: {unknown}"
        )


class TestAtLeastOneLineageFileExists:
    """Sanity: the glob finds at least one YAML file."""

    def test_found_ymls(self):
        ymls = _all_lineage_ymls()
        assert len(ymls) > 0, f"No .yml files found under {LINEAGE_ROOT}"

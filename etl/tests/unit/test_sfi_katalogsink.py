# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Katalog-sinken holder distinkte koncepter, ikke forekomster.

Fejl disse tests fanger:
- sinken vokser med antal datapunkter i stedet for antal distinkte koncepter
  (ubegrænset hukommelse over en hel partition -> barneprocessen dræbes)
- en dedupe der kollapser ægte varianter og skjuler katalogkonflikter
"""

from __future__ import annotations

import pytest
from DSA.CodeSystem.sfi.catalog import (
    SfiElement,
    add_sfi_catalog_element,
    build_sfi_codesystem,
)

pytestmark = pytest.mark.unit


def _elem(**overrides: str) -> SfiElement:
    felter = dict(
        code="SKEMA.Puls",
        display="Puls",
        datatype="integer",
        unit="bpm",
        sfi_path="Vitale/Puls",
        source_element_id="7",
        source_element_name="Puls",
        source_model_name="SKEMA",
        source_model_version="3",
        status="active",
        parent_code="SKEMA.Vitale",
    )
    felter.update(overrides)
    return SfiElement(**felter)


def test_identiske_forekomster_kollapser_til_een():
    sink: dict[str, list[SfiElement]] = {}
    for _ in range(10_000):
        add_sfi_catalog_element(sink, _elem())
    assert len(sink["SKEMA.Puls"]) == 1


def test_aegte_varianter_bevares_og_konflikten_er_synlig():
    sink: dict[str, list[SfiElement]] = {}
    add_sfi_catalog_element(sink, _elem())
    add_sfi_catalog_element(sink, _elem(unit="/min"))
    add_sfi_catalog_element(sink, _elem())
    assert len(sink["SKEMA.Puls"]) == 2
    _, properties, _ = build_sfi_codesystem(sink)
    konflikt = [p for p in properties if p.get("KEY") == "conflict_fields" and p.get("VALUE")]
    assert konflikt, "enhedskonflikten skal stadig markeres efter dedupe"


def test_kataloget_er_ens_med_og_uden_dubletter():
    med: dict[str, list[SfiElement]] = {}
    uden: dict[str, list[SfiElement]] = {}
    varianter = [_elem(), _elem(source_model_version="4", display="Puls (slag/min)")]
    for v in varianter:
        for _ in range(50):
            med.setdefault(v.code, []).append(v)
        add_sfi_catalog_element(uden, v)
    assert build_sfi_codesystem(med) == build_sfi_codesystem(uden)

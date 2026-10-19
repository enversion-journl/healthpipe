# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Specialerne skal ende paa de Observations, SFI-stien rent faktisk bygger.

Opslagene mod ``arc.ifdb_aktivitetsudfoerelse[_speciale]`` hentes én gang pr.
chunk i orkestratoren. Sammensaetningen af extensions stod laenge kun i
fallback-stien, mens SFI-byggerne — dem der faktisk koerer — aldrig fik
opslagene. Resultatet var, at specialerne blev hentet for hver eneste chunk og
derefter kastet vaek: ingen Observation bar extensionen.

Testene her holder begge SFI-byggere fast paa, at de tager imod opslagene og
laegger resultatet paa hver Observation, de udsender.
"""

from __future__ import annotations

import inspect
from typing import Any
from unittest.mock import MagicMock

import pytest
from DSA.FHIR.Aktivitetsresultat.activity_metadata import (
    apply_aktivitetsudfoerelse_extensions,
    build_aktivitetsudfoerelse_extensions,
)
from DSA.FHIR.Observation import observation_sfi_builder as sfi
from DSA.FHIR.utils.ifdb.routing import TargetResource
from DSA.FHIR.utils.ifdb.routing_fallback import RoutingDecision
from utils.fhir.fhir_extensions import (
    EXTENSION_AKTIVITETSUDFOERELSE_SPECIALE,
    EXTENSION_AKTIVITETTYPE_KODE_HIERARKI,
)

UDF_ID = "4711"


def _raekke() -> dict[str, Any]:
    return {
        "OBJEKT_ID": 42,
        "UDFOERELSE_ID": UDF_ID,
        "AKTIVITETTYPE_KODE": "Maalinger.Blodtryk",
        "PERIOD_START": "2026-06-01T15:06:00",
    }


def _speciale_opslag() -> dict[str, list[dict[str, Any]]]:
    return {
        UDF_ID: [
            {
                "AKTIVITETSUDFOERELSE_ID": UDF_ID,
                "SPECIALE_KODE": "0901",
                "SPECIALE_KODETEKST": "Intern medicin",
                "SPECIALE_HOSPITAL_KODE": "6600",
                "SPECIALE_HOSPITAL_NAVN": "Sygehus Lillebaelt",
                "SPECIALE_HOSPITAL_ORGKODE": "6600A",
            }
        ]
    }


def _urler(observation: dict) -> list[str]:
    return [e.get("url") for e in observation.get("extension", [])]


def _stub_byggeren(monkeypatch: pytest.MonkeyPatch, antal: int = 1) -> None:
    """Lad per-raekke-byggeren udsende kendte Observations uden XML."""

    def _byg(**_kwargs: Any) -> list[tuple[str, dict]]:
        return [
            (
                f"obs-{i}",
                {"resourceType": "Observation", "extension": [{"url": "urn:eksisterende"}]},
            )
            for i in range(antal)
        ]

    monkeypatch.setattr(sfi, "_build_sfi_observations_for_row", _byg)
    monkeypatch.setattr(
        sfi,
        "_resolve_row_target_resource",
        lambda *a, **k: RoutingDecision(
            target_resource=TargetResource.OBSERVATION,
            observation_category=None,
            routing_source="test",
            routing_confidence="high",
            routing_reason="test",
        ),
    )
    for validator in (
        "_validate_observation_dicts_sample",
        "_validate_procedure_dicts_sample",
        "_validate_condition_dicts_sample",
    ):
        monkeypatch.setattr(sfi, validator, lambda *a, **k: None)


class TestSammensaetningen:
    def test_specialer_og_hierarki_samles_for_raekken(self) -> None:
        extensions = build_aktivitetsudfoerelse_extensions(
            _raekke(), {UDF_ID: {"AKTIVITETSUDFOERELSE_ID": UDF_ID}}, _speciale_opslag()
        )
        urler = [e["url"] for e in extensions]
        assert EXTENSION_AKTIVITETSUDFOERELSE_SPECIALE in urler
        assert EXTENSION_AKTIVITETTYPE_KODE_HIERARKI in urler

    def test_uden_opslag_gaar_hierarkiet_stadig_med(self) -> None:
        # Hierarkiet afhaenger kun af koden, saa det maa ikke falde bort,
        # naar opslagene mangler.
        urler = [e["url"] for e in build_aktivitetsudfoerelse_extensions(_raekke(), None, None)]
        assert urler == [EXTENSION_AKTIVITETTYPE_KODE_HIERARKI]

    def test_eksisterende_extensions_bevares(self) -> None:
        observation = {"extension": [{"url": "urn:eksisterende"}]}
        apply_aktivitetsudfoerelse_extensions(observation, [{"url": "urn:ny"}])
        assert _urler(observation) == ["urn:eksisterende", "urn:ny"]

    def test_tom_liste_roerer_ikke_observationen(self) -> None:
        observation = {"resourceType": "Observation"}
        apply_aktivitetsudfoerelse_extensions(observation, [])
        assert "extension" not in observation


class TestBeggeSfiByggereLaeggerDetPaa:
    def test_den_enkle_bygger(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _stub_byggeren(monkeypatch, antal=2)

        observationer, _, _, fejl = sfi._build_sfi_observation_dicts_for_rows(
            rows=[_raekke()],
            parsed_data_by_index=[None],
            logger=MagicMock(),
            resolved=None,
            routing_resolver=MagicMock(),
            udf_lookup={UDF_ID: {"AKTIVITETSUDFOERELSE_ID": UDF_ID}},
            speciale_lookup=_speciale_opslag(),
        )

        assert fejl == 0
        assert len(observationer) == 2
        # Hver eneste Observation fra raekken skal baere specialet — ikke kun den foerste.
        for _, observation in observationer:
            assert EXTENSION_AKTIVITETSUDFOERELSE_SPECIALE in _urler(observation)
            assert "urn:eksisterende" in _urler(observation)

    def test_den_samlede_bygger(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _stub_byggeren(monkeypatch)

        observationer, _, _, _, fejl = sfi._build_sfi_unified_dicts_for_rows(
            rows=[_raekke()],
            parsed_data_by_index=[None],
            kontekst_by_index=[None],
            logger=MagicMock(),
            resolved=None,
            routing_resolver=MagicMock(),
            udf_lookup={UDF_ID: {"AKTIVITETSUDFOERELSE_ID": UDF_ID}},
            speciale_lookup=_speciale_opslag(),
        )

        assert fejl == 0
        assert observationer
        for _, observation in observationer:
            assert EXTENSION_AKTIVITETSUDFOERELSE_SPECIALE in _urler(observation)


@pytest.mark.parametrize(
    "bygger",
    [sfi._build_sfi_observation_dicts_for_rows, sfi._build_sfi_unified_dicts_for_rows],
)
def test_byggerne_tager_imod_opslagene(bygger) -> None:
    """Vaernet mod at opslagene igen bliver droppet paa vejen ind."""
    parametre = inspect.signature(bygger).parameters
    assert "udf_lookup" in parametre
    assert "speciale_lookup" in parametre

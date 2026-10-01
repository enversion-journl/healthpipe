# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for parallel XML parsing in aktivitetsresultat asset transform.

These tests verify two critical guarantees introduced by parallel parsing:

1) Deterministic output:
   Parallel parsing must produce exactly the same parsed component payloads
   in the same row order as sequential parsing.

2) Behavioral parity:
   The full Observation transform output must remain unchanged when switching
   between sequential and parallel XML parsing modes.
"""

from __future__ import annotations

import dagster as dg
import pytest
from core.parallel import ParallelPolicy, ParallelStage
from DSA.FHIR.Aktivitetsresultat import orchestrator as akt_orchestrator
from DSA.FHIR.Aktivitetsresultat import routing_glue as akt_routing
from DSA.FHIR.utils.ifdb import xml_parsing_workers as akt_xml
from DSA.FHIR.utils.ifdb.aktivitetsresultat_config import SFI_MODE_LEGACY
from tests.validation.test_xml_parser_massive_diff import _build_massive_cases

pytestmark = pytest.mark.unit


def _build_asset_rows(num_cases: int = 1200) -> list[dict]:
    """Build synthetic AKTIVITETSRESULTAT-like rows for asset-level testing."""
    cases = _build_massive_cases(num_cases)
    rows: list[dict] = []
    for tc in cases:
        aktivitets_id = tc["aktivitets_id"]
        rows.append(
            {
                "OBJEKT_ID": aktivitets_id,
                "PATIENT_ID": aktivitets_id + 100000,
                "ENCOUNTER_ID": aktivitets_id + 200000,
                "EPISODE_OF_CARE_ID": aktivitets_id + 300000,
                "AKTIVITETTYPE_KODE": "A01",
                "AKTIVITETTYPE_KODETEKST": "Aktivitet",
                "RESULTATTYPE_KODE": "R01",
                "RESULTATTYPE_KODETEKST": "Resultat",
                "OVERSKRIFT": f"OBS_{aktivitets_id}",
                "XML": tc["xml"],
                "KLADDE": 0,
                "PERIOD_START": "2024-01-15T10:30:00",
                "SLETTET": 0,
            }
        )
    return rows


def _stage_with_workers(
    stage: ParallelStage,
    *,
    max_workers: int,
    threshold: int = 1,
    chunksize: int = 100,
) -> ParallelStage:
    """Return a test copy of a stage with deterministic worker settings."""
    return ParallelStage(
        stage_id=stage.stage_id,
        policy=ParallelPolicy(
            max_workers=max_workers,
            parallel_threshold=threshold,
            chunksize=chunksize,
            executor_kind=stage.policy.executor_kind,
        ),
        max_logged_errors=stage.max_logged_errors,
        mp_context=stage.mp_context,
        before_parallel_start=stage.before_parallel_start,
    )


@pytest.mark.slow
def test_parallel_parse_lossless_is_deterministic(monkeypatch):
    """Parallel lossless parse helper must match sequential output exactly."""
    rows = _build_asset_rows(1500)
    logger = dg.get_dagster_logger()

    monkeypatch.setattr(
        akt_xml,
        "_XML_LOSSLESS_PARSE_STAGE",
        _stage_with_workers(akt_xml._XML_LOSSLESS_PARSE_STAGE, max_workers=1),
    )
    seq_parsed, seq_kontekst, seq_errors = akt_xml._parse_xml_lossless_for_rows(rows, logger)

    monkeypatch.setattr(
        akt_xml,
        "_XML_LOSSLESS_PARSE_STAGE",
        _stage_with_workers(akt_xml._XML_LOSSLESS_PARSE_STAGE, max_workers=4),
    )
    par_parsed, par_kontekst, par_errors = akt_xml._parse_xml_lossless_for_rows(rows, logger)

    assert par_errors == seq_errors
    assert len(par_parsed) == len(seq_parsed)
    for i in range(len(seq_parsed)):
        assert par_parsed[i] == seq_parsed[i], f"Mismatch at index {i}"
    for i in range(len(seq_kontekst)):
        assert par_kontekst[i] == seq_kontekst[i], f"Kontekst mismatch at index {i}"


@pytest.mark.slow
def test_transform_lossless_output_stable_parallel_vs_sequential(monkeypatch):
    """Observation ordering and component cardinality must remain identical in lossless mode."""
    rows = _build_asset_rows(1500)

    monkeypatch.setattr(akt_orchestrator, "_FAST_MODE", False)
    monkeypatch.setattr(akt_routing, "_SFI_MODE", SFI_MODE_LEGACY)
    monkeypatch.setattr(
        akt_xml,
        "_XML_LOSSLESS_PARSE_STAGE",
        _stage_with_workers(akt_xml._XML_LOSSLESS_PARSE_STAGE, max_workers=1),
    )
    seq_obs, _, _ = akt_orchestrator._transform_aktivitetsresultat_to_fhir_dicts(rows, context=None)

    monkeypatch.setattr(
        akt_xml,
        "_XML_LOSSLESS_PARSE_STAGE",
        _stage_with_workers(akt_xml._XML_LOSSLESS_PARSE_STAGE, max_workers=4),
    )
    par_obs, _, _ = akt_orchestrator._transform_aktivitetsresultat_to_fhir_dicts(rows, context=None)

    seq_ids = [obs["identifier"][0]["value"] for _, obs in seq_obs if obs.get("identifier")]
    par_ids = [obs["identifier"][0]["value"] for _, obs in par_obs if obs.get("identifier")]
    assert par_ids == seq_ids

    seq_component_counts = [len(obs.get("component") or []) for _, obs in seq_obs]
    par_component_counts = [len(obs.get("component") or []) for _, obs in par_obs]
    assert par_component_counts == seq_component_counts


@pytest.mark.skip(reason="Parallel observation worker path removed; dict build is sequential")
def test_observation_build_retry_preserves_rows(monkeypatch):
    """If a worker result is invalid, synchronous retry must preserve all rows."""
    rows = _build_asset_rows(200)
    logger = dg.get_dagster_logger()

    monkeypatch.setattr(akt_orchestrator, "_FAST_MODE", True)
    monkeypatch.setattr(
        akt_xml,
        "_XML_FAST_PARSE_STAGE",
        _stage_with_workers(akt_xml._XML_FAST_PARSE_STAGE, max_workers=1),
    )
    parsed_components, _ = akt_xml._parse_xml_components_for_rows(rows, logger)

    observations, _, _, worker_errors = akt_orchestrator._build_observation_dicts_for_rows(
        rows=rows,
        parsed_data_by_index=parsed_components,
        logger=logger,
    )

    assert worker_errors == 0
    assert len(observations) == len(rows)

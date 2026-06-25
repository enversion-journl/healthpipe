# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Determinism tests for the dict-based AKTIVITETSRESULTAT pipeline.

Verifies that the dict-based FHIR build produces identical output regardless
of XML parse parallelism level, and that the sequential dict build is stable
across multiple invocations.
"""

from __future__ import annotations

import hashlib
import json

import dagster as dg
import pytest
from core.parallel import ParallelPolicy, ParallelStage
from DSA.FHIR.Aktivitetsresultat import orchestrator as akt_orchestrator
from DSA.FHIR.Aktivitetsresultat import routing_glue as akt_routing
from DSA.FHIR.utils.ifdb import xml_parsing_workers as akt_xml
from DSA.FHIR.utils.ifdb.aktivitetsresultat_config import SFI_MODE_LEGACY
from tests.validation.test_xml_parser_massive_diff import _build_massive_cases


def _build_asset_rows(num_cases: int = 1200) -> list[dict]:
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


def _hash_dicts(dicts: list[tuple[str, dict]]) -> str:
    h = hashlib.sha256()
    for _, d in dicts:
        d_copy = {k: v for k, v in d.items() if k != "id"}
        h.update(json.dumps(d_copy, sort_keys=True).encode())
    return h.hexdigest()


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
def test_dict_build_deterministic_with_parallel_xml_parse(monkeypatch):
    """Dict build output must be identical regardless of XML parse parallelism."""
    rows = _build_asset_rows(1500)

    monkeypatch.setattr(akt_orchestrator, "_FAST_MODE", False)
    monkeypatch.setattr(akt_routing, "_SFI_MODE", SFI_MODE_LEGACY)

    monkeypatch.setattr(
        akt_xml,
        "_XML_LOSSLESS_PARSE_STAGE",
        _stage_with_workers(akt_xml._XML_LOSSLESS_PARSE_STAGE, max_workers=1),
    )
    seq_dicts, _, _ = akt_orchestrator._transform_aktivitetsresultat_to_fhir_dicts(
        rows, context=None
    )

    monkeypatch.setattr(
        akt_xml,
        "_XML_LOSSLESS_PARSE_STAGE",
        _stage_with_workers(akt_xml._XML_LOSSLESS_PARSE_STAGE, max_workers=4),
    )
    par_dicts, _, _ = akt_orchestrator._transform_aktivitetsresultat_to_fhir_dicts(
        rows, context=None
    )

    assert len(seq_dicts) == len(par_dicts)

    seq_hash = _hash_dicts(seq_dicts)
    par_hash = _hash_dicts(par_dicts)
    assert seq_hash == par_hash, "Dict output differs between sequential and parallel XML parse"


@pytest.mark.slow
def test_dict_build_stable_across_runs(monkeypatch):
    """Two sequential runs must produce identical output (excluding UUIDs)."""
    rows = _build_asset_rows(500)

    monkeypatch.setattr(akt_orchestrator, "_FAST_MODE", False)
    monkeypatch.setattr(akt_routing, "_SFI_MODE", SFI_MODE_LEGACY)
    monkeypatch.setattr(
        akt_xml,
        "_XML_LOSSLESS_PARSE_STAGE",
        _stage_with_workers(akt_xml._XML_LOSSLESS_PARSE_STAGE, max_workers=1),
    )

    run1, _, _ = akt_orchestrator._transform_aktivitetsresultat_to_fhir_dicts(rows, context=None)
    run2, _, _ = akt_orchestrator._transform_aktivitetsresultat_to_fhir_dicts(rows, context=None)

    h1 = _hash_dicts(run1)
    h2 = _hash_dicts(run2)
    assert h1 == h2, "Dict output not stable across two sequential runs"


@pytest.mark.slow
def test_dict_build_fast_mode_deterministic(monkeypatch):
    """Fast mode dict build must also be deterministic with parallel parsing."""
    rows = _build_asset_rows(1500)

    monkeypatch.setattr(akt_orchestrator, "_FAST_MODE", True)
    monkeypatch.setattr(akt_routing, "_SFI_MODE", SFI_MODE_LEGACY)

    monkeypatch.setattr(
        akt_xml,
        "_XML_FAST_PARSE_STAGE",
        _stage_with_workers(akt_xml._XML_FAST_PARSE_STAGE, max_workers=1),
    )
    seq, _, _ = akt_orchestrator._transform_aktivitetsresultat_to_fhir_dicts(rows, context=None)

    monkeypatch.setattr(
        akt_xml,
        "_XML_FAST_PARSE_STAGE",
        _stage_with_workers(akt_xml._XML_FAST_PARSE_STAGE, max_workers=4),
    )
    par, _, _ = akt_orchestrator._transform_aktivitetsresultat_to_fhir_dicts(rows, context=None)

    assert _hash_dicts(seq) == _hash_dicts(par)


def test_dict_build_ordering_matches_input(monkeypatch):
    """Each row_dict[i] must map to observation_dicts[i].

    This is a legacy-builder invariant — SFI mode produces 0..N Observations
    per row depending on routing + XML content, so the test is pinned to
    legacy via the mode flag.
    """
    rows = _build_asset_rows(200)

    monkeypatch.setattr(akt_orchestrator, "_FAST_MODE", False)
    monkeypatch.setattr(akt_routing, "_SFI_MODE", SFI_MODE_LEGACY)
    monkeypatch.setattr(
        akt_xml,
        "_XML_LOSSLESS_PARSE_STAGE",
        _stage_with_workers(akt_xml._XML_LOSSLESS_PARSE_STAGE, max_workers=1),
    )

    dicts, _, _ = akt_orchestrator._transform_aktivitetsresultat_to_fhir_dicts(rows, context=None)

    for i, row in enumerate(rows):
        _, obs_dict = dicts[i]
        expected_id = str(row["OBJEKT_ID"])
        actual_ids = obs_dict.get("identifier", [])
        assert any(
            ident.get("value") == expected_id for ident in actual_ids
        ), f"Row {i}: expected identifier {expected_id}"


def test_dict_build_component_count_stable(monkeypatch):
    """Component counts per observation must be deterministic."""
    rows = _build_asset_rows(200)
    logger = dg.get_dagster_logger()

    monkeypatch.setattr(akt_orchestrator, "_FAST_MODE", False)
    monkeypatch.setattr(
        akt_xml,
        "_XML_LOSSLESS_PARSE_STAGE",
        _stage_with_workers(akt_xml._XML_LOSSLESS_PARSE_STAGE, max_workers=1),
    )

    parsed, kontekst, _ = akt_xml._parse_xml_lossless_for_rows(rows, logger)

    dicts1, _, _, _ = akt_orchestrator._build_observation_dicts_for_rows(
        rows, parsed, logger, kontekst_by_index=kontekst
    )
    dicts2, _, _, _ = akt_orchestrator._build_observation_dicts_for_rows(
        rows, parsed, logger, kontekst_by_index=kontekst
    )

    counts1 = [len(d.get("component", [])) for _, d in dicts1]
    counts2 = [len(d.get("component", [])) for _, d in dicts2]
    assert counts1 == counts2

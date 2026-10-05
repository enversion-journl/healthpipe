# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Test that XML parallel parser works under ``spawn`` start method.

Regression for prod OOM (2026-04-22): ``fork`` caused child workers to
inherit the parent's 1.3 GB of mmap'd identifier caches, contributing to
SIGKILL on memory-constrained pods. ``spawn`` avoids the inheritance by
starting each worker with a fresh Python interpreter.

This test verifies the spawn path actually completes successfully when
invoked from an imported module (the Dagster asset case), as opposed to
``__main__`` (which requires ``if __name__ == "__main__":`` bootstrap
boilerplate and is irrelevant for our runtime).
"""

from unittest.mock import MagicMock

import pytest
from core.parallel import ParallelPolicy, ParallelStage
from DSA.FHIR.utils.ifdb import aktivitetsresultat_config as config_module
from DSA.FHIR.utils.ifdb import xml_parsing_workers as xml_module

pytestmark = pytest.mark.unit

# Sample XML mimicking AKTIVITETSRESULTAT shape.
_SAMPLE_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Test note</Overskrift>
  <Element type="string">
    <Navn>Field1</Navn>
    <Vaerdi>Value1</Vaerdi>
  </Element>
  <Element type="string">
    <Navn>Field2</Navn>
    <Vaerdi>Value2</Vaerdi>
  </Element>
</Afdelingsydelsesresultat>"""


def _stage_with_workers(
    stage: ParallelStage,
    *,
    max_workers: int,
    threshold: int = 1,
) -> ParallelStage:
    return ParallelStage(
        stage_id=stage.stage_id,
        policy=ParallelPolicy(
            max_workers=max_workers,
            parallel_threshold=threshold,
            chunksize=stage.policy.chunksize,
            executor_kind=stage.policy.executor_kind,
        ),
        max_logged_errors=stage.max_logged_errors,
        mp_context=stage.mp_context,
        before_parallel_start=stage.before_parallel_start,
    )


def test_spawn_mp_context_is_set():
    assert config_module._XML_POOL_MP_CONTEXT.get_start_method() == "spawn"


def test_xml_parse_parallel_uses_spawn_successfully(monkeypatch):
    """Parallel XML parsing under spawn mode must complete without falling
    back to sequential. Validates that spawn works from imported-module
    context (which is how Dagster runs assets)."""
    rows = [{"OBJEKT_ID": f"A{i}", "PATIENT_ID": f"P{i}", "XML": _SAMPLE_XML} for i in range(200)]

    # Force parallel path
    monkeypatch.setattr(
        xml_module,
        "_XML_LOSSLESS_PARSE_STAGE",
        _stage_with_workers(xml_module._XML_LOSSLESS_PARSE_STAGE, max_workers=2),
    )

    logger = MagicMock()
    parsed, kontekst, errors = xml_module._parse_xml_lossless_for_rows(rows, logger)

    # All rows parsed, no fallback warning
    assert errors == 0
    assert sum(1 for p in parsed if p) == len(rows)
    fallback_warnings = [
        c
        for c in logger.warning.call_args_list
        if "fallback" in str(c).lower() or "falling back" in str(c).lower()
    ]
    assert not fallback_warnings, (
        f"Parallel parse fell back to sequential — spawn context failed: " f"{fallback_warnings[0]}"
    )


def test_xml_parse_fast_mode_uses_spawn_without_fallback(monkeypatch):
    """The FAST path (opt-in via IFDB_XML_FAST_MODE=1) uses a separate
    ProcessPool. Same spawn-context fix applies — verify it also does not
    fall back to sequential on spawn context."""
    rows = [{"OBJEKT_ID": f"A{i}", "PATIENT_ID": f"P{i}", "XML": _SAMPLE_XML} for i in range(200)]

    monkeypatch.setattr(
        xml_module,
        "_XML_FAST_PARSE_STAGE",
        _stage_with_workers(xml_module._XML_FAST_PARSE_STAGE, max_workers=2),
    )

    logger = MagicMock()
    xml_module._parse_xml_components_for_rows(rows, logger)

    fallback_warnings = [
        c
        for c in logger.warning.call_args_list
        if "fallback" in str(c).lower() or "falling back" in str(c).lower()
    ]
    assert (
        not fallback_warnings
    ), f"Parallel fast-parse fell back to sequential: {fallback_warnings[0]}"

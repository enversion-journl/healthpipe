# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Pin test: Observation_IFDB asset must pass markers list to transform.

The contract being pinned (from the chunked transform+post refactor plan):
the three Observation_IFDB assets accept a ``dict[str, Any]`` input, not a
``list[dict]``, and must extract the ``"markers"`` list before passing it
to the transform/post layer. The chunked refactor must preserve this — if
the wrapper accidentally passes the raw dict to the helper, the chunked
loop would try to slice a dict instead of a row list, which is a silent
correctness bug.

This test pins the existing extraction contract on the un-chunked daily,
monthly and full-load assets, then re-runs after each migration commit to
prove the wrapper still extracts ``"markers"`` correctly.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from DSA.FHIR.Observation.Observation_IFDB import (
    fhir_observation_daily_ifdb,
    fhir_observation_full_load_ifdb,
    fhir_observation_monthly_ifdb,
)


def _make_context() -> MagicMock:
    ctx = MagicMock(spec=dg.AssetExecutionContext)
    ctx.has_partition_key = True
    ctx.partition_key = "2026-01-01"
    ctx.log = MagicMock()
    return ctx


def _invoke(asset_def, *args):
    """Call the underlying decorated function directly, bypassing Dagster's
    invocation plumbing.

    Mirrors the pattern used in test_arc_afdlgrbeh_ils.py for unit-testing
    @dg.asset functions without spinning up the full execution machinery.
    """
    return asset_def.op.compute_fn.decorated_fn(*args)


@pytest.mark.parametrize(
    "asset_def",
    [
        fhir_observation_daily_ifdb,
        fhir_observation_monthly_ifdb,
        fhir_observation_full_load_ifdb,
    ],
    ids=["daily", "monthly", "full_load"],
)
def test_asset_extracts_markers_list_before_transform(asset_def) -> None:
    """The asset receives ``dict[str, Any]`` and must call the transform
    helper with the value of the ``markers`` key — never the whole dict.
    """
    sentinel_markers = [
        {"OBJECT_ID": "OBS-1", "PATIENT_ID": "PAT-1"},
        {"OBJECT_ID": "OBS-2", "PATIENT_ID": "PAT-2"},
    ]
    extracted_data = {
        "markers": sentinel_markers,
        "metadata": {"source": "eventmarker"},  # extra keys must be ignored
    }

    ctx = _make_context()
    ifdb_oracle = MagicMock()
    fhir_server = MagicMock()
    fhir_database = MagicMock()

    with (
        patch(
            "DSA.FHIR.Observation.Observation_IFDB._transform_observations_to_fhir",
            return_value=[],
        ) as mock_transform,
        patch(
            "DSA.FHIR.Observation.Observation_IFDB._post_observations_to_fhir",
            return_value=dg.MaterializeResult(metadata={"posted_count": dg.MetadataValue.int(0)}),
        ) as mock_post,
    ):
        _invoke(asset_def, ctx, extracted_data, ifdb_oracle, fhir_server, fhir_database)

    # The transform must have been called at least once with the markers list.
    assert (
        mock_transform.call_count >= 1
    ), "transform helper was never invoked — extraction contract broken"

    seen_rows: list[Any] = []
    for call in mock_transform.call_args_list:
        # First positional arg is rows (list[dict]).
        seen_rows.append(call.args[0] if call.args else call.kwargs.get("rows"))

    # Across all chunks (one in pre-chunked, possibly more after), the union
    # of rows passed to transform must equal the markers list. The dict itself
    # must never appear.
    union: list[Any] = []
    for batch in seen_rows:
        assert isinstance(batch, list), (
            f"transform received non-list ({type(batch).__name__}) — wrapper "
            f"likely passed the dict instead of markers"
        )
        for r in batch:
            assert isinstance(r, dict), (
                f"transform received non-dict row ({type(r).__name__}); "
                f"wrapper likely flattened the dict keys"
            )
            assert "OBJECT_ID" in r, (
                f"transform received unexpected row {r}; wrapper likely "
                f"passed the outer dict's keys instead of markers list"
            )
            union.append(r)

    assert (
        union == sentinel_markers
    ), f"transform did not receive the markers list verbatim; got {union!r}"

    # Post must also have been called (possibly per-chunk after migration).
    assert mock_post.call_count >= 1


def test_asset_handles_empty_markers_gracefully() -> None:
    """An empty ``markers`` value must produce no transform call (or at
    most a single transform call with an empty list)."""
    extracted_data: dict[str, Any] = {"markers": []}

    ctx = _make_context()

    with (
        patch(
            "DSA.FHIR.Observation.Observation_IFDB._transform_observations_to_fhir",
            return_value=[],
        ) as mock_transform,
        patch(
            "DSA.FHIR.Observation.Observation_IFDB._post_observations_to_fhir",
            return_value=dg.MaterializeResult(metadata={"posted_count": dg.MetadataValue.int(0)}),
        ),
    ):
        _invoke(
            fhir_observation_daily_ifdb,
            ctx,
            extracted_data,
            MagicMock(),
            MagicMock(),
            MagicMock(),
        )

    # Pre-chunked: transform is called once with []. After migration the
    # chunked helper short-circuits empty rows and may not call transform
    # at all. Both are acceptable; the contract pinned here is "transform
    # is never called with the dict itself".
    for call in mock_transform.call_args_list:
        rows = call.args[0] if call.args else call.kwargs.get("rows")
        assert isinstance(
            rows, list
        ), f"transform received non-list on empty markers: {type(rows).__name__}"


def test_asset_handles_missing_markers_key() -> None:
    """Missing ``"markers"`` key must default to empty list, never raise.

    The pre-refactor code uses ``extracted_data.get("markers", [])``. The
    chunked wrapper must keep the same default.
    """
    extracted_data: dict[str, Any] = {"unrelated": "value"}

    ctx = _make_context()

    with (
        patch(
            "DSA.FHIR.Observation.Observation_IFDB._transform_observations_to_fhir",
            return_value=[],
        ),
        patch(
            "DSA.FHIR.Observation.Observation_IFDB._post_observations_to_fhir",
            return_value=dg.MaterializeResult(metadata={"posted_count": dg.MetadataValue.int(0)}),
        ),
    ):
        # Should not raise — falling back to [] is the contract.
        _invoke(
            fhir_observation_daily_ifdb,
            ctx,
            extracted_data,
            MagicMock(),
            MagicMock(),
            MagicMock(),
        )

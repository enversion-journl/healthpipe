# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

from types import SimpleNamespace

import dagster as dg
import pytest
from DSA.FHIR.Aktivitetsresultat import orchestrator as mod


class _StreamingRows(list):
    def __init__(self, rows: list[dict]) -> None:
        super().__init__()
        self._rows = rows
        self.iter_batch_sizes: list[int | None] = []

    def __len__(self) -> int:
        return len(self._rows)

    def __iter__(self):
        yield from self._rows

    def __getitem__(self, _index):
        raise AssertionError("streaming path must not slice the full input")

    def iter_batches(self, chunk_size: int | None = None):
        self.iter_batch_sizes.append(chunk_size)
        if chunk_size is None:
            yield self._rows
            return
        for start in range(0, len(self._rows), chunk_size):
            yield self._rows[start : start + chunk_size]


class _FakeFhirDatabase:
    def __init__(self) -> None:
        self.suspend_calls = 0
        self.clear_calls: list[tuple[set[str], str]] = []

    def suspend_identifier_caches(self) -> None:
        self.suspend_calls += 1

    def log_identifier_lookup_status(self, *_args, **_kwargs) -> None:
        return None

    def safe_clear_identifier_caches_for_resources(
        self,
        resource_types: set[str],
        *,
        reason: str,
    ) -> None:
        self.clear_calls.append((resource_types, reason))


def _fake_context() -> SimpleNamespace:
    return SimpleNamespace(
        has_partition_key=True,
        partition_key="2026-04",
        log=SimpleNamespace(
            info=lambda *args, **kwargs: None,
            warning=lambda *args, **kwargs: None,
        ),
    )


@pytest.mark.unit
def test_aktivitetsresultat_pipeline_consumes_streamed_batches(monkeypatch) -> None:
    rows = _StreamingRows(
        [
            {"OBJEKT_ID": "1", "UDFOERELSE_ID": "U1"},
            {"OBJEKT_ID": "2", "UDFOERELSE_ID": "U2"},
            {"OBJEKT_ID": "3", "UDFOERELSE_ID": "U3"},
        ]
    )
    fhir_database = _FakeFhirDatabase()
    transform_calls: list[list[str]] = []
    post_counts: list[int] = []

    monkeypatch.setattr(mod, "_AKTIVITETSRESULTAT_CHUNK_SIZE", 2)
    monkeypatch.setattr(mod, "_sfi_mode_enabled", lambda: False)
    monkeypatch.setattr(mod, "_ensure_reference_dependencies", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        mod,
        "_pre_resolve_references",
        lambda *_args, **_kwargs: SimpleNamespace(
            lookups_by_resource_type=lambda: {},
            miss_counts={"Patient": 0, "Encounter": 0, "EpisodeOfCare": 0},
        ),
    )
    monkeypatch.setattr(mod, "_fetch_aktivitetsudfoerelse_data", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        mod,
        "_fetch_aktivitetsudfoerelse_speciale_data",
        lambda *args, **kwargs: {},
    )

    def _fake_transform(chunk, *_args, **_kwargs):
        transform_calls.append([row["OBJEKT_ID"] for row in chunk])
        return [(f"obs-{row['OBJEKT_ID']}", {}) for row in chunk], [], []

    monkeypatch.setattr(mod, "_transform_aktivitetsresultat_to_fhir_dicts", _fake_transform)

    import DSA.FHIR.utils.ifdb.fhir_posting as posting_mod

    def _fake_post_resource_dicts_direct(**kwargs):
        post_counts.append(len(kwargs["resource_dicts"]))
        return dg.MaterializeResult(
            metadata={
                "posted_count": dg.MetadataValue.int(len(kwargs["resource_dicts"])),
                "failed_count": dg.MetadataValue.int(0),
            }
        )

    monkeypatch.setattr(
        posting_mod,
        "post_resource_dicts_direct",
        _fake_post_resource_dicts_direct,
    )

    result = mod._run_aktivitetsresultat_pipeline_chunked(
        extracted_data=rows,
        context=_fake_context(),
        ifdb_oracle=SimpleNamespace(),
        fhir_database=fhir_database,
    )

    assert rows.iter_batch_sizes == [2]
    assert transform_calls == [["1", "2"], ["3"]]
    assert post_counts == [2, 1]
    assert fhir_database.suspend_calls == 2
    assert fhir_database.clear_calls == [
        ({"Observation", "Procedure", "Condition"}, "post-partition cleanup")
    ]
    assert result.metadata["posted_count"].value == 3
    assert result.metadata["chunk_count"].value == 2

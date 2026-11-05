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
            release_chunk_scoped_ids=lambda: 0,
        ),
    )
    monkeypatch.setattr(mod, "_fetch_aktivitetsudfoerelse_data_arc", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        mod,
        "_fetch_aktivitetsudfoerelse_speciale_data_arc",
        lambda *args, **kwargs: {},
    )

    def _fake_transform(chunk, *_args, **_kwargs):
        transform_calls.append([row["OBJEKT_ID"] for row in chunk])
        return [(f"obs-{row['OBJEKT_ID']}", {}) for row in chunk], [], []

    monkeypatch.setattr(mod, "_transform_aktivitetsresultat_to_fhir_dicts", _fake_transform)

    def _fake_post_resource_dicts_direct(**kwargs):
        post_counts.append(len(kwargs["resource_dicts"]))
        return dg.MaterializeResult(
            metadata={
                "posted_count": dg.MetadataValue.int(len(kwargs["resource_dicts"])),
                "failed_count": dg.MetadataValue.int(0),
            }
        )

    monkeypatch.setattr(
        mod,
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


@pytest.mark.unit
def test_udf_og_speciale_opslag_er_chunk_lokale(monkeypatch) -> None:
    """Opslagene må ikke akkumulere på tværs af chunks — hver chunk starter fra tomme dicts."""
    rows = _StreamingRows(
        [
            {"OBJEKT_ID": "1", "UDFOERELSE_ID": "U1"},
            {"OBJEKT_ID": "2", "UDFOERELSE_ID": "U2"},
            {"OBJEKT_ID": "3", "UDFOERELSE_ID": "U3"},
        ]
    )
    fhir_database = _FakeFhirDatabase()
    udf_lookup_pr_chunk: list[set[str]] = []
    speciale_lookup_pr_chunk: list[set[str]] = []
    hentede_udf_ids: list[list[str]] = []

    monkeypatch.setattr(mod, "_AKTIVITETSRESULTAT_CHUNK_SIZE", 1)
    monkeypatch.setattr(mod, "_sfi_mode_enabled", lambda: False)
    monkeypatch.setattr(mod, "_ensure_reference_dependencies", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        mod,
        "_pre_resolve_references",
        lambda *_args, **_kwargs: SimpleNamespace(
            lookups_by_resource_type=lambda: {},
            miss_counts={"Patient": 0, "Encounter": 0, "EpisodeOfCare": 0},
            release_chunk_scoped_ids=lambda: 0,
        ),
    )

    def _fake_udf_fetch(udf_ids, *_args, **_kwargs):
        hentede_udf_ids.append(list(udf_ids))
        return {udf_id: {"OBJEKT_ID": udf_id} for udf_id in udf_ids}

    monkeypatch.setattr(mod, "_fetch_aktivitetsudfoerelse_data_arc", _fake_udf_fetch)
    monkeypatch.setattr(
        mod,
        "_fetch_aktivitetsudfoerelse_speciale_data_arc",
        lambda udf_ids, *_args, **_kwargs: {udf_id: [{"SPECIALE_KODE": "X"}] for udf_id in udf_ids},
    )

    def _fake_transform(chunk, *_args, udf_lookup=None, speciale_lookup=None, **_kwargs):
        udf_lookup_pr_chunk.append(set(udf_lookup or {}))
        speciale_lookup_pr_chunk.append(set(speciale_lookup or {}))
        return [(f"obs-{row['OBJEKT_ID']}", {}) for row in chunk], [], []

    monkeypatch.setattr(mod, "_transform_aktivitetsresultat_to_fhir_dicts", _fake_transform)

    monkeypatch.setattr(
        mod,
        "post_resource_dicts_direct",
        lambda **kwargs: dg.MaterializeResult(
            metadata={
                "posted_count": dg.MetadataValue.int(len(kwargs["resource_dicts"])),
                "failed_count": dg.MetadataValue.int(0),
            }
        ),
    )

    mod._run_aktivitetsresultat_pipeline_chunked(
        extracted_data=rows,
        context=_fake_context(),
        ifdb_oracle=SimpleNamespace(),
        fhir_database=fhir_database,
        mssql_target_staging=SimpleNamespace(),
    )

    # Hver chunk ser kun sine egne udførelser — ikke de foregående chunks'.
    assert udf_lookup_pr_chunk == [{"U1"}, {"U2"}, {"U3"}]
    assert speciale_lookup_pr_chunk == [{"U1"}, {"U2"}, {"U3"}]
    # Alle udførelser hentes stadig (ingen falsk cache-hit, intet spring over).
    assert hentede_udf_ids == [["U1"], ["U2"], ["U3"]]


# ---------------------------------------------------------------------------
# Én sti gennem chunk-forløbet: under én chunk er ikke en særlig gren.
# ---------------------------------------------------------------------------


class _PostOptager:
    """Optager, hvad orkestreringen poster (type og id'er), og svarer med tal.

    ``recovered_for`` lader testen simulere, at posten af én familie måtte
    genoprette rækker efter unique-violation — tallet skal nå metadata.
    """

    def __init__(self, recovered_for: dict[str, int] | None = None) -> None:
        self.posted: dict[str, list[str]] = {}
        self._recovered_for = recovered_for or {}

    def __call__(self, **kwargs) -> dg.MaterializeResult:
        rtype = kwargs["resource_type"]
        dicts = kwargs["resource_dicts"]
        self.posted.setdefault(rtype, []).extend(rid for rid, _ in dicts)
        return dg.MaterializeResult(
            metadata={
                "posted_count": dg.MetadataValue.int(len(dicts)),
                "failed_count": dg.MetadataValue.int(0),
                "unique_violation_recovered_count": dg.MetadataValue.int(
                    self._recovered_for.get(rtype, 0) if dicts else 0
                ),
            }
        )


def _fake_transform_alle_familier(chunk, *_args, **_kwargs):
    """Legacy-transformen erstattet: én Observation pr. række, Procedure for 1 og 3, Condition for 2."""
    obs = [(f"obs-{row['OBJEKT_ID']}", {}) for row in chunk]
    proc = [(f"proc-{row['OBJEKT_ID']}", {}) for row in chunk if row["OBJEKT_ID"] != "2"]
    cond = [(f"cond-{row['OBJEKT_ID']}", {}) for row in chunk if row["OBJEKT_ID"] == "2"]
    return obs, proc, cond


def _patch_pipeline_omgivelser(monkeypatch, *, chunk_size: int, post: _PostOptager) -> None:
    monkeypatch.setattr(mod, "_AKTIVITETSRESULTAT_CHUNK_SIZE", chunk_size)
    monkeypatch.setattr(mod, "_sfi_mode_enabled", lambda: False)
    monkeypatch.setattr(mod, "_ensure_reference_dependencies", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        mod,
        "_pre_resolve_references",
        lambda *_args, **_kwargs: SimpleNamespace(
            lookups_by_resource_type=lambda: {},
            miss_counts={"Patient": 0, "Encounter": 0, "EpisodeOfCare": 0},
            release_chunk_scoped_ids=lambda: 0,
        ),
    )
    monkeypatch.setattr(mod, "_fetch_aktivitetsudfoerelse_data_arc", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        mod,
        "_fetch_aktivitetsudfoerelse_speciale_data_arc",
        lambda *args, **kwargs: {},
    )
    monkeypatch.setattr(
        mod, "_transform_aktivitetsresultat_to_fhir_dicts", _fake_transform_alle_familier
    )

    monkeypatch.setattr(mod, "post_resource_dicts_direct", post)


def _tre_raekker() -> _StreamingRows:
    return _StreamingRows(
        [
            {"OBJEKT_ID": "1", "UDFOERELSE_ID": "U1"},
            {"OBJEKT_ID": "2", "UDFOERELSE_ID": "U2"},
            {"OBJEKT_ID": "3", "UDFOERELSE_ID": "U3"},
        ]
    )


def _metadata_vaerdier(result: dg.MaterializeResult) -> dict:
    return {key: value.value for key, value in result.metadata.items()}


@pytest.mark.unit
def test_enkelt_chunk_rapporterer_familier_dubletter_og_recovery(monkeypatch) -> None:
    """Under én chunk er metadata komplet: familietal, dubletter, recovery-tæller, chunk-tal."""
    fhir_database = _FakeFhirDatabase()
    post = _PostOptager(recovered_for={"Procedure": 1})
    _patch_pipeline_omgivelser(monkeypatch, chunk_size=10, post=post)

    result = mod._run_aktivitetsresultat_pipeline_chunked(
        extracted_data=_tre_raekker(),
        context=_fake_context(),
        ifdb_oracle=SimpleNamespace(),
        fhir_database=fhir_database,
    )

    meta = _metadata_vaerdier(result)
    assert meta["chunk_count"] == 1
    assert meta["extracted_count"] == 3
    assert meta["posted_count"] == 3
    assert meta["procedure_posted_count"] == 2
    assert meta["condition_posted_count"] == 1
    assert meta["failed_count"] == 0
    assert meta["duplicates_removed"] == 0
    assert meta["unique_violation_recovered_count"] == 1
    assert meta["method"] == "direct_postgres_dict_chunked"
    assert fhir_database.suspend_calls == 1
    assert fhir_database.clear_calls == [
        ({"Observation", "Procedure", "Condition"}, "post-partition cleanup")
    ]


@pytest.mark.unit
@pytest.mark.parametrize("chunk_size", [1, 2, 10])
def test_samme_sti_uanset_chunkstoerrelse(monkeypatch, chunk_size: int) -> None:
    """Chunk-størrelsen ændrer kun chunk_count — postede id'er og tal er de samme."""
    fhir_database = _FakeFhirDatabase()
    post = _PostOptager()
    _patch_pipeline_omgivelser(monkeypatch, chunk_size=chunk_size, post=post)

    result = mod._run_aktivitetsresultat_pipeline_chunked(
        extracted_data=_tre_raekker(),
        context=_fake_context(),
        ifdb_oracle=SimpleNamespace(),
        fhir_database=fhir_database,
    )

    assert sorted(post.posted["Observation"]) == ["obs-1", "obs-2", "obs-3"]
    assert sorted(post.posted["Procedure"]) == ["proc-1", "proc-3"]
    assert post.posted["Condition"] == ["cond-2"]
    meta = _metadata_vaerdier(result)
    assert meta["chunk_count"] == -(-3 // chunk_size)
    assert (
        meta["posted_count"],
        meta["procedure_posted_count"],
        meta["condition_posted_count"],
    ) == (
        3,
        2,
        1,
    )
    assert meta["unique_violation_recovered_count"] == 0
    # Én suspend pr. chunk, én samlet rydning pr. kørsel — uanset antal chunks.
    assert fhir_database.suspend_calls == meta["chunk_count"]
    assert len(fhir_database.clear_calls) == 1


# ---------------------------------------------------------------------------
# Run-tag-override af chunkstørrelsen: målekørsler kan chunke anderledes uden miljøændring.
# ---------------------------------------------------------------------------


def _context_med_tags(tags: dict[str, str]) -> SimpleNamespace:
    ctx = _fake_context()
    ctx.run = SimpleNamespace(tags=tags)
    return ctx


@pytest.mark.unit
def test_run_tag_overstyrer_chunkstoerrelsen(monkeypatch) -> None:
    """Miljøets chunk (her 1) giver 3 chunks; tagget fhir/aktres_chunk_size=10 giver én."""
    fhir_database = _FakeFhirDatabase()
    post = _PostOptager()
    _patch_pipeline_omgivelser(monkeypatch, chunk_size=1, post=post)

    result = mod._run_aktivitetsresultat_pipeline_chunked(
        extracted_data=_tre_raekker(),
        context=_context_med_tags({"fhir/aktres_chunk_size": "10"}),
        ifdb_oracle=SimpleNamespace(),
        fhir_database=fhir_database,
    )

    meta = _metadata_vaerdier(result)
    assert (meta["chunk_count"], meta["chunk_size"]) == (1, 10)
    assert fhir_database.suspend_calls == 1
    assert sorted(post.posted["Observation"]) == ["obs-1", "obs-2", "obs-3"]


@pytest.mark.unit
@pytest.mark.parametrize("tag", ["nul", "0", "-5"])
def test_ugyldigt_chunk_tag_ignoreres(monkeypatch, tag: str) -> None:
    """Et ugyldigt tag må aldrig vælte kørslen — miljøets chunk bruges."""
    fhir_database = _FakeFhirDatabase()
    post = _PostOptager()
    _patch_pipeline_omgivelser(monkeypatch, chunk_size=2, post=post)

    result = mod._run_aktivitetsresultat_pipeline_chunked(
        extracted_data=_tre_raekker(),
        context=_context_med_tags({"fhir/aktres_chunk_size": tag}),
        ifdb_oracle=SimpleNamespace(),
        fhir_database=fhir_database,
    )

    meta = _metadata_vaerdier(result)
    assert (meta["chunk_count"], meta["chunk_size"]) == (2, 2)

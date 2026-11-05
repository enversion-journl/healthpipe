# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Metadata widths and complete 47-source/38-target accounting for LPR3."""

import json
from pathlib import Path

import pytest
from DSI.csv_files.lpr3_datasets import (
    ARCHIVE_DATASETS,
    ARCHIVE_SOURCES,
    ARCHIVES_BY_TABLE,
    SOURCE_DATASETS,
    SOURCES_BY_PREFIX,
    archive_ddl,
    extract_groups,
)

pytestmark = pytest.mark.unit


def test_all_source_columns_roundtrip_without_handwritten_type_substitutions():
    metadata = json.loads(
        (Path(__file__).parents[2] / "DSI/csv_files/lpr3_metadata.json").read_text()
    )
    assert {d.source_prefix for d in SOURCE_DATASETS} == set(metadata)
    for dataset in SOURCE_DATASETS:
        assert [list(c) for c in dataset.metadata_records] == metadata[dataset.source_prefix][
            "columns"
        ]
        assert len(set(dataset.source_columns)) == len(dataset.source_columns)


def test_ext_names_keep_pairs_separate_and_arc_accounts_for_every_source():
    assert len({d.ext_table for d in SOURCE_DATASETS}) == len(SOURCE_DATASETS)
    assert sorted(p for pair in ARCHIVE_SOURCES.values() for p in pair) == sorted(SOURCES_BY_PREFIX)
    assert set(ARCHIVE_SOURCES["lpr3_kontakt"]) == {"DimKontakt", "fctKontakt"}
    assert ARCHIVE_SOURCES["lpr3_bridge_sygehusophold"] == ("FctBridge_Sygehusophold",)


@pytest.mark.parametrize("workers", [2, 4, 8])
def test_parallel_passes_partition_all_47_tables_and_isolate_procedure(workers):
    groups = extract_groups(workers)
    assert len(groups) == workers
    assert set(groups[0]) == {"DimProcedureRegistrering", "fctProcedureRegistrering"}
    assert sorted(p for group in groups for p in group) == sorted(SOURCES_BY_PREFIX)
    assert all(groups)


def test_wire_widths_preserve_unicode_binary_and_fractional_datetime():
    kontakt = {c.name: c for c in SOURCES_BY_PREFIX["DimKontakt"].columns}
    assert kontakt["Kontakt_ID"].wire_width == 32
    assert kontakt["LPR_Entity_ID"].wire_width == 36
    assert kontakt["DW_ValidFrom"].sql_type == "DATETIME2(7)"
    assert kontakt["DW_ValidFrom"].wire_width == 27
    adresse = {c.name: c for c in SOURCES_BY_PREFIX["DimAdresse"].columns}
    assert adresse["Vejnavn"].wire_width == 500  # Metadata is 1000 bytes of NVARCHAR.


def test_every_source_column_is_accounted_for_exactly_once_in_archive_contract():
    dispositions = [item for dataset in ARCHIVE_DATASETS for item in dataset.dispositions]
    actual = [(d.source_prefix, d.source_column) for d in dispositions]
    expected = [(d.source_prefix, c.name) for d in SOURCE_DATASETS for c in d.columns]
    assert sorted(actual) == sorted(expected)
    assert len(actual) == len(set(actual)) == 1042
    assert all(d.reason and d.action in ("keep", "resolve", "omit") for d in dispositions)
    for dataset in ARCHIVE_DATASETS:
        assert not any(c.casefold().startswith("dw_sk_") for c in dataset.arc_columns)
        assert not any(
            c.casefold() in ("dw_isdeleted", "dw_isinferred") for c in dataset.arc_columns
        )
        assert not set(dataset.surrogate_columns) & set(dataset.change_detect_columns)
        assert set(dataset.merge_keys) <= set(dataset.arc_columns)


def test_binary_id_exception_does_not_invent_lpr_entity_id():
    for entity, column in (
        ("kontaktlokation", "Opholdadresse_ID"),
        ("betalingsoplysning", "Betalingsoplysning_ID"),
    ):
        dataset = ARCHIVES_BY_TABLE[f"lpr3_{entity}"]
        assert dataset.key_policy.components == (column,)
        assert dataset.key_policy.method == "binary_id"
        assert "LPR_Entity_ID" not in dataset.arc_columns
        assert dataset.merge_keys == (f"{entity}_id",)
        assert dataset.columns[0].sql_type == "VARBINARY(16)"
    assert ARCHIVES_BY_TABLE["lpr3_kontakt"].key_policy.method == "uuid"


def test_references_preserve_role_version_and_null_sentinel_contract():
    kontakt = ARCHIVES_BY_TABLE["lpr3_kontakt"]
    ref = next(r for r in kontakt.references if r.source_column == "DW_SK_Organisation_Henvisende")
    assert [c.name for c in ref.columns] == [
        "organisation_henvisende_noegle",
        "organisation_henvisende_gyldig_fra",
    ]
    assert all(c.nullable for c in ref.columns)
    assert "-1 resolves to NULL" in ref.sentinel
    assert "DimOrganisation" in kontakt.required_sources
    absent = next(
        r for r in ARCHIVES_BY_TABLE["lpr3_sygehusophold"].references if r.missing_dimension
    )
    assert absent.target_entity == "sygehusopholdegenskaber"


def test_archive_ddl_stays_in_lockstep_with_every_spec():
    directory = Path(__file__).parents[2] / "infrastructure/ddl/sources/lpr3"
    files = archive_ddl()
    assert {name: (directory / name).read_text() for name in files} == files
    assert len(files) == len(ARCHIVE_DATASETS)

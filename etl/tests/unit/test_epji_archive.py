# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Invariant tests for the EPJI archive layer (arc.epji_*)."""

from __future__ import annotations

import re
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.unit


@pytest.fixture
def archive_config():
    from DSI.oracle.epji_archive import EPJI_LOCATION_ARCHIVE_CONFIG

    return EPJI_LOCATION_ARCHIVE_CONFIG


def test_archive_merge_keys(archive_config):
    assert archive_config.strategy == "upsert"
    assert archive_config.update_only_on_change is True
    assert archive_config.merge_keys == ("code",)
    assert set(archive_config.non_key_columns) == {
        "location_pk",
        "display",
        "sor_code",
        "identifier_system",
    }


def test_detect_deletes_on_full_snapshot_source(archive_config):
    # ext_epji_location is a genuine full snapshot every run (unlike
    # arc.sor_register's cumulative version file), so soft-deleting locations
    # that vanish from the latest snapshot is correct here.
    assert archive_config.detect_deletes is True
    assert archive_config.upstream_is_full_load is True


def test_dedup_source_on_keys_prefers_sor_code(archive_config):
    # EXTERNAL_ID has no enforced uniqueness upstream in EPJI.LOCATION, so
    # the archive (not the extract) must guard against a duplicate `code`.
    # sor_code sorting first in non_key_columns means a duplicate pair with
    # different, non-empty SOR codes deterministically keeps one — and
    # execute_archive_merge reports the collapse as rows_dedup_dropped.
    assert archive_config.dedup_source_on_keys is True
    assert archive_config.non_key_columns[0] == "sor_code"


def test_merge_key_never_in_non_key_columns(archive_config):
    overlap = set(archive_config.merge_keys) & set(archive_config.non_key_columns)
    assert not overlap


def test_archive_source_table_matches_extract_target(archive_config):
    from DSI.oracle.epji_extract import EXT_COLUMNS

    assert archive_config.source_schema == "ext"
    assert archive_config.source_table == "epji_location"
    assert archive_config.target_schema == "arc"
    assert archive_config.target_table == "epji_location"
    assert set(archive_config.non_key_columns) | set(archive_config.merge_keys) == set(EXT_COLUMNS)


def test_arc_epji_shakalike_depends_on_extract():
    from DSI.oracle.epji_archive import arc_epji_shakalike

    dep_keys = {key.to_user_string() for key in arc_epji_shakalike.dependency_keys}
    assert "ext_epji_location" in dep_keys
    assert "staging_infra_source_epji" in dep_keys


def test_arc_epji_shakalike_registered_in_organisation_cs_job():
    from jobs.organization_jobs import organisation_cs_job

    selection_str = str(organisation_cs_job.selection)
    assert "arc_epji_shakalike" in selection_str
    assert "ext_epji_location" in selection_str
    assert "cs_shakalike" in selection_str


@pytest.mark.parametrize(
    "config_name",
    [
        "EPJI_LOCATION_ARCHIVE_CONFIG",
        "EPJI_PRACTITIONER_ARCHIVE_CONFIG",
        "EPJI_PRACTITIONER_IDENTIFIER_ARCHIVE_CONFIG",
        "EPJI_PRACTITIONER_ROLE_ARCHIVE_CONFIG",
    ],
)
def test_ddl_column_lists_match_non_key_columns(config_name):
    """Each arc.epji_* CREATE TABLE block must contain exactly the archive
    config's merge keys and non_key_columns, plus the soft-delete flag and
    the standard audit columns, and its primary key must be the merge keys.

    Guards against silent column-list and key drift between the DDL and the MERGE.
    """
    import DSI.oracle.epji_archive as epji_archive

    archive_config = getattr(epji_archive, config_name)
    table = archive_config.target_table
    ddl_path = (
        Path(__file__).resolve().parents[2]
        / "infrastructure"
        / "ddl"
        / "sources"
        / "epji"
        / f"{table}.sql"
    )
    ddl = ddl_path.read_text(encoding="utf-8")

    audit_cols = {"dw_timestamp_insert", "dw_timestamp_update", "dw_is_deleted"}

    pattern = re.compile(
        rf"CREATE TABLE \[arc\]\.\[{table}\](.*?)\bEND\b",
        re.DOTALL,
    )
    match = pattern.search(ddl)
    assert match, f"No CREATE TABLE block found for arc.{table}"

    body = match.group(1)
    col_names = set(re.findall(r"\[([^\]]+)\]", body))
    col_names = {name for name in col_names if not name.startswith(("PK_", "DF_", "UQ_"))}
    col_names -= audit_cols

    expected = set(archive_config.non_key_columns) | set(archive_config.merge_keys)
    assert col_names == expected, (
        f"DDL drift for arc.{table}:\n"
        f"  missing in DDL: {expected - col_names}\n"
        f"  extra in DDL:   {col_names - expected}"
    )

    primary_key = re.search(r"PRIMARY KEY CLUSTERED\s*\(([^)]*)\)", body)
    assert primary_key, f"No clustered primary key in arc.{table}"
    assert tuple(re.findall(r"\[([^\]]+)\]", primary_key.group(1))) == archive_config.merge_keys


@pytest.mark.parametrize(
    ("pending_above_cap", "override_above_cap", "merged"),
    [(1, None, False), (0, None, True), (1, 1, True)],
    ids=["above_cap_fails_before_merge", "at_cap_merges", "run_override_lifts_cap"],
)
def test_practitioner_archive_checks_soft_delete_cap_before_merge(
    monkeypatch, pending_above_cap, override_above_cap, merged
):
    import dagster as dg
    import DSI.oracle.epji_archive as epji_archive
    from core.archive_merge import ArchiveMergeResult
    from core.dfm_config import RunModeConfig
    from core.etl_thresholds import get_soft_delete_threshold

    cap = get_soft_delete_threshold("arc.epji_practitioner")
    override = None if override_above_cap is None else cap + override_above_cap

    @contextmanager
    def connection():
        conn = MagicMock()
        conn.cursor.return_value.fetchone.return_value = (cap + pending_above_cap,)
        yield conn

    resource = MagicMock()
    resource.get_connection = connection
    merge_calls = []

    def fake_merge(*args):
        merge_calls.append(args)
        return ArchiveMergeResult(
            row_count=0,
            new_checkpoint=None,
            detect_deletes=True,
            update_only_on_change=True,
            strategy="upsert",
            upstream_is_full_load=True,
        )

    monkeypatch.setattr(epji_archive, "execute_archive_merge", fake_merge)

    def run():
        return epji_archive._run_practitioner_archive(
            MagicMock(),
            resource,
            RunModeConfig(soft_delete_threshold=override),
            epji_archive.EPJI_PRACTITIONER_ARCHIVE_CONFIG,
        )

    if merged:
        run()
    else:
        with pytest.raises(dg.Failure):
            run()
    assert bool(merge_calls) is merged

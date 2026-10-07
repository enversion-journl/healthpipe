# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for the HAIBA DSO sync.

The failure these guard against is quiet and severe: a config or DDL change
that lets CPR-bearing data (map clean_codes, arc tables) reach the DSO
reporting database, or a sync that publishes 'ikke_koert' relation columns
because it ran between the rebuild and the contact stamp.
"""

import re
from pathlib import Path

import pytest
from DSA.SQL.dm.haiba import DM_TABLES
from DSO.load.sync_configs import HAIBA_DM_CONFIGS
from DSO.load.sync_haiba import all_haiba_dso_sync_assets

pytestmark = pytest.mark.unit

DM_DDL = (
    Path(__file__).resolve().parents[2]
    / "infrastructure"
    / "ddl"
    / "tables"
    / "haiba"
    / "02_dm.sql"
)

EXPECTED_TABLES = {t.dm_table for t in DM_TABLES} | {t.egenskaber_table for t in DM_TABLES}


def test_configs_cover_exactly_the_eight_dm_tables():
    """Every fact and egenskaber table syncs; nothing else does. A missing
    table is silently absent from DSO, an extra one could drag staging
    internals along."""
    assert {c.table for c in HAIBA_DM_CONFIGS.values()} == EXPECTED_TABLES
    assert all(c.schema == "dm" for c in HAIBA_DM_CONFIGS.values())


def test_map_arc_and_meta_never_sync():
    """map clean_codes carry CPR on the case tables, and arc/ext/meta are
    pipeline internals. Only the dm schema syncs, and every table name must be
    exactly one of the dm names — the arc/ext tables use lowercase names
    (haiba_agg_infektion), so a case-sensitive match rejects them."""
    for config in HAIBA_DM_CONFIGS.values():
        assert config.schema == "dm", f"{config.table}: only the dm schema syncs"
        assert config.table in EXPECTED_TABLES, f"{config.table}: not a dm table name"
        assert (
            config.source_schema is None and config.source_table is None
        ), f"{config.table}: must not read from another schema/table"


def test_sync_mode_matches_the_rebuild():
    """The dm rebuild is TRUNCATE + INSERT, so every dw_timestamp renews each
    run — a watermark sync would move the full table anyway. Fact and
    egenskaber share the dw_ek primary key (1:1)."""
    for config in HAIBA_DM_CONFIGS.values():
        assert config.mode == "truncate_insert", config.table
        base = config.table.removesuffix("Egenskaber")
        assert config.primary_key == f"dw_ek_{base}", config.table


def test_no_synced_table_declares_a_cpr_column():
    """The GDPR point of the whole split: dm is CPR-free by
    construction, so DSO needs no access restriction. Verified against the
    DDL every synced table is created from."""
    sql = DM_DDL.read_text(encoding="utf-8")
    for config in HAIBA_DM_CONFIGS.values():
        block = re.search(
            rf"CREATE TABLE \[dm\]\.\[{re.escape(config.table)}\] \((.*?)\n    \);",
            sql,
            re.DOTALL,
        )
        assert block, f"no CREATE TABLE block for dm.{config.table}"
        columns = set(re.findall(r"^\s{8}\[(\w+)\]\s+\w", block.group(1), re.MULTILINE))
        assert "cprnr" not in columns, f"dm.{config.table} carries CPR"
        assert "clean_code" not in columns, f"dm.{config.table} carries the map key"


def _deps(asset) -> set[str]:
    key = next(iter(asset.keys))
    return {k.to_user_string() for k in asset.asset_deps[key]}


def test_every_sync_waits_for_the_dso_ddl():
    """Syncing into a table that does not exist on DSO fails at runtime;
    dso_infra_haiba creates the dm tables (and only those) on DSO."""
    for asset in all_haiba_dso_sync_assets:
        assert "dso_infra_haiba" in _deps(asset), asset.key.to_user_string()


def test_case_syncs_wait_for_the_contact_stamp():
    """The rebuild resets the relation keys on the fact to NULL and
    match_kvalitet_* on egenskaber to 'ikke_koert'; the kontakt asset stamps
    both afterwards. A sync ordered on the bare rebuild would publish a week
    of 'relation not computed' to every DSO consumer — so BOTH fact and
    egenskaber case syncs wait for the kontakt asset."""
    by_name = {a.key.to_user_string(): _deps(a) for a in all_haiba_dso_sync_assets}
    for name in (
        "dso_sync_dm_haiba_cases_infektion",
        "dso_sync_dm_haiba_cases_infektion_egenskaber",
    ):
        assert "dm_haiba_cases_infektion_kontakt" in by_name[name], name
    for name in ("dso_sync_dm_haiba_cases_ssi", "dso_sync_dm_haiba_cases_ssi_egenskaber"):
        assert "dm_haiba_cases_ssi_kontakt" in by_name[name], name
    # The agg tables have no contact relation and wait only for their rebuild.
    assert "dm_haiba_agg_infektion" in by_name["dso_sync_dm_haiba_agg_infektion_egenskaber"]


def test_fact_syncs_wait_for_the_dimensions_their_keys_point_at():
    by_name = {a.key.to_user_string(): _deps(a) for a in all_haiba_dso_sync_assets}
    for name in ("dso_sync_dm_haiba_agg_infektion", "dso_sync_dm_haiba_cases_infektion"):
        assert "dso_sync_dim_organisation" in by_name[name], name
    for name in ("dso_sync_dm_haiba_cases_infektion", "dso_sync_dm_haiba_cases_ssi"):
        assert "dso_sync_dim_borger" in by_name[name], name


def test_sync_assets_are_registered():
    from DSO.load import all_dso_sync_assets

    for asset in all_haiba_dso_sync_assets:
        assert asset in all_dso_sync_assets


def test_every_dm_table_has_a_filtered_consumer_view():
    """SDS's window rolls every January and soft-deletes roughly a fifth of
    the archive; those rows never leave dm, so an unfiltered SELECT over-counts
    more year over year — silently, in the direction of 'more infections'.
    vw_dm.HAIBA_* makes the safe read the default: every one of the eight dm
    tables must have a view that filters dw_is_deleted = 0."""
    view_sql = (
        Path(__file__).resolve().parents[2]
        / "infrastructure"
        / "ddl"
        / "views"
        / "haiba"
        / "01_vw_dm.sql"
    ).read_text(encoding="utf-8")
    for table in EXPECTED_TABLES:
        block = re.search(
            rf"CREATE OR ALTER VIEW \[vw_dm\]\.\[{re.escape(table)}\]\s*"
            rf"AS\s*SELECT \*\s*FROM \[dm\]\.\[{re.escape(table)}\]\s*"
            rf"WHERE \[dw_is_deleted\] = 0;",
            view_sql,
        )
        assert block, f"vw_dm.{table} missing or not filtering dw_is_deleted = 0"


def test_view_asset_waits_for_the_tables_it_wraps():
    from infrastructure import dso_infra_views_haiba

    assert "dso_infra_haiba" in _deps(dso_infra_views_haiba)


def test_the_two_jobs_split_at_the_borger_handoff():
    """The dm layer must NOT run in the same job as the stub-patient creation.

    A stub posted to Aidbox only becomes a dw_ek_Borger once the nightly DFM
    chain has projected it, so publishing in the same run would put
    dw_ek_Borger = -1 (and no FHIR contact) on DSO for every new CPR. Ingest
    owns extract/archive/stubs; publish owns dm + sync."""
    from jobs.haiba_jobs import haiba_ingest_job, haiba_publish_job

    ingest = str(haiba_ingest_job.selection)
    publish = str(haiba_publish_job.selection)

    for group in ("haiba_extract", "haiba_archive", "haiba_borger"):
        assert group in ingest, group
        assert group not in publish, group
    for group in ("haiba_dm", "haiba_dso_sync"):
        assert group in publish, group
        assert group not in ingest, group


def test_stub_patient_asset_has_an_owning_job():
    """fhir_patient_haiba was in no job at all before the split — reachable
    only by a manual materialize, so a delivery with new CPRs never got its
    stubs created on a scheduled run."""
    from jobs.haiba_jobs import haiba_ingest_job

    assert "haiba_borger" in str(haiba_ingest_job.selection)


def test_case_rebuilds_order_after_the_stub_creation():
    """Both assets hang off arc.haiba_cases_*, so without this dep they are
    siblings and the rebuild may run BEFORE the stubs are posted."""
    from DSA.SQL.dm.haiba import dm_haiba_cases_infektion, dm_haiba_cases_ssi

    for asset in (dm_haiba_cases_infektion, dm_haiba_cases_ssi):
        assert "fhir_patient_haiba" in _deps(asset), asset.key.to_user_string()


def test_agg_rebuilds_do_not_wait_for_patients():
    """The agg tables hold no person data — making them wait on the stub asset
    would couple them to Aidbox for nothing."""
    from DSA.SQL.dm.haiba import dm_haiba_agg_infektion, dm_haiba_agg_ortopaedi

    for asset in (dm_haiba_agg_infektion, dm_haiba_agg_ortopaedi):
        assert "fhir_patient_haiba" not in _deps(asset), asset.key.to_user_string()

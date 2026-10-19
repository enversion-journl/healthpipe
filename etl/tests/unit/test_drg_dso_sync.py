# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for the DRG DSO sync, views and publish job.

The failures these guard against are quiet and severe: a config or DDL change
that lets CPR-bearing data (map clean-code minting, arc/ext contact tables with
raw CPR) reach the DSO reporting database; a sync ordered before the table's FK
targets exist on DSO (RI failure at runtime); or this repo's presentation layer
accidentally exposing the person-referable fct / demografi dim, whose DPA views
are owned by the separate security-views repo.
"""

import re
from pathlib import Path

import pytest
from DSO.load.sync_configs import DRG_SYNC_CONFIGS
from DSO.load.sync_drg import all_drg_dso_sync_assets

pytestmark = pytest.mark.unit

_DDL = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl"
_TABLES_DDL = [
    _DDL / "tables" / "drg" / "03_dims.sql",
    _DDL / "tables" / "drg" / "04_facts.sql",
    _DDL / "tables" / "drg" / "06_bridges.sql",
]
_VIEWS_DIR = _DDL / "views" / "drg"

# The seven CPR-free tables that make up the queryable DRG model on DSO.
EXPECTED = {
    ("dim", "DRGKontaktEgenskaber"),
    ("dim", "DRGKontaktKlassifikation"),
    ("dim", "DRGDiagnose"),
    ("dim", "DRGProcedure"),
    ("bridge", "DRGDiagnoseGruppe"),
    ("bridge", "DRGProcedureGruppe"),
    ("fct", "DRGKontakt"),
}


def test_map_arc_ext_and_meta_never_sync():
    """map.DRG* mints IDENTITY keys and its clean_codes derive from the contact
    (the fct uuid, incl. CPR-bearing source); arc/ext hold the raw delivery with
    CPR; meta is the pipeline register. Only the CPR-free dim/fct/bridge schemas
    may leave staging."""
    for c in DRG_SYNC_CONFIGS.values():
        assert c.schema in {"dim", "fct", "bridge"}, f"{c.table}: schema {c.schema} must not sync"
        assert (
            c.source_schema is None and c.source_table is None
        ), f"{c.table}: must sync the same-named DSA table, not read another"


def test_sync_mode_is_watermark_on_update_timestamp():
    """DRG's DSA layer is a MERGE (fct/dims) / append-only INSERT (bridges), so
    dw_timestamp_update advances only on the rows that actually changed — a
    watermark delta moves exactly those, unlike HAIBA's truncate+insert full
    rebuild. Append-only bridges never update in place, so their watermark only
    ever picks up freshly inserted membership rows."""
    for c in DRG_SYNC_CONFIGS.values():
        assert c.mode == "watermark", f"{c.table}: mode {c.mode}"
        assert c.watermark_column == "dw_timestamp_update", c.table


def _table_block(schema: str, table: str) -> str:
    for f in _TABLES_DDL:
        sql = f.read_text(encoding="utf-8")
        m = re.search(
            rf"CREATE TABLE \[{schema}\]\.\[{re.escape(table)}\]\s*\((.*?)" rf"CONSTRAINT \[PK_",
            sql,
            re.DOTALL,
        )
        if m:
            return m.group(1)
    raise AssertionError(f"no CREATE TABLE block for {schema}.{table}")


def test_no_synced_table_declares_a_cpr_column():
    """The GDPR point of the whole layer: dim/fct/bridge are CPR-free by
    construction (the fct carries dw_ek_Borger, never raw CPR), so DSO needs no
    access restriction on them. The clean_code columns are attribute/uuid
    hashes, not CPR, and are intentionally kept — so this checks CPR only.
    Verified against the DDL every synced table is created from."""
    for schema, table in EXPECTED:
        cols = _table_block(schema, table)
        assert "cpr" not in cols.lower(), f"{schema}.{table} declares a CPR-like column"


def _deps(asset) -> set[str]:
    key = next(iter(asset.keys))
    return {k.to_user_string() for k in asset.asset_deps[key]}


def _by_name() -> dict[str, set[str]]:
    return {a.key.to_user_string(): _deps(a) for a in all_drg_dso_sync_assets}


def test_every_sync_waits_for_the_dso_ddl_and_its_dsa_upstream():
    """Syncing into a table that does not exist on DSO fails at runtime, and
    syncing before the DSA rebuild has filled it publishes stale rows. Every
    sync waits for dso_infra_drg and for the DSA asset of the same name."""
    for asset in all_drg_dso_sync_assets:
        deps = _deps(asset)
        assert "dso_infra_drg" in deps, asset.key.to_user_string()
        # config key == DSA asset name; sync name is dso_sync_<lower(key)>.
        dsa = asset.key.to_user_string()[len("dso_sync_") :]
        # the DSA upstream keeps its original casing (e.g. dim_DRGDiagnose)
        assert any(d.lower() == dsa for d in deps), asset.key.to_user_string()


def test_group_bridges_sync_after_their_code_dims():
    """A bridge row's dw_ek_DRGDiagnose / dw_ek_DRGProcedure is an FK into the
    code dim; the producing-unit column FKs dim.Organisation. Sync those first
    or the linked-server upsert violates RI on DSO."""
    by = _by_name()
    assert "dso_sync_dim_drgdiagnose" in by["dso_sync_bridge_drgdiagnosegruppe"]
    assert "dso_sync_dim_drgprocedure" in by["dso_sync_bridge_drgproceduregruppe"]
    assert "dso_sync_dim_organisation" in by["dso_sync_bridge_drgproceduregruppe"]


def test_fact_syncs_last_after_all_its_fk_targets():
    """fct.DRGKontakt FKs the klassifikation + egenskaber dims, both group
    bridges, and the shared borger/organisation dims. All must be on DSO first."""
    fct = _by_name()["dso_sync_fct_drgkontakt"]
    for target in (
        "dso_sync_dim_drgkontaktklassifikation",
        "dso_sync_dim_drgkontaktegenskaber",
        "dso_sync_bridge_drgdiagnosegruppe",
        "dso_sync_bridge_drgproceduregruppe",
        "dso_sync_dim_borger",
        "dso_sync_dim_organisation",
    ):
        assert target in fct, target


def test_sync_assets_are_registered():
    from DSO.load import all_dso_sync_assets

    for asset in all_drg_dso_sync_assets:
        assert asset in all_dso_sync_assets


# --- presentation views (CPR-free) live here; DPA views live elsewhere --------


def test_cpr_free_pass_through_views_exist():
    """The three code/klassifikation code dims and both group bridges are
    CPR-free, so their presentation views are plain pass-throughs in views/drg."""
    dim_sql = (_VIEWS_DIR / "01_vw_dim.sql").read_text(encoding="utf-8")
    for table in ("DRGKontaktKlassifikation", "DRGDiagnose", "DRGProcedure"):
        assert re.search(
            rf"CREATE OR ALTER VIEW \[vw_dim\]\.\[{table}\]\s*AS\s*"
            rf"SELECT \* FROM \[dim\]\.\[{table}\];",
            dim_sql,
        ), f"vw_dim.{table} pass-through missing"

    bridge_sql = (_VIEWS_DIR / "03_vw_bridge.sql").read_text(encoding="utf-8")
    for table in ("DRGDiagnoseGruppe", "DRGProcedureGruppe"):
        assert re.search(
            rf"CREATE OR ALTER VIEW \[vw_bridge\]\.\[{table}\]\s*AS\s*"
            rf"SELECT \* FROM \[bridge\]\.\[{table}\];",
            bridge_sql,
        ), f"vw_bridge.{table} pass-through missing"


def test_no_person_or_demografi_view_is_defined_here():
    """The DPA-secured views for the person-referable fct.DRGKontakt and the
    demografi-carrying dim.DRGKontaktEgenskaber are owned by the separate
    security-views repo. This repo's presentation layer must not define them —
    a plain pass-through here would expose demografi/person data unfiltered."""
    combined = "\n".join(p.read_text(encoding="utf-8") for p in _VIEWS_DIR.glob("*.sql"))
    # Only actual view DEFINITIONS are forbidden — a bare mention in a comment
    # (e.g. documenting the vw_fct.DRGKontakt consumer join) is fine and expected.
    assert not re.search(
        r"CREATE OR ALTER VIEW\s*\[vw_fct\]", combined
    ), "views/drg must not define a fct view (DPA-secured elsewhere)"
    assert not re.search(
        r"CREATE OR ALTER VIEW\s*\[vw_\w+\]\.\[DRGKontaktEgenskaber\]", combined
    ), "views/drg must not define a view named after dim.DRGKontaktEgenskaber"
    assert not re.search(
        r"FROM\s*\[dim\]\.\[DRGKontaktEgenskaber\]", combined
    ), "views/drg must not select from dim.DRGKontaktEgenskaber (DPA-secured elsewhere)"


def test_view_asset_wraps_the_tables_without_security_infra():
    """dso_infra_views_drg builds only CPR-free pass-throughs, so it depends on
    dso_infra_drg (the tables) and must NOT pull in dso_infra_security — the
    secured views are not built here anymore."""
    from infrastructure import dso_infra_views_drg

    deps = _deps(dso_infra_views_drg)
    assert "dso_infra_drg" in deps
    assert "dso_infra_security" not in deps


def test_publish_job_owns_the_dsa_rebuild_and_sync_ingest_does_not():
    """Split like HAIBA: ingest owns extract/archive/stub-creation; publish owns
    the DSA rebuild (pre-staging -> fct) plus the DSO sync, so nothing publishes
    between a delivery's unknown-CPR stub creation and the nightly mint. The
    stub asset MUST be in an ingest job — otherwise it runs in no job and
    tier-C CPRs are never created, leaving those contacts at dw_ek_Borger = -1
    forever."""
    from jobs.drg_jobs import DRG_INGEST_JOBS, drg_publish_job

    ingest = " ".join(str(job.selection) for job in DRG_INGEST_JOBS)
    publish = str(drg_publish_job.selection)

    for asset in ("ext_drg", "ext_drgpsyk", "arc_drg_kontakt", "arc_drgpsyk_kontakt"):
        assert asset in ingest, asset
    for asset in ("fhir_patient_somatik_drg", "fhir_patient_psyk_drg"):
        assert asset in ingest, asset
    for group in (
        "drg_pre_staging",
        "drg_staging",
        "drg_map",
        "drg_dimension",
        "drg_fact",
        "drg_dso_sync",
    ):
        assert group in publish, group
        assert group not in ingest, group


def test_ingest_jobs_are_split_per_leverancetype_and_kadence():
    """A psyk failure must not be able to stop the somatic weekly work, and M/U
    must not share an ext load."""
    from jobs.drg_jobs import (
        drg_ingest_psyk_maaned_job,
        drg_ingest_somatik_maaned_job,
        drg_ingest_somatik_uge_job,
    )

    somatik_m = str(drg_ingest_somatik_maaned_job.selection)
    psyk_m = str(drg_ingest_psyk_maaned_job.selection)

    assert "ext_drg" in somatik_m and "ext_drgpsyk" not in somatik_m
    assert "ext_drgpsyk" in psyk_m and "arc_drg_kontakt" not in psyk_m

    # Same assets for both somatik cadences — only the configured cadence differs.
    assert str(drg_ingest_somatik_uge_job.selection) == somatik_m


def test_each_ingest_job_carries_its_own_checkpoint_gate():
    """Without the gate in the selection nothing advances the checkpoint, and
    every run would re-read the same delivery forever."""
    from jobs.drg_jobs import (
        drg_ingest_psyk_maaned_job,
        drg_ingest_somatik_maaned_job,
        drg_ingest_somatik_uge_job,
    )

    for job, gate in (
        (drg_ingest_somatik_maaned_job, "drg_leverance_checkpoint_drg"),
        (drg_ingest_somatik_uge_job, "drg_leverance_checkpoint_drg"),
        (drg_ingest_psyk_maaned_job, "drg_leverance_checkpoint_drgpsyk"),
    ):
        assert gate in str(job.selection), job.name


def test_freshness_runs_outside_the_ingest_jobs():
    """It must report whether or not anything loads."""
    from jobs.drg_jobs import DRG_INGEST_JOBS, drg_freshness_job

    assert "drg_freshness" in str(drg_freshness_job.selection)
    for job in DRG_INGEST_JOBS:
        assert "drg_freshness" not in str(job.selection), job.name

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contract guards for dim.Aftale as SCD Type 1.

dim.Aftale used to be the only Type 2 dimension in the ERP model, while
fct.AftaleVarePris.dw_ek_Aftale is a declared FK to a dimension whose lineage PK
is dw_ek_Aftale alone. A join on that key therefore multiplied the fact rows by
the number of versions. The history also carried almost no information: 14 of 18
superseded rows were source noise, and the remaining 4 were hoved_slut
extensions that the current row already describes.

The business key is (lnr, aftale), and where the archive holds duplicates the
newest archive row wins (greatest dw_timestamp_update). Expired AND deleted
agreements must stay, because historical purchases reference them.

These tests pin the properties whose loss would reintroduce Type 2, the fan-out,
or a soft delete of exactly the agreements that must be preserved.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ETL = Path(__file__).resolve().parents[2]
DSA_SQL = ETL / "DSA" / "SQL"
DDL = ETL / "infrastructure" / "ddl"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Source dedup: the business key is (lnr, aftale), newest archive row wins
# ---------------------------------------------------------------------------


def test_pre_stg_dedups_lafth_on_business_key():
    """Plausible bug: the lafth join is left undeduplicated. arc.ils_lafth
    upserts on prrowid alone, so several rowids can sit on the same
    (lnr, aftale) — notably when Progress recreates a record under a new rowid.
    Without dedup the INNER JOIN fans out and dim.Aftale can serve a stale
    candidate."""
    src = _read(DSA_SQL / "staging" / "pre_stg_Vareaftaler_ils.py")
    assert (
        "PARTITION BY h.lnr, h.aftale" in src
    ), "the lafth join must be deduplicated (ROW_NUMBER rn=1) on (lnr, aftale)"


def test_pre_stg_lafth_dedup_prefers_newest_archive_row():
    """Plausible bug: the tiebreaker degrades to prrowid alone (as in the fct
    chain, where only the header's EXISTENCE matters). In the dimension the
    header's VALUES matter, so the newest archive row has to win."""
    src = _read(DSA_SQL / "staging" / "pre_stg_Vareaftaler_ils.py")
    assert (
        "ORDER BY h.dw_timestamp_update DESC, h.prrowid DESC" in src
    ), "newest archive row must win; prrowid DESC is only a deterministic tiebreaker"


# ---------------------------------------------------------------------------
# dim.Aftale is Type 1
# ---------------------------------------------------------------------------


def test_dim_aftale_asset_has_no_scd2_machinery():
    """Plausible bug: the Type 2 passes survive a later edit, and the version
    history — and with it the FK fan-out — comes back."""
    src = _read(DSA_SQL / "dim" / "dim_Aftale.py")
    for forbidden in ("dw_valid_from", "dw_valid_to", "dw_is_deleted", "9999-12-31"):
        assert forbidden not in src, f"a Type 1 dimension must not reference {forbidden}"


def test_dim_aftale_asset_merges_on_entity_key():
    """Plausible bug: the MERGE matches on clean_code instead of dw_ek_Aftale,
    so the FK from fct.AftaleVarePris is no longer one-to-one."""
    src = _read(DSA_SQL / "dim" / "dim_Aftale.py")
    assert (
        "ON tgt.dw_ek_Aftale = src.dw_ek_Aftale" in src
    ), "the MERGE must match on the entity key dw_ek_Aftale"


def test_dim_aftale_asset_never_soft_deletes():
    """Plausible bug: a soft delete is reintroduced. The source removes
    agreements once they expire, so a soft-delete pass would hide exactly the
    expired agreements that historical purchase reporting needs."""
    src = _read(DSA_SQL / "dim" / "dim_Aftale.py")
    assert "Missing_Source" not in src, "the soft-delete pass must not come back"
    assert (
        "NOT MATCHED BY SOURCE" not in src
    ), "the MERGE must not delete or flag rows missing from source"


def test_dim_aftale_updates_only_on_change():
    """Plausible bug: the MERGE updates unconditionally, so dw_timestamp_update
    moves on every run and stops being a change signal — exactly the defect
    arc.ils_lafth has (645-651 of ~710 rows updated every run)."""
    src = _read(DSA_SQL / "dim" / "dim_Aftale.py")
    assert "WHEN MATCHED AND (" in src, "the MERGE needs a change guard on WHEN MATCHED"


# ---------------------------------------------------------------------------
# DDL, view, sync and lineage
# ---------------------------------------------------------------------------


def test_dim_aftale_ddl_is_type1():
    """Plausible bug: the SCD2 columns survive in the DDL, so a fresh
    environment gets a different table shape than the migrated one."""
    ddl = _read(DDL / "tables" / "erp" / "02_dims.sql")
    start = ddl.index("CREATE TABLE [dim].[Aftale](")
    block = ddl[start : ddl.index("GO", start)]
    for forbidden in ("dw_valid_from", "dw_valid_to", "dw_is_deleted"):
        assert forbidden not in block, f"dim.Aftale is Type 1 and must not have {forbidden}"
    assert (
        "PRIMARY KEY CLUSTERED ([dw_ek_Aftale] ASC)" in block
    ), "the PK must be dw_ek_Aftale alone"
    # The Type 1 rewrite replaced this table wholesale, so a column added by
    # concurrent work could silently disappear from the new shape.
    assert "[alle_maa_benytte]" in block, "the Type 1 table must keep alle_maa_benytte"


def test_vw_dim_aftale_has_no_soft_delete_filter():
    """Plausible bug: the view still filters on dw_is_deleted, which no longer
    exists — or filters expired agreements away."""
    views = _read(DDL / "views" / "erp" / "01_vw_dim.sql")
    start = views.index("CREATE OR ALTER VIEW [vw_dim].[Aftale]")
    block = views[start : views.index("GO", start)]
    assert "dw_is_deleted" not in block, "vw_dim.Aftale must not filter on dw_is_deleted"


def test_dso_sync_keys_dim_aftale_on_entity_key_alone():
    """Plausible bug: the sync config keeps the composite key
    [dw_ek_Aftale, dw_valid_from], which no longer exists."""
    src = _read(ETL / "DSO" / "load" / "sync_configs.py")
    start = src.index('"dim_Aftale": SyncTableConfig(')
    block = src[start : src.index("),", start)]
    assert "dw_valid_from" not in block, "dw_valid_from no longer exists"
    assert 'primary_key="dw_ek_Aftale"' in block, "the key is dw_ek_Aftale alone"
    # truncate_insert is the full-refresh mode kept for Type 2 dimensions. The
    # Type 1 applier maintains dw_timestamp_update as a change signal and never
    # deletes, so the delta sync every other ERP dim uses applies here too.
    assert 'mode="watermark"' in block, "Type 1 dim.Aftale syncs on a watermark"
    assert 'watermark_column="dw_timestamp_update"' in block, "the watermark is dw_timestamp_update"


def test_grain_validation_spec_for_dim_aftale_drops_the_version_column():
    """Plausible bug: the ERP grain validator keeps validating the grain as
    [dw_ek_Aftale, dw_valid_from]. The column is gone after the migration, so
    the GROUP BY throws and the check reports a permanent spurious failure —
    while the real uniqueness check on dw_ek_Aftale never runs."""
    src = _read(ETL / "scripts" / "validate_erp_grain.py")
    start = src.index("DIM_SPECS: list[GrainSpec] = [")
    block = src[start : src.index('GrainSpec(\n        "Faktura",', start)]
    assert "dw_valid_from" not in block, "dw_valid_from no longer exists"
    assert '["dw_ek_Aftale"],' in block, "the dim.Aftale grain is dw_ek_Aftale alone"


def test_dim_aftale_lineage_declares_type1():
    """Plausible bug: lineage keeps promising Type 2, so the star schema
    documents a dimension that does not exist."""
    yml = _read(ETL / "data_lineage" / "dim" / "dim_aftale.yml")
    assert "SCD: type1" in yml, "lineage must declare type1"
    for forbidden in ("dw_valid_from:", "dw_valid_to:", "dw_is_deleted:"):
        assert forbidden not in yml, f"lineage must not document {forbidden}"


# ---------------------------------------------------------------------------
# The MERGE must write every lafth enrichment column
# ---------------------------------------------------------------------------

_LAFTH_ENRICHMENT_DIM_COLUMNS = [
    "standard_leveringsdage",
    "dw_ek_Dato_sysdato",
    "dato_tid_sysdato",
    "bruger",
    "aktiv",
    "fragt_beloeb",
    "sw_fragt",
    "gebyr_beloeb",
    "sw_gebyr",
    "graense_beloeb",
    "fragt_graense_beloeb",
]


def test_merge_writes_every_lafth_enrichment_column():
    """Plausible bug: a column is dropped from the MERGE's SET or INSERT list.
    That is the only silent break in the chain — the dim column stays NULL while
    the pipeline stays green. A column missing from pre_stg, stg, the DDL or the
    migration makes the next layer fail loudly with "Invalid column name"."""
    dim = _read(DSA_SQL / "dim" / "dim_Aftale.py")
    set_block = dim[dim.index("UPDATE SET") : dim.index("WHEN NOT MATCHED BY TARGET")]
    insert_columns = dim[dim.index("INSERT (") : dim.index("VALUES (")]

    for col in _LAFTH_ENRICHMENT_DIM_COLUMNS:
        assert f"tgt.{col} = src.{col}" in set_block, f"the SET block must write {col}"
        assert f" {col}," in insert_columns, f"the INSERT column list must include {col}"

    assert (
        "dato_key('s.sysdato')" in dim
    ), "dw_ek_Dato_sysdato must be built by applying the shared dato_key helper to sysdato"


# ---------------------------------------------------------------------------
# The archive must keep preserving expired and deleted agreements
# ---------------------------------------------------------------------------


def test_ils_archives_never_detect_deletes():
    """Plausible bug: detect_deletes is switched on to "tidy up" the archive.
    The source removes agreements once they expire, so that would flag exactly
    the expired agreements as deleted — and they must be preserved, because
    historical purchases reference them."""
    src = _read(ETL / "DSI" / "mssql" / "archive" / "ils_archive_configs.py")
    assert "detect_deletes=True" not in src, (
        "the ILS archives must keep detect_deletes=False: the source removes expired "
        "agreements, and they must stay in the archive"
    )

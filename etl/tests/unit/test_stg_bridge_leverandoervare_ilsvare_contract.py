# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contract tests for stg_bridge_Leverandoervare_ILSVare.

Critical properties: (1) the pair set has NO dedup — reintroducing rn=1 would
silently recreate the information loss the bridge exists to fix; (2) the scope
table is built from HUB ENTITIES (> CP_Bridge), never from the pair set's key
values — a counterpart key must not widen the soft-delete scope (see spec §5's
(1|1)/(1|2)/(2|2) example).
"""

import pytest

pytestmark = pytest.mark.unit

from DSA.SQL.staging.stg_bridge_Leverandoervare_ILSVare import _build_pairs_sql, _build_scope_sql


def test_pair_set_is_not_deduplicated():
    sql = _build_pairs_sql(None)
    assert "INTO [stg].[bridge_Leverandoervare_ILSVare]" in sql
    assert "STRING_AGG" in sql
    assert (
        "rn = 1" not in sql.split("ils_pairs")[-1].split("STRING_AGG")[0]
    ), "no rn=1 winner-selection between pair extraction and aggregation"


def test_scope_table_reads_hub_not_pairs():
    sql = _build_scope_sql(None)
    assert "INTO [stg].[bridge_Leverandoervare_ILSVare_delta_entiteter]" in sql
    assert "[stg].[pre_Leverandoervare_ILSVare_delta_entiteter]" in sql
    assert "ils_pairs" not in sql, "scope must come from hub entities, not pair counterparts"


def test_pairs_scope_cte_reads_materialized_scope_table():
    sql = _build_pairs_sql(None)
    assert "FROM [stg].[bridge_Leverandoervare_ILSVare_delta_entiteter]" in sql
    assert (
        "[stg].[pre_Leverandoervare_ILSVare_delta_entiteter]" not in sql
    ), "pairs must not re-derive scope from the hub"


def test_checkpoint_filters_scope_output_only():
    # pairs-SQL'ens scope-CTE laeser den allerede filtrerede
    # stg.bridge_Leverandoervare_ILSVare_delta_entiteter (0 egne cp-filtre); scope-tabellen
    # anvender filteret direkte (1)
    assert _build_pairs_sql("2026-07-01").count("> '2026-07-01'") == 0
    assert _build_scope_sql("2026-07-01").count("> '2026-07-01'") == 1


def test_null_checkpoint_reads_everything():
    assert "dw_timestamp_update > '" not in _build_pairs_sql(None)
    assert "dw_timestamp_update > '" not in _build_scope_sql(None)


def test_bridge_pairs_include_alternate_lvnr_slots():
    """Spec 2026-08-27: alternative lvnr-slots skal indgaa som par-raekker med
    egne bridge_source-labels, saa bridge_kilder kan skelne primaer fra
    alternativ."""
    from DSA.SQL.staging.stg_bridge_Leverandoervare_ILSVare import _build_pairs_sql

    sql = _build_pairs_sql(None)
    for slot in ("lvnr##2", "lvnr##3", "lvnr##4"):
        assert f"alt_s.[{slot}]" in sql
    for slot in ("lvnr##2", "lvnr##3"):
        assert f"alt_d.[{slot}]" in sql
    assert "alt_d.[lvnr##4]" not in sql
    assert "'laftdstam_alt', 6" in sql
    assert "'laftd_alt', 7" in sql

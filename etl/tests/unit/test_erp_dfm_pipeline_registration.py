# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dependency-graph tests for the ERP DFM pipelines.

Guards the arc-only read contract for the migrated ERP domains (Budget, ILS
dimensions, Vare/Kreditor, Vareudlevering) plus the KMD UNSPSC apply-pass and
security sync gates: every pre-staging/staging step must depend on archive
(arc_*) assets — never on the retired partitioned ext extracts — and the DSO
syncs must wait for the assets that gate them.
"""

import pytest
from dagster import AssetKey
from definitions import defs


def _parents(key_str: str) -> set[str]:
    graph = defs.resolve_asset_graph()
    key = AssetKey.from_user_string(key_str)
    assert graph.has(key), f"Missing asset key: {key_str}"
    return {k.to_user_string() for k in graph.get(key).parent_keys}


@pytest.mark.unit
class TestBudgetPipelineGraph:
    def test_pre_stg_budget_reads_the_seven_sydax_archives(self):
        assert _parents("erp/pre_stg_Budget_sydax") == {
            "arc_budgetpost_sydax",
            "arc_budgetmodel_sydax",
            "arc_budgetpoststatus_sydax",
            "arc_budgetbeloebtype_sydax",
            "arc_finanskonto_sydax",
            "arc_finanskonto_omkostningssted_sydax",
            "arc_omkostningssted_hierarki_sydax",
        }

    def test_both_budget_stagings_read_pre_stg_only(self):
        assert _parents("erp/stg_fct_Budget_sydax") == {"erp/pre_stg_Budget_sydax"}
        assert _parents("erp/stg_dim_BudgetEgenskaber_sydax") == {"erp/pre_stg_Budget_sydax"}


@pytest.mark.unit
class TestIlsDimensionsPipelineGraph:
    def test_pre_stg_vareaftaler_reads_arc_only(self):
        parents = _parents("erp/pre_stg_Vareaftaler_ils")
        assert parents, "pre_stg_Vareaftaler_ils has no upstream deps"
        assert all(p.startswith("arc_ils_") for p in parents), parents
        assert {"arc_ils_laftd", "arc_ils_lafth", "arc_ils_lev"} <= parents

    def test_stg_dim_ilsvare_reads_hub_and_arc_lookups(self):
        parents = _parents("erp/stg_dim_ILSVare")
        assert "erp/pre_stg_Leverandoervare_ILSVare_delta_entiteter" in parents, parents
        lookups = parents - {"erp/pre_stg_Leverandoervare_ILSVare_delta_entiteter"}
        assert lookups and all(p.startswith("arc_ils_") for p in lookups), parents
        assert {"arc_ils_vareart", "arc_ils_zz_oper"} <= parents


@pytest.mark.unit
class TestVareKreditorPipelineGraph:
    def test_entity_delta_hub_reads_arc_only(self):
        parents = _parents("erp/pre_stg_Leverandoervare_ILSVare_delta_entiteter")
        assert parents and all(
            p.startswith(("arc_ils_", "arc_mysupply_")) for p in parents
        ), parents
        assert {
            "arc_ils_laftdstam",
            "arc_ils_lev",
            "arc_ils_vare",
            "arc_mysupply_statistic_invoice_line",
        } <= parents

    def test_stg_dim_kreditor_reads_the_three_source_archives(self):
        # Unified Kreditor staging reads sydax + ils + mysupply archives; the
        # invoice header is included because header-only changes (valutakode)
        # must flag the CVR for re-staging.
        assert _parents("erp/stg_dim_Kreditor") == {
            "arc_sydax_kreditor",
            "arc_ils_lev",
            "arc_mysupply_metadata",
            "arc_mysupply_statistic_invoice_header",
        }


@pytest.mark.unit
class TestVareudleveringPipelineGraph:
    def test_pre_stg_reads_the_udlev_archive_only(self):
        assert _parents("erp/pre_stg_Vareudlevering_ils") == {"arc_ils_udlev"}

    def test_stg_fct_reads_pre_stg_and_lev_archive(self):
        assert _parents("erp/stg_fct_Vareudlevering_ils") == {
            "erp/pre_stg_Vareudlevering_ils",
            "arc_ils_lev",
        }

    def test_stg_dim_reads_pre_stg_and_lookups(self):
        assert _parents("erp/stg_dim_Vareudlevering_ils") == {
            "erp/pre_stg_Vareudlevering_ils",
            "arc_ils_projekt",
            "arc_ils_lgrvare",
            "arc_ils_varetype2",
        }


@pytest.mark.unit
class TestKmdUnspscPipelineGraph:
    def test_apply_pass_depends_on_ref_match(self):
        assert "erp/ref_kmd_fakturalinje_unspsc_match" in _parents("fct_apply_fakturalinje_unspsc")

    def test_fakturalinje_dso_sync_waits_for_apply_pass(self):
        # Syncing fct.Fakturalinje before the UNSPSC apply-pass would ship
        # unenriched rows to DSO.
        assert "fct_apply_fakturalinje_unspsc" in _parents("dso_sync_fct_fakturalinje")


@pytest.mark.unit
class TestSecurityPipelineGraph:
    def test_security_syncs_wait_for_populate(self):
        for sync in ("dso_sync_sec_rolledomaene", "dso_sync_sec_rborganisation"):
            assert _parents(sync) == {"dso_infra_security", "populate_security"}, sync

    def test_populate_consumes_extract(self):
        assert _parents("populate_security") == {
            "extract_security",
            "staging_infra_security",
            "cs_sor",
        }

    def test_populate_depends_on_sor_hierarchy_source(self):
        """The expansion reads codesystem.SORHierarchy, which cs_sor owns."""
        assert "cs_sor" in _parents("populate_security")

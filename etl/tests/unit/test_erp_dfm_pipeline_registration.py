# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dependency-graph guards for the ERP DFM pipelines.

Guards the arc-only read contract for the migrated ERP domains (Budget, ILS
dimensions, Vare/Kreditor, Vareudlevering) plus the KMD UNSPSC apply-pass and
security sync gates: every pre-staging/staging step must depend on archive
(arc_*) assets — never on the retired partitioned ext extracts — and the DSO
syncs must wait for the assets that gate them.

Reading the wrong upstream fails *silently*: the pipeline stays green and the
data is stale or unenriched. That is why these guards exist (see the carve-out
in docs/testing-guidelines.md).

Every check below is a **rule plus a subset**, never an exact parent set: an
exact set fails on every legitimate upstream addition and can never fail for
an asset added tomorrow. The rule states the contract ("reads archives only");
the subset pins the specific upstreams whose absence would be a real fault.
"""

import pytest
from dagster import AssetKey
from definitions import defs

ARC = ("arc_",)
ARC_ILS = ("arc_ils_",)


def _parents(key_str: str) -> set[str]:
    graph = defs.resolve_asset_graph()
    key = AssetKey.from_user_string(key_str)
    assert graph.has(key), f"Missing asset key: {key_str}"
    return {k.to_user_string() for k in graph.get(key).parent_keys}


def _assert_reads_only(key_str: str, prefixes: tuple[str, ...], required: set[str]) -> None:
    """Assert every upstream of key_str matches prefixes, and required ones are present.

    The non-empty assertion is the vacuous-pass guard: an asset that resolves
    to zero parents would otherwise satisfy `all(...)` trivially and stay green.
    """
    parents = _parents(key_str)
    assert parents, f"{key_str} has no upstream deps — wiring or naming changed"
    offenders = sorted(p for p in parents if not p.startswith(prefixes))
    assert not offenders, f"{key_str} reads non-{prefixes} upstreams: {offenders}"
    missing = sorted(required - parents)
    assert not missing, f"{key_str} no longer reads: {missing}"


@pytest.mark.unit
class TestBudgetPipelineGraph:
    def test_pre_stg_budget_reads_the_seven_sydax_archives(self):
        _assert_reads_only(
            "erp/pre_stg_Budget_sydax",
            ARC,
            {
                "arc_sydax_budgetpost",
                "arc_sydax_budgetmodel",
                "arc_sydax_budgetpoststatus",
                "arc_sydax_budgetbeloebtype",
                "arc_sydax_finanskonto",
                "arc_sydax_finanskonto_omkostningssted",
                "arc_sydax_omkostningssted_hierarki",
            },
        )
        # A dep naming a key that is not a materializable asset is never waited
        # for: Budget would run before its archives are refreshed, and nothing fails.
        graph = defs.resolve_asset_graph()
        unmaterializable = sorted(
            p
            for p in _parents("erp/pre_stg_Budget_sydax")
            if not graph.get(AssetKey.from_user_string(p)).is_materializable
        )
        assert not unmaterializable, unmaterializable

    def test_both_budget_stagings_read_pre_stg_only(self):
        # Staging must go through pre-staging, never straight to the archives.
        for staging in ("erp/stg_fct_Budget_sydax", "erp/stg_dim_BudgetEgenskaber_sydax"):
            _assert_reads_only(staging, ("erp/pre_stg_",), {"erp/pre_stg_Budget_sydax"})


@pytest.mark.unit
class TestIlsDimensionsPipelineGraph:
    def test_pre_stg_vareaftaler_reads_arc_only(self):
        _assert_reads_only(
            "erp/pre_stg_Vareaftaler_ils",
            ARC_ILS,
            {"arc_ils_laftd", "arc_ils_lafth", "arc_ils_lev"},
        )

    def test_stg_dim_ilsvare_reads_hub_and_arc_lookups(self):
        hub = "erp/pre_stg_Leverandoervare_ILSVare_delta_entiteter"
        parents = _parents("erp/stg_dim_ILSVare")
        assert hub in parents, parents
        lookups = parents - {hub}
        assert lookups, "stg_dim_ILSVare has no arc lookups — wiring changed"
        assert all(p.startswith(ARC_ILS) for p in lookups), parents
        assert {"arc_ils_vareart", "arc_ils_zz_oper"} <= parents

    def test_ilsbruger_chain_reads_arc_and_runs_stg_map_dim_dso(self):
        _assert_reads_only("erp/stg_dim_ILSBruger", ARC_ILS, {"arc_ils_zz_oper", "arc_ils_sgh"})
        assert "erp/stg_dim_ILSBruger" in _parents("map_ILSBruger")
        assert {"erp/stg_dim_ILSBruger", "map_ILSBruger"} <= _parents("dim_ILSBruger")
        assert "dim_ILSBruger" in _parents("dso_sync_dim_ilsbruger")


@pytest.mark.unit
class TestVareKreditorPipelineGraph:
    def test_entity_delta_hub_reads_arc_only(self):
        _assert_reads_only(
            "erp/pre_stg_Leverandoervare_ILSVare_delta_entiteter",
            ("arc_ils_", "arc_mysupply_"),
            {
                "arc_ils_laftdstam",
                "arc_ils_lev",
                "arc_ils_vare",
                "arc_mysupply_statistic_invoice_line",
            },
        )

    def test_stg_dim_kreditor_reads_the_source_archives(self):
        # Unified Kreditor staging reads sydax + ils + mysupply archives. The
        # invoice header is required because header-only changes (valutakode)
        # must flag the CVR for re-staging.
        _assert_reads_only(
            "erp/stg_dim_Kreditor",
            ARC,
            {
                "arc_sydax_kreditor",
                "arc_ils_lev",
                "arc_mysupply_metadata",
                "arc_mysupply_statistic_invoice_header",
            },
        )


@pytest.mark.unit
class TestVareudleveringPipelineGraph:
    def test_pre_stg_reads_the_udlev_archive_only(self):
        _assert_reads_only("erp/pre_stg_Vareudlevering_ils", ARC_ILS, {"arc_ils_udlev"})

    def test_stg_fct_reads_pre_stg_and_arc_only(self):
        _assert_reads_only(
            "erp/stg_fct_Vareudlevering_ils",
            ("erp/pre_stg_", "arc_ils_"),
            {"erp/pre_stg_Vareudlevering_ils", "arc_ils_lev"},
        )

    def test_stg_dim_reads_pre_stg_and_arc_lookups(self):
        _assert_reads_only(
            "erp/stg_dim_Vareudlevering_ils",
            ("erp/pre_stg_", "arc_ils_"),
            {
                "erp/pre_stg_Vareudlevering_ils",
                "arc_ils_projekt",
                "arc_ils_lgrvare",
                "arc_ils_varetype2",
            },
        )


@pytest.mark.unit
class TestOrdrePipelineGraph:
    def test_pre_stg_reads_the_ordh_archive_only(self):
        assert _parents("erp/pre_stg_Ordre_ils") == {"arc_ils_ordh"}

    def test_stagings_read_pre_stg_and_the_user_archive(self):
        assert _parents("erp/stg_dim_Ordre_ils") == {"erp/pre_stg_Ordre_ils", "arc_ils_zz_oper"}
        assert _parents("erp/stg_fct_Ordre_ils") == {"erp/pre_stg_Ordre_ils"}

    def test_dim_waits_for_map_and_its_ilsbruger_outrigger(self):
        assert "erp/stg_dim_Ordre_ils" in _parents("map_Ordre")
        parents = _parents("dim_Ordre")
        assert {"erp/stg_dim_Ordre_ils", "map_Ordre", "map_ILSBruger"} <= parents
        # The creator is resolved from the archive in staging, never from dim.ILSBruger.
        assert "dim_ILSBruger" not in parents

    def test_fct_waits_for_the_maps_it_resolves(self):
        assert {
            "erp/stg_fct_Ordre_ils",
            "map_Ordre",
            "map_Lager",
            "map_ILSLeverandoer",
        } <= _parents("fct_Ordre")

    def test_fct_dso_sync_waits_for_its_dimensions(self):
        assert {
            "fct_Ordre",
            "dso_sync_dim_ordre",
            "dso_sync_dim_lager",
            "dso_sync_dim_ilsleverandoer",
        } <= _parents("dso_sync_fct_ordre")


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
            assert {"dso_infra_security", "populate_security"} <= _parents(sync), sync

    def test_populate_consumes_extract_and_sor_hierarchy(self):
        # cs_sor owns codesystem.SORHierarchy, which the expansion reads.
        assert {
            "extract_security",
            "staging_infra_security",
            "cs_sor",
        } <= _parents("populate_security")

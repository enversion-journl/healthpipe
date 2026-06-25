# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
PipelineRunner: execute production job selections for execution-order tests.

Materialises the same assets as the production ``erp_daily_etl``,
``erp_monthly_etl`` and ``erp_full_load_etl`` job selections. The key lists
below are hand-maintained constants (``dg.materialize`` needs partitioned
and unpartitioned assets in separate calls, so the job selections cannot be
used verbatim) — ``tests/unit/test_erp_execution_order_runner_drift.py``
asserts they stay in sync with ``jobs/erp_jobs.py``.

Excluded on purpose: DSO sync (ordering gaps surface upstream of DSO),
ILS-source assets (different source system), and the allowance-charge
extracts (stale schema in the local devcontainer source DB).
"""

from __future__ import annotations

import logging
from typing import Any

import dagster as dg
from tests.utils.materialize_helpers import materialize_assets_with_dependencies

logger = logging.getLogger(__name__)

# ── Asset key lists ──────────────────────────────────────────────────
# Derived from the production job group selections in ``jobs/erp_jobs.py``.
# Kept here as constants so the runner can materialise them in two phases
# (partitioned extract → unpartitioned downstream) which is required by
# ``dg.materialize`` (cannot mix partitioned & unpartitioned in one call).

# NOTE: each extraction config is its own asset — the header/metadata/line
# extracts must all be listed; materializing only one does not pull in the
# others. The allowance_charge extracts are deliberately excluded: the local
# devcontainer source DB has a stale schema for those tables (missing the
# GebyrKategori* columns staging reads) and the conftest provides empty ext
# stubs, which the staging LEFT JOINs tolerate.
EXTRACT_DAILY_DRIVER_KEYS: list[str] = [
    "ext_statistic_invoice_header_mysupply_daily",
    "ext_metadata_mysupply_daily",
    "ext_statistic_invoice_line_mysupply_daily",
    "ext_kreditor_sydax_daily",
    "ext_finanskonto_sydax_daily",
]

EXTRACT_MONTHLY_DRIVER_KEYS: list[str] = [
    "ext_statistic_invoice_header_mysupply_monthly",
    "ext_metadata_mysupply_monthly",
    "ext_statistic_invoice_line_mysupply_monthly",
    "ext_kreditor_sydax_monthly",
    "ext_finanskonto_sydax_monthly",
]

EXTRACT_FULL_LOAD_KEYS: list[str] = [
    "ext_statistic_invoice_header_mysupply_full_load",
    "ext_metadata_mysupply_full_load",
    "ext_statistic_invoice_line_mysupply_full_load",
    "ext_kreditor_sydax_full_load",
    "ext_finanskonto_sydax_full_load",
]

ENRICHED_DAILY_KEYS: list[str] = [
    "ext_finanspostering_enriched_sydax_daily",
    "ext_finanskladdelinje_enriched_sydax_daily",
    "ext_finanskladdelinje_sydax_daily",
    "ext_finanskladde_sydax_daily",
    "ext_budgetpost_enriched_sydax_daily",
]

ENRICHED_MONTHLY_KEYS: list[str] = [
    "ext_finanspostering_enriched_sydax_monthly",
    "ext_finanskladdelinje_enriched_sydax_monthly",
    "ext_finanskladdelinje_sydax_monthly",
    "ext_finanskladde_sydax_monthly",
    "ext_budgetpost_enriched_sydax_monthly",
]

ENRICHED_FULL_LOAD_KEYS: list[str] = [
    "ext_finanspostering_enriched_sydax_full_load",
    "ext_finanskladdelinje_enriched_sydax_full_load",
    "ext_finanskladdelinje_sydax_full_load",
    "ext_finanskladde_sydax_full_load",
    "ext_budgetpost_enriched_sydax_full_load",
]

DOWNSTREAM_KEYS: list[str] = [
    # Pre-staging
    "erp/pre_stg_fakturalinje_mysupply",
    "erp/pre_stg_postering_sydax",
    "erp/pre_stg_finanskladdelinje_sydax",
    # Dim staging
    "erp/stg_dim_kreditor_mysupply",
    "erp/stg_dim_kreditor_sydax",
    "erp/stg_dim_kontoplan_sydax",
    "erp/stg_dim_postering_sydax",
    "erp/stg_dim_faktura_mysupply",
    "erp/stg_dim_fakturalinje_mysupply",
    "erp/stg_dim_vare_mysupply",
    "erp/stg_dim_vare",
    "stg_dim_ean",
    "stg_okosted",
    "erp/stg_dim_finanskladde_sydax",
    "erp/stg_dim_finanskladdelinje_sydax",
    "erp/stg_dim_budget_egenskaber_sydax",
    # Fact staging
    "erp/stg_fct_postering_sydax",
    "erp/stg_fct_fakturalinje_mysupply",
    "erp/stg_fct_faktura_mysupply",
    "erp/stg_fct_budget_sydax",
    "erp/stg_fct_finanskladdelinje_sydax",
    # Maps
    "map_kreditor",
    "map_kontoplan",
    "map_vare",
    "map_faktura",
    "map_fakturalinje",
    "map_postering",
    "map_ean",
    "map_okosted",
    "map_finanskladde",
    "map_finanskladdelinje",
    "map_budget_egenskaber",
    "map_budget",
    # Dimensions
    "dim_kreditor",
    "dim_kontoplan",
    "dim_vare",
    "dim_faktura",
    "dim_fakturalinje",
    "dim_postering",
    "dim_ean",
    "dim_finanskladde",
    "dim_finanskladdelinje",
    "dim_budget_egenskaber",
    # Facts
    "fct_postering",
    "fct_faktura",
    "fct_fakturalinje",
    "fct_budget",
    "fct_finanskladdelinje",
    # Bridge + apply-pass
    "ref_faktura_to_postering",
    "fct_apply_faktura_postering_relations",
]


class PipelineRunner:
    """Execute production ERP pipeline phases against the devcontainer DW."""

    def __init__(
        self,
        instance: dg.DagsterInstance,
        resources: dict[str, Any],
    ) -> None:
        self.instance = instance
        self.resources = resources

    # ── Public run modes ──────────────────────────────────────────────

    def run_daily(self, date_str: str) -> None:
        """Execute one full daily pipeline run for *date_str*."""
        logger.info("=== run_daily(%s) ===", date_str)

        self._materialize(
            EXTRACT_DAILY_DRIVER_KEYS,
            partition=date_str,
            label="daily extract (base)",
        )
        self._materialize(
            ENRICHED_DAILY_KEYS,
            partition=date_str,
            label="daily extract (enriched)",
        )
        self._materialize(DOWNSTREAM_KEYS, label="downstream")

    def run_monthly(self, month_str: str) -> None:
        """Execute one full monthly pipeline run for *month_str* (YYYY-MM)."""
        logger.info("=== run_monthly(%s) ===", month_str)

        self._materialize(
            EXTRACT_MONTHLY_DRIVER_KEYS,
            partition=month_str,
            label="monthly extract (base)",
        )
        self._materialize(
            ENRICHED_MONTHLY_KEYS,
            partition=month_str,
            label="monthly extract (enriched)",
        )
        self._materialize(DOWNSTREAM_KEYS, label="downstream")

    def run_full_load(self) -> None:
        """Execute the full-load pipeline (no partition filter)."""
        logger.info("=== run_full_load() ===")

        self._materialize(
            EXTRACT_FULL_LOAD_KEYS,
            label="full-load extract (base)",
        )
        self._materialize(
            ENRICHED_FULL_LOAD_KEYS,
            label="full-load extract (enriched)",
        )
        self._materialize(DOWNSTREAM_KEYS, label="downstream")

    def run_assets(
        self,
        asset_keys: list[str],
        partition: str | None = None,
    ) -> None:
        """Escape hatch: materialize specific assets in given order.

        Used by adversarial B-probes to materialize assets in deliberately
        wrong orders.
        """
        logger.info("=== run_assets(%s, partition=%s) ===", asset_keys, partition)
        self._materialize(asset_keys, partition=partition, label="custom")

    # ── Internal ──────────────────────────────────────────────────────

    def _materialize(
        self,
        asset_keys: list[str],
        partition: str | None = None,
        label: str = "",
    ) -> None:
        logger.info("Materializing [%s]: %d assets", label, len(asset_keys))

        result = materialize_assets_with_dependencies(
            instance=self.instance,
            asset_keys=asset_keys,
            partition=partition,
            resources=self.resources,
            raise_on_error=True,
        )

        if result is None or not result.success:
            raise RuntimeError(
                f"Pipeline phase [{label}] failed. "
                f"Partition={partition}, keys={asset_keys[:5]}..."
            )
        logger.info("Phase [%s] completed successfully", label)

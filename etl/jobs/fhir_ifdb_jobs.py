# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR maintenance job definitions for IFDB data.

The partition-based daily/monthly/full-load jobs are gone: every IFDB FHIR
resource is produced on the DFM path, so the whole pipeline runs as the single
``ifdb_dfm_daily_job`` (see ``jobs/ifdb_dfm_jobs.py``), and backfills are driven
by ``RunModeConfig`` (``mode=full_load`` / ``period`` / ``ids``) instead of by
partition keys.

What remains here is the SFI_datapoint reconciliation job, which is manual and
periodic rather than part of the nightly chain.
"""

import os

import dagster as dg


def _max_concurrent(env_name: str, default: int) -> int:
    """Resolve a per-run asset concurrency cap from the environment.

    The default multiprocess_executor uses ``multiprocessing.cpu_count()``,
    which on 16-core pods would run 16 assets in parallel — and each heavy
    asset (e.g. AKTIVITETSRESULTAT) spawns its own ProcessPool for XML parsing.
    Combined, that oversubscribed the pods' memory budget and triggered
    SIGKILL/OOM on XML workers. Callers pass a conservative default and can
    override it per cadence, or globally via ``FHIR_IFDB_MAX_CONCURRENT``.
    """
    return int(
        os.environ.get(
            env_name,
            os.environ.get("FHIR_IFDB_MAX_CONCURRENT", str(default)),
        )
    )


_sfi_reconciliation_executor = dg.multiprocess_executor.configured({"max_concurrent": 1})

# ============================================================================
# Maintenance / Reconciliation Jobs
# ============================================================================

sfi_datapoint_reconciliation_assets = dg.AssetSelection.assets("dso_reconcile_sfi_datapoint_store")

sfi_datapoint_reconciliation_job = dg.define_asset_job(
    name="sfi_datapoint_reconciliation_job",
    selection=sfi_datapoint_reconciliation_assets,
    executor_def=_sfi_reconciliation_executor,
    description=(
        "Manual/periodic batched reconciliation of fhir.SFI_datapoint_store. Use this "
        "after full-load/backfill upstream runs to rebuild historical SFI datapoints."
    ),
)

__all__ = [
    "sfi_datapoint_reconciliation_job",
]

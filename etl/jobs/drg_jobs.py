# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dagster jobs for the DRG-grupperet LPR pipeline.

Ingestion is split three ways, and each RUN handles ONE takstsystem. The split
is not cosmetic (see
docs/source_systems/drg/drg_leverancedetektion_beslutningsnotat.md):

* **Cadence.** A monthly (M) and a weekly (U) delivery for the same
  takstsystem collide in ext — they share KONTAKT_VAERDI for the overlapping
  activity years, which guard_key_uniqueness correctly fails. Separate jobs
  give two ext->arc cycles per night instead of a backlog spread over days.
* **Leverancetype.** Somatik and psyk come from different files and write to
  different ext tables, so nothing forces them to share a fate. In one job a
  psyk failure would make the RUN fail, and a run-status sensor would then
  never reach the somatic U work — psyk would block somatik through the run
  status even though the asset graph kept them apart.
* **Takstsystem.** One per run, so a schema change in a new takstsystem fails
  only that takstsystem, and each one's checkpoint commits independently.

Automation is one serialised queue per leverancetype: a daily schedule builds
the queue and requests its head, and a run-status sensor requests the next
entry when the previous run succeeds (etl/schedules/drg_schedules.py,
etl/sensors/drg_queue_sensor.py). Because U entries sit after all M entries,
a failed M stops the queue before U is reached — the gating is a property of
the queue, not a separate trigger.

``drg_publish_job`` is not on that clock: ``sensors/drg_publish_sensor.py``
fires it on ``ifdb_dfm_daily_job`` SUCCESS, after the chain has minted
dw_ek_Borger for the ingest queue's stub Patients. Ungated — the publish is
delta, so a night without a delivery is a no-op.

No DQA subjects — the source is files, not a queryable system. Control totals
come from SDP_ExportLog (hard fail in extract); the staleness alarm is its own
asset (drg_leverance_freshness) deliberately outside the load path.
"""

import dagster as dg
from DSI.csv_files.drg_datasets import DATASETS


def _ingest_selection(leverancetype: str, ext_asset: str, borger_asset: str) -> dg.AssetSelection:
    """Everything one leverancetype's ingest run touches.

    Derived from DATASETS rather than listed, so a new archive table is picked
    up without anyone remembering to add it here.
    """
    arc = [f"arc_{d.ext_table}" for d in DATASETS if d.leverancetype == leverancetype]
    gate = f"drg_leverance_checkpoint_{leverancetype.lower()}"
    return dg.AssetSelection.assets(ext_asset, *arc, gate, borger_asset)


_INGEST_DESCRIPTION = (
    "Load one takstsystem's {kadence} delivery into ext, MERGE the year slices "
    "that carry newer data into arc, create stub Patients for unknown CPRs, and "
    "advance the delivery checkpoint. A run with no new delivery is a fast "
    "no-op: ext is not even recreated."
)

drg_ingest_somatik_maaned_job = dg.define_asset_job(
    name="drg_ingest_somatik_maaned_job",
    selection=_ingest_selection("DRG", "ext_drg", "fhir_patient_somatik_drg"),
    # Op keys stay unpostfixed on a feature deployment; only asset keys carry the postfix.
    config=dg.RunConfig(ops={"ext_drg": {"config": {"kadence": "M"}}}),
    description=_INGEST_DESCRIPTION.format(kadence="monthly (M)"),
)

drg_ingest_somatik_uge_job = dg.define_asset_job(
    name="drg_ingest_somatik_uge_job",
    selection=_ingest_selection("DRG", "ext_drg", "fhir_patient_somatik_drg"),
    config=dg.RunConfig(ops={"ext_drg": {"config": {"kadence": "U"}}}),
    description=_INGEST_DESCRIPTION.format(kadence="weekly (U)"),
)

drg_ingest_psyk_maaned_job = dg.define_asset_job(
    name="drg_ingest_psyk_maaned_job",
    selection=_ingest_selection("DRGPSYK", "ext_drgpsyk", "fhir_patient_psyk_drg"),
    config=dg.RunConfig(ops={"ext_drgpsyk": {"config": {"kadence": "M"}}}),
    description=_INGEST_DESCRIPTION.format(kadence="monthly (M)"),
)

drg_freshness_job = dg.define_asset_job(
    name="drg_freshness_job",
    selection=dg.AssetSelection.groups("drg_freshness"),
    description=(
        "Check the delivery share for a stale series. Its own job on purpose: "
        "it must report whether or not anything loads, so a stale feed cannot "
        "block ingestion and a failing ingest cannot hide a stale feed."
    ),
)

drg_publish_job = dg.define_asset_job(
    name="drg_publish_job",
    selection=dg.AssetSelection.groups(
        "drg_pre_staging",
        "drg_staging",
        "drg_map",
        "drg_dimension",
        "drg_fact",
        "drg_dso_sync",
    ),
    description=(
        "Rebuild the DRG DSA layer from arc (pre-staging -> staging -> map key "
        "minting -> code/klassifikation/egenskaber dims + group-key maps and "
        "bridges -> fct) and sync the CPR-free dim/fct/bridge tables to DSO. "
        "Delta-scoped off the DFM checkpoint plus the contacts still at "
        "dw_ek_Borger = -1 (last 14 days), which every run re-resolves against "
        "the current dim.Borger. Triggered by drg_publish_sensor on "
        "ifdb_dfm_daily_job SUCCESS, so unknown-CPR stubs created on ingest "
        "have a dw_ek_Borger before publish."
    ),
)

DRG_INGEST_JOBS = [
    drg_ingest_somatik_maaned_job,
    drg_ingest_somatik_uge_job,
    drg_ingest_psyk_maaned_job,
]

__all__ = [
    "DRG_INGEST_JOBS",
    "drg_freshness_job",
    "drg_ingest_psyk_maaned_job",
    "drg_ingest_somatik_maaned_job",
    "drg_ingest_somatik_uge_job",
    "drg_publish_job",
]

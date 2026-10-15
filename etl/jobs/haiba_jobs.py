# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dagster jobs for the HAIBA delivery pipeline, split in two at the point where
the chain leaves this domain.

``haiba_ingest_job``   extract -> archive -> stub patients (Aidbox)
``haiba_publish_job``  dm rebuild + contact stamp -> DSO sync

Why two and not one
-------------------
A HAIBA case resolves its dw_ek_Borger through ref.PatientToBorger, and for a
CPR the FHIR server has never seen that key does not exist yet. The ingest job
creates a stub Patient in Aidbox for those (DSA/FHIR/Patient/Patient_HAIBA.py),
but the projection Aidbox -> fhir.Patient -> ref.PatientToBorger is owned by the
nightly DFM pipeline: ifdb_dfm_daily_job at 06:00.

Running the whole thing as one job would therefore publish the new cases with
dw_ek_Borger = -1 (and match_kvalitet_* = 'ingen_borger', so no FHIR contact
either) on the very run that creates the stubs. Splitting keeps that unresolved
window inside staging: nothing publishes between the stub creation and the
nightly mint, so the -1 never reaches DSO.

Cadence
-------
The ingest job is scheduled daily at 05:00 CET (schedules/haiba_schedules.py),
one hour before the DFM base job, so a delivery that landed overnight has its
stubs in Aidbox before the chain that projects them runs. Re-running it against
a delivery already in the archive is a data no-op — the MERGE has
update_only_on_change with leverance_dato excluded from the change columns, the
delivery register upserts its row, and the stub creation resolves against
Aidbox and posts nothing. It is not free, though: the newest file per HAI type
is re-read and re-staged every run.

The publish job is triggered by haiba_publish_sensor after the nightly chain
has minted the keys — once for each new delivery, and again on later nights
while cases are still resolving from dw_ek_Borger = -1 to a real citizen. That
recovery works because the dm layer is a full rebuild from arc with stable
dw_ek from the map layer: every run re-resolves every row against the current
ref.PatientToBorger, so a case fixes itself in place rather than needing a
backfill. Running the job by hand does the same thing and is always safe.
"""

import dagster as dg

haiba_ingest_job = dg.define_asset_job(
    name="haiba_ingest_job",
    selection=dg.AssetSelection.groups("haiba_extract", "haiba_archive", "haiba_borger"),
    description=(
        "Load the newest HAIBA delivery per HAI type into ext.haiba_*, MERGE "
        "into the arc.haiba_* archive, and create stub Patient resources in "
        "Aidbox for CPRs FHIR does not know. Stops before the dm layer: the "
        "stubs only become a dw_ek_Borger once the nightly DFM chain has "
        "projected them."
    ),
)

haiba_publish_job = dg.define_asset_job(
    name="haiba_publish_job",
    selection=dg.AssetSelection.groups("haiba_dm", "haiba_dso_sync"),
    description=(
        "Rebuild the HAIBA dm layer (map mint, facts, egenskaber, contact "
        "relation) from arc and sync the eight dm tables to DSO. A full "
        "rebuild, so running it re-resolves dw_ek_Borger for every case "
        "against the current ref.PatientToBorger."
    ),
)

__all__ = ["haiba_ingest_job", "haiba_publish_job"]

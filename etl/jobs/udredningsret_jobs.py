# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dagster jobs for the udredningsret pipeline (SDS LPR_Udredningsret), split in two
at the point where the chain leaves the archive.

``udredningsret_ingest_job``   extract -> archive -> stub patients (FHIR)
``udredningsret_publish_job``  pre-staging -> staging -> map -> dim/fct -> DSO sync

Ingest
------

Delivery detection (delivery-detection pattern, level 1) makes the job cheap to run
daily: ``ext_udredningsret`` lists the share, compares the newest filename's
packaging timestamp with the DFM delivery checkpoint
(``meta.DFMState`` ('dfm', 'udredningsret_leverance_1083')) and reads NOTHING
when no newer file exists — it writes an empty manifest, the archive asset
short-circuits on it, and the gate asset leaves the checkpoint alone. A day
without a new delivery is therefore a few metadata queries and a directory
listing, and reports SUCCESS. When a new file has landed the full chain runs
and the gate stamps the checkpoint last.

``udredningsret_leverance_alder`` is in the same job but is NOT a dependency of
the load: a stale share alarms without blocking whatever has actually arrived.

``udredningsret_borger`` (``fhir_patient_udredningsret``) runs last in the same
job and creates stub Patient resources in FHIR for CPRs the deliveries carry
but the FHIR server has never seen. Like HAIBA and DRG, the stubs are created
on ingest but only become a real dw_ek_Borger once the nightly DFM chain has
minted them — which is why staging/publishing is a separate job.

No DQA subjects in ingest — the source is files. Control totals come from
SDP_ExportLog (hard fail in the extract); the remaining guards run inline in
the archive.

Publish, and why it is a separate job
-------------------------------------
A udredningsforloeb resolves its dw_ek_Borger through dim.Borger, and for a
CPR the FHIR server has never seen that key does not exist yet. The ingest job
creates a stub Patient for those (``udredningsret_borger``), but the projection
FHIR -> fhir.Patient -> ref.PatientToBorger -> dim.Borger is owned by the nightly
DFM chain. Running ingest and publish as one job would therefore publish the new
forloeb with dw_ek_Borger = -1 on the very run that creates the stubs — the same
reason DRG (jobs/drg_jobs.py) and HAIBA (jobs/haiba_jobs.py) are split. Keeping
publish separate holds that unresolved window inside staging: nothing publishes
between the stub creation and the nightly mint.

``udredningsret_publish_job`` runs the groups ``udredningsret_pre_staging``,
``udredningsret_staging``, ``udredningsret_map``, ``udredningsret_dimension``,
``udredningsret_fact`` and — added by US-6, spec section 8 in
docs/source_systems/udredningsret/udredningsret_model.md —
``udredningsret_dso_sync``, which moves the CPR-free dim/fct tables to DSO. US-5's
marker map and fact join the existing ``udredningsret_map`` and ``udredningsret_fact``
groups and therefore need no change here. It runs delta by
default: stg.pre_Helbredsforloeb scopes the run to helbredsforloeb touched after
the MIN downstream checkpoint, plus every helbredsforloeb whose live fact rows
still carry dw_ek_Borger = -1 (decision U9), so a later run after the nightly
mint repairs the borger key in place without a full rebuild. Running it by hand
is always safe: the map layer keeps dw_ek stable and the loaders MERGE.
"""

import dagster as dg

udredningsret_ingest_job = dg.define_asset_job(
    name="udredningsret_ingest_job",
    selection=dg.AssetSelection.groups(
        "udredningsret_extract", "udredningsret_archive", "udredningsret_borger"
    ),
    description=(
        "Load the newest udredningsret delivery (SDS LPR_Udredningsret, weekly full "
        "snapshot) that is newer than the DFM delivery checkpoint into ext, MERGE it into "
        "arc.udredningsret_mont on business keys (helbredsforloeb, markoer, loebenummer) "
        "with soft-delete over the whole snapshot, stamp the checkpoint, and create stub "
        "Patient resources in FHIR for unknown CPRs (udredningsret_borger). A run without "
        "a new file reads nothing. The stubs only become a real dw_ek_Borger once the "
        "nightly DFM chain has minted them, so staging/dim/fct (US-3–US-5) and DSO (US-6) "
        "follow in their own jobs."
    ),
)

udredningsret_publish_job = dg.define_asset_job(
    name="udredningsret_publish_job",
    # US-3, US-4, US-5 and US-6 groups. Each group is added in the US that
    # defines it: tests/unit/test_asset_selection_groups_exist.py refuses a
    # selection that names a group no asset defines yet, because such a selection
    # resolves silently empty. Order inside the selection is cosmetic — the run
    # order is the asset graph's (staging -> map -> dim/fct -> DSO sync).
    selection=dg.AssetSelection.groups(
        "udredningsret_pre_staging",
        "udredningsret_staging",
        "udredningsret_map",
        "udredningsret_dimension",
        "udredningsret_fact",
        "udredningsret_dso_sync",
    ),
    description=(
        "Rebuild the udredningsret DSA layer from arc.udredningsret_mont (scope table "
        "stg.pre_Helbredsforloeb -> pre/stg staging -> map key minting -> "
        "Forloebsmarkoer/klassifikation/egenskaber dims -> fct.Udredningsforloeb -> "
        "fct.UdredningsretMarkoer) and sync the CPR-free dim/fct tables to DSO "
        "(US-6). Delta "
        "by default, with borger recovery for forloeb still at dw_ek_Borger = -1 and "
        "stable dw_ek from the map layer. Split from udredningsret_ingest_job for the "
        "same reason as DRG and HAIBA: unknown-CPR stubs created on ingest only become "
        "a real dw_ek_Borger once the nightly DFM chain has minted them, so publishing "
        "is deferred until after that mint."
    ),
)

__all__ = ["udredningsret_ingest_job", "udredningsret_publish_job"]

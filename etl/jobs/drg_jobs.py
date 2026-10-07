# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dagster job for the DRG-grupperet LPR pipeline (extract -> archive -> stubs).

Unscheduled: the extract picks the newest usable file per takstsystem, so a
rerun against the same deliveries is a no-op in the archive.

No DQA subjects — the source is files, not a queryable system. Control totals
come from SDP_ExportLog (hard fail in extract); remaining guards run inline
in extract/archive. Same placement as HAIBA.

Somatik deliveries are ~2.5-4 GB bz2 per takstsystem and stream sequentially.
Use the extract's ``takstsystemer`` run-config to limit a test run.
"""

import dagster as dg

drg_ingest_job = dg.define_asset_job(
    name="drg_ingest_job",
    selection=dg.AssetSelection.groups("drg_extract", "drg_archive", "drg_borger"),
    description=(
        "Load the newest DRG/DRGPSYK delivery per takstsystem into ext, MERGE "
        "into the arc.drg* archive, and create stub Patient resources in FHIR "
        "for unknown CPRs (drg_borger). Like HAIBA, the stubs are created on "
        "ingest but only become a real dw_ek_Borger once the nightly DFM chain "
        "has minted them — so publishing (drg_publish_job) is deferred until "
        "after that mint."
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
        "bridges -> fct) and sync the CPR-free dim/fct/bridge tables to DSO. A "
        "full rebuild with stable dw_ek from the map layer, so a rerun "
        "re-resolves every contact's dw_ek_Borger against the current "
        "ref.PatientToBorger and updates in place. Split from drg_ingest_job "
        "for the same reason as HAIBA: unknown-CPR stubs created on ingest only "
        "become a real dw_ek_Borger once the nightly DFM chain has minted them, "
        "so publishing is deferred until after that mint."
    ),
)

__all__ = ["drg_ingest_job", "drg_publish_job"]

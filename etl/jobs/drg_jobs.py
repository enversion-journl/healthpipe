# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dagster job for the DRG-grupperet LPR pipeline (extract -> archive).

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
    selection=dg.AssetSelection.groups("drg_extract", "drg_archive"),
    description=(
        "Load the newest DRG/DRGPSYK delivery per takstsystem into ext and "
        "MERGE into the arc.drg* archive."
    ),
)

__all__ = ["drg_ingest_job"]

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dagster job for udredningsret ingestion (SDS LPR_Udredningsret: extract -> archive).

Delivery detection ([internal ticket] pattern, level 1) makes the job cheap to run
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

No DQA subjects in ingest — the source is files. Control totals come from
SDP_ExportLog (hard fail in the extract); the remaining guards run inline in
the archive.
"""

import dagster as dg

udredningsret_ingest_job = dg.define_asset_job(
    name="udredningsret_ingest_job",
    selection=dg.AssetSelection.groups("udredningsret_extract", "udredningsret_archive"),
    description=(
        "Load the newest udredningsret delivery (SDS LPR_Udredningsret, weekly full "
        "snapshot) that is newer than the DFM delivery checkpoint into ext, MERGE it into "
        "arc.udredningsret_mont on business keys (helbredsforloeb, markoer, loebenummer) "
        "with soft-delete over the whole snapshot, and stamp the checkpoint. A run without "
        "a new file reads nothing. Staging/dim/fct (US-3–US-5) and DSO (US-6) follow in "
        "their own jobs."
    ),
)

__all__ = ["udredningsret_ingest_job"]

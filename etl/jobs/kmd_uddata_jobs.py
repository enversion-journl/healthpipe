# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
KMD UdData pipeline jobs.

Dedicated job for the KMD Indkøbsanalyse archive pipeline. Pulls
delta rows from KMD UdData (audit-MAX watermark), MERGE-upserts into
arc.kmd_uddata_unspsc, arc.kmd_uddata_spend_analysis_lines and arc.kmd_uddata_invoice.

Manual / on-demand run for now — no schedule.
"""

import dagster as dg

kmd_uddata_archive_job = dg.define_asset_job(
    name="kmd_uddata_archive_job",
    selection=dg.AssetSelection.groups("kmd_uddata_archive"),
    description=(
        "Extract + archive KMD UdData (UNSPSC + SpendAnalysisLines + Invoice). "
        "Full load on first run, then delta via audit-MAX watermark; UPSERTs into arc.*."
    ),
    tags={"domain": "kmd_uddata", "layer": "archive"},
)

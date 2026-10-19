# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""One ingestion stream for weekly (U) and monthly (M) deliveries: 47 extracts, 38 archives."""

import dagster as dg

lpr3_ingest_job = dg.define_asset_job(
    name="lpr3_ingest_job",
    selection=dg.AssetSelection.groups("lpr3_extract", "lpr3_archive"),
    executor_def=dg.multiprocess_executor.configured({"max_concurrent": 4}),
    description=(
        "Read the newest SDS LPR3 full snapshot into 47 ext tables using four "
        "disjoint tar passes, resolve delivery-local keys, and archive 38 tables. "
        "The delivery checkpoint advances only after every archive completes. "
        "No newer delivery is a no-op. A failed run cannot be re-executed from "
        "failure; a new run reloads the whole delivery. No FHIR, DSO or nightly "
        "IFDB-chain dependencies."
    ),
)

lpr3_freshness_job = dg.define_asset_job(
    name="lpr3_freshness_job",
    selection=dg.AssetSelection.groups("lpr3_freshness"),
    description=(
        "Alarm on a stale LPR3 delivery share. Its own job, so a stale share is visible "
        "whether or not the ingest loads, and a failing ingest cannot hide it."
    ),
)

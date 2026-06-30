# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ERP daily DFM (dim/fact/master) pipeline job.

Non-partitioned daily ERP refresh — covers current-state-only sources
(lagerbeholdning, afdlgrbeh) and master/reference dimensions like
dim.UNSPSC that depend on multi-source archives. Cannot be meaningfully
backfilled by partition.

Includes the kmd_uddata_archive group so dim.UNSPSC's KMD upstream is
materialised by this job. The standalone kmd_uddata_archive_job remains
available for ad-hoc runs. The mysupply_archive, ils_archive, and
sydax_archive groups cover the new DFM-based ERP archives (each runs in
parallel with its legacy partition extract). The standalone
sydax_archive_job remains available for ad-hoc runs.
"""

import dagster as dg

erp_daily_dfm_job = dg.define_asset_job(
    name="erp_daily_dfm_job",
    selection=dg.AssetSelection.groups(
        # Lagerbeholdning (existing)
        "extract_lagerbeholdning",
        "pre_staging_lagerbeholdning",
        "dimensions_lagerbeholdning",
        "facts_lagerbeholdning",
        "dso_sync_lagerbeholdning",
        # KMD UdData archive (existing — depended on by dim.UNSPSC)
        "kmd_uddata_archive",
        # MySupply DFM archive (new — runs in parallel with legacy partition extract)
        "mysupply_archive",
        # ILS DFM archive (new — runs in parallel with legacy ils.py partition extract)
        "ils_archive",
        # SydAX DFM archive (new — ext.*_sydax_dfm → arc.*_sydax, all 17 base tables)
        "sydax_archive",
        # UNSPSC (new)
        "extract_ils",
        "staging_unspsc",
        "map_unspsc",
        "dimensions_unspsc",
        "dso_sync_unspsc",
        # KMD IndkoebsanalyseLinje (SpendAnalysisLines fact)
        "staging_indkoebsanalyse",
        "facts_indkoebsanalyse",
        "dso_sync_indkoebsanalyse",
        # Vareudlevering (migrated off partitions — [internal ticket]; reads arc.ils_udlev)
        "pre_staging_vareudlevering",
        "staging_vareudlevering",
        "maps_vareudlevering",
        "dimensions_vareudlevering",
        "facts_vareudlevering",
        "dso_sync_vareudlevering",
    ),
    description=(
        "Daily ERP non-partitioned refresh: lagerbeholdning current-state "
        "snapshot + KMD UdData archive + MySupply/ILS/SydAX DFM archives + "
        "dim.UNSPSC + IndkoebsanalyseLinje + Vareudlevering (arc.ils_udlev). "
        "Cannot be partition-backfilled — sources reflect current state only "
        "or are master data."
    ),
)

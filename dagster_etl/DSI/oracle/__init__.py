# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSI Oracle source configs — IFDB.

Defines table configs and generates extraction assets
using the shared Oracle connector.
"""

from .ifdb import (  # noqa: F401
    ENCOUNTER_CONFIG,
    EPISODE_OF_CARE_CONFIG,
    EVENTMARKER_CONFIG,
    FULL_LOAD_BATCH_SIZE,
    LOCATION_CONFIG,
    PATIENT_CONFIG,
    _extract_markers_for_patient_batch,
    _get_patient_ids_batch,
    extract_eventmarker_alle_markoerer_daily,
    extract_eventmarker_alle_markoerer_full_load,
    extract_eventmarker_alle_markoerer_monthly,
    ifdb_all_assets,
    ifdb_daily_assets,
    ifdb_full_load_assets,
    ifdb_monthly_assets,
)

from DSI.extract.IFDB import (
    HENVISNING_CONFIG,
    HENVISNING_HISTORIK_CONFIG,
    REFERRALINFORMATION_CONFIG,
    all_assets as new_ifdb_all_assets,
    new_ifdb_daily_assets,
    new_ifdb_full_load_assets,
    new_ifdb_monthly_assets,
)

from .ifdb_extract import (
    ifdb_archive_extract_assets,
)
from .ifdb_archive import (
    ifdb_archive_assets,
)


from DSI.extract.BOM_IFDB import (
    all_assets as bom_ifdb_all_assets,
    bom_ifdb_daily_assets,
    bom_ifdb_full_load_assets,
    bom_ifdb_monthly_assets,
)

daily_extraction_assets = [*ifdb_daily_assets, *new_ifdb_daily_assets, *bom_ifdb_daily_assets]
monthly_extraction_assets = [
    *ifdb_monthly_assets,
    *new_ifdb_monthly_assets,
    *bom_ifdb_monthly_assets,
]
full_load_extraction_assets = [
    *ifdb_full_load_assets,
    *new_ifdb_full_load_assets,
    *bom_ifdb_full_load_assets,
]
all_extraction_assets = [*ifdb_all_assets, *new_ifdb_all_assets, *bom_ifdb_all_assets]

all_ifdb_archive_assets = [*ifdb_archive_extract_assets, *ifdb_archive_assets]

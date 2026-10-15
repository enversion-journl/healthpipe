# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSI Oracle source configs — IFDB, EPJI.

All IFDB extraction runs through the DFM pipeline (``ifdb_extract`` +
``ifdb_archive``). ``ifdb`` contributes no assets — only the
``TableExtractionConfig`` definitions used for on-demand Oracle lookups.
"""

from DSI.oracle.epji_archive import arc_epji_shakalike  # noqa: F401
from DSI.oracle.epji_extract import epji_location_extract_assets  # noqa: F401
from DSI.oracle.ifdb import (  # noqa: F401
    ENCOUNTER_CONFIG,
    EPISODE_OF_CARE_CONFIG,
    LOCATION_CONFIG,
    PATIENT_CONFIG,
)
from DSI.oracle.ifdb_archive import (
    ifdb_archive_assets,
)
from DSI.oracle.ifdb_extract import (
    ifdb_archive_extract_assets,
)

all_ifdb_archive_assets = [*ifdb_archive_extract_assets, *ifdb_archive_assets]

all_epji_archive_assets = [*epji_location_extract_assets, arc_epji_shakalike]

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSI extraction configs and assets for new IFDB tables."""

from DSI.extract.IFDB.administrative_procedure import (
    ADMINISTRATIVE_PROCEDURE_CONFIG,
    extract_administrative_procedure_bulk,
    extract_administrative_procedure_daily,
    extract_administrative_procedure_monthly,
)
from DSI.extract.IFDB.aktivitetsresultat import (
    AKTIVITETSRESULTAT_CONFIG,
    extract_aktivitetsresultat_bulk,
    extract_aktivitetsresultat_daily,
    extract_aktivitetsresultat_monthly,
)
from DSI.extract.IFDB.aktivitetsudfoerelse import (
    AKTIVITETSUDFOERELSE_CONFIG,
    extract_aktivitetsudfoerelse_bulk,
    extract_aktivitetsudfoerelse_daily,
    extract_aktivitetsudfoerelse_monthly,
)
from DSI.extract.IFDB.diagnose import (
    DIAGNOSE_CONFIG,
    extract_diagnose_bulk,
    extract_diagnose_daily,
    extract_diagnose_monthly,
)
from DSI.extract.IFDB.henvisning import (
    HENVISNING_CONFIG,
    extract_henvisning_bulk,
    extract_henvisning_daily,
    extract_henvisning_monthly,
)
from DSI.extract.IFDB.henvisning_historik import (
    HENVISNING_HISTORIK_CONFIG,
    extract_henvisning_historik_bulk,
    extract_henvisning_historik_daily,
    extract_henvisning_historik_monthly,
)
from DSI.extract.IFDB.pas_diagnose import (
    PAS_DIAGNOSE_CONFIG,
    extract_pas_diagnose_bulk,
    extract_pas_diagnose_daily,
    extract_pas_diagnose_monthly,
)
from DSI.extract.IFDB.referralinformation import (
    REFERRALINFORMATION_CONFIG,
    extract_referralinformation_bulk,
    extract_referralinformation_daily,
    extract_referralinformation_monthly,
)
from DSI.extract.IFDB.ris_ydelse import (
    RIS_YDELSE_CONFIG,
    extract_ris_ydelse_bulk,
    extract_ris_ydelse_daily,
    extract_ris_ydelse_monthly,
)

new_ifdb_daily_assets = [
    extract_aktivitetsresultat_daily,
    extract_diagnose_daily,
    extract_pas_diagnose_daily,
    extract_administrative_procedure_daily,
    extract_ris_ydelse_daily,
    extract_aktivitetsudfoerelse_daily,
    extract_henvisning_daily,
    extract_henvisning_historik_daily,
    extract_referralinformation_daily,
]

new_ifdb_monthly_assets = [
    extract_aktivitetsresultat_monthly,
    extract_diagnose_monthly,
    extract_pas_diagnose_monthly,
    extract_administrative_procedure_monthly,
    extract_ris_ydelse_monthly,
    extract_aktivitetsudfoerelse_monthly,
    extract_henvisning_monthly,
    extract_henvisning_historik_monthly,
    extract_referralinformation_monthly,
]

new_ifdb_full_load_assets = [
    extract_aktivitetsresultat_bulk,
    extract_diagnose_bulk,
    extract_pas_diagnose_bulk,
    extract_administrative_procedure_bulk,
    extract_ris_ydelse_bulk,
    extract_aktivitetsudfoerelse_bulk,
    extract_henvisning_bulk,
    extract_henvisning_historik_bulk,
    extract_referralinformation_bulk,
]

all_assets = [
    *new_ifdb_daily_assets,
    *new_ifdb_monthly_assets,
    *new_ifdb_full_load_assets,
]

__all__ = [
    "all_assets",
    "new_ifdb_daily_assets",
    "new_ifdb_monthly_assets",
    "new_ifdb_full_load_assets",
]

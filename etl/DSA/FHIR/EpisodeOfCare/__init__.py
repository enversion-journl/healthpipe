# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from .EpisodeOfCare_IFDB_dfm import fhir_server_eoc_dfm
from .EpisodeOfCare_QualityCheck import (
    fhir_quality_check_eoc_healthissue_code,
    fhir_quality_check_eoc_previous_references,
)

all_assets = [
    fhir_server_eoc_dfm,
    fhir_quality_check_eoc_previous_references,
    fhir_quality_check_eoc_healthissue_code,
]

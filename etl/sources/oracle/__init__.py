# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Oracle source assets.
"""

from .ifdb_assets import (
    ifdb_encounter_source,
    ifdb_episode_of_care_source,
    ifdb_eventmarker_source,
    ifdb_healthissue_source,
    ifdb_location_source,
    ifdb_observable_assets,
    ifdb_patient_oplysninger_columna_source,
    ifdb_patient_oplysninger_officielle_source,
)

__all__ = [
    "ifdb_encounter_source_spec",
    "ifdb_episode_of_care_source_spec",
    "ifdb_eventmarker_source_spec",
    "ifdb_healthissue_source_spec",
    "ifdb_patient_oplysninger_columna_source_spec",
    "ifdb_patient_oplysninger_officielle_source_spec",
    "ifdb_location_source_spec",
]

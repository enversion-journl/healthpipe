# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import dagster as dg

# Using AssetSpec to declare external assets, which is the modern approach
# for sources not materialized by Dagster. This provides a clear signal to the planner.

mssql_patient_columna_source = dg.AssetSpec(
    key="mssql_patient_columna_source",
    group_name="mssql_source",
    description="The dbo.PATIENT_OPLYSNINGER_COLUMNA table in MSSQL.",
)

mssql_patient_officielle_source = dg.AssetSpec(
    key="mssql_patient_officielle_source",
    group_name="mssql_source",
    description="The dbo.PATIENT_OPLYSNINGER_OFFICIELLE table in MSSQL.",
)

mssql_episode_of_care_source = dg.AssetSpec(
    key="mssql_episode_of_care_source",
    group_name="mssql_source",
    description="The dbo.EPISODE_OF_CARE table in MSSQL.",
)

mssql_encounter_source = dg.AssetSpec(
    key="mssql_encounter_source",
    group_name="mssql_source",
    description="The dbo.ENCOUNTER table in MSSQL.",
)

mssql_eventmarker_source = dg.AssetSpec(
    key="mssql_eventmarker_source",
    group_name="mssql_source",
    description="The dbo.EVENTMARKER table in MSSQL.",
)

mssql_location_source = dg.AssetSpec(
    key="mssql_location_source",
    group_name="mssql_source",
    description="The dbo.LOCATION table in MSSQL, containing patient stay information.",
)

mssql_healthissue_source = dg.AssetSpec(
    key="mssql_healthissue_source",
    group_name="mssql_source",
    description="The dbo.HEALTHISSUE table in MSSQL, containing patient health problems.",
)

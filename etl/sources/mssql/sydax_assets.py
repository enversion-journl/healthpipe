# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import dagster as dg

# Using AssetSpec to declare external assets, which is the modern approach
# for sources not materialized by Dagster. This provides a clear signal to the planner.

# Finans schema tables
sydax_finanspostering_kreditor_trans_relation_source = dg.AssetSpec(
    key="sydax_finanspostering_kreditor_trans_relation_source",
    group_name="sydax_source",
    description="The Finans.FinansposteringKreditorTransRelation table in SYDAX MSSQL database.",
)

sydax_finanskonto_source = dg.AssetSpec(
    key="sydax_finanskonto_source",
    group_name="sydax_source",
    description="The Finans.Finanskonto table in SYDAX MSSQL database.",
)

sydax_finanspostering_source = dg.AssetSpec(
    key="sydax_finanspostering_source",
    group_name="sydax_source",
    description="The Finans.Finanspostering table in SYDAX MSSQL database.",
)

# Afdeling table for organization hierarchy
sydax_afdeling_source = dg.AssetSpec(
    key="sydax_afdeling_source",
    group_name="sydax_source",
    description="The Finans.Afdeling table in SYDAX MSSQL database - contains organizational hierarchy.",
)

# Kreditor schema tables
sydax_kreditortransaktion_source = dg.AssetSpec(
    key="sydax_kreditortransaktion_source",
    group_name="sydax_source",
    description="The Kreditor.Kreditortransaktion table in SYDAX MSSQL database.",
)

sydax_kreditor_source = dg.AssetSpec(
    key="sydax_kreditor_source",
    group_name="sydax_source",
    description="The Kreditor.Kreditor table in SYDAX MSSQL database.",
)

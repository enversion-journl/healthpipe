# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import dagster as dg

# Using AssetSpec to declare external assets, which is the modern approach
# for sources not materialized by Dagster. This provides a clear signal to the planner.

# ILS dbo schema tables
ils_vare_source = dg.AssetSpec(
    key="ils_vare_source",
    group_name="ils_source",
    description="The dbo.vare table in ILS MSSQL database.",
)

ils_vareart_source = dg.AssetSpec(
    key="ils_vareart_source",
    group_name="ils_source",
    description="The dbo.vareart table in ILS MSSQL database.",
)

ils_varegrp_source = dg.AssetSpec(
    key="ils_varegrp_source",
    group_name="ils_source",
    description="The dbo.varegrp table in ILS MSSQL database.",
)

ils_varetype_source = dg.AssetSpec(
    key="ils_varetype_source",
    group_name="ils_source",
    description="The dbo.varetype table in ILS MSSQL database.",
)

ils_varetype2_source = dg.AssetSpec(
    key="ils_varetype2_source",
    group_name="ils_source",
    description="The dbo.varetype2 table in ILS MSSQL database.",
)

ils_projekt_source = dg.AssetSpec(
    key="ils_projekt_source",
    group_name="ils_source",
    description="The dbo.projekt table in ILS MSSQL database.",
)

ils_lev_source = dg.AssetSpec(
    key="ils_lev_source",
    group_name="ils_source",
    description="The dbo.lev table in ILS MSSQL database.",
)

ils_lafth_source = dg.AssetSpec(
    key="ils_lafth_source",
    group_name="ils_source",
    description="The dbo.lafth table in ILS MSSQL database.",
)

ils_laftd_source = dg.AssetSpec(
    key="ils_laftd_source",
    group_name="ils_source",
    description="The dbo.laftd table in ILS MSSQL database.",
)

ils_laftdpris_source = dg.AssetSpec(
    key="ils_laftdpris_source",
    group_name="ils_source",
    description="The dbo.laftdpris table in ILS MSSQL database.",
)

ils_laftdstam_source = dg.AssetSpec(
    key="ils_laftdstam_source",
    group_name="ils_source",
    description="The dbo.laftdstam table in ILS MSSQL database.",
)

ils_udlev_source = dg.AssetSpec(
    key="ils_udlev_source",
    group_name="ils_source",
    description="The dbo.udlev table in ILS MSSQL database.",
)

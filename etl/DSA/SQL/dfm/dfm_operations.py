# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DFM operational assets.

dfm_reset — ad-hoc checkpoint reset via meta.usp_dfm_reset.
"""

import dagster as dg
from core.dfm_config import DfmResetConfig
from core.resources import MSSQLResource


@dg.asset(
    name="dfm_reset",
    group_name="dfm_operations",
    compute_kind="mssql",
    description="Reset a DFM checkpoint via meta.usp_dfm_reset. Ad-hoc use only.",
)
def dfm_reset(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: DfmResetConfig,
) -> dg.MaterializeResult:
    """Reset a checkpoint to a specific value."""
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "EXEC [meta].[usp_dfm_reset] "
                "@schema_name = ?, @table_name = ?, "
                "@new_value = ?, @reason = ?, @scope = ?",
                (
                    config.schema_name,
                    config.table_name,
                    config.new_value,
                    config.reason,
                    config.scope,
                ),
            )
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"DFM reset: [{config.scope}] {config.schema_name}.{config.table_name} "
        f"→ {config.new_value!r} (reason: {config.reason!r})"
    )
    return dg.MaterializeResult(
        metadata={
            "scope": config.scope,
            "schema_name": config.schema_name,
            "table_name": config.table_name,
            "new_value": config.new_value,
            "reason": config.reason,
        }
    )

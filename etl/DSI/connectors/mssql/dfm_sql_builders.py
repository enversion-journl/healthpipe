# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DFM-aware T-SQL WHERE clause builder for MSSQL extracts.

Analogous to build_oracle_where() in DSI/oracle/ifdb_extract.py but
emitting T-SQL syntax (CAST, DATEADD, etc.) instead of Oracle TO_DATE.

Used by the DFM asset factory (DSI/connectors/factory.py) and can be
called directly by hand-written MSSQL DFM extract assets.
"""

from __future__ import annotations

from typing import cast

from core.dfm_config import RunModeConfig
from DSI.connectors.extract_config import DfmExtractConfig
from utils.sql_safety import safe_bracket_quote, safe_table_ref, validate_checkpoint_value


def build_mssql_where(
    run_config: RunModeConfig,
    checkpoint: str | None,
    extract_config: DfmExtractConfig,
) -> str | None:
    """Build a T-SQL WHERE clause from RunModeConfig + DfmExtractConfig.

    Handles all modes: delta, full_load, period, ids.
    Respects lookback_days (design-time default + run-time override).

    Args:
        run_config: Run-time config controlling mode and overrides.
        checkpoint: Current checkpoint value (from DfmManager). May be None
            on first run or for full-load tables.
        extract_config: Design-time extraction config defining the delta
            column/expression and checkpoint type.

    Returns:
        A WHERE clause string (without the WHERE keyword), or None when
        no filtering should be applied (full-load, or first delta run).
    """
    if run_config.delta_column:
        col: str | None = safe_bracket_quote(run_config.delta_column)
    else:
        col = extract_config.effective_delta_col

    if run_config.mode == "full_load" or extract_config.is_full_load:
        return None

    if run_config.mode == "delta":
        if not checkpoint:
            return None
        validate_checkpoint_value(checkpoint)
        lookback = run_config.lookback_days or extract_config.lookback_days or 0
        if extract_config.checkpoint_type == "int":
            cp_val = int(checkpoint)
            if lookback > 0:
                cp_val = max(0, cp_val - lookback)
            return f"{col} > {cp_val}"
        else:
            safe_cp = checkpoint.replace("'", "''")
            if lookback > 0:
                return f"{col} > DATEADD(day, -{lookback}, " f"CAST('{safe_cp}' AS DATETIME2))"
            return f"{col} > CAST('{safe_cp}' AS DATETIME2)"

    if run_config.mode == "period":
        safe_from = cast(str, run_config.date_from).replace("'", "''")
        if run_config.date_to:
            safe_to = run_config.date_to.replace("'", "''")
            return (
                f"{col} >= CAST('{safe_from}' AS DATETIME2) "
                f"AND {col} < DATEADD(day, 1, CAST('{safe_to}' AS DATE))"
            )
        return f"{col} >= CAST('{safe_from}' AS DATETIME2)"

    if run_config.mode == "ids":
        id_col = safe_bracket_quote(run_config.id_filter_column or "id")
        if run_config.id_filter_table:
            safe_tbl = safe_table_ref(run_config.id_filter_table)
            # Alias + qualify the subquery column: if id_filter_column doesn't exist
            # in the filter table, the query fails loudly instead of silently
            # correlating to the outer table (always-true → full extract).
            return f"{id_col} IN " f"(SELECT [flt].{id_col} FROM {safe_tbl} AS [flt])"
        escaped = ", ".join(
            f"'{v.replace(chr(39), chr(39) * 2)}'"
            for v in cast(list[str], run_config.id_filter_values)
        )
        return f"{id_col} IN ({escaped})"

    return None

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ref.KommuneToPopulationsansvar — the hospital with population responsibility per municipality.

POPULATIONSANSVAR_BY_KOMMUNEKODE is the source, and the table is a full snapshot of it: the
MERGE deletes a row the mapping no longer has (WHEN NOT MATCHED BY SOURCE THEN DELETE). Written
to staging, where dim_Borger joins it, and to DSO.
"""

import dagster as dg
from core.resources import MSSQLResource
from utils.sql.write_timestamp import WRITE_TIMESTAMP_UTC

REF_TABLE = "[ref].[KommuneToPopulationsansvar]"

# Municipality codes are four digits, as in dk-core-MunicipalityCodes and the CPR register.
POPULATIONSANSVAR_BY_KOMMUNEKODE: dict[str, str] = {
    "0410": "SLB",
    "0607": "SLB",
    "0621": "SLB",
    "0630": "SLB",
    "0510": "SHS",
    "0540": "SHS",
    "0550": "SHS",
    "0580": "SHS",
    "0420": "OUH",
    "0430": "OUH",
    "0440": "OUH",
    "0450": "OUH",
    "0461": "OUH",
    "0479": "OUH",
    "0480": "OUH",
    "0482": "OUH",
    "0492": "OUH",
    "0530": "EGS",
    "0561": "EGS",
    "0563": "EGS",
    "0573": "EGS",
    "0575": "EGS",
}

SNAPSHOT_TABLE = "#kommune_to_populationsansvar"

# Collated to the database: tempdb's own collation differs from the DSO database's.
CREATE_SNAPSHOT_SQL = f"""
    CREATE TABLE {SNAPSHOT_TABLE} (
        [kommunekode]       VARCHAR(4)   COLLATE DATABASE_DEFAULT NOT NULL PRIMARY KEY,
        [populationsansvar] NVARCHAR(50) COLLATE DATABASE_DEFAULT NOT NULL
    )
"""

MERGE_SQL = f"""
    MERGE {REF_TABLE} AS tgt
    USING {SNAPSHOT_TABLE} AS src
        ON tgt.[kommunekode] = src.[kommunekode]
    WHEN MATCHED AND tgt.[populationsansvar] <> src.[populationsansvar] THEN
        UPDATE SET [populationsansvar] = src.[populationsansvar],
                   [dw_timestamp_update] = {WRITE_TIMESTAMP_UTC}
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ([kommunekode], [populationsansvar])
        VALUES (src.[kommunekode], src.[populationsansvar])
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
"""


def _write_snapshot(resource: MSSQLResource) -> tuple[int, int]:
    """Make the table equal to the mapping; returns (rows changed, rows in the table)."""
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {SNAPSHOT_TABLE}")
            cursor.execute(CREATE_SNAPSHOT_SQL)
            for kommunekode, populationsansvar in POPULATIONSANSVAR_BY_KOMMUNEKODE.items():
                cursor.execute(
                    f"INSERT INTO {SNAPSHOT_TABLE} ([kommunekode], [populationsansvar]) "
                    "VALUES (?, ?)",
                    [kommunekode, populationsansvar],
                )
            cursor.execute(MERGE_SQL)
            changed = cursor.rowcount
            cursor.execute(f"SELECT COUNT(*) FROM {REF_TABLE}")
            total = int(cursor.fetchone()[0])
            cursor.execute(f"DROP TABLE IF EXISTS {SNAPSHOT_TABLE}")
            conn.commit()
            return changed, total
        finally:
            cursor.close()


@dg.asset(
    name="ref_KommuneToPopulationsansvar",
    group_name="ref",
    compute_kind="mssql",
    deps=["staging_infra_ref", "dso_infra_ref"],
    tags={"layer": "ref", "module": "shared_dimensions", "domain": "ref"},
    description=(
        "ref.KommuneToPopulationsansvar — municipality code to the hospital with population "
        "responsibility. Full snapshot of the mapping in code, written to staging and DSO."
    ),
)
def ref_KommuneToPopulationsansvar(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Write the municipality mapping to ref.KommuneToPopulationsansvar on staging and DSO."""
    staging_changed, staging_rows = _write_snapshot(mssql_target_staging)
    dso_changed, dso_rows = _write_snapshot(mssql_target_dso)
    context.log.info(
        f"{REF_TABLE}: staging {staging_changed} changed of {staging_rows}, "
        f"DSO {dso_changed} changed of {dso_rows}"
    )
    return dg.MaterializeResult(
        metadata={
            "table": "ref.KommuneToPopulationsansvar",
            "staging_rows": staging_rows,
            "staging_changed": staging_changed,
            "dso_rows": dso_rows,
            "dso_changed": dso_changed,
        }
    )

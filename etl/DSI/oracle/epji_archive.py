# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
EPJI archive layer — UPSERT ext.epji_location and the three ext.epji_practitioner*
tables into their arc.* counterparts.

The practitioner archives follow the same full-snapshot reasoning as the location
archive below. PRACTITIONER and PRACTITIONER_ROLE are keyed on the source PK, which
is unique upstream. PRACTITIONER_IDENTIFIER is keyed on (PRACTITIONER_PK, SYSTEM):
EPJ rewrites every identifier row of an account with a new PK each time the account
is saved, so a PK key would soft-delete and reinsert them. The pair is unique today
but not enforced upstream; there is deliberately no dedup, so a duplicate fails the
MERGE on the arc primary key. Each practitioner archive counts the rows its MERGE
would soft-delete and fails before writing when the count passes its cap in
core/etl_thresholds.py, so a partially extracted ext cannot retire the rest of the
staff.

Unlike SOR's cumulative version file, ext_epji_location delivers a genuine
full snapshot every run (no watermark). detect_deletes=True is therefore
correct here: a location that disappears from EPJI should be soft-deleted
in the archive rather than silently vanishing with no trace.

EXTERNAL_ID has no enforced uniqueness upstream in EPJI.LOCATION (a
data-entry error could give two distinct locations the same code), so
dedup_source_on_keys=True wraps ext in a ROW_NUMBER dedup keyed on `code`
rather than assuming the extract already guarantees it. sor_code sorts
first in non_key_columns so a duplicate pair with different, non-empty SOR
codes deterministically keeps one and reports the collapse as
rows_dedup_dropped (with a warning log) — see ArchiveMergeConfig's
dedup_source_on_keys docstring for the general mechanism.

Named after the source (arc.epji_location), not the SHAK-a-like consumer —
see epji_extract.py for the full reasoning. The asset itself keeps the
`arc_epji_shakalike` name: it is still the specific step that feeds the
SHAK-a-like pipeline, even though the table it writes is a general-purpose
EPJI location archive.

Uses execute_archive_merge, the same DFM pattern as the ILS/SYDAX/SOR archives.
"""

import dagster as dg
from core.archive_merge import (
    build_archive_metadata,
    build_soft_delete_count_sql,
    execute_archive_merge,
)
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.etl_thresholds import check_soft_delete_threshold
from core.resources import MSSQLResource
from DSI.oracle.epji_extract import (
    PRACTITIONER_COLUMN_TYPES,
    PRACTITIONER_IDENTIFIER_COLUMN_TYPES,
    PRACTITIONER_ROLE_COLUMN_TYPES,
)

_MERGE_KEYS = ("code",)

EPJI_LOCATION_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="epji_location",
    target_schema="arc",
    target_table="epji_location",
    merge_keys=_MERGE_KEYS,
    # sor_code first: the tie-break priority for a duplicate `code` — a row
    # carrying a SOR code beats one without, matching the archive's job of
    # keeping the mapping usable. location_pk/display/identifier_system are
    # only tie-breakers among rows that already agree on sor_code.
    non_key_columns=("sor_code", "location_pk", "display", "identifier_system"),
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=True,
    upstream_is_full_load=True,
    dedup_source_on_keys=True,
    enable_bulk_insert_on_first_load=True,
)


@dg.asset(
    name="arc_epji_shakalike",
    group_name="epji_archive",
    compute_kind="mssql",
    deps=["ext_epji_location", "staging_infra_source_epji"],
    tags={"domain": "codesystem", "layer": "archive", "source": "epji"},
    description=(
        "UPSERT ext.epji_location → arc.epji_location (persistent EPJI "
        "location archive). Soft-deletes locations absent from the latest "
        "snapshot."
    ),
)
def arc_epji_shakalike(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    result = execute_archive_merge(
        context, mssql_target_staging, EPJI_LOCATION_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd into arc.epji_location — inserted={result.rows_inserted}, "
        f"updated={result.rows_updated}, soft_deleted={result.rows_soft_deleted}; "
        f"mode={config.mode}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.epji_location",
            target_table="arc.epji_location",
        )
    )


def _practitioner_archive_config(
    target_table: str, column_types: dict[str, str], merge_keys: tuple[str, ...]
) -> ArchiveMergeConfig:
    return ArchiveMergeConfig(
        source_schema="ext",
        source_table=target_table,
        target_schema="arc",
        target_table=target_table,
        merge_keys=merge_keys,
        non_key_columns=tuple(c for c in column_types if c not in merge_keys),
        strategy="upsert",
        update_only_on_change=True,
        detect_deletes=True,
        upstream_is_full_load=True,
        enable_bulk_insert_on_first_load=True,
    )


EPJI_PRACTITIONER_ARCHIVE_CONFIG = _practitioner_archive_config(
    "epji_practitioner", PRACTITIONER_COLUMN_TYPES, ("PK",)
)
EPJI_PRACTITIONER_IDENTIFIER_ARCHIVE_CONFIG = _practitioner_archive_config(
    "epji_practitioner_identifier",
    PRACTITIONER_IDENTIFIER_COLUMN_TYPES,
    ("PRACTITIONER_PK", "SYSTEM"),
)
EPJI_PRACTITIONER_ROLE_ARCHIVE_CONFIG = _practitioner_archive_config(
    "epji_practitioner_role", PRACTITIONER_ROLE_COLUMN_TYPES, ("PK",)
)


def _run_practitioner_archive(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
    archive_config: ArchiveMergeConfig,
) -> dg.MaterializeResult:
    table = archive_config.target_table
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(build_soft_delete_count_sql(archive_config))
            pending_soft_deletes = cursor.fetchone()[0]
        finally:
            cursor.close()
    check_soft_delete_threshold(
        f"arc.{table}", pending_soft_deletes, context, override=config.soft_delete_threshold
    )

    result = execute_archive_merge(context, mssql_target_staging, archive_config, config)

    context.log.info(
        f"MERGEd into arc.{table} — inserted={result.rows_inserted}, "
        f"updated={result.rows_updated}, soft_deleted={result.rows_soft_deleted}; "
        f"mode={config.mode}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table=f"ext.{table}",
            target_table=f"arc.{table}",
        )
    )


@dg.asset(
    name="arc_epji_practitioner",
    group_name="epji_practitioner_archive",
    compute_kind="mssql",
    deps=["ext_epji_practitioner", "staging_infra_source_epji"],
    tags={"domain": "epji", "layer": "archive", "source": "epji", "entity": "practitioner"},
    description=(
        "UPSERT ext.epji_practitioner → arc.epji_practitioner (one row per EPJ "
        "account). Soft-deletes accounts absent from the latest snapshot."
    ),
)
def arc_epji_practitioner(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    return _run_practitioner_archive(
        context, mssql_target_staging, config, EPJI_PRACTITIONER_ARCHIVE_CONFIG
    )


@dg.asset(
    name="arc_epji_practitioner_identifier",
    group_name="epji_practitioner_archive",
    compute_kind="mssql",
    deps=["ext_epji_practitioner_identifier", "staging_infra_source_epji"],
    tags={
        "domain": "epji",
        "layer": "archive",
        "source": "epji",
        "entity": "practitioner_identifier",
    },
    description=(
        "UPSERT ext.epji_practitioner_identifier → arc.epji_practitioner_identifier, "
        "one row per (PRACTITIONER_PK, SYSTEM). Soft-deletes pairs absent from the "
        "latest snapshot."
    ),
)
def arc_epji_practitioner_identifier(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    return _run_practitioner_archive(
        context, mssql_target_staging, config, EPJI_PRACTITIONER_IDENTIFIER_ARCHIVE_CONFIG
    )


@dg.asset(
    name="arc_epji_practitioner_role",
    group_name="epji_practitioner_archive",
    compute_kind="mssql",
    deps=["ext_epji_practitioner_role", "staging_infra_source_epji"],
    tags={
        "domain": "epji",
        "layer": "archive",
        "source": "epji",
        "entity": "practitioner_role",
    },
    description=(
        "UPSERT ext.epji_practitioner_role → arc.epji_practitioner_role (Columna "
        "role catalogue). Soft-deletes roles absent from the latest snapshot."
    ),
)
def arc_epji_practitioner_role(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    return _run_practitioner_archive(
        context, mssql_target_staging, config, EPJI_PRACTITIONER_ROLE_ARCHIVE_CONFIG
    )


epji_practitioner_archive_assets = [
    arc_epji_practitioner,
    arc_epji_practitioner_identifier,
    arc_epji_practitioner_role,
]

__all__ = [
    "EPJI_LOCATION_ARCHIVE_CONFIG",
    "EPJI_PRACTITIONER_ARCHIVE_CONFIG",
    "EPJI_PRACTITIONER_IDENTIFIER_ARCHIVE_CONFIG",
    "EPJI_PRACTITIONER_ROLE_ARCHIVE_CONFIG",
    "arc_epji_shakalike",
    "epji_practitioner_archive_assets",
]

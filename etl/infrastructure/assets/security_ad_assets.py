# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DPA security AD pipeline assets: extract from AD -> populate staging.

Wraps the AD extraction/load modules (infrastructure/security/export_healthpipe_ad_groups.py,
infrastructure/security/load_healthpipe_ad_to_mssql.py) as Dagster assets so the full chain
runs in security_daily_job: extract_security -> populate_security -> dso_sync_sec_* -> views.
(Those modules live in the app package — NOT under scripts/, which is dev-only tooling and
is not importable in the deployed Dagster runtime.)

NOTE: extract_security does a live LDAPS bind to AD, so the Dagster runtime needs
network/VPN access to the AD endpoint. The AD credentials are read from LDAP_* env.
"""

import os

import dagster as dg


@dg.asset(
    name="extract_security",
    group_name="extract_security",
    compute_kind="python",
    tags={"domain": "security", "layer": "extract"},
    description="Extract HealthPipe AD groups (roles/org/bopael) over LDAPS; returns rows in-memory (+ writes JSONL for audit).",
)
def extract_security(context: dg.AssetExecutionContext) -> dict:
    from infrastructure.security.export_healthpipe_ad_groups import run_export

    data = run_export()
    counts = {k: len(v) for k, v in data.items()}
    context.log.info(f"AD extract row counts: {counts}")
    context.add_output_metadata({f"{k}_rows": v for k, v in counts.items()})
    return data


@dg.asset(
    name="populate_security",
    group_name="populate_security",
    compute_kind="mssql",
    # cs_sor owns codesystem.SORHierarchy, which the expansion reads.
    deps=["staging_infra_security", "cs_sor"],
    tags={"domain": "security", "layer": "populate"},
    description="TRUNCATE+INSERT extracted AD data into staging.sec.RolleDomaene / sec.RBOrganisation.",
)
def populate_security(
    context: dg.AssetExecutionContext,
    extract_security: dict,
) -> dg.MaterializeResult:
    from infrastructure.security.load_healthpipe_ad_to_mssql import (
        _connect,
        _load_rb_organisation,
        _load_rolle_domaene,
        _min_count_ratio,
    )

    host = (os.environ.get("MSSQL_TARGET_STAGING_HOST") or "mssql").strip()
    port = int((os.environ.get("MSSQL_TARGET_STAGING_PORT") or "1433").strip())
    db = (os.environ.get("MSSQL_TARGET_STAGING_DB") or "staging").strip()
    user = (os.environ.get("MSSQL_TARGET_STAGING_USER") or "").strip()
    password = (os.environ.get("MSSQL_TARGET_STAGING_PASSWORD") or "").strip()
    if not user or not password:
        raise ValueError("MSSQL_TARGET_STAGING_USER and MSSQL_TARGET_STAGING_PASSWORD must be set")

    min_ratio = _min_count_ratio()

    # ExtractTooSmallError (udtræk væsentligt mindre end sidste load) propagerer
    # bevidst uden at blive fanget her, så Dagster-runet markeres failed.
    conn = _connect(host, port, db, user, password)
    try:
        rd_ins = _load_rolle_domaene(
            conn,
            extract_security["roller"],
            extract_security["bopael"],
            dry_run=False,
            min_ratio=min_ratio,
        )
        rb = _load_rb_organisation(
            conn, extract_security["org"], dry_run=False, min_ratio=min_ratio
        )
    finally:
        conn.close()

    # The skip path has all-zero metrics without having counted the live table;
    # log it explicitly so it does not read as "swapped to empty".
    if rb["swapped"]:
        rb_msg = (
            f"{rb['rows_new']} rows/{rb['users_new']} users swapped in "
            f"(before: {rb['rows_old']}/{rb['users_old']})"
        )
    else:
        rb_msg = "load skipped (empty extract) — live table untouched"
    context.log.info(f"sec.RolleDomaene: {rd_ins} inserted; sec.RBOrganisation: {rb_msg}")
    return dg.MaterializeResult(
        metadata={
            "rolledomaene_inserted": rd_ins,
            "rborganisation_rows_old": rb["rows_old"],
            "rborganisation_rows_new": rb["rows_new"],
            "rborganisation_users_old": rb["users_old"],
            "rborganisation_users_new": rb["users_new"],
            "rborganisation_swapped": rb["swapped"],
        }
    )


all_security_ad_assets = [extract_security, populate_security]

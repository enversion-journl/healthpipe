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
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource


@dg.asset(
    name="extract_security",
    group_name="extract_security",
    compute_kind="python",
    tags={"domain": "security", "layer": "extract"},
    description=(
        "Extract HealthPipe AD groups (roles/org) and the failed member lookups over "
        "LDAPS; returns rows in-memory (+ writes JSONL for audit)."
    ),
)
def extract_security(context: dg.AssetExecutionContext) -> dict:
    from infrastructure.security.export_healthpipe_ad_groups import run_export

    data = run_export()
    for warning in data["advarsler"]:
        context.log.warning(warning)
    counts = {k: len(v) for k, v in data.items() if k not in ("source", "advarsler")}
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
    description=(
        "Load extracted AD data into staging: sec.RolleDomaene (TRUNCATE+INSERT), "
        "sec.RBOrganisation (swap), and the historised sec.RolleDomaeneHistorik and "
        "sec.SORTildelingHistorik. Adds the fixed test users to sec.RolleDomaene and "
        "sec.RBOrganisation when SECURITY_SEED_ENABLED is on (never in prod)."
    ),
)
def populate_security(
    context: dg.AssetExecutionContext,
    extract_security: dict,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    from infrastructure.security.load_healthpipe_ad_to_mssql import (
        _ROLLE_DOMAENE_HISTORIK,
        _SOR_TILDELING_HISTORIK,
        _build_rolle_domaene_rows,
        _build_sor_tildeling_rows,
        _connect,
        _HistorikTabel,
        _load_historik,
        _load_rb_organisation,
        _load_rolle_domaene,
        _max_lost_users_ratio,
        _max_removed_grants_ratio,
        _min_count_ratio,
    )
    from infrastructure.security.security_seed import seed_enabled

    host = (os.environ.get("MSSQL_TARGET_STAGING_HOST") or "mssql").strip()
    port = int((os.environ.get("MSSQL_TARGET_STAGING_PORT") or "1433").strip())
    db = (os.environ.get("MSSQL_TARGET_STAGING_DB") or "staging").strip()
    user = (os.environ.get("MSSQL_TARGET_STAGING_USER") or "").strip()
    password = (os.environ.get("MSSQL_TARGET_STAGING_PASSWORD") or "").strip()
    if not user or not password:
        raise ValueError("MSSQL_TARGET_STAGING_USER and MSSQL_TARGET_STAGING_PASSWORD must be set")

    min_ratio = _min_count_ratio()
    max_removed_grants_ratio = _max_removed_grants_ratio()
    max_lost_users_ratio = _max_lost_users_ratio()
    seed = seed_enabled()

    # ExtractTooSmallError (udtræk for lille eller for forskelligt fra sidste load) propagerer
    # bevidst uden at blive fanget her, så Dagster-runet markeres failed.
    source = extract_security.get("source") or {}
    base_dn = source.get("table")
    rolle_domaene_rows = _build_rolle_domaene_rows(
        extract_security["roller"], warn=context.log.warning
    )
    historik: list[tuple[_HistorikTabel, list[tuple]]] = [
        (_ROLLE_DOMAENE_HISTORIK, rolle_domaene_rows),
        (_SOR_TILDELING_HISTORIK, _build_sor_tildeling_rows(extract_security["org"])),
    ]
    historik_metrics: dict[str, dict] = {}
    if extract_security["fejlede_opslag"]:
        context.log.warning(
            f"{len(extract_security['fejlede_opslag'])} AD member lookups failed; the history "
            "tables close no rows this run and only open new keys"
        )
    conn = _connect(host, port, db, user, password)
    try:
        with DfmManager(mssql_target_staging, context).target_runs(
            "sec",
            {
                "RolleDomaene": base_dn,
                "RBOrganisation": base_dn,
                **{tabel.name: base_dn for tabel, _ in historik},
            },
            dagster_run_id=context.run_id,
            mode="full_load",
            source_user=source.get("user"),
            source_host=source.get("host"),
        ) as rows:
            rd_ins = _load_rolle_domaene(
                conn, rolle_domaene_rows, dry_run=False, min_ratio=min_ratio
            )
            rows["RolleDomaene"] = rd_ins
            rb = _load_rb_organisation(
                conn,
                extract_security["org"],
                dry_run=False,
                min_ratio=min_ratio,
                max_removed_grants_ratio=max_removed_grants_ratio,
                max_lost_users_ratio=max_lost_users_ratio,
            )
            rows["RBOrganisation"] = rb["rows_new"] if rb["swapped"] else 0
            for tabel, tabel_rows in historik:
                metrics = _load_historik(
                    conn,
                    tabel,
                    tabel_rows,
                    run_id=context.run_id,
                    dry_run=False,
                    close_rows=not extract_security["fejlede_opslag"],
                )
                historik_metrics[tabel.name] = metrics
                rows[tabel.name] = metrics["closed"] + metrics["inserted"]
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
    context.log.info(
        f"sec.RolleDomaene: {rd_ins} inserted; sec.RBOrganisation: {rb_msg}; "
        f"testbrugere (seed): {'on' if seed else 'off'}; "
        + "; ".join(
            f"sec.{name}: {m['closed']} closed, {m['inserted']} inserted"
            for name, m in historik_metrics.items()
        )
    )
    return dg.MaterializeResult(
        metadata={
            "security_seed_enabled": seed,
            "rolledomaene_inserted": rd_ins,
            "rborganisation_rows_old": rb["rows_old"],
            "rborganisation_rows_new": rb["rows_new"],
            "rborganisation_users_old": rb["users_old"],
            "rborganisation_users_new": rb["users_new"],
            "rborganisation_removed_grants": rb["removed_grants"],
            "rborganisation_lost_users": rb["lost_users"],
            "rborganisation_swapped": rb["swapped"],
            **{
                f"{name.lower()}_{key}": value
                for name, m in historik_metrics.items()
                for key, value in m.items()
            },
        }
    )


all_security_ad_assets = [extract_security, populate_security]

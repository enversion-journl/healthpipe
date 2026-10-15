# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Guard the centralized-migrations invariants.

- migrations live in one directory, not per-domain;
- and that directory is MSSQL-only, because ``staging_migrations`` and
  ``dso_migrations`` execute every file in it against their own MSSQL database.
"""

from infrastructure.utils.ddl_loader import get_ddl_path

# Tokens that cannot appear in T-SQL. A file carrying one of these is meant for
# the FHIR server (Postgres), and dropping it here does not make it run there —
# it makes staging_migrations and dso_migrations fail on it and take the
# infrastructure DAG down. FHIR-server work belongs in a Python asset:
# idempotent DDL in infrastructure/assets/fhir_ifdb_infrastructure.py, one-off
# data operations in dqa/remediation/ as operator-triggered assets.
_POSTGRES_ONLY_TOKENS = (
    "jsonb",
    "pg_index",
    "pg_class",
    "pg_views",
    "pg_indexes",
    "plpgsql",
    "on conflict",
)


def test_migrations_dir_is_mssql_only():
    migrations_dir = get_ddl_path() / "migrations"
    offenders: dict[str, list[str]] = {}
    for path in sorted(migrations_dir.glob("*.sql")):
        lowered = path.read_text(encoding="utf-8").lower()
        hits = [token for token in _POSTGRES_ONLY_TOKENS if token in lowered]
        if hits:
            offenders[path.name] = hits
    assert offenders == {}, (
        "ddl/migrations/ is executed against MSSQL by both staging_migrations and "
        "dso_migrations, so Postgres SQL there fails the infrastructure DAG. Move "
        "FHIR-server work into a Python asset (see dqa/remediation/). Found: "
        f"{offenders}"
    )


def test_no_migrations_dirs_under_tables():
    tables_dir = get_ddl_path() / "tables"
    offenders = sorted(str(p) for p in tables_dir.rglob("migrations") if p.is_dir())
    assert offenders == [], (
        "Per-domain migration directories are forbidden; migrations belong in "
        f"infrastructure/ddl/migrations/ as yyyymmdd_name.sql. Found: {offenders}"
    )

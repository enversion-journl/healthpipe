#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Create the empty databases the pipeline expects, idempotently.

Public-path bootstrap: the vanilla MSSQL image starts with no application
databases. This connects to the `mssql` compose service as `sa` and issues a
guarded CREATE DATABASE for each configured database. It restores no data — the
schema is built afterwards by the `full_infrastructure_setup` Dagster job.

Uses `pymssql` (a project dependency, pure TDS) so it needs no system ODBC
driver. Defaults below mirror the real source-DB names so the same connection
config works in public and internal setups; override any of them via the
environment.

Set ``MSSQL_SKIP_DB_BOOTSTRAP=true`` to skip entirely. Data-baked images (the
internal devcontainer) already ship every application database under its real
name, so this generic bootstrap would only add empty, unused duplicates.
"""

import os
import sys
import time

HOST = os.getenv("MSSQL_BOOTSTRAP_HOST", "mssql")
PORT = os.getenv("MSSQL_BOOTSTRAP_PORT", "1433")
SA_PASSWORD = os.getenv("MSSQL_SA_PASSWORD", "tempPassword1!")

# (env var, default value) — values are kept verbatim; keys only are curated.
DATABASES = [
    ("MSSQL_STAGING_DB", "staging"),
    ("MSSQL_DSO_DB", "dso"),
    ("MSSQL_ILS_DB", "ils"),
    ("MSSQL_COSMIC_DB", "cosmic"),
    ("MSSQL_MYSUPPLY_DB", "mysupply"),
    ("MSSQL_SYDAX_DB", "sydax"),
]


def connect_with_retry(attempts: int = 30, delay: int = 2) -> "pymssql.Connection":
    # Imported lazily (not at module top) so the MSSQL_SKIP_DB_BOOTSTRAP early-return
    # in main() needs no DB driver — a data-baked setup skips without importing pymssql.
    import pymssql

    # autocommit=True: CREATE DATABASE cannot run inside a multi-statement transaction.
    last_err: Exception | None = None
    for i in range(1, attempts + 1):
        try:
            conn = pymssql.connect(
                server=HOST,
                port=PORT,
                user="sa",
                password=SA_PASSWORD,
                autocommit=True,
                login_timeout=5,
            )
            print(f"Connected to {HOST}:{PORT}")
            return conn
        except Exception as e:  # noqa: BLE001 - retry on any connect error
            last_err = e
            print(f"Attempt {i}/{attempts}: MSSQL not ready ({e}); waiting {delay}s")
            time.sleep(delay)
    raise SystemExit(f"Could not connect to MSSQL after {attempts} attempts: {last_err}")


def main() -> int:
    if os.getenv("MSSQL_SKIP_DB_BOOTSTRAP", "").strip().lower() in {"1", "true", "yes"}:
        print(
            "MSSQL_SKIP_DB_BOOTSTRAP set — skipping database bootstrap "
            "(data-baked image already provides the application databases)."
        )
        return 0
    conn = connect_with_retry()
    cursor = conn.cursor()
    for env_var, default in DATABASES:
        name = os.getenv(env_var, default)
        cursor.execute(
            "IF DB_ID(%s) IS NULL EXEC('CREATE DATABASE [' + %s + ']')",
            (name, name),
        )
        print(f"Database ready: {name}")
    cursor.close()
    conn.close()
    print("All databases created.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

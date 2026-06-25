# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DDL Loader Utility

Provides functions to load and execute SQL DDL files from the infrastructure/ddl/ directory.
"""

from pathlib import Path

import dagster as dg
from core.resources import MSSQLResource


def get_ddl_path() -> Path:
    """Get the path to the ddl/ directory."""
    return Path(__file__).parent.parent / "ddl"


def load_sql_file(relative_path: str) -> str:
    """
    Load a SQL file from the ddl/ directory.

    Args:
        relative_path: Path relative to ddl/ directory (e.g., "common/00_schemas.sql")

    Returns:
        SQL content as string

    Raises:
        FileNotFoundError: If the SQL file doesn't exist
    """
    ddl_path = get_ddl_path()
    full_path = ddl_path / relative_path

    if not full_path.exists():
        raise FileNotFoundError(f"SQL file not found: {full_path}")

    with open(full_path, encoding="utf-8") as f:
        return f.read()


def load_sql_files_from_directory(
    relative_dir: str, pattern: str = "*.sql"
) -> list[tuple[str, str]]:
    """
    Load all SQL files from a directory, sorted by filename.

    Args:
        relative_dir: Path relative to ddl/ directory (e.g., "common/functions")
        pattern: Glob pattern for matching files (default: "*.sql")

    Returns:
        List of (filename, sql_content) tuples, sorted by filename
    """
    ddl_path = get_ddl_path()
    dir_path = ddl_path / relative_dir

    if not dir_path.exists():
        return []

    sql_files = sorted(dir_path.glob(pattern))
    result = []

    for sql_file in sql_files:
        with open(sql_file, encoding="utf-8") as f:
            result.append((sql_file.name, f.read()))

    return result


def execute_sql(
    mssql_resource: MSSQLResource,
    sql: str,
    context: dg.AssetExecutionContext | None = None,
    description: str = "SQL",
    collect_errors: bool = False,
    autocommit: bool = False,
) -> int:
    """
    Execute a SQL statement using the provided MSSQL resource.

    Args:
        mssql_resource: The MSSQL resource to use
        sql: SQL to execute
        context: Optional Dagster context for logging
        description: Description for logging
        collect_errors: If True, continue executing all batches and collect errors
            instead of failing on the first one. Raises after all batches have run.

    Returns:
        Number of rows affected (or 0 for DDL statements)
    """
    if context:
        context.log.info(f"Executing {description}...")

    with mssql_resource.get_connection() as conn:
        cursor = conn.cursor()
        previous_autocommit = getattr(conn, "autocommit", None)
        if previous_autocommit is not None:
            conn.autocommit = autocommit
        try:
            batches = split_sql_batches(sql)
            errors: list[tuple[int, str, Exception]] = []

            total_affected = 0
            for i, batch in enumerate(batches, 1):
                batch = batch.strip()
                if not batch:
                    continue

                try:
                    cursor.execute(batch)
                    total_affected += cursor.rowcount if cursor.rowcount > 0 else 0
                except Exception as e:
                    batch_preview = batch[:200].replace("\n", " ")
                    if collect_errors:
                        if context:
                            context.log.error(f"[batch {i}] {e}\n  SQL: {batch_preview}")
                        errors.append((i, batch_preview, e))
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        cursor.close()
                        cursor = conn.cursor()
                    else:
                        if context:
                            context.log.error(f"Error in batch {i}: {e}")
                        raise

            if previous_autocommit is not None:
                conn.commit()
        finally:
            if previous_autocommit is not None:
                conn.autocommit = previous_autocommit
            cursor.close()

    if errors:
        summary = "\n".join(
            f"  Batch {num}: {err}\n    SQL: {sql_preview}" for num, sql_preview, err in errors
        )
        raise RuntimeError(
            f"{description}: {len(errors)} batch(es) failed out of {len(batches)}:\n{summary}"
        )

    if context:
        context.log.info(f"Completed {description}")

    return total_affected


def split_sql_batches(sql: str) -> list[str]:
    """
    Split SQL into batches separated by GO statements.

    SQL Server uses GO as a batch separator. This function handles that
    by splitting the script into separate executable batches.
    """
    import re

    # Split on GO statements (case insensitive, must be on its own line)
    # Pattern: start of line, optional whitespace, GO, optional whitespace, end of line
    batches = re.split(r"^\s*GO\s*$", sql, flags=re.MULTILINE | re.IGNORECASE)

    return [batch.strip() for batch in batches if batch.strip()]


def execute_sql_file(
    mssql_resource: MSSQLResource,
    relative_path: str,
    context: dg.AssetExecutionContext | None = None,
    autocommit: bool = False,
) -> int:
    """
    Load and execute a SQL file.

    Args:
        mssql_resource: The MSSQL resource to use
        relative_path: Path relative to ddl/ directory
        context: Optional Dagster context for logging

    Returns:
        Number of rows affected
    """
    sql = load_sql_file(relative_path)
    return execute_sql(
        mssql_resource,
        sql,
        context,
        description=relative_path,
        autocommit=autocommit,
    )


def execute_sql_files_from_directory(
    mssql_resource: MSSQLResource,
    relative_dir: str,
    context: dg.AssetExecutionContext | None = None,
    pattern: str = "*.sql",
    autocommit: bool = False,
) -> dict[str, int]:
    """
    Load and execute all SQL files from a directory in sorted order.

    Runs every file and every batch, collecting ALL errors so they can be
    diagnosed in a single run instead of failing on the first one.

    Args:
        mssql_resource: The MSSQL resource to use
        relative_dir: Path relative to ddl/ directory
        context: Optional Dagster context for logging
        pattern: Glob pattern for matching files

    Returns:
        Dictionary mapping filename to rows affected
    """
    sql_files = load_sql_files_from_directory(relative_dir, pattern)
    results = {}
    file_errors: list[tuple[str, Exception]] = []

    for filename, sql in sql_files:
        if context:
            context.log.info(f"Executing {relative_dir}/{filename}...")

        try:
            rows_affected = execute_sql(
                mssql_resource,
                sql,
                context=context,
                description=f"{relative_dir}/{filename}",
                collect_errors=True,
                autocommit=autocommit,
            )
            results[filename] = rows_affected
        except Exception as e:
            file_errors.append((filename, e))
            if context:
                context.log.error(f"❌ {filename}: {e}")

    if file_errors:
        summary = "\n\n".join(f"=== {fn} ===\n{err}" for fn, err in file_errors)
        raise RuntimeError(f"{relative_dir}: {len(file_errors)} file(s) had errors:\n\n{summary}")

    return results


def execute_legacy_sql2019_functions_if_needed(
    mssql_resource: MSSQLResource,
    context: dg.AssetExecutionContext | None = None,
) -> dict[str, int]:
    """Deploy legacy SQL 2019 compatibility functions when GREATEST() is unavailable."""
    with mssql_resource.get_connection() as conn:
        from utils.sql.greatest_int3 import detect_mssql_greatest_support

        if detect_mssql_greatest_support(conn):
            if context:
                context.log.info("GREATEST() available — skipping common/functions/legacy_sql2019")
            return {}

    if context:
        context.log.info(
            "GREATEST() unavailable (SQL Server 2019 dev) — deploying legacy_sql2019 functions"
        )
    return execute_sql_files_from_directory(
        mssql_resource, "common/functions/legacy_sql2019", context
    )


# Tracker table that records which migration filenames have been applied per
# database. Created on demand by `execute_migrations`.
_MIGRATION_HISTORY_BOOTSTRAP_SQL = """
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'migration_history'
)
BEGIN
    CREATE TABLE [meta].[migration_history] (
        [filename]    NVARCHAR(260)  NOT NULL,
        [applied_at]  DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_migration_history] PRIMARY KEY CLUSTERED ([filename])
    );
END
""".strip()


def execute_migrations(
    mssql_resource: MSSQLResource,
    relative_dir: str,
    context: dg.AssetExecutionContext | None = None,
) -> dict[str, str]:
    """
    Execute migration SQL files, recording each successful filename in
    [meta].[migration_history] so a file is applied at most once per database.

    Behavior:
        1. Bootstrap [meta].[migration_history] (idempotent CREATE).
        2. Load the set of already-applied filenames from the tracker.
        3. List the migration files on disk in sorted (filename) order.
        4. For each file: if its filename is in the applied set, mark "skipped";
           otherwise execute the file's SQL and, on success, INSERT the
           filename into the tracker.
        5. On any migration error, propagate immediately (fail-fast). Files
           already inserted in this run remain recorded; later files do not run.

    Args:
        mssql_resource: The MSSQL resource to use.
        relative_dir: Path relative to the ddl/ directory (e.g. "migrations").
        context: Optional Dagster context for logging.

    Returns:
        Dictionary mapping filename to "applied" or "skipped".
    """
    # 1. Bootstrap the tracker and load the applied set in a single connection.
    with mssql_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(_MIGRATION_HISTORY_BOOTSTRAP_SQL)
            conn.commit()
            cursor.execute("SELECT filename FROM [meta].[migration_history]")
            applied: set[str] = {row[0] for row in cursor.fetchall()}
        finally:
            cursor.close()

    # 2. Discover and partition files.
    sql_files = load_sql_files_from_directory(relative_dir)
    results: dict[str, str] = {}

    # Mark already-applied files first so the result dict carries every file.
    for filename, _ in sql_files:
        if filename in applied:
            results[filename] = "skipped"
            if context:
                context.log.info(f"Skipping {relative_dir}/{filename} (already applied)")

    # 3. Execute pending files in sorted order, fail-fast.
    for filename, sql in sql_files:
        if filename in applied:
            continue

        execute_sql(
            mssql_resource,
            sql,
            context=context,
            description=f"{relative_dir}/{filename}",
        )

        with mssql_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "INSERT INTO [meta].[migration_history] (filename) VALUES (?)",
                    (filename,),
                )
                conn.commit()
            finally:
                cursor.close()

        results[filename] = "applied"
        if context:
            context.log.info(f"Applied {relative_dir}/{filename}")

    return results


def baseline_migrations(
    mssql_resource: MSSQLResource,
    relative_dir: str = "migrations",
    context: dg.AssetExecutionContext | None = None,
    only: list[str] | None = None,
) -> dict[str, str]:
    """
    Mark migration files as applied WITHOUT executing their SQL ("baseline" /
    "stamp to head"): record each filename in [meta].[migration_history] so a
    later ``execute_migrations`` skips it.

    Use this when a database is provisioned *fresh from the current ``tables/``
    DDL*, which already incorporates the net effect of every historical
    migration (migrations only patch OLDER, already-deployed databases). On such
    a database running those migrations is at best redundant and at worst fails:
    many of them patch ``dim``/``fct``/``conceptmap`` tables that the
    infrastructure DAG creates *after* the migrations step, so they reference
    objects that do not exist yet. Baselining records them as applied so only
    genuinely new migrations run later.

    File discovery and ordering are shared with ``execute_migrations`` (both use
    ``load_sql_files_from_directory``), so the baselined set can never drift from
    the set the runner would execute — add a migration and it is baselined
    automatically, no hand-maintained list.

    SAFETY — only baseline a database whose schema is known to be at migration
    head (e.g. freshly built from the current DDL). Baselining a database that
    is genuinely *behind* head permanently skips the intervening migrations.
    Because the effect of every migration is assumed to be reproducible from the
    current DDL + seed assets, a migration that introduces state NOT in the DDL
    (e.g. a pure reference-data INSERT) must NOT be baselined — pass an explicit
    ``only`` allowlist to exclude it and let it run via ``execute_migrations``.

    Args:
        mssql_resource: target database resource.
        relative_dir: migrations directory relative to ``ddl/`` (default
            ``"migrations"``).
        context: optional Dagster context for logging.
        only: optional explicit allowlist of filenames to baseline. When given,
            every name must exist on disk (raises otherwise) and only those are
            stamped. When omitted, ALL on-disk migration files in
            ``relative_dir`` are baselined (dynamic — the common case).

    Returns:
        ``{filename: "baselined" | "already-present"}`` for every targeted file.
    """
    sql_files = load_sql_files_from_directory(relative_dir)
    on_disk = [name for name, _ in sql_files]
    if not on_disk:
        # load_sql_files_from_directory returns [] for a missing/empty dir;
        # refuse rather than silently baseline nothing.
        raise RuntimeError(
            f"baseline_migrations: no .sql files found under ddl/{relative_dir} "
            "— refusing to baseline an empty set (check the directory path)."
        )

    if only is not None:
        on_disk_set = set(on_disk)
        missing = [name for name in only if name not in on_disk_set]
        if missing:
            raise RuntimeError(
                "baseline_migrations: allowlist entries not found on disk under "
                f"ddl/{relative_dir}: {missing}"
            )
        targets = [name for name in on_disk if name in set(only)]
    else:
        targets = on_disk

    with mssql_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # Bootstrap assumes the `meta` schema exists (common/00_schemas.sql
            # creates it). Create it defensively in case baseline runs first.
            cursor.execute("IF SCHEMA_ID('meta') IS NULL EXEC('CREATE SCHEMA [meta]')")
            cursor.execute(_MIGRATION_HISTORY_BOOTSTRAP_SQL)
            conn.commit()

            cursor.execute("SELECT filename FROM [meta].[migration_history]")
            applied: set[str] = {row[0] for row in cursor.fetchall()}

            results: dict[str, str] = {}
            for filename in targets:
                if filename in applied:
                    results[filename] = "already-present"
                    continue
                # Record as applied WITHOUT executing the file's SQL.
                cursor.execute(
                    "INSERT INTO [meta].[migration_history] (filename) VALUES (?)",
                    (filename,),
                )
                results[filename] = "baselined"
            conn.commit()
        finally:
            cursor.close()

    if context:
        n_new = sum(1 for v in results.values() if v == "baselined")
        context.log.info(
            f"Baselined {n_new} migration(s) under ddl/{relative_dir} "
            f"({len(results) - n_new} already present); SQL was NOT executed."
        )
    return results

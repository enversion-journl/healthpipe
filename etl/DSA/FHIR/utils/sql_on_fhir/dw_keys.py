# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""dw_ek_Borger, dw_sk_Borger and dw_ek_Organisation on the flat fhir.* tables.

Runs after the SoF load of a table and updates the keys in place. A row is re-evaluated
when the fhir row changed, when its borger changed in dim.Borger, or when its
organisation changed in dim.Organisation (for an inherited organisation: when the referenced
row changed); it is written only when a key gets a new value,
and only then does ``dw_timestamp_update`` move, so the DSO sync stays a delta.

Lookup rules (see docs/architecture/fhir_dw_keys_trin2.md):

* ``dw_ek_Borger``: dim.Borger on ``fhir_Patient_id``, also when every version of the
  borger is soft-deleted.
* ``dw_sk_Borger``: the live version whose ``[dw_valid_from, dw_valid_to)`` holds the row's
  time; on overlap the newest ``dw_valid_from`` wins, as in ``dbo.GetBorgerSK_TVF``. The
  function is not reused because it reads WITH (NOLOCK), and a persisted key read from a
  rolled-back dim_Borger version would never be re-evaluated.
* ``dw_ek_Organisation``: dim.Organisation on ``fhir_Organization_id``. A table with more
  than one organisation reference carries one key per role, ``dw_ek_Organisation_<role>``.
  A table that references another keyed fhir table instead of an organisation inherits
  that table's ``dw_ek_Organisation``.
* -1 when the reference is missing or not found; ``dw_sk_Borger`` is also -1 when the row
  has no time. The fhir row's own ``dw_is_deleted`` plays no part.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

UNKNOWN = -1

# dim_Organisation logs no DFM run and commits once at its end, so its read bound cannot
# be held back to a run in flight; re-reading this much before the run start covers one.
# The same bound is used for the fhir table an inherited organisation is read from.
ORGANISATION_READ_MARGIN = timedelta(hours=1)

BATCH_ROWS = 250_000


@dataclass(frozen=True)
class OrganisationKey:
    """One organisation reference column and the dw-key it fills.

    With ``via`` the column references that table's primary key, and the key is that
    table's ``dw_ek_Organisation``.
    """

    column: str
    role: str | None = None
    via: "DWKeysConfig | None" = None

    def __post_init__(self) -> None:
        if self.via is not None and [o.key_column for o in self.via.organisations] != [
            "dw_ek_Organisation"
        ]:
            raise ValueError(f"{self.via.table}: inherit only from a single dw_ek_Organisation")

    @property
    def key_column(self) -> str:
        return f"dw_ek_Organisation_{self.role}" if self.role else "dw_ek_Organisation"

    @property
    def alias(self) -> str:
        return f"organisation_{self.role}" if self.role else "organisation"


@dataclass(frozen=True)
class DWKeysConfig:
    """Which columns of one flat table carry the references the keys are looked up from."""

    asset_suffix: str
    table: str
    primary_key: str
    organisations: tuple[OrganisationKey, ...]
    patient_column: str | None = None
    time_column: str | None = None
    # The time column holds the raw FHIR dateTime text; the lookup reads its clock time.
    time_column_is_text: bool = False

    def __post_init__(self) -> None:
        if (self.patient_column is None) != (self.time_column is None):
            raise ValueError(f"{self.table}: patient_column and time_column must be set together")
        if self.time_column_is_text and self.time_column is None:
            raise ValueError(f"{self.table}: time_column_is_text needs a time_column")
        if not self.organisations:
            raise ValueError(f"{self.table}: needs at least one organisation column")
        if len(self.organisations) > 1 and not all(o.role for o in self.organisations):
            raise ValueError(f"{self.table}: more than one organisation column needs a role each")
        if len({o.key_column for o in self.organisations}) != len(self.organisations):
            raise ValueError(f"{self.table}: organisation roles must be distinct")

    @property
    def has_borger(self) -> bool:
        return self.patient_column is not None

    @property
    def asset_name(self) -> str:
        return f"fhir_dw_keys_{self.asset_suffix}"

    @property
    def checkpoint_fhir(self) -> str:
        return f"{self.table}_DWKeys"

    @property
    def checkpoint_borger(self) -> str:
        return f"{self.table}_DWKeys_Borger"

    @property
    def checkpoint_organisation(self) -> str:
        return f"{self.table}_DWKeys_Organisation"


def key_columns(cfg: DWKeysConfig) -> list[str]:
    columns = ["dw_ek_Borger", "dw_sk_Borger"] if cfg.has_borger else []
    return [*columns, *(o.key_column for o in cfg.organisations)]


def key_sources(cfg: DWKeysConfig) -> dict[str, str]:
    """Per key column, where a -1 comes from."""
    sources = (
        {"dw_ek_Borger": "dim.Borger", "dw_sk_Borger": "dim.Borger, or no time"}
        if cfg.has_borger
        else {}
    )
    for organisation in cfg.organisations:
        via = organisation.via
        sources[organisation.key_column] = (
            f"fhir.{via.table}" if via is not None else "dim.Organisation"
        )
    return sources


def time_sql(cfg: DWKeysConfig, alias: str = "f") -> str:
    """The row's time as DATETIME2(0); NULL when absent or unparseable.

    A text column is read as the clock time it carries, as the EpisodeOfCare period
    columns are loaded: any offset is dropped, not converted, and YYYY / YYYY-MM padded.
    """
    column = f"{alias}.[{cfg.time_column}]"
    if not cfg.time_column_is_text:
        return column
    return (
        f"CASE WHEN {column} LIKE N'[0-9][0-9][0-9][0-9]' "
        f"THEN TRY_CONVERT(DATETIME2(0), {column} + N'-01-01', 120) "
        f"WHEN {column} LIKE N'[0-9][0-9][0-9][0-9]-[0-9][0-9]' "
        f"THEN TRY_CONVERT(DATETIME2(0), {column} + N'-01', 120) "
        f"ELSE TRY_CONVERT(DATETIME2(0), REPLACE(LEFT({column}, 19), N'T', N' '), 120) END"
    )


def lookup_sql(cfg: DWKeysConfig, alias: str = "f") -> tuple[str, str]:
    """(SELECT list producing the key columns, OUTER APPLY blocks) for the row ``alias``."""
    expressions = []
    applies = []
    if cfg.has_borger:
        patient = f"{alias}.[{cfg.patient_column}]"
        expressions += [
            f"COALESCE(borger_entity.dw_ek_Borger, {UNKNOWN}) AS dw_ek_Borger",
            f"COALESCE(borger_version.dw_sk_Borger, {UNKNOWN}) AS dw_sk_Borger",
        ]
        applies += [
            f"""OUTER APPLY (
                SELECT TOP (1) d.dw_ek_Borger
                FROM [dim].[Borger] AS d
                WHERE d.fhir_Patient_id = {patient}
                ORDER BY d.dw_is_deleted, d.dw_valid_from DESC
            ) AS borger_entity""",
            f"CROSS APPLY (SELECT {time_sql(cfg, alias)} AS t) AS row_time",
            f"""OUTER APPLY (
                SELECT TOP (1) d.dw_sk_Borger
                FROM [dim].[Borger] AS d
                WHERE d.fhir_Patient_id = {patient}
                  AND d.dw_is_deleted = 0
                  AND row_time.t >= d.dw_valid_from
                  AND row_time.t < d.dw_valid_to
                ORDER BY d.dw_valid_from DESC, d.dw_sk_Borger DESC
            ) AS borger_version""",
        ]
    for organisation in cfg.organisations:
        expressions.append(
            f"COALESCE({organisation.alias}.dw_ek_Organisation, {UNKNOWN}) "
            f"AS {organisation.key_column}"
        )
        via = organisation.via
        if via is not None:
            applies.append(
                f"""OUTER APPLY (
            SELECT v.dw_ek_Organisation
            FROM [fhir].[{via.table}] AS v
            WHERE v.[{via.primary_key}] = {alias}.[{organisation.column}]
        ) AS {organisation.alias}"""
            )
            continue
        applies.append(
            f"""OUTER APPLY (
            SELECT TOP (1) o.dw_ek_Organisation
            FROM [dim].[Organisation] AS o
            WHERE o.fhir_Organization_id = {alias}.[{organisation.column}]
            ORDER BY o.dw_is_deleted, o.dw_ek_Organisation DESC
        ) AS {organisation.alias}"""
        )
    return ",\n            ".join(expressions), "\n        ".join(applies)


def candidates_table_sql(cfg: DWKeysConfig) -> str:
    """Create an empty ``#candidates`` with the table's primary-key column.

    Run without parameters: a temp table created inside a parameterised batch (executed
    via sp_executesql) is dropped when that batch ends.
    """
    pk = f"[{cfg.primary_key}]"
    return f"""
        SET NOCOUNT ON;
        DROP TABLE IF EXISTS #candidates;
        SELECT TOP (0) f.{pk} INTO #candidates FROM [fhir].[{cfg.table}] AS f;
        CREATE UNIQUE CLUSTERED INDEX [IX_candidates] ON #candidates ({pk});
    """


def candidates_sql(cfg: DWKeysConfig) -> str:
    """Fill ``#candidates`` with the primary keys of rows to re-evaluate; SELECT the count.

    Parameters, in order: the fhir checkpoint, then the borger checkpoint (only when the
    table carries borger keys), then the organisation checkpoint once per organisation
    column. An inherited organisation follows the rows of the table it is read from, whose
    ``dw_timestamp_update`` moves when SoF re-merges the row or its own key changes, so a
    full load of that table makes every referencing row a candidate. ``>=``, not ``>``: the
    fhir and dim.Borger timestamps and the run start are rounded to whole seconds, so a write
    in the checkpoint's second carries the checkpoint's value.
    """
    table = f"[fhir].[{cfg.table}]"
    pk = f"[{cfg.primary_key}]"
    parts = [f"SELECT f.{pk} FROM {table} AS f WHERE f.dw_timestamp_update >= ?"]
    if cfg.has_borger:
        parts.append(
            f"""SELECT f.{pk} FROM {table} AS f
            WHERE f.[{cfg.patient_column}] IN (
                SELECT d.fhir_Patient_id FROM [dim].[Borger] AS d
                WHERE d.dw_timestamp_update >= ?
            )"""
        )
    for organisation in cfg.organisations:
        via = organisation.via
        source = (
            f"SELECT v.[{via.primary_key}] FROM [fhir].[{via.table}] AS v"
            if via is not None
            else "SELECT o.fhir_Organization_id FROM [dim].[Organisation] AS o"
        )
        source_alias = "v" if via is not None else "o"
        parts.append(
            f"""SELECT f.{pk} FROM {table} AS f
        WHERE f.[{organisation.column}] IN (
            {source}
            WHERE {source_alias}.dw_timestamp_update >= ?
        )"""
        )
    union = "\n        UNION\n        ".join(parts)
    return f"""
        SET NOCOUNT ON;
        INSERT INTO #candidates ({pk})
        {union};
        SELECT COUNT_BIG(*) FROM #candidates;
    """


def batch_bound_sql(cfg: DWKeysConfig, *, from_candidates: bool) -> str:
    """Upper primary key of the next batch after ``?``; NULL when nothing is left."""
    source = "#candidates" if from_candidates else f"[fhir].[{cfg.table}]"
    pk = f"[{cfg.primary_key}]"
    return f"""
        SELECT MAX(b.{pk}) FROM (
            SELECT TOP ({BATCH_ROWS}) s.{pk} FROM {source} AS s
            WHERE (? IS NULL OR s.{pk} > ?)
            ORDER BY s.{pk}
        ) AS b
    """


def update_sql(cfg: DWKeysConfig, *, from_candidates: bool) -> str:
    """UPDATE one batch ``(lower, upper]`` and SELECT the number of rows written.

    Parameters, in order: lower (twice, NULL for the first batch) and upper.
    """
    pk = f"[{cfg.primary_key}]"
    expressions, applies = lookup_sql(cfg, "f")
    keys = key_columns(cfg)
    assignments = ",\n            ".join(f"f.[{c}] = computed.[{c}]" for c in keys)
    changed = "\n           OR ".join(f"f.[{c}] <> computed.[{c}]" for c in keys)
    join = f"JOIN #candidates AS k ON k.{pk} = f.{pk}" if from_candidates else ""
    return f"""
        SET NOCOUNT ON;
        UPDATE f
        SET {assignments},
            f.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
        FROM [fhir].[{cfg.table}] AS f
        {join}
        CROSS APPLY (
            SELECT {expressions}
            FROM (SELECT 1 AS x) AS e
            {applies}
        ) AS computed
        WHERE (? IS NULL OR f.{pk} > ?)
          AND f.{pk} <= ?
          AND ({changed});
        SELECT @@ROWCOUNT;
    """


def distribution_sql(cfg: DWKeysConfig, *, from_candidates: bool) -> str:
    """Per key: rows with a found key and with -1, in the table or its candidates.

    For a text time column also the rows whose time is present but unparseable.
    """
    pk = f"[{cfg.primary_key}]"
    parts = []
    for c in key_columns(cfg):
        parts += [
            f"COALESCE(SUM(CASE WHEN f.[{c}] > 0 THEN 1 ELSE 0 END), 0) AS [{c}_found]",
            f"COALESCE(SUM(CASE WHEN f.[{c}] = {UNKNOWN} THEN 1 ELSE 0 END), 0) AS [{c}_minus1]",
        ]
    if cfg.time_column_is_text:
        parts.append(
            f"COALESCE(SUM(CASE WHEN f.[{cfg.time_column}] IS NOT NULL "
            f"AND ({time_sql(cfg)}) IS NULL THEN 1 ELSE 0 END), 0) AS [time_unparseable]"
        )
    join = f"JOIN #candidates AS k ON k.{pk} = f.{pk}" if from_candidates else ""
    return f"SELECT {', '.join(parts)} FROM [fhir].[{cfg.table}] AS f {join}"


def fhir_watermark_sql(cfg: DWKeysConfig) -> str:
    return (
        "SELECT CONVERT(NVARCHAR(50), MAX(dw_timestamp_update), 121) " f"FROM [fhir].[{cfg.table}]"
    )


def organisation_read_bound(started_at: str | None) -> str | None:
    """The run start less :data:`ORGANISATION_READ_MARGIN`, in the checkpoint's text form."""
    if started_at is None:
        return None
    bound = datetime.fromisoformat(started_at[:19]) - ORGANISATION_READ_MARGIN
    return bound.isoformat(sep=" ")


def resolve_scope(mode: str, checkpoints: dict[str, str | None]) -> tuple[bool, str | None]:
    """(full table?, reason) for a run mode and the stored checkpoints.

    A full pass is taken in ``full_load`` mode and whenever a checkpoint is missing: a
    delta needs every source's watermark, or rows changed before the first run are missed.
    """
    if mode == "full_load":
        return True, "full_load"
    if mode != "delta":
        raise ValueError(f"mode {mode!r} is not supported; use 'delta' or 'full_load'")
    missing = [name for name, value in checkpoints.items() if value is None]
    if missing:
        return True, f"missing checkpoint: {', '.join(missing)}"
    return False, None


def run_dw_keys(
    cursor: Any,
    conn: Any,
    cfg: DWKeysConfig,
    *,
    full: bool,
    checkpoints: dict[str, str | None],
    log: Any,
) -> dict[str, Any]:
    """Re-evaluate the keys of the table (or its candidates) in primary-key batches.

    Counts found keys and -1 per key column among the rows evaluated, and logs a warning
    for the -1s: a reference the dimensions do not have, or a row without a time.
    """
    candidates: int | None = None
    if not full:
        params = [checkpoints[cfg.checkpoint_fhir]]
        if cfg.has_borger:
            params.append(checkpoints[cfg.checkpoint_borger])
        params += [checkpoints[cfg.checkpoint_organisation]] * len(cfg.organisations)
        cursor.execute(candidates_table_sql(cfg))
        conn.commit()
        cursor.execute(candidates_sql(cfg), params)
        candidates = int(cursor.fetchone()[0])
        conn.commit()
        log.info("%s: %d candidate rows", cfg.table, candidates)

    bound = batch_bound_sql(cfg, from_candidates=not full)
    update = update_sql(cfg, from_candidates=not full)
    lower: Any = None
    written = batches = 0
    if candidates != 0:
        while True:
            cursor.execute(bound, (lower, lower))
            row = cursor.fetchone()
            upper = row[0] if row else None
            if upper is None:
                break
            cursor.execute(update, (lower, lower, upper))
            written += int(cursor.fetchone()[0] or 0)
            conn.commit()
            batches += 1
            lower = upper
    log.info("%s: %d rows written in %d batch(es)", cfg.table, written, batches)

    distribution = None
    if candidates != 0:
        cursor.execute(distribution_sql(cfg, from_candidates=not full))
        names = [d[0] for d in cursor.description]
        distribution = dict(zip(names, (int(v or 0) for v in cursor.fetchone()), strict=True))
        unknown = {
            c: distribution[f"{c}_minus1"] for c in key_columns(cfg) if distribution[f"{c}_minus1"]
        }
        if unknown:
            sources = key_sources(cfg)
            log.warning(
                "%s: key -1 among the rows evaluated: %s",
                cfg.table,
                ", ".join(f"{c}={n} ({sources[c]})" for c, n in unknown.items()),
            )
        if distribution.get("time_unparseable"):
            log.warning(
                "%s: %d rows evaluated with a %s that cannot be parsed",
                cfg.table,
                distribution["time_unparseable"],
                cfg.time_column,
            )

    cursor.execute(fhir_watermark_sql(cfg))
    row = cursor.fetchone()
    return {
        "candidates": candidates,
        "rows_written": written,
        "batches": batches,
        "distribution": distribution,
        "fhir_watermark": row[0] if row else None,
    }

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Målebænk for SOF-MERGE'n: dagens ét-statement mod portioneret MERGE med indeks fra.

Kører den rigtige SOF-kode (``extract_and_merge_sof_view``) mod en KOPI af måltabellen
på staging, så INSERT-stien kan måles for et vindue af nye rækker uden at føre data ind
i den rigtige tabel. Et replay mod den rigtige tabel ville ikke måle det samme: rækkerne
findes allerede, MERGE'n matcher dem uden ændring og skriver intet.

Pr. variant: kopiér måltabellen (SELECT INTO), genskab dens PK, defaults og sekundære
indeks, hent vinduet fra Aidbox og MERGE det ind i kopien med variantens plan, gem
fasetiderne, drop kopien. Kopien fylder ca. som måltabellen (staging QR ~35 GB), og
vinduets rækker staget i tempdb; pladsvogteren kræver ``min_free_gb`` ud over skønnet.

Kræver ingen lås på måltabellen og ændrer den ikke.
"""

# Ingen ``from __future__ import annotations``: dg.Config kan ikke opløses fra
# strengannoteringer (PEP 563).
import time
from typing import Any

import dagster as dg
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.sql_on_fhir.sof_utils import (
    SOF_DEFAULT_MERGE_BATCH_ROWS,
    SOF_RESOURCE_CONFIGS,
    SofBackfillPlan,
    extract_and_merge_sof_view,
)

BENCH_SUFFIX = "_bench"
VARIANTS: dict[str, tuple[int, bool]] = {
    # navn -> (batch_rows, disable_indexes)
    "single_on": (0, False),  # dagens adfærd
    "single_off": (0, True),
    "batched_on": (SOF_DEFAULT_MERGE_BATCH_ROWS, False),
    "batched_off": (SOF_DEFAULT_MERGE_BATCH_ROWS, True),  # foreslået backfill-tilstand
}


class SofMergeBenchmarkConfig(dg.Config):
    """Vindue (Aidbox ``ts``, ISO 8601) og varianter. Standard er dagens adfærd mod den
    foreslåede backfill-tilstand på QuestionnaireResponse."""

    view: str = "questionnaire_response_view"
    window_from: str
    window_to: str
    variants: list[str] = ["single_on", "batched_off"]
    batch_rows: int = SOF_DEFAULT_MERGE_BATCH_ROWS
    min_free_gb: float = 60.0
    keep_copy: bool = False
    # 0 = hele måltabellen (repræsentativt for målets størrelse, men kopien fylder som
    # tabellen: 97 GB for QuestionnaireResponse på staging 8/9). Et positivt tal kopierer
    # kun så mange rækker — mindre midlertidig plads, mindre repræsentativt for MERGE-
    # prisen mod et stort mål; sammenligningen mellem varianter holder stadig.
    copy_rows: int = 0


def bench_table_name(table: str) -> str:
    return f"{table}{BENCH_SUFFIX}"


def validate_variants(variants: list[str]) -> list[str]:
    unknown = [v for v in variants if v not in VARIANTS]
    if unknown:
        raise ValueError(f"Ukendte varianter {unknown}; kendte: {list(VARIANTS)}")
    if not variants:
        raise ValueError("Mindst én variant skal måles")
    return list(dict.fromkeys(variants))


def plan_for_variant(variant: str, batch_rows: int) -> SofBackfillPlan:
    rows, disable = VARIANTS[variant]
    return SofBackfillPlan(
        active=bool(rows or disable),
        batch_rows=batch_rows if rows else 0,
        disable_indexes=disable,
        reason=f"benchmark variant {variant}",
    )


def table_size_sql(table: str) -> str:
    return f"""
        SELECT
            SUM(ps.reserved_page_count) * 8.0 / 1024 AS total_mb,
            SUM(CASE WHEN ps.index_id IN (0, 1) THEN ps.row_count ELSE 0 END) AS row_count
        FROM sys.dm_db_partition_stats AS ps
        WHERE ps.object_id = OBJECT_ID(N'[fhir].[{table}]')
    """


def free_space_gb_sql() -> str:
    return """
        SELECT MIN(vs.available_bytes) / 1073741824.0 AS free_gb
        FROM sys.database_files AS f
        CROSS APPLY sys.dm_os_volume_stats(DB_ID(), f.file_id) AS vs
        WHERE f.type = 0
    """


def index_definitions_sql(table: str) -> str:
    """Én række pr. indeks på måltabellen: navn, type, unik, PK, nøgle- og INCLUDE-kolonner."""
    return f"""
        SELECT
            i.name,
            i.type,
            i.is_unique,
            i.is_primary_key,
            STUFF((
                SELECT ', ' + QUOTENAME(c.name) + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
                FROM sys.index_columns ic
                JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
                ORDER BY ic.key_ordinal
                FOR XML PATH('')
            ), 1, 2, '') AS key_columns,
            STUFF((
                SELECT ', ' + QUOTENAME(c.name)
                FROM sys.index_columns ic
                JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
                ORDER BY ic.index_column_id
                FOR XML PATH('')
            ), 1, 2, '') AS include_columns
        FROM sys.indexes i
        WHERE i.object_id = OBJECT_ID(N'[fhir].[{table}]')
          AND i.type IN (1, 2)
          AND i.name IS NOT NULL
        ORDER BY i.index_id
    """


def default_definitions_sql(table: str) -> str:
    return f"""
        SELECT c.name, dc.definition
        FROM sys.default_constraints dc
        JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
        WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[{table}]')
    """


def computed_definitions_sql(table: str) -> str:
    return f"""
        SELECT c.name, cc.definition, cc.is_persisted
        FROM sys.computed_columns cc
        JOIN sys.columns c ON c.object_id = cc.object_id AND c.column_id = cc.column_id
        WHERE cc.object_id = OBJECT_ID(N'[fhir].[{table}]')
    """


def copy_layout_statements(
    copy: str,
    index_rows: list[tuple[Any, ...]],
    default_rows: list[tuple[Any, ...]],
    computed_rows: list[tuple[Any, ...]] | None = None,
) -> list[str]:
    """DDL, der giver SELECT INTO-kopien måltabellens beregnede kolonner, PK, defaults
    og sekundære indeks.

    SELECT INTO gør en beregnet kolonne til en almindelig kolonne, som MERGE'n så
    skulle udfylde selv; derfor droppes den og genskabes med måltabellens udtryk, før
    indeksene bygges. Navne får kopiens navn som præfiks, så de ikke støder sammen med
    måltabellens constraint-navne, der er unikke pr. skema.
    """
    statements: list[str] = []
    for col, definition, is_persisted in computed_rows or []:
        persisted = " PERSISTED" if is_persisted else ""
        statements.append(
            f"ALTER TABLE [fhir].[{copy}] DROP COLUMN [{col}]; "
            f"ALTER TABLE [fhir].[{copy}] ADD [{col}] AS {definition}{persisted};"
        )
    for col, definition in default_rows:
        statements.append(
            f"ALTER TABLE [fhir].[{copy}] ADD CONSTRAINT [DF_{copy}_{col}] DEFAULT {definition} FOR [{col}];"
        )
    for name, index_type, is_unique, is_primary_key, key_columns, include_columns in index_rows:
        clustered = "CLUSTERED" if int(index_type) == 1 else "NONCLUSTERED"
        if is_primary_key:
            statements.append(
                f"ALTER TABLE [fhir].[{copy}] ADD CONSTRAINT [PK_{copy}] PRIMARY KEY {clustered} ({key_columns});"
            )
            continue
        unique = "UNIQUE " if is_unique else ""
        include = f" INCLUDE ({include_columns})" if include_columns else ""
        statements.append(
            f"CREATE {unique}{clustered} INDEX [{name}] ON [fhir].[{copy}] ({key_columns}){include};"
        )
    return statements


def _one(cursor: Any, sql: str) -> tuple:
    cursor.execute(sql)
    row = cursor.fetchone()
    return tuple(row) if row else ()


def _size(cursor: Any, table: str) -> dict[str, float]:
    total_mb, rows = _one(cursor, table_size_sql(table))
    return {"total_mb": round(float(total_mb or 0), 1), "rows": int(rows or 0)}


def preflight(cursor: Any, cfg: SofMergeBenchmarkConfig, table: str, log: Any) -> dict[str, Any]:
    variants = validate_variants(cfg.variants)
    target = _size(cursor, table)
    if target["rows"] == 0:
        raise dg.Failure(f"[fhir].[{table}] er tom — intet at kopiere.")
    (free_gb,) = _one(cursor, free_space_gb_sql())
    free_gb = float(free_gb or 0)
    # Kopien fylder som måltabellen (eller den kopierede andel af den); vinduets staging
    # i tempdb regnes som en tiendedel.
    share = min(1.0, cfg.copy_rows / target["rows"]) if cfg.copy_rows > 0 else 1.0
    estimate_gb = target["total_mb"] * share * 1.1 / 1024
    if free_gb - estimate_gb < cfg.min_free_gb:
        raise dg.Failure(
            f"For lidt plads: {free_gb:.1f} GB fri, kopien skønnes til {estimate_gb:.1f} GB, "
            f"og der skal være mindst {cfg.min_free_gb:.0f} GB tilbage."
        )
    info = {
        "table": table,
        "target_rows": target["rows"],
        "target_total_mb": target["total_mb"],
        "free_gb_before": round(free_gb, 1),
        "estimate_gb": round(estimate_gb, 1),
        "copy_rows": cfg.copy_rows or target["rows"],
        "variants": variants,
        "window": [cfg.window_from, cfg.window_to],
    }
    log.info(
        "SOF-målebænk: fhir.%s %d rækker, %.0f MB; %.1f GB fri, kopi skønnet til %.1f GB; vindue %s -> %s.",
        table,
        target["rows"],
        target["total_mb"],
        free_gb,
        estimate_gb,
        cfg.window_from,
        cfg.window_to,
    )
    return info


def _drop_if_exists(cursor: Any, conn: Any, table: str) -> None:
    cursor.execute(
        f"IF OBJECT_ID(N'[fhir].[{table}]', N'U') IS NOT NULL DROP TABLE [fhir].[{table}];"
    )
    conn.commit()


def copy_sql(table: str, copy: str, copy_rows: int = 0) -> str:
    top = f"TOP ({int(copy_rows)}) " if copy_rows > 0 else ""
    return f"SELECT {top}* INTO [fhir].[{copy}] FROM [fhir].[{table}]"


def build_copy(
    cursor: Any, conn: Any, log: Any, table: str, copy: str, copy_rows: int = 0
) -> dict[str, Any]:
    """Kopiér måltabellen (eller ``copy_rows`` af den) og genskab dens layout."""
    out: dict[str, Any] = {}
    _drop_if_exists(cursor, conn, copy)
    started = time.monotonic()
    cursor.execute(copy_sql(table, copy, copy_rows))
    conn.commit()
    out["copy_rows_s"] = round(time.monotonic() - started, 1)
    cursor.execute(index_definitions_sql(table))
    index_rows = [tuple(r) for r in cursor.fetchall()]
    cursor.execute(default_definitions_sql(table))
    default_rows = [tuple(r) for r in cursor.fetchall()]
    cursor.execute(computed_definitions_sql(table))
    computed_rows = [tuple(r) for r in cursor.fetchall()]
    statements = copy_layout_statements(copy, index_rows, default_rows, computed_rows)
    layout_s: dict[str, float] = {}
    for statement in statements:
        name = (
            statement.split("[")[2].split("]")[0]
            if statement.startswith("CREATE")
            else statement.split("[")[3].split("]")[0]
        )
        t0 = time.monotonic()
        cursor.execute(statement)
        conn.commit()
        layout_s[name] = round(time.monotonic() - t0, 1)
    out["layout_s"] = layout_s
    out["layout_total_s"] = round(sum(layout_s.values()), 1)
    out["size_before"] = _size(cursor, copy)
    log.info(
        "Kopi fhir.%s: %d rækker kopieret på %.0f s, layout (%d objekter) på %.0f s, %.0f MB.",
        copy,
        out["size_before"]["rows"],
        out["copy_rows_s"],
        len(statements),
        out["layout_total_s"],
        out["size_before"]["total_mb"],
    )
    return out


def run_benchmark(
    fhir_database: Any,
    mssql_staging: Any,
    context: Any,
    cfg: SofMergeBenchmarkConfig,
) -> dict[str, Any]:
    view_cfg = SOF_RESOURCE_CONFIGS[cfg.view]
    table = view_cfg["mssql_table"]
    copy = bench_table_name(table)
    with mssql_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            info = preflight(cursor, cfg, table, context.log)
        finally:
            cursor.close()

    results: dict[str, dict[str, Any]] = {}
    for variant in info["variants"]:
        plan = plan_for_variant(variant, cfg.batch_rows)
        context.log.info("SOF-målebænk: variant %s — %s", variant, plan.describe())
        with mssql_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                result = build_copy(cursor, conn, context.log, table, copy, cfg.copy_rows)
            finally:
                cursor.close()

        timings: dict[str, Any] = {}
        started = time.monotonic()
        rows, affected = extract_and_merge_sof_view(
            fhir_database,
            mssql_staging,
            view_cfg["view_name"],
            view_cfg["resource_table"],
            view_cfg["id_column"],
            copy,
            view_cfg["primary_keys"],
            partition_start=cfg.window_from,
            partition_end=cfg.window_to,
            context=context,
            null_sentinel_columns=view_cfg.get("null_sentinel_columns"),
            normalized_datetime_columns=view_cfg.get("normalized_datetime_columns"),
            backfill_override=plan,
            timings=timings,
        )
        result.update(
            {
                "variant": variant,
                "plan": plan.describe(),
                "rows": rows,
                "rows_affected": affected,
                "wall_s": round(time.monotonic() - started, 1),
                "timings": timings,
            }
        )
        with mssql_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                result["size_after"] = _size(cursor, copy)
                if not cfg.keep_copy:
                    _drop_if_exists(cursor, conn, copy)
                (free_gb,) = _one(cursor, free_space_gb_sql())
                result["free_gb_after"] = round(float(free_gb or 0), 1)
            finally:
                cursor.close()
        context.log.info(
            "SOF-målebænk: %s — %d rækker, MERGE %.0f s i %d portioner, indeks fra %.0f s / "
            "genopbygget %.0f s, i alt %.0f s.",
            variant,
            rows,
            timings.get("merge_s", 0),
            timings.get("merge_batches", 0),
            timings.get("index_disable_s", 0),
            timings.get("index_rebuild_s", 0),
            result["wall_s"],
        )
        results[variant] = result
    return {"info": info, "results": results, "summary": summary_table(results)}


def summary_table(results: dict[str, dict[str, Any]]) -> str:
    rows = [
        "| Variant | Rækker | Fetch+stage s | Indeks på stage s | MERGE s | Portioner | Indeks fra s | Genopbyg s | I alt s | ms pr. række |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for variant, r in results.items():
        t = r.get("timings", {})
        n = r.get("rows") or 0
        per_row = (t.get("merge_s", 0) + t.get("index_rebuild_s", 0)) * 1000 / n if n else 0
        rows.append(
            f"| {variant} | {n} | {round(t.get('fetch_s', 0) + t.get('stage_s', 0))} | "
            f"{t.get('create_index_s', 0)} | {t.get('merge_s', 0)} | {t.get('merge_batches', 0)} | "
            f"{t.get('index_disable_s', 0)} | {t.get('index_rebuild_s', 0)} | {r.get('wall_s', 0)} | "
            f"{per_row:.2f} |"
        )
    return "\n".join(rows)


@dg.asset(
    name="sof_merge_benchmark",
    group_name="fhir_operations",
    compute_kind="mssql",
    description=(
        "Målebænk: MERGE af et Aidbox-vindue ind i en KOPI af en fhir.*-tabel på staging, "
        "dagens ét statement mod portioneret MERGE med indeks fra. Ændrer ikke måltabellen; "
        "kopien droppes."
    ),
)
def sof_merge_benchmark(
    context: dg.AssetExecutionContext,
    config: SofMergeBenchmarkConfig,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    outcome = run_benchmark(fhir_database, mssql_target_staging, context, config)
    context.log.info("SOF-målebænk færdig:\n%s", outcome["summary"])
    return dg.MaterializeResult(
        metadata={
            "summary": dg.MetadataValue.md(outcome["summary"]),
            "info": dg.MetadataValue.json(outcome["info"]),
            "results": dg.MetadataValue.json(outcome["results"]),
        }
    )


sof_merge_benchmark_job = dg.define_asset_job(
    name="sof_merge_benchmark_job",
    selection=dg.AssetSelection.assets("sof_merge_benchmark"),
    description="Målebænk for SOF-MERGE'n mod en kopi af måltabellen.",
    tags={"domain": "fhir", "kind": "operations"},
)

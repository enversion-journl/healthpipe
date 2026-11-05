# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Målebænk for storens fysiske layout og status for dens partitionering.

Målebænken (``dso_sfi_store_layout_benchmark``) bygger kopier af et udsnit af
fhir.SFI_datapoint_store i op til tre layouts og måler præcis de operationer, der er
målt som flaskehalse i backfill-kæden (docs/architecture/sfi_store_partitionering.md):

  1. indsættelsestid pr. portion, mens kopien vokser (portionstiden i storen steg fra
     27 til 62 s inden for én kørsel — det er den kurve, der skal flade ud),
  2. indeksbygning efter indlæsning og et SWITCH ud/ind af en partition inkl.
     genopbygning af det ikke-alignede indeks (forudsætningen for den nye backfill-sti —
     et SWITCH, der fejler her, fejler også på den rigtige store, og fejlteksten kommer med),
  3. DELETE + INSERT af genpostede ressourcer (den daglige stis pris pr. række),
  4. fire aftagerforespørgsler (patient, element+periode, model-aggregat, månedsscan),
  5. pladsforbrug pr. layout (storen fylder 3,9 GB pr. mio. rækker i dag).

Layouts: ``baseline`` = det gamle layout (klynget PK på id, ukomprimerede brede indeks),
``rowstore`` = partitioneret rowstore med fire brede, alignede indeks og et ikke-alignet
SourceResource-indeks (den forkastede mellemløsning, til sammenligning), ``columnstore``
= basis-DDL'ens layout: clustered columnstore på månedspartitioner med de to smalle,
alignede B-træer fra sfi_store_partitioning.STORE_INDEXES.

Read-only mod storen (kun SELECT). Skriver kun til fhir.SFI_datapoint_store_bench_* og
dropper dem igen, medmindre ``keep_tables`` er sat. Kræver ingen lås på storen. Pladsen
kontrolleres, før der skrives en række.

Statusasset'et (``dso_sfi_store_partition_status``) er read-only og rapporterer
partitioneringens tilstand: fyldte partitioner, indeks slået fra, rækker i arbejds-
tabellen og hvor mange måneders grænser der er tilbage.
"""

# Ingen ``from __future__ import annotations``: dg.Config-klasser kan ikke opløses fra
# strengannoteringer (PEP 563), og Dagster fejler ved definitionstid.
import re
import time
from typing import Any

import dagster as dg
from core.resources import MSSQLResource
from DSO.load import sfi_store_partitioning as part

BENCH_PREFIX = "SFI_datapoint_store_bench_"
VARIANTS = ("baseline", "rowstore", "columnstore")
# Forventet pladsfaktor pr. layout i forhold til storens nuværende bytes pr. række
# (bruges KUN til pladsvogteren før indlæsning; det målte tal er resultatet).
FOOTPRINT_FACTOR = {"baseline": 1.0, "rowstore": 0.7, "columnstore": 0.35}
# Arbejdskopien til SWITCH-testen rummer én partition; regnes som en fjerdedel.
WORK_COPY_FACTOR = 0.25

# Rowstore-sammenligningen: fire brede, alignede indeks plus et ikke-alignet indeks til
# DELETE-joinet. Kun til målebænken; basis-DDL'en er columnstore.
ROWSTORE_INDEXES: tuple[part.StoreIndex, ...] = (
    part.StoreIndex(
        name="UX_FHIR_SFI_datapoint_store_DatapointKey",
        key_columns=("datapoint_key", part.PARTITION_COLUMN),
        unique=True,
    ),
    part.StoreIndex(
        name="IX_FHIR_SFI_datapoint_store_ElementPatientDate",
        key_columns=("element_code", part.PARTITION_COLUMN, "patient_id"),
        include_columns=(
            "occurred_at",
            "element_display",
            "source_model_name",
            "organization_id",
            "value_type",
            "has_value",
            "value_int",
            "value_dec",
            "value_quantity",
            "value_unit",
        ),
    ),
    part.StoreIndex(
        name="IX_FHIR_SFI_datapoint_store_ModelElement",
        key_columns=("source_model_name", part.PARTITION_COLUMN, "element_code"),
        include_columns=("patient_id", "organization_id", "occurred_at", "element_display"),
    ),
    part.StoreIndex(
        name="IX_FHIR_SFI_datapoint_store_PatientOccurredDt",
        key_columns=("patient_id", part.PARTITION_COLUMN),
        include_columns=(
            "element_code",
            "element_display",
            "source_model_name",
            "source_resource",
            "occurred_at",
        ),
    ),
    part.StoreIndex(
        name="IX_FHIR_SFI_datapoint_store_SourceResource",
        key_columns=("source_resource", "source_resource_id"),
        aligned=False,
    ),
)


def variant_indexes(variant: str) -> tuple[part.StoreIndex, ...]:
    """B-træerne pr. layout (baseline har sine egne literale statements)."""
    if variant == "rowstore":
        return ROWSTORE_INDEXES
    if variant == "columnstore":
        return part.STORE_INDEXES
    return ()


# Dagens layout (før partitioneringen), til sammenligning. {t} = tabelnavn.
BASELINE_LAYOUT_SQL: tuple[str, ...] = (
    "ALTER TABLE {t} ADD CONSTRAINT [PK_{n}] PRIMARY KEY CLUSTERED ([sfi_datapoint_id]) "
    "WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];",
    "CREATE UNIQUE NONCLUSTERED INDEX [UX_FHIR_SFI_datapoint_store_DatapointKey] "
    "ON {t} ([datapoint_key] ASC) ON [PRIMARY];",
    "CREATE NONCLUSTERED INDEX [IX_FHIR_SFI_datapoint_store_ElementPatientDate] "
    "ON {t} ([element_code] ASC, [occurred_at_dt] ASC, [patient_id] ASC) "
    "INCLUDE ([occurred_at], [element_display], [source_model_name], [organization_id], "
    "[value_type], [has_value], [value_int], [value_dec], [value_quantity], [value_unit]) "
    "ON [PRIMARY];",
    "CREATE NONCLUSTERED INDEX [IX_FHIR_SFI_datapoint_store_ModelElement] "
    "ON {t} ([source_model_name] ASC, [occurred_at_dt] ASC, [element_code] ASC) "
    "INCLUDE ([patient_id], [organization_id], [occurred_at], [element_display]) ON [PRIMARY];",
    "CREATE NONCLUSTERED INDEX [IX_FHIR_SFI_datapoint_store_PatientOccurredDt] "
    "ON {t} ([patient_id] ASC, [occurred_at_dt] ASC) "
    "INCLUDE ([element_code], [element_display], [source_model_name], [source_resource], "
    "[occurred_at]) ON [PRIMARY];",
    "CREATE NONCLUSTERED INDEX [IX_FHIR_SFI_datapoint_store_SourceResource] "
    "ON {t} ([source_resource] ASC, [source_resource_id] ASC) INCLUDE ([datapoint_key]) "
    "ON [PRIMARY];",
)

# Forespørgselsformerne, aftagerne skriver (jf. brugervejledningen og indeksdesignet).
QUERY_SHAPES: tuple[tuple[str, str], ...] = (
    (
        "patient",
        "SELECT COUNT_BIG(*), MAX([occurred_at_dt]) FROM {t} WHERE [patient_id] = ?",
    ),
    (
        "element_periode",
        "SELECT COUNT_BIG(*), COUNT(DISTINCT [patient_id]) FROM {t} "
        "WHERE [element_code] = ? AND [occurred_at_dt] >= ? AND [occurred_at_dt] < ?",
    ),
    (
        "model_aggregat",
        "SELECT [element_code], COUNT_BIG(*) FROM {t} WHERE [source_model_name] = ? "
        "GROUP BY [element_code]",
    ),
    (
        "maanedsscan",
        "SELECT DATEFROMPARTS(YEAR([occurred_at_dt]), MONTH([occurred_at_dt]), 1), COUNT_BIG(*) "
        "FROM {t} WHERE [occurred_at_dt] IS NOT NULL "
        "GROUP BY DATEFROMPARTS(YEAR([occurred_at_dt]), MONTH([occurred_at_dt]), 1)",
    ),
)


class SfiStoreLayoutBenchmarkConfig(dg.Config):
    """Udsnit og omfang af målingen. Standardværdierne passer til en times kørsel på dev."""

    source_rows: int = 5_000_000
    load_batch_rows: int = 500_000
    repost_resources: int = 20_000
    variants: list[str] = ["baseline", "rowstore", "columnstore"]
    keep_tables: bool = False
    min_free_gb: float = 60.0


def bench_table(variant: str) -> str:
    return f"[fhir].[{BENCH_PREFIX}{variant}]"


def bench_work_table(variant: str) -> str:
    return f"[fhir].[{BENCH_PREFIX}{variant}_work]"


def validate_variants(variants: list[str]) -> list[str]:
    unknown = [v for v in variants if v not in VARIANTS]
    if unknown:
        raise ValueError(f"Ukendte layouts {unknown}; kendte: {list(VARIANTS)}")
    if not variants:
        raise ValueError("Mindst ét layout skal måles")
    return list(dict.fromkeys(variants))


def estimate_footprint_gb(source_rows: int, bytes_per_row: float, variants: list[str]) -> float:
    """Konservativt pladsskøn for kopierne inkl. arbejdskopier til SWITCH-testen."""
    factor = sum(FOOTPRINT_FACTOR[v] * (1 + WORK_COPY_FACTOR) for v in variants)
    return source_rows * bytes_per_row * factor / 1024**3


def layout_statements(variant: str, table: str) -> list[str]:
    """DDL, der giver en SELECT INTO-kopi (heap, samme kolonner) det ønskede layout."""
    if variant == "baseline":
        name = table.split(".")[-1].strip("[]")
        return [s.format(t=table, n=name) for s in BASELINE_LAYOUT_SQL]
    if variant == "rowstore":
        name = table.split(".")[-1].strip("[]")
        return [part.clustered_key_sql(table, f"UQ_{name}_IdOccurred")] + [
            ix.create_sql(table) for ix in ROWSTORE_INDEXES
        ]
    if variant == "columnstore":
        # En columnstore skal være alignet med basistabellen, og en SELECT INTO-kopi er
        # en heap uden partitionering (målt: "a columnstore index must be partition-
        # aligned with the base table"). Kopien partitioneres derfor først med et klynget
        # rowstore-indeks på partitionskolonnen, som derefter konverteres til columnstore
        # med DROP_EXISTING (samme navn). Basis-DDL'en opretter tabellen direkte på
        # skemaet og springer det trin over.
        cci_name = part.columnstore_index_name(table)
        rowstore_first = (
            f"CREATE CLUSTERED INDEX [{cci_name}]\n"
            f"    ON {table} ([{part.PARTITION_COLUMN}] ASC)\n"
            f"    WITH (DATA_COMPRESSION = PAGE)\n"
            f"    ON [{part.PARTITION_SCHEME}]([{part.PARTITION_COLUMN}]);"
        )
        return [rowstore_first, part.columnstore_index_sql(table, drop_existing=True)] + [
            ix.create_sql(table) for ix in part.STORE_INDEXES
        ]
    raise ValueError(f"Ukendt layout {variant!r}")


def secondary_index_names(variant: str) -> list[str]:
    """De sekundære indeks, der droppes og genskabes i indeksbygningsmålingen."""
    if variant == "baseline":
        return [
            "UX_FHIR_SFI_datapoint_store_DatapointKey",
            "IX_FHIR_SFI_datapoint_store_ElementPatientDate",
            "IX_FHIR_SFI_datapoint_store_ModelElement",
            "IX_FHIR_SFI_datapoint_store_PatientOccurredDt",
            "IX_FHIR_SFI_datapoint_store_SourceResource",
        ]
    return [ix.name for ix in variant_indexes(variant)]


def size_breakdown_sql(table: str) -> str:
    return f"""
        SELECT
            SUM(ps.reserved_page_count) * 8.0 / 1024 AS total_mb,
            SUM(CASE WHEN ps.index_id IN (0, 1) THEN ps.reserved_page_count ELSE 0 END) * 8.0 / 1024
                AS data_mb,
            SUM(CASE WHEN ps.index_id IN (0, 1) THEN ps.row_count ELSE 0 END) AS row_count
        FROM sys.dm_db_partition_stats AS ps
        WHERE ps.object_id = OBJECT_ID(N'{table}')
    """


def index_sizes_sql(table: str) -> str:
    return f"""
        SELECT i.name, SUM(ps.reserved_page_count) * 8.0 / 1024 AS mb
        FROM sys.dm_db_partition_stats AS ps
        JOIN sys.indexes AS i ON i.object_id = ps.object_id AND i.index_id = ps.index_id
        WHERE ps.object_id = OBJECT_ID(N'{table}')
        GROUP BY i.name
        ORDER BY i.name
    """


def free_space_gb_sql() -> str:
    """Mindste frie plads på de volumener, databasens datafiler ligger på."""
    return """
        SELECT MIN(vs.available_bytes) / 1073741824.0 AS free_gb
        FROM sys.database_files AS f
        CROSS APPLY sys.dm_os_volume_stats(DB_ID(), f.file_id) AS vs
        WHERE f.type = 0
    """


def server_info_sql() -> str:
    return (
        "SELECT CAST(SERVERPROPERTY('Edition') AS NVARCHAR(256)), "
        "CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(64)), "
        "CAST(SERVERPROPERTY('EngineEdition') AS INT)"
    )


def store_columns_sql(table: str = part.STORE_TABLE) -> str:
    return f"""
        SELECT c.name
        FROM sys.columns AS c
        WHERE c.object_id = OBJECT_ID(N'{table}')
        ORDER BY c.column_id
    """


def id_window_sql(source_rows: int) -> str:
    """Laveste id og id'et for række nr. ``source_rows`` i id-orden (de ældste rækker)."""
    return f"""
        SELECT
            (SELECT MIN([sfi_datapoint_id]) FROM {part.STORE_TABLE}) AS id_lo,
            (SELECT MAX(id) FROM (
                SELECT TOP ({int(source_rows)}) [sfi_datapoint_id] AS id
                FROM {part.STORE_TABLE}
                ORDER BY [sfi_datapoint_id]
            ) AS x) AS id_hi
    """


def repost_sample_sql(table: str, resources: int) -> str:
    """Stikprøve af ressourcer til genpost-testen. NOCOUNT slås fra igen til sidst:
    indstillingen er sessionsbundet, og DELETE/INSERT-tællerne bagefter læses fra
    cursor.rowcount (målt: 0 rækker rapporteret, fordi NOCOUNT stod tilbage).
    """
    return f"""
        SET NOCOUNT ON;
        IF OBJECT_ID(N'tempdb..#repost') IS NOT NULL DROP TABLE #repost;
        SELECT TOP ({int(resources)}) [source_resource], [source_resource_id]
        INTO #repost
        FROM (
            SELECT DISTINCT [source_resource], [source_resource_id]
            FROM {table}
            WHERE [sfi_datapoint_id] % 53 = 0
        ) AS x
        ORDER BY [source_resource], [source_resource_id];
        CREATE CLUSTERED INDEX [IX_repost] ON #repost ([source_resource], [source_resource_id]);
        SELECT COUNT(*) FROM #repost;
        SET NOCOUNT OFF;
    """


def rowgroup_state_sql(table: str) -> str:
    return f"""
        SELECT rg.state_description, COUNT(*) AS row_groups, SUM(rg.total_rows) AS total_rows,
               SUM(rg.deleted_rows) AS deleted_rows
        FROM sys.column_store_row_groups AS rg
        WHERE rg.object_id = OBJECT_ID(N'{table}')
        GROUP BY rg.state_description
    """


def most_populated_partition_sql(table: str) -> str:
    return f"""
        SELECT TOP (1) ps.partition_number, ps.row_count
        FROM sys.dm_db_partition_stats AS ps
        WHERE ps.object_id = OBJECT_ID(N'{table}') AND ps.index_id = 1
        ORDER BY ps.row_count DESC, ps.partition_number
    """


def _ms(started_ns: int) -> int:
    return (time.monotonic_ns() - started_ns) // 1_000_000


def _object_name(statement: str) -> str:
    """Navnet på det indeks eller den constraint, en layout-sætning opretter."""
    match = re.search(r"(?:INDEX|CONSTRAINT)\s+\[(\w+)\]", statement)
    return match.group(1) if match else statement[:40]


def _one(cursor: Any) -> Any:
    return part.fetch_single_row(cursor, result_name="benchmark")


def _timed(cursor: Any, conn: Any, sql: str, params: tuple | None = None) -> tuple[int, int]:
    """(ms, rowcount) for én skrivende eller læsende sætning, committet."""
    started = time.monotonic_ns()
    if params is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, params)
    rowcount = cursor.rowcount if cursor.rowcount is not None and cursor.rowcount >= 0 else 0
    if getattr(cursor, "description", None):
        cursor.fetchall()
    conn.commit()
    return _ms(started), rowcount


def _drop_if_exists(cursor: Any, conn: Any, table: str) -> None:
    cursor.execute(f"IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {table};")
    conn.commit()


def _store_column_names(cursor: Any) -> list[str]:
    cursor.execute(store_columns_sql())
    names = [str(r[0]) for r in cursor.fetchall()]
    if not names:
        raise RuntimeError(f"{part.STORE_TABLE} har ingen kolonner — findes tabellen?")
    return names


def column_list(names: list[str]) -> str:
    return ", ".join(f"[{n}]" for n in names)


def qualified_column_list(names: list[str], alias: str) -> str:
    """Kolonneliste med alias — til SELECT fra en join, hvor #repost deler kolonnenavne
    med storen (målt fejl: 'Ambiguous column name source_resource_id')."""
    return ", ".join(f"{alias}.[{n}]" for n in names)


def select_into_columns(names: list[str]) -> str:
    """Kolonneliste til ``SELECT TOP (0) ... INTO``-kopien.

    Id-kolonnen pakkes ind i et udtryk, så en IDENTITY-egenskab på kilden IKKE følger
    med kopien (ellers afviser kopien de eksplicitte id'er, der indsættes bagefter),
    og ISNULL holder den NOT NULL, så den kan bære den klyngede nøgle.
    """
    return ", ".join(
        f"ISNULL(CAST([{n}] AS BIGINT), 0) AS [{n}]" if n == "sfi_datapoint_id" else f"[{n}]"
        for n in names
    )


def _size(cursor: Any, table: str) -> dict[str, float]:
    cursor.execute(size_breakdown_sql(table))
    total_mb, data_mb, rows = _one(cursor)
    return {
        "total_mb": round(float(total_mb or 0), 1),
        "data_mb": round(float(data_mb or 0), 1),
        "index_mb": round(float(total_mb or 0) - float(data_mb or 0), 1),
        "rows": int(rows or 0),
    }


def _index_sizes(cursor: Any, table: str) -> dict[str, float]:
    cursor.execute(index_sizes_sql(table))
    return {str(name or "heap"): round(float(mb or 0), 1) for name, mb in cursor.fetchall()}


def preflight(cursor: Any, cfg: SfiStoreLayoutBenchmarkConfig, log: Any) -> dict[str, Any]:
    """Server, storens størrelse og fri plads — fejler FØR der skrives noget."""
    variants = validate_variants(cfg.variants)
    cursor.execute(server_info_sql())
    edition, version, engine = _one(cursor)
    store = _size(cursor, part.STORE_TABLE)
    if store["rows"] == 0:
        raise dg.Failure(f"{part.STORE_TABLE} er tom — intet at måle på.")
    bytes_per_row = store["total_mb"] * 1024 * 1024 / store["rows"]
    cursor.execute(free_space_gb_sql())
    free_gb = float(_one(cursor)[0] or 0)
    estimate_gb = estimate_footprint_gb(cfg.source_rows, bytes_per_row, variants)
    if free_gb - estimate_gb < cfg.min_free_gb:
        raise dg.Failure(
            f"For lidt plads: {free_gb:.1f} GB fri, kopierne skønnes til {estimate_gb:.1f} GB, "
            f"og der skal være mindst {cfg.min_free_gb:.0f} GB tilbage. Sæt source_rows ned."
        )
    info = {
        "edition": str(edition),
        "product_version": str(version),
        "engine_edition": int(engine),
        "store_rows": store["rows"],
        "store_total_mb": store["total_mb"],
        "store_bytes_per_row": round(bytes_per_row, 1),
        "free_gb_before": round(free_gb, 1),
        "estimate_gb": round(estimate_gb, 1),
        "variants": variants,
    }
    log.info(
        "Målebænk: %s %s; storen %d rækker, %.0f MB (%.0f B/række); %.1f GB fri, skøn %.1f GB.",
        edition,
        version,
        store["rows"],
        store["total_mb"],
        bytes_per_row,
        free_gb,
        estimate_gb,
    )
    return info


def ensure_partition_objects(cursor: Any, conn: Any) -> None:
    """Partitionsfunktion og -skema med samme navne og grænser som basis-DDL'en, så
    målebænken kan køre FØR infrastrukturjobbet (og dermed før migrationen) er kørt."""
    cursor.execute(part.partition_function_sql())
    cursor.execute(part.partition_scheme_sql())
    conn.commit()


def run_variant(
    cursor: Any,
    conn: Any,
    log: Any,
    cfg: SfiStoreLayoutBenchmarkConfig,
    variant: str,
    *,
    cols: str,
    copy_cols: str,
    src_cols: str,
    id_lo: int,
    id_hi: int,
) -> dict[str, Any]:
    table = bench_table(variant)
    work = bench_work_table(variant)
    result: dict[str, Any] = {"variant": variant, "table": table}
    _drop_if_exists(cursor, conn, work)
    _drop_if_exists(cursor, conn, table)

    # Tom kopi med storens kolonner og typer (uden IDENTITY), derefter layoutet.
    cursor.execute(f"SELECT TOP (0) {copy_cols} INTO {table} FROM {part.STORE_TABLE}")
    conn.commit()
    layout_ms: dict[str, int] = {}
    for statement in layout_statements(variant, table):
        ms, _ = _timed(cursor, conn, statement)
        layout_ms[_object_name(statement)] = ms
    result["layout_ms"] = layout_ms

    # 0. Varm kildesiden: alle layouts læser samme id-interval fra storen. Uden opvarmning
    #    betaler det første layout for kolde disklæsninger, og de næste får siderne fra
    #    buffer pool (målt: 101 mod 50 µs/række i første portion).
    started = time.monotonic_ns()
    cursor.execute(
        f"SELECT COUNT_BIG(*) FROM {part.STORE_TABLE} WHERE [sfi_datapoint_id] BETWEEN ? AND ?",
        (id_lo, id_hi),
    )
    cursor.fetchall()
    conn.commit()
    result["warm_source_ms"] = _ms(started)
    log.info("%s: kildesiden varmet på %d ms.", variant, result["warm_source_ms"])

    # 1. Indlæsning i portioner — portionstiden som funktion af kopiens størrelse.
    batches: list[dict[str, Any]] = []
    inserted = 0
    lo = id_lo
    while inserted < cfg.source_rows and lo <= id_hi:
        hi = min(lo + cfg.load_batch_rows - 1, id_hi)
        ms, rows = _timed(
            cursor,
            conn,
            f"INSERT INTO {table} ({cols}) SELECT {cols} FROM {part.STORE_TABLE} "
            f"WHERE [sfi_datapoint_id] BETWEEN ? AND ?",
            (lo, hi),
        )
        if rows:
            batches.append(
                {
                    "rows": rows,
                    "ms": ms,
                    "us_per_row": round(ms * 1000 / rows, 1),
                    "cum_rows": inserted + rows,
                }
            )
        inserted += rows
        lo = hi + 1
        log.info(
            "%s: portion %d — %d rækker på %d ms (%.1f µs/række), %d rækker i alt.",
            variant,
            len(batches),
            rows,
            ms,
            (ms * 1000 / rows) if rows else 0.0,
            inserted,
        )
    result["load"] = {
        "rows": inserted,
        "batches": batches,
        "total_ms": sum(b["ms"] for b in batches),
        "first_batch_us_per_row": batches[0]["us_per_row"] if batches else None,
        "last_batch_us_per_row": batches[-1]["us_per_row"] if batches else None,
    }

    # 5. Plads efter indlæsning.
    result["size"] = _size(cursor, table)
    result["index_mb"] = _index_sizes(cursor, table)
    log.info(
        "%s: %d rækker, %.0f MB i alt, heraf %.0f MB indeks (%s).",
        variant,
        result["size"]["rows"],
        result["size"]["total_mb"],
        result["size"]["index_mb"],
        ", ".join(f"{k} {v:.0f}" for k, v in result["index_mb"].items()),
    )
    if variant == "columnstore":
        cursor.execute(rowgroup_state_sql(table))
        result["rowgroups"] = {
            str(state): {"row_groups": int(n), "rows": int(rows or 0), "deleted": int(deleted or 0)}
            for state, n, rows, deleted in cursor.fetchall()
        }

    # 2a. Indeksbygning efter indlæsning (SWITCH-stiens faste pris pr. vindue).
    rebuild_ms: dict[str, int] = {}
    if variant == "columnstore":
        ms, _ = _timed(cursor, conn, part.reorganize_sql(table))
        rebuild_ms["REORGANIZE"] = ms
    for name in secondary_index_names(variant):
        cursor.execute(f"DROP INDEX [{name}] ON {table}")
        conn.commit()
    for statement in layout_statements(variant, table):
        name = _object_name(statement)
        if name in secondary_index_names(variant):
            ms, _ = _timed(cursor, conn, statement)
            rebuild_ms[name] = ms
            log.info("%s: byggede [%s] på %d ms.", variant, name, ms)
    result["index_build_ms"] = rebuild_ms
    result["size_after_rebuild"] = _size(cursor, table)
    log.info(
        "%s: efter genopbygning %.0f MB i alt, heraf %.0f MB indeks.",
        variant,
        result["size_after_rebuild"]["total_mb"],
        result["size_after_rebuild"]["index_mb"],
    )

    # 3. Genpost: DELETE + INSERT af et udsnit af ressourcer (den daglige stis pris).
    cursor.execute(repost_sample_sql(table, cfg.repost_resources))
    sampled = int(_one(cursor)[0])
    conn.commit()
    delete_ms, deleted = _timed(
        cursor,
        conn,
        f"DELETE t FROM {table} AS t INNER JOIN #repost AS r "
        f"ON r.[source_resource] = t.[source_resource] "
        f"AND r.[source_resource_id] = t.[source_resource_id]",
    )
    # Rækkerne hentes fra storen til en temp-tabel FØR målingen, så INSERT-tiden kun
    # måler målsidens indeksvedligeholdelse (målt: 148 s mod 7 s for samme INSERT,
    # fordi det første layout læste storens rækker koldt).
    cursor.execute("IF OBJECT_ID(N'tempdb..#repost_rows') IS NOT NULL DROP TABLE #repost_rows")
    fetch_ms, fetched = _timed(
        cursor,
        conn,
        f"SELECT {src_cols} INTO #repost_rows FROM {part.STORE_TABLE} AS s "
        f"INNER JOIN #repost AS r ON r.[source_resource] = s.[source_resource] "
        f"AND r.[source_resource_id] = s.[source_resource_id]",
    )
    insert_ms, reinserted = _timed(
        cursor,
        conn,
        f"INSERT INTO {table} ({cols}) SELECT {cols} FROM #repost_rows",
    )
    # 3b. Samme sletning afgrænset til portionens datointerval (den daglige stis form):
    #     kun portionens måneder læses. Rækkerne indsættes igen bagefter, så kopien er
    #     hel til SWITCH- og forespørgselstestene.
    cursor.execute(
        "SELECT MIN([occurred_at_dt]), MAX([occurred_at_dt]), "
        "SUM(CASE WHEN [occurred_at_dt] IS NULL THEN 1 ELSE 0 END) FROM #repost_rows"
    )
    dt_lo, dt_hi, dt_nulls = _one(cursor)
    conn.commit()
    delete_window_ms, deleted_window = 0, 0
    if dt_lo is not None:
        delete_window_ms, deleted_window = _timed(
            cursor,
            conn,
            f"DELETE t FROM {table} AS t INNER JOIN #repost AS r "
            f"ON r.[source_resource] = t.[source_resource] "
            f"AND r.[source_resource_id] = t.[source_resource_id] "
            f"WHERE t.[occurred_at_dt] >= ? AND t.[occurred_at_dt] <= ?",
            (dt_lo, dt_hi),
        )
    if dt_nulls:
        ms, n = _timed(
            cursor,
            conn,
            f"DELETE t FROM {table} AS t INNER JOIN #repost AS r "
            f"ON r.[source_resource] = t.[source_resource] "
            f"AND r.[source_resource_id] = t.[source_resource_id] "
            f"WHERE t.[occurred_at_dt] IS NULL",
        )
        delete_window_ms += ms
        deleted_window += n
    insert2_ms, _ = _timed(
        cursor,
        conn,
        f"INSERT INTO {table} ({cols}) SELECT {cols} FROM #repost_rows",
    )
    cursor.execute("DROP TABLE #repost_rows")
    conn.commit()
    result["repost"] = {
        "resources": sampled,
        "deleted_rows": deleted,
        "delete_ms": delete_ms,
        "delete_window_ms": delete_window_ms,
        "delete_window_rows": deleted_window,
        "fetch_rows": fetched,
        "fetch_ms": fetch_ms,
        "reinserted_rows": reinserted,
        "insert_ms": insert_ms,
        "insert_again_ms": insert2_ms,
        "us_per_row": round((delete_ms + insert_ms) * 1000 / reinserted, 1) if reinserted else None,
    }
    log.info(
        "%s: genpost af %d ressourcer — DELETE %d rækker på %d ms (med datointerval %d ms), "
        "INSERT %d rækker på %d ms.",
        variant,
        sampled,
        deleted,
        delete_ms,
        delete_window_ms,
        reinserted,
        insert_ms,
    )

    # 2b. SWITCH ud og ind af den mest fyldte partition (kun partitionerede layouts).
    if variant == "baseline":
        result["switch"] = {"skipped": "upartitioneret layout"}
    else:
        result["switch"] = switch_test(
            cursor, conn, log, variant, table=table, work=work, copy_cols=copy_cols
        )
        log.info(
            "%s: SWITCH %s; genopbygning af ikke-alignet indeks %d ms.",
            variant,
            "ok" if result["switch"].get("ok") else "FEJL",
            result["switch"].get("non_aligned_rebuild_ms", 0),
        )

    # 4. Aftagerforespørgsler, to kørsler hver (første koldere end anden).
    result["queries"] = query_tests(cursor, conn, table)
    log.info("%s: forespørgsler %s", variant, result["queries"])

    if not cfg.keep_tables:
        _drop_if_exists(cursor, conn, work)
        _drop_if_exists(cursor, conn, table)
    return result


def switch_test(
    cursor: Any, conn: Any, log: Any, variant: str, *, table: str, work: str, copy_cols: str
) -> dict[str, Any]:
    """SWITCH-forudsætningen efterprøvet på kopien: identisk arbejdstabel, eventuelle
    ikke-alignede indeks slået fra på begge sider (kun rowstore-sammenligningen), partition
    ud og ind igen, genopbygning bagefter. Fejlteksten gemmes i stedet for at vælte
    målingen — det er selve resultatet."""
    out: dict[str, Any] = {}
    non_aligned = [ix.name for ix in variant_indexes(variant) if not ix.aligned]
    try:
        cursor.execute(f"SELECT TOP (0) {copy_cols} INTO {work} FROM {part.STORE_TABLE}")
        conn.commit()
        for statement in layout_statements(variant, work):
            cursor.execute(statement)
        conn.commit()
        cursor.execute(most_populated_partition_sql(table))
        partition_number, rows = (int(v) for v in _one(cursor))
        out["partition_number"] = partition_number
        out["rows"] = rows
        for name in non_aligned:
            cursor.execute(f"ALTER INDEX [{name}] ON {work} DISABLE")
            cursor.execute(f"ALTER INDEX [{name}] ON {table} DISABLE")
        conn.commit()
        out["switch_out_ms"], _ = _timed(
            cursor, conn, part.switch_partition_sql(partition_number, source=table, target=work)
        )
        out["switch_in_ms"], _ = _timed(
            cursor, conn, part.switch_partition_sql(partition_number, source=work, target=table)
        )
        out["ok"] = True
        log.info(
            "%s: SWITCH af partition %d (%d rækker) ud %d ms / ind %d ms.",
            variant,
            partition_number,
            rows,
            out["switch_out_ms"],
            out["switch_in_ms"],
        )
    except Exception as error:  # noqa: BLE001 — fejlteksten er måleresultatet
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        out["ok"] = False
        out["error"] = str(error)[:1000]
        log.warning("%s: SWITCH-testen fejlede: %s", variant, error)
    finally:
        rebuild_ms = 0
        for name in non_aligned:
            try:
                ms, _ = _timed(cursor, conn, f"ALTER INDEX [{name}] ON {table} REBUILD")
                rebuild_ms += ms
            except Exception as error:  # noqa: BLE001
                log.warning("%s: genopbygning af [%s] fejlede: %s", variant, name, error)
        out["non_aligned_rebuild_ms"] = rebuild_ms
    return out


def query_tests(cursor: Any, conn: Any, table: str) -> dict[str, Any]:
    cursor.execute(
        f"SELECT TOP (1) [patient_id] FROM {table} "
        f"WHERE [patient_id] IS NOT NULL AND [sfi_datapoint_id] % 101 = 0"
    )
    patient_row = cursor.fetchone()
    cursor.execute(
        f"SELECT TOP (1) [element_code] FROM {table} WHERE [element_code] IS NOT NULL "
        f"GROUP BY [element_code] ORDER BY COUNT_BIG(*) DESC"
    )
    element_row = cursor.fetchone()
    cursor.execute(
        f"SELECT TOP (1) [source_model_name] FROM {table} WHERE [source_model_name] IS NOT NULL "
        f"GROUP BY [source_model_name] ORDER BY COUNT_BIG(*) DESC"
    )
    model_row = cursor.fetchone()
    cursor.execute(
        f"SELECT MIN([occurred_at_dt]), DATEADD(DAY, 90, MIN([occurred_at_dt])) FROM {table}"
    )
    date_row = cursor.fetchone()
    conn.commit()
    if not (patient_row and element_row and model_row and date_row and date_row[0]):
        return {"skipped": "kopien mangler parametre (tom eller uden datoer)"}
    params = {
        "patient": (patient_row[0],),
        "element_periode": (element_row[0], date_row[0], date_row[1]),
        "model_aggregat": (model_row[0],),
        "maanedsscan": (),
    }
    out: dict[str, Any] = {}
    for name, template in QUERY_SHAPES:
        sql = template.format(t=table)
        runs = []
        for _ in range(2):
            started = time.monotonic_ns()
            cursor.execute(sql, params[name]) if params[name] else cursor.execute(sql)
            rows = cursor.fetchall()
            runs.append({"ms": _ms(started), "rows": len(rows)})
        conn.commit()
        out[name] = {"first_ms": runs[0]["ms"], "second_ms": runs[1]["ms"], "rows": runs[1]["rows"]}
    return out


def summary_table(results: dict[str, dict[str, Any]]) -> str:
    """Markdown-tabel med de tal, beslutningen træffes på."""
    rows = [
        "| Layout | Rækker | MB i alt | MB indeks | Indlæsning µs/række (første→sidste portion) | "
        "Indeksbygning s | Genpost µs/række | DELETE uden/med dato ms | SWITCH ud/ind ms | "
        "Genopbyg ikke-alignet s | patient ms | element+periode ms | model ms | månedsscan ms |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for variant, r in results.items():
        load = r.get("load", {})
        switch = r.get("switch", {})
        q = r.get("queries", {})
        switch_txt = (
            f"{switch.get('switch_out_ms', '-')}/{switch.get('switch_in_ms', '-')}"
            if switch.get("ok")
            else switch.get("skipped", "FEJL")
        )
        query_cells = " | ".join(
            _query_ms(q, name)
            for name in ("patient", "element_periode", "model_aggregat", "maanedsscan")
        )
        rows.append(
            f"| {variant} | {r.get('size', {}).get('rows', '-')} | "
            f"{r.get('size', {}).get('total_mb', '-')} | "
            f"{r.get('size', {}).get('index_mb', '-')} | "
            f"{load.get('first_batch_us_per_row', '-')}→{load.get('last_batch_us_per_row', '-')} | "
            f"{round(sum(r.get('index_build_ms', {}).values()) / 1000, 1)} | "
            f"{r.get('repost', {}).get('us_per_row', '-')} | "
            f"{r.get('repost', {}).get('delete_ms', '-')}/"
            f"{r.get('repost', {}).get('delete_window_ms', '-')} | {switch_txt} | "
            f"{round(switch.get('non_aligned_rebuild_ms', 0) / 1000, 1)} | "
            f"{query_cells} |"
        )
    return "\n".join(rows)


def _query_ms(queries: dict[str, Any], name: str) -> str:
    value = queries.get(name)
    if isinstance(value, dict) and "second_ms" in value:
        return str(value["second_ms"])
    return "-"


def run_benchmark(
    cursor: Any, conn: Any, log: Any, cfg: SfiStoreLayoutBenchmarkConfig
) -> dict[str, Any]:
    info = preflight(cursor, cfg, log)
    ensure_partition_objects(cursor, conn)
    names = _store_column_names(cursor)
    cols = column_list(names)
    copy_cols = select_into_columns(names)
    cursor.execute(id_window_sql(cfg.source_rows))
    id_lo, id_hi = (int(v) for v in _one(cursor))
    conn.commit()
    results: dict[str, dict[str, Any]] = {}
    for variant in info["variants"]:
        log.info("Målebænk: layout %s (id %d..%d).", variant, id_lo, id_hi)
        results[variant] = run_variant(
            cursor,
            conn,
            log,
            cfg,
            variant,
            cols=cols,
            copy_cols=copy_cols,
            src_cols=qualified_column_list(names, "s"),
            id_lo=id_lo,
            id_hi=id_hi,
        )
    cursor.execute(free_space_gb_sql())
    info["free_gb_after"] = round(float(_one(cursor)[0] or 0), 1)
    conn.commit()
    return {"info": info, "results": results, "summary": summary_table(results)}


@dg.asset(
    name="dso_sfi_store_layout_benchmark",
    group_name="fhir_dso_sync_maintenance",
    compute_kind="mssql",
    description=(
        "Målebænk: kopier af et udsnit af fhir.SFI_datapoint_store i dagens layout, det "
        "partitioneret rowstore (mellemløsningen) og basis-DDL'ens columnstore. Måler indlæsning pr. "
        "portion, indeksbygning, SWITCH ud/ind, genpost (DELETE+INSERT), aftagerforespørgsler "
        "og plads. Read-only mod storen; skriver kun til fhir.SFI_datapoint_store_bench_*."
    ),
)
def dso_sfi_store_layout_benchmark(
    context: dg.AssetExecutionContext,
    config: SfiStoreLayoutBenchmarkConfig,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_dso.get_connection() as conn:
        cursor = conn.cursor()
        try:
            outcome = run_benchmark(cursor, conn, context.log, config)
        finally:
            cursor.close()
    context.log.info("Målebænk færdig:\n%s", outcome["summary"])
    return dg.MaterializeResult(
        metadata={
            "summary": dg.MetadataValue.md(outcome["summary"]),
            "info": dg.MetadataValue.json(outcome["info"]),
            "results": dg.MetadataValue.json(outcome["results"]),
        }
    )


sfi_store_layout_benchmark_job = dg.define_asset_job(
    name="sfi_store_layout_benchmark_job",
    selection=dg.AssetSelection.assets("dso_sfi_store_layout_benchmark"),
    description="Målebænk for storens fysiske layout (baseline, rowstore, columnstore) på en kopi.",
    tags={"domain": "fhir", "kind": "operations"},
)


# ---------------------------------------------------------------------------
# Status for partitioneringen (read-only)
# ---------------------------------------------------------------------------


def partition_status_sql() -> str:
    """Fyldte partitioner med rækketal og nedre grænse (RANGE RIGHT: partition p >= 2
    begynder ved grænse p-1; partition 1 er alt før første grænse, inkl. NULL)."""
    return f"""
        SELECT ps.partition_number, ps.row_count,
               CONVERT(NVARCHAR(19), prv.value, 126) AS lower_bound
        FROM sys.dm_db_partition_stats AS ps
        LEFT JOIN sys.partition_range_values AS prv
            ON prv.function_id = (SELECT function_id FROM sys.partition_functions
                                  WHERE name = N'{part.PARTITION_FUNCTION}')
           AND prv.boundary_id = ps.partition_number - 1
        WHERE ps.object_id = OBJECT_ID(N'{part.STORE_TABLE}')
          AND ps.index_id = 1
          AND ps.row_count > 0
        ORDER BY ps.partition_number
    """


def sequence_value_sql() -> str:
    return (
        "SELECT CAST(current_value AS BIGINT) FROM sys.sequences "
        f"WHERE name = N'{part.SEQUENCE_NAME}' AND schema_id = SCHEMA_ID(N'{part.SEQUENCE_SCHEMA}')"
    )


def collect_partition_status(cursor: Any) -> dict[str, Any]:
    status: dict[str, Any] = {}
    status["store_exists"] = part.table_exists(cursor, part.STORE_TABLE)
    status["store_partitioned"] = status["store_exists"] and part.table_is_partitioned(cursor)
    status["work_table_exists"] = part.table_exists(cursor, part.WORK_TABLE)
    if status["store_exists"]:
        status["store_size"] = _size(cursor, part.STORE_TABLE)
        cursor.execute(part.nonclustered_indexes_sql(part.STORE_TABLE))
        status["disabled_indexes"] = [str(n) for n, d in cursor.fetchall() if d]
    if status["store_partitioned"]:
        cursor.execute(partition_status_sql())
        populated = [
            {"partition": int(p), "rows": int(rows), "lower_bound": lb}
            for p, rows, lb in cursor.fetchall()
        ]
        status["populated_partitions"] = populated
        total_partitions = len(part.monthly_boundaries()) + 1
        highest = max((p["partition"] for p in populated), default=1)
        status["months_of_boundary_headroom"] = total_partitions - highest
    if status["work_table_exists"]:
        cursor.execute(part.populated_partitions_sql(part.WORK_TABLE))
        status["work_rows"] = sum(int(rows) for _, rows in cursor.fetchall())
    cursor.execute(sequence_value_sql())
    row = cursor.fetchone()
    status["sequence_current_value"] = int(row[0]) if row and row[0] is not None else None
    return status


@dg.asset(
    name="dso_sfi_store_partition_status",
    group_name="fhir_dso_sync_maintenance",
    compute_kind="mssql",
    description=(
        "Read-only status for fhir.SFI_datapoint_store: partitioneret eller ej, fyldte "
        "partitioner, indeks slået fra, rækker i arbejdstabellen og måneders grænse-råderum."
    ),
)
def dso_sfi_store_partition_status(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_dso.get_connection() as conn:
        cursor = conn.cursor()
        try:
            status = collect_partition_status(cursor)
        finally:
            cursor.close()
    context.log.info("Partitionsstatus: %s", status)
    if status.get("disabled_indexes"):
        context.log.warning("Indeks slået fra på storen: %s", ", ".join(status["disabled_indexes"]))
    if status.get("work_rows"):
        context.log.warning(
            "%d rækker står i arbejdstabellen fra en afbrudt kørsel.", status["work_rows"]
        )
    return dg.MaterializeResult(metadata={"status": dg.MetadataValue.json(status)})

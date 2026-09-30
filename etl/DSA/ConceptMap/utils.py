# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Shared utilities for ConceptMap assets.

Provides the merge_conceptmap_table function that handles the temp-table + MERGE
pattern for conceptmap tables, plus common row-conversion helpers used by
individual ConceptMap builders.
"""

import re
from collections import defaultdict
from collections.abc import Callable
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path
from typing import Any

ScoringAdjuster = Callable[[str, str], float]

# Danish/common glue words — base list extended per ConceptMap domain via ``extra_stop_words``.
_DEFAULT_STOP_WORDS: frozenset[str] = frozenset(
    {"og", "af", "i", "på", "til", "for", "med", "den", "det", "de", "en", "et"}
)

# Legacy default when ``extract_keywords(text)`` is called with no overrides (backward compatible).
_LEGACY_STOP_WORDS: frozenset[str] = frozenset(
    {
        "og",
        "af",
        "i",
        "på",
        "til",
        "for",
        "med",
        "den",
        "det",
        "de",
        "en",
        "et",
        "rsd",
        "vs",
        "fam",
        "dki",
        "fat",
        "open",
        "master",
    }
)


def extract_keywords(
    text: str,
    stop_words: set[str] | None = None,
    extra_stop_words: set[str] | None = None,
    important_acronyms: set[str] | None = None,
) -> set[str]:
    """Split display text into keyword tokens; optional per-domain stop-word and acronym rules."""
    text_lower = text.lower()
    words = text_lower.replace(",", " ").replace("(", " ").replace(")", " ").split()

    if stop_words is not None:
        effective_stops = set(stop_words)
        if extra_stop_words:
            effective_stops |= extra_stop_words
    elif extra_stop_words is not None or important_acronyms is not None:
        effective_stops = set(_DEFAULT_STOP_WORDS) | (extra_stop_words or set())
    else:
        effective_stops = set(_LEGACY_STOP_WORDS)

    keywords = {w for w in words if len(w) > 3 and w not in effective_stops}
    if important_acronyms:
        for word in words:
            if word in important_acronyms:
                keywords.add(word)
    return keywords


def semantic_similarity(
    source_lower: str,
    target_lower: str,
    source_keywords: set[str] | None = None,
    target_keywords: set[str] | None = None,
    *,
    extract_kw: Callable[[str], set[str]] = extract_keywords,
    scoring_adjuster: ScoringAdjuster | None = None,
    clamp_upper: bool = False,
) -> float:
    """Blend SequenceMatcher ratio with keyword overlap (50/50) when both keyword sets are non-empty."""
    direct_score = SequenceMatcher(None, source_lower, target_lower).ratio()
    if source_keywords is None:
        source_keywords = extract_kw(source_lower)
    if target_keywords is None:
        target_keywords = extract_kw(target_lower)

    if source_keywords and target_keywords:
        kw_overlap = len(source_keywords & target_keywords) / max(
            len(source_keywords), len(target_keywords)
        )
        combined = (direct_score * 0.5) + (kw_overlap * 0.5)
    else:
        combined = direct_score

    if scoring_adjuster is not None:
        adjusted = combined + scoring_adjuster(source_lower, target_lower)
        if clamp_upper:
            return max(0.0, min(1.0, adjusted))
        return max(0.0, adjusted)

    return combined


def determine_relationship(
    score: float,
    is_hierarchical_fallback: bool = False,
    *,
    is_known_mapping: bool = False,
) -> str:
    if is_known_mapping:
        return "equal" if score >= 0.95 else "equivalent"
    if is_hierarchical_fallback:
        return "narrower"
    if score >= 0.95:
        return "equal"
    elif score >= 0.85:
        return "equivalent"
    elif score >= 0.7:
        return "narrower"
    elif score >= 0.5:
        return "inexact"
    return "unmatched"


def find_all_descendants(root_code: str, hierarchy: dict[str, list[str]]) -> set[str]:
    descendants: set[str] = {root_code}

    def _traverse(code: str) -> None:
        for child in hierarchy.get(code, []):
            if child not in descendants:
                descendants.add(child)
                _traverse(child)

    _traverse(root_code)
    return descendants


def get_parent_code(code: str, hierarchy: dict[str, list[str]]) -> str | None:
    for parent, children in hierarchy.items():
        if code in children:
            return parent
    return None


def find_root_code_region_syd(
    hierarchy: dict[str, list[str]], concepts: dict[str, str]
) -> str | None:
    """Pick hierarchy root: prefer REGION_SYD, else display containing 'region', else arbitrary root."""
    all_children: set[str] = set()
    for children in hierarchy.values():
        all_children.update(children)
    roots = set(concepts.keys()) - all_children
    if "REGION_SYD" in roots:
        return "REGION_SYD"
    for root in roots:
        if "region" in concepts[root].lower():
            return root
    return next(iter(roots)) if roots else None


def find_best_match(
    source_display: str,
    target_concepts: dict[str, str],
    target_properties: dict[str, dict[str, str]],
    target_keywords_cache: dict[str, set[str]] | None = None,
    source_keywords: set[str] | None = None,
    entity_type_filter: str | None = None,
    min_similarity: float = 0.3,
    *,
    extract_kw: Callable[[str], set[str]] = extract_keywords,
    scoring_adjuster: ScoringAdjuster | None = None,
    clamp_upper: bool = False,
) -> tuple[str, float] | None:
    best_match: tuple[str, float] | None = None
    best_score = 0.0
    if source_keywords is None:
        source_keywords = extract_kw(source_display)
    source_lower = source_display.lower()

    for target_code, target_display in target_concepts.items():
        if best_score >= 0.95:
            break
        if entity_type_filter:
            if target_properties.get(target_code, {}).get("entityType") != entity_type_filter:
                continue
        target_lower = target_display.lower()
        if source_lower == target_lower:
            return (target_code, 1.0)

        if target_keywords_cache is not None and target_code in target_keywords_cache:
            target_kw = target_keywords_cache[target_code]
        else:
            target_kw = extract_kw(target_display)
            if target_keywords_cache is not None:
                target_keywords_cache[target_code] = target_kw

        if source_keywords and target_kw:
            if len(source_keywords & target_kw) == 0 and (
                len(source_lower) > 10 or len(target_lower) > 10
            ):
                continue

        score = semantic_similarity(
            source_lower,
            target_lower,
            source_keywords,
            target_kw,
            extract_kw=extract_kw,
            scoring_adjuster=scoring_adjuster,
            clamp_upper=clamp_upper,
        )
        if score > best_score and score >= min_similarity:
            best_score = score
            best_match = (target_code, score)
    return best_match


def manual_mappings_from_os2mo_rows(
    rows: list[dict[str, Any]],
) -> dict[str, tuple[str, str]]:
    """Build manual override map from DSI ``ext_os2mo_*`` asset output (CSV rows as dicts)."""
    mappings: dict[str, tuple[str, str]] = {}
    for row in rows:
        source_code = str(row.get("source_code", "")).strip()
        target_code = str(row.get("target_code", "")).strip()
        relationship = str(row.get("relationship", "")).strip()
        if source_code and target_code:
            mappings[source_code] = (target_code, relationship)
    return mappings


def rows_to_concept_dict(rows: list[tuple]) -> dict[str, str]:
    return {str(r[0]): str(r[1]) if r[1] else "" for r in rows}


def rows_to_properties_dict(rows: list[tuple]) -> dict[str, dict[str, str]]:
    props: dict[str, dict[str, str]] = defaultdict(dict)
    for r in rows:
        props[str(r[0])][str(r[1])] = str(r[2]) if r[2] else ""
    return dict(props)


def rows_to_hierarchy_dict(rows: list[tuple]) -> dict[str, list[str]]:
    hierarchy: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        hierarchy[str(r[0])].append(str(r[1]))
    return dict(hierarchy)


def merge_conceptmap_table(
    cursor,
    table_name: str,
    rows: list[tuple],
    *,
    delete_stale: bool = False,
) -> dict:
    """
    MERGE mapping rows into conceptmap.{table_name}.

    Each row is a tuple: (source_code, source_display, relationship, target_code, target_display)

    Args:
        delete_stale: When True, target rows whose (source_code, target_code) pair
            is absent from ``rows`` are deleted (full-snapshot semantics). Use
            when the builder emits the complete mapping set each run — e.g.
            SHAKToSOR, where a SHAK code that gets a new SOR target would
            otherwise leave its old (source_code, old target_code) row behind.

    Returns:
        Dict with insert/update (and delete, when delete_stale) counts and total.
    """
    cursor.execute("DROP TABLE IF EXISTS #tmp_mapping;")
    cursor.execute(
        """
        CREATE TABLE #tmp_mapping (
            [source_code]    NVARCHAR(100) COLLATE DATABASE_DEFAULT NOT NULL,
            [source_display] NVARCHAR(500) COLLATE DATABASE_DEFAULT NULL,
            [relationship]  NVARCHAR(50)  COLLATE DATABASE_DEFAULT NOT NULL,
            [target_code]    NVARCHAR(100) COLLATE DATABASE_DEFAULT NOT NULL,
            [target_display] NVARCHAR(500) COLLATE DATABASE_DEFAULT NULL,
            PRIMARY KEY ([source_code], [target_code])
        );
    """
    )

    if rows:
        if hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True
        for i in range(0, len(rows), 1000):
            cursor.executemany(
                "INSERT INTO #tmp_mapping VALUES (?, ?, ?, ?, ?)",
                rows[i : i + 1000],
            )

    delete_clause = "WHEN NOT MATCHED BY SOURCE THEN DELETE" if delete_stale else ""
    cursor.execute(
        f"""
        MERGE INTO [conceptmap].[{table_name}] AS target
        USING #tmp_mapping AS source
        ON target.[source_code] = source.[source_code]
           AND target.[target_code] = source.[target_code]
        WHEN MATCHED THEN
            UPDATE SET
                target.[source_display] = source.[source_display],
                target.[relationship] = source.[relationship],
                target.[target_display] = source.[target_display],
                target.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ([source_code], [source_display], [relationship],
                    [target_code], [target_display],
                    [dw_timestamp_insert], [dw_timestamp_update])
            VALUES (source.[source_code], source.[source_display], source.[relationship],
                    source.[target_code], source.[target_display],
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0)))
        {delete_clause}
        OUTPUT $action;
    """
    )
    merge_results = cursor.fetchall()
    inserted = sum(1 for r in merge_results if r[0] == "INSERT")
    updated = sum(1 for r in merge_results if r[0] == "UPDATE")

    cursor.execute("DROP TABLE IF EXISTS #tmp_mapping;")

    result = {
        "inserted": inserted,
        "updated": updated,
        "total_mappings": len(rows),
        "table": f"conceptmap.{table_name}",
    }
    if delete_stale:
        result["deleted"] = sum(1 for r in merge_results if r[0] == "DELETE")
    return result


# ---------------------------------------------------------------------------
# DW_EK resolution (join to map.* after MERGE; NULL until map rows exist)
# ---------------------------------------------------------------------------

_CONCEPTMAP_DW_EK_SQL_PATH = Path(__file__).resolve().parent / "conceptmap_dw_ek_resolution.sql"
_CONCEPTMAP_TABLE_HEADER = re.compile(r"^\s*--\s*conceptmap_table:\s*([a-zA-Z0-9_]+)\s*$")


@lru_cache(maxsize=1)
def _load_conceptmap_dw_ek_update_sql() -> dict[str, str]:
    """Parse ``conceptmap_dw_ek_resolution.sql`` into per-table UPDATE batches."""
    if not _CONCEPTMAP_DW_EK_SQL_PATH.is_file():
        raise FileNotFoundError(
            f"Missing ConceptMap SQL file: {_CONCEPTMAP_DW_EK_SQL_PATH}",
        )
    text = _CONCEPTMAP_DW_EK_SQL_PATH.read_text(encoding="utf-8")
    blocks: dict[str, list[str]] = {}
    current: str | None = None
    for line in text.splitlines():
        m = _CONCEPTMAP_TABLE_HEADER.match(line)
        if m:
            current = m.group(1)
            blocks[current] = []
            continue
        if current is not None:
            blocks[current].append(line)
    return {k: "\n".join(v).strip() for k, v in blocks.items() if v}


def sql_update_conceptmap_dw_ek(table_name: str) -> str:
    """Return the UPDATE batch that resolves DW_EK columns for ``conceptmap.{table_name}``."""
    mapping = _load_conceptmap_dw_ek_update_sql()
    if table_name not in mapping:
        allowed = ", ".join(sorted(mapping))
        raise ValueError(f"Unknown conceptmap table_name={table_name!r}. Allowed: {allowed}")
    return mapping[table_name].strip()


def update_conceptmap_dw_ek(cursor, table_name: str) -> None:
    """Populate DW_EK columns on ``conceptmap.{table_name}`` from map tables (no commit)."""
    cursor.execute(sql_update_conceptmap_dw_ek(table_name))

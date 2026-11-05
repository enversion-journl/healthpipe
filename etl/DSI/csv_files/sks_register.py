# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SKS Register extraction asset.

Downloads the complete SKS (Sundhedsvæsenets Klassifikationssystem) file from
Sundhedsdatastyrelsen and writes to ext.sks_register for consumption by the
cs_sks CodeSystem builder.

Data source: https://filer.sundhedsdata.dk/sks/data/skscomplete/SKScomplete.txt

The file is fixed-width, latin-1 encoded, and contains every SKS code version
(historical and current) across 10 classifications. Each code carries a
3-letter classification prefix (dia, opr, til, ...) which is stripped so the
CodeSystem is unique on clean codes; the classification ends up as a FHIR
property instead. Only the newest version per code is kept (no history), and
codes whose validity has ended are marked Inactive.

Fixed-width layout (0-based positions; lines are right-trimmed when trailing
fields are empty):
    0-22    code incl. 3-letter classification prefix
    23-30   valid-from date (YYYYMMDD)
    31-38   latest update date (YYYYMMDD)
    39-46   valid-to date (YYYYMMDD, 25000101 = no end)
    47-166  display text (120 chars, space-padded)
    167-172 two 3-digit auxiliary fields (not used)
    179-181 "SKS" marker (when present)
    187     update-type digit (1=created, 3=closed) — NOT validity status;
            validity is determined by the valid-to date alone.
"""

import gzip
import logging
import re
from datetime import date, datetime

import dagster as dg
from core.dfm_manager import DfmManager
from core.dfm_source import http_source
from core.resources import HTTPDownloadResource, MSSQLResource
from dagster import AssetExecutionContext
from dateutil.parser import parse as _dateutil_parse

logger = logging.getLogger(__name__)

SKS_COMPLETE_PATH = "SKScomplete.txt"

# 3-letter classification prefixes present in SKScomplete.txt. The prefix is
# stripped from the code and stored as the `classification` FHIR property.
KNOWN_PREFIXES = frozenset({"adm", "atc", "dia", "opr", "pro", "res", "spc", "til", "und", "uly"})

# Structural spot-check for the fixed-width layout: 3-letter lowercase prefix,
# 20-char code field, then three 8-digit date fields, then the display text.
# Lines that fail this pattern count as format violations — if the source file
# format changes, the load fails loudly instead of overwriting the existing
# CodeSystem with garbage (see _parse_sks_lines).
LINE_PATTERN = re.compile(
    r"^(?P<prefix>[a-z]{3})"
    r"(?P<code>.{20})"
    r"(?P<valid_from>\d{8})"
    r"(?P<updated>\d{8})"
    r"(?P<valid_to>\d{8})"
    r"(?:.*)$"
)

# Fail the load if more than this fraction of non-blank lines violate the
# expected fixed-width format.
MAX_INVALID_LINE_FRACTION = 0.01

EXT_COLUMNS = ["code", "display", "parent_code", "classification", "Inactive"]


def _decode_sks_bytes(data: bytes) -> str:
    """Gunzip if gzip-magic-prefixed, then decode latin-1 (SKScomplete.txt encoding)."""
    if len(data) > 2 and data[0] == 0x1F and data[1] == 0x8B:
        data = gzip.decompress(data)
    return data.decode("latin-1")


def _fetch_sks_complete(download: HTTPDownloadResource, context: dg.AssetExecutionContext) -> str:
    """Download SKScomplete.txt and return its decoded text content."""
    context.log.info(f"Downloading {download.url(SKS_COMPLETE_PATH)}...")
    text = _decode_sks_bytes(download.fetch(SKS_COMPLETE_PATH))
    context.log.info(f"Downloaded {len(text.encode('latin-1')) / (1024 * 1024):.1f} MB")
    return text


def _parse_date(value: str) -> date | None:
    """Parse a date field in two passes.

    First pass: strict YYYYMMDD as promised by the fixed-width format.
    Second pass: dateutil's flexible parser, so a benign date-format drift
    in the source doesn't drop records — but it logs a warning, since the
    format then no longer matches the documented layout. None when both
    passes fail (the line counts as a format violation in _parse_sks_lines).
    """
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        pass

    try:
        parsed = _dateutil_parse(value).date()
    except (ValueError, OverflowError):
        logger.warning(
            "SKS date field %r could not be parsed (strict YYYYMMDD and "
            "flexible fallback both failed) — the source date format may "
            "have changed",
            value,
        )
        return None

    logger.warning(
        "SKS date field %r did not match YYYYMMDD but parsed as %s via "
        "flexible fallback — the source date format may have changed",
        value,
        parsed,
    )
    return parsed


def _parse_sks_lines(text: str) -> list[dict]:
    """Parse fixed-width SKScomplete lines into version records.

    Returns one record per line: {code, classification, valid_from, valid_to,
    display} with valid_from/valid_to as datetime.date. Every non-blank line
    is structurally validated against LINE_PATTERN (prefix, code field, three
    real calendar dates). Lines with an unknown classification prefix are
    skipped (forward compatibility with new classifications); lines that
    violate the format count as invalid, and the whole parse raises
    ValueError when the invalid fraction exceeds MAX_INVALID_LINE_FRACTION —
    so a changed source format aborts the load before any data is replaced.
    """
    records: list[dict] = []
    invalid = 0
    total = 0
    invalid_samples: list[str] = []

    for raw in text.splitlines():
        line = raw.rstrip()
        if not line:
            continue
        total += 1

        match = LINE_PATTERN.match(line)
        if not match:
            invalid += 1
            if len(invalid_samples) < 5:
                invalid_samples.append(line[:80])
            continue

        valid_from = _parse_date(match["valid_from"])
        valid_to = _parse_date(match["valid_to"])
        code = match["code"].strip()
        if valid_from is None or valid_to is None or not code:
            invalid += 1
            if len(invalid_samples) < 5:
                invalid_samples.append(line[:80])
            continue

        prefix = match["prefix"]
        if prefix not in KNOWN_PREFIXES:
            # Structurally valid but unknown classification — skip, don't fail.
            continue

        records.append(
            {
                "code": code,
                "classification": prefix,
                "valid_from": valid_from,
                "valid_to": valid_to,
                "display": line[47:167].strip(),
            }
        )

    if total and invalid / total > MAX_INVALID_LINE_FRACTION:
        raise ValueError(
            f"SKScomplete format violation: {invalid}/{total} lines "
            f"({invalid / total:.1%}) do not match the expected fixed-width "
            f"layout (threshold {MAX_INVALID_LINE_FRACTION:.0%}). "
            f"Samples: {invalid_samples}"
        )

    if invalid:
        logger.warning(
            "SKScomplete: %d/%d lines skipped as format violations (below "
            "the %.0f%% threshold). Samples: %s",
            invalid,
            total,
            MAX_INVALID_LINE_FRACTION * 100,
            invalid_samples,
        )

    return records


def _deduplicate_newest(
    records: list[dict],
) -> tuple[dict[str, dict], dict[str, set[str]]]:
    """Keep the newest version per clean code; collect classification membership.

    Newest = max (valid_from, valid_to). A code appearing under several
    classification prefixes keeps all of them as classification membership,
    while display/validity come from the single newest version overall.
    """
    newest: dict[str, dict] = {}
    classifications: dict[str, set[str]] = {}

    for rec in records:
        code = rec["code"]
        classifications.setdefault(code, set()).add(rec["classification"])
        current = newest.get(code)
        if current is None or (rec["valid_from"], rec["valid_to"]) > (
            current["valid_from"],
            current["valid_to"],
        ):
            newest[code] = rec

    return newest, classifications


def _build_hierarchy(codes: set[str]) -> dict[str, str]:
    """Build parent mapping via prefix-matching (shorter codes are parents)."""
    sorted_codes = sorted(codes, key=lambda c: (len(c), c))
    code_set: set[str] = set()
    parent_map: dict[str, str] = {}

    for code in sorted_codes:
        code_set.add(code)
        for i in range(len(code) - 1, 0, -1):
            candidate = code[:i]
            if candidate in code_set:
                parent_map[code] = candidate
                break

    return parent_map


def _build_ext_rows(
    newest_by_code: dict[str, dict],
    classifications_by_code: dict[str, set[str]],
    today: date,
    context: dg.AssetExecutionContext,
) -> list[dict[str, str]]:
    """Build ext rows: one row per (code, classification) with shared
    display/ParentCode/Inactive per code."""
    parent_map = _build_hierarchy(set(newest_by_code))
    ext_rows: list[dict[str, str]] = []

    for code in sorted(newest_by_code):
        rec = newest_by_code[code]
        inactive = "true" if rec["valid_to"] < today else "false"
        for classification in sorted(classifications_by_code[code]):
            ext_rows.append(
                {
                    "code": code,
                    "display": rec["display"],
                    "parent_code": parent_map.get(code, ""),
                    "classification": classification,
                    "Inactive": inactive,
                }
            )

    context.log.info(f"Built {len(ext_rows)} ext rows for {len(newest_by_code)} unique codes")
    return ext_rows


def _write_to_ext_table(
    conn, ext_rows: list[dict[str, str]], context: dg.AssetExecutionContext
) -> int:
    """Drop/create ext.sks_register and insert all rows."""
    cursor = conn.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS [ext].[sks_register]")

        col_defs = []
        for col in EXT_COLUMNS:
            if col == "code":
                col_defs.append(f"[{col}] NVARCHAR(100) NOT NULL")
            else:
                col_defs.append(f"[{col}] NVARCHAR(500) NULL")
        col_defs.append("[dw_timestamp_insert] DATETIME2(0) NOT NULL DEFAULT GETDATE()")
        col_defs.append("[dw_timestamp_update] DATETIME2(0) NOT NULL DEFAULT GETDATE()")

        ddl = f"CREATE TABLE [ext].[sks_register] (\n    {','.join(col_defs)}\n)"
        cursor.execute(ddl)

        placeholders = ", ".join(["?"] * len(EXT_COLUMNS))
        col_names = ", ".join(f"[{c}]" for c in EXT_COLUMNS)
        insert_sql = f"INSERT INTO [ext].[sks_register] ({col_names}) VALUES ({placeholders})"

        chunk_size = 1000
        total_inserted = 0
        for i in range(0, len(ext_rows), chunk_size):
            chunk = ext_rows[i : i + chunk_size]
            rows_to_insert = [
                tuple(row.get(col, "") or None for col in EXT_COLUMNS) for row in chunk
            ]
            cursor.executemany(insert_sql, rows_to_insert)
            total_inserted += len(chunk)

        conn.commit()
        context.log.info(f"Inserted {total_inserted} rows into ext.sks_register")
        return total_inserted
    finally:
        cursor.close()


class SKSExtractConfig(dg.Config):
    """Runtime configuration for SKS register extraction.

    Attributes:
        classifications: Must be ``["all"]``. Only a full extract is
            supported because ``cs_sks`` does a full-snapshot
            ``delete_stale`` MERGE — loading a partial extract would delete
            every other classification's codes from the deployed CodeSystem.
        min_records: Sanity floor for parsed version records (full file). The
            load aborts below this, so a truncated or malformed download never
            replaces the existing CodeSystem. The file has held 100k+ rows for
            years; 50k signals something is badly wrong.
    """

    classifications: list[str] = ["all"]
    min_records: int = 50_000


@dg.asset(
    name="extract_sks_register",
    group_name="codesystem_extract",
    compute_kind="python",
    tags={"domain": "codesystem", "layer": "extract", "source": "sks"},
    description=(
        "Download SKScomplete.txt from Sundhedsdatastyrelsen and load into "
        "ext.sks_register. Codes are stripped of their classification prefix "
        "(unique clean codes), deduplicated to the newest version, and marked "
        "Inactive when their validity has ended. Produces rows "
        "(Code, display, ParentCode, classification, Inactive) "
        "for the cs_sks CodeSystem builder."
    ),
)
def extract_sks_register(
    context: AssetExecutionContext,
    config: SKSExtractConfig,
    mssql_target_staging: MSSQLResource,
    sks_download: HTTPDownloadResource,
) -> dg.MaterializeResult:
    if "all" not in config.classifications:
        raise RuntimeError(
            "extract_sks_register only supports classifications=['all']. cs_sks "
            "does a full-snapshot delete_stale MERGE, so loading a partial extract "
            "would delete every other classification's codes from the deployed "
            "CodeSystem. Got: "
            f"{config.classifications!r}."
        )

    source_url = sks_download.url(SKS_COMPLETE_PATH)
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "ext",
        "sks_register",
        dagster_run_id=context.run_id,
        mode="full_load",
        **http_source(source_url),
    )
    try:
        text = _fetch_sks_complete(sks_download, context)
        records = _parse_sks_lines(text)

        if len(records) < config.min_records:
            raise RuntimeError(
                f"Only {len(records)} SKS records parsed from {source_url} "
                f"(expected >= {config.min_records}). Refusing to load — the "
                f"download is likely truncated or the file format has changed."
            )
        if not records:
            raise RuntimeError(f"No SKS records parsed from {source_url}")

        context.log.info(f"Parsed {len(records)} version records")

        newest_by_code, classifications_by_code = _deduplicate_newest(records)

        today = date.today()
        ext_rows = _build_ext_rows(newest_by_code, classifications_by_code, today, context)

        if not ext_rows:
            raise RuntimeError("No SKS ext rows built — nothing to load")

        with mssql_target_staging.get_connection() as conn:
            row_count = _write_to_ext_table(conn, ext_rows, context)
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    classification_counts: dict[str, int] = {}
    for prefixes in classifications_by_code.values():
        for prefix in prefixes:
            classification_counts[prefix] = classification_counts.get(prefix, 0) + 1

    active_count = sum(1 for rec in newest_by_code.values() if rec["valid_to"] >= today)

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "unique_codes": dg.MetadataValue.int(len(newest_by_code)),
            "active_codes": dg.MetadataValue.int(active_count),
            "inactive_codes": dg.MetadataValue.int(len(newest_by_code) - active_count),
            "classification_counts": dg.MetadataValue.json(classification_counts),
            "source": dg.MetadataValue.text(source_url),
        }
    )

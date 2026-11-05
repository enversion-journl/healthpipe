# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DFM extract: SilkeborgData E26PERS.TXT -> ext.silkeborgdata_e26_pers.

E26PERS is the employment master of SD's payroll system: one row per employment
per monthly delivery, carrying CPR, name and address. The file is
semicolon-separated with NO header, so parsing is positional against
FILE_COLUMNS — which makes the field-count validation in _parse_line the load's
only structural safety net.

The delivery folder's date is the DFM checkpoint, owned by
arc.silkeborgdata_e26_pers and read here via checkpoint_from. This extract NEVER
moves the checkpoint; the archive asset does, after its MERGE has committed.
That is what makes a half-finished run self-healing: if anything fails between
reading and merging, the checkpoint stands still and the next run retries the
very same delivery.

Two nul-states are deliberately different:
  - no delivery selected      -> green, skipped=True. The normal day.
  - a selected delivery with
    zero usable rows          -> HARD FAILURE. ext would be empty, the MERGE a
                                 no-op, MAX(dw_leverance_dato) unchanged and the
                                 checkpoint stuck, so the same folder would be
                                 re-selected every day, green, forever.

Between those two sits the case neither catches: a file that is truncated but not
empty. Field-count validation cannot see it (every line it does contain is
well-formed) and neither can is_delivery_settled, whose own docstring says a file
cut exactly at a line break parses cleanly. That is what the pre-commit volume
gate is for — see VOLUME_TOLERANCE_PCT and _assert_volume_within_tolerance. It
runs inside write_to_ext_table's transaction, after the last row is staged and
before the commit, so a rejected delivery leaves ext untouched and the checkpoint
standing; the next run retries the same folder. The archive carries the same test
as a blocking asset check afterwards, but by then the rows are in and `active`
has been recomputed for the whole population, so undoing it is real work. The
gate is the cheap place.

Logging discipline: the file contains CPR, name and address. Log counts, column
names, line numbers and paths — never field values, not even in exceptions.
"""

import os
from collections.abc import Iterator
from datetime import date, datetime
from typing import Any

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.dfm_source import share_source
from core.resources import FileShareResource, MSSQLResource
from DSI.csv_files.silkeborgdata import (
    LEVERANCE_COLUMNS,
    Delivery,
    discover_deliveries,
    parse_sd_date,
    parse_sd_decimal,
    partition_settled_deliveries,
    read_delivery_rows,
    select_deliveries,
    write_to_ext_table,
)
from pydantic import Field

# Upper case. NOT E26PERSF.TXT, which sits in the same folder and would parse
# without complaint — discover_deliveries matches the whole name.
FILE_NAME = "E26PERS.TXT"
ARC_TABLE = "silkeborgdata_e26_pers"

# The 41 archive column names in the FILE's positional order. This list is the
# ONLY source of field order: the DDL, the ext CREATE, the INSERT and the archive
# config are all derived from it, so they cannot drift apart.
#
# Positions 26/27 are CONFIRMED correct — do not swap them. Profiling saw
# jubilee dates before hire dates and read it as a swap, but jubil_dato is a
# jubilee SENIORITY date that credits earlier public service, so lying before the
# hire date is what it is supposed to do. Measured over 1,627,400 rows: position
# 27 equals position 26 in 59 % (no prior service) and is earlier in a smoothly
# decaying tail (1 year 95k, 2 years 77k, 3 years 64k ...). A swap would put that
# tail on the positive side. To re-check after a reload:
#     SELECT DATEDIFF(YEAR, [Ansaettelsesdato], [Jubilaeumsdato]) AS aar, COUNT(*)
#     FROM [arc].[silkeborgdata_e26_pers]
#     WHERE [Ansaettelsesdato] IS NOT NULL AND [Jubilaeumsdato] IS NOT NULL
#     GROUP BY DATEDIFF(YEAR, [Ansaettelsesdato], [Jubilaeumsdato]) ORDER BY 2 DESC;
# The spike must sit at 0 with a negative tail. Ignore the buckets near 7990 —
# those are the 9999-12-31 sentinel rows described below.
FILE_COLUMNS: list[str] = [
    "Inst",  # 1  inst
    "Tjenestenummer",  # 2  tjnr
    "CprNr",  # 3  cprnr
    "Afdelingskode",  # 4  afdkode
    "Tjenestekode",  # 5  tjkode
    "Stillingskode",  # 6  stilkode
    "Udtraeksdato",  # 7  periode
    "Lederkode",  # 8  lederkode
    "Ovk",  # 9  ovk
    "Loenklasse",  # 10 lonkl
    "Trin",  # 11 trin
    "Garantiloen",  # 12 garantilon_trin — a STEP, not an amount
    "GarantiloenBeloeb",  # 13 garantilon_kr
    "Afloenning",  # 14 loko
    "Anciennitetsdato",  # 15 ancien_dato
    "Deltidsbeskaeftigelseskode",  # 16 beskkode
    "Loendecimal",  # 17 londecimal
    "Beskaeftigelsesdecimal",  # 18 beskdecimal
    "TimerIPerioden",  # 19 timer
    "Navn",  # 20 navn
    "Stilling",  # 21 stilling
    "Adresse",  # 22 adresse
    "Postnr",  # 23 postnr
    "Postdistrikt",  # 24 postdistrikt
    "Kommunekode",  # 25 kmkode
    "Ansaettelsesdato",  # 26 ansaet_dato
    "Jubilaeumsdato",  # 27 jubil_dato
    "Statuskode",  # 28 statuskode
    "Statusdato",  # 29 statusdato
    "Hensaettelseskode",  # 30 hens_kode
    "Udbetalingskode",  # 31 udb_kode
    "Aconto",  # 32 aconto
    "Acontoperiode",  # 33 ac_periode
    "Discokode",  # 34 discokode
    "Jobstatuskode",  # 35 jskode
    "Omraadekode",  # 36 omr
    "Pensionskode",  # 37 pens_kode
    "Pensionsfaggruppe",  # 38 Pens_faggrup
    "Refusionskode",  # 39 ref_kode
    "Refusionsprocent",  # 40 ref_pct
    "Refusionsbeloeb",  # 41 ref_kr
]

FIELD_COUNT = len(FILE_COLUMNS)

DATE_COLUMNS: frozenset[str] = frozenset(
    {
        "Udtraeksdato",
        "Anciennitetsdato",
        "Ansaettelsesdato",
        "Jubilaeumsdato",
        "Statusdato",
        "Acontoperiode",
    }
)

# column -> SQL type. Types match arc exactly, so the archive MERGE is a plain
# column copy. Decimal precision follows the SAS informat with a little room,
# because COMMAXw.d counts separators in w.
DECIMAL_COLUMNS: dict[str, str] = {
    "GarantiloenBeloeb": "DECIMAL(11,2)",
    "Loendecimal": "DECIMAL(7,4)",
    "Beskaeftigelsesdecimal": "DECIMAL(7,4)",
    "TimerIPerioden": "DECIMAL(9,2)",
    "Aconto": "DECIMAL(11,2)",
    "Refusionsprocent": "DECIMAL(9,4)",
    "Refusionsbeloeb": "DECIMAL(13,2)",
}

# NVARCHAR widths are the SAS $w. width exactly, so an over-long value fails
# visibly rather than being truncated. CprNr stays text — a numeric type would
# drop leading zeros.
_TEXT_WIDTHS: dict[str, int] = {
    "Inst": 2,
    "Tjenestenummer": 5,
    "CprNr": 10,
    "Afdelingskode": 4,
    "Tjenestekode": 1,
    "Stillingskode": 4,
    "Lederkode": 2,
    "Ovk": 5,
    "Loenklasse": 3,
    "Trin": 2,
    "Garantiloen": 2,
    "Afloenning": 1,
    "Deltidsbeskaeftigelseskode": 1,
    "Navn": 40,
    "Stilling": 24,
    "Adresse": 30,
    "Postnr": 5,
    "Postdistrikt": 24,
    "Kommunekode": 3,
    "Statuskode": 1,
    "Hensaettelseskode": 1,
    "Udbetalingskode": 1,
    "Discokode": 6,
    "Jobstatuskode": 1,
    "Omraadekode": 1,
    "Pensionskode": 1,
    "Pensionsfaggruppe": 3,
    "Refusionskode": 1,
}

KEY_COLUMNS: tuple[str, ...] = ("Inst", "Tjenestenummer", "Udtraeksdato")

# Cap on per-row rejection warnings. A file whose keys are systematically blank
# — a case the parser explicitly handles — would otherwise emit one Dagster
# event per line, tens of thousands of them on a real delivery.
_MAX_KEY_REJECT_WARNINGS = 20

# A period whose row count moves more than this against the previous period is
# either a real organisational change or a truncated file. Canonical here, and
# imported by the period_volume asset check on arc: the two are the same test on
# the same quantity, run before and after the archive MERGE, and two constants
# would drift.
#
# 3 %, not 10 %. Both users block, so the number is the size of the truncation
# that still gets through: at 10 % that is ~3.800 persons of a full delivery — a
# file cut at 91 % would pass, and `active` is recomputed for the whole
# population on every delivery, so everyone missing would be marked as departed
# all the way into the reporting database. A monthly payroll delivery does not
# move 3 % in headcount under normal conditions, so the false-positive cost is
# low, and a genuine large change has the config override.
VOLUME_TOLERANCE_PCT = 3.0


class VolumeGateError(ValueError):
    """A delivery's row count moved too far from the previous period.

    Raised from the pre-commit gate, so ext is rolled back and the archive never
    sees the delivery. Its own exception type because the one legitimate way
    past it — volume_tolerance_pct — is named in the message, and because the
    tests have to tell it apart from the 0-usable-rows failure.
    """


# 41 file columns + the two provenance columns stamped from the delivery folder.
EXT_COLUMNS: list[str] = [*FILE_COLUMNS, *LEVERANCE_COLUMNS]


def _ext_column_types() -> dict[str, str]:
    """ext column -> SQL type, in EXT_COLUMNS order. Derived, never hand-listed."""
    types: dict[str, str] = {}
    for column in FILE_COLUMNS:
        if column in DATE_COLUMNS:
            types[column] = "DATE NULL"
        elif column in DECIMAL_COLUMNS:
            types[column] = f"{DECIMAL_COLUMNS[column]} NULL"
        else:
            nullable = "NOT NULL" if column in KEY_COLUMNS else "NULL"
            types[column] = f"NVARCHAR({_TEXT_WIDTHS[column]}) {nullable}"
    # Udtraeksdato is a key column and must not be nullable in ext either.
    types["Udtraeksdato"] = "DATE NOT NULL"
    types["dw_leverance_dato"] = "DATE NOT NULL"
    types["dw_leverance_mappe"] = "NVARCHAR(50) NOT NULL"
    return types


class SilkeborgDataExtractConfig(RunModeConfig):
    """RunModeConfig plus one flat-file-specific field.

    Inherits mode/date_from/date_to/advance_checkpoint and their validation, so
    the asset is operated with exactly the same launchpad payloads as ILS,
    MySupply, SydAX and KMD UdData — a run is configured as
    ``ops: {<asset>: {config: {mode: full_load}}}`` regardless of source.
    Subclassing RunModeConfig follows HaibaArchiveConfig and DrgArchiveConfig.
    """

    tolerate_parse_errors: bool = Field(
        default=False,
        description=(
            "Convert unparsable date/decimal values to NULL instead of failing. "
            "Only for the first investigation of a new delivery — an archive full "
            "of silent NULLs is worse than a failed run."
        ),
    )

    volume_tolerance_pct: float | None = Field(
        default=None,
        description=(
            "Override the pre-commit volume gate's tolerance (default "
            f"{VOLUME_TOLERANCE_PCT:.0f} %) for a delivery whose size really did "
            "change. The gate reads the archive, not the run mode, so a full_load "
            "does not wave it through — this field is the only way past it. Set it "
            "once, for the run that loads the confirmed delivery."
        ),
    )


def _new_stats() -> dict[str, int]:
    return {
        "rows_rejected_blank_key": 0,
        "duplicates_collapsed": 0,
        "duplicates_across_deliveries": 0,
        "unparsable_dates": 0,
        "unparsable_decimals": 0,
        "trailing_minus_values": 0,
    }


def _normalise_field_count(fields: list[str], line_no: int) -> list[str]:
    """Validate the field count, tolerating one blank trailing field.

    The count is confirmed at 41, but this validation is mandatory anyway: the
    file has no header, so a future change to the column set cannot be detected
    by name — only by count. Without it, every column after the shift would be
    wrong without anything failing.

    Every line in the real file ends with a delimiter followed by whitespace, so
    csv yields 42 fields with a blank 42nd. The comparison is therefore on the
    STRIPPED value: comparing with "" exactly rejected the whole file. A trailing
    field carrying real content still fails — that would be a 42nd column, not a
    trailing delimiter, and guessing its position would silently shift every
    column after it.

    The message carries the line number and the counts, never row content.
    """
    count = len(fields)
    if count == FIELD_COUNT + 1 and fields[-1].strip() == "":
        # Line ends with a delimiter, optionally followed by whitespace.
        fields = fields[:-1]
        count = FIELD_COUNT
    if count != FIELD_COUNT:
        raise ValueError(
            f"{FILE_NAME} line {line_no}: expected {FIELD_COUNT} fields, found {count}"
        )
    return fields


def _parse_line(
    fields: list[str],
    line_no: int,
    stats: dict[str, int],
    tolerate: bool,
    context: dg.AssetExecutionContext,
) -> dict[str, Any] | None:
    """Validate and type-convert one line. None means the row is rejected."""
    fields = _normalise_field_count(fields, line_no)

    row: dict[str, Any] = {}
    for column, raw in zip(FILE_COLUMNS, fields, strict=True):
        value = raw.strip()
        if column in DATE_COLUMNS:
            row[column] = parse_sd_date(
                value, column=column, line_no=line_no, tolerate=tolerate, stats=stats
            )
        elif column in DECIMAL_COLUMNS:
            row[column] = parse_sd_decimal(
                value, column=column, line_no=line_no, tolerate=tolerate, stats=stats
            )
        else:
            # Empty after strip -> NULL for every nullable column. No other
            # cleaning: no case normalisation, no zero-padding. DSI is raw 1:1.
            row[column] = value or None

    if any(row[key] is None for key in KEY_COLUMNS):
        stats["rows_rejected_blank_key"] += 1
        rejected = stats["rows_rejected_blank_key"]
        # A systematically key-less file would otherwise emit one Dagster event
        # per line. The first few identify the problem; the total is reported in
        # the run metadata as rows_rejected_blank_key.
        if rejected <= _MAX_KEY_REJECT_WARNINGS:
            blank = [key for key in KEY_COLUMNS if row[key] is None]
            context.log.warning(
                f"{FILE_NAME} line {line_no}: blank primary-key column(s) "
                f"{', '.join(blank)} — row rejected"
            )
        elif rejected == _MAX_KEY_REJECT_WARNINGS + 1:
            context.log.warning(
                f"{FILE_NAME}: more than {_MAX_KEY_REJECT_WARNINGS} rows with a blank "
                f"primary-key column — no longer logging them individually. The total "
                f"is reported as rows_rejected_blank_key in the run metadata."
            )
        return None

    return row


def _iter_ext_rows(
    deliveries: list[Delivery],
    encoding: str,
    stats: dict[str, int],
    tolerate: bool,
    context: dg.AssetExecutionContext,
    rows_per_delivery: dict[str, dict[str, int]],
    usable_per_period: dict[date, int],
) -> Iterator[dict[str, Any]]:
    """Stream every selected delivery, stamp provenance and dedupe on the key.

    Deliveries arrive in DESCENDING date order, so "keep the first occurrence"
    keeps the row from the NEWEST folder. With ascending order the oldest version
    would win and dw_leverance_dato would point at the wrong delivery — a silent
    error that would only surface as stale values in the archive.

    Deduplication is a precondition for enable_bulk_insert_on_first_load on the
    archive config, and for the MERGE at all: SQL Server rejects a MERGE where
    two source rows hit the same target row. Only the key set is held in memory.

    rows_per_delivery records two counts per folder, and the difference matters:
      "usable" — rows that parsed and carried a complete key
      "kept"   — rows that also survived deduplication
    A delivery with usable == 0 holds no loadable data and is a hard error. A
    delivery with usable > 0 but kept == 0 is legitimate: every one of its rows
    was superseded by a newer folder, which is exactly what a re-issued period
    looks like during a backfill.

    usable_per_period feeds the pre-commit volume gate, and is grouped by
    Udtraeksdato rather than by folder so it compares like with like against the
    archive, which the gate groups the same way. It counts USABLE rows, not kept
    ones, for the same reason the 0-row failure above does: kept is a
    cross-folder artefact and is legitimately 0 for a superseded folder, while
    usable measures the file — and a truncated file is what the gate is for.

    Two folders carrying the same period fold with max(), not sum(): that is a
    re-issue, and after deduplication the period contributes one row per key
    once, not twice. max() is the honest floor — summing would let a re-issue
    look like the period doubling and trip the gate on a healthy load.
    """
    # key -> index of the delivery that first claimed it. An int per key is
    # cheap and lets a collision be attributed to the right cause: a duplicate
    # inside one file, or SD re-sending a period in an older folder.
    seen: dict[tuple, int] = {}

    for index, delivery in enumerate(deliveries):
        usable = 0
        kept = 0
        usable_this_folder: dict[date, int] = {}
        for line_no, fields in read_delivery_rows(delivery.path, encoding):
            if not any(field.strip() for field in fields):
                continue
            row = _parse_line(fields, line_no, stats, tolerate, context)
            if row is None:
                continue

            usable += 1
            # Udtraeksdato is in KEY_COLUMNS, so a usable row always carries one.
            period: date = row["Udtraeksdato"]
            usable_this_folder[period] = usable_this_folder.get(period, 0) + 1
            key = tuple(row[column] for column in KEY_COLUMNS)
            claimed_by = seen.get(key)
            if claimed_by is not None:
                stats["duplicates_collapsed"] += 1
                if claimed_by != index:
                    # Claimed by a NEWER folder (deliveries run newest-first), so
                    # SD re-sent this period. Counted separately, so a re-issue
                    # is visible in metadata instead of vanishing.
                    stats["duplicates_across_deliveries"] += 1
                continue
            seen[key] = index

            row["dw_leverance_dato"] = delivery.delivery_date
            row["dw_leverance_mappe"] = delivery.folder
            kept += 1
            yield row

        for period, count in usable_this_folder.items():
            usable_per_period[period] = max(usable_per_period.get(period, 0), count)

        rows_per_delivery[delivery.folder] = {"usable": usable, "kept": kept}
        context.log.info(
            f"{delivery.folder}/{FILE_NAME}: {usable} usable row(s), {kept} kept " f"after dedup"
        )


def _archive_period_counts(conn) -> dict[date, int]:
    """Rows per period already in the archive — the gate's baseline.

    Grouped by Udtraeksdato and not by dw_leverance_dato on purpose. The folder
    date says which delivery last wrote a row, so a re-issued period moves rows
    between folders and the per-folder counts change without the archive
    changing. The period is a property of the row, survives the MERGE, and is
    the same grouping the period_volume asset check uses — the gate and the
    check therefore compare the same numbers.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            SELECT [Udtraeksdato], COUNT_BIG(*)
            FROM [arc].[{ARC_TABLE}]
            WHERE [Udtraeksdato] IS NOT NULL
            GROUP BY [Udtraeksdato]
            """
        )
        counts: dict[date, int] = {}
        for period, count in cursor.fetchall():
            counts[period.date() if isinstance(period, datetime) else period] = int(count)
        return counts
    finally:
        cursor.close()


def _volume_verdicts(
    *,
    run_counts: dict[date, int],
    archive_counts: dict[date, int],
    tolerance_pct: float,
) -> list[dict[str, Any]]:
    """One verdict per period this load ADDS, against the nearest older period.

    Pure, so the rule can be tested without a database.

    Only periods the archive does not already hold are judged. A period that is
    already archived cannot shrink from this load — the archive MERGE updates and
    inserts, it never deletes — so re-reading it is harmless, and gating it would
    turn every full_load into a re-litigation of the whole history.

    The baseline is the nearest STRICTLY OLDER period, taken from the archive and
    from the periods this same load has already cleared. That makes a run that
    brings two months at once compare month 1 against the archive and month 2
    against month 1, instead of comparing both against the same old baseline and
    reporting a drift that was really two normal steps.
    """
    known = dict(archive_counts)
    verdicts: list[dict[str, Any]] = []

    for period in sorted(run_counts):
        rows = run_counts[period]

        if period in archive_counts:
            # Already archived. Not judged; still the best baseline for the next
            # period, so take whichever side is larger — the MERGE unions them.
            known[period] = max(known[period], rows)
            continue

        older = [candidate for candidate in known if candidate < period]
        previous = max(older) if older else None
        previous_rows = known[previous] if previous is not None else 0

        if previous is None or previous_rows <= 0:
            verdicts.append(
                {
                    "period": str(period),
                    "rows": rows,
                    "previous_period": None,
                    "previous_rows": None,
                    "deviation_pct": None,
                    "compared": False,
                    "passed": True,
                }
            )
        else:
            deviation_pct = (rows - previous_rows) / previous_rows * 100.0
            verdicts.append(
                {
                    "period": str(period),
                    "rows": rows,
                    "previous_period": str(previous),
                    "previous_rows": previous_rows,
                    "deviation_pct": round(deviation_pct, 2),
                    "compared": True,
                    "passed": abs(deviation_pct) <= tolerance_pct,
                }
            )

        known[period] = rows

    return verdicts


def _assert_volume_within_tolerance(
    *,
    run_counts: dict[date, int],
    archive_counts: dict[date, int],
    tolerance_pct: float,
    overridden: bool,
    context: dg.AssetExecutionContext,
) -> dict[str, Any]:
    """Run the gate. Raises VolumeGateError if any new period is out of bounds.

    Returns a metadata dict for the materialisation, so a run that passed still
    says by how much — the gate is only trustworthy if the number it accepted is
    visible on the green runs too.

    An empty archive is not gated. There is no previous period to compare the
    first delivery against, and chaining the comparison through a bootstrap load
    of the whole history would test SD's past reorganisations rather than this
    file. A first load is a supervised operation; the archive it writes is the
    baseline every later run is held to.
    """
    if not archive_counts:
        context.log.info(
            "Volume gate skipped: the archive holds no periods yet, so there is no "
            "previous period to compare this load against. Every later run is gated."
        )
        return {
            "volume_gate": dg.MetadataValue.text("skipped — empty archive"),
            "volume_gate_tolerance_pct": dg.MetadataValue.float(tolerance_pct),
        }

    verdicts = _volume_verdicts(
        run_counts=run_counts,
        archive_counts=archive_counts,
        tolerance_pct=tolerance_pct,
    )
    compared = [verdict for verdict in verdicts if verdict["compared"]]
    failed = [verdict for verdict in compared if not verdict["passed"]]

    for verdict in compared:
        line = (
            f"{verdict['period']}: {verdict['rows']} usable row(s) vs "
            f"{verdict['previous_period']}: {verdict['previous_rows']} "
            f"({verdict['deviation_pct']:+.1f} %, tolerance ±{tolerance_pct:.1f} %)"
        )
        if verdict["passed"]:
            context.log.info(f"Volume gate {line}")
        else:
            context.log.error(f"Volume gate REJECTED {line}")

    if failed:
        raise VolumeGateError(
            f"{FILE_NAME}: "
            + "; ".join(
                f"period {verdict['period']} holds {verdict['rows']} usable row(s), "
                f"{verdict['deviation_pct']:+.1f} % against {verdict['previous_period']} "
                f"({verdict['previous_rows']})"
                for verdict in failed
            )
            + f" — outside the ±{tolerance_pct:.1f} % volume gate. A truncated or "
            f"half-written file looks exactly like this, and accepting it would mark "
            f"everyone it does not contain as departed, because `active` is recomputed "
            f"for the whole population on every delivery. Nothing was committed: ext is "
            f"unchanged and the DFM checkpoint stands still, so the same delivery is "
            f"retried on the next run. Inspect the file; if the change is real, re-run "
            f"with ops.extract_silkeborgdata_e26_pers.config.volume_tolerance_pct set "
            f"above {max(abs(verdict['deviation_pct']) for verdict in failed):.1f}."
        )

    return {
        "volume_gate": dg.MetadataValue.text(
            f"passed — {len(compared)} period(s) compared"
            if compared
            else "passed — no period added that has an older one to compare against"
        ),
        "volume_gate_tolerance_pct": dg.MetadataValue.float(tolerance_pct),
        "volume_gate_tolerance_overridden": dg.MetadataValue.bool(overridden),
        "volume_gate_verdicts": dg.MetadataValue.json(verdicts),
    }


@dg.asset(
    name="extract_silkeborgdata_e26_pers",
    group_name="silkeborgdata_archive",
    compute_kind="python",
    deps=["staging_infra_source_silkeborgdata"],
    tags={
        "domain": "silkeborgdata",
        "layer": "extract",
        "source": "silkeborgdata",
        "entity": "e26pers",
    },
    description=(
        "DFM extract: SilkeborgData E26PERS.TXT from the mounted delivery share -> "
        "ext.silkeborgdata_e26_pers. Delta = delivery folders newer than the "
        "DFM checkpoint on arc.silkeborgdata_e26_pers. A pre-commit volume gate "
        f"rejects any period whose row count moves more than ±{VOLUME_TOLERANCE_PCT:.0f} % "
        "against the previous one, before the rows are committed; override a "
        "confirmed large change with volume_tolerance_pct."
    ),
)
def extract_silkeborgdata_e26_pers(
    context: dg.AssetExecutionContext,
    config: SilkeborgDataExtractConfig,
    mssql_target_staging: MSSQLResource,
    silkeborgdata_share: FileShareResource,
) -> dg.MaterializeResult:
    """Read the new SilkeborgData deliveries into ext.silkeborgdata_e26_pers."""
    encoding = silkeborgdata_share.encoding
    effective_mode = "delta" if config.mode == "ids" else config.mode

    dfm = DfmManager(mssql_target_staging, context)
    with dfm.connection():
        run_ctx = dfm.start_dfm_run(
            "ext",
            ARC_TABLE,
            dagster_run_id=context.run_id,
            mode=effective_mode,
            checkpoint_from=("arc", ARC_TABLE),
            **share_source(silkeborgdata_share.base_dir, FILE_NAME),
        )
        checkpoint = run_ctx.checkpoint
        context.log.info(
            f"DFM extract [{FILE_NAME}] mode={effective_mode}, checkpoint={checkpoint!r}"
        )

        try:
            # Fails hard on a missing or empty folder. A folder one level too high is not
            # empty and passes; checkpoint_not_stalled then only warns.
            base_dir = silkeborgdata_share.root()
            context.log.info(f"Delivery share: {base_dir} (encoding={encoding})")

            discovered = discover_deliveries(base_dir, FILE_NAME)
            selected = select_deliveries(discovered, config, checkpoint, context)

            # Truncate at the oldest folder that is still being written. Dropping
            # only the unsettled folder while loading newer ones would advance the
            # checkpoint past it, and the delta selection would never offer it
            # again — see partition_settled_deliveries().
            selected, unsettled, deferred = partition_settled_deliveries(selected)
            if unsettled:
                context.log.warning(
                    f"Skipping {len(unsettled)} delivery folder(s) still being written "
                    f"(younger than the settle window): "
                    f"{', '.join(d.folder for d in unsettled)}"
                )
            if deferred:
                context.log.warning(
                    f"Deferring {len(deferred)} settled delivery folder(s) newer than "
                    f"the oldest unsettled one: {', '.join(d.folder for d in deferred)}. "
                    f"Loading them now would move the checkpoint past the unsettled "
                    f"folder, which would then never be fetched."
                )

            column_types = _ext_column_types()

            if not selected:
                # The normal state between deliveries. ext is recreated EMPTY
                # rather than left alone: the archive asset runs on in the same
                # job, and an empty source makes its MERGE a true no-op, whereas
                # stale ext rows would re-merge the last delivery every day.
                with mssql_target_staging.get_connection() as conn:
                    write_to_ext_table(conn, ARC_TABLE, column_types, iter(()), context)
                dfm.complete_dfm_run(run_ctx.log_id, rows_affected=0)
                reason = _no_op_reason(
                    discovered=discovered,
                    unsettled=unsettled,
                    deferred=deferred,
                    mode=effective_mode,
                    checkpoint=checkpoint,
                    config=config,
                )
                context.log.info(f"Nothing to load: {reason}")
                return dg.MaterializeResult(
                    metadata={
                        "skipped": dg.MetadataValue.bool(True),
                        "reason": dg.MetadataValue.text(reason),
                        "row_count": dg.MetadataValue.int(0),
                        "deliveries_available": dg.MetadataValue.int(len(discovered)),
                        "deliveries_skipped_unsettled": dg.MetadataValue.int(len(unsettled)),
                        "deliveries_deferred_newer": dg.MetadataValue.int(len(deferred)),
                        "checkpoint_before": dg.MetadataValue.text(str(checkpoint)),
                        "mode": dg.MetadataValue.text(effective_mode),
                        "encoding": dg.MetadataValue.text(encoding),
                        "source": dg.MetadataValue.text("silkeborgdata-share"),
                    }
                )

            context.log.info(
                f"Loading {len(selected)} delivery folder(s), newest first: "
                f"{', '.join(d.folder for d in selected)}"
            )

            stats = _new_stats()
            rows_per_delivery: dict[str, dict[str, int]] = {}
            usable_per_period: dict[date, int] = {}
            bytes_read = sum(_file_size(d.path) for d in selected)

            # Read the gate's baseline on its own short connection, before the
            # load's transaction opens. Reading it inside that transaction would
            # hold shared locks on arc for the whole insert, and the archive
            # MERGE hits the same table later in this job.
            with mssql_target_staging.get_connection() as conn:
                archive_counts = _archive_period_counts(conn)

            tolerance_pct = (
                config.volume_tolerance_pct
                if config.volume_tolerance_pct is not None
                else VOLUME_TOLERANCE_PCT
            )
            gate_metadata: dict[str, Any] = {}

            def _volume_gate(inserted: int) -> None:
                """Pre-commit gate — see _assert_volume_within_tolerance.

                Called by write_to_ext_table after the last flush and before the
                commit, so a rejection rolls the staged rows back instead of
                handing them to the archive MERGE. usable_per_period is complete
                at this point: the generator has been drained.
                """
                context.log.info(
                    f"Volume gate: judging {len(usable_per_period)} period(s) across "
                    f"{inserted} staged row(s) against {len(archive_counts)} archived "
                    f"period(s), tolerance ±{tolerance_pct:.1f} %"
                )
                gate_metadata.update(
                    _assert_volume_within_tolerance(
                        run_counts=usable_per_period,
                        archive_counts=archive_counts,
                        tolerance_pct=tolerance_pct,
                        overridden=config.volume_tolerance_pct is not None,
                        context=context,
                    )
                )

            with mssql_target_staging.get_connection() as conn:
                row_count = write_to_ext_table(
                    conn,
                    ARC_TABLE,
                    column_types,
                    _iter_ext_rows(
                        selected,
                        encoding,
                        stats,
                        config.tolerate_parse_errors,
                        context,
                        rows_per_delivery,
                        usable_per_period,
                    ),
                    context,
                    pre_commit=_volume_gate,
                )

            # A selected delivery whose FILE holds nothing loadable must fail, not
            # pass: ext would be empty, the MERGE a no-op and the checkpoint stuck,
            # so the same folder would be re-selected every day, green, forever.
            #
            # The test is on "usable" (parsed with a complete key), NOT on "kept":
            # a delivery whose rows were all superseded by a newer folder
            # legitimately contributes nothing, and that is what a re-issued period
            # looks like during a backfill. The newer folder's rows carry the newer
            # dw_leverance_dato, so the checkpoint still advances past both.
            unusable = [
                d.folder
                for d in selected
                if rows_per_delivery.get(d.folder, {}).get("usable", 0) == 0
            ]
            if unusable:
                raise ValueError(
                    f"{FILE_NAME}: delivery folder(s) {', '.join(unusable)} were selected "
                    f"and read but yielded 0 usable rows — the file is empty, truncated "
                    f"or has no complete keys. Refusing to report success: an empty load "
                    f"leaves the DFM checkpoint stuck and the same folder would be "
                    f"re-read every day. Inspect the file(s) under {base_dir}."
                )

            _log_stats(context, stats)
            dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)

            return dg.MaterializeResult(
                metadata={
                    "row_count": dg.MetadataValue.int(row_count),
                    "deliveries_loaded": dg.MetadataValue.text(
                        ", ".join(d.folder for d in selected)
                    ),
                    "delivery_dates": dg.MetadataValue.text(
                        ", ".join(d.delivery_date.isoformat() for d in selected)
                    ),
                    "rows_per_delivery": dg.MetadataValue.json(rows_per_delivery),
                    "deliveries_skipped_unsettled": dg.MetadataValue.int(len(unsettled)),
                    "deliveries_deferred_newer": dg.MetadataValue.int(len(deferred)),
                    "checkpoint_before": dg.MetadataValue.text(str(checkpoint)),
                    "mode": dg.MetadataValue.text(effective_mode),
                    "rows_rejected_blank_key": dg.MetadataValue.int(
                        stats["rows_rejected_blank_key"]
                    ),
                    "duplicates_collapsed": dg.MetadataValue.int(stats["duplicates_collapsed"]),
                    "duplicates_across_deliveries": dg.MetadataValue.int(
                        stats["duplicates_across_deliveries"]
                    ),
                    "unparsable_dates": dg.MetadataValue.int(stats["unparsable_dates"]),
                    "unparsable_decimals": dg.MetadataValue.int(stats["unparsable_decimals"]),
                    "trailing_minus_values": dg.MetadataValue.int(stats["trailing_minus_values"]),
                    "bytes_read": dg.MetadataValue.int(bytes_read),
                    "encoding": dg.MetadataValue.text(encoding),
                    "source": dg.MetadataValue.text("silkeborgdata-share"),
                    **gate_metadata,
                }
            )

        except Exception as exc:
            # Without this the failed run sits as 'running' in meta.DFMRunLog
            # until the stale sweep cleans it up.
            dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
            raise


def _no_op_reason(
    *,
    discovered: list[Delivery],
    unsettled: list[Delivery],
    deferred: list[Delivery],
    mode: str,
    checkpoint: str | None,
    config: "SilkeborgDataExtractConfig",
) -> str:
    """Why this run loaded nothing.

    The green no-op is the expected outcome on most days, so it is also the path
    whose message gets read during a real investigation. Reporting "nothing newer
    than the checkpoint" when the actual cause was the settle filter, an empty
    period window, or a share holding no deliveries at all sends that
    investigation the wrong way.
    """
    if not discovered:
        return f"no delivery folder under the share holds {FILE_NAME}"
    if unsettled or deferred:
        folders = ", ".join(d.folder for d in (unsettled + deferred))
        return (
            f"every eligible delivery is still being written, or is newer than one "
            f"that is ({folders}) — deferred to the next run"
        )
    if mode == "period":
        return (
            f"no delivery folder in the requested period "
            f"({config.date_from} .. {config.date_to or 'open'})"
        )
    if mode == "full_load":
        return f"no delivery folder under the share holds {FILE_NAME}"
    return f"no delivery folder newer than the checkpoint ({checkpoint!r})"


def _file_size(path: str) -> int:
    return os.stat(path).st_size


def _log_stats(context: dg.AssetExecutionContext, stats: dict[str, int]) -> None:
    """Log the non-zero counters. Counts only — never field values."""
    if stats["rows_rejected_blank_key"]:
        context.log.warning(
            f"Rejected {stats['rows_rejected_blank_key']} row(s) with a blank "
            f"primary-key column"
        )
    if stats["duplicates_collapsed"]:
        context.log.warning(
            f"Collapsed {stats['duplicates_collapsed']} duplicate key(s) "
            f"({stats['duplicates_across_deliveries']} across delivery folders) "
            f"— kept the newest delivery's row"
        )
    if stats["trailing_minus_values"]:
        context.log.info(f"Normalised {stats['trailing_minus_values']} trailing-minus decimal(s)")
    for counter in ("unparsable_dates", "unparsable_decimals"):
        if stats[counter]:
            context.log.warning(
                f"{stats[counter]} {counter.replace('_', ' ')} converted to NULL "
                f"(tolerate_parse_errors=True)"
            )


__all__ = [
    "ARC_TABLE",
    "DATE_COLUMNS",
    "DECIMAL_COLUMNS",
    "EXT_COLUMNS",
    "FIELD_COUNT",
    "FILE_COLUMNS",
    "FILE_NAME",
    "KEY_COLUMNS",
    "SilkeborgDataExtractConfig",
    "VOLUME_TOLERANCE_PCT",
    "VolumeGateError",
    "extract_silkeborgdata_e26_pers",
]

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DRG-grupperet LPR extraction assets — tar.bz2 deliveries from SDS.

SDS drops full snapshots per (leverancetype, takstsystem) on the NGC share:
`Region_1083_<DRG|DRGPSYK>_<T24|T25|T26>[_<M|U>]_<timestamp>.tar.bz2`. Files
accumulate; the NEWEST file per (leverancetype, takstsystem) is authoritative
regardless of cadence (decision — M and U are packaging
cadences of the same LPR update track, but M covers THREE activity years and
U only two, which the archive handles with year-scoped delete detection, see
drg_archive.py).

The share is a read-only SMB mount, the ``drg_share`` resource bound to
DRG_BASE_DIR — an unset variable is a hard error, never a fallback (an
unmounted share and "no new deliveries" must not look alike).

Each tar contains one folder (T2x/) with semicolon-separated, fully quoted
cp1252 CSVs plus SDS' own column schemas (Metadata_*.csv) and an export log
(SDP_ExportLog_*.csv) with the row count per file. Members are streamed
straight from the tar into the ext tables — the ~16 GB somatik deliveries are
never unpacked to disk. Row counts are validated against the export log after
each archive: a mismatch is a delivery defect and fails the run.

ext is a 1:1 mirror of the source INCLUDING the per-delivery integer IDs —
they are needed for the intra-delivery UUID resolution views in
drg_archive.py, and ext is the debugging surface for the latest delivery.
The archive layer deliberately excludes them (review decision).

Key columns are validated against their declared max width so the narrow
archive keys can never abort a MERGE with a truncation error (the HAIBA
lesson). GDPR: the files carry raw CPR — row contents are never
logged or reported, only file names, counts and delivery dates.
"""

import os
import re
import tarfile
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from typing import TypedDict, cast

import dagster as dg
from core.dfm_manager import DfmManager
from core.dfm_source import share_connection
from core.file_delivery import DELIMITER, iter_members, open_member_csv
from core.resources import FileShareResource, MSSQLResource
from DSI.connectors.delivery_detection import (
    DELIVERY_SCHEMA,
    DELIVERY_TARGET_TYPE,
    delivery_identity,
    delivery_unit,
    is_new_delivery,
)
from DSI.csv_files._sds_tar import (
    INSERT_CHUNK_SIZE,
    ExtWriter,
    dataset_prefix,
    execute_statements,
    export_log_deviations,
    ext_create_sql,
    has_mojibake,
    parse_archive_timestamp,
    parse_export_log,
    run_writers,
    schema_drift,
)
from DSI.csv_files.drg_datasets import (
    MIN_VALUE_WIDTH,
    PSYK_DATASETS,
    SOMATIK_DATASETS,
    DrgDataset,
)
from pydantic import Field, field_validator

TAKSTSYSTEM_COLUMN = "takstsystem"
DELIVERY_COLUMN = "leverance_dato"

# One row per delivery this run actually loaded — the contract the archive
# layer reads. Lives in ext because it is per-run provenance, and
# there is one per leverancetype because the two extracts own separate ext
# tables and must never clear each other's manifest.
MANIFEST_SUFFIX = "leverance_manifest"

# Sources for the delivery's data cap, recorded alongside it so a derived cap
# is visible as such rather than inferred later.
CAP_REAL = "reel"
CAP_DERIVED = "afledt"

# Forventet kadence: somatik faar U-leverancer ugentligt (mandag)
# og M ca. d. 14.-18.; psyk kun M ca. d. 12. Graenserne giver slack for
# helligdage. Kun den FRISKESTE leverance pr. leverancetype vurderes —
# pensionerede takstsystemer (fx somatik T24) faar aldrig nye filer.
MAX_DELIVERY_AGE_DAYS = {"DRG": 10, "DRGPSYK": 40}

# Region_1083_DRG_T26_U_20260824190546.tar.bz2 (kadence optional on the
# oldest files: Region_1083_DRG_T24_20250812235425.tar.bz2).
ARCHIVE_RE = re.compile(
    r"^Region_1083_(?P<leverancetype>DRG|DRGPSYK)_(?P<takstsystem>T\d{2})"
    r"(?:_(?P<kadence>[MU]))?_(?P<timestamp>\d{14})\.tar\.bz2$"
)

# Data member: T26_DRGKONTAKT_1083_20260823.csv / T26_Psyk_kontakt_1083_20260810.csv
DATA_MEMBER_RE = re.compile(r"^T\d{2}_(?P<prefix>.+)_1083_\d{8}\.csv$")


@dataclass(frozen=True)
class DrgArchiveFile:
    """One tar.bz2 delivery on the share."""

    path: str
    leverancetype: str
    takstsystem: str
    kadence: str | None
    timestamp: datetime

    @property
    def filename(self) -> str:
        return os.path.basename(self.path)

    @property
    def delivery_date(self) -> date:
        return self.timestamp.date()


class DrgExtractConfig(dg.Config):
    kadence: str | None = Field(
        default=None,
        description=(
            "Cadence this run handles: 'M' (monthly — also picks up the oldest "
            "files, which carry no cadence letter) or 'U' (weekly). Set by the "
            "job. Left unset, the run takes the newest file per takstsystem "
            "regardless of cadence AND performs no delivery detection, i.e. the "
            "pre-detection behaviour — useful for an ad-hoc reload, expensive "
            "as a schedule."
        ),
    )
    takstsystemer: list[str] | None = Field(
        default=None,
        description=(
            "Limit the run to these takstsystemer (e.g. ['T26']). Default: every "
            "takstsystem present in the delivery folder. NOTE: the archive's "
            "delete detection is scoped to the (takstsystem, aktivitetsaar) "
            "slices actually present in ext, so a filtered run only refreshes "
            "the filtered slices."
        ),
    )
    delivery_on_or_before: str | None = Field(
        default=None,
        description=(
            "Pin the run to deliveries packed on/before this ISO date "
            "(YYYY-MM-DD) for reproducing an earlier state. Default: newest."
        ),
    )
    max_delivery_age_days: int | None = Field(
        default=None,
        description=(
            "Override the staleness limit (days) for the newest usable "
            "delivery. Defaults per leverancetype: DRG=10 (U is weekly), "
            "DRGPSYK=40 (M is monthly). The check is skipped when "
            "delivery_on_or_before is set — a pinned run is deliberately old."
        ),
    )

    # Validator, not typing.Literal — Dagster's config mapping cannot express
    # Literal. A typo ("m") must be rejected at launch: it would otherwise
    # filter every file away and be green as "intet nyt" forever.
    @field_validator("kadence")
    @classmethod
    def _kadence_er_kendt(cls, value: str | None) -> str | None:
        if value is not None and value not in VALID_KADENCER:
            raise ValueError(f"kadence {value!r} skal vaere en af {VALID_KADENCER} eller None")
        return value


def parse_archive_name(name: str) -> tuple[str, str, str | None, datetime] | None:
    match = ARCHIVE_RE.match(name)
    if not match:
        return None
    timestamp = parse_archive_timestamp(match["timestamp"])
    if timestamp is None:
        return None
    return match["leverancetype"], match["takstsystem"], match["kadence"], timestamp


# The checkpoint key space per takstsystem is exactly these two: the cadence
# comes from the JOB, not from the filename's cadence letter. A file with no
# letter is mapped to "M" by the monthly job's file filter, at selection time —
# so an unmapped None here would silently become a third bucket.
VALID_KADENCER = ("M", "U")
VALID_LEVERANCETYPER = ("DRG", "DRGPSYK")
_TAKSTSYSTEM_RE = re.compile(r"^T\d{2}$")


def drg_delivery_unit(leverancetype: str, takstsystem: str, kadence: str) -> str:
    """DRG's delivery unit: (leverancetype, takstsystem, kadence).

    The leverancetype IS the unit prefix — "drg_leverance_T26_M" and
    "drgpsyk_leverance_T26_M" — rather than a "drg" prefix with the
    leverancetype repeated inside. It carries the same information in one
    place, and it matches how the two feeds are already told apart everywhere
    else in this source (arc.drg_kontakt vs arc.drgpsyk_kontakt).

    Lives here rather than in the shared detection module: that module must not
    name the systems that use it, or every new source would have to edit it.
    """
    if leverancetype not in VALID_LEVERANCETYPER:
        raise ValueError(f"leverancetype {leverancetype!r} must be one of {VALID_LEVERANCETYPER}")
    if not _TAKSTSYSTEM_RE.match(takstsystem or ""):
        raise ValueError(f"takstsystem {takstsystem!r} must look like 'T26'")
    if kadence not in VALID_KADENCER:
        raise ValueError(
            f"kadence {kadence!r} must be one of {VALID_KADENCER} — it is the "
            f"JOB's cadence, so a file without a cadence letter belongs to the "
            f"monthly job's 'M' bucket by its file filter, not by a None key"
        )
    return delivery_unit(leverancetype.lower(), takstsystem, kadence)


def matches_kadence(archive: DrgArchiveFile, kadence: str | None) -> bool:
    """Does this file belong to the cadence the run is handling?

    The monthly bucket also takes files with NO cadence letter: the oldest
    deliveries (4 x T24, 4 x T25) predate the M/U convention and are monthly
    full deliveries. That defaulting lives HERE, at selection time, and nowhere
    else — the checkpoint key's cadence comes from the job, so an unmapped None
    would otherwise become a silent third bucket.
    """
    if kadence is None:
        return True
    if kadence == "M":
        return archive.kadence in ("M", None)
    return archive.kadence == kadence


def detection_enabled(kadence: str | None, on_or_before: date | None) -> bool:
    """Should this run skip deliveries it has already ingested?

    No, for the two deliberate re-reads: a pin reproduces an earlier state, and
    an unset cadence is an ad-hoc reload. Detection would skip exactly the
    delivery the operator is trying to re-read. Same convention RunModeConfig
    uses for period/ids, which never advance a checkpoint either.
    """
    return kadence is not None and on_or_before is None


def select_pending(
    archives: list[DrgArchiveFile],
    checkpoints: dict[str, str | None],
) -> tuple[list[DrgArchiveFile], list[str]]:
    """Drop the deliveries already ingested, per takstsystem.

    This is tier 1: pure file identity against the DFM checkpoint, decided on
    the filename alone. It is what makes a run with no new delivery cost an
    os.listdir instead of ~16 GB of decompression.
    """
    pending: list[DrgArchiveFile] = []
    notes: list[str] = []
    for archive in archives:
        identity = delivery_identity(archive.timestamp)
        if is_new_delivery(checkpoints.get(archive.takstsystem), identity):
            pending.append(archive)
        else:
            notes.append(f"{archive.takstsystem}: {archive.filename} er allerede indlaest")
    return pending, notes


def discover_archives(root: str, leverancetype: str) -> list[DrgArchiveFile]:
    """Every parseable delivery for the leverancetype, Arkiv/ excluded."""
    found: list[DrgArchiveFile] = []
    for name in sorted(os.listdir(root)):
        parsed = parse_archive_name(name)
        if parsed is None:
            continue
        ltype, takstsystem, kadence, timestamp = parsed
        if ltype != leverancetype:
            continue
        found.append(
            DrgArchiveFile(
                path=os.path.join(root, name),
                leverancetype=ltype,
                takstsystem=takstsystem,
                kadence=kadence,
                timestamp=timestamp,
            )
        )
    return found


def is_usable(
    archive: DrgArchiveFile, datasets: tuple[DrgDataset, ...], encoding: str
) -> str | None:
    """Cheap pre-flight: stream the tar start and check the export log.

    Returns None when usable, otherwise a reason. Metadata files and the
    export log precede the data files, so only the first few MB are
    decompressed. A delivery is unusable when the export log is missing or
    reports zero rows for a mandatory dataset (the header-only lesson from
    HAIBA: an empty delivery must never win as "newest").
    """
    expected_prefixes = {d.source_prefix for d in datasets}
    try:
        with tarfile.open(archive.path, mode="r|bz2") as tar:
            for member in tar:
                base = os.path.basename(member.name)
                if not base.startswith("SDP_ExportLog"):
                    continue
                handle = tar.extractfile(member)
                if handle is None:
                    return "SDP_ExportLog kunne ikke laeses"
                rows = _parse_export_log(handle.read().decode(encoding))
                for prefix in expected_prefixes:
                    counts = [n for f, n in rows.items() if _dataset_prefix(f) == prefix]
                    if not counts:
                        return f"datasaettet {prefix} mangler i eksportloggen"
                    if max(counts) == 0 and prefix != "LEVERANCEINFO":
                        return f"datasaettet {prefix} har 0 raekker"
                return None
    except (tarfile.TarError, OSError, EOFError) as exc:
        return f"arkivet kunne ikke aabnes: {exc}"
    return "SDP_ExportLog mangler i arkivet"


def select_newest(
    archives: list[DrgArchiveFile],
    datasets: tuple[DrgDataset, ...],
    encoding: str,
    context: dg.AssetExecutionContext | None = None,
    on_or_before: date | None = None,
    takstsystemer: list[str] | None = None,
) -> tuple[dict[str, DrgArchiveFile], list[str]]:
    """Newest USABLE delivery per takstsystem, regardless of cadence.

    Walks candidates newest-first and takes the first that passes the
    pre-flight. Fallbacks are reported as notes so they are visible in the
    Dagster UI instead of silent — accepting an unusable file as "newest"
    would soft-delete the takstsystem's year slices in the archive.
    """
    notes: list[str] = []
    selected: dict[str, DrgArchiveFile] = {}
    by_takst: dict[str, list[DrgArchiveFile]] = {}
    for archive in archives:
        if takstsystemer is not None and archive.takstsystem not in takstsystemer:
            continue
        if on_or_before is not None and archive.delivery_date > on_or_before:
            continue
        by_takst.setdefault(archive.takstsystem, []).append(archive)

    for takstsystem in sorted(by_takst):
        candidates = sorted(by_takst[takstsystem], key=lambda a: a.timestamp, reverse=True)
        for candidate in candidates:
            reason = is_usable(candidate, datasets, encoding)
            if reason is None:
                selected[takstsystem] = candidate
                break
            note = f"{takstsystem}: springer {candidate.filename} over ({reason})"
            notes.append(note)
            if context is not None:
                context.log.warning(note)
        else:
            note = f"{takstsystem}: INGEN brugbar leverance fundet"
            notes.append(note)
            if context is not None:
                context.log.error(note)
    return selected, notes


def check_delivery_age(
    selected: dict[str, DrgArchiveFile],
    discovered_takstsystemer: set[str],
    leverancetype: str,
    today: date,
    max_age_days: int | None = None,
) -> dict[str, int]:
    """Foraeldet leverance-alarm: det AKTIVE takstsystem skal have
    en frisk, brugbar leverance.

    Det aktive takstaar er altid det hoejeste takstsystem paa sharet (T26 >
    T25 > ...): pensionerede takstsystemer faar aldrig nye filer og er
    forventet gamle, og en sen genleverance paa et gammelt takstsystem maa
    ikke skjule, at det produktive spor er doedt. Er det hoejeste takstsystem
    slet ikke i `selected` (alle dets filer afvist af pre-flighten), er det
    samme alarm. Fejler haardt frem for at advare — en foraeldet/manglende
    nyeste fil betyder at SMB-mountet er defekt, at SDS er holdt op med at
    levere, eller at pre-flighten afviser alt.
    Returnerer alderen i dage pr. takstsystem til asset-metadata.

    Naas kun af ufiltrerede ad-hoc-koersler: alle schedulerede jobs saetter
    kadence, saa det schedulerede check ejes af drg_leverance_freshness.
    """
    limit = max_age_days if max_age_days is not None else MAX_DELIVERY_AGE_DAYS[leverancetype]
    ages = {t: (today - archive.delivery_date).days for t, archive in selected.items()}
    if not discovered_takstsystemer:
        return ages
    active = max(discovered_takstsystemer)
    if active not in selected:
        raise ValueError(
            f"{leverancetype}: det aktive takstsystem {active} har INGEN "
            f"brugbar leverance (alle filer afvist af pre-flighten) — "
            f"undersoeg leverancerne paa sharet."
        )
    if ages[active] > limit:
        raise ValueError(
            f"{leverancetype}: det aktive takstsystem {active}s nyeste "
            f"brugbare leverance er {ages[active]} dage gammel (graense: "
            f"{limit}). Er SMB-sharet mountet, og leverer SDS stadig? "
            f"Overstyr med max_delivery_age_days hvis alderen er forklaret."
        )
    return ages


def manifest_table(leverancetype: str) -> str:
    """ext table holding this leverancetype's delivery manifest."""
    return f"{leverancetype.lower()}_{MANIFEST_SUFFIX}"


def _manifest_create_sql(leverancetype: str) -> str:
    return (
        f"DROP TABLE IF EXISTS [ext].[{manifest_table(leverancetype)}];\n"
        f"CREATE TABLE [ext].[{manifest_table(leverancetype)}] (\n"
        f"    [leverancetype] NVARCHAR(10) NOT NULL,\n"
        f"    [{TAKSTSYSTEM_COLUMN}] NVARCHAR(3) NOT NULL,\n"
        f"    [kadence] NVARCHAR(1) NULL,\n"
        f"    [leverance_ts] DATETIME2(0) NOT NULL,\n"
        f"    [filnavn] NVARCHAR(200) NOT NULL,\n"
        f"    [{DELIVERY_COLUMN}] DATE NOT NULL,\n"
        f"    [lpropdateringsdato] NVARCHAR(10) NULL,\n"
        f"    [cap_kilde] NVARCHAR(6) NULL,\n"
        f"    [dw_timestamp_insert] DATETIME2(0) NOT NULL DEFAULT GETDATE()\n"
        f");"
    )


def normalise_cap(value: str) -> str:
    """LPROPDATERINGSDATO as an ISO date, or a loud failure.

    The cap decides which activity-year slices may overwrite the archive, and
    it is compared as a STRING. A non-ISO spelling would compare wrongly and
    mis-order M against U silently — exactly the failure the design refuses to
    accept — so an unrecognised value fails the run instead.
    """
    text = (value or "").strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError(
            f"LPROPDATERINGSDATO {text!r} is not an ISO date. The cap is "
            f"compared as a string, so an unknown spelling would silently "
            f"mis-order deliveries — verify the delivery before loading."
        ) from exc


def read_delivery_cap(conn, datasets: tuple[DrgDataset, ...], takstsystem: str) -> str | None:
    """The delivery's real data cap, from the LEVERANCEINFO member in ext.

    Returns None when the leverancetype has no LEVERANCEINFO dataset at all
    (psyk), which is the signal that the slice-level comparison does not apply.
    """
    info = next((d for d in datasets if d.source_prefix == "LEVERANCEINFO"), None)
    if info is None:
        return None
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT DISTINCT [LPROPDATERINGSDATO] FROM [ext].[{info.ext_table}] "
            f"WHERE [{TAKSTSYSTEM_COLUMN}] = ?",
            takstsystem,
        )
        values = [row[0] for row in cursor.fetchall() if row[0] and row[0].strip()]
    finally:
        cursor.close()
    # Normalise every value BEFORE taking the max: MAX on the raw NVARCHAR only
    # orders correctly if all spellings are ISO, and would let normalise_cap
    # validate only the (possibly wrongly chosen) winner.
    return max(normalise_cap(value) for value in values) if values else None


def write_manifest(
    conn,
    leverancetype: str,
    archive: DrgArchiveFile,
    cap: str | None,
    cap_kilde: str | None,
) -> None:
    """Record that this run loaded `archive`, with the cap it carries."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"INSERT INTO [ext].[{manifest_table(leverancetype)}] "
            f"([leverancetype], [{TAKSTSYSTEM_COLUMN}], [kadence], [leverance_ts], "
            f"[filnavn], [{DELIVERY_COLUMN}], [lpropdateringsdato], [cap_kilde]) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            leverancetype,
            archive.takstsystem,
            archive.kadence,
            archive.timestamp,
            archive.filename,
            archive.delivery_date.isoformat(),
            cap,
            cap_kilde,
        )
        conn.commit()
    finally:
        cursor.close()


# Freshness caps per (leverancetype, kadence). U is weekly, M monthly — an
# M-check on the U cap would false-alarm every month between the 18th and the
# next 10th.
FRESHNESS_CAPS = {("DRG", "U"): 10, ("DRG", "M"): 40, ("DRGPSYK", "M"): 40}


def top_two_takstsystemer(archives: list[DrgArchiveFile], leverancetype: str) -> list[str]:
    """The takstsystemer a live feed is expected from, per leverancetype.

    Measured from the delivery inventory (drg_profileringsrapport.md §4), not
    guessed: several takstsystemer deliver M at once (T25 M and T26 M share
    2026-08-18), and a series only stops AFTER it has become third-highest
    (somatik T24 M stopped 2026-03-17; T26 M started 2025-12-14). So the two
    highest are exactly the ones that should still be arriving — a closed
    tariff year leaves the set by itself, with no configuration.
    """
    return sorted({a.takstsystem for a in archives if a.leverancetype == leverancetype})[-2:]


def evaluate_freshness(
    archives: list[DrgArchiveFile],
    today: date,
    caps: dict[tuple[str, str], int] | None = None,
) -> tuple[list[str], dict[str, int]]:
    """Stale-feed problems on the share, and the age of each checked series.

    Reads filenames only — no tar is opened, no database is touched, and no
    checkpoint or run filter is consulted. It reports on the SHARE, not on what
    a given run chose to load, which is the whole reason it lives outside the
    load path.
    """
    limits = {**FRESHNESS_CAPS, **(caps or {})}
    problems: list[str] = []
    ages: dict[str, int] = {}
    for leverancetype in ("DRG", "DRGPSYK"):
        systems = top_two_takstsystemer(archives, leverancetype)
        if not systems:
            continue
        in_scope = [
            a for a in archives if a.leverancetype == leverancetype and a.takstsystem in systems
        ]
        for kadence in ("M", "U"):
            limit = limits.get((leverancetype, kadence))
            if limit is None:
                continue
            relevant = [a for a in in_scope if matches_kadence(a, kadence)]
            if not relevant:
                # The series has never existed for these takstsystemer (psyk has
                # no U at all). Nothing to be stale about.
                continue
            if kadence == "U":
                # Aggregated across the two: the U series belongs to the ACTIVE
                # tariff year and moves between takstsystemer at rollover, with
                # an overlap (T26 U started 2026-02-25, T25 U stopped
                # 2026-03-09). Checking it per takstsystem would false-alarm for
                # the months a new takstsystem has M but no U yet.
                newest = max(relevant, key=lambda a: a.timestamp)
                age = (today - newest.delivery_date).days
                ages[f"{leverancetype}/U"] = age
                if age > limit:
                    problems.append(
                        f"{leverancetype}/U: nyeste U-leverance ({newest.filename}) er "
                        f"{age} dage gammel (graense {limit}) — det ugentlige spor ser doedt ud"
                    )
                continue
            for takstsystem in systems:
                files = [a for a in relevant if a.takstsystem == takstsystem]
                if not files:
                    continue
                newest = max(files, key=lambda a: a.timestamp)
                age = (today - newest.delivery_date).days
                ages[f"{leverancetype}/M/{takstsystem}"] = age
                if age > limit:
                    problems.append(
                        f"{leverancetype}/M/{takstsystem}: nyeste leverance "
                        f"({newest.filename}) er {age} dage gammel (graense {limit})"
                    )
    return problems, ages


def _dataset_prefix(csv_filename: str) -> str | None:
    return dataset_prefix(csv_filename, DATA_MEMBER_RE)


_parse_export_log = parse_export_log


def validate_member_schema(
    dataset: DrgDataset, columns: list[str], filename: str, takstsystem: str | None = None
) -> None:
    """SDS' own Metadata_* schema against the union spec — fail loudly on drift.

    Optional columns may only be missing for the takstsystemer where they are
    KNOWN to be missing (T24/T25). A column that disappears from a newer
    takstsystem's schema is a regression: the merge would overwrite the
    archived values with NULL across the whole slice.
    """
    allowed_missing = (
        set(dataset.optional_columns)
        if takstsystem is None or takstsystem in dataset.optional_takstsystemer
        else set()
    )
    unknown, missing = schema_drift(columns, dataset.source_columns, allowed_missing)
    if unknown:
        raise ValueError(
            f"{filename}: ukendte kolonner {unknown} — SDS har aendret skemaet. "
            f"Opdater drg_datasets.py (og arc-DDL) foer indlaesning."
        )
    if missing:
        raise ValueError(
            f"{filename}: obligatoriske kolonner mangler: {missing} — SDS har "
            f"aendret skemaet. En kolonne der forsvinder fra et nyere "
            f"takstsystem ville NULLe arkivets vaerdier ved merge."
        )


def _ext_create_sql(dataset: DrgDataset) -> str:
    """Key columns are NOT NULL at their declared (narrow) width; every other
    column gets at least MIN_VALUE_WIDTH so ext and arc can never diverge."""
    key_source_columns = {k for k in dataset.merge_keys if k in dataset.source_columns}
    columns: list[tuple[str, int, bool]] = []
    for name in dataset.source_columns:
        if name in key_source_columns:
            columns.append(
                (name, dataset.key_widths.get(name, dataset.source_widths.get(name, 100)), True)
            )
        else:
            columns.append(
                (name, max(MIN_VALUE_WIDTH, dataset.source_widths.get(name, 100)), False)
            )
    return ext_create_sql(
        dataset.ext_table, [f"[{TAKSTSYSTEM_COLUMN}] NVARCHAR(3) NOT NULL"], columns
    )


class _ExtWriter(ExtWriter):
    """Chunked writer for one ext table; created lazily on first data row.

    INSERT_CHUNK_SIZE is read from THIS module at construction time, so a test
    that patches drg.INSERT_CHUNK_SIZE still forces a flush per row.
    """

    def __init__(self, conn, dataset: DrgDataset, context: dg.AssetExecutionContext):
        super().__init__(
            conn,
            dataset.ext_table,
            [TAKSTSYSTEM_COLUMN, *dataset.source_columns, DELIVERY_COLUMN],
            context,
            chunk_size=INSERT_CHUNK_SIZE,
            row_word="raekker",
        )
        self.dataset = dataset


def _stream_data_member(
    handle,
    dataset: DrgDataset,
    archive: DrgArchiveFile,
    writer: _ExtWriter,
    encoding: str,
    filename: str,
    aar_counter: Counter,
) -> int:
    """Stream one CSV member from the tar into its ext table. Returns row count."""
    reader = open_member_csv(handle, encoding, DELIMITER)
    header = next(reader, None)
    if header is None:
        raise ValueError(f"{filename}: filen er tom (ingen header)")
    validate_member_schema(dataset, header, filename, takstsystem=archive.takstsystem)

    key_source_columns = {k for k in dataset.merge_keys if k in dataset.source_columns}
    positions: list[tuple[int, int | None]] = []  # (index i source_columns, index i header)
    header_index = {name: i for i, name in enumerate(header)}
    for i, name in enumerate(dataset.source_columns):
        positions.append((i, header_index.get(name)))
    aar_index = (
        header_index.get(dataset.aktivitetsaar_column) if dataset.aktivitetsaar_column else None
    )
    key_width_overflow: Counter = Counter()
    mojibake: Counter = Counter()

    delivery = archive.delivery_date.isoformat()
    takstsystem = archive.takstsystem
    n = 0
    for raw in reader:
        if not raw:
            continue
        if len(raw) != len(header):
            raise ValueError(
                f"{filename}: raekke {reader.line_num} har {len(raw)} felter, "
                f"headeren har {len(header)}"
            )
        n += 1
        row_defective = False
        values: list[str | None] = [None] * len(dataset.source_columns)
        for spec_index, src_index in positions:
            if src_index is None:
                continue  # optional kolonne der ikke findes i dette takstsystems skema
            value = raw[src_index].strip()
            name = dataset.source_columns[spec_index]
            if has_mojibake(value):
                mojibake[name] += 1
            if name in key_source_columns:
                max_width = dataset.key_widths.get(name, dataset.source_widths.get(name, 100))
                if len(value) > max_width:
                    key_width_overflow[name] += 1
                    row_defective = True
                    continue  # taelles og fejler samlet nedenfor — vaerdien logges ALDRIG
                values[spec_index] = (
                    value  # noeglekolonner beholder '' (NULL matcher aldrig i MERGE ON)
                )
            else:
                values[spec_index] = value or None
        if aar_index is not None:
            aar_counter[raw[aar_index].strip()] += 1
        if row_defective:
            # Raekken maa aldrig naa databasen: NULL i en NOT NULL-noegle ville
            # give en raa driverfejl ved naeste buffer-flush FOER den designede
            # (GDPR-sikre) fejl nedenfor kunne kastes.
            continue
        writer.add((takstsystem, *values, delivery))
    if key_width_overflow:
        # GDPR: kun kolonnenavne og antal — aldrig vaerdierne.
        raise ValueError(
            f"{filename}: noeglekolonner overskrider deres deklarerede bredde: "
            f"{dict(key_width_overflow)}. Arkivets noegler er bevidst smalle "
            f" — undersoeg leverancen."
        )
    if mojibake:
        # GDPR: kun kolonnenavne og antal — aldrig vaerdierne.
        raise ValueError(
            f"{filename}: mulig encoding-korruption (U+FFFD/mojibake) i "
            f"kolonner: {dict(mojibake)}. Filen matcher naeppe {encoding!r} — "
            f"undersoeg leverancen foer indlaesning."
        )
    return n


class LoadArchiveResult(TypedDict):
    fil: str
    leverance_dato: str
    kadence: str
    raekker: dict[str, int]
    aktivitetsaar: dict[str, int]


def load_archive_into_ext(
    conn,
    archive: DrgArchiveFile,
    datasets: tuple[DrgDataset, ...],
    writers: dict[str, _ExtWriter],
    encoding: str,
    context: dg.AssetExecutionContext,
) -> LoadArchiveResult:
    """Stream every data member of one delivery into the ext tables.

    Member order in SDS' archives is alphabetical (Metadata_* < SDP_ExportLog
    < data files), but the loader is deliberately order-robust: actual row
    counts are collected during the stream and validated against the export
    log AFTER the whole archive is read.
    """
    by_prefix = {d.source_prefix: d for d in datasets}
    export_log: dict[str, int] | None = None
    actual_counts: dict[str, int] = {}
    aar_counter: Counter = Counter()
    context.log.info(f"laeser {archive.filename}")

    for base, handle in iter_members(archive.path):
        if base.startswith("Metadata_"):
            continue  # skemaet valideres mod headeren i selve datafilen
        if base.startswith("SDP_ExportLog"):
            export_log = _parse_export_log(handle.read().decode(encoding))
            continue
        prefix = _dataset_prefix(base)
        if prefix is None:
            context.log.warning(f"{archive.filename}: ignorerer ukendt medlem {base}")
            continue
        dataset = by_prefix.get(prefix)
        if dataset is None:
            raise ValueError(
                f"{archive.filename}: ukendt datasaet {base} — SDS har "
                f"tilfoejet et datasaet. Opdater drg_datasets.py."
            )
        actual_counts[base] = _stream_data_member(
            handle, dataset, archive, writers[dataset.ext_table], encoding, base, aar_counter
        )

    if export_log is None:
        raise ValueError(
            f"{archive.filename}: SDP_ExportLog mangler — kontroltotaller kan ikke haandhaeves"
        )
    deviations = export_log_deviations(export_log, actual_counts)
    if deviations:
        raise ValueError(
            f"{archive.filename}: raekkeantal afviger fra SDP_ExportLog: {deviations} — "
            f"leverancen er defekt eller indlaesningen ufuldstaendig."
        )
    return {
        "fil": archive.filename,
        "leverance_dato": archive.delivery_date.isoformat(),
        "kadence": archive.kadence or "-",
        "raekker": dict(sorted(actual_counts.items())),
        "aktivitetsaar": dict(sorted(aar_counter.items())),
    }


def _read_checkpoints(
    mssql: MSSQLResource,
    context: dg.AssetExecutionContext,
    leverancetype: str,
    kadence: str,
    candidates: list[DrgArchiveFile],
) -> dict[str, str | None]:
    """The delivery already ingested per takstsystem, from meta.DFMState.

    A read-only peek through the framework's existing checkpoint API — the
    detection adds no DFM machinery of its own. A takstsystem with no row has
    never been loaded, and NULL reads as "everything is new".
    """
    dfm = DfmManager(mssql, context)
    checkpoints: dict[str, str | None] = {}
    with dfm.connection():
        for takstsystem in sorted({a.takstsystem for a in candidates}):
            unit = drg_delivery_unit(leverancetype, takstsystem, kadence)
            checkpoints[takstsystem] = dfm.get_checkpoint(
                DELIVERY_SCHEMA, unit, target_type=DELIVERY_TARGET_TYPE
            )
    return checkpoints


def _run_extract(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    share: FileShareResource,
    datasets: tuple[DrgDataset, ...],
    leverancetype: str,
    config: DrgExtractConfig,
) -> dg.MaterializeResult:
    root = share.root()
    encoding = share.encoding
    on_or_before = None
    if config.delivery_on_or_before:
        on_or_before = date.fromisoformat(config.delivery_on_or_before)
        context.log.warning(f"leverance-pin aktiv: paa/foer {on_or_before}")

    archives = discover_archives(root, leverancetype)
    candidates = [a for a in archives if matches_kadence(a, config.kadence)]
    if config.kadence is not None:
        context.log.info(
            f"kadence {config.kadence}: {len(candidates)} af {len(archives)} filer i spil"
        )

    # Tier 1. A pin or a backfill deliberately re-reads an already-ingested
    # delivery, so detection is off there — the same convention RunModeConfig
    # uses for period/ids.
    detection_on = detection_enabled(config.kadence, on_or_before)
    already_loaded: list[str] = []
    if detection_on:
        checkpoints = _read_checkpoints(
            mssql, context, leverancetype, cast(str, config.kadence), candidates
        )
        candidates, already_loaded = select_pending(candidates, checkpoints)
        for note in already_loaded:
            context.log.info(note)
    else:
        context.log.warning(
            "leverancedetektion er slaaet fra for denne koersel — hele den "
            "nyeste leverance genlaeses (saet kadence, og undlad "
            "delivery_on_or_before, for at slaa den til)"
        )

    selected, notes = select_newest(
        candidates,
        datasets,
        encoding,
        context=context,
        on_or_before=on_or_before,
        takstsystemer=config.takstsystemer,
    )
    if not selected:
        if detection_on:
            # Nothing pending is the NORMAL outcome on most days. ext is left
            # exactly as it was — the tables are not even recreated, so the
            # previous delivery stays available for debugging. The manifest
            # must still be EMPTIED, though: the arc assets declare their
            # no-op off it, and a stale manifest would keep scoping every
            # downstream run to the previous delivery.
            with mssql.get_connection() as conn:
                execute_statements(conn, _manifest_create_sql(leverancetype))
            context.log.info(f"{leverancetype}: ingen nye leverancer — ext roeres ikke")
            return dg.MaterializeResult(
                metadata={
                    "leverancetype": leverancetype,
                    "besluttet": "intet nyt",
                    "kadence": config.kadence,
                    "allerede_indlaest": already_loaded or ["ingen"],
                    "fallbacks": notes or ["ingen"],
                }
            )
        raise ValueError(
            f"Ingen brugbare {leverancetype}-leverancer fundet i {root} "
            f"(takstsystem-filter: {config.takstsystemer}, kadence: {config.kadence})"
        )

    leverance_alder: dict[str, int] = {}
    if on_or_before is not None:
        context.log.warning("leverance-pin aktiv — foraeldelses-checket springes over")
    elif config.kadence is None and config.takstsystemer is None:
        discovered = {a.takstsystem for a in archives}
        leverance_alder = check_delivery_age(
            selected, discovered, leverancetype, date.today(), config.max_delivery_age_days
        )
    else:
        # A filtered run sees one cadence and often one takstsystem, so
        # "the active takstsystem" cannot be derived here without lying. The
        # standalone freshness asset owns that check — it reads the whole share
        # and is deliberately independent of this run's filters.
        context.log.info(
            "filtreret koersel — foraeldelses-checket haandteres af det "
            "selvstaendige freshness-asset, ikke her"
        )

    manifest_rows: list[dict] = []
    context.log.info(
        f"indlaeser {len(selected)} leverance(r): "
        + ", ".join(a.filename for a in selected.values())
    )
    with (
        DfmManager(mssql, context).target_runs(
            "ext",
            {d.ext_table: d.source_prefix for d in datasets},
            dagster_run_id=context.run_id,
            mode="full_load",
            **share_connection(root),
        ) as rows,
        mssql.get_connection() as conn,
    ):
        # One commit for the whole DDL block: the ext tables and the manifest are
        # recreated together or not at all.
        for dataset in datasets:
            execute_statements(conn, _ext_create_sql(dataset), commit=False)
        execute_statements(conn, _manifest_create_sql(leverancetype), commit=False)
        conn.commit()

        writers = {d.ext_table: _ExtWriter(conn, d, context) for d in datasets}

        def _load_selected() -> list[LoadArchiveResult]:
            return [
                load_archive_into_ext(
                    conn, selected[takstsystem], datasets, writers, encoding, context
                )
                for takstsystem in sorted(selected)
            ]

        # run_writers discards the buffers on any exception, so a designed
        # (GDPR-safe) failure mid-load is the error that surfaces instead of a
        # driver error from the doomed final INSERT (review F19).
        per_takst = run_writers(writers, _load_selected)
        conn.commit()

        # The manifest is written AFTER the rows commit, and the cap is read
        # back out of ext: LEVERANCEINFO sorts after the big data members in
        # the tar, so the cap is not known while they stream.
        supports_cap = any(d.source_prefix == "LEVERANCEINFO" for d in datasets)
        for takstsystem in sorted(selected):
            archive = selected[takstsystem]
            cap: str | None = None
            cap_kilde: str | None = None
            if supports_cap:
                cap = read_delivery_cap(conn, datasets, takstsystem)
                if cap is None:
                    # Never observed in 113 archives, but is_usable() permits a
                    # 0-row LEVERANCEINFO. Fall back to the packaging date so
                    # the delivery can still be ordered — marked as derived,
                    # because for an M file it overstates the real cap.
                    cap = archive.delivery_date.isoformat()
                    cap_kilde = CAP_DERIVED
                    context.log.warning(
                        f"{archive.filename}: LEVERANCEINFO har ingen "
                        f"LPROPDATERINGSDATO — bruger filnavnets dato {cap} som "
                        f"afledt cap. Undersoeg leverancen."
                    )
                else:
                    cap_kilde = CAP_REAL
            write_manifest(conn, leverancetype, archive, cap, cap_kilde)
            manifest_rows.append(
                {
                    "takstsystem": takstsystem,
                    "fil": archive.filename,
                    "kadence": archive.kadence or "-",
                    "leverance_ts": archive.timestamp.isoformat(timespec="seconds"),
                    "cap": cap or "-",
                    "cap_kilde": cap_kilde or "-",
                }
            )

        totals = {d.ext_table: 0 for d in datasets}
        by_prefix = {d.source_prefix: d.ext_table for d in datasets}
        for entry in per_takst:
            for filename, count in entry["raekker"].items():
                totals[by_prefix[cast(str, _dataset_prefix(filename))]] += count
        rows.update(totals)

    return dg.MaterializeResult(
        metadata={
            "leverancetype": leverancetype,
            "takstsystemer": ", ".join(sorted(selected)),
            "leverancer": per_takst,
            "raekker_pr_ext_tabel": dict(sorted(totals.items())),
            "leverance_alder_dage": leverance_alder or "ikke vurderet her",
            "kadence": config.kadence,
            "allerede_indlaest": already_loaded or ["ingen"],
            "manifest": manifest_rows,
            "fallbacks": notes or ["ingen"],
        }
    )


def _build_asset(leverancetype: str, datasets: tuple[DrgDataset, ...], description: str):
    @dg.asset(
        name=f"ext_{leverancetype.lower()}",
        group_name="drg_extract",
        compute_kind="csv",
        tags={"domain": "drg", "layer": "extract", "source": "drg"},
        description=description,
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_staging: MSSQLResource,
        drg_share: FileShareResource,
        config: DrgExtractConfig,
    ) -> dg.MaterializeResult:
        return _run_extract(
            context, mssql_target_staging, drg_share, datasets, leverancetype, config
        )

    return _asset


ext_drg = _build_asset(
    "DRG",
    SOMATIK_DATASETS,
    "Load the newest usable somatik delivery per takstsystem (DRG T24-T26) "
    "1:1 into ext.drg_* — streamed straight from the tar.bz2, validated "
    "against SDS' own export log.",
)

ext_drgpsyk = _build_asset(
    "DRGPSYK",
    PSYK_DATASETS,
    "Load the newest usable psykiatri delivery per takstsystem (DRGPSYK "
    "T24-T26) 1:1 into ext.drgpsyk_* — streamed straight from the tar.bz2, "
    "validated against SDS' own export log.",
)


@dg.asset(
    name="drg_leverance_freshness",
    group_name="drg_freshness",
    compute_kind="python",
    tags={"domain": "drg", "layer": "extract", "source": "drg"},
    description=(
        "Alarm when a DRG delivery series on the share has gone stale. Checks "
        "DRG/M, DRG/U and DRGPSYK/M across the two highest takstsystemer per "
        "leverancetype. Filenames only — deliberately independent of what any "
        "ingest run chose to load, so it reports whether or not anything loads."
    ),
)
def drg_leverance_freshness(
    context: dg.AssetExecutionContext, drg_share: FileShareResource
) -> dg.MaterializeResult:
    root = drg_share.root()
    archives = discover_archives(root, "DRG") + discover_archives(root, "DRGPSYK")
    problems, ages = evaluate_freshness(archives, date.today())
    for problem in problems:
        context.log.error(problem)
    if problems:
        raise ValueError(
            "Foraeldede DRG-leverancer paa sharet: "
            + " | ".join(problems)
            + ". Er SMB-sharet mountet, og leverer SDS stadig?"
        )
    return dg.MaterializeResult(
        metadata={"alder_dage": ages or "ingen serier fundet", "filer_paa_share": len(archives)}
    )


drg_extract_assets = [ext_drg, ext_drgpsyk]
drg_freshness_assets = [drg_leverance_freshness]

__all__ = [
    "drg_delivery_unit",
    "drg_extract_assets",
    "drg_freshness_assets",
    "drg_leverance_freshness",
    "ext_drg",
    "ext_drgpsyk",
]

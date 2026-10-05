# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from DSI.connectors.mssql.models import RowView

# ---------------------------------------------------------------------------
# Builder metadata – allows the loader to compute NVARCHAR widths from schema
# ---------------------------------------------------------------------------

# Type alias for the source-column length lookup passed to estimators.
# Keys are **lower-cased** column names; values are CHARACTER_MAXIMUM_LENGTH
# from INFORMATION_SCHEMA (``-1`` means MAX, ``None`` means unknown).
SourceLengthLookup = dict[str, int | None]

# Sentinel returned by estimators when the result should be NVARCHAR(MAX).
MAX_LENGTH: int = -1


@dataclass(frozen=True)
class CleanCodeBuilderMeta:
    """Metadata attached to a builder function for dynamic width estimation.

    Attributes:
        source_columns: The source columns the builder reads.  Used for
            diagnostics and for the estimator to look up column lengths.
        estimate_max_len: Given a *source_length_lookup* (lower-cased column
            name -> CHARACTER_MAXIMUM_LENGTH), return the maximum possible
            output length or ``MAX_LENGTH`` for NVARCHAR(MAX).  Return
            ``None`` when the estimate cannot be determined.
    """

    source_columns: Sequence[str]
    estimate_max_len: Callable[[SourceLengthLookup], int | None]


def attach_builder_meta(
    builder: Callable[[RowView], str | None],
    meta: CleanCodeBuilderMeta,
) -> Callable[[RowView], str | None]:
    """Attach a :class:`CleanCodeBuilderMeta` to a builder function."""
    builder._clean_code_meta = meta  # type: ignore[attr-defined]
    return builder


def get_builder_meta(
    builder: Callable[[RowView], str | None],
) -> CleanCodeBuilderMeta | None:
    """Retrieve the :class:`CleanCodeBuilderMeta` from a builder, if present."""
    return getattr(builder, "_clean_code_meta", None)


# ---------------------------------------------------------------------------
# Estimator helpers
# ---------------------------------------------------------------------------


def _lookup_max(lookup: SourceLengthLookup, *columns: str) -> int | None:
    """Return the maximum CHARACTER_MAXIMUM_LENGTH across *columns*.

    Returns ``MAX_LENGTH`` if any column is MAX (``-1``), ``None`` if none of
    the columns are present in the lookup.
    """
    lengths: list[int] = []
    for col in columns:
        length = lookup.get(col.lower())
        if length is None:
            continue
        if length == MAX_LENGTH or length < 0:
            return MAX_LENGTH
        lengths.append(length)
    return max(lengths) if lengths else None


def _simple_estimator(prefix: str, column: str) -> Callable[[SourceLengthLookup], int | None]:
    """Estimator for ``PREFIX_{value}`` builders."""
    # Output: len(prefix) + 1 (underscore) + source_column_length
    prefix_overhead = len(prefix) + 1 if prefix else 0

    def estimator(lookup: SourceLengthLookup) -> int | None:
        col_len = _lookup_max(lookup, column)
        if col_len is None:
            return None
        if col_len == MAX_LENGTH:
            return MAX_LENGTH
        return prefix_overhead + col_len

    return estimator


def _cvr_estimator(*columns: str) -> Callable[[SourceLengthLookup], int | None]:
    """Estimator for ``CVR_{digits}`` builders.

    cvr numbers in Denmark are at most 8 digits, but the source column may
    contain longer strings.  The output is bounded by ``CVR_`` (4 chars) +
    the longest source column length.
    """
    cvr_prefix_len = 4  # "CVR_"

    def estimator(lookup: SourceLengthLookup) -> int | None:
        col_len = _lookup_max(lookup, *columns)
        if col_len is None:
            return None
        if col_len == MAX_LENGTH:
            return MAX_LENGTH
        return cvr_prefix_len + col_len

    return estimator


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


def normalize_value(value: Any) -> str | None:
    """Apply standard normalization: trim, remove internal spaces, upper-case."""
    if value is None:
        return None
    if isinstance(value, datetime):
        text = value.replace(microsecond=0).strftime("%Y%m%d%H%M%S")
    elif isinstance(value, date):
        text = value.strftime("%Y%m%d")
    elif isinstance(value, float):
        text = str(int(value)).strip() if value == int(value) else str(value).strip()
    else:
        text = str(value).strip()
    if not text:
        return None
    text = re.sub(r"\s+", "", text)
    return text.upper()


# ---------------------------------------------------------------------------
# Shared composition helpers
# ---------------------------------------------------------------------------


def build_clean_code(prefix: str, *parts: Any) -> str | None:
    normalized_parts = [
        normalized for normalized in (normalize_value(part) for part in parts) if normalized
    ]
    if not normalized_parts:
        return None
    combined = "_".join(normalized_parts)
    return f"{prefix}_{combined}" if prefix else combined


# ---------------------------------------------------------------------------
# Builder factories
# ---------------------------------------------------------------------------


def _make_simple_builder(prefix: str, column: str) -> Callable[[RowView], str | None]:
    """Factory for PREFIX_{normalized_value} clean codes."""

    def builder(row: RowView) -> str | None:
        value = normalize_value(row.get(column))
        if not value:
            return None
        return f"{prefix}_{value}" if prefix else value

    attach_builder_meta(
        builder,
        CleanCodeBuilderMeta(
            source_columns=(column,),
            estimate_max_len=_simple_estimator(prefix, column),
        ),
    )
    return builder


def _make_cvr_builder(
    *columns: str,
    strip_k_prefix: bool = False,
) -> Callable[[RowView], str | None]:
    """Factory for CVR_{digits} clean codes from one or more columns."""

    def builder(row: RowView) -> str | None:
        for column_name in columns:
            raw_value = row.get(column_name)
            if raw_value is None:
                continue
            text = str(raw_value).strip()
            if not text:
                continue
            if strip_k_prefix and text.lower().startswith("k"):
                text = text[1:].strip()
            digits = re.sub(r"[^0-9]", "", text)
            if digits:
                return f"CVR_{digits}"
        return None

    attach_builder_meta(
        builder,
        CleanCodeBuilderMeta(
            source_columns=tuple(columns),
            estimate_max_len=_cvr_estimator(*columns),
        ),
    )
    return builder


# ---------------------------------------------------------------------------
# cvr candidates
# ---------------------------------------------------------------------------

CVR_CANDIDATES = (
    "cvrnr",
    "CVRnr",
    "SE_CVRnr",
    "leverandoer_cvr",
    "LeverandoerCVR",
    "leverandorCVR",
    "cvr_nr",
    "cvr",
    "LeverandorCVRNummer",
    "faktura_udsteder_cvr_nummer",
    "juridisk_enhed",
)


def resolve_cvr_identifier(row: RowView) -> str | None:
    for key in CVR_CANDIDATES:
        candidate = normalize_value(row.get(key))
        if candidate:
            return candidate
    return None


# ---------------------------------------------------------------------------
# Concrete builders (with metadata)
# ---------------------------------------------------------------------------


def org_clean_code(row: RowView) -> str | None:
    return build_clean_code("ean", row.get("regnings_modtager_id", ""))


attach_builder_meta(
    org_clean_code,
    CleanCodeBuilderMeta(
        source_columns=("regnings_modtager_id",),
        estimate_max_len=_simple_estimator("ean", "regnings_modtager_id"),
    ),
)


def kontoplan_clean_code(row: RowView) -> str | None:
    return build_clean_code("KONTOPLAN", row.get("finanskonto_id", "FINANSKONTOID"))


attach_builder_meta(
    kontoplan_clean_code,
    CleanCodeBuilderMeta(
        source_columns=("finanskonto_id",),
        estimate_max_len=_simple_estimator("KONTOPLAN", "finanskonto_id"),
    ),
)


def afdelingsnummer_clean_code(row: RowView) -> str | None:
    return build_clean_code("AFDELINGSNUMMER", row.get("Afdelingsnummer", "AFDELINGSNUMMER"))


attach_builder_meta(
    afdelingsnummer_clean_code,
    CleanCodeBuilderMeta(
        source_columns=("Afdelingsnummer",),
        estimate_max_len=_simple_estimator("AFDELINGSNUMMER", "Afdelingsnummer"),
    ),
)


def cvr_clean_code(row: RowView) -> str | None:
    return _make_cvr_builder(*CVR_CANDIDATES)(row)


attach_builder_meta(
    cvr_clean_code,
    CleanCodeBuilderMeta(
        source_columns=tuple(CVR_CANDIDATES),
        estimate_max_len=_cvr_estimator(*CVR_CANDIDATES),
    ),
)


lev_ils_clean_Code = _make_cvr_builder("se_nr")


def ils_leverandoer_varenummer_clean_code(row: RowView) -> str | None:
    return build_clean_code("LEVVARE", row.get("lvnr_1"))


attach_builder_meta(
    ils_leverandoer_varenummer_clean_code,
    CleanCodeBuilderMeta(
        source_columns=("lvnr_1",),
        estimate_max_len=_simple_estimator("LEVVARE", "lvnr_1"),
    ),
)


vare_clean_code_ils = _make_simple_builder("VARE", "vnr")

ilsvare_clean_code_ils = _make_simple_builder("ILSVARE", "vnr")


def lgrvare_leverandoer_varenummer_clean_code(row: RowView) -> str | None:
    """Generate Leverandoer_Varenummer_CleanCode for lgrvare using lvnr."""
    return build_clean_code("LEVVARE", row.get("lvnr"))


attach_builder_meta(
    lgrvare_leverandoer_varenummer_clean_code,
    CleanCodeBuilderMeta(
        source_columns=("lvnr",),
        estimate_max_len=_simple_estimator("LEVVARE", "lvnr"),
    ),
)


def pmdm_leverandoer_varenummer_clean_code(row: RowView) -> str | None:
    """Generate Leverandoer_Varenummer_CleanCode for pmdm using lvnr."""
    return build_clean_code("LEVVARE", row.get("lvnr"))


attach_builder_meta(
    pmdm_leverandoer_varenummer_clean_code,
    CleanCodeBuilderMeta(
        source_columns=("lvnr",),
        estimate_max_len=_simple_estimator("LEVVARE", "lvnr"),
    ),
)


def indkfak_leverandoer_varenummer_clean_code(row: RowView) -> str | None:
    """Generate Leverandoer_Varenummer_CleanCode for indkfak using levvarenr."""
    return build_clean_code("LEVVARE", row.get("levvarenr"))


attach_builder_meta(
    indkfak_leverandoer_varenummer_clean_code,
    CleanCodeBuilderMeta(
        source_columns=("levvarenr",),
        estimate_max_len=_simple_estimator("LEVVARE", "levvarenr"),
    ),
)


def indkfak_vare_clean_code(row: RowView) -> str | None:
    """Generate clean_code_vare_raw for indkfak, filtering out EjOk entries."""
    vnr = row.get("vnr")
    if vnr and "EjOk" in str(vnr):
        return None
    return build_clean_code("VARE", vnr)


attach_builder_meta(
    indkfak_vare_clean_code,
    CleanCodeBuilderMeta(
        source_columns=("vnr",),
        estimate_max_len=_simple_estimator("VARE", "vnr"),
    ),
)


def vareaftale_clean_code(row: RowView) -> str | None:
    aftale = normalize_value(row.get("aftale"))
    vnr = normalize_value(row.get("vnr"))
    if aftale and vnr:
        return f"AFTALE_{aftale}_VARE_{vnr}"
    return None


def _vareaftale_estimator(lookup: SourceLengthLookup) -> int | None:
    """Estimate ``AFTALE_{aftale}_VARE_{vnr}``."""
    aftale_len = _lookup_max(lookup, "aftale")
    vnr_len = _lookup_max(lookup, "vnr")
    if aftale_len is None or vnr_len is None:
        return None
    if aftale_len == MAX_LENGTH or vnr_len == MAX_LENGTH:
        return MAX_LENGTH
    # "AFTALE_" (7) + aftale + "_VARE_" (6) + vnr
    return 7 + aftale_len + 6 + vnr_len


attach_builder_meta(
    vareaftale_clean_code,
    CleanCodeBuilderMeta(
        source_columns=("aftale", "vnr"),
        estimate_max_len=_vareaftale_estimator,
    ),
)


laftd_cvr_clean_code = _make_cvr_builder("lnr", strip_k_prefix=True)
lafth_cvr_clean_code = laftd_cvr_clean_code


def laftd_vare_clean_code(row: RowView) -> str | None:
    vare_number = normalize_value(row.get("vnr"))
    if vare_number:
        return f"VARE_{vare_number}"
    for candidate in ("lvnr_1", "lvnr_2", "lvnr_3", "lvnr2"):
        vare_number = normalize_value(row.get(candidate))
        if vare_number:
            return f"VARE_{vare_number}"
    return None


_LAFTD_VARE_CANDIDATES = ("vnr", "lvnr_1", "lvnr_2", "lvnr_3", "lvnr2")


def _laftd_vare_estimator(lookup: SourceLengthLookup) -> int | None:
    """Estimate ``VARE_{value}`` where value is the max of all candidates."""
    col_len = _lookup_max(lookup, *_LAFTD_VARE_CANDIDATES)
    if col_len is None:
        return None
    if col_len == MAX_LENGTH:
        return MAX_LENGTH
    return 5 + col_len  # "VARE_" (5) + value


attach_builder_meta(
    laftd_vare_clean_code,
    CleanCodeBuilderMeta(
        source_columns=_LAFTD_VARE_CANDIDATES,
        estimate_max_len=_laftd_vare_estimator,
    ),
)


def udlev_lager_clean_code(row: RowView) -> str | None:
    """Composite Lager clean_code from sgh and lgr.

    Format examples: SGH_4200, SGH_4200_LGR_0100, LGR_0100.
    lgr is left-padded with zeros to 4 digits.
    """
    sgh = normalize_value(row.get("sgh"))
    lgr_raw = normalize_value(row.get("lgr"))

    lgr = lgr_raw.zfill(4) if lgr_raw else None

    parts = []
    if sgh:
        parts.append(f"SGH_{sgh}")
    if lgr:
        parts.append(f"LGR_{lgr}")

    if not parts:
        return None
    return "_".join(parts)


def _udlev_lager_estimator(lookup: SourceLengthLookup) -> int | None:
    """Estimate max length for ``SGH_{sgh}_LGR_{lgr}``."""
    sgh_len = _lookup_max(lookup, "sgh")
    lgr_len = _lookup_max(lookup, "lgr")
    if sgh_len is None and lgr_len is None:
        return None
    if (sgh_len is not None and sgh_len == MAX_LENGTH) or (
        lgr_len is not None and lgr_len == MAX_LENGTH
    ):
        return MAX_LENGTH
    # Worst case: "SGH_" (4) + sgh + "_" (1) + "LGR_" (4) + max(lgr, 4)
    sgh_part = (4 + sgh_len) if sgh_len is not None else 0
    lgr_part = (4 + max(lgr_len, 4)) if lgr_len is not None else 0
    separator = 1 if sgh_part and lgr_part else 0
    return sgh_part + separator + lgr_part


attach_builder_meta(
    udlev_lager_clean_code,
    CleanCodeBuilderMeta(
        source_columns=("sgh", "lgr"),
        estimate_max_len=_udlev_lager_estimator,
    ),
)


def udlev_rekvirent_clean_code(row: RowView) -> str | None:
    """Composite Rekvirent clean_code from sgh and rnr.

    Format examples: SGH_4200, SGH_4200_RNR_0100, RNR_0100.
    rnr is left-padded with zeros to 4 digits.
    """
    sgh = normalize_value(row.get("sgh"))
    rnr_raw = normalize_value(row.get("rnr"))

    rnr = rnr_raw.zfill(4) if rnr_raw else None

    parts = []
    if sgh:
        parts.append(f"SGH_{sgh}")
    if rnr:
        parts.append(f"RNR_{rnr}")

    if not parts:
        return None
    return "_".join(parts)


def _udlev_rekvirent_estimator(lookup: SourceLengthLookup) -> int | None:
    """Estimate max length for ``SGH_{sgh}_RNR_{rnr}``."""
    sgh_len = _lookup_max(lookup, "sgh")
    rnr_len = _lookup_max(lookup, "rnr")
    if sgh_len is None and rnr_len is None:
        return None
    if (sgh_len is not None and sgh_len == MAX_LENGTH) or (
        rnr_len is not None and rnr_len == MAX_LENGTH
    ):
        return MAX_LENGTH
    # Worst case: "SGH_" (4) + sgh + "_" (1) + "RNR_" (4) + max(rnr, 4)
    sgh_part = (4 + sgh_len) if sgh_len is not None else 0
    rnr_part = (4 + max(rnr_len, 4)) if rnr_len is not None else 0
    separator = 1 if sgh_part and rnr_part else 0
    return sgh_part + separator + rnr_part


attach_builder_meta(
    udlev_rekvirent_clean_code,
    CleanCodeBuilderMeta(
        source_columns=("sgh", "rnr"),
        estimate_max_len=_udlev_rekvirent_estimator,
    ),
)
# Backward-compatible clean codes kept in extract output.
# Fact lookups now resolve Organization via Lager and Vare via cvr/LEVVARE(+vnr fallback).
udlev_vare_clean_code = _make_simple_builder("VARE", "vnr")
udlev_org_clean_code = _make_simple_builder("AFDELING", "rnr")


# Clean code for vareudlevering: "UDLEV_{prrowid}"
def vareudlevering_clean_code(row: RowView) -> str | None:
    return build_clean_code("UDLEV", row.get("prrowid"))


def _vareudlevering_estimator(lookup: SourceLengthLookup) -> int | None:
    """Estimate ``UDLEV_{prrowid}``."""
    prrowid_len = _lookup_max(lookup, "prrowid")
    if prrowid_len is None:
        return None
    if prrowid_len == MAX_LENGTH:
        return MAX_LENGTH
    # "UDLEV_" (6) + prrowid
    return 6 + prrowid_len


attach_builder_meta(
    vareudlevering_clean_code,
    CleanCodeBuilderMeta(
        source_columns=("prrowid",),
        estimate_max_len=_vareudlevering_estimator,
    ),
)


mysupply_metadata_cvr_clean_code = _make_cvr_builder("leverandoer_cvr")
mysupply_header_cvr_clean_code = _make_cvr_builder("faktura_udsteder_cvr_nummer")
mysupply_line_vare_clean_code = _make_simple_builder("LEVVARE", "item_faktura_udsteder_vare_nr")
faktura_mysupply_clean_code = _make_simple_builder("FAKTURA", "faktura_nr")
faktura_sydax_clean_code = _make_simple_builder("FAKTURA", "Fakturanummer")


def fakturalinje_id_mysupply_clean_code(row: RowView) -> str | None:
    fakturalinje_id = row.get("id")
    if fakturalinje_id is None:
        return None
    return str(fakturalinje_id)


def _id_estimator(lookup: SourceLengthLookup) -> int | None:
    """Estimate for raw ``id`` passthrough (no prefix)."""
    return _lookup_max(lookup, "id")


attach_builder_meta(
    fakturalinje_id_mysupply_clean_code,
    CleanCodeBuilderMeta(
        source_columns=("id",),
        estimate_max_len=_id_estimator,
    ),
)


def postering_clean_code(row: RowView) -> str | None:
    kildesystem = normalize_value(row.get("Kildesystem"))
    finanspostering_id = normalize_value(row.get("FinansposteringId"))
    if kildesystem and finanspostering_id:
        return f"{kildesystem}_{finanspostering_id}"
    if finanspostering_id:
        return finanspostering_id
    return None


def _postering_estimator(lookup: SourceLengthLookup) -> int | None:
    """Estimate ``kildesystem_finanspostering_id`` (worst case)."""
    ks_len = _lookup_max(lookup, "Kildesystem")
    fp_len = _lookup_max(lookup, "FinansposteringId")
    if fp_len is None:
        return None
    if fp_len == MAX_LENGTH:
        return MAX_LENGTH
    if ks_len is not None:
        if ks_len == MAX_LENGTH:
            return MAX_LENGTH
        # Worst case: "kildesystem_finanspostering_id"
        return ks_len + 1 + fp_len
    # Only finanspostering_id
    return fp_len


attach_builder_meta(
    postering_clean_code,
    CleanCodeBuilderMeta(
        source_columns=("Kildesystem", "FinansposteringId"),
        estimate_max_len=_postering_estimator,
    ),
)


# ---------------------------------------------------------------------------
# Lagerbeholdning (lgrbeh + afdlgrbeh)
# ---------------------------------------------------------------------------

lgrbeh_lager_clean_code = udlev_lager_clean_code  # Same sgh+lgr pattern

lgrbeh_rekvirent_clean_code = udlev_rekvirent_clean_code  # Same sgh+rnr pattern

lgrbeh_vare_clean_code = _make_simple_builder("VARE", "vnr")

lgrbeh_org_clean_code = udlev_org_clean_code  # Same AFDELING+rnr pattern

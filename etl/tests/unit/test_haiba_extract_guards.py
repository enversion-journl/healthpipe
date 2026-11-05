# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Guards on the HAIBA extract: invariant 1 and arc-width rejection.

The failures these pin down are the quiet destructive ones: a HAI type whose
deliveries are all header-only slipping through and soft-deleting its archive
history behind a green run; an over-wide value (an 11-digit "CPR") aborting the whole archive
MERGE with error 8152 that names neither column nor row; and the width map
drifting from the arc DDL. GDPR: the width guard must never surface values —
only (column, HAI type) counts.
"""

import re
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from core.resources import FileShareResource
from DSI.csv_files.haiba import (
    AGG_INFEKTION,
    AGG_ORTOPAEDI,
    CASES_INFEKTION,
    CASES_SSI,
    HaibaExtractConfig,
    HaibaFile,
    _enforce_ext_is_loadable,
    _extract_schema,
    _rows_within_arc_widths,
    select_newest,
    unusable_types_for_schema,
)

pytestmark = pytest.mark.unit

_DDL_DIR = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl" / "sources" / "haiba"

_AGG_HEADER = (
    "HAI;HAIBA_dato;Rolle_ansvar;ejerforhold;RegionKode;region;Sundhedsinstitution;"
    "Matrikel;OverliggendeEnhed;AfdelingAfsnit;SORIdentifier;antal;risikodoegn"
)
_ORTO_HEADER = _AGG_HEADER.replace(";antal;risikodoegn", ";antal90;antal365;operationer")


def _write(path: Path, header: str, rows: list[str]) -> None:
    path.write_text("\r\n".join([header, *rows]) + "\r\n", encoding="cp1252")


def _file(path: Path, hai_type: str, kind: str, delivery: str) -> HaibaFile:
    return HaibaFile(str(path), hai_type, kind, date.fromisoformat(delivery))


# --- DDL parity -------------------------------------------------------------------


def test_arc_widths_match_the_archive_ddl():
    """Every NVARCHAR width in the archive DDL must equal schema.arc_width, and
    every text column in the schema must have a width — a drift on either side
    silently re-opens the 8152 abort the guard exists to prevent."""
    ddl = "\n".join(p.read_text(encoding="utf-8") for p in sorted(_DDL_DIR.glob("*.sql")))
    blocks = re.split(r"CREATE TABLE \[arc\]\.\[(\w+)\]", ddl)
    ddl_widths: dict[str, dict[str, int]] = {}
    for table, body in zip(blocks[1::2], blocks[2::2], strict=True):
        ddl_widths[table] = {
            column: int(width)
            for column, width in re.findall(r"\[(\w+)\]\s+NVARCHAR\((\d+)\)", body)
        }

    for schema in (AGG_INFEKTION, AGG_ORTOPAEDI, CASES_INFEKTION, CASES_SSI):
        declared = ddl_widths[schema.ext_table]
        for column, width in declared.items():
            assert schema.arc_width(column) == width, f"{schema.ext_table}.{column}"
        for column in schema.all_columns:
            if column == "leverance_dato":
                continue
            assert schema.arc_width(column) is not None, f"{schema.ext_table}.{column} unmapped"
            assert column in declared, f"{schema.ext_table}.{column} not NVARCHAR in DDL"


# --- invariant 1: unusable types --------------------------------------------------


def test_header_only_type_fails_the_owning_asset_only(tmp_path):
    """BACT (infektion header, all files header-only) must block AGG_INFEKTION
    but not AGG_ORTOPAEDI — the two schemas share kind='agg', so ownership is
    decided by the newest candidate's header."""
    a1 = tmp_path / "HAIBA_BACT_agg_RegS_2026-08-15.csv"
    a2 = tmp_path / "HAIBA_BACT_agg_RegS_2026-08-08.csv"
    _write(a1, _AGG_HEADER, [])
    _write(a2, _AGG_HEADER, [])
    usable = tmp_path / "HAIBA_Acute_HIP_agg_RegS_2026-08-15.csv"
    _write(usable, _ORTO_HEADER, ["Acute_HIP;2024-05-01;Kontaktansvar;;1083;;;;;;;1;2;3"])

    files = [
        _file(a1, "BACT", "agg", "2026-08-15"),
        _file(a2, "BACT", "agg", "2026-08-08"),
        _file(usable, "Acute_HIP", "agg", "2026-08-15"),
    ]
    _, notes, unusable = select_newest(files, "agg", "cp1252")
    assert any("no usable file" in n for n in notes)

    assert unusable_types_for_schema(files, unusable, AGG_INFEKTION, "cp1252") == ["BACT"]
    assert unusable_types_for_schema(files, unusable, AGG_ORTOPAEDI, "cp1252") == []


def test_retired_type_produces_no_note_and_passes(tmp_path):
    """A type with NO files at all is a genuine retirement: no note, no block —
    its soft-delete is real and must flow."""
    usable = tmp_path / "HAIBA_UTI_agg_RegS_2026-08-15.csv"
    _write(usable, _AGG_HEADER, ["UTI;2024-05-01;Kontaktansvar;;1083;;;;;;;1;2.5"])
    files = [_file(usable, "UTI", "agg", "2026-08-15")]
    _, notes, unusable = select_newest(files, "agg", "cp1252")
    assert notes == []
    assert unusable == []
    assert unusable_types_for_schema(files, unusable, AGG_INFEKTION, "cp1252") == []


def test_extract_fails_on_blocked_type_and_allows_the_override(tmp_path, monkeypatch):
    agg_dir = tmp_path / "HAIBAagg"
    agg_dir.mkdir()
    _write(agg_dir / "HAIBA_BACT_agg_RegS_2026-08-15.csv", _AGG_HEADER, [])
    _write(
        agg_dir / "HAIBA_UTI_agg_RegS_2026-08-15.csv",
        _AGG_HEADER,
        ["UTI;2024-05-01;Kontaktansvar;;1083;;;;;;;1;2.5"],
    )

    import DSI.csv_files.haiba as module

    monkeypatch.setattr(
        module, "_write_to_ext_table", lambda conn, schema, rows, ctx: sum(1 for _ in rows)
    )
    monkeypatch.setattr(module, "_arc_active_counts", lambda conn, schema: {})
    monkeypatch.setattr(module, "_read_checkpoints", lambda *a, **k: {})
    ctx = MagicMock()
    share = FileShareResource(base_dir=str(tmp_path))

    with pytest.raises(ValueError, match="BACT"):
        _extract_schema(ctx, MagicMock(), share, AGG_INFEKTION, HaibaExtractConfig())

    result = _extract_schema(
        ctx,
        MagicMock(),
        share,
        AGG_INFEKTION,
        HaibaExtractConfig(allow_missing_hai_types=["BACT"]),
    )
    assert result.metadata["row_count"].value == 1


def test_width_rejection_on_a_key_column_fails_the_load():
    """A too-wide value in a MERGE-key column drops a whole case, not an
    over-long free-text field, so the archive would soft-delete it."""
    with pytest.raises(ValueError, match="merge-key column"):
        _enforce_ext_is_loadable(
            CASES_SSI,
            {"SSI_HIP": 5},
            {"SSI_HIP": 5},
            {("cprnr", "SSI_HIP"): 1},
            [],
            MagicMock(),
        )


def test_width_rejection_on_a_free_text_column_does_not_fail():
    """A non-key over-wide value stays a counted warning — it is truncated
    context, not an identity, and the row still carries its key."""
    _enforce_ext_is_loadable(
        CASES_SSI,
        {"SSI_HIP": 5},
        {"SSI_HIP": 5},
        {("HAI_producent", "SSI_HIP"): 1},
        [],
        MagicMock(),
    )


def test_a_file_emptied_by_width_rejection_fails_the_load():
    """has_data_row guaranteed a data row, so 0 survivors means every row was
    width-rejected — the type would vanish and read as a retirement."""
    with pytest.raises(ValueError, match="0 rows"):
        _enforce_ext_is_loadable(
            AGG_INFEKTION,
            {},
            {"BACT": 100},
            {("region", "BACT"): 3},
            ["BACT (f.csv)"],
            MagicMock(),
        )


def test_vocabulary_drift_warns_but_does_not_fail():
    """A delivered type absent from a non-empty archive is a new type or a
    filename/column mismatch: visible as a warning, not fatal."""
    ctx = MagicMock()
    _enforce_ext_is_loadable(AGG_INFEKTION, {"COHA": 100}, {"BACT": 100}, {}, [], ctx)
    assert ctx.log.warning.called


# --- arc-width rejection -------------------------------------------------------------


def _ssi_row(**overrides) -> dict[str, str | None]:
    row = {column: None for column in CASES_SSI.all_columns}
    row.update(
        {
            "HAI_type": "SSI_HIP",
            "cprnr": "0101801111",
            "operation_date": "2024-02-10 00:00:00",
            "leverance_dato": "2026-08-22",
        }
    )
    row.update(overrides)
    return row


def test_too_wide_cpr_and_sor_rows_are_rejected_and_counted():
    rejections: dict[tuple[str, str], int] = {}
    rows = [
        _ssi_row(),
        _ssi_row(cprnr="01018011112"),  # 11 chars > NVARCHAR(10)
        _ssi_row(HAI_producent="1" * 31),  # 31 chars > NVARCHAR(30)
    ]
    kept = list(_rows_within_arc_widths(CASES_SSI, "SSI_HIP", iter(rows), rejections))
    assert len(kept) == 1
    assert rejections == {("cprnr", "SSI_HIP"): 1, ("HAI_producent", "SSI_HIP"): 1}


def test_rejection_counts_never_carry_values():
    """GDPR: the malformed value (which may be CPR-like) must not appear in the
    rejection structure that reaches metadata and logs."""
    rejections: dict[tuple[str, str], int] = {}
    secret = "01018011112"
    list(_rows_within_arc_widths(CASES_SSI, "SSI_HIP", iter([_ssi_row(cprnr=secret)]), rejections))
    assert secret not in repr(rejections)


def test_boundary_width_passes():
    rejections: dict[tuple[str, str], int] = {}
    kept = list(
        _rows_within_arc_widths(CASES_SSI, "SSI_HIP", iter([_ssi_row(cprnr="0" * 10)]), rejections)
    )
    assert len(kept) == 1 and not rejections

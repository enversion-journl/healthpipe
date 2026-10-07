# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for HAIBA delivery extraction: filename parsing, newest-usable-file
selection (the two archive invariants), SAS date normalisation, schema
detection, and header-name column mapping."""

from datetime import date

import pytest
from DSI.csv_files import haiba
from DSI.csv_files.haiba import (
    AGG_INFEKTION,
    AGG_ORTOPAEDI,
    CASES_INFEKTION,
    CASES_SSI,
    HaibaFile,
    detect_schema,
    discover_files,
    haiba_dir,
    haiba_encoding,
    iter_rows,
    parse_filename,
    parse_sas_datetime,
    select_newest,
)

pytestmark = pytest.mark.unit

AGG_HEADER = (
    "HAI;HAIBA_dato;Rolle_ansvar;ejerforhold;RegionKode;region;"
    "Sundhedsinstitution;Matrikel;OverliggendeEnhed;AfdelingAfsnit;"
    "SORIdentifier;antal;risikodoegn"
)
ORTHO_HEADER = (
    "HAI;HAIBA_dato;Rolle_ansvar;ejerforhold;RegionKode;region;"
    "Sundhedsinstitution;Matrikel;OverliggendeEnhed;AfdelingAfsnit;"
    "SORIdentifier;antal90;antal365;operationer"
)
CASES_HEADER = (
    "HAI_type;cprnr;HAI_date;HAI_Opholdsadresse;HAI_Kontaktansvar;extid;"
    "larlabnr;HAI_RegionKode;HAI_Region;HAI_ejerforhold;HAI_Matrikel;"
    "HAI_Sundhedsinstitution;HAI_OverliggendeEnhed;HAI_AfdelingAfsnit"
)


def _write(path, header, *rows, encoding="cp1252"):
    path.write_bytes(("\r\n".join([header, *rows]) + "\r\n").encode(encoding))
    return str(path)


def _agg_row(sor="245231000016009", antal="3"):
    return (
        f"BACT;2026-04-25;Kontaktansvar;Offentlig;1083;Example Region;"
        f"Odense Universitetshospital;OUH Odense;OUH Afd;OUH Afsnit;"
        f"{sor};{antal};1234.5"
    )


# --------------------------------------------------------------- filenames ---


@pytest.mark.parametrize(
    "name,expected",
    [
        (
            "HAIBA_BACT_agg_RegS_2026-08-15.csv",
            ("BACT", "agg", date(2026, 8, 15)),
        ),
        (
            "HAIBA_BACT_cases_RegS_2026_08_08.csv",
            ("BACT", "cases", date(2026, 8, 8)),
        ),
        (
            "HAIBA_CDI_HOHA_agg_RegS_2026-08-15.csv",
            ("CDI_HOHA", "agg", date(2026, 8, 15)),
        ),
        (
            "HAIBA_SSI_HIP_cases_RegS_2025_08_12.csv",
            ("SSI_HIP", "cases", date(2025, 8, 12)),
        ),
    ],
)
def test_parse_filename_accepts_both_date_spellings(name, expected):
    assert parse_filename(name) == expected


@pytest.mark.parametrize(
    "name",
    [
        # SDS' readme carries a date in its name and sits in the same folder.
        "0_laesmig_2021-11-16.txt",
        "nyTVHtest.txt",
        "tom CPR testfil.txt",
        "HAIBA_BACT_agg_RegS_2026-08-15.csv.bak",
        "HAIBA_BACT_agg_RegH_2026-08-15.csv",
        "HAIBA_BACT_agg_RegS_2026-13-45.csv",
    ],
)
def test_parse_filename_rejects_non_deliveries(name):
    assert parse_filename(name) is None


def test_discover_files_ignores_non_delivery_files(tmp_path):
    agg = tmp_path / "HAIBAagg"
    cpr = tmp_path / "HAIBAcpr"
    agg.mkdir()
    cpr.mkdir()
    _write(agg / "HAIBA_BACT_agg_RegS_2026-08-15.csv", AGG_HEADER, _agg_row())
    (agg / "0_laesmig_2021-11-16.txt").write_text("readme")
    (cpr / "tom CPR testfil.txt").write_text("")
    _write(cpr / "HAIBA_BACT_cases_RegS_2026_08_08.csv", CASES_HEADER)

    found = discover_files(str(tmp_path))

    assert sorted((f.hai_type, f.kind) for f in found) == [
        ("BACT", "agg"),
        ("BACT", "cases"),
    ]


# ------------------------------------------------- invariants 1 and 2 --------


def test_select_newest_skips_header_only_file_and_falls_back(tmp_path):
    """Invariant 2: a 197-byte header-only delivery must not win.

    Accepting it would empty ext for that HAI type, which the archive reads as
    "SDS deleted everything" and soft-deletes the whole history.
    """
    _write(tmp_path / "HAIBA_UTI_cases_RegS_2026_03_21.csv", CASES_HEADER, "UTI;x;y")
    _write(tmp_path / "HAIBA_UTI_cases_RegS_2026_03_28.csv", CASES_HEADER)

    selected, notes, unusable = select_newest(discover_files(str(tmp_path)), "cases")

    assert selected["UTI"].delivery_date == date(2026, 3, 21)
    assert any("2026_03_28" in note for note in notes)
    assert unusable == []


def test_select_newest_reports_type_with_no_usable_file(tmp_path):
    _write(tmp_path / "HAIBA_UTI_cases_RegS_2026_03_28.csv", CASES_HEADER)

    selected, notes, unusable = select_newest(discover_files(str(tmp_path)), "cases")

    assert "UTI" not in selected
    assert any("no usable file" in note for note in notes)
    # The structured value, not the log string, is what invariant 1's block reads.
    assert unusable == ["UTI"]


def test_select_newest_keeps_type_absent_from_latest_delivery(tmp_path):
    """Invariant 1: every known type is selected at its OWN newest delivery.

    HAIBAagg has no 2026-03-28 files at all and UTI was absent for five weeks.
    Selecting per delivery date rather than per type would drop those types
    from ext and soft-delete them from the archive.
    """
    _write(tmp_path / "HAIBA_BACT_agg_RegS_2026-04-25.csv", AGG_HEADER, _agg_row())
    _write(tmp_path / "HAIBA_UTI_agg_RegS_2026-01-10.csv", AGG_HEADER, _agg_row())

    selected, _, _ = select_newest(discover_files(str(tmp_path)), "agg")

    assert set(selected) == {"BACT", "UTI"}
    assert selected["UTI"].delivery_date == date(2026, 1, 10)


def test_select_newest_prefers_latest_delivery(tmp_path):
    for day in ("2026-04-11", "2026-04-25", "2026-04-18"):
        _write(tmp_path / f"HAIBA_BACT_agg_RegS_{day}.csv", AGG_HEADER, _agg_row())

    selected, notes, _ = select_newest(discover_files(str(tmp_path)), "agg")

    assert selected["BACT"].delivery_date == date(2026, 4, 25)
    assert notes == []


# --------------------------------------------------------------- SAS dates ---


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("10FEB2023:13:00:00.000", "2023-02-10 13:00:00"),
        ("25APR2024:09:58:00.000", "2024-04-25 09:58:00"),
        ("04JUL2024:13:27:00", "2024-07-04 13:27:00"),
        # Already ISO, or unparseable: pass through so the column keeps
        # working if SDS switches format.
        ("2025-01-22 17:00:00", "2025-01-22 17:00:00"),
        ("", ""),
        ("not a date", "not a date"),
        ("10XXX2023:13:00:00.000", "10XXX2023:13:00:00.000"),
        ("32FEB2023:13:00:00.000", "32FEB2023:13:00:00.000"),
    ],
)
def test_parse_sas_datetime(raw, expected):
    assert parse_sas_datetime(raw) == expected


# ---------------------------------------------------------- schema routing ---


def test_detect_schema_separates_infection_from_orthopedic():
    assert detect_schema("agg", AGG_HEADER.split(";")) is AGG_INFEKTION
    assert detect_schema("agg", ORTHO_HEADER.split(";")) is AGG_ORTOPAEDI


def test_detect_schema_separates_cases_from_ssi():
    assert detect_schema("cases", CASES_HEADER.split(";")) is CASES_INFEKTION
    ssi_header = list(CASES_SSI.columns)
    assert detect_schema("cases", ssi_header) is CASES_SSI


def test_detect_schema_routes_unknown_hai_type_by_header():
    """A HAI type nobody has seen still routes, because the measure columns
    decide — not a hardcoded type list."""
    header = AGG_HEADER.replace("HAI;", "HAI;").split(";")
    assert detect_schema("agg", header) is AGG_INFEKTION


# --------------------------------------------------------- row extraction ---


def test_iter_rows_maps_findings_by_name_not_position(tmp_path):
    """UTI ships findings1-4 today and shipped 6 in Feb 2025; BACT ships 7.

    Positional mapping would slide UTI's findings into the wrong columns the
    moment the count changes.
    """
    header = CASES_HEADER + ";findings1;findings2;findings3;findings4"
    row = (
        "UTI;0101256073;2026-04-25 10:00:00;238071000016007;239391000016000;"
        "ext1;lab1;1083;Example Region;Offentlig;OUH;OUH SI;OUH OE;OUH AA;"
        "Escherichia coli(path);;;"
    )
    path = _write(tmp_path / "HAIBA_UTI_cases_RegS_2026_04_25.csv", header, row)
    file = HaibaFile(path, "UTI", "cases", date(2026, 4, 25))

    rows = list(iter_rows(file, CASES_INFEKTION))

    assert len(rows) == 1
    assert rows[0]["findings1"] == "Escherichia coli(path)"
    assert rows[0]["findings2"] is None
    # findings5-7 exist in the ext table but not in this file.
    assert "findings5" not in rows[0]
    assert rows[0]["leverance_dato"] == "2026-04-25"


def test_iter_rows_raises_on_unknown_column(tmp_path):
    """SDS changes schemas without notice; a silently dropped column is a KPI
    that is quietly wrong."""
    path = _write(
        tmp_path / "HAIBA_BACT_agg_RegS_2026-04-25.csv",
        AGG_HEADER + ";nyt_felt",
        _agg_row() + ";42",
    )
    file = HaibaFile(path, "BACT", "agg", date(2026, 4, 25))

    with pytest.raises(ValueError, match="nyt_felt"):
        list(iter_rows(file, AGG_INFEKTION))


def test_iter_rows_decodes_windows_1252(tmp_path):
    """SDS' readme specifies Windows-1252, and the difference from latin-1 is
    silent: byte 0x96 is an en dash in cp1252 but an unprintable control
    character in latin-1, so the wrong codec corrupts text without raising."""
    label = "Børne– Afsnit"  # ø is 0xF8 in both codecs; – is 0x96 in cp1252 only
    assert label.encode("cp1252") == b"B\xf8rne\x96 Afsnit"
    row = _agg_row().replace("OUH Afsnit", label)
    path = _write(tmp_path / "HAIBA_BACT_agg_RegS_2026-04-25.csv", AGG_HEADER, row)
    file = HaibaFile(path, "BACT", "agg", date(2026, 4, 25))

    rows = list(iter_rows(file, AGG_INFEKTION))

    assert rows[0]["AfdelingAfsnit"] == label


def test_iter_rows_keeps_aggregate_rows_without_sor(tmp_path):
    """RegionKode 7777/8888/9999 carry the region-vs-land KPI and have no
    SORIdentifier — they must survive extraction, not be filtered out."""
    national = "BACT;2026-04-25;Kontaktansvar;;9999;Hele landet;;;;;;12;9999.0"
    path = _write(
        tmp_path / "HAIBA_BACT_agg_RegS_2026-04-25.csv",
        AGG_HEADER,
        _agg_row(),
        national,
    )
    file = HaibaFile(path, "BACT", "agg", date(2026, 4, 25))

    rows = list(iter_rows(file, AGG_INFEKTION))

    assert len(rows) == 2
    assert rows[1]["RegionKode"] == "9999"
    # Merge-key columns keep '' rather than NULL: a NULL never matches itself
    # in the archive MERGE's ON clause, so the row would be re-inserted every
    # run. 0.5% of agg rows have no SORIdentifier.
    assert rows[1]["SORIdentifier"] == ""
    assert rows[1]["ejerforhold"] is None


def test_iter_rows_skips_blank_lines(tmp_path):
    path = _write(
        tmp_path / "HAIBA_BACT_agg_RegS_2026-04-25.csv",
        AGG_HEADER,
        _agg_row(),
        "",
        _agg_row(antal="7"),
    )
    file = HaibaFile(path, "BACT", "agg", date(2026, 4, 25))

    assert len(list(iter_rows(file, AGG_INFEKTION))) == 2


# ------------------------------------------------------------ ext contract ---


def test_ext_columns_cover_max_findings_across_types():
    """BACT ships findings1-7 and SSI ships 9; the ext table is dimensioned by
    the maximum, not by whichever file happens to be read first."""
    assert "findings7" in CASES_INFEKTION.all_columns
    assert "findings8" not in CASES_INFEKTION.all_columns
    assert "findings9" in CASES_SSI.all_columns
    assert CASES_INFEKTION.all_columns[-1] == haiba.DELIVERY_COLUMN


def test_iter_rows_raises_on_ragged_row(tmp_path):
    """A truncated row would silently drop its trailing columns — antal and
    risikodoegn are the last two, so the loss lands as a wrong KPI."""
    path = _write(
        tmp_path / "HAIBA_BACT_agg_RegS_2026-04-25.csv",
        AGG_HEADER,
        _agg_row(),
        "BACT;2026-04-25;Kontaktansvar",
    )
    file = HaibaFile(path, "BACT", "agg", date(2026, 4, 25))

    with pytest.raises(ValueError, match="3 fields, expected 13"):
        list(iter_rows(file, AGG_INFEKTION))


# ------------------------------------------------------- mount / environment ---


def test_haiba_dir_fails_loudly_when_unset(monkeypatch):
    """An unmounted share and "no new deliveries" look identical from the
    outside. Falling back to a plausible local path is how a pipeline stays
    green for months while reading nothing."""
    monkeypatch.delenv("HAIBA_BASE_DIR", raising=False)

    with pytest.raises(ValueError, match="HAIBA_BASE_DIR"):
        haiba_dir()


def test_haiba_dir_fails_loudly_when_blank(monkeypatch):
    monkeypatch.setenv("HAIBA_BASE_DIR", "   ")

    with pytest.raises(ValueError, match="HAIBA_BASE_DIR"):
        haiba_dir()


def test_haiba_dir_fails_when_share_is_not_mounted(monkeypatch, tmp_path):
    """The variable is injected by the Nomad job, so it can be set while the
    CIFS mount itself failed."""
    monkeypatch.setenv("HAIBA_BASE_DIR", str(tmp_path / "mnt" / "ftproot"))

    with pytest.raises(ValueError, match="not a directory"):
        haiba_dir()


def test_haiba_dir_returns_mounted_path(monkeypatch, tmp_path):
    monkeypatch.setenv("HAIBA_BASE_DIR", str(tmp_path))

    assert haiba_dir() == str(tmp_path)


def test_haiba_encoding_comes_from_the_environment(monkeypatch):
    """The same mount carries SilkeborgData in DOS-Nordic cp865; one shared
    constant would corrupt one of the two sources."""
    monkeypatch.setenv("HAIBA_FILE_ENCODING", "cp865")
    assert haiba_encoding() == "cp865"

    monkeypatch.delenv("HAIBA_FILE_ENCODING", raising=False)
    assert haiba_encoding() == "cp1252"


# ------------------------------------------------------------ pinnet load ---


def test_select_newest_honours_delivery_pin(tmp_path):
    """SDS drops a whole calendar year each January. 2020 survives only in
    deliveries up to 2025-12-27, so a backfill must be able to reach past the
    newest file."""
    for day in ("2025-12-27", "2026-01-03", "2026-08-15"):
        _write(tmp_path / f"HAIBA_BACT_agg_RegS_{day}.csv", AGG_HEADER, _agg_row())

    pinned, _, _ = select_newest(discover_files(str(tmp_path)), "agg", None, date(2025, 12, 27))
    newest, _, _ = select_newest(discover_files(str(tmp_path)), "agg")

    assert pinned["BACT"].delivery_date == date(2025, 12, 27)
    assert newest["BACT"].delivery_date == date(2026, 8, 15)


def test_select_newest_pin_still_skips_empty_files(tmp_path):
    """The pin narrows the search; it must not bypass the header-only guard."""
    _write(tmp_path / "HAIBA_BACT_agg_RegS_2025-12-20.csv", AGG_HEADER, _agg_row())
    _write(tmp_path / "HAIBA_BACT_agg_RegS_2025-12-27.csv", AGG_HEADER)

    selected, notes, _ = select_newest(
        discover_files(str(tmp_path)), "agg", None, date(2025, 12, 27)
    )

    assert selected["BACT"].delivery_date == date(2025, 12, 20)
    assert any("2025-12-27" in n for n in notes)

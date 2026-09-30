# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for DRG extract (DSI/csv_files/drg.py).

Builds synthetic tar.bz2 deliveries and checks filename parsing, select_newest
across cadences, schema validation (including T24/T25 schemas without the T26
columns), export-log control totals and the key-width guard. The database is
faked — it is the stream logic under test.
"""

import csv
import io
import tarfile
from datetime import date

import pytest
from DSI.csv_files import drg
from DSI.csv_files.drg_datasets import DATASETS

pytestmark = pytest.mark.unit

TAKST = next(d for d in DATASETS if d.ext_table == "drg_takstbeskrivelse")
INFO = next(d for d in DATASETS if d.ext_table == "drg_leveranceinfo")
KONTAKT = next(d for d in DATASETS if d.ext_table == "drg_kontakt")


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


def _csv_bytes(header: list[str], rows: list[list[str]], encoding: str = "cp1252") -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=";", quoting=csv.QUOTE_ALL)
    writer.writerow(header)
    writer.writerows(rows)
    return buffer.getvalue().encode(encoding)


def build_archive(tmp_path, name: str, members: dict[str, bytes]):
    """Skriv en tar.bz2 med medlemmer under T26/ (alfabetisk, som SDS)."""
    path = tmp_path / name
    with tarfile.open(path, mode="w:bz2") as tar:
        for member_name in sorted(members):
            data = members[member_name]
            info = tarfile.TarInfo(name=f"T26/{member_name}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return str(path)


def export_log(rows: dict[str, int]) -> bytes:
    lines = ["FilNavn;AntalRaekker;FilStoerrelseBytes;Oprettet"]
    for filename, count in rows.items():
        lines.append(f"{filename};{count};0;2026-08-24 18:43:47")
    return "\n".join(lines).encode("cp1252")


def takst_members(n_takst: int = 2) -> dict[str, bytes]:
    """Minimal leverance med takstbeskrivelse + leveranceinfo."""
    takst_rows = [[f"01MA0{i}", f"Beskrivelse {i}"] for i in range(1, n_takst + 1)]
    info_rows = [["DRG", "2026-08-23"]]
    return {
        "T26_DRGTAKSTBESKRIVELSE_1083_20260823.csv": _csv_bytes(["DRG", "BESKRIVELSE"], takst_rows),
        "T26_LEVERANCEINFO_1083_20260823.csv": _csv_bytes(
            ["DATAOMRAADE", "LPROPDATERINGSDATO"], info_rows
        ),
        "SDP_ExportLog_20260823.csv": export_log(
            {
                "T26_DRGTAKSTBESKRIVELSE_1083_20260823.csv": n_takst,
                "T26_LEVERANCEINFO_1083_20260823.csv": 1,
            }
        ),
    }


MINI_DATASETS = (TAKST, INFO)


class FakeCursor:
    def __init__(self, store):
        self.store = store

    def execute(self, sql, *params):
        self.store.setdefault("executed", []).append(sql)

    def executemany(self, sql, rows):
        table = sql.split("[ext].[")[1].split("]")[0]
        self.store.setdefault(table, []).extend(rows)

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.store: dict = {}

    def cursor(self):
        return FakeCursor(self.store)

    def commit(self):
        pass


def _archive_file(path: str) -> drg.DrgArchiveFile:
    parsed = drg.parse_archive_name(path.rsplit("/", 1)[-1])
    assert parsed is not None
    ltype, takst, kadence, ts = parsed
    return drg.DrgArchiveFile(
        path=path, leverancetype=ltype, takstsystem=takst, kadence=kadence, timestamp=ts
    )


def _load(tmp_path, members: dict[str, bytes], datasets=MINI_DATASETS, encoding: str = "cp1252"):
    path = build_archive(tmp_path, "Region_1083_DRG_T26_U_20260824190546.tar.bz2", members)
    archive = _archive_file(path)
    conn = FakeConn()

    class _Log:
        def info(self, *_):
            pass

        def warning(self, *_):
            pass

        def error(self, *_):
            pass

    class _Ctx:
        log = _Log()

    context = _Ctx()
    writers = {d.ext_table: drg._ExtWriter(conn, d, context) for d in datasets}
    result = drg.load_archive_into_ext(conn, archive, datasets, writers, encoding, context)
    for writer in writers.values():
        writer.close()
    return conn.store, result


# ---------------------------------------------------------------------------
# filnavnsparsning + select_newest
# ---------------------------------------------------------------------------


def test_parse_archive_name_varianter():
    ltype, takst, kadence, ts = drg.parse_archive_name(
        "Region_1083_DRG_T26_U_20260824190546.tar.bz2"
    )
    assert (ltype, takst, kadence) == ("DRG", "T26", "U")
    assert ts.date() == date(2026, 8, 24)

    ltype, takst, kadence, _ = drg.parse_archive_name(
        "Region_1083_DRGPSYK_T24_M_20250811232037.tar.bz2"
    )
    assert (ltype, takst, kadence) == ("DRGPSYK", "T24", "M")

    # aeldste filer uden kadence
    ltype, takst, kadence, _ = drg.parse_archive_name("Region_1083_DRG_T24_20250812235425.tar.bz2")
    assert (ltype, takst, kadence) == ("DRG", "T24", None)

    assert drg.parse_archive_name("Readme.txt") is None
    assert drg.parse_archive_name("Region_1083_DRG_T26_X_20260824190546.tar.bz2") is None


def test_select_newest_paa_tvaers_af_kadence(tmp_path):
    """Newest file wins regardless of cadence."""
    old_m = build_archive(tmp_path, "Region_1083_DRG_T26_M_20260818114549.tar.bz2", takst_members())
    new_u = build_archive(tmp_path, "Region_1083_DRG_T26_U_20260824190546.tar.bz2", takst_members())
    archives = [_archive_file(old_m), _archive_file(new_u)]
    selected, notes = drg.select_newest(archives, MINI_DATASETS, "cp1252")
    assert selected["T26"].kadence == "U"
    assert notes == []


def test_select_newest_falder_tilbage_ved_ubrugelig_leverance(tmp_path):
    """En leverance med 0 raekker i et datasaet maa aldrig vinde (soft-delete-vaern)."""
    good = build_archive(tmp_path, "Region_1083_DRG_T26_M_20260818114549.tar.bz2", takst_members())
    empty = takst_members()
    empty["SDP_ExportLog_20260823.csv"] = export_log(
        {
            "T26_DRGTAKSTBESKRIVELSE_1083_20260823.csv": 0,
            "T26_LEVERANCEINFO_1083_20260823.csv": 1,
        }
    )
    bad = build_archive(tmp_path, "Region_1083_DRG_T26_U_20260824190546.tar.bz2", empty)
    archives = [_archive_file(good), _archive_file(bad)]
    selected, notes = drg.select_newest(archives, MINI_DATASETS, "cp1252")
    assert selected["T26"].kadence == "M"
    assert any("0 raekker" in note for note in notes)


def test_select_newest_pin_og_filter(tmp_path):
    a24 = build_archive(tmp_path, "Region_1083_DRG_T24_M_20260317155935.tar.bz2", takst_members())
    a26 = build_archive(tmp_path, "Region_1083_DRG_T26_U_20260824190546.tar.bz2", takst_members())
    archives = [_archive_file(a24), _archive_file(a26)]

    selected, _ = drg.select_newest(
        archives, MINI_DATASETS, "cp1252", on_or_before=date(2026, 6, 1)
    )
    assert list(selected) == ["T24"]

    selected, _ = drg.select_newest(archives, MINI_DATASETS, "cp1252", takstsystemer=["T26"])
    assert list(selected) == ["T26"]


# ---------------------------------------------------------------------------
# skemavalidering
# ---------------------------------------------------------------------------


def test_ukendt_kolonne_fejler():
    with pytest.raises(ValueError, match="ukendte kolonner"):
        drg.validate_member_schema(TAKST, ["DRG", "BESKRIVELSE", "NY_KOLONNE"], "fil.csv")


def test_manglende_obligatorisk_kolonne_fejler():
    with pytest.raises(ValueError, match="obligatoriske kolonner mangler"):
        drg.validate_member_schema(TAKST, ["DRG"], "fil.csv")


# ---------------------------------------------------------------------------
# indlaesning + kontroltotaller
# ---------------------------------------------------------------------------


def test_indlaesning_stempler_takstsystem_og_leverance(tmp_path):
    store, result = _load(tmp_path, takst_members())
    rows = store["drg_takstbeskrivelse"]
    assert len(rows) == 2
    assert rows[0][0] == "T26"  # takstsystem foerst
    assert rows[0][-1] == "2026-08-24"  # leverance_dato sidst (fra filnavnets timestamp)
    assert result["raekker"]["T26_DRGTAKSTBESKRIVELSE_1083_20260823.csv"] == 2


def test_eksportlog_mismatch_fejler(tmp_path):
    members = takst_members()
    members["SDP_ExportLog_20260823.csv"] = export_log(
        {
            "T26_DRGTAKSTBESKRIVELSE_1083_20260823.csv": 999,
            "T26_LEVERANCEINFO_1083_20260823.csv": 1,
        }
    )
    with pytest.raises(ValueError, match="afviger fra SDP_ExportLog"):
        _load(tmp_path, members)


def test_manglende_eksportlog_fejler(tmp_path):
    members = takst_members()
    del members["SDP_ExportLog_20260823.csv"]
    with pytest.raises(ValueError, match="SDP_ExportLog mangler"):
        _load(tmp_path, members)


def test_ukendt_datasaet_fejler(tmp_path):
    members = takst_members()
    members["T26_NYT_DATASAET_1083_20260823.csv"] = _csv_bytes(["A"], [["1"]])
    with pytest.raises(ValueError, match="ukendt datasaet"):
        _load(tmp_path, members)


def test_forkert_feltantal_fejler(tmp_path):
    members = takst_members()
    broken = b'"DRG";"BESKRIVELSE"\n"01MA01"\n'
    members["T26_DRGTAKSTBESKRIVELSE_1083_20260823.csv"] = broken
    members["SDP_ExportLog_20260823.csv"] = export_log(
        {
            "T26_DRGTAKSTBESKRIVELSE_1083_20260823.csv": 1,
            "T26_LEVERANCEINFO_1083_20260823.csv": 1,
        }
    )
    with pytest.raises(ValueError, match="felter"):
        _load(tmp_path, members)


def test_for_lang_noeglevaerdi_fejler_uden_at_logge_vaerdien(tmp_path):
    members = takst_members()
    too_long = "X" * 10  # DRG-noeglen er deklareret 6 tegn bred
    members["T26_DRGTAKSTBESKRIVELSE_1083_20260823.csv"] = _csv_bytes(
        ["DRG", "BESKRIVELSE"], [[too_long, "resten"], ["01MA01", "ok"]]
    )
    with pytest.raises(ValueError) as excinfo:
        _load(tmp_path, members)
    assert "overskrider deres deklarerede bredde" in str(excinfo.value)
    assert too_long not in str(excinfo.value)  # GDPR: vaerdien maa aldrig naevnes


def test_ext_create_sql_noegler_er_not_null():
    sql = drg._ext_create_sql(TAKST)
    assert "DROP TABLE IF EXISTS [ext].[drg_takstbeskrivelse]" in sql
    assert "[DRG] NVARCHAR(6) NOT NULL" in sql
    assert "[BESKRIVELSE] NVARCHAR(500) NULL" in sql
    assert "[takstsystem] NVARCHAR(3) NOT NULL" in sql
    assert "[leverance_dato] DATE NOT NULL" in sql


# ---------------------------------------------------------------------------
# encoding sanity
# ---------------------------------------------------------------------------


def test_mojibake_fejler_uden_at_logge_vaerdien(tmp_path):
    """UTF-8-som-cp1252-markoeren (ï¿½) skal fejle med kolonne+antal, ikke vaerdi."""
    members = takst_members()
    corrupt = "Beskrivelse med ï¿½ midt i teksten"
    members["T26_DRGTAKSTBESKRIVELSE_1083_20260823.csv"] = _csv_bytes(
        ["DRG", "BESKRIVELSE"], [["01MA01", corrupt], ["01MA02", "ok"]]
    )
    with pytest.raises(ValueError) as excinfo:
        _load(tmp_path, members)
    assert "encoding-korruption" in str(excinfo.value)
    assert "BESKRIVELSE" in str(excinfo.value)
    assert corrupt not in str(excinfo.value)  # GDPR: vaerdien maa aldrig naevnes


def test_u_fffd_fejler_ved_utf8_kilde(tmp_path):
    """U+FFFD i en UTF-8-leverance (afsenderen har selv erstattet tegn) fejler."""
    members = takst_members()  # ASCII-only medlemmer er gyldig UTF-8
    members["T26_DRGTAKSTBESKRIVELSE_1083_20260823.csv"] = _csv_bytes(
        ["DRG", "BESKRIVELSE"],
        [["01MA01", "tekst med �"], ["01MA02", "ok"]],
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="encoding-korruption"):
        _load(tmp_path, members, encoding="utf-8")


# ---------------------------------------------------------------------------
# stale-delivery alarm
# ---------------------------------------------------------------------------


def _selected(*names: str) -> dict[str, drg.DrgArchiveFile]:
    files = [_archive_file(n) for n in names]
    return {f.takstsystem: f for f in files}


def test_foraeldet_leverance_fejler():
    selected = _selected("Region_1083_DRG_T26_U_20260801190546.tar.bz2")
    with pytest.raises(ValueError, match="dage gammel"):
        drg.check_delivery_age(selected, {"T26"}, "DRG", today=date(2026, 8, 26))


def test_pensioneret_takstsystem_udloeser_ikke_alarmen():
    """Det AKTIVE takstsystem (hoejeste) vurderes — T24 faar aldrig nye filer."""
    selected = _selected(
        "Region_1083_DRG_T24_M_20260317155935.tar.bz2",
        "Region_1083_DRG_T26_U_20260824190546.tar.bz2",
    )
    ages = drg.check_delivery_age(selected, {"T24", "T26"}, "DRG", today=date(2026, 8, 26))
    assert ages["T24"] > 100  # gammel, men lovlig — det aktive spor er T26
    assert ages["T26"] == 2


def test_frisk_genleverance_paa_gammelt_takstsystem_skjuler_ikke_doedt_aktivt_spor():
    """En sen T24-genleverance maa ikke undertrykke alarmen for et T26-spor,
    der er gaaet i staa."""
    selected = _selected(
        "Region_1083_DRG_T24_M_20260824000000.tar.bz2",  # frisk genleverance
        "Region_1083_DRG_T26_U_20260523000000.tar.bz2",  # aktivt spor doedt
    )
    with pytest.raises(ValueError, match="T26.*dage gammel"):
        drg.check_delivery_age(selected, {"T24", "T26"}, "DRG", today=date(2026, 8, 26))


def test_aktivt_takstsystem_uden_brugbar_leverance_fejler():
    """Afviser pre-flighten alle det aktive takstsystems filer, er det samme
    alarm — en frisk T25 maa ikke goere assettet groent."""
    selected = _selected("Region_1083_DRG_T25_M_20260824000000.tar.bz2")
    with pytest.raises(ValueError, match="T26.*INGEN brugbar"):
        drg.check_delivery_age(selected, {"T25", "T26"}, "DRG", today=date(2026, 8, 26))


def test_psyk_graense_er_maanedlig():
    month_old = _selected("Region_1083_DRGPSYK_T26_M_20260727120000.tar.bz2")
    ages = drg.check_delivery_age(month_old, {"T26"}, "DRGPSYK", today=date(2026, 8, 26))
    assert ages == {"T26": 30}  # under DRGPSYK-graensen paa 40

    with pytest.raises(ValueError, match="dage gammel"):
        drg.check_delivery_age(month_old, {"T26"}, "DRGPSYK", today=date(2026, 10, 1))


# ---------------------------------------------------------------------------
# takstsystem-scoped schema validation + overflow-row skip
# ---------------------------------------------------------------------------


def test_optional_kolonner_maa_kun_mangle_i_gamle_takstsystemer():
    """En kolonne der forsvinder fra T26-skemaet er en regression, der ville
    NULLe arkivets vaerdier ved merge — det skal fejle."""
    t25_header = [c for c in KONTAKT.source_columns if c not in KONTAKT.optional_columns]
    drg.validate_member_schema(KONTAKT, t25_header, "fil.csv", takstsystem="T25")  # lovligt
    with pytest.raises(ValueError, match="obligatoriske kolonner mangler"):
        drg.validate_member_schema(KONTAKT, t25_header, "fil.csv", takstsystem="T26")


def test_overflow_raekker_naar_aldrig_databasen(tmp_path, monkeypatch):
    """En raekke med for lang noeglevaerdi maa aldrig flushes til ext — NULL i
    en NOT NULL-noegle ville give en raa driverfejl FOER den designede
    (GDPR-sikre) fejl."""
    monkeypatch.setattr(drg, "INSERT_CHUNK_SIZE", 1)  # flush pr. raekke
    members = takst_members()
    members["T26_DRGTAKSTBESKRIVELSE_1083_20260823.csv"] = _csv_bytes(
        ["DRG", "BESKRIVELSE"], [["01MA01", "ok"], ["X" * 10, "for lang noegle"]]
    )
    path = build_archive(tmp_path, "Region_1083_DRG_T26_M_20260818114549.tar.bz2", members)
    archive = _archive_file(path)
    conn = FakeConn()

    class _Log:
        def info(self, *_):
            pass

        def warning(self, *_):
            pass

    class _Ctx:
        log = _Log()

    writers = {d.ext_table: drg._ExtWriter(conn, d, _Ctx()) for d in MINI_DATASETS}
    with pytest.raises(ValueError, match="overskrider deres deklarerede bredde"):
        drg.load_archive_into_ext(conn, archive, MINI_DATASETS, writers, "cp1252", _Ctx())
    flushed = conn.store.get("drg_takstbeskrivelse", [])
    assert len(flushed) == 1  # kun den gode raekke blev flushet
    assert all(row[1] is not None for row in flushed)  # DRG-noeglen er aldrig None

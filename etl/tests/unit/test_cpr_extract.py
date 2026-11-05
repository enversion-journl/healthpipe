# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests of the CPR load; the personal numbers are synthetic and invalid (day 00, month 00)."""

from collections import defaultdict
from types import SimpleNamespace

import dagster as dg
import pytest
from core.resources import FileShareResource
from DSI.csv_files.cpr import (
    ARKIVTABELLER,
    BlokdelingFejlede,
    Leverance,
    UkendtRecordlaengde,
    efterslaeb,
    filart,
    gennemloeb,
    kolonnenavn,
    kontroller_kaede,
    kontroller_leverance,
    linjer,
    naeste_leverance,
    sidst_indlaeste_leverance,
    udskaer,
    vaelg_filer,
    vaelg_layout,
)
from DSI.csv_files.cpr_layouts import LAYOUTS

pytestmark = pytest.mark.unit

PNR_A = "0000000001"
PNR_B = "0000000002"


def laes_leverance(sti: str, encoding: str) -> tuple:
    """The whole file materialised."""
    leverance = Leverance(sti)
    raekker: dict = defaultdict(list)
    for tabel, raekke in gennemloeb(sti, encoding, leverance):
        raekker[tabel].append(raekke)
    return leverance, raekker


def _pad(felter: dict, recordtype: str) -> str:
    layout = LAYOUTS[recordtype]
    buf = [" "] * layout.recordlaengde
    for felt in layout.felter:
        vaerdi = felter.get(felt.navn, "")[: felt.laengde].ljust(felt.laengde)
        buf[felt.position - 1 : felt.position - 1 + felt.laengde] = list(vaerdi)
    return "".join(buf)


def _start(proddto="20260908", forrig="20260907", opgave="208109"):
    return _pad(
        {
            "RECORDTYPE": "000",
            "OPGAVENR": opgave,
            "PRODDTO": proddto,
            "PRODDTOFORRIG": forrig,
        },
        "000",
    )


def _slut(taeller: int):
    return _pad({"RECORDTYPE": "999", "TAELLER": str(taeller)}, "999")


def _person(pnr=PNR_A, status="01"):
    return _pad({"RECORDTYPE": "001", "PNR": pnr, "STATUS": status, "KOEN": "K"}, "001")


def _adresse_b(pnr=PNR_A, uuid="11111111-2222-3333-4444-555555555555"):
    """Address record as the delivery actually sends it: the B variant."""
    return _pad(
        {"RECORDTYPE": "002", "PNR": pnr, "KOMKOD": "0461", "ADRESSE_UUID": uuid},
        "002B",
    )


def _hist_adresse_b(pnr=PNR_A, tilfly="199501010000", frafly="200501010000"):
    return _pad(
        {
            "RECORDTYPE": "023",
            "PNR": pnr,
            "TILFLYDTO": tilfly,
            "FRAFLYDTO": frafly,
        },
        "023B",
    )


def _skriv(tmp_path, navn, linjer_, encoding="cp1252"):
    sti = tmp_path / navn
    sti.write_text("\n".join(linjer_) + "\n", encoding=encoding)
    return str(sti)


# --------------------------------------------------------------- layout choice


def test_layout_is_chosen_on_both_code_and_length():
    """The core of it all: the code alone cannot tell the variants apart."""
    assert vaelg_layout("002", 306) is LAYOUTS["002"]
    assert vaelg_layout("002", 382) is LAYOUTS["002B"]
    assert vaelg_layout("023", 96) is LAYOUTS["023"]
    assert vaelg_layout("023", 132) is LAYOUTS["023B"]


def test_known_type_with_unexpected_length_fails_hard():
    """A variant change at CPR must not pass silently."""
    with pytest.raises(UkendtRecordlaengde, match="known lengths"):
        vaelg_layout("002", 400)


def test_error_message_carries_no_record_content():
    """The message must not write a record into the log file."""
    try:
        vaelg_layout("001", 999)
    except UkendtRecordlaengde as exc:
        assert PNR_A not in str(exc)
        assert "999" in str(exc)  # only the length, not the content
    else:
        pytest.fail("expected UkendtRecordlaengde")


def test_unknown_record_type_gives_none_instead_of_error():
    """CPR may add a type that is of no interest here."""
    assert vaelg_layout("777", 50) is None


# -------------------------------------------------------------- field slicing


def test_b_variant_extra_fields_are_included():
    """With the right layout chosen, street name and address UUID are there."""
    uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    linje = _adresse_b(uuid=uuid)
    v = udskaer(linje, vaelg_layout("002", len(linje)))
    assert v["adresse_uuid"] == uuid
    assert "vejnvn" in v


def test_wrong_layout_would_lose_the_tail_without_failing():
    """The base variant reads a longer record without error but loses the tail, so length counts."""
    linje = _adresse_b()
    med_forkert = udskaer(linje, LAYOUTS["002"])
    assert "adresse_uuid" not in med_forkert  # lost, without a single error
    assert med_forkert["komkod"] == "0461"  # shared fields are still correct


def test_record_type_is_not_a_column():
    v = udskaer(_person(), LAYOUTS["001"])
    assert "recordtype" not in v


def test_empty_fields_become_empty_string_not_none():
    """Key columns are NOT NULL, and NULL does not match itself in a MERGE."""
    v = udskaer(_hist_adresse_b(frafly=""), LAYOUTS["023B"])
    assert v["fraflydto"] == ""
    assert v["fraflydto"] is not None


def test_column_names_are_normalised():
    assert kolonnenavn("START_DT-PERSON") == "start_dt_person"
    assert kolonnenavn("ADRESSE_UUID") == "adresse_uuid"


# -------------------------------------------------------------------- the pass


def test_delivery_is_distributed_across_archive_tables(tmp_path):
    sti = _skriv(
        tmp_path,
        "d260908.l208109",
        [_start(), _person(PNR_A), _adresse_b(PNR_A), _person(PNR_B), _slut(3)],
    )
    leverance, raekker = laes_leverance(sti, "cp1252")
    assert leverance.records_laest == 3
    assert leverance.personer_beroert == 2
    assert len(raekker["cpr_001_person"]) == 2
    assert len(raekker["cpr_002_adresse"]) == 1


def test_start_record_metadata_is_read(tmp_path):
    sti = _skriv(tmp_path, "d260908.l208109", [_start("20260908", "20260907"), _slut(0)])
    leverance, _ = laes_leverance(sti, "cp1252")
    assert leverance.produktionsdato == "20260908"
    assert leverance.forrige_produktionsdato == "20260907"
    assert leverance.opgavenr == "208109"


def test_unknown_record_type_is_counted_but_not_archived(tmp_path):
    sti = _skriv(
        tmp_path,
        "d260908.l208109",
        [_start(), _person(), "777" + "0" * 40, _slut(2)],
    )
    leverance, raekker = laes_leverance(sti, "cp1252")
    assert leverance.ukendte_recordtyper == {"777": 1}
    assert "777" not in raekker


def test_file_without_line_breaks_is_split_on_record_length(tmp_path):
    sti = tmp_path / "d260908.l208109"
    sti.write_text(_person() + _person(PNR_B), encoding="cp1252")
    assert [x[0:3] for x in linjer(str(sti), "cp1252")] == ["001", "001"]


# ---------------------------------------------------------------------- checks


def test_missing_start_record_fails():
    leverance = Leverance("/et/sted/d260908.l208109")
    leverance.records_laest = 5
    with pytest.raises(ValueError, match="start record is missing"):
        kontroller_leverance(leverance)


def test_control_total_that_does_not_match_fails():
    leverance = Leverance("/et/sted/d260908.l208109")
    leverance.produktionsdato = "20260908"
    leverance.records_laest = 100
    leverance.records_i_slutrecord = 250
    with pytest.raises(ValueError, match="incomplete"):
        kontroller_leverance(leverance)


def test_missing_end_record_fails():
    """Without an end record the file is rejected, since a truncated file otherwise looks whole."""
    leverance = Leverance("/et/sted/d260908.l208109")
    leverance.produktionsdato = "20260908"
    leverance.records_laest = 100
    with pytest.raises(ValueError, match="end record is missing"):
        kontroller_leverance(leverance)


def test_end_record_without_readable_counter_fails(tmp_path):
    """A counter that is not digits is as blind as no end record."""
    sti = _skriv(
        tmp_path,
        "d260908.l208109",
        [_start(), _person(), _pad({"RECORDTYPE": "999", "TAELLER": "xx"}, "999")],
    )
    leverance, _ = laes_leverance(sti, "cp1252")
    assert leverance.records_i_slutrecord is None
    with pytest.raises(ValueError, match="end record is missing"):
        kontroller_leverance(leverance)


def test_truncated_line_file_without_end_record_fails(tmp_path):
    """The line-based path must catch truncation as hard as the blocked one."""
    sti = _skriv(tmp_path, "d260908.l208109", [_start(), _person(PNR_A), _person(PNR_B)])
    leverance, _ = laes_leverance(sti, "cp1252")
    with pytest.raises(ValueError, match="end record is missing"):
        kontroller_leverance(leverance)


def test_too_many_unknown_record_types_fails():
    leverance = Leverance("/et/sted/d260908.l208109")
    leverance.produktionsdato = "20260908"
    leverance.records_laest = 1000
    leverance.records_i_slutrecord = 1000
    leverance.ukendte_recordtyper["777"] = 50  # 5 %
    with pytest.raises(ValueError, match="unknown record type"):
        kontroller_leverance(leverance)


def test_few_unknown_record_types_pass():
    leverance = Leverance("/et/sted/d260908.l208109")
    leverance.produktionsdato = "20260908"
    leverance.records_laest = 1000
    leverance.records_i_slutrecord = 1000
    leverance.ukendte_recordtyper["777"] = 2
    kontroller_leverance(leverance)  # no error


def test_gap_in_operation_fails():
    """When the archive is at a change extract, a gap is a skipped daily delivery."""
    leverance = Leverance("/et/sted/d260908.l208109")
    leverance.produktionsdato = "20260908"
    leverance.forrige_produktionsdato = "20260907"
    with pytest.raises(ValueError, match="gap in the delivery chain"):
        kontroller_kaede(leverance, "20260905", sidst_var_grundudtraek=False)


def test_gap_after_base_extract_is_accepted_but_recorded():
    """An older base extract may be put to use, but the missing window is recorded."""
    leverance = Leverance("/et/sted/d260714.l208109")
    leverance.produktionsdato = "20260714"
    leverance.forrige_produktionsdato = "20260713"
    vindue = kontroller_kaede(leverance, "20260609", sidst_var_grundudtraek=True)
    assert vindue == ("20260609", "20260713")


def test_rerun_of_same_delivery_is_allowed():
    """A rerun of the same delivery is idempotent, not rejected by the chain check."""
    leverance = Leverance("/et/sted/d260724.l208109")
    leverance.produktionsdato = "20260724"
    leverance.forrige_produktionsdato = "20260723"
    assert kontroller_kaede(leverance, "20260724", sidst_var_grundudtraek=False) is None


def test_unbroken_chain_gives_no_blind_window():
    leverance = Leverance("/et/sted/d260908.l208109")
    leverance.produktionsdato = "20260908"
    leverance.forrige_produktionsdato = "20260907"
    assert kontroller_kaede(leverance, "20260907", sidst_var_grundudtraek=False) is None


def test_first_delivery_after_base_extract_without_gap_is_clean():
    leverance = Leverance("/et/sted/d260610.l208109")
    leverance.produktionsdato = "20260610"
    leverance.forrige_produktionsdato = "20260609"
    assert kontroller_kaede(leverance, "20260609", sidst_var_grundudtraek=True) is None


def test_base_extract_requires_no_chain():
    """A base extract stands alone and does not follow on from a previous one."""
    leverance = Leverance("/et/sted/d260609.l208110")
    leverance.produktionsdato = "20260609"
    leverance.forrige_produktionsdato = "20260608"
    assert kontroller_kaede(leverance, "20260101", sidst_var_grundudtraek=False) is None


def test_first_load_must_be_a_base_extract():
    leverance = Leverance("/et/sted/d260908.l208109")
    leverance.produktionsdato = "20260908"
    leverance.forrige_produktionsdato = "20260907"
    with pytest.raises(ValueError, match="the first load must be a base extract"):
        kontroller_kaede(leverance, None)


def test_first_base_extract_is_accepted_on_an_empty_archive():
    leverance = Leverance("/et/sted/d260609.l208110")
    leverance.produktionsdato = "20260609"
    assert kontroller_kaede(leverance, None) is None


@pytest.mark.parametrize("navn", ["d260906.l208109", "d260601.l208110"])
def test_a_delivery_older_than_the_archive_is_rejected(navn):
    """Also an older base extract: it would roll all persons in the file back."""
    leverance = Leverance(f"/et/sted/{navn}")
    leverance.produktionsdato = "20" + navn[1:7]
    leverance.forrige_produktionsdato = "20260905"
    with pytest.raises(ValueError, match="older delivery"):
        kontroller_kaede(leverance, "20260908", sidst_var_grundudtraek=False)


# --------------------------------------------------------------- file handling


def test_file_kind_is_known_from_the_task_number():
    assert filart("d260609.l208110") == "grundudtraek"
    assert filart("d260908.l208109") == "aendring"
    assert filart("noget-andet.txt") == "ukendt filnavnsmønster"


def test_files_come_in_date_order(tmp_path):
    """The order decides whether the chain check can work at all."""
    for navn in ("d260908.l208109", "d260609.l208110", "d260907.l208109"):
        _skriv(tmp_path, navn, [_start(), _slut(0)])
    valgte = [x.rsplit("/", 1)[-1] for x in vaelg_filer(str(tmp_path))]
    assert valgte == ["d260609.l208110", "d260907.l208109", "d260908.l208109"]


def test_files_without_a_valid_name_are_skipped(tmp_path):
    _skriv(tmp_path, "d260908.l208109", [_start(), _slut(0)])
    (tmp_path / "laesmig.txt").write_text("ikke en leverance", encoding="utf-8")
    assert len(vaelg_filer(str(tmp_path))) == 1


def test_two_deliveries_with_the_same_date_are_rejected(tmp_path):
    for navn in ("d260908.l208109", "d260908.l208110"):
        _skriv(tmp_path, navn, [_start(), _slut(0)])
    with pytest.raises(ValueError, match="same production date"):
        naeste_leverance(str(tmp_path), "20260907")


def test_another_file_on_the_loaded_date_is_rejected(tmp_path):
    """If a second file arrives after the first is archived, it must not disappear silently."""
    from DSI.csv_files.cpr import kontroller_ingen_anden_fil_paa_datoen

    _skriv(tmp_path, "d260908.l208109", [_start(), _slut(0)])
    kontroller_ingen_anden_fil_paa_datoen(str(tmp_path), "20260908", "d260908.l208109")
    kontroller_ingen_anden_fil_paa_datoen(str(tmp_path), "20260908", None)

    _skriv(tmp_path, "d260908.l208110", [_start(), _slut(0)])
    with pytest.raises(ValueError, match="d260908.l208110"):
        kontroller_ingen_anden_fil_paa_datoen(str(tmp_path), "20260908", "d260908.l208109")


def test_next_delivery_and_backlog_see_the_same_files(tmp_path):
    for navn in ("d260907.l208109", "d260908.l208109", "d260909.l208109"):
        _skriv(tmp_path, navn, [_start(), _slut(0)])
    assert naeste_leverance(str(tmp_path), "20260907").endswith("d260908.l208109")
    assert efterslaeb(str(tmp_path), "20260907") == ["20260908", "20260909"]


def test_file_name_date_must_be_the_start_records(tmp_path):
    """Otherwise the file selection takes the file again every night while the watermark stands."""
    sti = _skriv(tmp_path, "d260908.l208109", [_start(proddto="20260909"), _slut(0)])
    leverance, _ = laes_leverance(sti, "cp1252")
    with pytest.raises(ValueError, match="the file name says 20260908"):
        kontroller_leverance(leverance)


def test_an_unknown_task_number_is_rejected(tmp_path):
    sti = _skriv(tmp_path, "d260908.l208111", [_start(opgave="208111"), _slut(0)])
    leverance, _ = laes_leverance(sti, "cp1252")
    with pytest.raises(ValueError, match="Only base extracts and daily change extracts"):
        kontroller_leverance(leverance)


# ---------------------------------------------------------------------- tables


def test_all_archive_tables_have_a_known_layout():
    for tabel in ARKIVTABELLER:
        assert vaelg_layout(tabel.recordtype, LAYOUTS[tabel.recordtype].recordlaengde)


def test_key_columns_exist_in_the_layout():
    """A key that does not correspond to a field would fail only in the database."""
    for tabel in ARKIVTABELLER:
        varianter = [LAYOUTS[n] for n in LAYOUTS if n[:3] == tabel.recordtype]
        for layout in varianter:
            kolonner = {kolonnenavn(f.navn) for f in layout.felter}
            manglende = set(tabel.noegle) - kolonner
            assert not manglende, f"{tabel.tabel}: {manglende} not found in {layout.recordtype}"


# --------------------------------------------------------------------- archive


def test_tables_without_unique_source_are_replaced_instead_of_merged():
    """The two historical tables have no unique key and are therefore replaced per person."""
    from DSI.csv_files.cpr import ARKIVTABELLER

    erstattes = {t.tabel for t in ARKIVTABELLER if t.erstattes}
    assert erstattes == {"cpr_023_historisk_adresse", "cpr_034_historisk_beskyttelse"}
    assert {t.tabel for t in ARKIVTABELLER if t.akkumulerer} == {"cpr_099_haendelse"}


def test_archive_configuration_covers_all_columns():
    """A column in ext must not silently be missing from the MERGE's column list."""
    from DSI.csv_files.cpr import kolonner_for_tabel
    from DSI.csv_files.cpr_archive import KONFIGURATIONER

    for tabel in KONFIGURATIONER:
        konfiguration = KONFIGURATIONER[tabel]
        daekket = set(konfiguration.merge_keys) | set(konfiguration.non_key_columns)
        forventet = set(kolonner_for_tabel(tabel)) | {"leverance_dato"}
        assert daekket == forventet, f"{tabel}: {forventet - daekket} missing"


def test_delivery_date_is_outside_the_change_check():
    """Otherwise every run would rewrite all rows — except 996, where the
    date is exactly what says when the person was last in a delivery."""
    from DSI.csv_files.cpr import ARKIVTABELLER
    from DSI.csv_files.cpr_archive import KONFIGURATIONER

    sidst_set = {t.tabel for t in ARKIVTABELLER if t.sidst_set}
    assert sidst_set == {"cpr_996_data_fremadrettet"}
    for tabel, konfiguration in KONFIGURATIONER.items():
        assert "leverance_dato" in konfiguration.non_key_columns
        forventet = tabel in sidst_set
        assert ("leverance_dato" in konfiguration.change_detect_columns) is forventet, tabel


def test_events_are_appended_without_anything_being_deleted():
    """099 is a log: only new events are appended, and duplicates in the file count once."""
    from DSI.csv_files.cpr_archive import tilfoej_haendelser

    mssql = _FalskMssql()
    tal = tilfoej_haendelser(mssql, "cpr_099_haendelse", ("pnr", "timestampu", "haendelse"))
    sql = " ".join(s for s, _ in mssql.kald)
    assert sql.startswith("INSERT INTO [arc].[cpr_099_haendelse]")
    assert "NOT EXISTS" in sql and "GROUP BY" in sql
    assert "DELETE" not in sql and "UPDATE" not in sql
    assert tal["soft_slettet"] == 0 and mssql.committet


def test_delete_detection_is_scoped_to_the_deliverys_persons():
    """Persons the delivery does not mention are not soft-deleted; the scope reads the snapshot."""
    from DSI.csv_files.cpr_archive import SCOPE_TABEL, byg_afgraensning

    predikat = byg_afgraensning()
    assert predikat.startswith("tgt.[pnr] IN")
    assert f"[meta].[{SCOPE_TABEL}]" in predikat
    assert "[ext]." not in predikat


def test_replacement_per_person_uses_the_frozen_set_of_persons():
    """The two replaced tables are scoped by the same snapshot as the twelve MERGEs."""
    from DSI.csv_files.cpr_archive import SCOPE_TABEL, erstat_pr_person

    mssql = _FalskMssql({"SELECT COUNT(*) FROM [ext].[cpr_023_historisk_adresse]": (0,)})
    tal = erstat_pr_person(mssql, "cpr_023_historisk_adresse", ("pnr", "komkod"))

    udsagn = [sql for sql, _ in mssql.kald]
    slet = next(s for s in udsagn if s.startswith("DELETE"))
    indsaet = next(s for s in udsagn if s.startswith("INSERT"))
    for sql in (slet, indsaet):
        assert f"[pnr] IN (SELECT [pnr] FROM [meta].[{SCOPE_TABEL}])" in sql
        assert "[ext].[cpr_001_person]" not in sql, "the set of persons is read live from ext"
    assert "FROM [ext].[cpr_023_historisk_adresse]" in indsaet  # the content is the delivery
    assert mssql.committet and mssql.committet[-1] >= 2, "not committed after both statements"
    assert set(tal) == {"indsat", "opdateret", "soft_slettet", "hard_slettet"}


def test_replacement_fails_hard_when_ext_has_rows_without_person_record():
    """A 023/034 row without a person record in the delivery stops the run without a commit."""
    from DSI.csv_files.cpr_archive import erstat_pr_person

    mssql = _FalskMssql({"SELECT COUNT(*) FROM [ext].[cpr_034_historisk_beskyttelse]": (3,)})
    with pytest.raises(ValueError, match="without a person record"):
        erstat_pr_person(mssql, "cpr_034_historisk_beskyttelse", ("pnr", "beskyttype"))
    assert not mssql.committet, "the half replacement was committed"
    assert any(
        s.startswith("DELETE") for s, _ in mssql.kald
    )  # the delete did happen and is rolled back


def test_historical_personal_numbers_are_scoped_on_the_current_number():
    """In 022 the scope is on pnrgaeld (the current number), not on the historical pnr."""
    from DSI.csv_files.cpr_archive import byg_afgraensning

    historiske = next(t for t in ARKIVTABELLER if t.recordtype == "022")
    assert historiske.personkolonne == "pnrgaeld"
    assert byg_afgraensning(personkolonne=historiske.personkolonne).startswith("tgt.[pnrgaeld] IN")
    assert all(t.personkolonne == "pnr" for t in ARKIVTABELLER if t.recordtype != "022")


def test_person_column_is_the_field_in_position_4():
    """The number the scope is on must be in position 4-13 — the same place
    as the person record's pnr, so the same kind of number is compared."""
    for tabel in ARKIVTABELLER:
        for layout in (LAYOUTS[n] for n in LAYOUTS if n[:3] == tabel.recordtype):
            felt = next(f for f in layout.felter if f.position == 4)
            assert kolonnenavn(felt.navn) == tabel.personkolonne, tabel.tabel


def test_ext_columns_follow_the_longest_variant():
    """Ext must be able to receive both variants without a schema change."""
    from DSI.csv_files.cpr import kolonner_for_tabel

    adresse = kolonner_for_tabel("cpr_002_adresse")
    assert "adresse_uuid" in adresse and "vejnvn" in adresse
    historisk = kolonner_for_tabel("cpr_023_historisk_adresse")
    assert "adresse_uuid" in historisk


def test_infrastructure_asset_is_registered():
    """The asset is in the combined list that the definitions read."""
    from infrastructure import all_infrastructure_assets

    navne = {a.key.path[-1] for a in all_infrastructure_assets}
    assert "staging_infra_source_cpr" in navne


def test_scope_can_be_set_on_the_configuration():
    """ArchiveMergeConfig is copied as a frozen dataclass, the same way as in the asset."""
    from DSI.csv_files.cpr_archive import (
        KONFIGURATIONER,
        byg_afgraensning,
        med_afgraensning,
    )

    grund = KONFIGURATIONER["cpr_001_person"]
    assert grund.delete_scope_predicate is None
    med = med_afgraensning(grund, byg_afgraensning())
    assert med.delete_scope_predicate == byg_afgraensning()
    assert med.merge_keys == grund.merge_keys
    assert grund.delete_scope_predicate is None  # the original is untouched


# ---------------------------------------------------------------------------
# The day without a file: weekend, public holiday, or a delivery not arrived
# ---------------------------------------------------------------------------


class _FalskCursor:
    """Collects the SQL the code issues, so the test can see what is actually run."""

    def __init__(self, kald: list, svar: dict):
        self.kald = kald
        self._svar = svar
        self._sidste = ""
        self.rowcount = 0

    def execute(self, sql, parametre=None):
        self._sidste = " ".join(sql.split())
        self.kald.append((self._sidste, parametre))

    def executemany(self, sql, raekker):
        self.kald.append((" ".join(sql.split()), list(raekker)))

    def fetchone(self):
        for noegle, vaerdi in self._svar.items():
            if noegle in self._sidste:
                return vaerdi
        return None

    def close(self):
        pass


class _FalskForbindelse:
    def __init__(self, kald: list, svar: dict, kvittering: list):
        self.kald = kald
        self._svar = svar
        self._kvittering = kvittering

    def cursor(self):
        return _FalskCursor(self.kald, self._svar)

    def commit(self):
        self._kvittering.append(len(self.kald))

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class _FalskMssql:
    def __init__(self, svar: dict | None = None):
        self.kald: list = []
        self.committet: list = []
        self._svar = svar or {}

    def get_connection(self):
        return _FalskForbindelse(self.kald, self._svar, self.committet)


def test_watermark_counts_only_archived_deliveries():
    """The watermark only moves past deliveries the archive has stamped."""
    mssql = _FalskMssql({"sys.tables": (1,), "CONVERT(CHAR(8)": ("20260908", "aendring")})
    sidst, var_grund = sidst_indlaeste_leverance(mssql)

    assert sidst == "20260908"
    assert var_grund is False
    opslag = [sql for sql, _ in mssql.kald if "CPRLeverance" in sql]
    assert opslag, "the delivery register was not queried at all"
    assert any(
        "HAVING COUNT(*) = COUNT([arkiveret_tidspunkt])" in sql for sql in opslag
    ), "the watermark reads deliveries where not all tables are archived"


def test_backlog_check_counts_from_the_loaded_delivery():
    """A delivery that is not yet archived is also loaded and is not waiting."""
    mssql = _FalskMssql({"sys.tables": (1,), "CONVERT(CHAR(8)": ("20260909", "aendring")})
    sidst, _ = sidst_indlaeste_leverance(mssql, kun_arkiverede=False)

    assert sidst == "20260909"
    opslag = [sql for sql, _ in mssql.kald if "CPRLeverance" in sql]
    assert opslag and not any("arkiveret_tidspunkt" in sql for sql in opslag)


@pytest.mark.parametrize(("ventende", "bestaaet"), [(3, True), (4, False)])
def test_backlog_check_warns_above_the_limit_and_shows_only_dates(monkeypatch, ventende, bestaaet):
    from DSI.csv_files import cpr as modul

    datoer = [f"202609{dag:02d}" for dag in range(10, 10 + ventende)]
    monkeypatch.setattr(modul, "EFTERSLAEB_GRAENSE", 3)
    monkeypatch.setattr(
        modul, "sidst_indlaeste_leverance", lambda mssql, kun_arkiverede=True: ("20260909", False)
    )
    monkeypatch.setattr(modul, "efterslaeb", lambda rod, sidst: datoer)

    resultat = modul.cpr_efterslaeb_er_lille.node_def.compute_fn.decorated_fn(
        object(), SimpleNamespace(root=lambda: "/findes/ikke")
    )

    assert resultat.passed is bestaaet
    assert resultat.severity == dg.AssetCheckSeverity.WARN
    metadata = {k: getattr(v, "value", v) for k, v in resultat.metadata.items()}
    assert metadata["leverancer_i_efterslaeb"] == ventende
    assert metadata["aeldste_ventende"] == datoer[0] and metadata["nyeste_ventende"] == datoer[-1]
    assert "/findes/ikke" not in repr(metadata)


def test_no_new_delivery_empties_ext(monkeypatch, tmp_path):
    """Ext is emptied when no file came."""
    from DSI.csv_files import cpr as modul

    _skriv(tmp_path, "d260908.l208109", [_start(), _slut(0)])
    monkeypatch.setattr(modul, "naeste_leverance", lambda *_: None)
    monkeypatch.setattr(modul, "sidst_indlaeste_leverance", lambda *_: ("20260908", False))

    # The ext tables are recreated against the fake connection, so the test sees the SQL issued.
    mssql = _FalskMssql({"sp_getapplock": (0,), "sys.tables": (0,)})
    resultat = modul.ext_cpr(
        context=dg.build_asset_context(),
        mssql_target_staging=mssql,
        cpr_share=FileShareResource(base_dir=str(tmp_path)),
        config=modul.CprExtractConfig(),
    )

    assert resultat.metadata["leverance"] == "ingen ny"

    genskabte = {
        sql.split("[ext].[")[1].split("]")[0]
        for sql, _ in mssql.kald
        if sql.startswith("CREATE TABLE [ext].[")
    }
    assert genskabte == {
        t.tabel for t in ARKIVTABELLER
    }, "ext was not emptied; the previous delivery would be archived again"

    # DDL is transactional in SQL Server, and get_connection closes without
    # committing. Without a commit the emptying is rolled back, and the fault
    # would look fixed while the archive kept rerunning the previous delivery.
    assert mssql.committet, "the emptying was never committed and is rolled back"
    assert mssql.committet[-1] >= len(mssql.kald), "committed too early"


def test_a_delivery_without_persons_is_stamped_archived(monkeypatch, tmp_path):
    """There is nothing to archive, and without the stamp the delivery would be left as pending."""
    from unittest.mock import MagicMock

    from DSI.csv_files import cpr as modul

    _skriv(tmp_path, "d260909.l208109", [_start(proddto="20260909", forrig="20260908"), _slut(0)])
    monkeypatch.setattr(modul, "sidst_indlaeste_leverance", lambda *_: ("20260908", False))
    monkeypatch.setattr(modul, "DfmManager", MagicMock())

    mssql = _FalskMssql({"sp_getapplock": (0,), "sys.tables": (0,)})
    resultat = modul.ext_cpr(
        context=dg.build_asset_context(),
        mssql_target_staging=mssql,
        cpr_share=FileShareResource(base_dir=str(tmp_path)),
        config=modul.CprExtractConfig(),
    )

    assert resultat.metadata["personer"] == 0
    stempler = [(sql, p) for sql, p in mssql.kald if "SET [arkiveret_tidspunkt]" in sql]
    assert stempler == [(stempler[0][0], ("20260909",))]


def _indlaes(monkeypatch, tmp_path, linjer_):
    """Run ext_cpr against a synthetic file and a fake connection; return the connection's calls."""
    from unittest.mock import MagicMock

    from DSI.csv_files import cpr as modul

    _skriv(tmp_path, "d260909.l208109", linjer_)
    monkeypatch.setattr(modul, "sidst_indlaeste_leverance", lambda *_, **__: ("20260908", False))
    monkeypatch.setattr(modul, "DfmManager", MagicMock())
    mssql = _FalskMssql({"sp_getapplock": (0,), "sys.tables": (0,)})
    resultat = modul.ext_cpr(
        context=dg.build_asset_context(),
        mssql_target_staging=mssql,
        cpr_share=FileShareResource(base_dir=str(tmp_path)),
        config=modul.CprExtractConfig(),
    )
    return resultat, mssql


def test_delivery_rows_are_written_to_their_own_tables_and_get_the_delivery_date(
    monkeypatch, tmp_path
):
    start = _start(proddto="20260909", forrig="20260908")
    resultat, mssql = _indlaes(
        monkeypatch,
        tmp_path,
        [start, _person(PNR_A), _adresse_b(PNR_A), _person(PNR_B), _slut(3)],
    )

    indsat = {
        sql.split("[ext].[")[1].split("]")[0]: raekker
        for sql, raekker in mssql.kald
        if sql.startswith("INSERT INTO [ext].[")
    }
    assert {tabel: len(raekker) for tabel, raekker in indsat.items()} == {
        "cpr_001_person": 2,
        "cpr_002_adresse": 1,
    }
    assert [r[0] for r in indsat["cpr_001_person"]] == [PNR_A, PNR_B]
    # The last value is the placeholder; the real date is set by the UPDATE afterwards.
    assert {r[-1] for raekker in indsat.values() for r in raekker} == {"1900-01-01"}
    datoer = [p for sql, p in mssql.kald if sql.startswith("UPDATE [ext].[")]
    assert len(datoer) == len(ARKIVTABELLER) and set(datoer) == {("20260909",)}
    assert resultat.metadata["raekker_skrevet"] == 3


def test_a_failed_delivery_leaves_no_inserted_rows(monkeypatch, tmp_path):
    """The control total does not match: the buffered rows are discarded and never sent."""
    from unittest.mock import MagicMock

    from DSI.csv_files import cpr as modul

    _skriv(
        tmp_path,
        "d260909.l208109",
        [_start(proddto="20260909", forrig="20260908"), _person(PNR_A), _slut(5)],
    )
    monkeypatch.setattr(modul, "sidst_indlaeste_leverance", lambda *_, **__: ("20260908", False))
    monkeypatch.setattr(modul, "DfmManager", MagicMock())
    mssql = _FalskMssql({"sp_getapplock": (0,), "sys.tables": (0,)})
    with pytest.raises(ValueError, match="incomplete"):
        modul.ext_cpr(
            context=dg.build_asset_context(),
            mssql_target_staging=mssql,
            cpr_share=FileShareResource(base_dir=str(tmp_path)),
            config=modul.CprExtractConfig(),
        )

    assert not [sql for sql, _ in mssql.kald if sql.startswith("INSERT INTO [ext].[")]
    assert not [sql for sql, _ in mssql.kald if sql.startswith("UPDATE [ext].[")]


def test_archive_stamp_is_written_and_committed():
    """The archive stamp is committed."""
    from DSI.csv_files.cpr import marker_arkiveret

    mssql = _FalskMssql()
    marker_arkiveret(mssql, "20260908")

    opdateringer = [sql for sql, _ in mssql.kald if sql.startswith("UPDATE")]
    assert len(opdateringer) == 1, "not exactly one update was written"
    sql = opdateringer[0]
    assert "[arkiveret_tidspunkt] = CAST(SYSDATETIME() AS datetime2(0))" in sql
    assert "WHERE [leverance_dato] = CONVERT(date, ?, 112)" in sql

    _, parametre = next((k for k in mssql.kald if k[0].strip().startswith("UPDATE")), (None, None))
    assert parametre == ("20260908",)

    assert mssql.committet, "the stamp was never committed and is rolled back"


# ---------------------------------------------------------------------------
# Blocked file with several variants of the same record type
#
# The file carries the record type code but not the variant letter, so the
# record length is decided by what follows the cut.
# ---------------------------------------------------------------------------


def test_blocked_file_chooses_the_variant_that_resynchronises(tmp_path):
    """002 exists in two lengths. The delivery sends the long one; the cut must follow it."""
    sti = tmp_path / "d260908.l208109"
    sti.write_text(_adresse_b() + _adresse_b(PNR_B) + _person(), encoding="cp1252")

    laest = list(linjer(str(sti), "cp1252"))

    assert [len(x) for x in laest] == [
        LAYOUTS["002B"].recordlaengde,
        LAYOUTS["002B"].recordlaengde,
        LAYOUTS["001"].recordlaengde,
    ]
    assert [x[:3] for x in laest] == ["002", "002", "001"]
    # The personal numbers must be in their right place — the proof that the cut held
    assert udskaer(laest[0], LAYOUTS["002B"])["pnr"] == PNR_A
    assert udskaer(laest[1], LAYOUTS["002B"])["pnr"] == PNR_B


def test_blocked_file_with_short_variant_also_resynchronises(tmp_path):
    """The opposite way: if the source sends the short variant, the long one must not be taken."""
    kort = _pad({"RECORDTYPE": "002", "PNR": PNR_A, "KOMKOD": "0461"}, "002")
    assert len(kort) == LAYOUTS["002"].recordlaengde < LAYOUTS["002B"].recordlaengde
    sti = tmp_path / "d260908.l208109"
    sti.write_text(kort + _person(), encoding="cp1252")

    laest = list(linjer(str(sti), "cp1252"))

    assert [len(x) for x in laest] == [
        LAYOUTS["002"].recordlaengde,
        LAYOUTS["001"].recordlaengde,
    ]
    assert udskaer(laest[0], LAYOUTS["002"])["pnr"] == PNR_A


def test_blocked_file_that_is_truncated_fails(tmp_path):
    """Half a record at the end must not disappear silently."""
    sti = tmp_path / "d260908.l208109"
    sti.write_text(_person() + _person()[:40], encoding="cp1252")

    with pytest.raises(BlokdelingFejlede):
        list(linjer(str(sti), "cp1252"))


def test_blocked_file_with_unknown_record_type_fails(tmp_path):
    """Without a length the next record's start is unknown. Everything after would be shifted."""
    sti = tmp_path / "d260908.l208109"
    sti.write_text(_person() + "xyz" + " " * 100, encoding="cp1252")

    with pytest.raises(BlokdelingFejlede):
        list(linjer(str(sti), "cp1252"))


# ---------------------------------------------------------------------------
# Column names


@pytest.mark.parametrize(
    "feltnavn,forventet",
    [
        ("START_DT-FLYTTEPÅBUD", "start_dt_flyttepaabud"),
        ("MYNTXT-FØDESTED", "myntxt_foedested"),
        ("ÆGTEPNR", "aegtepnr"),
        ("PNR", "pnr"),
        ("ADRESSE_UUID", "adresse_uuid"),
    ],
)
def test_danish_characters_are_transliterated_in_column_names(feltnavn, forventet):
    """Without transliteration flyttepåbud becomes flyttep_bud — the word chopped in the middle."""
    assert kolonnenavn(feltnavn) == forventet


# --------------------------------------------------------------------- backlog


def test_backlog_is_the_deliveries_after_the_loaded_one_oldest_first(tmp_path):
    from DSI.csv_files.cpr import efterslaeb

    for navn in ("d260917.l208109", "d260915.l208109", "d260916.l208109", "noter.txt"):
        (tmp_path / navn).write_text("")
    assert efterslaeb(str(tmp_path), "20260915") == ["20260916", "20260917"]
    assert efterslaeb(str(tmp_path), None) == ["20260915", "20260916", "20260917"]
    assert efterslaeb(str(tmp_path), "20260917") == []


def test_control_total_is_interpreted_as_the_product_description_says():
    """The counter covers records excluding start/end; error records (after the end record) do
    not count; the ceiling 99999999 can only be checked downwards; no other tolerance."""
    from DSI.csv_files.cpr import TAELLER_LOFT, Leverance

    lev = Leverance("/tmp/d260916.l208109")
    assert lev.kontroltotal_differens is None and lev.kontroltotal_stemmer() is None
    lev.records_laest = 100
    lev.records_i_slutrecord = 100
    assert lev.kontroltotal_differens == 0 and lev.kontroltotal_stemmer() is True
    lev.records_i_slutrecord = 102
    assert lev.kontroltotal_differens == 2 and lev.kontroltotal_stemmer() is False
    lev.records_i_slutrecord = 99
    assert lev.kontroltotal_stemmer() is False
    # three error records after the end record: read 103, the counter says 100
    lev.records_laest = 103
    lev.pr_recordtype["910"] = 3
    lev.records_i_slutrecord = 100
    assert lev.records_i_taelleren == 100 and lev.kontroltotal_stemmer() is True
    # the ceiling: the file has more records than the counter can show
    lev.records_laest = TAELLER_LOFT + 5
    lev.pr_recordtype["910"] = 0
    lev.records_i_slutrecord = TAELLER_LOFT
    assert lev.kontroltotal_stemmer() is True
    lev.records_laest = TAELLER_LOFT - 1
    assert lev.kontroltotal_stemmer() is False


def test_unknown_codes_are_not_named_in_the_error_message():
    from DSI.csv_files.cpr import Leverance, kontroller_leverance

    leverance = Leverance("/et/sted/d260908.l208109")
    leverance.produktionsdato = "20260908"
    leverance.records_laest = 1000
    leverance.records_i_slutrecord = 1000
    leverance.ukendte_recordtyper["777"] = 50
    with pytest.raises(ValueError) as fejl:
        kontroller_leverance(leverance)
    assert "777" not in str(fejl.value) and "50 records" in str(fejl.value)
    assert leverance.ukendte_records == 50


def test_data_fremadrettet_is_counted_per_value(tmp_path):
    """996 says whether the person is delivered in future; only the count leaves the pass."""
    fremad = [
        _pad({"RECORDTYPE": "996", "PNR": PNR_A, "PERSONKREDS": "J", "ABONNEMENT": "N"}, "996"),
        _pad({"RECORDTYPE": "996", "PNR": PNR_B, "PERSONKREDS": "N", "ABONNEMENT": "N"}, "996"),
    ]
    sti = _skriv(
        tmp_path, "d260908.l208109", [_start(), _person(PNR_A), _person(PNR_B), *fremad, _slut(4)]
    )
    leverance, _ = laes_leverance(sti, "cp1252")
    assert dict(leverance.data_fremadrettet) == {
        "personkreds=J|abonnement=N": 1,
        "personkreds=N|abonnement=N": 1,
    }

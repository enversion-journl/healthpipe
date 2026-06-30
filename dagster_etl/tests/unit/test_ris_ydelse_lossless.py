# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Lossless parity tests for RIS_YDELSE -> FHIR Procedure.

Verifies that all 25 source columns are mapped to standard FHIR fields or
custom extensions. Raw XML content is preserved.

Coverage:
- Standard fields: identifier, status, code, category, subject, encounter,
  performedDateTime
- Extensions: episodeOfCare, modtidspunkt, risYdelseXml,
  risYdelseSourceMeta, risYdelseLocationMeta
- Edge cases: deleted records, missing XML, boolean flag robustness
"""

import pytest
from DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse import (
    _build_ris_location_meta_extensions,
    _build_ris_source_meta_extensions,
    _transform_ris_ydelse_to_fhir,
)
from utils.fhir.fhir_extensions import (
    EXTENSION_EPISODE_OF_CARE,
    EXTENSION_MODTIDSPUNKT,
    EXTENSION_RIS_YDELSE_LOCATION_META,
    EXTENSION_RIS_YDELSE_SOURCE_META,
    EXTENSION_RIS_YDELSE_XML,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_RIS_YDELSE,
    SYSTEM_SKS,
)
from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED

SAMPLE_XML = """<rekvisition>
  <service>
    <kode>UXRC00</kode>
    <tillaegskode>ZZ1234</tillaegskode>
  </service>
</rekvisition>"""

SAMPLE_XML_WITH_SKS_PREFIX = """<rekvisition>
  <service>
    <kode>SKS.a.proUXRC00</kode>
  </service>
</rekvisition>"""


def _full_row(**overrides: object) -> dict:
    """Build a complete RIS_YDELSE row with sensible defaults."""
    base = {
        "OBJEKT_ID": "RIS-001",
        "MODTIDSPUNKT": "2024-07-15T10:00:00",
        "SLETTET": 0,
        "PATIENT_ID": "PAT-200",
        "ENCOUNTER_ID": "ENC-400",
        "EPISODE_OF_CARE_ID": "EOC-500",
        "TILSTEDEVAERELSE_ID": "TILST-600",
        "XML": SAMPLE_XML,
        "STAMAFD_KODE": "STA-01",
        "STAMAFD_NAVN": "Røntgenafdeling",
        "STAMAFD_ORGKODE": "ORG-RIS",
        "UDFOERT_ADMENHED_KODE": "UAE-01",
        "UDFOERT_ADMENHED_NAVN": "CT-scanner 2",
        "UDFOERT_ADMENHED_ORGKODE": "ORG-CT",
        "UDFOERT_TIDSPUNKT": "2024-07-15T11:30:00",
        "OPDATERET_TID": "2024-07-15T12:00:00",
        "BESKED_ID": "MSG-789",
        "FILNAVN": "ris_20240715.xml",
        "STATE": "FINAL_REPORT",
        "TYPE": "RIS_SERVICE",
        "RTEE": "RTEE-001",
        "IGNORED": 0,
        "PERIOD_START": "2024-07-15T10:00:00",
        "PERIOD_END": "2024-07-15T12:00:00",
        "UDFOERT_ADMENHED_SORCODE": "SOR-CT-01",
        "STAMAFD_SORCODE": "SOR-STA-01",
        "LPR3_IGNORED": 0,
    }
    base.update(overrides)
    return base


class _MockContext:
    log = None

    class _Log:
        def info(self, *a, **kw):
            pass

        def warning(self, *a, **kw):
            pass

    def __init__(self):
        self.log = self._Log()


# =============================================================================
# Standard FHIR field tests
# =============================================================================


class TestStandardFields:
    def test_identifier(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row()], _MockContext())
        assert procs[0].identifier[0].system == SYSTEM_IFDB_RIS_YDELSE
        assert procs[0].identifier[0].value == "RIS-001:1"

    def test_subject_reference(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row()], _MockContext())
        assert procs[0].subject.identifier.system == SYSTEM_IFDB_PATIENT_ID
        assert procs[0].subject.identifier.value == "PAT-200"

    def test_encounter_reference(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row()], _MockContext())
        assert procs[0].encounter.identifier.system == SYSTEM_IFDB_OBJECT_ID
        assert procs[0].encounter.identifier.value == "ENC-400"

    def test_code_from_xml_service(self):
        procs = _transform_ris_ydelse_to_fhir(
            [_full_row(XML=SAMPLE_XML_WITH_SKS_PREFIX)], _MockContext()
        )
        code = procs[0].code
        assert code.coding[0].system == SYSTEM_SKS
        assert code.coding[0].code == "UXRC00"

    def test_state_filter_excludes_non_final_reports(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row(STATE="DRAFT_REPORT")], _MockContext())
        assert procs == []

    def test_strict_mode_raises_on_unknown_state(self):
        """DRAFT_REPORT is neither allowed nor known-non-final → strict raises."""
        with pytest.raises(ValueError, match="unsupported STATE"):
            _transform_ris_ydelse_to_fhir(
                [_full_row(STATE="DRAFT_REPORT")],
                _MockContext(),
                strict_source_validation=True,
            )

    @pytest.mark.parametrize("state", ["BOOKED", "ORDERED", "ORDERED_AWAITING_ACK", "EXECUTED"])
    def test_strict_mode_silently_skips_known_non_final_states(self, state: str):
        """Known-non-final states are part of routine source traffic and must
        NOT raise in strict mode — they are skipped silently, the same as
        they would be in tolerant mode. Strict mode is reserved for
        genuinely unknown STATE values so new source shapes surface loudly
        instead of being swallowed."""
        procs = _transform_ris_ydelse_to_fhir(
            [_full_row(STATE=state)],
            _MockContext(),
            strict_source_validation=True,
        )
        assert procs == []

    @pytest.mark.parametrize("state", ["BOOKED", "ORDERED", "ORDERED_AWAITING_ACK", "EXECUTED"])
    def test_tolerant_mode_also_skips_known_non_final_states(self, state: str):
        procs = _transform_ris_ydelse_to_fhir(
            [_full_row(STATE=state)],
            _MockContext(),
            strict_source_validation=False,
        )
        assert procs == []

    def test_category_radiology(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row()], _MockContext())
        cat = procs[0].category
        assert cat.coding[0].code == "radiology"

    def test_status_completed(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row()], _MockContext())
        assert procs[0].status == "completed"

    def test_status_deleted(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row(SLETTET=1)], _MockContext())
        assert procs[0].status == "entered-in-error"
        is_deleted = [e for e in procs[0].extension if e.url == EXTENSION_FHIR_IS_DELETED]
        assert len(is_deleted) == 1
        assert is_deleted[0].valueInteger == 1

    def test_performed_period_when_start_and_end(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row()], _MockContext())
        assert procs[0].performedPeriod is not None
        assert "2024-07-15" in str(procs[0].performedPeriod.start)
        assert "2024-07-15" in str(procs[0].performedPeriod.end)
        assert procs[0].performedDateTime is None

    def test_performed_datetime_when_no_period(self):
        procs = _transform_ris_ydelse_to_fhir(
            [_full_row(PERIOD_START=None, PERIOD_END=None)], _MockContext()
        )
        assert procs[0].performedDateTime is not None
        assert "2024-07-15" in str(procs[0].performedDateTime)
        assert procs[0].performedPeriod is None


# =============================================================================
# Extension tests
# =============================================================================


class TestExtensions:
    def test_episode_of_care(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row()], _MockContext())
        eoc = [e for e in procs[0].extension if e.url == EXTENSION_EPISODE_OF_CARE]
        assert len(eoc) == 1
        assert eoc[0].valueReference.identifier.value == "EOC-500"

    def test_modtidspunkt(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row()], _MockContext())
        mod = [e for e in procs[0].extension if e.url == EXTENSION_MODTIDSPUNKT]
        assert len(mod) == 1
        assert "2024-07-15" in str(mod[0].valueDateTime)

    def test_xml_preserved(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row()], _MockContext())
        xml_ext = [e for e in procs[0].extension if e.url == EXTENSION_RIS_YDELSE_XML]
        assert len(xml_ext) == 1
        assert "<rekvisition>" in xml_ext[0].valueString
        assert "<kode>UXRC00</kode>" in xml_ext[0].valueString

    def test_xml_absent_when_empty(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row(XML=None)], _MockContext())
        assert procs == []

    def test_strict_mode_raises_on_invalid_xml(self):
        with pytest.raises(ValueError, match="invalid XML"):
            _transform_ris_ydelse_to_fhir(
                [_full_row(XML="<rekvisition>")],
                _MockContext(),
                strict_source_validation=True,
            )

    def test_strict_mode_raises_when_no_service_groups_exist(self):
        with pytest.raises(ValueError, match="produced no service groups"):
            _transform_ris_ydelse_to_fhir(
                [_full_row(XML="<rekvisition><foo>bar</foo></rekvisition>")],
                _MockContext(),
                strict_source_validation=True,
            )

    def test_source_meta_complete(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row()], _MockContext())
        src = [e for e in procs[0].extension if e.url == EXTENSION_RIS_YDELSE_SOURCE_META]
        assert len(src) == 1
        urls = {se.url for se in src[0].extension}
        assert "tilstedevaerselseId" in urls
        assert "beskedId" in urls
        assert "filnavn" in urls
        assert "state" in urls
        assert "type" in urls
        assert "rtee" in urls
        assert "opdateretTid" in urls
        assert "periodStart" in urls
        assert "periodEnd" in urls
        assert "elementNummer" in urls
        assert "primaryCode" in urls
        assert "tillaegskode" in urls

    def test_source_meta_boolean_flags(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row()], _MockContext())
        src = [e for e in procs[0].extension if e.url == EXTENSION_RIS_YDELSE_SOURCE_META][0]
        ignored = [se for se in src.extension if se.url == "ignored"]
        assert len(ignored) == 1
        assert ignored[0].valueBoolean is False
        lpr3 = [se for se in src.extension if se.url == "lpr3Ignored"]
        assert len(lpr3) == 1
        assert lpr3[0].valueBoolean is False

    def test_source_meta_values(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row()], _MockContext())
        src = [e for e in procs[0].extension if e.url == EXTENSION_RIS_YDELSE_SOURCE_META][0]
        sub_map = {se.url: se for se in src.extension}
        assert sub_map["tilstedevaerselseId"].valueString == "TILST-600"
        assert sub_map["beskedId"].valueString == "MSG-789"
        assert sub_map["filnavn"].valueString == "ris_20240715.xml"
        assert sub_map["state"].valueString == "FINAL_REPORT"
        assert sub_map["type"].valueString == "RIS_SERVICE"
        assert sub_map["rtee"].valueString == "RTEE-001"
        assert sub_map["primaryCode"].valueString == "UXRC00"

    def test_location_meta_complete(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row()], _MockContext())
        loc = [e for e in procs[0].extension if e.url == EXTENSION_RIS_YDELSE_LOCATION_META]
        assert len(loc) == 1
        urls = {se.url for se in loc[0].extension}
        assert "stamafdelingKode" in urls
        assert "stamafdelingNavn" in urls
        assert "stamafdelingOrgkode" in urls
        assert "stamafdelingSorcode" in urls
        assert "udfoertEnhedKode" in urls
        assert "udfoertEnhedNavn" in urls
        assert "udfoertEnhedOrgkode" in urls
        assert "udfoertEnhedSorcode" in urls

    def test_location_meta_values(self):
        procs = _transform_ris_ydelse_to_fhir([_full_row()], _MockContext())
        loc = [e for e in procs[0].extension if e.url == EXTENSION_RIS_YDELSE_LOCATION_META][0]
        sub_map = {se.url: se for se in loc.extension}
        assert sub_map["stamafdelingNavn"].valueString == "Røntgenafdeling"
        assert sub_map["udfoertEnhedNavn"].valueString == "CT-scanner 2"
        assert sub_map["udfoertEnhedSorcode"].valueString == "SOR-CT-01"


# =============================================================================
# Edge cases
# =============================================================================


class TestEdgeCases:
    def test_all_optional_none(self):
        row = _full_row(
            ENCOUNTER_ID=None,
            EPISODE_OF_CARE_ID=None,
            XML=None,
            MODTIDSPUNKT=None,
            TILSTEDEVAERELSE_ID=None,
            STAMAFD_KODE=None,
            STAMAFD_NAVN=None,
            STAMAFD_ORGKODE=None,
            STAMAFD_SORCODE=None,
            UDFOERT_ADMENHED_KODE=None,
            UDFOERT_ADMENHED_NAVN=None,
            UDFOERT_ADMENHED_ORGKODE=None,
            UDFOERT_ADMENHED_SORCODE=None,
            BESKED_ID=None,
            FILNAVN=None,
            STATE=None,
            RTEE=None,
            IGNORED=None,
            PERIOD_START=None,
            PERIOD_END=None,
            LPR3_IGNORED=None,
            OPDATERET_TID=None,
        )
        procs = _transform_ris_ydelse_to_fhir([row], _MockContext())
        # No XML procedure groups -> filtered out
        assert procs == []

    def test_multiple_rows_deterministic(self):
        rows = [_full_row(OBJEKT_ID=f"RIS-{i}") for i in range(15)]
        procs = _transform_ris_ydelse_to_fhir(rows, _MockContext())
        assert len(procs) == 15
        for i, proc in enumerate(procs):
            assert proc.identifier[0].value == f"RIS-{i}:1"

    def test_multiple_services_create_multiple_procedures(self):
        xml = """<rekvisition>
        <service><kode>UXAA01</kode></service>
        <service><kode>UXAA02</kode><tillaegskode>ZZ9999</tillaegskode></service>
        </rekvisition>"""
        procs = _transform_ris_ydelse_to_fhir(
            [_full_row(OBJEKT_ID="RIS-MULTI", XML=xml)], _MockContext()
        )
        assert len(procs) == 2
        assert procs[0].identifier[0].value == "RIS-MULTI:1"
        assert procs[1].identifier[0].value == "RIS-MULTI:2"
        assert procs[0].code.coding[0].code == "UXAA01"
        assert procs[1].code.coding[0].code == "UXAA02"


# =============================================================================
# Builder isolation tests
# =============================================================================


class TestSourceMetaBuilder:
    def test_empty_produces_nothing(self):
        exts = _build_ris_source_meta_extensions({})
        assert exts == []

    def test_partial_produces_meta(self):
        exts = _build_ris_source_meta_extensions({"STATE": "active"})
        assert len(exts) == 1
        assert exts[0].extension[0].url == "state"


class TestLocationMetaBuilder:
    def test_empty_produces_nothing(self):
        exts = _build_ris_location_meta_extensions({})
        assert exts == []

    def test_full_location(self):
        row = {
            "STAMAFD_KODE": "K1",
            "STAMAFD_NAVN": "Afd",
            "STAMAFD_ORGKODE": "O1",
            "STAMAFD_SORCODE": "S1",
            "UDFOERT_ADMENHED_KODE": "UK1",
            "UDFOERT_ADMENHED_NAVN": "UN1",
            "UDFOERT_ADMENHED_ORGKODE": "UO1",
            "UDFOERT_ADMENHED_SORCODE": "US1",
        }
        exts = _build_ris_location_meta_extensions(row)
        assert len(exts) == 1
        assert len(exts[0].extension) == 8


# =============================================================================
# Column coverage
# =============================================================================


class TestColumnCoverage:
    EXTRACTED_COLUMNS = [
        "OBJEKT_ID",
        "MODTIDSPUNKT",
        "SLETTET",
        "PATIENT_ID",
        "ENCOUNTER_ID",
        "EPISODE_OF_CARE_ID",
        "TILSTEDEVAERELSE_ID",
        "XML",
        "STAMAFD_KODE",
        "STAMAFD_NAVN",
        "STAMAFD_ORGKODE",
        "UDFOERT_ADMENHED_KODE",
        "UDFOERT_ADMENHED_NAVN",
        "UDFOERT_ADMENHED_ORGKODE",
        "UDFOERT_TIDSPUNKT",
        "OPDATERET_TID",
        "BESKED_ID",
        "FILNAVN",
        "STATE",
        "TYPE",
        "RTEE",
        "IGNORED",
        "PERIOD_START",
        "PERIOD_END",
        "UDFOERT_ADMENHED_SORCODE",
        "STAMAFD_SORCODE",
        "LPR3_IGNORED",
    ]

    def test_all_columns_produce_output(self):
        """Every extraction column produces some FHIR output."""
        row = _full_row()
        procs = _transform_ris_ydelse_to_fhir([row], _MockContext())
        proc_json = procs[0].json()
        timestamp_cols = {
            "MODTIDSPUNKT",
            "UDFOERT_TIDSPUNKT",
            "OPDATERET_TID",
            "PERIOD_START",
            "PERIOD_END",
        }
        flag_cols = {"SLETTET", "IGNORED", "LPR3_IGNORED"}
        xml_cols = {"XML"}
        for col in self.EXTRACTED_COLUMNS:
            val = row.get(col)
            if val is None:
                continue
            if col in timestamp_cols or col in flag_cols or col in xml_cols:
                continue
            assert str(val) in proc_json, f"Column {col} with value {val} not found in output"

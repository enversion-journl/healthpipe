# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Lossless parity tests for ADMINISTRATIVE_PROCEDURE -> FHIR Procedure.

Verifies that all 28 source columns are mapped to standard FHIR fields or
custom extensions. No source data is dropped.

Coverage:
- Standard fields: identifier, status, code, subject, encounter, performedPeriod, bodySite
- Extensions: episodeOfCare, sourceCreatedAt, sourceModifiedAt,
  procedureSourceMeta, procedureLocationMeta
- Edge cases: deleted records, missing optional fields, boolean flag robustness
"""

import pytest
from DSA.FHIR.Procedure.Procedure_IFDB_AdminProc_dfm import (
    _build_location_meta_extensions,
    _build_source_meta_extensions,
    _is_true_flag,
    _transform_adm_procedure_to_fhir,
)
from utils.fhir.fhir_extensions import (
    EXTENSION_EPISODE_OF_CARE,
    EXTENSION_PROCEDURE_LOCATION_META,
    EXTENSION_PROCEDURE_SOURCE_META,
    EXTENSION_SOURCE_CREATED_AT,
    EXTENSION_SOURCE_MODIFIED_AT,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_ADMINISTRATIVE_PROCEDURE,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SKS,
)
from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED

pytestmark = pytest.mark.unit


def _full_row(**overrides: object) -> dict:
    """Build a complete ADMINISTRATIVE_PROCEDURE row with sensible defaults."""
    base = {
        "OBJECT_ID": "ADM-001",
        "PATIENT_ID": "PAT-100",
        "ENCOUNTER_ID": "ENC-200",
        "EPISODE_OF_CARE_ID": "EOC-300",
        "PERIOD_START": "2024-06-01T08:00:00",
        "PERIOD_END": "2024-06-01T09:30:00",
        "PROCEDURE_CODE": "KXBA10",
        "PROCEDURE_TEXT": "Knæprotese",
        "STATUS_CODE": "A",
        "STATUS_TEXT": "aktiv",
        "INDICATION_OF_SIDE_CODE": "TUL1",
        "INDICATION_OF_SIDE_TEXT": "Venstre",
        "USED_CONTRAST_CODE": "0",
        "USED_CONTRAST_TEXT": "Ingen kontrast",
        "STAFF_CATEGORY_CODE": "7170",
        "STAFF_CATEGORY_TEXT": "Ortopædkirurg",
        "LOCATION_CODE": "LOC-01",
        "LOCATION_NAME": "Operationsgang 3",
        "LOCATION_ORGCODE": "ORG-SHS",
        "LOCATION_SORCODE": "SOR-1234",
        "EPJI_CREATED_DATETIME": "2024-06-01T07:55:00",
        "EPJI_MODIFICATION_DATETIME": "2024-06-01T09:35:00",
        "IFDB_MODIFICATION_DATETIME": "2024-06-01T09:36:00",
        "DELETED": 0,
        "USER_ID": "USR-42",
        "USER_NAME": "DrHansen",
        "USER_ROLE": "Overlæge",
        "LPR3_IGNORED": 0,
        "USER_ROLE_NAME": "Ortopæd-overlæge",
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
    def test_identifier_from_object_id(self):
        rows = [_full_row()]
        procs = _transform_adm_procedure_to_fhir(rows, _MockContext())
        assert len(procs) == 1
        # Præcis ét identifier: aftagerfladen joiner (source_system,
        # source_identifier_value) som to uafhængige first() over listen.
        assert len(procs[0].identifier) == 1
        assert procs[0].identifier[0].system == SYSTEM_IFDB_ADMINISTRATIVE_PROCEDURE
        assert procs[0].identifier[0].value == "ADM-001"

    def test_subject_reference(self):
        procs = _transform_adm_procedure_to_fhir([_full_row()], _MockContext())
        subj = procs[0].subject
        assert subj.identifier.system == SYSTEM_IFDB_PATIENT_ID
        assert subj.identifier.value == "PAT-100"

    def test_encounter_reference(self):
        procs = _transform_adm_procedure_to_fhir([_full_row()], _MockContext())
        enc = procs[0].encounter
        assert enc.identifier.system == SYSTEM_IFDB_OBJECT_ID
        assert enc.identifier.value == "ENC-200"

    def test_code_sks(self):
        procs = _transform_adm_procedure_to_fhir(
            [_full_row(PROCEDURE_CODE="SKS.a.oprKXBA10")], _MockContext()
        )
        code = procs[0].code
        assert code.coding[0].system == SYSTEM_SKS
        assert code.coding[0].code == "KXBA10"
        assert code.coding[0].display == "Knæprotese"
        assert code.text == "Knæprotese"

    def test_status_completed(self):
        procs = _transform_adm_procedure_to_fhir([_full_row()], _MockContext())
        assert procs[0].status == "completed"

    def test_status_deleted(self):
        procs = _transform_adm_procedure_to_fhir([_full_row(DELETED=1)], _MockContext())
        assert procs[0].status == "entered-in-error"
        is_deleted = [e for e in procs[0].extension if e.url == EXTENSION_FHIR_IS_DELETED]
        assert len(is_deleted) == 1
        assert is_deleted[0].valueInteger == 1

    def test_performed_period_both_dates(self):
        procs = _transform_adm_procedure_to_fhir([_full_row()], _MockContext())
        period = procs[0].performedPeriod
        assert period is not None
        assert "2024-06-01" in str(period.start)
        assert "09:30" in str(period.end)

    def test_performed_period_start_only(self):
        procs = _transform_adm_procedure_to_fhir([_full_row(PERIOD_END=None)], _MockContext())
        period = procs[0].performedPeriod
        assert period is not None
        assert period.start is not None
        assert period.end is None

    def test_performed_period_absent_when_period_start_is_missing(self):
        procs = _transform_adm_procedure_to_fhir(
            [_full_row(PERIOD_START=None, PERIOD_END=None)], _MockContext()
        )
        period = procs[0].performedPeriod
        assert period is None

    def test_body_site_mapped(self):
        procs = _transform_adm_procedure_to_fhir([_full_row()], _MockContext())
        assert procs[0].bodySite is not None
        bs = procs[0].bodySite[0]
        assert bs.coding[0].code == "TUL1"
        assert bs.text == "Venstre"

    def test_body_site_absent_when_no_side(self):
        procs = _transform_adm_procedure_to_fhir(
            [_full_row(INDICATION_OF_SIDE_CODE=None, INDICATION_OF_SIDE_TEXT=None)],
            _MockContext(),
        )
        assert procs[0].bodySite is None


# =============================================================================
# Extension tests
# =============================================================================


class TestExtensions:
    def test_episode_of_care_extension(self):
        procs = _transform_adm_procedure_to_fhir([_full_row()], _MockContext())
        eoc_exts = [e for e in procs[0].extension if e.url == EXTENSION_EPISODE_OF_CARE]
        assert len(eoc_exts) == 1
        assert eoc_exts[0].valueReference.identifier.value == "EOC-300"

    def test_source_created_at(self):
        procs = _transform_adm_procedure_to_fhir([_full_row()], _MockContext())
        created = [e for e in procs[0].extension if e.url == EXTENSION_SOURCE_CREATED_AT]
        assert len(created) == 1
        assert "2024-06-01" in str(created[0].valueDateTime)

    def test_source_modified_at(self):
        procs = _transform_adm_procedure_to_fhir([_full_row()], _MockContext())
        modified = [e for e in procs[0].extension if e.url == EXTENSION_SOURCE_MODIFIED_AT]
        assert len(modified) == 1
        assert "2024-06-01" in str(modified[0].valueDateTime)

    def test_procedure_source_meta_complete(self):
        procs = _transform_adm_procedure_to_fhir([_full_row()], _MockContext())
        src_meta = [e for e in procs[0].extension if e.url == EXTENSION_PROCEDURE_SOURCE_META]
        assert len(src_meta) == 1
        sub_urls = {se.url for se in src_meta[0].extension}
        assert "statusCode" in sub_urls
        assert "statusText" in sub_urls
        assert "usedContrastCode" in sub_urls
        assert "staffCategoryCode" in sub_urls
        assert "staffCategoryText" in sub_urls
        assert "userId" in sub_urls
        assert "userName" in sub_urls
        assert "userRole" in sub_urls
        assert "userRoleName" in sub_urls
        assert "ifdbModifiedAt" in sub_urls

    def test_procedure_source_meta_values(self):
        procs = _transform_adm_procedure_to_fhir([_full_row()], _MockContext())
        src_meta = [e for e in procs[0].extension if e.url == EXTENSION_PROCEDURE_SOURCE_META][0]
        sub_map = {se.url: se for se in src_meta.extension}
        assert sub_map["statusCode"].valueString == "A"
        assert sub_map["userName"].valueString == "DrHansen"
        assert sub_map["staffCategoryCode"].valueString == "7170"

    def test_lpr3_ignored_boolean(self):
        procs = _transform_adm_procedure_to_fhir([_full_row()], _MockContext())
        src_meta = [e for e in procs[0].extension if e.url == EXTENSION_PROCEDURE_SOURCE_META][0]
        lpr3 = [se for se in src_meta.extension if se.url == "lpr3Ignored"]
        assert len(lpr3) == 1
        assert lpr3[0].valueBoolean is False

    def test_location_meta_complete(self):
        procs = _transform_adm_procedure_to_fhir([_full_row()], _MockContext())
        loc = [e for e in procs[0].extension if e.url == EXTENSION_PROCEDURE_LOCATION_META]
        assert len(loc) == 1
        sub_urls = {se.url for se in loc[0].extension}
        assert "code" in sub_urls
        assert "name" in sub_urls
        assert "orgCode" in sub_urls
        assert "sorCode" in sub_urls

    def test_location_meta_values(self):
        procs = _transform_adm_procedure_to_fhir([_full_row()], _MockContext())
        loc = [e for e in procs[0].extension if e.url == EXTENSION_PROCEDURE_LOCATION_META][0]
        sub_map = {se.url: se for se in loc.extension}
        assert sub_map["name"].valueString == "Operationsgang 3"
        assert sub_map["sorCode"].valueString == "SOR-1234"


# =============================================================================
# Edge cases
# =============================================================================


class TestEdgeCases:
    def test_all_optional_fields_none(self):
        row = _full_row(
            ENCOUNTER_ID=None,
            EPISODE_OF_CARE_ID=None,
            PERIOD_START=None,
            PERIOD_END=None,
            EPJI_CREATED_DATETIME=None,
            EPJI_MODIFICATION_DATETIME=None,
            INDICATION_OF_SIDE_CODE=None,
            INDICATION_OF_SIDE_TEXT=None,
            STATUS_CODE=None,
            STATUS_TEXT=None,
            USED_CONTRAST_CODE=None,
            USED_CONTRAST_TEXT=None,
            STAFF_CATEGORY_CODE=None,
            STAFF_CATEGORY_TEXT=None,
            LOCATION_CODE=None,
            LOCATION_NAME=None,
            LOCATION_ORGCODE=None,
            LOCATION_SORCODE=None,
            USER_ID=None,
            USER_NAME=None,
            USER_ROLE=None,
            USER_ROLE_NAME=None,
            IFDB_MODIFICATION_DATETIME=None,
            LPR3_IGNORED=None,
        )
        procs = _transform_adm_procedure_to_fhir([row], _MockContext())
        assert len(procs) == 1
        proc = procs[0]
        assert proc.status == "completed"
        assert proc.encounter is None
        assert proc.bodySite is None

    def test_multiple_rows_deterministic(self):
        rows = [_full_row(OBJECT_ID=f"ADM-{i}") for i in range(20)]
        procs = _transform_adm_procedure_to_fhir(rows, _MockContext())
        assert len(procs) == 20
        for i, proc in enumerate(procs):
            assert proc.identifier[0].value == f"ADM-{i}"

    def test_boolean_flag_variations(self):
        for truthy in [1, "1", True, "true", "yes", "Y"]:
            assert _is_true_flag(truthy) is True
        for falsy in [0, "0", False, "false", "no", None, ""]:
            assert _is_true_flag(falsy) is False


# =============================================================================
# Source meta builder isolation tests
# =============================================================================


class TestSourceMetaBuilder:
    def test_empty_row_no_meta(self):
        row = {k: None for k in ["STATUS_CODE", "STATUS_TEXT", "USER_ID"]}
        exts = _build_source_meta_extensions(row)
        assert exts == []

    def test_partial_row(self):
        row = {"STATUS_CODE": "C", "USER_ID": "U1"}
        exts = _build_source_meta_extensions(row)
        assert len(exts) == 1
        urls = {se.url for se in exts[0].extension}
        assert "statusCode" in urls
        assert "userId" in urls


class TestLocationMetaBuilder:
    def test_empty_location(self):
        exts = _build_location_meta_extensions({})
        assert exts == []

    def test_full_location(self):
        row = {
            "LOCATION_CODE": "L1",
            "LOCATION_NAME": "OP3",
            "LOCATION_ORGCODE": "O1",
            "LOCATION_SORCODE": "s1",
        }
        exts = _build_location_meta_extensions(row)
        assert len(exts) == 1
        assert len(exts[0].extension) == 4


# =============================================================================
# Column count verification
# =============================================================================


class TestColumnCoverage:
    EXTRACTED_COLUMNS = [
        "OBJECT_ID",
        "PATIENT_ID",
        "ENCOUNTER_ID",
        "EPISODE_OF_CARE_ID",
        "PERIOD_START",
        "PERIOD_END",
        "PROCEDURE_CODE",
        "PROCEDURE_TEXT",
        "STATUS_CODE",
        "STATUS_TEXT",
        "INDICATION_OF_SIDE_CODE",
        "INDICATION_OF_SIDE_TEXT",
        "USED_CONTRAST_CODE",
        "USED_CONTRAST_TEXT",
        "STAFF_CATEGORY_CODE",
        "STAFF_CATEGORY_TEXT",
        "LOCATION_CODE",
        "LOCATION_NAME",
        "LOCATION_ORGCODE",
        "LOCATION_SORCODE",
        "EPJI_CREATED_DATETIME",
        "EPJI_MODIFICATION_DATETIME",
        "IFDB_MODIFICATION_DATETIME",
        "DELETED",
        "USER_ID",
        "USER_NAME",
        "USER_ROLE",
        "LPR3_IGNORED",
        "USER_ROLE_NAME",
    ]

    def test_all_columns_produce_output(self):
        """Every extraction column produces some FHIR output."""
        row = _full_row()
        procs = _transform_adm_procedure_to_fhir([row], _MockContext())
        proc = procs[0]
        proc_json = proc.json()
        for col in self.EXTRACTED_COLUMNS:
            val = row.get(col)
            if val is None:
                continue
            assert str(val) in proc_json or col in {
                "DELETED",
                "LPR3_IGNORED",
                "EPJI_CREATED_DATETIME",
                "EPJI_MODIFICATION_DATETIME",
                "IFDB_MODIFICATION_DATETIME",
                "PERIOD_START",
                "PERIOD_END",
            }, f"Column {col} with value {val} not found in output"

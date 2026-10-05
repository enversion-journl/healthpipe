# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL contract tests for the ref.PatientToBorger, ref.Patient_Address_To_Borger,
and dim.Borger build assets.

These are string-level smoke tests: they inspect the Python source of each
asset and assert that critical SQL patterns are present. They catch typos
and accidental removal of load-bearing constructs without needing a live
MSSQL instance.

For end-to-end behavior, see the integration tests in
tests/integration/datawarehouse/ref/ (skipped by default, run in devcontainer/CI).
"""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

SRC_DIR = Path(__file__).resolve().parents[2] / "DSA" / "SQL" / "ref"


def _read(filename: str) -> str:
    return (SRC_DIR / filename).read_text()


class TestRefPatientBorgerSQLContract:
    """ref.PatientToBorger asset emits the right MERGE + cascade-delete SQL."""

    def test_uses_merge_into_ref_patient_borger(self):
        sql = _read("patient_borger.py")
        assert re.search(
            r"MERGE\s+INTO\s+\[ref\]\.\[PatientToBorger\]", sql
        ), "ref_patient_borger asset must MERGE INTO [ref].[PatientToBorger]"

    def test_sources_from_fhir_patient_with_identifier_filter(self):
        sql = _read("patient_borger.py")
        assert "FROM [fhir].[Patient]" in sql
        assert (
            "patient_ssn" in sql and "x_ecpr" in sql and "d_ecpr" in sql
        ), "MERGE source must project cpr/x_ecpr/d_ecpr from fhir.Patient"

    def test_insert_branch_assigns_zero_isdeleted(self):
        sql = _read("patient_borger.py")
        not_matched = re.search(
            r"WHEN NOT MATCHED BY TARGET.*?VALUES.*?\)",
            sql,
            flags=re.DOTALL,
        )
        assert not_matched, "Missing WHEN NOT MATCHED BY TARGET branch"
        assert "0," in not_matched.group(), "New rows should be inserted with dw_is_deleted = 0"

    def test_update_branch_resurrects_soft_deleted(self):
        sql = _read("patient_borger.py")
        matched = re.search(
            r"WHEN MATCHED AND.*?UPDATE SET.*?dw_is_deleted\s*=\s*0",
            sql,
            flags=re.DOTALL,
        )
        assert matched, (
            "WHEN MATCHED branch must clear dw_is_deleted (un-delete patients that "
            "reappear in the source)"
        )

    def test_cascade_soft_delete_for_missing_patients(self):
        sql = _read("patient_borger.py")
        assert re.search(
            r"UPDATE\s+tgt\s+SET\s+dw_is_deleted\s*=\s*1.*NOT EXISTS",
            sql,
            flags=re.DOTALL,
        ), "Missing cascade soft-delete for patient_ids removed from fhir.Patient"

    def test_uses_dfm_checkpoint_filter(self):
        sql = _read("patient_borger.py")
        assert "DfmManager" in sql
        assert (
            "last_updated >" in sql
        ), "MERGE source must filter on last_updated > checkpoint for delta"

    def test_uses_soft_delete_threshold(self):
        sql = _read("patient_borger.py")
        assert "check_soft_delete_threshold" in sql


class TestRefPatientAddressBorgerSQLContract:
    """ref.Patient_Address_To_Borger emits MERGE with patient-scoped delta."""

    def test_uses_merge_not_truncate(self):
        sql = _read("patient_address_borger.py")
        assert "MERGE INTO [ref].[Patient_Address_To_Borger]" in sql
        assert "TRUNCATE" not in sql, "Must use MERGE, not TRUNCATE"
        assert "DBCC CHECKIDENT" not in sql, "Must not RESEED (no more truncate pattern)"

    def test_merge_keyed_on_patient_id_and_address_period_start(self):
        sql = _read("patient_address_borger.py")
        assert re.search(
            r"ON tgt\.patient_id = src\.patient_id\s+AND tgt\.address_period_start = src\.address_period_start",
            sql,
        ), "MERGE key must be (patient_id, address_period_start)"

    def test_lead_over_address_period_start(self):
        sql = _read("patient_address_borger.py")
        assert re.search(
            r"LEAD\(\s*address_period_start\s*\)\s*OVER\s*\(\s*PARTITION BY patient_id",
            sql,
        ), "Missing LEAD(address_period_start) OVER (PARTITION BY patient_id ORDER BY ...)"

    def test_no_dateadd_minus_one_second(self):
        """Half-open interval: LEAD result must NOT be offset by -1 second."""
        sql = _read("patient_address_borger.py")
        assert not re.search(
            r"DATEADD\(\s*SECOND\s*,\s*-1\s*,\s*LEAD\(",
            sql,
        ), "Must NOT offset LEAD by -1 second — half-open [from, to) interval"

    def test_patient_scoped_delta_via_changed_patients(self):
        sql = _read("patient_address_borger.py")
        assert (
            "changed_patients_table" in sql
        ), "Must identify changed patient set for patient-scoped delta"
        assert "SELECT DISTINCT patient_id" in sql
        assert "FROM [fhir].[Patient]" in sql
        assert "FROM [fhir].[Patient_Address]" in sql
        assert "last_updated >" in sql

    def test_lead_source_excludes_deleted_fhir_addresses(self):
        sql = _read("patient_address_borger.py")
        assert re.search(
            r"INNER JOIN \[fhir\]\.\[Patient_Address\] pa\s+"
            r"ON pa\.patient_id = rpb\.patient_id\s+"
            r"AND pa\.dw_is_deleted = 0",
            sql,
        ), "LEAD source must ignore soft-deleted fhir.Patient_Address rows"

    def test_default_row_when_no_live_addresses(self):
        sql = _read("patient_address_borger.py")
        assert re.search(
            r"NOT EXISTS\s*\(\s*SELECT 1 FROM \[fhir\]\.\[Patient_Address\] pa\s+"
            r"WHERE pa\.patient_id = rpb\.patient_id\s+"
            r"AND pa\.dw_is_deleted = 0",
            sql,
        ), "Default 1900-row must treat deleted-only patients as having no address"

    def test_soft_delete_scoped_to_changed_patients(self):
        sql = _read("patient_address_borger.py")
        assert re.search(
            r"NOT MATCHED BY SOURCE.*patient_id IN.*\{changed_patients_table\}.*dw_is_deleted = 0",
            sql,
            flags=re.DOTALL,
        ), (
            "NOT MATCHED BY SOURCE must be scoped to changed_patient_set "
            "and only target non-deleted rows"
        )

    def test_first_row_forced_to_min_sentinel(self):
        sql = _read("patient_address_borger.py")
        assert re.search(
            r"MIN\(address_period_start\)\s+OVER\s+\(\s*PARTITION BY patient_id",
            sql,
        ), "First row per patient must be forced to '1900-01-01' via MIN() OVER"
        assert "'1900-01-01'" in sql

    def test_last_row_uses_end_of_time_sentinel(self):
        sql = _read("patient_address_borger.py")
        assert (
            "'9999-12-31 23:59:59'" in sql
        ), "Last row per patient must have dw_valid_to = '9999-12-31 23:59:59'"

    def test_dedup_by_patient_period_with_last_updated_tiebreak(self):
        sql = _read("patient_address_borger.py")
        assert re.search(
            r"ROW_NUMBER\(\)\s+OVER\s*\(\s*PARTITION BY patient_id,\s*address_period_start",
            sql,
        ), "Must dedup on (patient_id, address_period_start)"
        assert re.search(
            r"ORDER BY last_updated DESC",
            sql,
        ), "Dedup tiebreak must be last_updated DESC"

    def test_patients_without_address_get_default_row(self):
        sql = _read("patient_address_borger.py")
        assert "UNION ALL" in sql
        assert re.search(
            r"NOT EXISTS\s*\(\s*SELECT 1 FROM \[fhir\]\.\[Patient_Address\]",
            sql,
        ), "Must synthesize default row for patients without any fhir.Patient_Address entry"

    def test_checkpoint_covers_patient_and_address_clocks(self):
        sql = _read("patient_address_borger.py")
        assert "MAX(last_updated)" in sql
        assert "FROM [fhir].[Patient]" in sql
        assert "FROM [fhir].[Patient_Address]" in sql

    def test_dw_iscurrent_set_from_validto_sentinel(self):
        sql = _read("patient_address_borger.py")
        assert re.search(
            r"dw_valid_to\s*=\s*CAST\('9999-12-31 23:59:59' AS datetime2\(0\)\).*?THEN 1",
            sql,
            flags=re.DOTALL,
        ), "dw_is_current must be derived from dw_valid_to = end-of-time sentinel"

    def test_soft_delete_clears_is_current(self):
        sql = _read("patient_address_borger.py")
        not_matched = re.search(
            r"WHEN NOT MATCHED BY SOURCE.*?OUTPUT",
            sql,
            flags=re.DOTALL,
        )
        assert not_matched, "Missing WHEN NOT MATCHED BY SOURCE branch"
        assert re.search(
            r"dw_is_current\s*=\s*0",
            not_matched.group(),
        ), "Soft-deleted rows must set dw_is_current = 0"

    def test_clears_current_before_merge(self):
        sql = _read("patient_address_borger.py")
        assert re.search(
            r"UPDATE tgt\s+SET tgt\.dw_is_current = 0.*?"
            r"FROM \[ref\]\.\[Patient_Address_To_Borger\].*?"
            r"MERGE INTO \[ref\]\.\[Patient_Address_To_Borger\]",
            sql,
            flags=re.DOTALL,
        ), (
            "Must clear dw_is_current for changed patients before MERGE "
            "(filtered unique index is checked per-row)"
        )

    def test_uses_dfm(self):
        sql = _read("patient_address_borger.py")
        assert "DfmManager" in sql

    def test_uses_soft_delete_threshold(self):
        sql = _read("patient_address_borger.py")
        assert "check_soft_delete_threshold" in sql


class TestDimBorgerSQLContract:
    """dim.Borger uses MERGE keyed on dw_sk_Borger from ref tables."""

    @staticmethod
    def _dim_sql() -> str:
        path = Path(__file__).resolve().parents[2] / "DSA" / "SQL" / "dim" / "borger.py"
        return path.read_text()

    def test_uses_merge_not_truncate(self):
        sql = self._dim_sql()
        # The MERGE targets a scoped updatable CTE over dim.Borger (sk-in-source
        # OR ek-in-changed) so NOT MATCHED BY SOURCE does not scan all rows;
        # branch semantics are unchanged.
        assert "MERGE INTO ScopedTarget" in sql
        assert re.search(
            r"WITH ScopedTarget AS \(\s+SELECT \* FROM \[dim\]\.\[Borger\]",
            sql,
        ), "ScopedTarget must be a plain filtered projection of dim.Borger"
        assert "TRUNCATE" not in sql, "Must use MERGE, not TRUNCATE"
        assert "IDENTITY_INSERT" not in sql, "No IDENTITY_INSERT needed (IDENTITY dropped)"

    def test_merge_keyed_on_dw_sk_borger(self):
        sql = self._dim_sql()
        assert re.search(
            r"ON tgt\.dw_sk_Borger = src\.dw_sk_Borger",
            sql,
        ), "MERGE key must be dw_sk_Borger (carried from ref.Patient_Address_To_Borger)"

    def test_sentinel_guard_on_soft_delete_branch(self):
        """NOT MATCHED BY SOURCE must exclude sentinel rows (dw_sk_Borger > 0)."""
        sql = self._dim_sql()
        assert re.search(
            r"NOT MATCHED BY SOURCE.*dw_sk_Borger > 0",
            sql,
            flags=re.DOTALL,
        ), "Soft-delete branch must guard sentinels with dw_sk_Borger > 0"

    def test_joins_fhir_and_ref_tables(self):
        sql = self._dim_sql()
        assert "[fhir].[Patient]" in sql
        assert "[fhir].[Patient_Address]" in sql
        assert "[ref].[PatientToBorger]" in sql
        assert "[ref].[Patient_Address_To_Borger]" in sql

    def test_filters_soft_deleted_ref_rows(self):
        sql = self._dim_sql()
        assert re.search(
            r"\[ref\]\.\[PatientToBorger\]\s+rpb.*rpb\.dw_is_deleted\s*=\s*0",
            sql,
            flags=re.DOTALL,
        ), "Must filter out soft-deleted patients (rpb.dw_is_deleted = 0)"

    def test_fhir_address_join_skips_deleted_rows(self):
        sql = self._dim_sql()
        assert sql.count("pa.dw_is_deleted = 0") >= 2, (
            "CurrentAddress and SourceRows must not paint attributes from "
            "soft-deleted fhir.Patient_Address rows"
        )

    def test_does_not_reference_legacy_stg_or_map_tables(self):
        sql = self._dim_sql()
        assert "[stg].[Borger]" not in sql, "Legacy stg.Borger reference still present"
        assert "[map].[Borger]" not in sql, "Legacy map.Borger reference still present"

    def test_current_address_denormalization(self):
        sql = self._dim_sql()
        assert "CurrentAddress" in sql
        assert "dw_is_current = 1" in sql, "Aktuel-adresse-denorm must filter on dw_is_current = 1"

    def test_uses_soft_delete_threshold(self):
        sql = self._dim_sql()
        assert "check_soft_delete_threshold" in sql

    def test_uses_dfm_for_delta(self):
        sql = self._dim_sql()
        assert "DfmManager" in sql
        assert (
            "changed_patients_table" in sql
        ), "Must identify changed patient set for patient-scoped delta"

    def test_soft_delete_scoped_to_changed_patients(self):
        sql = self._dim_sql()
        assert re.search(
            r"NOT MATCHED BY SOURCE.*\{changed_patients_table\}",
            sql,
            flags=re.DOTALL,
        ), (
            "NOT MATCHED BY SOURCE must be scoped to changed patients "
            "so unchanged patients are not accidentally soft-deleted"
        )

    def test_soft_delete_clears_is_current(self):
        sql = self._dim_sql()
        not_matched = re.search(
            r"WHEN NOT MATCHED BY SOURCE.*?OUTPUT",
            sql,
            flags=re.DOTALL,
        )
        assert not_matched, "Missing WHEN NOT MATCHED BY SOURCE branch"
        assert re.search(
            r"dw_is_current\s*=\s*0",
            not_matched.group(),
        ), "Soft-deleted rows must set dw_is_current = 0"

    def test_deleted_patient_is_not_current(self):
        sql = self._dim_sql()
        assert re.search(
            r"WHEN ISNULL\(p\.dw_is_deleted, 0\) = 1 THEN CAST\(0 AS BIT\)",
            sql,
        ), (
            "fhir.Patient.dw_is_deleted = 1 must force dw_is_current = 0 "
            "(CHECK CK_Dim_Borger_CurrentNotDeleted)"
        )

    def test_clears_current_before_merge(self):
        sql = self._dim_sql()
        assert re.search(
            r"UPDATE tgt\s+SET tgt\.dw_is_current = 0.*?"
            r"FROM \[dim\]\.\[Borger\].*?"
            r"MERGE INTO ScopedTarget",
            sql,
            flags=re.DOTALL,
        ), (
            "Must clear dw_is_current for changed borgere before MERGE "
            "(filtered unique index is checked per-row)"
        )

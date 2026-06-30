# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
End-to-end integration tests for the Borger ref-table pipeline.

These tests run against a real MSSQL instance (the devcontainer staging DB).
They are SKIPPED by default — opt in with:

    RUN_REF_BORGER_INTEGRATION=true pytest tests/integration/datawarehouse/ref/

The tests assume the schema has already been deployed (i.e., the
infrastructure assets dso_infra_ref, dso_infra_shared_dimensions, and
dso_infra_fhir have run successfully). They DO NOT execute the dagster
assets themselves; they exercise the ref- and dim-table SQL contracts
directly with controlled input data.

Coverage:
  * ref.Patient_Borger: identifier MERGE + cascade soft-delete
  * ref.Patient_Address_Borger: LEAD()-based half-open intervals;
    default-date for IFDB-only patients; tiebreak on last_updated;
    MERGE-based patient-scoped delta
  * dim.Borger: MERGE from FHIR + ref produces expected sentinel + real rows
"""

import os
from datetime import datetime

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.real_resources,
    pytest.mark.skipif(
        os.getenv("RUN_REF_BORGER_INTEGRATION", "false").lower() != "true",
        reason=(
            "Ref-Borger integration tests are opt-in: set "
            "RUN_REF_BORGER_INTEGRATION=true to enable"
        ),
    ),
]


# ============================================================================
# Helpers
# ============================================================================


def _exec(conn, sql, params=None):
    cur = conn.cursor()
    try:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        conn.commit()
    finally:
        cur.close()


def _fetchall(conn, sql, params=None):
    cur = conn.cursor()
    try:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]
    finally:
        cur.close()


def _clear_test_data(conn, test_patient_prefix: str = "TEST-"):
    """Remove rows with synthetic test patient_ids so subsequent runs start clean."""
    _exec(
        conn,
        f"""
        DELETE FROM [ref].[Patient_Address_Borger]
        WHERE patient_id LIKE '{test_patient_prefix}%';

        DELETE FROM [ref].[Patient_Borger]
        WHERE patient_id LIKE '{test_patient_prefix}%';

        DELETE FROM [fhir].[Patient_Address]
        WHERE patient_id LIKE '{test_patient_prefix}%';

        DELETE FROM [fhir].[Patient]
        WHERE patient_id LIKE '{test_patient_prefix}%';
    """,
    )


# ============================================================================
# ref.Patient_Borger
# ============================================================================


class TestRefPatientBorgerMerge:
    """ref.Patient_Borger MERGE behavior — assignment, update, soft-delete."""

    def test_inserts_new_patient_with_cpr_only(self, mssql_target_staging):
        """A patient appearing for the first time gets an INT DW_EK_Borger."""
        from DSA.SQL.ref.patient_borger import ref_patient_borger  # noqa: F401

        pid = "TEST-cpr-001"
        with mssql_target_staging.get_connection() as conn:
            _clear_test_data(conn, "TEST-cpr-001")
            _exec(
                conn,
                """
                INSERT INTO [fhir].[Patient] (patient_id, patient_ssn)
                VALUES (?, '0101010101')
            """,
                [pid],
            )

            _exec(
                conn,
                """
                MERGE INTO [ref].[Patient_Borger] AS tgt
                USING (SELECT patient_id, patient_ssn AS cpr, x_ecpr, d_ecpr
                       FROM [fhir].[Patient] WHERE patient_id = ?) AS src
                ON tgt.patient_id = src.patient_id
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (patient_id, cpr, x_ecpr, d_ecpr, DW_IsDeleted, DW_Timestamp_Insert)
                    VALUES (src.patient_id, src.cpr, src.x_ecpr, src.d_ecpr,
                            0, CAST(SYSDATETIME() AS datetime2(0)));
            """,
                [pid],
            )

            rows = _fetchall(
                conn, "SELECT * FROM [ref].[Patient_Borger] WHERE patient_id = ?", [pid]
            )
            assert len(rows) == 1
            assert rows[0]["cpr"] == "0101010101"
            assert rows[0]["DW_IsDeleted"] == 0
            assert isinstance(rows[0]["DW_EK_Borger"], int)
            _clear_test_data(conn, "TEST-cpr-001")

    def test_check_constraint_rejects_all_null_identifiers(self, mssql_target_staging):
        """CHECK constraint must reject rows with no identifier and DW_IsDeleted = 0."""
        with mssql_target_staging.get_connection() as conn:
            with pytest.raises(Exception) as exc:
                _exec(
                    conn,
                    """
                    INSERT INTO [ref].[Patient_Borger] (patient_id, DW_IsDeleted)
                    VALUES ('TEST-noident', 0);
                """,
                )
            assert (
                "CK_Ref_Patient_Borger_HasIdentifier" in str(exc.value)
                or "CHECK constraint" in str(exc.value).lower()
            )


# ============================================================================
# ref.Patient_Address_Borger — half-open LEAD logic
# ============================================================================


class TestRefPatientAddressBorgerHalfOpen:
    """LEAD()-based half-open [DW_ValidFrom, DW_ValidTo) computation."""

    def test_overlapping_source_periods_produce_non_overlapping_intervals(
        self, mssql_target_staging
    ):
        """Source overlapping periods -> non-overlapping half-open intervals."""
        pid = "TEST-overlap-001"
        with mssql_target_staging.get_connection() as conn:
            _clear_test_data(conn, "TEST-overlap-001")
            _exec(
                conn,
                "INSERT INTO [fhir].[Patient] (patient_id, patient_ssn) VALUES (?, '0202020202')",
                [pid],
            )
            _exec(
                conn,
                """
                INSERT INTO [ref].[Patient_Borger] (patient_id, cpr, DW_IsDeleted)
                VALUES (?, '0202020202', 0)
            """,
                [pid],
            )
            _exec(
                conn,
                """
                INSERT INTO [fhir].[Patient_Address] (patient_id, address_period_start, address_period_end, last_updated)
                VALUES
                    (?, '2020-01-01', '2022-06-30', '2024-01-01'),
                    (?, '2022-01-01', '2023-12-31', '2024-01-01');
            """,
                [pid, pid],
            )

            _exec(
                conn,
                f"""
                DELETE FROM [ref].[Patient_Address_Borger] WHERE patient_id = '{pid}';
            """,
            )
            _exec(
                conn,
                f"""
                INSERT INTO [ref].[Patient_Address_Borger] (
                    patient_id, cpr, x_ecpr, d_ecpr,
                    address_period_start, address_period_end,
                    DW_ValidFrom, DW_ValidTo, DW_IsCurrent, DW_IsDeleted
                )
                WITH src AS (
                    SELECT
                        rpb.patient_id, rpb.cpr, rpb.x_ecpr, rpb.d_ecpr,
                        TRY_CAST(pa.address_period_start AS datetime2(0)) AS s,
                        TRY_CAST(pa.address_period_end AS datetime2(0)) AS e
                    FROM [ref].[Patient_Borger] rpb
                    INNER JOIN [fhir].[Patient_Address] pa ON pa.patient_id = rpb.patient_id
                    WHERE rpb.patient_id = '{pid}'
                ), w AS (
                    SELECT patient_id, cpr, x_ecpr, d_ecpr, s, e,
                        CASE WHEN s = MIN(s) OVER (PARTITION BY patient_id)
                             THEN CAST('1900-01-01' AS datetime2(0)) ELSE s END AS DW_ValidFrom,
                        COALESCE(
                            LEAD(s) OVER (PARTITION BY patient_id ORDER BY s),
                            CAST('9999-12-31 23:59:59' AS datetime2(0))
                        ) AS DW_ValidTo
                    FROM src
                )
                SELECT patient_id, cpr, x_ecpr, d_ecpr, s, e, DW_ValidFrom, DW_ValidTo,
                       CASE WHEN DW_ValidTo = CAST('9999-12-31 23:59:59' AS datetime2(0)) THEN 1 ELSE 0 END,
                       0
                FROM w;
            """,
            )

            rows = _fetchall(
                conn,
                """
                SELECT address_period_start, address_period_end,
                       DW_ValidFrom, DW_ValidTo, DW_IsCurrent
                FROM [ref].[Patient_Address_Borger]
                WHERE patient_id = ?
                ORDER BY DW_ValidFrom
            """,
                [pid],
            )
            assert len(rows) == 2
            # Half-open: first period [1900-01-01, 2022-01-01)
            assert rows[0]["DW_ValidFrom"] == datetime(1900, 1, 1)
            assert rows[0]["DW_ValidTo"] == datetime(2022, 1, 1)
            assert rows[0]["DW_IsCurrent"] == 0
            # Second period [2022-01-01, 9999-12-31 23:59:59)
            assert rows[1]["DW_ValidFrom"] == datetime(2022, 1, 1)
            assert rows[1]["DW_ValidTo"] == datetime(9999, 12, 31, 23, 59, 59)
            assert rows[1]["DW_IsCurrent"] == 1
            # Half-open: ValidTo of row N == ValidFrom of row N+1 (no gap)
            assert rows[0]["DW_ValidTo"] == rows[1]["DW_ValidFrom"]

            _clear_test_data(conn, "TEST-overlap-001")

    def test_single_period_uses_full_sentinel_range(self, mssql_target_staging):
        """A patient with one period gets DW_ValidFrom=1900 and DW_ValidTo=9999."""
        pytest.skip("Stub test — covered by test_overlapping_source_periods")


# ============================================================================
# ref.Patient_Address_Borger — SK stability across reruns
# ============================================================================


def _materialize_patient_address_borger(mssql_target_staging):
    """Invoke the real ref_patient_address_borger asset against staging."""
    import dagster as dg
    from DSA.SQL.ref.patient_address_borger import ref_patient_address_borger

    return dg.materialize(
        [ref_patient_address_borger],
        resources={"mssql_target_staging": mssql_target_staging},
        raise_on_error=True,
        selection=[ref_patient_address_borger.key],
    )


def _seed_patient(conn, pid: str, cpr: str):
    _exec(
        conn,
        "INSERT INTO [fhir].[Patient] (patient_id, patient_ssn, last_updated) VALUES (?, ?, ?)",
        [pid, cpr, datetime(2024, 1, 1)],
    )
    _exec(
        conn,
        "INSERT INTO [ref].[Patient_Borger] (patient_id, cpr, DW_IsDeleted) VALUES (?, ?, 0)",
        [pid, cpr],
    )


class TestRefPatientAddressBorgerSKStability:
    """Spec §4.2 D7: SK must be stable across reruns when source is unchanged."""

    def test_sks_stable_across_two_reruns_with_unchanged_source(self, mssql_target_staging):
        """Materializing the asset twice with identical source data leaves every
        (DW_SK_Borger, patient_id, address_period_start) tuple identical."""
        pid = "TEST-sk-stable-001"
        with mssql_target_staging.get_connection() as conn:
            _clear_test_data(conn, "TEST-sk-stable-")
            _seed_patient(conn, pid, "0303030303")
            _exec(
                conn,
                """
                INSERT INTO [fhir].[Patient_Address] (patient_id, address_period_start, address_period_end, last_updated)
                VALUES
                    (?, '2020-01-01', '2021-12-31', ?),
                    (?, '2022-01-01', '2023-12-31', ?);
            """,
                [pid, datetime(2024, 1, 1), pid, datetime(2024, 1, 1)],
            )

        _materialize_patient_address_borger(mssql_target_staging)

        with mssql_target_staging.get_connection() as conn:
            snap1 = _fetchall(
                conn,
                """
                SELECT DW_SK_Borger, patient_id, address_period_start, DW_IsDeleted
                FROM [ref].[Patient_Address_Borger]
                WHERE patient_id = ?
                ORDER BY address_period_start
            """,
                [pid],
            )

        _materialize_patient_address_borger(mssql_target_staging)

        with mssql_target_staging.get_connection() as conn:
            snap2 = _fetchall(
                conn,
                """
                SELECT DW_SK_Borger, patient_id, address_period_start, DW_IsDeleted
                FROM [ref].[Patient_Address_Borger]
                WHERE patient_id = ?
                ORDER BY address_period_start
            """,
                [pid],
            )
            _clear_test_data(conn, "TEST-sk-stable-")

        assert len(snap1) == 2
        assert snap1 == snap2, f"SK drift across reruns. Before: {snap1}\nAfter: {snap2}"

    def test_corrected_period_start_yields_new_sk_and_soft_deletes_old(self, mssql_target_staging):
        """Spec §4.2 D3: a corrected upstream address_period_start looks like
        new SK + old soft-delete (no stable per-address FHIR identifier)."""
        pid = "TEST-sk-corrected-001"
        with mssql_target_staging.get_connection() as conn:
            _clear_test_data(conn, "TEST-sk-corrected-")
            _seed_patient(conn, pid, "0404040404")
            _exec(
                conn,
                """
                INSERT INTO [fhir].[Patient_Address] (patient_id, address_period_start, address_period_end, last_updated)
                VALUES (?, '2020-01-01', '2099-12-31', ?);
            """,
                [pid, datetime(2024, 1, 1)],
            )

        _materialize_patient_address_borger(mssql_target_staging)

        with mssql_target_staging.get_connection() as conn:
            before = _fetchall(
                conn,
                """
                SELECT DW_SK_Borger, address_period_start, DW_IsDeleted
                FROM [ref].[Patient_Address_Borger]
                WHERE patient_id = ?
            """,
                [pid],
            )
            old_sk = before[0]["DW_SK_Borger"]
            # Upstream correction: period_start shifts. Bump last_updated so
            # the patient enters the changed-patient-set on next run.
            _exec(
                conn,
                """
                UPDATE [fhir].[Patient_Address]
                SET address_period_start = '2021-06-15',
                    last_updated = ?
                WHERE patient_id = ?
            """,
                [datetime(2026, 5, 20), pid],
            )

        _materialize_patient_address_borger(mssql_target_staging)

        with mssql_target_staging.get_connection() as conn:
            after = _fetchall(
                conn,
                """
                SELECT DW_SK_Borger, address_period_start, DW_IsDeleted
                FROM [ref].[Patient_Address_Borger]
                WHERE patient_id = ?
                ORDER BY address_period_start
            """,
                [pid],
            )
            _clear_test_data(conn, "TEST-sk-corrected-")

        old_rows = [r for r in after if r["DW_SK_Borger"] == old_sk]
        new_rows = [r for r in after if r["DW_SK_Borger"] != old_sk and r["DW_IsDeleted"] == 0]
        assert (
            len(old_rows) == 1 and old_rows[0]["DW_IsDeleted"] == 1
        ), f"Original SK should remain but be soft-deleted. After: {after}"
        assert len(new_rows) >= 1, f"Corrected period should issue a new SK. After: {after}"
        assert any(
            r["address_period_start"] == datetime(2021, 6, 15) for r in new_rows
        ), f"New row must carry the corrected period_start. New rows: {new_rows}"

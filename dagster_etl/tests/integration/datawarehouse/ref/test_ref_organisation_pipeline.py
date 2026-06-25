# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
End-to-end integration tests for the Organisation ref-table pipeline.

These tests run against a real MSSQL instance (the devcontainer staging DB).
They are SKIPPED by default — opt in with:

    RUN_REF_ORGANISATION_INTEGRATION=true pytest tests/integration/datawarehouse/ref/

The tests assume the schema has already been deployed (i.e. staging_infra_ref /
staging_infra_organisation and the seeding migration 20260618_ref_organisation
have run). They exercise the ref- and dim-table SQL contracts directly with
controlled input data — they do NOT execute the dagster assets.

Coverage:
  * ref.Organisation: organization_id MERGE + cascade soft-delete + CHECK
  * dim.Organisation: Type 1 MERGE from fhir.Organization + ref.Organisation,
    including self-referencing parent resolution via part_of_id
  * DW_EK continuity: a reconciliation check comparing dim.Organisation row
    counts and verifying every live SOR org in fhir.Organization resolves a
    DW_EK (the OKOSTED rows are already gone on development, so the legacy
    vs. new comparison reduces to "all SOR orgs present").

Part of US #28193.
"""

import os

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.real_resources,
    pytest.mark.skipif(
        os.getenv("RUN_REF_ORGANISATION_INTEGRATION", "false").lower() != "true",
        reason=(
            "Ref-Organisation integration tests are opt-in: set "
            "RUN_REF_ORGANISATION_INTEGRATION=true to enable"
        ),
    ),
]

TEST_PREFIX = "TEST-ORG-"


# ============================================================================
# Helpers
# ============================================================================


def _exec(conn, sql, params=None):
    cur = conn.cursor()
    try:
        cur.execute(sql, params) if params is not None else cur.execute(sql)
        conn.commit()
    finally:
        cur.close()


def _fetchall(conn, sql, params=None):
    cur = conn.cursor()
    try:
        cur.execute(sql, params) if params is not None else cur.execute(sql)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]
    finally:
        cur.close()


def _scalar(conn, sql, params=None):
    cur = conn.cursor()
    try:
        cur.execute(sql, params) if params is not None else cur.execute(sql)
        return cur.fetchone()[0]
    finally:
        cur.close()


def _clear_test_data(conn, prefix: str = TEST_PREFIX):
    """Remove synthetic test rows so reruns start clean."""
    _exec(
        conn,
        f"""
        DELETE FROM [dim].[Organisation]  WHERE Code LIKE '{prefix}%';
        DELETE FROM [ref].[Organisation]  WHERE organization_id LIKE '{prefix}%';
        DELETE FROM [fhir].[Organization] WHERE organization_id LIKE '{prefix}%';
    """,
    )


# The MERGE/soft-delete SQL the ref_organisation asset runs (kept in sync with
# DSA/SQL/ref/organisation.py — the contract test guards the asset source).
_REF_MERGE = """
    MERGE INTO [ref].[Organisation] AS tgt
    USING (
        SELECT f.organization_id, f.identifier_value_sor AS sor_code
        FROM [fhir].[Organization] f
        WHERE f.identifier_value_sor IS NOT NULL
          AND f.organization_id LIKE ?
    ) AS src
    ON tgt.organization_id = src.organization_id
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (organization_id, sor_code, DW_IsDeleted, DW_Timestamp_Insert)
        VALUES (src.organization_id, src.sor_code, 0, CAST(SYSDATETIME() AS datetime2(0)))
    WHEN MATCHED AND (ISNULL(tgt.sor_code,'') <> ISNULL(src.sor_code,'') OR tgt.DW_IsDeleted = 1) THEN
        UPDATE SET sor_code = src.sor_code, DW_IsDeleted = 0,
                   DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0));
"""

_REF_SOFT_DELETE = """
    UPDATE tgt SET DW_IsDeleted = 1, DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))
    FROM [ref].[Organisation] tgt
    WHERE tgt.DW_IsDeleted = 0
      AND tgt.organization_id LIKE ?
      AND NOT EXISTS (SELECT 1 FROM [fhir].[Organization] f WHERE f.organization_id = tgt.organization_id);
"""


# ============================================================================
# ref.Organisation
# ============================================================================


class TestRefOrganisationMerge:
    """ref.Organisation MERGE behavior — assignment, update, soft-delete."""

    def test_inserts_new_org_with_int_dw_ek(self, mssql_target_staging):
        oid = f"{TEST_PREFIX}001"
        with mssql_target_staging.get_connection() as conn:
            _clear_test_data(conn)
            _exec(
                conn,
                """
                INSERT INTO [fhir].[Organization] (organization_id, name, active, identifier_value_sor)
                VALUES (?, 'Test Org 1', 'true', '999001')
            """,
                [oid],
            )
            _exec(conn, _REF_MERGE, [f"{TEST_PREFIX}%"])

            rows = _fetchall(
                conn, "SELECT * FROM [ref].[Organisation] WHERE organization_id = ?", [oid]
            )
            assert len(rows) == 1
            assert rows[0]["sor_code"] == "999001"
            assert rows[0]["DW_IsDeleted"] == 0
            assert isinstance(rows[0]["DW_EK_Organisation"], int)
            _clear_test_data(conn)

    def test_cascade_soft_delete_when_org_leaves_fhir(self, mssql_target_staging):
        """An org removed from fhir.Organization is soft-deleted, keeping its DW_EK."""
        oid = f"{TEST_PREFIX}002"
        with mssql_target_staging.get_connection() as conn:
            _clear_test_data(conn)
            _exec(
                conn,
                """
                INSERT INTO [fhir].[Organization] (organization_id, name, active, identifier_value_sor)
                VALUES (?, 'Test Org 2', 'true', '999002')
            """,
                [oid],
            )
            _exec(conn, _REF_MERGE, [f"{TEST_PREFIX}%"])
            dw_ek = _scalar(
                conn,
                "SELECT DW_EK_Organisation FROM [ref].[Organisation] WHERE organization_id = ?",
                [oid],
            )

            # Org disappears from FHIR; soft-delete must flag it but keep the key.
            _exec(conn, "DELETE FROM [fhir].[Organization] WHERE organization_id = ?", [oid])
            _exec(conn, _REF_SOFT_DELETE, [f"{TEST_PREFIX}%"])

            rows = _fetchall(
                conn, "SELECT * FROM [ref].[Organisation] WHERE organization_id = ?", [oid]
            )
            assert len(rows) == 1, "Soft-delete must not physically remove the row (preserve FK)"
            assert rows[0]["DW_IsDeleted"] == 1
            assert rows[0]["DW_EK_Organisation"] == dw_ek
            _clear_test_data(conn)

    def test_check_constraint_rejects_null_sor_on_live_row(self, mssql_target_staging):
        """CHECK must reject a live row (DW_IsDeleted=0) with no sor_code."""
        with mssql_target_staging.get_connection() as conn:
            with pytest.raises(Exception) as exc:
                _exec(
                    conn,
                    """
                    INSERT INTO [ref].[Organisation] (organization_id, sor_code, DW_IsDeleted)
                    VALUES ('TEST-ORG-noident', NULL, 0);
                """,
                )
            msg = str(exc.value).lower()
            assert "ck_ref_organisation_hasidentifier" in msg or "check constraint" in msg


# ============================================================================
# dim.Organisation — Type 1 MERGE + parent resolution
# ============================================================================


class TestDimOrganisationParentResolution:
    """dim.Organisation resolves DW_EK_Org_Parent from fhir.Organization.part_of_id."""

    # MERGE source identical to DSA/SQL/dim/dim_organisation.py, scoped to test rows.
    _DIM_MERGE = """
        MERGE INTO [dim].[Organisation] AS target
        USING (
            SELECT
                r.DW_EK_Organisation,
                r.sor_code AS Code,
                COALESCE(rp.DW_EK_Organisation, -1) AS DW_EK_Org_Parent,
                rp.sor_code AS ParentCode,
                f.name AS Name,
                f.type_display AS Type,
                CASE WHEN LOWER(f.active) = 'true' THEN 1 ELSE 0 END AS Active
            FROM [ref].[Organisation] r
            JOIN [fhir].[Organization] f ON f.organization_id = r.organization_id
            LEFT JOIN [ref].[Organisation] rp
                ON rp.organization_id = f.part_of_id AND rp.DW_IsDeleted = 0
            WHERE r.DW_IsDeleted = 0 AND r.organization_id LIKE ?
        ) AS source
        ON target.DW_EK_Organisation = source.DW_EK_Organisation
        WHEN MATCHED THEN UPDATE SET
            Code = source.Code, DW_EK_Org_Parent = source.DW_EK_Org_Parent,
            ParentCode = source.ParentCode, Name = source.Name, Type = source.Type,
            Active = source.Active, DW_Timestamp_Update = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (DW_EK_Organisation, Code, DW_EK_Org_Parent, ParentCode, Name, Type, Active,
                    DW_Timestamp_Insert, DW_Timestamp_Update)
            VALUES (source.DW_EK_Organisation, source.Code, source.DW_EK_Org_Parent,
                    source.ParentCode, source.Name, source.Type, source.Active, GETDATE(), GETDATE());
    """

    def test_child_resolves_parent_dw_ek(self, mssql_target_staging):
        parent_oid, child_oid = f"{TEST_PREFIX}P", f"{TEST_PREFIX}C"
        with mssql_target_staging.get_connection() as conn:
            _clear_test_data(conn)
            _exec(
                conn,
                """
                INSERT INTO [fhir].[Organization] (organization_id, name, active, identifier_value_sor, part_of_id)
                VALUES (?, 'Parent Org', 'true', '999100', NULL),
                       (?, 'Child Org',  'true', '999101', ?)
            """,
                [parent_oid, child_oid, parent_oid],
            )
            _exec(conn, _REF_MERGE, [f"{TEST_PREFIX}%"])
            _exec(conn, self._DIM_MERGE, [f"{TEST_PREFIX}%"])

            parent_ek = _scalar(
                conn,
                "SELECT DW_EK_Organisation FROM [ref].[Organisation] WHERE organization_id = ?",
                [parent_oid],
            )
            child = _fetchall(
                conn,
                """
                SELECT d.DW_EK_Org_Parent, d.ParentCode
                FROM [dim].[Organisation] d
                JOIN [ref].[Organisation] r ON r.DW_EK_Organisation = d.DW_EK_Organisation
                WHERE r.organization_id = ?
            """,
                [child_oid],
            )
            assert len(child) == 1
            assert child[0]["DW_EK_Org_Parent"] == parent_ek, "Child must point at parent's DW_EK"
            assert child[0]["ParentCode"] == "999100"
            _clear_test_data(conn)

    def test_orphan_parent_falls_back_to_sentinel(self, mssql_target_staging):
        """An org whose part_of_id is not in ref resolves parent to -1."""
        oid = f"{TEST_PREFIX}ORPH"
        with mssql_target_staging.get_connection() as conn:
            _clear_test_data(conn)
            _exec(
                conn,
                """
                INSERT INTO [fhir].[Organization] (organization_id, name, active, identifier_value_sor, part_of_id)
                VALUES (?, 'Orphan Org', 'true', '999200', 'TEST-ORG-MISSING-PARENT')
            """,
                [oid],
            )
            _exec(conn, _REF_MERGE, [f"{TEST_PREFIX}%"])
            _exec(conn, self._DIM_MERGE, [f"{TEST_PREFIX}%"])
            parent_ek = _scalar(
                conn,
                """
                SELECT d.DW_EK_Org_Parent FROM [dim].[Organisation] d
                JOIN [ref].[Organisation] r ON r.DW_EK_Organisation = d.DW_EK_Organisation
                WHERE r.organization_id = ?
            """,
                [oid],
            )
            assert parent_ek == -1
            _clear_test_data(conn)


# ============================================================================
# DW_EK continuity / row-count reconciliation
# ============================================================================


class TestOrganisationReconciliation:
    """Baseline checks comparing the live ref/dim/fhir Organisation populations."""

    def test_every_live_sor_org_has_a_ref_key(self, mssql_target_staging):
        """No live fhir.Organization with a SOR code should be missing from ref."""
        with mssql_target_staging.get_connection() as conn:
            missing = _scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM [fhir].[Organization] f
                WHERE f.identifier_value_sor IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM [ref].[Organisation] r
                      WHERE r.organization_id = f.organization_id AND r.DW_IsDeleted = 0
                  )
            """,
            )
            assert missing == 0, (
                f"{missing} live SOR organizations in fhir.Organization have no "
                "ref.Organisation row — run ref_organisation."
            )

    def test_dim_matches_live_ref_count(self, mssql_target_staging):
        """dim.Organisation (minus the -1 sentinel) covers every live ref row.

        dim is never pruned (Type 1, no delete), so dim_count >= live_ref_count.
        Logs the delta for the migration reconciliation note.
        """
        with mssql_target_staging.get_connection() as conn:
            live_ref = _scalar(
                conn, "SELECT COUNT(*) FROM [ref].[Organisation] WHERE DW_IsDeleted = 0"
            )
            dim_total = _scalar(
                conn, "SELECT COUNT(*) FROM [dim].[Organisation] WHERE DW_EK_Organisation <> -1"
            )
            uncovered = _scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM [ref].[Organisation] r
                WHERE r.DW_IsDeleted = 0
                  AND NOT EXISTS (SELECT 1 FROM [dim].[Organisation] d
                                  WHERE d.DW_EK_Organisation = r.DW_EK_Organisation)
            """,
            )
            print(
                f"[reconciliation] live_ref={live_ref} dim_total={dim_total} uncovered_ref={uncovered}"
            )
            assert uncovered == 0, f"{uncovered} live ref rows have no dim.Organisation row"

    def test_no_dangling_dim_parent_fks(self, mssql_target_staging):
        """Every non-sentinel DW_EK_Org_Parent must exist in dim.Organisation."""
        with mssql_target_staging.get_connection() as conn:
            dangling = _scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM [dim].[Organisation] d
                WHERE d.DW_EK_Org_Parent NOT IN (-1)
                  AND NOT EXISTS (SELECT 1 FROM [dim].[Organisation] p
                                  WHERE p.DW_EK_Organisation = d.DW_EK_Org_Parent)
            """,
            )
            assert dangling == 0, f"{dangling} dim.Organisation rows have a dangling parent FK"

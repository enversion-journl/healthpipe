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
"""

import os

import pytest
from DSA.SQL.dim.dim_Organisation import _HIERARCHY_SQL

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
        DELETE d
        FROM [dim].[Organisation] d
        WHERE d.dw_ek_Organisation <> -1
          AND (
              EXISTS (
                  SELECT 1 FROM [ref].[Organisation] r
                  WHERE r.dw_ek_Organisation = d.dw_ek_Organisation
                    AND r.organization_id LIKE '{prefix}%'
              )
              OR d.kode LIKE '999[0-9][0-9][0-9]'
          );
        DELETE FROM [ref].[Organisation]  WHERE organization_id LIKE '{prefix}%';
        DELETE FROM [fhir].[Organization] WHERE organization_id LIKE '{prefix}%';
    """,
    )


# The soft-delete / MERGE SQL the ref_organisation asset runs (kept in sync with
# DSA/SQL/ref/organisation.py — the contract test guards the asset source).
# All three deletions run BEFORE the MERGE; only the MERGE resurrects.
_REF_FLAG_DELETE = """
    UPDATE tgt
    SET dw_is_deleted        = 1,
        dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
    FROM [ref].[Organisation] tgt
    INNER JOIN [fhir].[Organization] f
        ON f.organization_id = tgt.organization_id
    WHERE tgt.organization_id LIKE ?
      AND tgt.dw_is_deleted = 0
      AND f.dw_is_deleted = 1;
"""

_REF_UNIDENTIFIED_DELETE = """
    UPDATE tgt
    SET dw_is_deleted        = 1,
        dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
    FROM [ref].[Organisation] tgt
    INNER JOIN [fhir].[Organization] f
        ON f.organization_id = tgt.organization_id
    WHERE tgt.organization_id LIKE ?
      AND tgt.dw_is_deleted = 0
      AND f.identifier_value_sor IS NULL;
"""

_REF_MERGE = """
    MERGE INTO [ref].[Organisation] AS tgt
    USING (
        SELECT
            f.organization_id,
            f.identifier_value_sor AS sor_code,
            CAST(CASE WHEN f.dw_is_deleted = 1 THEN 1 ELSE 0 END AS BIT) AS dw_is_deleted
        FROM [fhir].[Organization] f
        WHERE f.identifier_value_sor IS NOT NULL
          AND f.organization_id LIKE ?
    ) AS src
    ON tgt.organization_id = src.organization_id
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (organization_id, sor_code, dw_is_deleted, dw_timestamp_insert, dw_timestamp_update)
        VALUES (src.organization_id, src.sor_code, src.dw_is_deleted,
                CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0)))
    WHEN MATCHED AND (
           ISNULL(tgt.sor_code,'') <> ISNULL(src.sor_code,'')
        OR tgt.dw_is_deleted <> src.dw_is_deleted
    ) THEN
        UPDATE SET sor_code = src.sor_code, dw_is_deleted = src.dw_is_deleted,
                   dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0));
"""

_REF_SOFT_DELETE = """
    UPDATE tgt SET dw_is_deleted = 1, dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
    FROM [ref].[Organisation] tgt
    WHERE tgt.dw_is_deleted = 0
      AND tgt.organization_id LIKE ?
      AND NOT EXISTS (SELECT 1 FROM [fhir].[Organization] f WHERE f.organization_id = tgt.organization_id);
"""


@pytest.fixture
def clean_org_test_data(mssql_target_staging):
    """Clear synthetic rows before AND after each write test.

    Teardown must not depend on the test body reaching its last line: a failed
    assertion used to leave a committed live dim row behind, and with
    UQ_dim_Organisation_kode_Live that residue is a constraint others trip over.
    """
    with mssql_target_staging.get_connection() as conn:
        _clear_test_data(conn)
    yield
    with mssql_target_staging.get_connection() as conn:
        _clear_test_data(conn)


def _run_ref_pipeline(conn, prefix: str = f"{TEST_PREFIX}%"):
    """Asset order: all deletions first, then the MERGE (the only resurrection)."""
    _exec(conn, _REF_FLAG_DELETE, [prefix])
    _exec(conn, _REF_UNIDENTIFIED_DELETE, [prefix])
    _exec(conn, _REF_SOFT_DELETE, [prefix])
    _exec(conn, _REF_MERGE, [prefix])


# ============================================================================
# ref.Organisation
# ============================================================================


class TestRefOrganisationMerge:
    """ref.Organisation MERGE behavior — assignment, update, soft-delete."""

    def test_inserts_new_org_with_int_dw_ek(self, mssql_target_staging, clean_org_test_data):
        oid = f"{TEST_PREFIX}001"
        with mssql_target_staging.get_connection() as conn:
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
            assert rows[0]["dw_is_deleted"] == 0
            assert isinstance(rows[0]["dw_ek_Organisation"], int)

    def test_cascade_soft_delete_when_org_leaves_fhir(
        self, mssql_target_staging, clean_org_test_data
    ):
        """An org removed from fhir.Organization is soft-deleted, keeping its DW_EK."""
        oid = f"{TEST_PREFIX}002"
        with mssql_target_staging.get_connection() as conn:
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
                "SELECT dw_ek_Organisation FROM [ref].[Organisation] WHERE organization_id = ?",
                [oid],
            )

            # Org disappears from FHIR; soft-delete must flag it but keep the key.
            _exec(conn, "DELETE FROM [fhir].[Organization] WHERE organization_id = ?", [oid])
            _exec(conn, _REF_SOFT_DELETE, [f"{TEST_PREFIX}%"])

            rows = _fetchall(
                conn, "SELECT * FROM [ref].[Organisation] WHERE organization_id = ?", [oid]
            )
            assert len(rows) == 1, "Soft-delete must not physically remove the row (preserve FK)"
            assert rows[0]["dw_is_deleted"] == 1
            assert rows[0]["dw_ek_Organisation"] == dw_ek

    def test_copies_fhir_deleted_flag_onto_live_ref_row(
        self, mssql_target_staging, clean_org_test_data
    ):
        """A fhir.Organization still present with dw_is_deleted=1 must mark ref deleted."""
        oid = f"{TEST_PREFIX}003"
        with mssql_target_staging.get_connection() as conn:
            _exec(
                conn,
                """
                INSERT INTO [fhir].[Organization]
                    (organization_id, name, active, identifier_value_sor, dw_is_deleted)
                VALUES (?, 'Test Org 3', 'true', '999003', 0)
            """,
                [oid],
            )
            _exec(conn, _REF_MERGE, [f"{TEST_PREFIX}%"])
            _exec(
                conn,
                "UPDATE [fhir].[Organization] SET dw_is_deleted = 1 WHERE organization_id = ?",
                [oid],
            )
            _exec(conn, _REF_MERGE, [f"{TEST_PREFIX}%"])
            flag = _scalar(
                conn,
                "SELECT dw_is_deleted FROM [ref].[Organisation] WHERE organization_id = ?",
                [oid],
            )
            assert flag == 1

    def test_new_uuid_reuses_sor_code_after_old_uuid_is_marked(
        self, mssql_target_staging, clean_org_test_data
    ):
        """A still-present fhir row with dw_is_deleted=1 must free the live SOR
        index before MERGE inserts the replacement UUID."""
        old_oid, new_oid = f"{TEST_PREFIX}REUSE-A", f"{TEST_PREFIX}REUSE-B"
        with mssql_target_staging.get_connection() as conn:
            _exec(
                conn,
                """
                INSERT INTO [fhir].[Organization]
                    (organization_id, name, active, identifier_value_sor, dw_is_deleted)
                VALUES (?, 'Old UUID', 'true', '999401', 0)
            """,
                [old_oid],
            )
            _run_ref_pipeline(conn)
            _exec(
                conn,
                """
                UPDATE [fhir].[Organization]
                SET dw_is_deleted = 1
                WHERE organization_id = ?
            """,
                [old_oid],
            )
            _exec(
                conn,
                """
                INSERT INTO [fhir].[Organization]
                    (organization_id, name, active, identifier_value_sor, dw_is_deleted)
                VALUES (?, 'New UUID', 'true', '999401', 0)
            """,
                [new_oid],
            )
            _run_ref_pipeline(conn)

            rows = _fetchall(
                conn,
                """
                SELECT organization_id, dw_is_deleted
                FROM [ref].[Organisation]
                WHERE organization_id IN (?, ?)
                ORDER BY organization_id
            """,
                [old_oid, new_oid],
            )
            by_id = {r["organization_id"]: r["dw_is_deleted"] for r in rows}
            assert by_id[old_oid] == 1
            assert by_id[new_oid] == 0

    def test_fhir_row_losing_its_sor_id_frees_the_code(
        self, mssql_target_staging, clean_org_test_data
    ):
        """A fhir row whose identifier_value_sor goes NULL drops out of the MERGE
        source. Left live it would keep a stale sor_code forever and block the next
        UUID that takes that code, so it must be soft-deleted instead."""
        stale_oid, next_oid = f"{TEST_PREFIX}NOSOR-A", f"{TEST_PREFIX}NOSOR-B"
        with mssql_target_staging.get_connection() as conn:
            _exec(
                conn,
                """
                INSERT INTO [fhir].[Organization]
                    (organization_id, name, active, identifier_value_sor, dw_is_deleted)
                VALUES (?, 'Had a SOR id', 'true', '999501', 0)
            """,
                [stale_oid],
            )
            _run_ref_pipeline(conn)

            # The org loses its SOR identifier; another UUID takes the code over.
            _exec(
                conn,
                "UPDATE [fhir].[Organization] SET identifier_value_sor = NULL "
                "WHERE organization_id = ?",
                [stale_oid],
            )
            _exec(
                conn,
                """
                INSERT INTO [fhir].[Organization]
                    (organization_id, name, active, identifier_value_sor, dw_is_deleted)
                VALUES (?, 'Took the code', 'true', '999501', 0)
            """,
                [next_oid],
            )
            _run_ref_pipeline(conn)

            rows = _fetchall(
                conn,
                """
                SELECT organization_id, sor_code, dw_is_deleted
                FROM [ref].[Organisation]
                WHERE organization_id IN (?, ?)
            """,
                [stale_oid, next_oid],
            )
            by_id = {r["organization_id"]: r for r in rows}
            assert (
                by_id[stale_oid]["dw_is_deleted"] == 1
            ), "a fhir row with no SOR id must not stay live holding a stale sor_code"
            assert by_id[next_oid]["dw_is_deleted"] == 0
            assert by_id[next_oid]["sor_code"] == "999501"

    def test_check_constraint_rejects_null_sor_on_live_row(
        self, mssql_target_staging, clean_org_test_data
    ):
        """CHECK must reject a live row (dw_is_deleted=0) with no sor_code."""
        with mssql_target_staging.get_connection() as conn:
            with pytest.raises(Exception) as exc:
                _exec(
                    conn,
                    """
                    INSERT INTO [ref].[Organisation] (organization_id, sor_code, dw_is_deleted)
                    VALUES ('TEST-ORG-noident', NULL, 0);
                """,
                )
            msg = str(exc.value).lower()
            assert "ck_ref_organisation_hasidentifier" in msg or "check constraint" in msg


# ============================================================================
# dim.Organisation — Type 1 MERGE + parent resolution
# ============================================================================


class TestDimOrganisationParentResolution:
    """dim.Organisation resolves dw_ek_Organisation_parent from fhir.Organization.part_of_id."""

    # MERGE source identical to DSA/SQL/dim/dim_Organisation.py, scoped to test rows.
    _DIM_MERGE = """
        MERGE INTO [dim].[Organisation] AS target
        USING (
            SELECT
                r.dw_ek_Organisation,
                r.organization_id AS fhir_Organization_id,
                r.sor_code AS kode,
                r.dw_is_deleted,
                COALESCE(rp.dw_ek_Organisation, -1) AS dw_ek_Organisation_parent,
                rp.sor_code AS kode_parent,
                f.name AS navn,
                CASE WHEN LOWER(f.active) = 'true' THEN 1 ELSE 0 END AS aktiv
            FROM [ref].[Organisation] r
            JOIN [fhir].[Organization] f ON f.organization_id = r.organization_id
            LEFT JOIN [ref].[Organisation] rp
                ON rp.organization_id = f.part_of_id
            WHERE r.organization_id LIKE ?
        ) AS source
        ON target.dw_ek_Organisation = source.dw_ek_Organisation
        WHEN MATCHED AND (
               ISNULL(target.kode, '') <> ISNULL(source.kode, '')
            OR target.dw_is_deleted <> source.dw_is_deleted
            OR ISNULL(target.dw_ek_Organisation_parent, -1)
                <> ISNULL(source.dw_ek_Organisation_parent, -1)
            OR ISNULL(target.kode_parent, '') <> ISNULL(source.kode_parent, '')
            OR ISNULL(target.fhir_Organization_id, N'') <> ISNULL(source.fhir_Organization_id, N'')
            OR ISNULL(target.navn, N'') <> ISNULL(source.navn, N'')
            OR ISNULL(target.aktiv, 0) <> ISNULL(source.aktiv, 0)
        ) THEN UPDATE SET
            kode = source.kode, dw_is_deleted = source.dw_is_deleted,
            dw_ek_Organisation_parent = source.dw_ek_Organisation_parent,
            kode_parent = source.kode_parent, navn = source.navn,
            fhir_Organization_id = source.fhir_Organization_id,
            aktiv = source.aktiv, dw_timestamp_update = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (dw_ek_Organisation, kode, dw_is_deleted, dw_ek_Organisation_parent, kode_parent,
                    fhir_Organization_id, navn, aktiv, dw_timestamp_insert, dw_timestamp_update)
            VALUES (source.dw_ek_Organisation, source.kode, source.dw_is_deleted,
                    source.dw_ek_Organisation_parent, source.kode_parent,
                    source.fhir_Organization_id, source.navn,
                    source.aktiv, GETDATE(), GETDATE());
    """

    def test_child_resolves_parent_dw_ek(self, mssql_target_staging, clean_org_test_data):
        parent_oid, child_oid = f"{TEST_PREFIX}P", f"{TEST_PREFIX}C"
        with mssql_target_staging.get_connection() as conn:
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
                "SELECT dw_ek_Organisation FROM [ref].[Organisation] WHERE organization_id = ?",
                [parent_oid],
            )
            child = _fetchall(
                conn,
                """
                SELECT d.dw_ek_Organisation_parent, d.kode_parent, d.fhir_Organization_id
                FROM [dim].[Organisation] d
                JOIN [ref].[Organisation] r ON r.dw_ek_Organisation = d.dw_ek_Organisation
                WHERE r.organization_id = ?
            """,
                [child_oid],
            )
            assert len(child) == 1
            assert (
                child[0]["dw_ek_Organisation_parent"] == parent_ek
            ), "Child must point at parent's DW_EK"
            assert child[0]["kode_parent"] == "999100"
            assert (
                child[0]["fhir_Organization_id"] == child_oid
            ), "fhir_Organization_id must carry the fhir.Organization resource id"

    def test_orphan_parent_falls_back_to_sentinel(self, mssql_target_staging, clean_org_test_data):
        """An org whose part_of_id is not in ref resolves parent to -1."""
        oid = f"{TEST_PREFIX}ORPH"
        with mssql_target_staging.get_connection() as conn:
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
                SELECT d.dw_ek_Organisation_parent FROM [dim].[Organisation] d
                JOIN [ref].[Organisation] r ON r.dw_ek_Organisation = d.dw_ek_Organisation
                WHERE r.organization_id = ?
            """,
                [oid],
            )
            assert parent_ek == -1


class TestDimOrganisationHierarchy:
    """Behaviour of the recursive SOR walk — root/leaf, unique live kode, no-op."""

    _DIM_MERGE = TestDimOrganisationParentResolution._DIM_MERGE

    def test_three_deep_chain_puts_root_on_niveau1(self, mssql_target_staging, clean_org_test_data):
        """max_dist - dist + 1 must put the root on niveau1, not the leaf."""
        root_oid, mid_oid, leaf_oid = (
            f"{TEST_PREFIX}H1",
            f"{TEST_PREFIX}H2",
            f"{TEST_PREFIX}H3",
        )
        with mssql_target_staging.get_connection() as conn:
            _exec(
                conn,
                """
                INSERT INTO [fhir].[Organization]
                    (organization_id, name, active, identifier_value_sor, part_of_id)
                VALUES (?, 'Root Org', 'true', '999301', NULL),
                       (?, 'Mid Org',  'true', '999302', ?),
                       (?, 'Leaf Org', 'true', '999303', ?)
            """,
                [root_oid, mid_oid, root_oid, leaf_oid, mid_oid],
            )
            _run_ref_pipeline(conn)
            _exec(conn, self._DIM_MERGE, [f"{TEST_PREFIX}%"])
            _exec(conn, _HIERARCHY_SQL)

            leaf = _fetchall(
                conn,
                """
                SELECT d.hierarki_niveau, d.kode_niveau1, d.kode_niveau3
                FROM [dim].[Organisation] d
                JOIN [ref].[Organisation] r ON r.dw_ek_Organisation = d.dw_ek_Organisation
                WHERE r.organization_id = ?
            """,
                [leaf_oid],
            )
            assert len(leaf) == 1
            assert leaf[0]["hierarki_niveau"] == 3
            assert leaf[0]["kode_niveau1"] == "999301"
            assert leaf[0]["kode_niveau3"] == "999303"

    def test_hierarchy_rerun_leaves_timestamp_untouched(
        self, mssql_target_staging, clean_org_test_data
    ):
        """EXCEPT must skip a no-op hierarchy refresh so the watermark stays put."""
        root_oid, leaf_oid = f"{TEST_PREFIX}H4", f"{TEST_PREFIX}H5"
        with mssql_target_staging.get_connection() as conn:
            _exec(
                conn,
                """
                INSERT INTO [fhir].[Organization]
                    (organization_id, name, active, identifier_value_sor, part_of_id)
                VALUES (?, 'Root Org', 'true', '999304', NULL),
                       (?, 'Leaf Org', 'true', '999305', ?)
            """,
                [root_oid, leaf_oid, root_oid],
            )
            _run_ref_pipeline(conn)
            _exec(conn, self._DIM_MERGE, [f"{TEST_PREFIX}%"])
            _exec(conn, _HIERARCHY_SQL)
            first_ts = _scalar(
                conn,
                """
                SELECT d.dw_timestamp_update
                FROM [dim].[Organisation] d
                JOIN [ref].[Organisation] r ON r.dw_ek_Organisation = d.dw_ek_Organisation
                WHERE r.organization_id = ?
            """,
                [leaf_oid],
            )
            _exec(conn, _HIERARCHY_SQL)
            second_ts = _scalar(
                conn,
                """
                SELECT d.dw_timestamp_update
                FROM [dim].[Organisation] d
                JOIN [ref].[Organisation] r ON r.dw_ek_Organisation = d.dw_ek_Organisation
                WHERE r.organization_id = ?
            """,
                [leaf_oid],
            )
            assert first_ts == second_ts

    def test_live_kode_unique_index_rejects_second_live_row(
        self, mssql_target_staging, clean_org_test_data
    ):
        """Two live dim rows with the same kode must violate UQ_dim_Organisation_kode_Live."""
        with mssql_target_staging.get_connection() as conn:
            _exec(
                conn,
                """
                INSERT INTO [dim].[Organisation]
                    (dw_ek_Organisation, kode, dw_is_deleted, navn)
                VALUES (-91001, '999306', 0, 'First live')
            """,
            )
            with pytest.raises(Exception) as exc:
                _exec(
                    conn,
                    """
                    INSERT INTO [dim].[Organisation]
                        (dw_ek_Organisation, kode, dw_is_deleted, navn)
                    VALUES (-91002, '999306', 0, 'Second live')
                """,
                )
            msg = str(exc.value).lower()
            assert "uq_dim_organisation_kode_live" in msg or "unique" in msg


# ============================================================================
# DW_EK continuity / row-count reconciliation
# ============================================================================


class TestOrganisationReconciliation:
    """Baseline checks comparing the live ref/dim/fhir Organisation populations."""

    def test_every_live_sor_org_has_a_ref_key(self, mssql_target_staging):
        """A LIVE fhir.Organization with a SOR code must resolve a live ref row.

        f.dw_is_deleted = 0 is part of the predicate on purpose: ref mirrors the
        fhir flag, so a soft-deleted fhir row is *expected* to have a soft-deleted
        ref row. Without the filter this asserts the opposite of the design and
        fails on every UUID the nightly reconcile retires.
        """
        with mssql_target_staging.get_connection() as conn:
            missing = _scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM [fhir].[Organization] f
                WHERE f.identifier_value_sor IS NOT NULL
                  AND f.dw_is_deleted = 0
                  AND NOT EXISTS (
                      SELECT 1 FROM [ref].[Organisation] r
                      WHERE r.organization_id = f.organization_id AND r.dw_is_deleted = 0
                  )
            """,
            )
            assert missing == 0, (
                f"{missing} live SOR organizations in fhir.Organization have no "
                "live ref.Organisation row — run ref_organisation."
            )

    def test_dim_matches_live_ref_count(self, mssql_target_staging):
        """dim.Organisation (minus the -1 sentinel) covers every live ref row.

        dim is never pruned (Type 1, no delete), so dim_count >= live_ref_count.
        Logs the delta for the migration reconciliation note.
        """
        with mssql_target_staging.get_connection() as conn:
            live_ref = _scalar(
                conn, "SELECT COUNT(*) FROM [ref].[Organisation] WHERE dw_is_deleted = 0"
            )
            dim_total = _scalar(
                conn, "SELECT COUNT(*) FROM [dim].[Organisation] WHERE dw_ek_Organisation <> -1"
            )
            uncovered = _scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM [ref].[Organisation] r
                WHERE r.dw_is_deleted = 0
                  AND NOT EXISTS (SELECT 1 FROM [dim].[Organisation] d
                                  WHERE d.dw_ek_Organisation = r.dw_ek_Organisation)
            """,
            )
            print(
                f"[reconciliation] live_ref={live_ref} dim_total={dim_total} uncovered_ref={uncovered}"
            )
            assert uncovered == 0, f"{uncovered} live ref rows have no dim.Organisation row"

    def test_no_dangling_dim_parent_fks(self, mssql_target_staging):
        """Every non-sentinel dw_ek_Organisation_parent must exist in dim.Organisation."""
        with mssql_target_staging.get_connection() as conn:
            dangling = _scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM [dim].[Organisation] d
                WHERE d.dw_ek_Organisation_parent NOT IN (-1)
                  AND NOT EXISTS (SELECT 1 FROM [dim].[Organisation] p
                                  WHERE p.dw_ek_Organisation = d.dw_ek_Organisation_parent)
            """,
            )
            assert dangling == 0, f"{dangling} dim.Organisation rows have a dangling parent FK"

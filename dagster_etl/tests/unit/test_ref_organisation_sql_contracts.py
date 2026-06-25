# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL contract tests for the ref.Organisation + dim.Organisation build assets
and the DW_EK-continuity seeding migration.

These are string-level smoke tests: they inspect the Python/SQL source and
assert that critical patterns are present (or absent). They catch typos and
accidental removal of load-bearing constructs without needing a live MSSQL
instance.

For end-to-end behavior, see the integration tests in
tests/integration/datawarehouse/organisation/ (skipped by default).

Part of US #28193 (fhir.Organization as the basis for dim.Organisation).
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REF_DIR = ROOT / "DSA" / "SQL" / "ref"
DIM_DIR = ROOT / "DSA" / "SQL" / "dim"
MIGRATIONS = ROOT / "infrastructure" / "ddl" / "migrations"
MIGRATION = MIGRATIONS / "20260618_ref_organisation.sql"
DROP_COLS_MIGRATION = MIGRATIONS / "20260618_dim_organisation_drop_legacy_columns.sql"

# Legacy columns that must not survive on dim.Organisation (US #28193).
_LEGACY_DIM_COLUMNS = [
    "SourceType",
    "CodeSource",
    "CodeSOR",
    "DW_EK_Org_ParentSOR",
    "DW_EK_Org_ParentSource",
    "ParentSOR",
    "ParentSource",
]


class TestRefOrganisationSQLContract:
    """ref.Organisation asset emits the right MERGE + cascade-delete SQL."""

    @staticmethod
    def _sql() -> str:
        return (REF_DIR / "organisation.py").read_text()

    def test_uses_merge_into_ref_organisation(self):
        assert re.search(
            r"MERGE\s+INTO\s+\[ref\]\.\[Organisation\]", self._sql()
        ), "ref_organisation asset must MERGE INTO [ref].[Organisation]"

    def test_sources_from_fhir_organization_with_identifier_filter(self):
        sql = self._sql()
        assert "FROM [fhir].[Organization]" in sql
        assert (
            "identifier_value_sor IS NOT NULL" in sql
        ), "MERGE source must filter to rows carrying a SOR identifier"

    def test_insert_branch_assigns_zero_isdeleted(self):
        not_matched = re.search(
            r"WHEN NOT MATCHED BY TARGET.*?VALUES.*?\)",
            self._sql(),
            flags=re.DOTALL,
        )
        assert not_matched, "Missing WHEN NOT MATCHED BY TARGET branch"
        assert "0," in not_matched.group(), "New rows inserted with DW_IsDeleted = 0"

    def test_update_branch_resurrects_soft_deleted(self):
        assert re.search(
            r"WHEN MATCHED AND.*?UPDATE SET.*?DW_IsDeleted\s*=\s*0",
            self._sql(),
            flags=re.DOTALL,
        ), "WHEN MATCHED branch must clear DW_IsDeleted (un-delete reappearing orgs)"

    def test_cascade_soft_delete_for_missing_orgs(self):
        assert re.search(
            r"UPDATE\s+tgt\s+SET\s+DW_IsDeleted\s*=\s*1.*NOT EXISTS",
            self._sql(),
            flags=re.DOTALL,
        ), "Missing cascade soft-delete for organization_ids removed from fhir.Organization"

    def test_uses_soft_delete_threshold(self):
        assert "check_soft_delete_threshold" in self._sql()

    def test_is_full_scan_not_dfm(self):
        """Organisation ref is a full-scan MERGE — no DFM checkpoint machinery."""
        sql = self._sql()
        assert "DfmManager" not in sql, "ref.Organisation must be full-scan (no DFM)"
        assert "last_updated >" not in sql, "No delta checkpoint filter expected"

    def test_registered_in_ref_package(self):
        init = (REF_DIR / "__init__.py").read_text()
        assert "ref_organisation" in init, "ref_organisation must be exported from ref package"


class TestDimOrganisationSQLContract:
    """dim.Organisation MERGEs from fhir.Organization + ref.Organisation (Type 1)."""

    @staticmethod
    def _sql() -> str:
        return (DIM_DIR / "dim_organisation.py").read_text()

    def test_uses_merge_into_dim_organisation(self):
        assert "MERGE INTO [dim].[Organisation]" in self._sql()

    def test_merge_keyed_on_dw_ek_organisation(self):
        assert re.search(
            r"target\.DW_EK_Organisation = source\.DW_EK_Organisation",
            self._sql(),
        ), "dim.Organisation MERGE must key on DW_EK_Organisation"

    def test_sources_from_ref_and_fhir(self):
        sql = self._sql()
        assert "FROM [ref].[Organisation]" in sql
        assert "[fhir].[Organization]" in sql

    def test_parent_resolved_via_part_of_id(self):
        assert re.search(
            r"organization_id\s*=\s*f\.part_of_id",
            self._sql(),
        ), "Parent DW_EK must be resolved by joining ref on fhir.Organization.part_of_id"

    def test_no_legacy_map_organisation_path(self):
        """Regression guard: dim must NOT read or import the retired legacy path.

        (The docstring may still *name* the old path for context — so guard on
        the table read and the imports, not on bare substrings.)
        """
        sql = self._sql()
        assert "[map].[Organisation]" not in sql, "dim.Organisation must not read map.Organisation"
        assert (
            "import map_organisation" not in sql
        ), "dim.Organisation must not import the map_organisation asset"
        assert (
            "import stg_organisation_sor" not in sql
        ), "dim.Organisation must not import the SOR-CSV staging asset"
        assert (
            "from DSA.SQL.staging" not in sql
        ), "dim.Organisation must not read the SOR-CSV staging directly"

    def test_is_type1_no_temporal_columns(self):
        sql = self._sql()
        assert (
            "DW_ValidFrom" not in sql and "DW_ValidTo" not in sql
        ), "dim.Organisation is Type 1 — no SCD2 validity columns"


class TestRefOrganisationSeedMigration:
    """The seeding migration preserves DW_EK 1:1 (downstream FK continuity)."""

    @staticmethod
    def _sql() -> str:
        return MIGRATION.read_text()

    def test_preserves_dw_ek_via_identity_insert(self):
        sql = self._sql()
        assert "SET IDENTITY_INSERT [ref].[Organisation] ON" in sql
        assert "SET IDENTITY_INSERT [ref].[Organisation] OFF" in sql

    def test_seeds_from_map_joined_to_fhir_on_sor_code(self):
        sql = self._sql()
        assert "[map].[Organisation]" in sql and "[fhir].[Organization]" in sql
        assert re.search(
            r"f\.identifier_value_sor\s*=\s*m\.Code",
            sql,
        ), "Seed must match map.Code to fhir.identifier_value_sor (the SOR code)"

    def test_is_idempotent_on_emptiness(self):
        assert (
            "NOT EXISTS (SELECT 1 FROM [ref].[Organisation])" in self._sql()
        ), "Seed must only fire when ref.Organisation is empty"

    def test_reseeds_identity_after_insert(self):
        assert "DBCC CHECKIDENT" in self._sql(), (
            "IDENTITY must be reseeded after IDENTITY_INSERT so new orgs get keys "
            "above the migrated maximum"
        )


class TestDimOrganisationDropLegacyColumnsMigration:
    """The drop-legacy-columns migration removes every orphaned column safely."""

    @staticmethod
    def _sql() -> str:
        return DROP_COLS_MIGRATION.read_text()

    def test_drops_every_legacy_column(self):
        sql = self._sql()
        for col in _LEGACY_DIM_COLUMNS:
            assert re.search(
                rf"DROP COLUMN \[{col}\]",
                sql,
            ), f"Migration must DROP COLUMN [{col}] from dim.Organisation"

    def test_drops_default_constraints_first(self):
        """SourceType/CodeSource were added with inline DEFAULT — must drop the
        auto-named default constraints before dropping the columns."""
        sql = self._sql()
        assert "sys.default_constraints" in sql
        assert "DROP CONSTRAINT" in sql

    def test_every_column_drop_is_guarded(self):
        """Idempotency: each DROP COLUMN sits behind an IF EXISTS sys.columns guard."""
        sql = self._sql()
        for col in _LEGACY_DIM_COLUMNS:
            assert re.search(
                rf"name = '{col}'.*?DROP COLUMN \[{col}\]",
                sql,
                flags=re.DOTALL,
            ), f"DROP COLUMN [{col}] must be guarded by an IF EXISTS sys.columns check"

    def test_does_not_touch_end_state_columns(self):
        """Must not drop any column that the end-state DDL keeps."""
        sql = self._sql()
        for keep in [
            "[Code]",
            "[ParentCode]",
            "[DW_EK_Org_Parent]",
            "[Name]",
            "[Type]",
            "[Active]",
        ]:
            assert f"DROP COLUMN {keep}" not in sql, f"Must not drop kept column {keep}"

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
End-to-end integration test for the SKS CodeSystem pipeline WITH database.

Full flow: SKScomplete.txt (Sundhedsdatastyrelsen) → ext.sks_register
           → codesystem.sks_* (MERGE) → content-complete CodeSystem → Aidbox

Prerequisites:
    - USE_REAL_DB_FOR_TESTS=true
    - TEST_ENV=dev (or similar non-prod)
    - Staging DB accessible with codesystem schema + ext schema present
    - DDL for codesystem.sks_* tables already applied (01_codesystem_tables.sql)
      including the (code, property_code, value) PK on sks_properties

Run with:
    USE_REAL_DB_FOR_TESTS=true TEST_ENV=dev \
        pytest tests/integration/test_sks_e2e_db.py -v
"""

import json
import os
from datetime import date

import pytest
import requests
from core.resources import FHIRServerResource, MSSQLResource
from DSA.CodeSystem.sks.sks import build_sks_codesystem
from DSA.CodeSystem.utils import merge_codesystem_tables
from DSI.csv_files.sks_register import (
    EXT_COLUMNS,
    KNOWN_PREFIXES,
    SKS_COMPLETE_URL,
    _build_hierarchy,
    _deduplicate_newest,
    _parse_sks_lines,
)
from infrastructure.assets.fhir_codesystem_sks_complete import (
    SKS_PROPERTY_DEFINITIONS,
    _build_concept_lookup,
    _build_concept_tree,
)
from requests.auth import HTTPBasicAuth

pytestmark = [
    pytest.mark.integration,
    pytest.mark.real_resources,
    pytest.mark.skipif(
        os.environ.get("USE_REAL_DB_FOR_TESTS") != "true"
        or "dev" not in (os.environ.get("TEST_ENV") or ""),
        reason="Requires USE_REAL_DB_FOR_TESTS=true and TEST_ENV containing 'dev'",
    ),
]


# ---------------------------------------------------------------------------
# Helpers (same logic as sks_register.py, no Dagster context)
# ---------------------------------------------------------------------------
def _fetch_sks_complete() -> str:
    """Download SKScomplete.txt from Sundhedsdatastyrelsen."""
    resp = requests.get(
        SKS_COMPLETE_URL,
        headers={"User-Agent": "Mozilla/5.0 (compatible; Dagster-SKS-Loader/2.0)"},
        timeout=300,
    )
    resp.raise_for_status()
    return resp.content.decode("latin-1")


def _build_ext_rows(
    newest_by_code: dict[str, dict],
    classifications_by_code: dict[str, set[str]],
    today: date,
) -> list[dict[str, str]]:
    """Context-free variant of sks_register._build_ext_rows."""
    parent_map = _build_hierarchy(set(newest_by_code))
    ext_rows: list[dict[str, str]] = []
    for code in sorted(newest_by_code):
        rec = newest_by_code[code]
        inactive = "true" if rec["valid_to"] < today else "false"
        for classification in sorted(classifications_by_code[code]):
            ext_rows.append(
                {
                    "code": code,
                    "display": rec["display"],
                    "parent_code": parent_map.get(code, ""),
                    "classification": classification,
                    "Inactive": inactive,
                }
            )
    return ext_rows


def _build_codesystem(
    ext_rows: list[dict[str, str]],
) -> tuple[list[dict], list[dict], list[dict]]:
    """Build codesystem triple from ext rows via the real builder function."""
    rows = [tuple(r.get(col, "") for col in EXT_COLUMNS) for r in ext_rows]
    return build_sks_codesystem(rows, EXT_COLUMNS)


def _merge(conn, concepts, properties, hierarchy) -> dict:
    """MERGE into codesystem.sks_* with the SKS-specific flags."""
    cursor = conn.cursor()
    try:
        result = merge_codesystem_tables(
            cursor,
            "sks",
            concepts,
            properties,
            hierarchy,
            multi_value_properties=True,
            delete_stale=True,
        )
        conn.commit()
    finally:
        cursor.close()
    return result


def _write_ext_table(conn, ext_rows: list[dict[str, str]]) -> int:
    """Drop/create ext.sks_register and insert rows (same as _write_to_ext_table)."""
    cursor = conn.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS [ext].[sks_register]")
        cursor.execute(
            """
            CREATE TABLE [ext].[sks_register] (
                [code] NVARCHAR(100) NOT NULL,
                [display] NVARCHAR(500) NULL,
                [parent_code] NVARCHAR(500) NULL,
                [classification] NVARCHAR(500) NULL,
                [Inactive] NVARCHAR(500) NULL,
                [dw_timestamp_insert] DATETIME2(0) NOT NULL DEFAULT GETDATE(),
                [dw_timestamp_update] DATETIME2(0) NOT NULL DEFAULT GETDATE()
            )
        """
        )
        placeholders = ", ".join(["?"] * len(EXT_COLUMNS))
        col_names = ", ".join(f"[{c}]" for c in EXT_COLUMNS)
        insert_sql = f"INSERT INTO [ext].[sks_register] ({col_names}) VALUES ({placeholders})"
        total = 0
        for i in range(0, len(ext_rows), 1000):
            chunk = ext_rows[i : i + 1000]
            rows = [tuple(r.get(col, "") or None for col in EXT_COLUMNS) for r in chunk]
            cursor.executemany(insert_sql, rows)
            total += len(chunk)
        conn.commit()
        return total
    finally:
        cursor.close()


def _count_table(conn, schema: str, table: str) -> int:
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            IF OBJECT_ID('[{schema}].[{table}]', 'U') IS NOT NULL
                SELECT COUNT(*) FROM [{schema}].[{table}]
            ELSE
                SELECT -1
        """
        )
        return cursor.fetchone()[0]
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def staging_resource():
    """Real staging MSSQL resource."""
    host = os.getenv("TEST_MSSQL_TARGET_STAGING_HOST") or os.getenv(
        "MSSQL_TARGET_STAGING_HOST", "localhost"
    )
    port = int(
        os.getenv("TEST_MSSQL_TARGET_STAGING_PORT")
        or os.getenv("MSSQL_TARGET_STAGING_PORT", "1433")
    )
    database = os.getenv("TEST_MSSQL_TARGET_STAGING_DB") or os.getenv(
        "MSSQL_TARGET_STAGING_DB", "DW"
    )
    user = os.getenv("TEST_MSSQL_TARGET_STAGING_USER") or os.getenv(
        "MSSQL_TARGET_STAGING_USER", "sa"
    )
    password = os.getenv("TEST_MSSQL_TARGET_STAGING_PASSWORD") or os.getenv(
        "MSSQL_TARGET_STAGING_PASSWORD", ""
    )
    return MSSQLResource(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


@pytest.fixture(scope="module")
def sks_ext_rows():
    """Download + parse SKScomplete.txt once for the whole module."""
    text = _fetch_sks_complete()
    records = _parse_sks_lines(text)
    assert records, "No records parsed from SKScomplete.txt"
    newest_by_code, classifications_by_code = _deduplicate_newest(records)
    today = date.today()
    return _build_ext_rows(newest_by_code, classifications_by_code, today)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestExtractToExtTable:
    """Step 1: SKScomplete.txt → ext.sks_register."""

    def test_write_ext_table(self, staging_resource, sks_ext_rows):
        assert len(sks_ext_rows) > 50000, f"Expected >50000 ext rows, got {len(sks_ext_rows)}"

        with staging_resource.get_connection() as conn:
            inserted = _write_ext_table(conn, sks_ext_rows)
            assert inserted == len(sks_ext_rows)

            count = _count_table(conn, "ext", "sks_register")
            assert count == len(
                sks_ext_rows
            ), f"ext.sks_register has {count}, expected {len(sks_ext_rows)}"

    def test_ext_table_has_all_classifications(self, staging_resource, sks_ext_rows):
        with staging_resource.get_connection() as conn:
            _write_ext_table(conn, sks_ext_rows)
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT DISTINCT [classification] FROM [ext].[sks_register]")
                db_classifications = {row[0] for row in cursor.fetchall()}
            finally:
                cursor.close()

        assert db_classifications == set(
            KNOWN_PREFIXES
        ), f"Missing classifications: {set(KNOWN_PREFIXES) - db_classifications}"

    def test_codes_have_no_classification_prefix(self, sks_ext_rows):
        """Clean codes must not start with a classification prefix pattern
        like 'diaDA...' — the prefix is stripped during parsing."""
        sample = {r["code"] for r in sks_ext_rows[:5000]}
        prefixed = {c for c in sample if c[:3].islower() and c[:3] in KNOWN_PREFIXES}
        assert not prefixed, f"Codes still carry classification prefix: {sorted(prefixed)[:10]}"

    def test_codes_unique_per_classification(self, sks_ext_rows):
        """One row per (Code, classification) — no duplicates."""
        seen = set()
        for r in sks_ext_rows:
            key = (r["code"], r["classification"])
            assert key not in seen, f"Duplicate ext row: {key}"
            seen.add(key)


class TestCodesystemMerge:
    """Step 2: ext.sks_register → codesystem.sks_* via MERGE."""

    def test_merge_populates_all_three_tables(self, staging_resource, sks_ext_rows):
        concepts, properties, hierarchy = _build_codesystem(sks_ext_rows)

        assert len(concepts) > 50000
        assert len(properties) > 50000
        assert len(hierarchy) > 10000

        unique_concepts = {c["CODE"].upper() for c in concepts}
        unique_properties = {
            (p["CODE"].upper(), p["KEY"].upper(), (p.get("VALUE") or "").upper())
            for p in properties
        }

        with staging_resource.get_connection() as conn:
            _write_ext_table(conn, sks_ext_rows)
            result = _merge(conn, concepts, properties, hierarchy)

        assert result["concepts_inserted"] + result["concepts_updated"] == len(unique_concepts)
        assert result["properties_inserted"] + result["properties_updated"] == len(
            unique_properties
        )
        assert result["hierarchy_inserted"] + result["hierarchy_updated"] > 0

    def test_codesystem_table_row_counts(self, staging_resource, sks_ext_rows):
        concepts, properties, hierarchy = _build_codesystem(sks_ext_rows)

        unique_concepts = {c["CODE"].upper() for c in concepts}
        unique_properties = {
            (p["CODE"].upper(), p["KEY"].upper(), (p.get("VALUE") or "").upper())
            for p in properties
        }

        with staging_resource.get_connection() as conn:
            _write_ext_table(conn, sks_ext_rows)
            _merge(conn, concepts, properties, hierarchy)

            concept_count = _count_table(conn, "codesystem", "SKSConcept")
            properties_count = _count_table(conn, "codesystem", "SKSProperties")
            hierarchy_count = _count_table(conn, "codesystem", "SKSHierarchy")

        assert concept_count == len(
            unique_concepts
        ), f"sks_concept: DB has {concept_count}, expected {len(unique_concepts)}"
        assert properties_count == len(
            unique_properties
        ), f"sks_properties: DB has {properties_count}, expected {len(unique_properties)}"
        assert hierarchy_count > 0, "sks_hierarchy should have rows"

    def test_merge_is_idempotent(self, staging_resource, sks_ext_rows):
        """Running MERGE twice with same data should produce 0 inserts/deletes on second run."""
        concepts, properties, hierarchy = _build_codesystem(sks_ext_rows)

        with staging_resource.get_connection() as conn:
            _write_ext_table(conn, sks_ext_rows)
            _merge(conn, concepts, properties, hierarchy)
            result2 = _merge(conn, concepts, properties, hierarchy)

        assert (
            result2["concepts_inserted"] == 0
        ), f"Second MERGE should insert 0 concepts, got {result2['concepts_inserted']}"
        assert result2["properties_inserted"] == 0
        assert result2["hierarchy_inserted"] == 0
        assert result2["concepts_deleted"] == 0
        assert result2["properties_deleted"] == 0
        assert result2["hierarchy_deleted"] == 0


class TestFullPipelineReadBack:
    """Step 3: Verify data can be read back from codesystem tables correctly."""

    def test_read_back_concept_sample(self, staging_resource, sks_ext_rows):
        concepts, properties, hierarchy = _build_codesystem(sks_ext_rows)

        with staging_resource.get_connection() as conn:
            _write_ext_table(conn, sks_ext_rows)
            _merge(conn, concepts, properties, hierarchy)

            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT TOP 10 [code], [display] FROM [codesystem].[SKSConcept] "
                    "ORDER BY [code]"
                )
                rows = cursor.fetchall()
            finally:
                cursor.close()

        assert len(rows) == 10
        for code, display in rows:
            assert code and len(code) > 0, "Code should not be empty"
            assert display and len(display) > 0, "display should not be empty"

    def test_hierarchy_references_valid_concepts(self, staging_resource, sks_ext_rows):
        concepts, properties, hierarchy = _build_codesystem(sks_ext_rows)

        with staging_resource.get_connection() as conn:
            _write_ext_table(conn, sks_ext_rows)
            _merge(conn, concepts, properties, hierarchy)

            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT h.[parent_code]
                    FROM [codesystem].[SKSHierarchy] h
                    LEFT JOIN [codesystem].[SKSConcept] c ON c.[code] = h.[parent_code]
                    WHERE c.[code] IS NULL
                """
                )
                orphan_parents = cursor.fetchall()

                cursor.execute(
                    """
                    SELECT h.[child_code]
                    FROM [codesystem].[SKSHierarchy] h
                    LEFT JOIN [codesystem].[SKSConcept] c ON c.[code] = h.[child_code]
                    WHERE c.[code] IS NULL
                """
                )
                orphan_children = cursor.fetchall()
            finally:
                cursor.close()

        assert (
            len(orphan_parents) == 0
        ), f"Found {len(orphan_parents)} ParentCodes not in sks_concept"
        assert (
            len(orphan_children) == 0
        ), f"Found {len(orphan_children)} ChildCodes not in sks_concept"

    def test_property_codes_and_values(self, staging_resource, sks_ext_rows):
        concepts, properties, hierarchy = _build_codesystem(sks_ext_rows)

        with staging_resource.get_connection() as conn:
            _write_ext_table(conn, sks_ext_rows)
            _merge(conn, concepts, properties, hierarchy)

            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT DISTINCT [property_code] FROM [codesystem].[SKSProperties]
                """
                )
                prop_codes = {row[0] for row in cursor.fetchall()}

                cursor.execute(
                    """
                    SELECT DISTINCT [value] FROM [codesystem].[SKSProperties]
                    WHERE [property_code] = 'classification'
                """
                )
                values = {row[0] for row in cursor.fetchall()}

                cursor.execute(
                    """
                    SELECT COUNT(*) FROM [codesystem].[SKSProperties]
                    WHERE [property_code] = 'inactive'
                """
                )
                inactive_count = cursor.fetchone()[0]
            finally:
                cursor.close()

        assert prop_codes == {
            "classification",
            "inactive",
        }, f"Unexpected property codes: {prop_codes}"
        assert values == set(
            KNOWN_PREFIXES
        ), f"Expected all classification prefixes in properties, got {values}"
        assert inactive_count > 5000, f"Expected >5000 inactive codes, got {inactive_count}"

    def test_multi_classification_codes_have_multiple_rows(self, staging_resource, sks_ext_rows):
        """Codes appearing in several classifications keep one property row each."""
        concepts, properties, hierarchy = _build_codesystem(sks_ext_rows)

        with staging_resource.get_connection() as conn:
            _write_ext_table(conn, sks_ext_rows)
            _merge(conn, concepts, properties, hierarchy)

            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT TOP 1 [code], COUNT(*)
                    FROM [codesystem].[SKSProperties]
                    WHERE [property_code] = 'classification'
                    GROUP BY [code]
                    HAVING COUNT(*) > 1
                """
                )
                row = cursor.fetchone()
            finally:
                cursor.close()

        assert row is not None, "Expected at least one code with multiple classification properties"


# ---------------------------------------------------------------------------
# FHIR Server fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def fhir_server_resource():
    """Real FHIR server resource for integration tests."""
    return FHIRServerResource(
        base_url=os.getenv("TEST_FHIR_SERVER_URL")
        or os.getenv("FHIR_SERVER_URL", "http://fhir-server:8888"),
        client_username=os.getenv("AIDBOX_CLIENT_ID", "root"),
        client_password=os.getenv("AIDBOX_CLIENT_SECRET", "secret"),
    )


@pytest.fixture(scope="module")
def aidbox_available(fhir_server_resource):
    """Check if Aidbox is reachable; skip tests if not."""
    try:
        resp = requests.get(
            f"{fhir_server_resource.base_url}/health",
            timeout=5,
            auth=HTTPBasicAuth(
                fhir_server_resource.client_username,
                fhir_server_resource.client_password,
            ),
        )
        return resp.status_code == 200
    except (requests.ConnectionError, requests.Timeout):
        return False


# ---------------------------------------------------------------------------
# TestFHIRServerSync: full pipeline → FHIR server
# ---------------------------------------------------------------------------
class TestFHIRServerSync:
    """Step 4: Build content-complete CodeSystem from staging and PUT to Aidbox."""

    def _ensure_staging(self, staging_resource, sks_ext_rows):
        """Populate ext + codesystem tables."""
        concepts, properties, hierarchy = _build_codesystem(sks_ext_rows)
        with staging_resource.get_connection() as conn:
            _write_ext_table(conn, sks_ext_rows)
            _merge(conn, concepts, properties, hierarchy)

    def _read_staging_tables(self, conn):
        """Read the three codesystem staging tables."""
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT [code], [display] FROM [codesystem].[SKSConcept] ORDER BY [code]"
            )
            concept_rows = cursor.fetchall()
            cursor.execute(
                "SELECT [code], [property_code], [value], [type] "
                "FROM [codesystem].[SKSProperties] "
                "ORDER BY [code], [property_code], [value]"
            )
            property_rows = cursor.fetchall()
            cursor.execute("SELECT [parent_code], [child_code] FROM [codesystem].[SKSHierarchy]")
            hierarchy_rows = cursor.fetchall()
        finally:
            cursor.close()
        return concept_rows, property_rows, hierarchy_rows

    def _build_and_put(self, staging_resource, fhir_server_resource):
        """Build content-complete CodeSystem and PUT to Aidbox. Returns response."""
        with staging_resource.get_connection() as conn:
            concept_rows, property_rows, hierarchy_rows = self._read_staging_tables(conn)

        lookup = _build_concept_lookup(concept_rows, property_rows)
        concept_tree = _build_concept_tree(lookup, hierarchy_rows)

        codesystem_dict = {
            "resourceType": "CodeSystem",
            "id": "sks",
            "url": "urn:oid:1.2.208.176.2.4",
            "name": "SKS",
            "description": "Sundhedsvaesenets Klassifikationssystem (SKS).",
            "status": "active",
            "hierarchyMeaning": "is-a",
            "publisher": "Sundhedsdatastyrelsen",
            "content": "complete",
            "count": len(lookup),
            "property": SKS_PROPERTY_DEFINITIONS,
            "concept": concept_tree,
        }

        resource_json = json.dumps(codesystem_dict, ensure_ascii=False, separators=(",", ":"))
        url = f"{fhir_server_resource.effective_base_url}/CodeSystem/sks"
        response = fhir_server_resource.session.put(
            url,
            data=resource_json.encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=600,
        )
        return response, len(lookup)

    @pytest.mark.skipif(
        os.environ.get("USE_REAL_DB_FOR_TESTS") != "true",
        reason="Requires USE_REAL_DB_FOR_TESTS=true",
    )
    def test_post_content_complete_codesystem(
        self, staging_resource, sks_ext_rows, fhir_server_resource, aidbox_available
    ):
        """PUT content-complete SKS CodeSystem to Aidbox and verify HTTP 200/201."""
        if not aidbox_available:
            pytest.skip("Aidbox not reachable")

        self._ensure_staging(staging_resource, sks_ext_rows)

        response, _ = self._build_and_put(staging_resource, fhir_server_resource)
        assert response.status_code in (
            200,
            201,
        ), f"Expected 200/201, got {response.status_code}: {response.text[:500]}"

    @pytest.mark.skipif(
        os.environ.get("USE_REAL_DB_FOR_TESTS") != "true",
        reason="Requires USE_REAL_DB_FOR_TESTS=true",
    )
    def test_get_codesystem_has_content_complete(
        self, staging_resource, sks_ext_rows, fhir_server_resource, aidbox_available
    ):
        """GET CodeSystem/sks and verify content=complete with concepts."""
        if not aidbox_available:
            pytest.skip("Aidbox not reachable")

        self._ensure_staging(staging_resource, sks_ext_rows)
        self._build_and_put(staging_resource, fhir_server_resource)

        url = f"{fhir_server_resource.effective_base_url}/CodeSystem/sks"
        resp = fhir_server_resource.session.get(url, timeout=60)
        assert resp.status_code == 200

        cs = resp.json()
        assert cs["content"] == "complete"
        assert cs["url"] == "urn:oid:1.2.208.176.2.4"
        assert cs.get("count", 0) > 60000
        assert isinstance(cs.get("concept"), list)
        assert len(cs["concept"]) > 0

    @pytest.mark.skipif(
        os.environ.get("USE_REAL_DB_FOR_TESTS") != "true",
        reason="Requires USE_REAL_DB_FOR_TESTS=true",
    )
    def test_codesystem_has_property_definitions(
        self, staging_resource, sks_ext_rows, fhir_server_resource, aidbox_available
    ):
        """Verify the CodeSystem declares classification + inactive properties."""
        if not aidbox_available:
            pytest.skip("Aidbox not reachable")

        self._ensure_staging(staging_resource, sks_ext_rows)
        self._build_and_put(staging_resource, fhir_server_resource)

        url = f"{fhir_server_resource.effective_base_url}/CodeSystem/sks"
        resp = fhir_server_resource.session.get(url, timeout=60)
        cs = resp.json()

        prop_defs = {p.get("code"): p for p in cs.get("property", [])}
        assert set(prop_defs) == {
            "classification",
            "inactive",
        }, f"Expected classification + inactive property definitions, got {set(prop_defs)}"
        assert prop_defs["classification"].get("type") == "string"
        assert prop_defs["inactive"].get("type") == "boolean"

    @pytest.mark.skipif(
        os.environ.get("USE_REAL_DB_FOR_TESTS") != "true",
        reason="Requires USE_REAL_DB_FOR_TESTS=true",
    )
    def test_codesystem_concepts_have_hierarchy(
        self, staging_resource, sks_ext_rows, fhir_server_resource, aidbox_available
    ):
        """Verify that root concepts have nested children (hierarchy preserved)."""
        if not aidbox_available:
            pytest.skip("Aidbox not reachable")

        self._ensure_staging(staging_resource, sks_ext_rows)
        self._build_and_put(staging_resource, fhir_server_resource)

        url = f"{fhir_server_resource.effective_base_url}/CodeSystem/sks"
        resp = fhir_server_resource.session.get(url, timeout=60)
        cs = resp.json()

        roots_with_children = [
            c for c in cs.get("concept", []) if "concept" in c and len(c["concept"]) > 0
        ]
        assert (
            len(roots_with_children) > 0
        ), "Expected at least one root concept with nested children"

    @pytest.mark.skipif(
        os.environ.get("USE_REAL_DB_FOR_TESTS") != "true",
        reason="Requires USE_REAL_DB_FOR_TESTS=true",
    )
    def test_lookup_operation(
        self, staging_resource, sks_ext_rows, fhir_server_resource, aidbox_available
    ):
        """Test $lookup on a known SKS code via the /fhir/ terminology endpoint."""
        if not aidbox_available:
            pytest.skip("Aidbox not reachable")

        self._ensure_staging(staging_resource, sks_ext_rows)
        self._build_and_put(staging_resource, fhir_server_resource)

        # Terminology operations require the /fhir/ prefix in Aidbox.
        url = (
            f"{fhir_server_resource.base_url.rstrip('/')}/fhir"
            f"/CodeSystem/$lookup?system=urn:oid:1.2.208.176.2.4&code=DA01"
        )
        resp = fhir_server_resource.session.get(url, timeout=60)

        assert resp.status_code == 200, f"$lookup returned {resp.status_code}: {resp.text[:500]}"
        data = resp.json()
        # FHIR $lookup returns a Parameters resource
        assert data.get("resourceType") == "Parameters"


# ---------------------------------------------------------------------------
# TestDeleteStalePositive: verify delete_stale removes obsolete rows
# ---------------------------------------------------------------------------
class TestDeleteStalePositive:
    """Positive tests that delete_stale actually deletes removed/stale rows."""

    def _direct_merge(self, conn, concepts, properties, hierarchy) -> dict:
        """MERGE helper identical to _merge() at module level."""
        cursor = conn.cursor()
        try:
            result = merge_codesystem_tables(
                cursor,
                "sks",
                concepts,
                properties,
                hierarchy,
                multi_value_properties=True,
                delete_stale=True,
            )
            conn.commit()
        finally:
            cursor.close()
        return result

    def _truncate_codesystem_tables(self, conn) -> None:
        """Clear the three codesystem.sks_* tables before the test."""
        cursor = conn.cursor()
        try:
            for table in ("SKSHierarchy", "SKSProperties", "SKSConcept"):
                cursor.execute(f"DELETE FROM [codesystem].[{table}]")
            conn.commit()
        finally:
            cursor.close()

    def _row_exists(self, conn, table: str, where_sql: str, params: tuple) -> bool:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"SELECT COUNT(*) FROM [codesystem].[{table}] WHERE {where_sql}",
                params,
            )
            return cursor.fetchone()[0] > 0
        finally:
            cursor.close()

    @pytest.mark.integration
    def test_delete_stale_removes_dropped_concept_and_stale_property(self, staging_resource):
        """
        Positive delete_stale test:
        - Merge set-1 (3 codes: A01 inactive, A02 active, A03 active).
        - Re-merge set-2 where A03 is entirely removed and A01 flips active
          (so its stale 'inactive=true' property row must be deleted).
        - Assert concepts_deleted > 0, properties_deleted > 0, and that the
          removed rows are truly gone from the DB.
        """
        # ----------------------------------------------------------------
        # Set-1: three concepts; A01 is inactive
        # ----------------------------------------------------------------
        concepts_1 = [
            {"CODE": "A01", "DISPLAY": "Code A01"},
            {"CODE": "A02", "DISPLAY": "Code A02"},
            {"CODE": "A03", "DISPLAY": "Code A03"},
        ]
        properties_1 = [
            {"CODE": "A01", "KEY": "classification", "VALUE": "dia", "TYPE": "String"},
            {"CODE": "A01", "KEY": "inactive", "VALUE": "true", "TYPE": "Boolean"},
            {"CODE": "A02", "KEY": "classification", "VALUE": "dia", "TYPE": "String"},
            {"CODE": "A03", "KEY": "classification", "VALUE": "dia", "TYPE": "String"},
        ]
        hierarchy_1: list[dict] = []

        # ----------------------------------------------------------------
        # Set-2: A03 removed entirely; A01 is now active (no inactive prop)
        # ----------------------------------------------------------------
        concepts_2 = [
            {"CODE": "A01", "DISPLAY": "Code A01"},
            {"CODE": "A02", "DISPLAY": "Code A02"},
        ]
        properties_2 = [
            {"CODE": "A01", "KEY": "classification", "VALUE": "dia", "TYPE": "String"},
            # A01's inactive=true row is intentionally absent → should be deleted
            {"CODE": "A02", "KEY": "classification", "VALUE": "dia", "TYPE": "String"},
        ]
        hierarchy_2: list[dict] = []

        with staging_resource.get_connection() as conn:
            self._truncate_codesystem_tables(conn)

            # First merge: populate
            self._direct_merge(conn, concepts_1, properties_1, hierarchy_1)

            # Verify set-1 state
            assert self._row_exists(
                conn, "SKSConcept", "[code] = ?", ("A03",)
            ), "A03 should exist after first merge"
            assert self._row_exists(
                conn,
                "SKSProperties",
                "[code] = ? AND [property_code] = ? AND [value] = ?",
                ("A01", "inactive", "true"),
            ), "A01 inactive=true should exist after first merge"

            # Second merge: reduced set
            result2 = self._direct_merge(conn, concepts_2, properties_2, hierarchy_2)

            # ---- Assertions on merge result counters ----
            assert result2["concepts_deleted"] > 0, (
                f"Expected concepts_deleted > 0, got {result2['concepts_deleted']}. "
                "A03 should have been deleted."
            )
            assert result2["properties_deleted"] > 0, (
                f"Expected properties_deleted > 0, got {result2['properties_deleted']}. "
                "A01's stale 'inactive=true' row should have been deleted."
            )

            # ---- Assertions on actual DB state ----
            assert not self._row_exists(
                conn, "SKSConcept", "[code] = ?", ("A03",)
            ), "A03 should be absent after second merge"

            assert not self._row_exists(
                conn,
                "SKSProperties",
                "[code] = ? AND [property_code] = ? AND [value] = ?",
                ("A01", "inactive", "true"),
            ), "A01 stale inactive=true property should be absent after second merge"

            # A01 and A02 should still exist
            assert self._row_exists(
                conn, "SKSConcept", "[code] = ?", ("A01",)
            ), "A01 should still exist"
            assert self._row_exists(
                conn, "SKSConcept", "[code] = ?", ("A02",)
            ), "A02 should still exist"

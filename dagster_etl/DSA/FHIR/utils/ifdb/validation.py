# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Validation module to ensure all FHIR attributes defined in fhir.ifdb.yml are
properly extracted, mapped, and posted.

This module validates:
1. Extraction: All YAML columns are extracted from IFDB database
2. Mapping: All extracted columns are mapped to FHIR resource attributes
3. Posting: All transformed resources are posted to FHIR server
"""

import ast
import re
from pathlib import Path
from typing import Any

from DSA.FHIR.utils.ifdb.yaml_loader import (
    get_canonical_definition,
    get_column_mappings,
    is_derived_column,
    load_fhir_ifdb_yaml,
    parse_column_from_path,
)


class ValidationResult:
    """Container for validation results for a single canonical resource."""

    def __init__(self, canonical_name: str):
        self.canonical_name = canonical_name
        self.yaml_columns: set[str] = set()
        self.extracted_columns: set[str] = set()
        self.mapped_columns: set[str] = set()
        self.missing_extraction: set[str] = set()
        self.missing_mapping: set[str] = set()
        self.has_extraction_function: bool = False
        self.has_transform_function: bool = False
        self.has_posting_function: bool = False
        self.uses_yaml_approach: bool = False
        self.errors: list[str] = []

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "canonical_name": self.canonical_name,
            "yaml_column_count": len(self.yaml_columns),
            "extracted_column_count": len(self.extracted_columns),
            "mapped_column_count": len(self.mapped_columns),
            "missing_extraction": sorted(self.missing_extraction),
            "missing_mapping": sorted(self.missing_mapping),
            "has_extraction_function": self.has_extraction_function,
            "has_transform_function": self.has_transform_function,
            "has_posting_function": self.has_posting_function,
            "uses_yaml_approach": self.uses_yaml_approach,
            "errors": self.errors,
        }


class FHIRAttributeValidator:
    """Validates FHIR attribute extraction and mapping against YAML definitions."""

    def __init__(self):
        self.yaml_data = load_fhir_ifdb_yaml()
        self.canonical_resources = self.yaml_data.get("canonical", {})
        self.results: dict[str, ValidationResult] = {}

    def get_all_canonical_names(self) -> list[str]:
        """Get all canonical resource names from YAML."""
        return list(self.canonical_resources.keys())

    def validate_all(self) -> dict[str, ValidationResult]:
        """
        Validate all canonical resources.

        Returns:
            Dictionary mapping canonical names to ValidationResult objects
        """
        for canonical_name in self.get_all_canonical_names():
            self.results[canonical_name] = self.validate_resource(canonical_name)

        return self.results

    def validate_resource(self, canonical_name: str) -> ValidationResult:
        """
        Validate a single canonical resource.

        Args:
            canonical_name: Name of the canonical resource to validate

        Returns:
            ValidationResult with findings
        """
        result = ValidationResult(canonical_name)

        # Step 1: Get YAML column definitions
        canonical = get_canonical_definition(canonical_name)
        if not canonical:
            result.errors.append(f"Canonical resource '{canonical_name}' not found in YAML")
            return result

        columns = get_column_mappings(canonical_name)
        result.yaml_columns = set(columns.keys())

        # Step 2: Check extraction (SQL queries)
        self._validate_extraction(canonical_name, result, columns)

        # Step 3: Check mapping (transform functions)
        self._validate_mapping(canonical_name, result)

        # Step 4: Check posting (posting functions)
        self._validate_posting(canonical_name, result)

        return result

    def _validate_extraction(
        self,
        canonical_name: str,
        result: ValidationResult,
        columns: dict[str, dict[str, Any]],
    ):
        """Validate that all YAML columns are extracted from database."""
        # Find extraction function
        extraction_func = self._find_extraction_function(canonical_name)
        if not extraction_func:
            result.missing_extraction = result.yaml_columns.copy()
            return

        result.has_extraction_function = True
        source_code = extraction_func

        # Check if it uses YAML-based approach
        if "build_select_query_for_canonical" in source_code:
            result.uses_yaml_approach = True
            # YAML-based approach should extract all columns (except derived)
            for col_name, col_def in columns.items():
                if not is_derived_column(col_def):
                    col_from = col_def.get("from", "")
                    if col_from and col_from != "Derived":
                        result.extracted_columns.add(col_name)
        else:
            # Hardcoded SQL - analyze the query
            sql_columns = self._extract_columns_from_sql(source_code)

            # Map SQL column names to YAML column names
            # Create mapping: SQL column (uppercase) -> YAML column name
            sql_to_yaml = {}
            for yaml_col_name, col_def in columns.items():
                col_from = col_def.get("from", "")
                if col_from and col_from != "Derived":
                    try:
                        _, sql_col = parse_column_from_path(col_from)
                        # Normalize SQL column to lowercase for comparison
                        sql_to_yaml[sql_col.lower()] = yaml_col_name
                    except ValueError:
                        pass

            # Map extracted SQL columns to YAML columns
            for sql_col in sql_columns:
                yaml_col = sql_to_yaml.get(sql_col)
                if yaml_col:
                    result.extracted_columns.add(yaml_col)

        # Find missing columns
        # Only check non-derived columns
        for col_name, col_def in columns.items():
            if not is_derived_column(col_def):
                col_from = col_def.get("from", "")
                if col_from and col_from != "Derived":
                    if col_name not in result.extracted_columns:
                        result.missing_extraction.add(col_name)

    def _validate_mapping(self, canonical_name: str, result: ValidationResult):
        """Validate that all extracted columns are mapped in transform functions."""
        # Find transform function
        source_code = self._find_transform_function(canonical_name)
        if not source_code:
            result.missing_mapping = result.extracted_columns.copy()
            return

        result.has_transform_function = True

        # Analyze transform function to find which columns are used
        mapped_columns = self._extract_columns_from_transform(source_code, result.extracted_columns)
        result.mapped_columns = mapped_columns

        # Find missing mappings
        for col in result.extracted_columns:
            if col not in result.mapped_columns:
                result.missing_mapping.add(col)

    def _validate_posting(self, canonical_name: str, result: ValidationResult):
        """Validate that posting function exists."""
        posting_source = self._find_posting_function(canonical_name)
        result.has_posting_function = posting_source is not None

    def _find_extraction_function(self, canonical_name: str) -> str | None:
        """
        Find the extraction function source code for a canonical resource.

        Returns:
            Source code string if found, None otherwise
        """
        # Try common patterns:
        # - transformed_fhir_{canonical}_partitioned_ifdb
        # - transform_adapter_ifdb_{adapter_name}
        patterns = [
            f"transformed_fhir_{canonical_name}_partitioned_ifdb",
            f"transformed_fhir_{canonical_name}_partitioned",
            f"transform_adapter_ifdb_{canonical_name}",
        ]

        # Get adapter name
        canonical = get_canonical_definition(canonical_name)
        if canonical:
            sources = canonical.get("sources", [])
            if sources:
                adapter_name = sources[0]
                patterns.append(f"transform_adapter_ifdb_{adapter_name}")

        # Search in common locations
        search_paths = [
            Path(__file__).parent.parent / "assets",
            Path(__file__).parent.parent / "transforms",
        ]

        for search_path in search_paths:
            if not search_path.exists():
                continue
            for py_file in search_path.glob("*.py"):
                try:
                    with open(py_file, encoding="utf-8") as f:
                        content = f.read()
                        for pattern in patterns:
                            if f"def {pattern}" in content:
                                # Extract function source code
                                func_source = self._extract_function_source(content, pattern)
                                if func_source:
                                    return func_source
                except Exception:
                    continue

        return None

    def _find_transform_function(self, canonical_name: str) -> str | None:
        """
        Find the transform function source code for a canonical resource.

        Returns:
            Source code string if found, None otherwise
        """
        patterns = [
            f"transformed_fhir_{canonical_name}_partitioned_ifdb",
            f"transform_fhir_{canonical_name}",
        ]

        search_paths = [
            Path(__file__).parent.parent / "assets",
            Path(__file__).parent.parent / "transforms",
        ]

        for search_path in search_paths:
            if not search_path.exists():
                continue
            for py_file in search_path.glob("*.py"):
                try:
                    with open(py_file, encoding="utf-8") as f:
                        content = f.read()
                        for pattern in patterns:
                            if f"def {pattern}" in content:
                                func_source = self._extract_function_source(content, pattern)
                                if func_source:
                                    return func_source
                        # Also check for helper transform functions (e.g., _transform_patients_from_rows)
                        # These are called from the main transform function
                        for pattern in [
                            f"_transform_{canonical_name.replace('_ifdb', '')}_from_rows"
                        ]:
                            if f"def {pattern}" in content:
                                func_source = self._extract_function_source(content, pattern)
                                if func_source:
                                    return func_source
                except Exception:
                    continue

        return None

    def _find_posting_function(self, canonical_name: str) -> str | None:
        """
        Find the posting function source code for a canonical resource.

        Returns:
            Source code string if found, None otherwise
        """
        # Map canonical names to resource types for posting function names
        canonical = get_canonical_definition(canonical_name)
        if not canonical:
            return None

        resource_type = canonical.get("resource", "")
        if not resource_type:
            return None

        # Convert resource type to function name (e.g., "Patient" -> "patient", "EpisodeOfCare" -> "episodeofcare")
        resource_lower = resource_type.lower().replace("of", "").replace(" ", "")
        patterns = [
            f"{resource_lower}_fhir_server_partitioned_ifdb",
            f"{resource_lower}_fhir_server_partitioned",
        ]

        search_paths = [Path(__file__).parent.parent / "assets"]

        for search_path in search_paths:
            if not search_path.exists():
                continue
            for py_file in search_path.glob("*.py"):
                try:
                    with open(py_file, encoding="utf-8") as f:
                        content = f.read()
                        for pattern in patterns:
                            if f"def {pattern}" in content:
                                func_source = self._extract_function_source(content, pattern)
                                if func_source:
                                    return func_source
                except Exception:
                    continue

        return None

    def _extract_function_source(self, file_content: str, function_name: str) -> str | None:
        """
        Extract function source code from file content.

        Uses AST to find the function definition and extract its source.
        """
        try:
            tree = ast.parse(file_content)
            lines = file_content.split("\n")

            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name == function_name:
                    # Get start and end line numbers (1-indexed in AST, 0-indexed in list)
                    start_line = node.lineno - 1
                    end_line = node.end_lineno if hasattr(node, "end_lineno") else start_line + 1

                    # Extract function source
                    func_lines = lines[start_line:end_line]
                    return "\n".join(func_lines)
        except Exception:
            pass

        return None

    def _extract_columns_from_sql(self, source_code: str) -> set[str]:
        """Extract column names from SQL SELECT query in source code."""
        columns = set()

        # Find SQL query strings (look for triple-quoted strings with SELECT)
        # Match SELECT ... FROM patterns, handling multi-line queries
        sql_pattern = r"SELECT\s+(.*?)\s+FROM"
        matches = re.findall(sql_pattern, source_code, re.IGNORECASE | re.DOTALL)

        for match in matches:
            # Split by comma and clean up each column
            for line in match.split("\n"):
                line = line.strip()
                if not line or line.startswith("--"):  # Skip empty lines and comments
                    continue

                # Remove trailing comma
                line = line.rstrip(",")

                # Extract column after table alias (e.g., "c.PATIENT_ID" or "PATIENT_ID")
                if "." in line:
                    # Has table alias (e.g., "c.PATIENT_ID")
                    col_part = line.split(".")[-1].strip()
                else:
                    # No alias, just column name
                    col_part = line.strip()

                # Remove AS aliases if present
                if " AS " in col_part.upper():
                    col_part = col_part.split(" AS ")[0].strip()

                # Remove any function calls (e.g., "TRUNC(...)")
                col_part = re.sub(r"^\w+\s*\([^)]*\)", "", col_part).strip()

                # Strip trailing parentheses that may remain after function removal
                col_part = col_part.rstrip(")")

                # Extract final column name
                col_part = col_part.split()[0] if col_part.split() else col_part

                # Clean up quotes
                col_part = col_part.strip('"').strip("'").strip()

                # Normalize: Oracle columns are uppercase, YAML uses lowercase
                if col_part and col_part.isupper():
                    columns.add(col_part.lower())
                elif col_part:
                    columns.add(col_part.lower())

        return columns

    def _extract_columns_from_transform(
        self, source_code: str, extracted_columns: set[str]
    ) -> set[str]:
        """Extract which columns are used in transform function."""
        mapped_columns = set()

        # Look for column access patterns:
        # - row.get("column_name")
        # - get_column_value(row, "column_name")
        # - row["column_name"]
        # - Direct tuple unpacking (variable names that match column names)

        for col in extracted_columns:
            # Check various patterns
            patterns = [
                rf'get_column_value\([^,]+,\s*["\']{col}["\']',
                rf'row\.get\(["\']{col}["\']',
                rf'row\[["\']{col}["\']',
                # Tuple unpacking: match variable names (handle snake_case and variations)
                # e.g., patient_id, patient_ID, PATIENT_ID -> all should match patient_id
                rf"\b{col.replace('_', '_?')}\b",
            ]

            for pattern in patterns:
                if re.search(pattern, source_code, re.IGNORECASE):
                    mapped_columns.add(col)
                    break

        return mapped_columns

    def generate_report(self, output_dir: Path | None = None) -> tuple[str, dict[str, Any]]:
        """
        Generate validation report in both Markdown and JSON formats.

        Args:
            output_dir: Optional directory to write report files

        Returns:
            Tuple of (markdown_report, json_data)
        """
        if not self.results:
            self.validate_all()

        # Generate JSON data
        json_data = {
            "summary": self._generate_summary(),
            "resources": {name: result.to_dict() for name, result in self.results.items()},
        }

        # Generate Markdown report
        markdown = self._generate_markdown_report(json_data)

        # Write files if output directory provided
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
            with open(output_dir / "validation_report.md", "w", encoding="utf-8") as f:
                f.write(markdown)
            import json

            with open(output_dir / "validation_report.json", "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=2)

        return markdown, json_data

    def _generate_summary(self) -> dict[str, Any]:
        """Generate summary statistics."""
        total_resources = len(self.results)
        resources_with_extraction = sum(
            1 for r in self.results.values() if r.has_extraction_function
        )
        resources_with_transform = sum(1 for r in self.results.values() if r.has_transform_function)
        resources_with_posting = sum(1 for r in self.results.values() if r.has_posting_function)
        resources_using_yaml = sum(1 for r in self.results.values() if r.uses_yaml_approach)

        total_yaml_columns = sum(len(r.yaml_columns) for r in self.results.values())
        total_missing_extraction = sum(len(r.missing_extraction) for r in self.results.values())
        total_missing_mapping = sum(len(r.missing_mapping) for r in self.results.values())

        return {
            "total_resources": total_resources,
            "resources_with_extraction": resources_with_extraction,
            "resources_with_transform": resources_with_transform,
            "resources_with_posting": resources_with_posting,
            "resources_using_yaml": resources_using_yaml,
            "total_yaml_columns": total_yaml_columns,
            "total_missing_extraction": total_missing_extraction,
            "total_missing_mapping": total_missing_mapping,
        }

    def _generate_markdown_report(self, json_data: dict[str, Any]) -> str:
        """Generate human-readable Markdown report."""
        summary = json_data["summary"]
        resources = json_data["resources"]

        lines = ["# FHIR Attribute Extraction and Mapping Validation Report", ""]
        lines.append(f"**Generated:** {self._get_timestamp()}", "")
        lines.append("## Summary", "")
        lines.append(f"- Total Canonical Resources: {summary['total_resources']}")
        lines.append(
            f"- Resources with Extraction Functions: {summary['resources_with_extraction']}"
        )
        lines.append(f"- Resources with Transform Functions: {summary['resources_with_transform']}")
        lines.append(f"- Resources with Posting Functions: {summary['resources_with_posting']}")
        lines.append(f"- Resources Using YAML-Based Approach: {summary['resources_using_yaml']}")
        lines.append(f"- Total YAML Columns: {summary['total_yaml_columns']}")
        lines.append(f"- Total Missing Extraction: {summary['total_missing_extraction']}")
        lines.append(f"- Total Missing Mapping: {summary['total_missing_mapping']}")
        lines.append("")

        # Resource details
        lines.append("## Resource Details", "")
        for name, data in sorted(resources.items()):
            lines.append(f"### {name}", "")
            lines.append(f"- **YAML Columns:** {data['yaml_column_count']}")
            lines.append(f"- **Extracted Columns:** {data['extracted_column_count']}")
            lines.append(f"- **Mapped Columns:** {data['mapped_column_count']}")
            lines.append(
                f"- **Uses YAML Approach:** {'✅' if data['uses_yaml_approach'] else '❌'}"
            )
            lines.append(
                f"- **Has Extraction Function:** {'✅' if data['has_extraction_function'] else '❌'}"
            )
            lines.append(
                f"- **Has Transform Function:** {'✅' if data['has_transform_function'] else '❌'}"
            )
            lines.append(
                f"- **Has Posting Function:** {'✅' if data['has_posting_function'] else '❌'}"
            )

            if data["missing_extraction"]:
                lines.append(f"- **Missing Extraction ({len(data['missing_extraction'])}):**")
                for col in data["missing_extraction"][:10]:  # Limit to 10
                    lines.append(f"  - {col}")
                if len(data["missing_extraction"]) > 10:
                    lines.append(f"  ... and {len(data['missing_extraction']) - 10} more")

            if data["missing_mapping"]:
                lines.append(f"- **Missing Mapping ({len(data['missing_mapping'])}):**")
                for col in data["missing_mapping"][:10]:
                    lines.append(f"  - {col}")
                if len(data["missing_mapping"]) > 10:
                    lines.append(f"  ... and {len(data['missing_mapping']) - 10} more")

            if data["errors"]:
                lines.append("- **Errors:**")
                for error in data["errors"]:
                    lines.append(f"  - {error}")

            lines.append("")

        return "\n".join(lines)

    def _get_timestamp(self) -> str:
        """Get current timestamp as string."""
        from datetime import datetime

        return datetime.now().isoformat()


__all__ = ["FHIRAttributeValidator", "ValidationResult"]

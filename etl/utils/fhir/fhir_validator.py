# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

# utils/fhir/fhir_validator.py
"""
FHIR Profile and Resource Validation Tool
Validates FHIR specifications against official profiles and standards
"""

import sys
from pathlib import Path
from typing import Any

import requests
import yaml


class FHIRValidator:
    def __init__(self, config_path: str = "docs/fhir-references.yml"):
        self.config = self._load_config(config_path)
        self.validation_server = self.config["fhir_references"]["validation_servers"].get(
            "hl7_public", "https://hl7.org/fhir/validator"
        )

    def _load_config(self, path: str) -> dict[str, Any]:
        """Load FHIR reference configuration"""
        try:
            with open(path, encoding="utf-8") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Warning: Configuration file {path} not found. Using defaults.")
            return {
                "fhir_references": {
                    "validation_servers": {"hl7_public": "https://hl7.org/fhir/validator"}
                }
            }

    def validate_profile_compliance(self, spec_path: str) -> dict[str, Any]:
        """Validate that project specs comply with FHIR profiles"""
        results = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "profile_checks": {},
            "resource_checks": {},
        }

        try:
            with open(spec_path, encoding="utf-8") as f:
                spec = yaml.safe_load(f)
        except Exception as e:
            results["errors"].append(f"Error reading spec file {spec_path}: {e}")
            results["valid"] = False
            return results

        fhir_spec = spec.get("fhir", {})

        # Validate canonical resources
        canonical_resources = fhir_spec.get("canonical", {})
        for resource_name, resource_def in canonical_resources.items():
            resource_results = self._validate_resource_definition(resource_name, resource_def)
            results["resource_checks"][resource_name] = resource_results

            if not resource_results["valid"]:
                results["valid"] = False
                results["errors"].extend(resource_results["errors"])

            results["warnings"].extend(resource_results["warnings"])

        # Validate adapters
        adapters = fhir_spec.get("adapters", {})
        for adapter_name, adapter_def in adapters.items():
            adapter_results = self._validate_adapter_definition(adapter_name, adapter_def)
            results["resource_checks"][f"adapter_{adapter_name}"] = adapter_results

            if not adapter_results["valid"]:
                results["valid"] = False
                results["errors"].extend(adapter_results["errors"])

            results["warnings"].extend(adapter_results["warnings"])

        # Validate projections
        projections = fhir_spec.get("projections", {})
        for proj_name, proj_def in projections.items():
            proj_results = self._validate_projection_definition(
                proj_name, proj_def, canonical_resources
            )
            results["resource_checks"][f"proj_{proj_name}"] = proj_results

            if not proj_results["valid"]:
                results["valid"] = False
                results["errors"].extend(proj_results["errors"])

            results["warnings"].extend(proj_results["warnings"])

        return results

    def _validate_resource_definition(
        self, resource_name: str, resource_def: dict[str, Any]
    ) -> dict[str, Any]:
        """Validate a single resource definition"""
        results = {"valid": True, "errors": [], "warnings": []}

        # Check required fields
        required_fields = ["resource"]
        for field in required_fields:
            if field not in resource_def:
                results["errors"].append(f"Missing required field '{field}' in {resource_name}")
                results["valid"] = False

        # Validate resource type
        resource_type = resource_def.get("resource", "")
        if resource_type and not self._is_valid_fhir_resource_type(resource_type):
            results["warnings"].append(
                f"Unknown FHIR resource type '{resource_type}' in {resource_name}"
            )

        # Validate profile URL
        if "profile" in resource_def:
            profile_url = resource_def["profile"]
            if not self._is_valid_profile_url(profile_url):
                results["errors"].append(f"Invalid profile URL for {resource_name}: {profile_url}")
                results["valid"] = False
            else:
                # Check if profile is accessible
                if not self._validate_profile_accessibility(profile_url):
                    results["warnings"].append(
                        f"Profile URL may not be accessible for {resource_name}: {profile_url}"
                    )

        # Validate keys
        keys = resource_def.get("keys", [])
        if not keys:
            results["warnings"].append(f"No keys defined for {resource_name}")
        else:
            # Check that key columns exist
            columns = resource_def.get("columns", {})
            for key in keys:
                if key not in columns:
                    results["errors"].append(
                        f"Key '{key}' not found in columns for {resource_name}"
                    )
                    results["valid"] = False

        # Validate column definitions
        columns = resource_def.get("columns", {})
        for col_name, col_def in columns.items():
            if not isinstance(col_def, dict):
                results["errors"].append(
                    f"Column definition for '{col_name}' in {resource_name} must be a dictionary"
                )
                results["valid"] = False
                continue

            # Check for required type field
            if "type" not in col_def:
                results["warnings"].append(
                    f"Column '{col_name}' in {resource_name} missing type definition"
                )

            # Validate reference fields
            if "ref" in col_def:
                ref_type = col_def["ref"]
                if not self._is_valid_fhir_resource_type(ref_type):
                    results["warnings"].append(
                        f"Invalid reference type '{ref_type}' for column '{col_name}' in {resource_name}"
                    )

        # Validate sources
        sources = resource_def.get("sources", [])
        if sources:
            for source in sources:
                if not isinstance(source, str):
                    results["errors"].append(
                        f"Source '{source}' in {resource_name} must be a string"
                    )
                    results["valid"] = False

        return results

    def _validate_adapter_definition(
        self, adapter_name: str, adapter_def: dict[str, Any]
    ) -> dict[str, Any]:
        """Validate an adapter definition"""
        results = {"valid": True, "errors": [], "warnings": []}

        # Check required fields
        if "resource" not in adapter_def:
            results["errors"].append(f"Missing required field 'resource' in adapter {adapter_name}")
            results["valid"] = False

        # Validate source tables if present
        if "source_tables" in adapter_def:
            source_tables = adapter_def["source_tables"]
            if not isinstance(source_tables, list):
                results["errors"].append(f"source_tables in adapter {adapter_name} must be a list")
                results["valid"] = False

        return results

    def _validate_projection_definition(
        self,
        proj_name: str,
        proj_def: dict[str, Any],
        canonical_resources: dict[str, Any],
    ) -> dict[str, Any]:
        """Validate a projection definition"""
        results = {"valid": True, "errors": [], "warnings": []}

        # Check required 'from' field
        if "from" not in proj_def:
            results["errors"].append(f"Missing required field 'from' in projection {proj_name}")
            results["valid"] = False
        else:
            from_resource = proj_def["from"]
            # Check if the referenced resource exists (with or without fhir_ prefix)
            resource_exists = (
                from_resource in canonical_resources
                or f"fhir_{from_resource}" in canonical_resources
                or from_resource.replace("fhir_", "") in canonical_resources
            )
            if not resource_exists:
                results["errors"].append(
                    f"Projection {proj_name} references non-existent canonical resource '{from_resource}'"
                )
                results["valid"] = False

        # Validate columns
        columns = proj_def.get("columns", {})
        if not columns:
            results["warnings"].append(f"No columns defined for projection {proj_name}")

        return results

    def _is_valid_fhir_resource_type(self, resource_type: str) -> bool:
        """Check if resource type is a valid FHIR resource"""
        valid_resources = {
            "Patient",
            "Practitioner",
            "Organization",
            "Location",
            "Device",
            "Encounter",
            "EpisodeOfCare",
            "Condition",
            "Procedure",
            "Observation",
            "Medication",
            "MedicationRequest",
            "MedicationStatement",
            "MedicationAdministration",
            "DocumentReference",
            "Composition",
            "ServiceRequest",
            "Flag",
            "CarePlan",
            "CareTeam",
            "AllergyIntolerance",
            "DiagnosticReport",
            "ImagingStudy",
            "Immunization",
            "Specimen",
            "FamilyMemberHistory",
        }
        return resource_type in valid_resources

    def _is_valid_profile_url(self, url: str) -> bool:
        """Check if profile URL is valid"""
        valid_patterns = [
            "http://hl7.org/fhir/StructureDefinition/",
            "http://hl7.dk/fhir/core/StructureDefinition/",
            "https://hl7.org/fhir/StructureDefinition/",
            "https://hl7.dk/fhir/core/StructureDefinition/",
        ]
        return any(url.startswith(pattern) for pattern in valid_patterns)

    def _validate_profile_accessibility(self, profile_url: str) -> bool:
        """Check if profile URL is accessible"""
        try:
            response = requests.head(profile_url, timeout=10)
            return response.status_code == 200
        except Exception:
            return False

    def validate_all_specs(self) -> dict[str, dict[str, Any]]:
        """Validate all FHIR specification files in the project"""
        model_dir = Path("model/fhir")
        results = {}

        if not model_dir.exists():
            print("No model/fhir directory found")
            return results

        for spec_file in model_dir.glob("*.yml"):
            print(f"Validating {spec_file}...")
            results[str(spec_file)] = self.validate_profile_compliance(str(spec_file))

        return results

    def generate_validation_report(self, results: dict[str, dict[str, Any]]) -> str:
        """Generate a validation report"""
        report = ["# FHIR Validation Report", ""]

        total_files = len(results)
        valid_files = sum(1 for r in results.values() if r["valid"])
        total_errors = sum(len(r["errors"]) for r in results.values())
        total_warnings = sum(len(r["warnings"]) for r in results.values())

        report.extend(
            [
                "## Summary",
                f"- Files validated: {total_files}",
                f"- Valid files: {valid_files}",
                f"- Files with errors: {total_files - valid_files}",
                f"- Total errors: {total_errors}",
                f"- Total warnings: {total_warnings}",
                "",
            ]
        )

        # Detailed results
        for spec_file, result in results.items():
            file_name = Path(spec_file).name
            status = "✓" if result["valid"] else "✗"

            report.extend([f"## {status} {file_name}", ""])

            if result["errors"]:
                report.extend(["### Errors", ""])
                for error in result["errors"]:
                    report.append(f"- {error}")
                report.append("")

            if result["warnings"]:
                report.extend(["### Warnings", ""])
                for warning in result["warnings"]:
                    report.append(f"- {warning}")
                report.append("")

            # Resource-specific checks
            if result["resource_checks"]:
                report.extend(["### Resource Checks", ""])
                for resource_name, resource_result in result["resource_checks"].items():
                    resource_status = "✓" if resource_result["valid"] else "✗"
                    report.append(f"- {resource_status} {resource_name}")
                    if resource_result["errors"]:
                        for error in resource_result["errors"]:
                            report.append(f"  - Error: {error}")
                    if resource_result["warnings"]:
                        for warning in resource_result["warnings"]:
                            report.append(f"  - Warning: {warning}")
                report.append("")

        return "\n".join(report)


# CLI interface
def main():
    validator = FHIRValidator()

    if len(sys.argv) < 2:
        print("FHIR Validator")
        print("Usage:")
        print(
            "  python -m utils.fhir.fhir_validator validate                    # Validate all specs"
        )
        print(
            "  python -m utils.fhir.fhir_validator validate <spec_file>        # Validate specific file"
        )
        print(
            "  python -m utils.fhir.fhir_validator report                      # Generate validation report"
        )
        return

    command = sys.argv[1].lower()

    if command == "validate":
        if len(sys.argv) > 2:
            # Validate specific file
            spec_file = sys.argv[2]
            if not Path(spec_file).exists():
                print(f"File not found: {spec_file}")
                return

            print(f"Validating {spec_file}...")
            results = validator.validate_profile_compliance(spec_file)

            if results["valid"]:
                print("✓ Validation passed")
            else:
                print("✗ Validation failed")

            if results["errors"]:
                print("\nErrors:")
                for error in results["errors"]:
                    print(f"  - {error}")

            if results["warnings"]:
                print("\nWarnings:")
                for warning in results["warnings"]:
                    print(f"  - {warning}")
        else:
            # Validate all files
            results = validator.validate_all_specs()

            for spec_file, result in results.items():
                file_name = Path(spec_file).name
                status = "✓" if result["valid"] else "✗"
                print(f"{status} {file_name}")

            # Generate and save report
            report = validator.generate_validation_report(results)
            report_file = Path("docs/fhir-validation-report.md")
            report_file.parent.mkdir(exist_ok=True)

            with open(report_file, "w", encoding="utf-8") as f:
                f.write(report)

            print(f"\nDetailed report saved to: {report_file}")

    elif command == "report":
        results = validator.validate_all_specs()
        report = validator.generate_validation_report(results)
        print(report)

    else:
        print(f"Unknown command: {command}")
        print("Use 'python -m utils.fhir.fhir_validator' for usage information")


if __name__ == "__main__":
    main()

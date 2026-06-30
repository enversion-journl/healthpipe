# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

# utils/fhir/fhir_helper.py
"""
FHIR Development Helper - Quick access to FHIR specifications and profiles
"""

import json
import sys
import webbrowser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
import yaml


class FHIRHelper:
    def __init__(self, config_path: str = "docs/fhir-references.yml"):
        self.config = self._load_config(config_path)
        self.cache_dir = Path("docs/fhir-cache")
        self.cache_dir.mkdir(exist_ok=True)

    def _load_config(self, path: str) -> dict[str, Any]:
        """Load FHIR reference configuration"""
        try:
            with open(path, encoding="utf-8") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Warning: Configuration file {path} not found. Using defaults.")
            return self._get_default_config()

    def _get_default_config(self) -> dict[str, Any]:
        """Default configuration if file not found"""
        return {
            "fhir_references": {
                "hl7_fhir": {
                    "base_url": "https://build.fhir.org",
                    "profiles": {"r4": "https://hl7.org/fhir/R4/"},
                },
                "dk_core": {"base_url": "https://hl7.dk/fhir/core", "profiles": {}},
            }
        }

    def open_profile(self, profile_url: str, version: str = "latest"):
        """Open FHIR profile in browser"""
        if "hl7.dk" in profile_url:
            # Danish core profile
            profile_name = profile_url.split("/")[-1]
            if "|" in profile_name:
                profile_name = profile_name.split("|")[0]
            url = urljoin(
                self.config["fhir_references"]["dk_core"]["base_url"],
                f"StructureDefinition/{profile_name}",
            )
        elif "hl7.org" in profile_url:
            # Standard FHIR profile
            profile_name = profile_url.split("/")[-1]
            if "|" in profile_name:
                profile_name = profile_name.split("|")[0]
            url = urljoin(
                self.config["fhir_references"]["hl7_fhir"]["profiles"]["r4"],
                f"StructureDefinition/{profile_name}",
            )
        else:
            url = profile_url

        print(f"Opening: {url}")
        webbrowser.open(url)
        return url

    def open_resource_docs(self, resource_type: str):
        """Open FHIR resource documentation"""
        resource_type = resource_type.lower()
        hl7_docs = self.config["fhir_references"]["hl7_fhir"].get("structure_definitions", {})

        if resource_type in hl7_docs:
            url = hl7_docs[resource_type]
        else:
            # Fallback to generic FHIR R4 resource page
            url = f"https://hl7.org/fhir/R4/{resource_type}.html"

        print(f"Opening {resource_type} documentation: {url}")
        webbrowser.open(url)
        return url

    def validate_profile_reference(self, profile_url: str) -> bool:
        """Validate that a profile URL exists and is accessible"""
        try:
            response = requests.head(profile_url, timeout=10)
            return response.status_code == 200
        except Exception as e:
            print(f"Validation error for {profile_url}: {e}")
            return False

    def get_profile_info(self, profile_url: str) -> dict[str, Any]:
        """Get profile information from URL"""
        cache_file = self.cache_dir / f"{profile_url.replace('/', '_').replace('|', '_')}.json"

        if cache_file.exists():
            try:
                with open(cache_file, encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error reading cache file: {e}")

        try:
            response = requests.get(profile_url, timeout=10)
            if response.status_code == 200:
                profile_data = response.json()
                try:
                    with open(cache_file, "w", encoding="utf-8") as f:
                        json.dump(profile_data, f, indent=2, ensure_ascii=False)
                except Exception as e:
                    print(f"Warning: Could not cache profile data: {e}")
                return profile_data
        except Exception as e:
            print(f"Error fetching profile: {e}")

        return {}

    def list_project_profiles(self) -> dict[str, str]:
        """List all FHIR profiles used in this project"""
        profiles = {}
        model_dir = Path("model/fhir")

        if not model_dir.exists():
            print("No model/fhir directory found")
            return profiles

        for spec_file in model_dir.glob("*.yml"):
            try:
                with open(spec_file, encoding="utf-8") as f:
                    spec = yaml.safe_load(f)
                    for resource_name, resource_def in (
                        spec.get("fhir", {}).get("canonical", {}).items()
                    ):
                        if "profile" in resource_def:
                            profiles[resource_name] = resource_def["profile"]
            except Exception as e:
                print(f"Error reading {spec_file}: {e}")

        return profiles

    def list_project_resources(self) -> dict[str, dict[str, Any]]:
        """List all FHIR resources defined in this project"""
        resources = {}
        model_dir = Path("model/fhir")

        if not model_dir.exists():
            return resources

        for spec_file in model_dir.glob("*.yml"):
            try:
                with open(spec_file, encoding="utf-8") as f:
                    spec = yaml.safe_load(f)
                    for resource_name, resource_def in (
                        spec.get("fhir", {}).get("canonical", {}).items()
                    ):
                        resources[resource_name] = {
                            "resource_type": resource_def.get("resource", "Unknown"),
                            "profile": resource_def.get("profile", "No profile"),
                            "keys": resource_def.get("keys", []),
                            "sources": resource_def.get("sources", []),
                        }
            except Exception as e:
                print(f"Error reading {spec_file}: {e}")

        return resources

    def validate_all_profiles(self) -> dict[str, bool]:
        """Validate all project profiles"""
        profiles = self.list_project_profiles()
        results = {}

        for name, url in profiles.items():
            print(f"Validating {name}...")
            results[name] = self.validate_profile_reference(url)

        return results

    def get_terminology_info(self, code_system: str) -> dict[str, Any]:
        """Get information about a terminology/code system"""
        terminologies = self.config["fhir_references"].get("terminologies", {})

        # Find matching terminology
        for name, url in terminologies.items():
            if code_system.lower() in name.lower() or url in code_system:
                return {
                    "name": name,
                    "url": url,
                    "description": self._get_terminology_description(name),
                }

        return {
            "name": "Unknown",
            "url": code_system,
            "description": "Unknown terminology",
        }

    def _get_terminology_description(self, terminology: str) -> str:
        """Get description for common terminologies"""
        descriptions = {
            "snomed_ct": "Systematized Nomenclature of Medicine Clinical Terms",
            "icd_10_dk": "International Classification of Diseases 10th Revision - Denmark",
            "atc": "Anatomical Therapeutic Chemical Classification System",
            "sks": "Sundhedsvæsenets Klassifikationssystem",
            "loinc": "Logical Observation Identifiers Names and Codes",
        }
        return descriptions.get(terminology, "Unknown terminology")

    def generate_profile_report(self) -> str:
        """Generate a comprehensive profile report"""
        profiles = self.list_project_profiles()
        resources = self.list_project_resources()
        validation_results = self.validate_all_profiles()

        report = ["# FHIR Project Profile Report", ""]

        # Summary
        total_profiles = len(profiles)
        valid_profiles = sum(1 for v in validation_results.values() if v)
        report.extend(
            [
                "## Summary",
                f"- Total profiles: {total_profiles}",
                f"- Valid profiles: {valid_profiles}",
                f"- Invalid profiles: {total_profiles - valid_profiles}",
                "",
            ]
        )

        # Detailed profile information
        report.extend(["## Profile Details", ""])
        for name, url in profiles.items():
            resource_info = resources.get(name, {})
            is_valid = validation_results.get(name, False)
            status = "✓" if is_valid else "✗"

            report.extend(
                [
                    f"### {status} {name}",
                    f"- **Resource Type**: {resource_info.get('resource_type', 'Unknown')}",
                    f"- **Profile URL**: [{url}]({url})",
                    f"- **Keys**: {', '.join(resource_info.get('keys', []))}",
                    f"- **Sources**: {', '.join(resource_info.get('sources', []))}",
                    "",
                ]
            )

        return "\n".join(report)


# CLI interface
def main():
    helper = FHIRHelper()

    if len(sys.argv) < 2:
        print("FHIR Development Helper")
        print("Usage:")
        print("  python -m utils.fhir.fhir_helper list                    # List project profiles")
        print("  python -m utils.fhir.fhir_helper resources               # List project resources")
        print(
            "  python -m utils.fhir.fhir_helper open <profile_name>     # Open profile in browser"
        )
        print(
            "  python -m utils.fhir.fhir_helper docs <resource_type>    # Open resource documentation"
        )
        print(
            "  python -m utils.fhir.fhir_helper validate                # Validate all profile URLs"
        )
        print(
            "  python -m utils.fhir.fhir_helper report                  # Generate profile report"
        )
        print(
            "  python -m utils.fhir.fhir_helper info <profile_name>     # Get profile information"
        )
        return

    command = sys.argv[1].lower()

    if command == "list":
        profiles = helper.list_project_profiles()
        if profiles:
            print("Project FHIR Profiles:")
            for name, url in profiles.items():
                print(f"  {name}: {url}")
        else:
            print("No FHIR profiles found in project")

    elif command == "resources":
        resources = helper.list_project_resources()
        if resources:
            print("Project FHIR Resources:")
            for name, info in resources.items():
                print(f"  {name}:")
                print(f"    Type: {info['resource_type']}")
                print(f"    Profile: {info['profile']}")
                print(f"    Keys: {info['keys']}")
                print(f"    Sources: {info['sources']}")
        else:
            print("No FHIR resources found in project")

    elif command == "open" and len(sys.argv) > 2:
        profile_name = sys.argv[2]
        profiles = helper.list_project_profiles()
        if profile_name in profiles:
            helper.open_profile(profiles[profile_name])
        else:
            print(f"Profile '{profile_name}' not found")
            print("Available profiles:", list(profiles.keys()))

    elif command == "docs" and len(sys.argv) > 2:
        resource_type = sys.argv[2]
        helper.open_resource_docs(resource_type)

    elif command == "validate":
        print("Validating FHIR profiles...")
        results = helper.validate_all_profiles()
        for name, is_valid in results.items():
            status = "✓" if is_valid else "✗"
            print(f"{status} {name}")

    elif command == "report":
        report = helper.generate_profile_report()
        print(report)

        # Save report to file
        report_file = Path("docs/fhir-profile-report.md")
        report_file.parent.mkdir(exist_ok=True)
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"\nReport saved to: {report_file}")

    elif command == "info" and len(sys.argv) > 2:
        profile_name = sys.argv[2]
        profiles = helper.list_project_profiles()
        if profile_name in profiles:
            profile_url = profiles[profile_name]
            info = helper.get_profile_info(profile_url)
            if info:
                print(f"Profile information for {profile_name}:")
                print(json.dumps(info, indent=2, ensure_ascii=False))
            else:
                print(f"Could not retrieve profile information for {profile_name}")
        else:
            print(f"Profile '{profile_name}' not found")

    else:
        print(f"Unknown command: {command}")
        print("Use 'python -m utils.fhir.fhir_helper' for usage information")


if __name__ == "__main__":
    main()

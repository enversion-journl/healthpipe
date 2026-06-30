# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Bulk Import module for high-performance data loading.

This module provides support for the FHIR $import operation, which enables
importing large volumes of resources using NDJSON format. This is significantly
faster than transaction bundles for initial bulk loads.
"""

import json
import time
from dataclasses import dataclass
from typing import Any

import dagster as dg
import requests
from fhir.resources.R4B.resource import Resource


@dataclass
class BulkImportResult:
    """Result of a bulk import operation."""

    success: bool
    total_resources: int
    resources_by_type: dict[str, int]
    import_time_seconds: float
    error_message: str | None = None
    operation_outcome: dict[str, Any] | None = None


class FHIRBulkImporter:
    """
    High-performance bulk import using FHIR $import operation.

    The $import operation allows importing resources in NDJSON format,
    which is significantly faster than transaction bundles for large datasets.
    """

    def __init__(
        self,
        base_url: str,
        session: requests.Session,
        auth: tuple[str, str] | None = None,
    ):
        """
        Initialize the bulk importer.

        Args:
            base_url: FHIR server base URL
            session: Requests session for connection pooling
            auth: Optional (username, password) tuple for basic auth
        """
        self.base_url = base_url.rstrip("/")
        self.session = session
        self.auth = auth

    def import_resources(
        self,
        resources: list[Resource],
        context: dg.AssetExecutionContext | None = None,
        mode: str = "upsert",
        poll_interval: int = 5,
        max_wait: int = 3600,
    ) -> BulkImportResult:
        """
        Import resources using FHIR $import operation.

        For massive datasets, this is 10-100x faster than transaction bundles.

        Args:
            resources: List of FHIR resources to import
            context: Optional Dagster execution context for logging
            mode: Import mode - "create" (new only) or "upsert" (create or update)
            poll_interval: Seconds between status polls for async operations
            max_wait: Maximum seconds to wait for async completion

        Returns:
            BulkImportResult with import statistics
        """
        if not resources:
            return BulkImportResult(
                success=True,
                total_resources=0,
                resources_by_type={},
                import_time_seconds=0.0,
            )

        logger = dg.get_dagster_logger()
        start_time = time.time()

        # Group resources by type
        by_type: dict[str, list[dict[str, Any]]] = {}
        for resource in resources:
            resource_type = resource.get_resource_type()
            if resource_type not in by_type:
                by_type[resource_type] = []
            by_type[resource_type].append(json.loads(resource.json(exclude_none=True)))

        resources_by_type = {rt: len(items) for rt, items in by_type.items()}
        logger.info(
            f"Preparing bulk import for {len(resources)} resources "
            f"across {len(by_type)} types: {resources_by_type}"
        )

        # Build $import Parameters resource
        import_params = self._build_import_parameters(by_type, mode)

        # Submit $import operation
        url = f"{self.base_url}/$import"
        headers = {
            "Content-Type": "application/fhir+json",
            "Accept": "application/fhir+json",
            "Prefer": "respond-async",  # Request async processing
        }

        try:
            response = self.session.post(
                url,
                json=import_params,
                headers=headers,
                auth=self.auth,
            )

            if response.status_code == 202:
                # Async accepted - poll for completion
                content_location = response.headers.get("Content-Location")
                if content_location:
                    logger.info(f"Bulk import accepted, polling status at: {content_location}")
                    result = self._poll_import_status(
                        content_location,
                        context,
                        poll_interval,
                        max_wait,
                    )
                    elapsed = time.time() - start_time
                    return BulkImportResult(
                        success=True,
                        total_resources=len(resources),
                        resources_by_type=resources_by_type,
                        import_time_seconds=elapsed,
                        operation_outcome=result,
                    )
                else:
                    logger.warning("Async import accepted but no Content-Location header")

            elif response.status_code == 200:
                # Synchronous completion
                elapsed = time.time() - start_time
                logger.info(f"Bulk import completed synchronously in {elapsed:.2f}s")
                return BulkImportResult(
                    success=True,
                    total_resources=len(resources),
                    resources_by_type=resources_by_type,
                    import_time_seconds=elapsed,
                    operation_outcome=response.json() if response.text else None,
                )

            else:
                # Error
                elapsed = time.time() - start_time
                error_msg = f"Import failed: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return BulkImportResult(
                    success=False,
                    total_resources=len(resources),
                    resources_by_type=resources_by_type,
                    import_time_seconds=elapsed,
                    error_message=error_msg,
                )

        except Exception as e:
            elapsed = time.time() - start_time
            error_msg = f"Import request failed: {str(e)}"
            logger.error(error_msg)
            return BulkImportResult(
                success=False,
                total_resources=len(resources),
                resources_by_type=resources_by_type,
                import_time_seconds=elapsed,
                error_message=error_msg,
            )

        # Fallback if no clear response
        elapsed = time.time() - start_time
        return BulkImportResult(
            success=True,
            total_resources=len(resources),
            resources_by_type=resources_by_type,
            import_time_seconds=elapsed,
        )

    def _build_import_parameters(
        self,
        resources_by_type: dict[str, list[dict[str, Any]]],
        mode: str,
    ) -> dict[str, Any]:
        """
        Build FHIR Parameters resource for $import operation.

        Args:
            resources_by_type: Dictionary mapping resource type to list of resources
            mode: Import mode ("create" or "upsert")

        Returns:
            Parameters resource as dictionary
        """
        import_params: dict[str, Any] = {
            "resourceType": "Parameters",
            "parameter": [],
        }

        for resource_type, type_resources in resources_by_type.items():
            # Convert resources to NDJSON format
            ndjson_lines = [json.dumps(r, separators=(",", ":")) for r in type_resources]
            ndjson_content = "\n".join(ndjson_lines)

            import_params["parameter"].append(
                {
                    "name": "input",
                    "part": [
                        {"name": "type", "valueString": resource_type},
                        {"name": "mode", "valueString": mode},
                        {"name": "data", "valueString": ndjson_content},
                    ],
                }
            )

        return import_params

    def _poll_import_status(
        self,
        status_url: str,
        context: dg.AssetExecutionContext | None,
        poll_interval: int,
        max_wait: int,
    ) -> dict[str, Any] | None:
        """
        Poll async import operation until complete.

        Args:
            status_url: URL to poll for status
            context: Optional execution context for logging
            poll_interval: Seconds between polls
            max_wait: Maximum seconds to wait

        Returns:
            Final response data or None

        Raises:
            TimeoutError: If import does not complete within max_wait
        """
        logger = dg.get_dagster_logger()
        start = time.time()
        poll_count = 0

        while time.time() - start < max_wait:
            poll_count += 1
            try:
                response = self.session.get(
                    status_url,
                    headers={"Accept": "application/fhir+json"},
                    auth=self.auth,
                )

                if response.status_code == 200:
                    # Complete
                    elapsed = time.time() - start
                    logger.info(
                        f"Bulk import completed after {poll_count} polls " f"({elapsed:.2f}s)"
                    )
                    return response.json() if response.text else None

                elif response.status_code == 202:
                    # Still processing
                    if context:
                        context.log.info(
                            f"Import still processing (poll {poll_count}), "
                            f"waiting {poll_interval}s..."
                        )
                    else:
                        logger.info(
                            f"Import still processing (poll {poll_count}), "
                            f"waiting {poll_interval}s..."
                        )
                    time.sleep(poll_interval)

                else:
                    # Unexpected status
                    logger.error(
                        f"Import status check returned {response.status_code}: " f"{response.text}"
                    )
                    return None

            except Exception as e:
                logger.warning(f"Poll {poll_count} failed: {e}, retrying...")
                time.sleep(poll_interval)

        raise TimeoutError(f"Bulk import did not complete within {max_wait}s")

    def import_from_ndjson_file(
        self,
        file_path: str,
        resource_type: str,
        mode: str = "upsert",
        context: dg.AssetExecutionContext | None = None,
    ) -> BulkImportResult:
        """
        Import resources from an NDJSON file.

        This is useful for very large datasets that are pre-staged as files.

        Args:
            file_path: Path to NDJSON file
            resource_type: FHIR resource type in the file
            mode: Import mode ("create" or "upsert")
            context: Optional execution context for logging

        Returns:
            BulkImportResult with import statistics
        """
        logger = dg.get_dagster_logger()
        start_time = time.time()

        try:
            with open(file_path) as f:
                ndjson_content = f.read()

            # Count resources
            line_count = len([line for line in ndjson_content.split("\n") if line.strip()])
            logger.info(f"Importing {line_count} {resource_type} resources from {file_path}")

            # Build parameters with file content
            import_params: dict[str, Any] = {
                "resourceType": "Parameters",
                "parameter": [
                    {
                        "name": "input",
                        "part": [
                            {"name": "type", "valueString": resource_type},
                            {"name": "mode", "valueString": mode},
                            {"name": "data", "valueString": ndjson_content},
                        ],
                    }
                ],
            }

            # Submit import
            url = f"{self.base_url}/$import"
            response = self.session.post(
                url,
                json=import_params,
                headers={
                    "Content-Type": "application/fhir+json",
                    "Accept": "application/fhir+json",
                },
                auth=self.auth,
            )

            elapsed = time.time() - start_time

            if response.status_code in (200, 201):
                return BulkImportResult(
                    success=True,
                    total_resources=line_count,
                    resources_by_type={resource_type: line_count},
                    import_time_seconds=elapsed,
                    operation_outcome=response.json() if response.text else None,
                )
            else:
                return BulkImportResult(
                    success=False,
                    total_resources=line_count,
                    resources_by_type={resource_type: line_count},
                    import_time_seconds=elapsed,
                    error_message=f"Import failed: {response.status_code} - {response.text}",
                )

        except Exception as e:
            elapsed = time.time() - start_time
            return BulkImportResult(
                success=False,
                total_resources=0,
                resources_by_type={},
                import_time_seconds=elapsed,
                error_message=str(e),
            )

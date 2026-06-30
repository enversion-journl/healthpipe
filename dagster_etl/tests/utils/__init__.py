# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Test utilities and helpers.

This package provides:
- materialize_helpers: Asset materialization utilities
- verification_helpers: Output verification utilities
- query_helpers: SQL query modification utilities
- staging_helpers: Staging table validation utilities
"""

from .materialize_helpers import (
    get_materialization_result,
    materialize_asset,
    materialize_assets_with_dependencies,
    materialize_erp_pipeline,
    materialize_patient_pipeline,
)
from .query_helpers import (
    add_patient_id_filter,
    create_test_query,
    extract_query_patient_id,
)
from .staging_helpers import (
    find_test_record_by_value,
    query_and_find_test_record,
    query_staging_table,
    validate_clean_code_generated,
    validate_source_system,
)
from .verification_helpers import (
    assert_fhir_resource_valid,
    verify_database_output,
    verify_fhir_encounter,
    verify_fhir_episode_of_care,
    verify_fhir_location,
    verify_fhir_observation,
    verify_fhir_patient,
    verify_fhir_resource,
    verify_pipeline_output,
)

__all__ = [
    # Materialize helpers
    "materialize_asset",
    "materialize_assets_with_dependencies",
    "materialize_patient_pipeline",
    "materialize_erp_pipeline",
    "get_materialization_result",
    # Verification helpers
    "assert_fhir_resource_valid",
    "verify_fhir_resource",
    "verify_fhir_patient",
    "verify_fhir_observation",
    "verify_fhir_encounter",
    "verify_fhir_episode_of_care",
    "verify_fhir_location",
    "verify_pipeline_output",
    "verify_database_output",
    # Query helpers
    "add_patient_id_filter",
    "create_test_query",
    "extract_query_patient_id",
    # Staging helpers
    "query_staging_table",
    "find_test_record_by_value",
    "query_and_find_test_record",
    "validate_clean_code_generated",
    "validate_source_system",
]

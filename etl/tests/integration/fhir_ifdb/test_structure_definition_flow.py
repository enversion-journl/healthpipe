# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for structure definition pipeline flow.

These tests can run with either mocked or real resources depending on configuration.
"""

import logging

import pytest
from tests.utils.materialize_helpers import (
    materialize_asset,
)

logger = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.structure_definition_test
@pytest.mark.order(1)
def test_fhir_server_structure_definition_insert_real(
    dagster_instance,
    real_fhir_server_resource,
):
    """
    Test inserting StructureDefinition resources into FHIR server.
    """
    # Materialize structure definitions
    result = materialize_asset(
        instance=dagster_instance,
        asset_key="fhir_structure_definitions",
        resources={
            "fhir_server": real_fhir_server_resource,
        },
    )

    # Verify pipeline succeeded
    assert result is not None

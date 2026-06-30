# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fixtures for ref.* integration tests (ref.Organisation, ref.Patient_Borger, etc.).

Provides a short-named ``mssql_target_staging`` fixture that delegates to the
centralized ``real_mssql_target_staging_resource`` from tests/fixtures/resources.py.
"""

import pytest


@pytest.fixture(scope="module")
def mssql_target_staging(real_mssql_target_staging_resource):
    """Alias so ref integration tests can request ``mssql_target_staging``."""
    return real_mssql_target_staging_resource

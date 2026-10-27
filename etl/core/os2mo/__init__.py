# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""OS2mo-specific implementation behind ``core.resources.OS2moResource``.

Pipeline code keeps importing the Dagster resource from ``core.resources``; the
modules here hold the transport details (Keycloak client-credentials tokens,
the pinned GraphQL endpoint, retry classification and cursor pagination) so
``resources.py`` stays a thin facade — the same split as ``core.fhir``.
"""

from core.os2mo.client import (
    OS2moClient,
    OS2moConfigurationError,
    build_graphql_url,
)

__all__ = [
    "OS2moClient",
    "OS2moConfigurationError",
    "build_graphql_url",
]

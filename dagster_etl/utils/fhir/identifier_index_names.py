# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Helpers for building Postgres index names from FHIR identifier systems."""

import re


def sanitize_system_for_identifier(identifier_system: str) -> str:
    """Sanitize an identifier system URI into a safe SQL identifier fragment.

    Replaces any non-alphanumeric character with ``_`` so the result is safe
    to embed in a Postgres index name without quoting.
    """
    return re.sub(r"[^A-Za-z0-9]+", "_", identifier_system).strip("_")

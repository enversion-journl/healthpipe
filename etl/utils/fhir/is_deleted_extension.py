# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unified IFDB deletion marker as a FHIR extension (urn:fhir:isDeleted).

Same URL on Patient, Encounter, EpisodeOfCare, and Observation; quality checks
set valueInteger=1 for orphans. Kept separate from fhir_extensions.py (URL-only constants).
"""

from __future__ import annotations

from typing import Any

from fhir.resources.R4B.extension import Extension

# valueInteger: 0 = not deleted, 1 = deleted (IFDB flag or quality-check orphan).
EXTENSION_FHIR_IS_DELETED = "urn:fhir:isDeleted"


def build_is_deleted_extension(deleted: int) -> Extension:
    """Build the unified isDeleted extension for IFDB resources and quality checks."""
    return Extension(url=EXTENSION_FHIR_IS_DELETED, valueInteger=int(deleted))


def build_is_deleted_extension_dict(deleted: int) -> dict[str, Any]:
    """Build the unified isDeleted extension as a plain dict for dict-based pipelines."""
    return {"url": EXTENSION_FHIR_IS_DELETED, "valueInteger": int(deleted)}

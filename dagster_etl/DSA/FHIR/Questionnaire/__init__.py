# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Questionnaire resource builders for IFDB AKTIVITETSRESULTAT data.

This package exposes the public builder API. Dagster asset registration lives
in the IFDB AKTIVITETSRESULTAT domain package to avoid duplicate wiring.
"""

from DSA.FHIR.Questionnaire.Questionnaire_IFDB import (
    QUESTIONNAIRE_CANONICAL_PREFIX,
    QUESTIONNAIRE_IDENTIFIER_SYSTEM,
    build_questionnaire_canonical,
    build_questionnaire_dict,
    build_questionnaire_from_xml,
    build_questionnaire_identifier_value,
    build_questionnaire_items_dict,
    build_questionnaire_version_pinned_reference,
)

# Asset registration is handled by the IFDB AKTIVITETSRESULTAT package.
# Keeping ``all_assets`` empty avoids duplicate wiring from this package.
all_assets: list = []

__all__ = [
    "QUESTIONNAIRE_CANONICAL_PREFIX",
    "QUESTIONNAIRE_IDENTIFIER_SYSTEM",
    "all_assets",
    "build_questionnaire_canonical",
    "build_questionnaire_dict",
    "build_questionnaire_from_xml",
    "build_questionnaire_identifier_value",
    "build_questionnaire_items_dict",
    "build_questionnaire_version_pinned_reference",
]

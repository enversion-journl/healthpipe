# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""QuestionnaireResponse resource builders for IFDB data."""

from DSA.FHIR.QuestionnaireResponse.QuestionnaireResponse_IFDB import (
    build_qr_identifier_value,
    build_questionnaire_response_dict,
    build_questionnaire_response_from_xml,
    build_questionnaire_response_items_dict,
)
from DSA.FHIR.QuestionnaireResponse.QuestionnaireResponse_IFDB_assets import (
    apply_resolved_refs,
    merge_flat_questionnaire_items,
    resolve_reference_maps,
    resolve_row_route,
)

# QuestionnaireResponse-pipelinen produceres i DSA/FHIR/Aktivitetsresultat/.
all_assets: list = []

__all__ = [
    "all_assets",
    "apply_resolved_refs",
    "build_qr_identifier_value",
    "build_questionnaire_response_dict",
    "build_questionnaire_response_from_xml",
    "build_questionnaire_response_items_dict",
    "merge_flat_questionnaire_items",
    "resolve_reference_maps",
    "resolve_row_route",
]

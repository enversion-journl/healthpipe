# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL on FHIR: Questionnaire / QuestionnaireResponse extraction using DFM checkpoints.

Both resource families are produced solely by the Aktivitetsresultat/SFI pipeline,
which runs on the DFM path — so the SOF and DSO layers run with it
instead of staying in the partitioned daily job.

Ordering inside the family is preserved from the partitioned assets: the *_item
child views follow their header, and questionnaire_response_item additionally
follows questionnaire_item (the item_key join target must be populated first).
"""

from DSA.FHIR.utils.sql_on_fhir.sof_dfm_factory import create_sof_dfm_asset

_SFI_PRODUCERS = [
    "fhir_sof_infrastructure",
    "fhir_server_aktivitetsresultat_dfm",
    "fhir_server_questionnaire_dfm",
    "fhir_server_questionnaire_response_dfm",
]

sof_questionnaire_dfm = create_sof_dfm_asset(
    asset_name="sof_questionnaire_dfm",
    config_key="questionnaire_view",
    checkpoint_resource="Questionnaire",
    deps=["fhir_sof_infrastructure", "fhir_server_questionnaire_dfm"],
)

sof_questionnaire_item_dfm = create_sof_dfm_asset(
    asset_name="sof_questionnaire_item_dfm",
    config_key="questionnaire_item_view",
    checkpoint_resource="Questionnaire_item",
    deps=["fhir_sof_infrastructure", "sof_questionnaire_dfm"],
)

sof_questionnaire_response_dfm = create_sof_dfm_asset(
    asset_name="sof_questionnaire_response_dfm",
    config_key="questionnaire_response_view",
    checkpoint_resource="QuestionnaireResponse",
    deps=[*_SFI_PRODUCERS, "sof_questionnaire_dfm"],
)

sof_questionnaire_response_item_dfm = create_sof_dfm_asset(
    asset_name="sof_questionnaire_response_item_dfm",
    config_key="questionnaire_response_item_view",
    checkpoint_resource="QuestionnaireResponse_item",
    deps=[
        "fhir_sof_infrastructure",
        "sof_questionnaire_response_dfm",
        # item_key resolution joins against fhir.Questionnaire_item.
        "sof_questionnaire_item_dfm",
    ],
)

__all__ = [
    "sof_questionnaire_dfm",
    "sof_questionnaire_item_dfm",
    "sof_questionnaire_response_dfm",
    "sof_questionnaire_response_item_dfm",
]

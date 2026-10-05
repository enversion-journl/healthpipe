# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dagster asset registration for IFDB AKTIVITETSRESULTAT (DFM)."""

import dagster as dg
from DSA.FHIR.Aktivitetsresultat.Aktivitetsresultat_IFDB_dfm import (
    _AKTIVITETSRESULTAT_OUT,
    fhir_server_aktivitetsresultat_sfi_dfm,
)

all_assets = [
    fhir_server_aktivitetsresultat_sfi_dfm,
]


def _yield_multi_asset_output(*, output_name: str, result: dg.MaterializeResult):
    yield dg.Output(
        value=None,
        output_name=output_name,
        metadata=result.metadata,
    )


__all__ = [
    "all_assets",
    "fhir_server_aktivitetsresultat_sfi_dfm",
    "_AKTIVITETSRESULTAT_OUT",
    "_yield_multi_asset_output",
]

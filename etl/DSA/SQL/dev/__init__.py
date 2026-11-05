# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dev-only assets — isolated development/debugging pipelines.

Registration is environment-gated: these assets truncate/delete warehouse
tables and must never be materialisable in a production deployment.
"""

from dagster import AssetsDefinition

from core.environment import is_prod_environment

from .reset_faktura import erp_faktura_reset_tables


def get_dev_assets() -> list[AssetsDefinition]:
    """Return dev-only assets; empty in prod deployments."""
    if is_prod_environment():
        return []
    return [
        erp_faktura_reset_tables,
    ]


all_assets = get_dev_assets()

__all__ = ["get_dev_assets", "all_assets"]

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contract tests for the ERP transaction-dimension shared-PK grain.

Transaction dims (Fakturalinje, Vareudlevering) share their dw_ek_* PK with
the sibling fact via map.* — the fct and dim sides must agree on the map
coordinate, and the DSO syncs must ship the shared key without
IDENTITY_INSERT (the key is resolved, not generated, on the DSO side).
"""

from __future__ import annotations

import pytest
from DSO.load.sync_configs import ERP_SYNC_CONFIGS
from scripts.validate_erp_grain import DIM_SPECS, FCT_SPECS

pytestmark = pytest.mark.unit


@pytest.mark.unit
def test_every_transaction_dim_fct_spec_has_matching_dim_spec():
    dim_by_entity = {spec.entity: spec for spec in DIM_SPECS}
    fct_with_shared_key = [spec for spec in FCT_SPECS if spec.map_table and spec.map_grain_column]
    assert fct_with_shared_key, "No transaction-dim fct specs found"
    for fct_spec in fct_with_shared_key:
        dim_spec = dim_by_entity.get(fct_spec.entity)
        assert dim_spec is not None, f"{fct_spec.entity}: fct spec has no dim spec"
        assert dim_spec.map_table == fct_spec.map_table, fct_spec.entity
        assert dim_spec.map_grain_column == fct_spec.map_grain_column, fct_spec.entity


@pytest.mark.unit
@pytest.mark.parametrize("entity", ["Fakturalinje", "Vareudlevering"])
def test_dso_sync_ships_shared_pk_without_identity_insert(entity):
    dim_cfg = ERP_SYNC_CONFIGS[f"dim_{entity}"]
    fct_cfg = ERP_SYNC_CONFIGS[f"fct_{entity}"]
    assert dim_cfg.primary_key == f"dw_ek_{entity}"
    assert fct_cfg.primary_key == f"dw_ek_{entity}"
    assert not getattr(fct_cfg, "identity_insert", False)

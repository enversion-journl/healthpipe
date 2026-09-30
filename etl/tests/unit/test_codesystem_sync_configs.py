# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""CodeSystem DSO sync must target the [codesystem] schema with PascalCase tables."""

import pytest
from DSO.load.sync_configs import ALL_SYNC_CONFIGS

pytestmark = pytest.mark.unit


def test_no_codesystem_data_targets_ref_schema():
    for key, cfg in ALL_SYNC_CONFIGS.items():
        if cfg.schema == "ref":
            assert not cfg.table.endswith(
                ("Concept", "Properties", "Hierarchy")
            ), f"{key} syncs codesystem data into [ref]: {cfg.table}"


def test_sfi_not_double_synced():
    sfi_keys = [k for k, c in ALL_SYNC_CONFIGS.items() if c.table.startswith("SFI")]
    assert sorted(sfi_keys) == [
        "codesystem_sfi_concept",
        "codesystem_sfi_hierarchy",
        "codesystem_sfi_properties",
    ], f"unexpected sfi sync configs: {sfi_keys}"

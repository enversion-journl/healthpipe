# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import dagster as dg
import pytest
from core.branch_postfix import postfix_asset_selection


@pytest.mark.unit
def test_postfix_asset_selection_handles_required_neighbors(monkeypatch):
    monkeypatch.setenv("DAGSTER_BRANCH_POSTFIX", "feature-fact-udleveringer")

    original_key = dg.AssetKey(["ext_lgrvare_ils_daily"])
    postfixed_key = dg.AssetKey(["ext_lgrvare_ils_daily_feature_fact_udleveringer"])
    key_replacements = {original_key: postfixed_key}

    selection = dg.AssetSelection.assets("ext_lgrvare_ils_daily").required_multi_asset_neighbors()
    transformed = postfix_asset_selection(selection, key_replacements)

    assert type(transformed).__name__ == "RequiredNeighborsAssetSelection"
    assert hasattr(transformed, "child")
    assert set(transformed.child.selected_keys) == {postfixed_key}

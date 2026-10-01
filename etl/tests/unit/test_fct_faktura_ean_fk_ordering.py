# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Guardrail: fct.Faktura and fct.Fakturalinje must depend on map.ean / dim.ean.

Both fact tables resolve dw_ek_EAN_regningsmodtager by LEFT JOINing map.ean
(unresolved -> -1). If the fact asset runs before map_EAN/dim_EAN are
materialized, the ean map can be empty/stale and every fact row silently gets
dw_ek_EAN_regningsmodtager = -1.

This invariant is independent of where the ean dimension sources its data, but
it became worth locking down when stg_dim_EAN was decoupled from the faktura
pre-staging table: the ean chain (stg_dim_EAN -> map_EAN -> dim_EAN) now sits on
its own upstream branch, so the only thing guaranteeing it runs before the facts
is these explicit asset dependencies.
"""

import sys

import DSA.SQL.fct.fct_Faktura  # noqa: F401
import DSA.SQL.fct.fct_Fakturalinje  # noqa: F401
import pytest

_faktura = sys.modules["DSA.SQL.fct.fct_Faktura"].fct_Faktura
_fakturalinje = sys.modules["DSA.SQL.fct.fct_Fakturalinje"].fct_Fakturalinje

_REQUIRED_EAN_DEPS = {"map_EAN", "dim_EAN"}


def _dep_key_strings(asset_def):
    return {"/".join(k.path) for k in asset_def.dependency_keys}


@pytest.mark.unit
class TestFctEanFkOrdering:
    """Ensure both invoice facts run after the ean map/dim are built."""

    def test_fct_faktura_depends_on_map_and_dim_ean(self):
        deps = _dep_key_strings(_faktura)
        missing = _REQUIRED_EAN_DEPS - deps
        assert not missing, (
            f"fct_Faktura is missing ean FK dependencies {sorted(missing)}. "
            "It resolves dw_ek_EAN_regningsmodtager against map.ean and must run "
            f"after them. Current deps: {sorted(deps)}"
        )

    def test_fct_fakturalinje_depends_on_map_and_dim_ean(self):
        deps = _dep_key_strings(_fakturalinje)
        missing = _REQUIRED_EAN_DEPS - deps
        assert not missing, (
            f"fct_Fakturalinje is missing ean FK dependencies {sorted(missing)}. "
            "It resolves dw_ek_EAN_regningsmodtager against map.ean and must run "
            f"after them. Current deps: {sorted(deps)}"
        )

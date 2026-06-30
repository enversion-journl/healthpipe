# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Guardrail: dim.Vare must NEVER use KMD data.

KMD ItemDescription/ItemName may contain patient-identifiable free text.
dim.Vare is a shared dimension visible to many users, so it must only
contain MySupply/ILS product-master data. UNSPSC classification is provided
via DW_EK_UNSPSC directly on fact tables instead.
"""

import sys

import DSA.SQL.dim.DimUpd_vare  # noqa: F401
import DSA.SQL.staging.stg_dim_vare  # noqa: F401
import pytest

_dim_module = sys.modules["DSA.SQL.dim.DimUpd_vare"]
_stg_module = sys.modules["DSA.SQL.staging.stg_dim_vare"]

_FORBIDDEN_REFERENCES = [
    "arc.spend_analysis_lines_kmd_uddata",
    "arc.invoice_kmd_uddata",
    "kmd_uddata",
    "ItemDescription",
    "ItemName",
    "SpendAnalysisLines",
]


@pytest.mark.unit
class TestDimVareNoKmdSources:
    """Ensure dim.Vare pipeline never references KMD tables or sensitive fields."""

    def test_dim_update_sql_no_kmd_references(self):
        """DimUpd_vare executed SQL must not reference any KMD table or field."""
        resolve_sql = getattr(_dim_module, "RESOLVE_SQL", "")
        merge_sql = getattr(_dim_module, "MERGE_SQL", "")
        combined = (resolve_sql + merge_sql).lower()

        for forbidden in _FORBIDDEN_REFERENCES:
            assert forbidden.lower() not in combined, (
                f"DimUpd_vare.py SQL contains forbidden reference '{forbidden}'. "
                "dim.Vare must NOT use KMD data."
            )

    def test_stg_dim_vare_sql_no_kmd_references(self):
        """stg_dim_vare executed SQL must not reference any KMD table or field."""
        source = _stg_module.__file__
        with open(source) as f:
            lines = f.readlines()

        sql_lines = [line for line in lines if not line.strip().startswith(("#", '"""', "'''"))]
        content = "".join(sql_lines)

        for forbidden in _FORBIDDEN_REFERENCES:
            assert forbidden.lower() not in content.lower(), (
                f"stg_dim_vare.py contains forbidden reference '{forbidden}'. "
                "dim.Vare must NOT use KMD data."
            )

    def test_dim_vare_asset_deps_no_kmd(self):
        """dim_vare asset dependencies must not include KMD-related assets."""
        asset_def = _dim_module.dim_vare
        dep_keys = asset_def.dependency_keys

        kmd_indicators = ["kmd", "spend_analysis", "invoice_kmd"]
        for dep_key in dep_keys:
            key_str = "/".join(dep_key.path).lower()
            for indicator in kmd_indicators:
                assert indicator not in key_str, (
                    f"dim_vare depends on '{dep_key}' which looks KMD-related. "
                    "dim.Vare must NOT depend on KMD assets."
                )

    def test_stg_dim_vare_asset_deps_no_kmd(self):
        """stg_dim_vare asset dependencies must not include KMD-related assets."""
        asset_def = _stg_module.stg_dim_vare
        dep_keys = asset_def.dependency_keys

        kmd_indicators = ["kmd", "spend_analysis", "invoice_kmd"]
        for dep_key in dep_keys:
            key_str = "/".join(dep_key.path).lower()
            for indicator in kmd_indicators:
                assert indicator not in key_str, (
                    f"stg_dim_vare depends on '{dep_key}' which looks KMD-related. "
                    "stg.dim_vare must NOT depend on KMD assets."
                )

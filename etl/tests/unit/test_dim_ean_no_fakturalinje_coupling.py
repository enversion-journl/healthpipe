# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Guardrail: dim.Ean must source ean numbers directly from the archive header,
not from the faktura+fakturalinje pre-staging table.

clean_code_EAN_regningsmodtager is a plain cast of
arc.mysupply_statistic_invoice_header.regnings_modtager_id
(see pre_stg_Fakturalinje_mysupply.py) — it borrows nothing from the
metadata/line join. Coupling dim.Ean to stg.pre_Fakturalinje_mysupply forces
the ean dimension onto the heavy faktura critical path for a single header
column. dim.Ean must instead read distinct regnings_modtager_id straight from
arc.mysupply_statistic_invoice_header, keeping the fct.Faktura / fct.Fakturalinje
FK → dim.Ean relationship intact.
"""

import sys

import DSA.SQL.staging.stg_dim_EAN  # noqa: F401
import pytest

_stg_module = sys.modules["DSA.SQL.staging.stg_dim_EAN"]

_FORBIDDEN_REFERENCES = [
    "pre_Fakturalinje_mysupply",
    "pre_stg_Fakturalinje_mysupply",
]

_REQUIRED_ARCHIVE_ASSET = "arc_mysupply_statistic_invoice_header"


@pytest.mark.unit
class TestDimEanNoFakturalinjeCoupling:
    """Ensure the ean staging asset reads from the archive header, not pre_stg."""

    def test_stg_dim_ean_sql_no_pre_stg_references(self):
        """stg_dim_EAN source must not reference the faktura+fakturalinje pre_stg table."""
        source = _stg_module.__file__
        with open(source) as f:
            lines = f.readlines()

        sql_lines = [line for line in lines if not line.strip().startswith(("#", '"""', "'''"))]
        content = "".join(sql_lines).lower()

        for forbidden in _FORBIDDEN_REFERENCES:
            assert forbidden.lower() not in content, (
                f"stg_dim_EAN.py references '{forbidden}'. dim.Ean must read ean numbers "
                "directly from arc.mysupply_statistic_invoice_header, not from pre_stg."
            )

    def test_stg_dim_ean_sql_reads_archive_header(self):
        """stg_dim_EAN source must read from arc.mysupply_statistic_invoice_header."""
        source = _stg_module.__file__
        with open(source) as f:
            content = f.read().lower()

        assert "mysupply_statistic_invoice_header" in content, (
            "stg_dim_EAN.py must read distinct regnings_modtager_id from "
            "arc.mysupply_statistic_invoice_header."
        )

    def test_stg_dim_ean_asset_deps_no_pre_stg(self):
        """stg_dim_EAN asset dependencies must not include the pre_stg faktura table."""
        dep_keys = _stg_module.stg_dim_EAN.dependency_keys
        for dep_key in dep_keys:
            key_str = "/".join(dep_key.path).lower()
            assert "pre_stg_fakturalinje" not in key_str and "pre_Fakturalinje" not in key_str, (
                f"stg_dim_EAN depends on '{dep_key}', which couples it to the faktura "
                "pre-staging pipeline. dim.Ean must depend on the archive header instead."
            )

    def test_stg_dim_ean_asset_deps_include_archive_header(self):
        """stg_dim_EAN must depend on arc_mysupply_statistic_invoice_header."""
        dep_keys = {"/".join(k.path) for k in _stg_module.stg_dim_EAN.dependency_keys}
        assert _REQUIRED_ARCHIVE_ASSET in dep_keys, (
            f"stg_dim_EAN must depend on '{_REQUIRED_ARCHIVE_ASSET}'. "
            f"Current deps: {sorted(dep_keys)}"
        )

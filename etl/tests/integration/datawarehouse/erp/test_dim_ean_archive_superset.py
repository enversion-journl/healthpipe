# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Integration test: dim.Ean (archive-direct) is a superset of the pre_stg ean set.

dim.Ean now sources ean numbers straight from arc.mysupply_statistic_invoice_header
instead of the faktura+fakturalinje pre-staging table. Because pre_stg INNER JOINs
metadata (headers without resolvable metadata are dropped), the archive-direct ean
set must be a SUPERSET of the ean set the faktura pipeline produces. A superset is
the safe direction for a conformed dimension: every ean referenced by
fct.Faktura / fct.Fakturalinje (whose clean_code_EAN_regningsmodtager comes from
pre_stg) is guaranteed to exist in dim.Ean, so no fact FK orphans to -1 for a
reason introduced by this decoupling.
"""

import pytest


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_stg_dim_ean_is_superset_of_pre_stg_ean(
    real_mssql_target_resource,
    erp_materialized_session,
):
    """Every clean_code_EAN in pre_stg must also appear in stg.dim_EAN."""
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT DISTINCT clean_code_EAN_regningsmodtager
                FROM [stg].[pre_Fakturalinje_mysupply]
                WHERE clean_code_EAN_regningsmodtager IS NOT NULL
                  AND clean_code_EAN_regningsmodtager <> ''
                """
            )
            pre_stg_eans = {row[0] for row in cursor.fetchall()}

            cursor.execute(
                """
                SELECT DISTINCT clean_code_EAN
                FROM [stg].[dim_EAN]
                WHERE clean_code_EAN IS NOT NULL AND clean_code_EAN <> ''
                """
            )
            dim_eans = {row[0] for row in cursor.fetchall()}
        finally:
            cursor.close()

    if not pre_stg_eans:
        pytest.skip("pre_Fakturalinje_mysupply has no ean rows in current test environment")

    missing = pre_stg_eans - dim_eans
    assert not missing, (
        f"{len(missing)} ean(s) present in pre_stg are missing from stg.dim_EAN "
        f"(archive-direct set must be a superset). Sample: {sorted(missing)[:10]}"
    )

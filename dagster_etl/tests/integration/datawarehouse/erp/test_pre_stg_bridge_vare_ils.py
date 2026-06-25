# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for ILS bridge pre-staging asset.
"""

import pytest
from tests.utils.materialize_helpers import get_materialization_result


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_pre_stg_bridge_vare_ils_materializes_successfully(
    dagster_instance,
    erp_materialized_session,
):
    """Bridge pre-staging asset should materialize with row_count metadata."""
    metadata = get_materialization_result(dagster_instance, "erp/pre_stg_bridge_vare_ils")
    assert metadata is not None, "Should have materialization metadata"
    assert metadata["row_count"].value >= 0, "Should report row count"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_pre_stg_bridge_vare_ils_has_unique_composite_key(
    real_mssql_target_resource,
    erp_materialized_session,
):
    """Bridge output should be unique at (CleanCode_Kreditor, CleanCode_LeverandoerVarenummer)."""
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total_count,
                    COUNT(DISTINCT CONCAT(CleanCode_Kreditor, '|', CleanCode_LeverandoerVarenummer)) AS distinct_count
                FROM [stg].[pre_bridge_vare_ils]
            """
            )
            total_count, distinct_count = cursor.fetchone()
        finally:
            cursor.close()
    assert total_count == distinct_count, (
        "Composite key should be unique in stg.pre_bridge_vare_ils: "
        f"{total_count} total, {distinct_count} distinct"
    )


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_pre_stg_bridge_vare_ils_prefers_complete_relationship_rows(
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    If any candidate row has lnr for a key, selected bridge row should not keep lnr NULL.

    This validates the completeness-first fallback policy before bridge priority.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                ;WITH lev_current AS (
                    SELECT
                        lnr,
                        CVR_CleanCode
                    FROM (
                        SELECT
                            lev.lnr,
                            lev.CVR_CleanCode,
                            ROW_NUMBER() OVER (
                                PARTITION BY lev.lnr
                                ORDER BY
                                    CASE WHEN lev.CVR_CleanCode IS NOT NULL AND lev.CVR_CleanCode <> '' THEN 0 ELSE 1 END,
                                    COALESCE(lev.pro2modified, lev.pro2created, lev.DW_LoadTimestamp) DESC,
                                    lev.DW_LoadTimestamp DESC
                            ) AS rn
                        FROM [stg].[lev_ils] lev
                        WHERE lev.lnr IS NOT NULL
                    ) ranked
                    WHERE rn = 1
                      AND CVR_CleanCode IS NOT NULL
                      AND CVR_CleanCode <> ''
                ),
                all_candidates AS (
                    SELECT
                        lev.CVR_CleanCode AS CleanCode_Kreditor,
                        s.Leverandoer_Varenummer_CleanCode AS CleanCode_LeverandoerVarenummer,
                        s.lnr
                    FROM [stg].[laftdstam_ils] s
                    INNER JOIN lev_current lev ON lev.lnr = s.lnr
                    WHERE s.[lvnr##1] IS NOT NULL AND LTRIM(RTRIM(s.[lvnr##1])) <> ''

                    UNION ALL

                    SELECT
                        lev.CVR_CleanCode,
                        lg.Leverandoer_Varenummer_CleanCode,
                        lg.lnr
                    FROM [stg].[lgrvare_ils] lg
                    INNER JOIN lev_current lev ON lev.lnr = lg.lnr
                    WHERE lg.lvnr IS NOT NULL AND LTRIM(RTRIM(lg.lvnr)) <> ''

                    UNION ALL

                    SELECT
                        lev.CVR_CleanCode,
                        ik.Leverandoer_Varenummer_CleanCode,
                        ik.lnr
                    FROM [stg].[indkfak_ils] ik
                    INNER JOIN lev_current lev ON lev.lnr = ik.lnr
                    WHERE ik.levvarenr IS NOT NULL AND LTRIM(RTRIM(ik.levvarenr)) <> ''
                      AND ik.levvarenr <> '-'
                      AND LEN(LTRIM(RTRIM(ik.levvarenr))) >= 3
                      AND ik.levvarenr LIKE '%[a-zA-Z0-9]%'
                      AND ik.Vare_CleanCode IS NOT NULL

                    UNION ALL

                    SELECT
                        lev.CVR_CleanCode,
                        CONCAT('LEVVARE_', UPPER(LTRIM(RTRIM(ld.[lvnr##1])))) AS Leverandoer_Varenummer_CleanCode,
                        ld.lnr
                    FROM [stg].[laftd_ils] ld
                    INNER JOIN lev_current lev ON lev.lnr = ld.lnr
                    WHERE ld.[lvnr##1] IS NOT NULL AND LTRIM(RTRIM(ld.[lvnr##1])) <> ''

                    UNION ALL

                    SELECT
                        lev.CVR_CleanCode,
                        p.Leverandoer_Varenummer_CleanCode,
                        p.lnr
                    FROM [stg].[pmdm_ils] p
                    INNER JOIN lev_current lev ON lev.lnr = p.lnr
                    WHERE p.lvnr IS NOT NULL AND LTRIM(RTRIM(p.lvnr)) <> ''
                ),
                candidate_with_lnr AS (
                    SELECT DISTINCT
                        CleanCode_Kreditor,
                        CleanCode_LeverandoerVarenummer
                    FROM all_candidates
                    WHERE CleanCode_Kreditor IS NOT NULL
                      AND CleanCode_Kreditor <> ''
                      AND CleanCode_LeverandoerVarenummer IS NOT NULL
                      AND CleanCode_LeverandoerVarenummer <> ''
                      AND lnr IS NOT NULL
                )
                SELECT COUNT(*)
                FROM [stg].[pre_bridge_vare_ils] bridge
                INNER JOIN candidate_with_lnr c
                    ON c.CleanCode_Kreditor = bridge.CleanCode_Kreditor
                   AND c.CleanCode_LeverandoerVarenummer = bridge.CleanCode_LeverandoerVarenummer
                WHERE bridge.lnr IS NULL
            """
            )
            selected_null_lnr_while_candidate_has_lnr = cursor.fetchone()[0]
        finally:
            cursor.close()
    assert selected_null_lnr_while_candidate_has_lnr == 0, (
        "Completeness fallback regression: selected bridge row has NULL lnr "
        "even though candidate rows for the same key contain lnr."
    )

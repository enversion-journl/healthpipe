# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration test: KMD IndkoebsanalyseLinje pipeline (arc → fct).

Gated by USE_REAL_DB_FOR_TESTS=true and TEST_ENV containing 'dev'.
No module-level skip — tests skip only when prerequisites are missing.
"""

import os
from datetime import datetime

import pytest
from tests.utils.materialize_helpers import materialize_assets_with_dependencies

pytestmark = [
    pytest.mark.integration,
    pytest.mark.erp,
    pytest.mark.real_resources,
    pytest.mark.skipif(
        os.environ.get("USE_REAL_DB_FOR_TESTS") != "true"
        or "dev" not in (os.environ.get("TEST_ENV") or ""),
        reason="Requires USE_REAL_DB_FOR_TESTS=true and TEST_ENV containing 'dev'",
    ),
]

_TEST_SAL_EK = 9_999_001
_TEST_UNSPSC_SK = 9_999_101
_TEST_UNSPSC_EK = 9_999_102
_TEST_UNSPSC_CODE = "99999001"
_TEST_CLEANCODE = f"KMD_SAL_{_TEST_SAL_EK}"


def _tables_ready(cursor) -> bool:
    cursor.execute(
        """
        SELECT
            OBJECT_ID('arc.spend_analysis_lines_kmd_uddata') AS sal,
            OBJECT_ID('arc.unspsc_kmd_uddata') AS ku,
            OBJECT_ID('map.IndkoebsanalyseLinje') AS map_i,
            OBJECT_ID('fct.IndkoebsanalyseLinje') AS fct_i
    """
    )
    sal, ku, map_i, fct_i = cursor.fetchone()
    return all(x is not None for x in (sal, ku, map_i, fct_i))


def _seed_fixture(cursor) -> None:
    now = datetime.now()
    cursor.execute(
        """
        IF NOT EXISTS (
            SELECT 1 FROM [map].[UNSPSC] WHERE CleanCode_UNSPSC = ?
        )
        INSERT INTO [map].[UNSPSC] (
            CleanCode_UNSPSC, DW_Source_ILS, DW_Source_KMD,
            DW_Timestamp_Insert, DW_Timestamp_Update
        )
        VALUES (?, 0, 1, SYSDATETIME(), SYSDATETIME());
        """,
        (_TEST_UNSPSC_CODE, _TEST_UNSPSC_CODE),
    )
    cursor.execute(
        """
        DECLARE @map_ek INT = (
            SELECT DW_EK_UNSPSC FROM [map].[UNSPSC] WHERE CleanCode_UNSPSC = ?
        );
        IF NOT EXISTS (
            SELECT 1 FROM [dim].[UNSPSC] WHERE DW_EK_UNSPSC = @map_ek
        )
        INSERT INTO [dim].[UNSPSC] (
            DW_EK_UNSPSC, UNSPSC_Kode, Source_ILS, Source_KMD,
            Beskrivelse_KMD, DW_Timestamp_Insert, DW_Timestamp_Update
        )
        VALUES (@map_ek, ?, 0, 1, N'Test UNSPSC', SYSDATETIME(), SYSDATETIME());
        """,
        (_TEST_UNSPSC_CODE, _TEST_UNSPSC_CODE),
    )
    cursor.execute(
        """
        MERGE [arc].[unspsc_kmd_uddata] AS tgt
        USING (
            SELECT
                ? AS DW_SK_UNSPSC,
                ? AS DW_EK_UNSPSC,
                ? AS UNSPSCClassID,
                CAST(1 AS BIT) AS DW_IsCurrent,
                ? AS DW_Timestamp_Insert,
                ? AS DW_Timestamp_Update
        ) AS src
        ON tgt.DW_SK_UNSPSC = src.DW_SK_UNSPSC
        WHEN MATCHED THEN UPDATE SET
            tgt.UNSPSCClassID = src.UNSPSCClassID,
            tgt.DW_IsCurrent = src.DW_IsCurrent,
            tgt.DW_Timestamp_Update = src.DW_Timestamp_Update
        WHEN NOT MATCHED THEN INSERT (
            DW_SK_UNSPSC, DW_EK_UNSPSC, UNSPSCClassID, DW_IsCurrent,
            DW_Timestamp_Insert, DW_Timestamp_Update
        ) VALUES (
            src.DW_SK_UNSPSC, src.DW_EK_UNSPSC, src.UNSPSCClassID, src.DW_IsCurrent,
            src.DW_Timestamp_Insert, src.DW_Timestamp_Update
        );
        """,
        (
            _TEST_UNSPSC_SK,
            _TEST_UNSPSC_EK,
            _TEST_UNSPSC_CODE,
            now,
            now,
        ),
    )
    # DW_EK_UNSPSC on arc row must match map.UNSPSC for stg join resolution.
    cursor.execute(
        "SELECT DW_EK_UNSPSC FROM [map].[UNSPSC] WHERE CleanCode_UNSPSC = ?",
        (_TEST_UNSPSC_CODE,),
    )
    map_unspsc_ek = cursor.fetchone()[0]
    cursor.execute(
        """
        UPDATE [arc].[unspsc_kmd_uddata]
        SET DW_EK_UNSPSC = ?
        WHERE DW_SK_UNSPSC = ?
        """,
        (map_unspsc_ek, _TEST_UNSPSC_SK),
    )
    cursor.execute(
        """
        MERGE [arc].[spend_analysis_lines_kmd_uddata] AS tgt
        USING (
            SELECT
                ? AS DW_EK_SpendAnalysisLine,
                ? AS DW_SK_UNSPSC,
                CAST(100.50 AS NUMERIC(38, 6)) AS LineAmount,
                CAST(2 AS NUMERIC(38, 6)) AS InvoicedQuantity,
                CAST(50.25 AS NUMERIC(38, 6)) AS UnitPrice,
                CAST(25 AS NUMERIC(38, 6)) AS LineTaxAmount,
                CAST(0.1 AS NUMERIC(38, 6)) AS CO2Emission,
                CAST(20260101 AS INT) AS DW_SK_Date_SpendAnalysis,
                CAST(20260102 AS INT) AS DW_SK_Date_Issue,
                CAST(20260103 AS INT) AS DW_SK_Date_OrderIssue,
                CAST(20260104 AS INT) AS DW_SK_Date_ActualDelivery,
                CAST(20260105 AS INT) AS DW_SK_Date_PaymentDue,
                CAST(20260106 AS INT) AS DW_SK_Date_Posting,
                CAST('2026-01-15' AS DATE) AS DW_ValidDate,
                ? AS DW_Timestamp_Insert,
                ? AS DW_Timestamp_Update
        ) AS src
        ON tgt.DW_EK_SpendAnalysisLine = src.DW_EK_SpendAnalysisLine
        WHEN MATCHED THEN UPDATE SET
            tgt.DW_SK_UNSPSC = src.DW_SK_UNSPSC,
            tgt.LineAmount = src.LineAmount,
            tgt.DW_Timestamp_Update = src.DW_Timestamp_Update
        WHEN NOT MATCHED THEN INSERT (
            DW_EK_SpendAnalysisLine, DW_SK_UNSPSC,
            LineAmount, InvoicedQuantity, UnitPrice, LineTaxAmount, CO2Emission,
            DW_SK_Date_SpendAnalysis, DW_SK_Date_Issue, DW_SK_Date_OrderIssue,
            DW_SK_Date_ActualDelivery, DW_SK_Date_PaymentDue, DW_SK_Date_Posting,
            DW_ValidDate, DW_Timestamp_Insert, DW_Timestamp_Update
        ) VALUES (
            src.DW_EK_SpendAnalysisLine, src.DW_SK_UNSPSC,
            src.LineAmount, src.InvoicedQuantity, src.UnitPrice, src.LineTaxAmount,
            src.CO2Emission,
            src.DW_SK_Date_SpendAnalysis, src.DW_SK_Date_Issue, src.DW_SK_Date_OrderIssue,
            src.DW_SK_Date_ActualDelivery, src.DW_SK_Date_PaymentDue, src.DW_SK_Date_Posting,
            src.DW_ValidDate, src.DW_Timestamp_Insert, src.DW_Timestamp_Update
        );
        """,
        (_TEST_SAL_EK, _TEST_UNSPSC_SK, now, now),
    )


def _cleanup_fixture(cursor) -> None:
    cursor.execute(
        "DELETE FROM [fct].[IndkoebsanalyseLinje] WHERE bk_SpendAnalysisLine = ?",
        (str(_TEST_SAL_EK),),
    )
    cursor.execute(
        "DELETE FROM [map].[IndkoebsanalyseLinje] WHERE CleanCode_IndkoebsanalyseLinje = ?",
        (_TEST_CLEANCODE,),
    )
    cursor.execute(
        "DELETE FROM [arc].[spend_analysis_lines_kmd_uddata] " "WHERE DW_EK_SpendAnalysisLine = ?",
        (_TEST_SAL_EK,),
    )
    cursor.execute(
        "DELETE FROM [arc].[unspsc_kmd_uddata] WHERE DW_SK_UNSPSC = ?",
        (_TEST_UNSPSC_SK,),
    )

    cursor.execute(
        "DELETE FROM [dim].[UNSPSC] WHERE UNSPSC_Kode = ? AND DW_EK_UNSPSC <> -1",
        (_TEST_UNSPSC_CODE,),
    )
    cursor.execute(
        "DELETE FROM [map].[UNSPSC] WHERE CleanCode_UNSPSC = ?",
        (_TEST_UNSPSC_CODE,),
    )


@pytest.mark.order(5)
def test_indkoebsanalyse_linje_unspsc_end_to_end(
    dagster_instance,
    real_mssql_target_resource,
):
    """Materialize stg → map → fct and verify DW_EK_UNSPSC resolves via dim.UNSPSC."""
    writes = os.environ.get("ENABLE_TEST_WRITES", "false").lower() == "true"
    if not writes:
        pytest.skip("Requires ENABLE_TEST_WRITES=true to seed arc fixture rows")

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            if not _tables_ready(cursor):
                pytest.skip("IndkoebsanalyseLinje DDL not deployed in staging database")
            _seed_fixture(cursor)
            conn.commit()
        finally:
            cursor.close()

    try:
        materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=[
                "erp/stg_fct_indkoebsanalyse_linje_kmd",
                "map_IndkoebsanalyseLinje",
                "fct_indkoebsanalyse_linje",
            ],
            resources={
                "mssql_target_staging": real_mssql_target_resource,
            },
        )

        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT
                        f.DW_EK_IndkoebsanalyseLinje,
                        f.DW_EK_UNSPSC,
                        f.LineAmount,
                        d.UNSPSC_Kode,
                        d.Beskrivelse_KMD
                    FROM [fct].[IndkoebsanalyseLinje] f
                    INNER JOIN [dim].[UNSPSC] d
                        ON d.DW_EK_UNSPSC = f.DW_EK_UNSPSC
                    WHERE f.bk_SpendAnalysisLine = ?
                    """,
                    (str(_TEST_SAL_EK),),
                )
                row = cursor.fetchone()
            finally:
                cursor.close()

        assert row is not None, "Expected seeded SpendAnalysisLine in fct.IndkoebsanalyseLinje"
        assert row[1] != -1, "DW_EK_UNSPSC should resolve for seeded line"
        assert row[2] == 100.50
        assert row[3] == _TEST_UNSPSC_CODE
    finally:
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                _cleanup_fixture(cursor)
                conn.commit()
            finally:
                cursor.close()

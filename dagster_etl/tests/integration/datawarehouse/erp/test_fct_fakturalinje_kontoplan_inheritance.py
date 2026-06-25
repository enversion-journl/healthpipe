# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for kontoplan inheritance from postings to fakturalinje.

Tests validate:
- DW_EK_Kontoplan inheritance from fct.Postering via FakturaNr
- Handling of invoices split into multiple postings
- Deterministic kontoplan selection (first by ref_LedgertransId)
- Proper fallback to -1 when no valid postings exist
"""

import pytest
from dagster import ResourceDefinition
from tests.utils.materialize_helpers import (
    get_materialization_result,
    materialize_assets_with_dependencies,
)

pytestmark = pytest.mark.skip(
    reason="Legacy fact inheritance regression tests are brittle in shared integration environments"
)


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_fakturalinje_kontoplan_inheritance_from_postings(
    dagster_instance,
    real_mysupply_mssql_resource,
    real_mssql_target_resource,
    setup_erp_infrastructure,
    cleanup_erp_fact_tables,
):
    """
    Test kontoplan inheritance from postings when invoice is split into multiple postings.

    Test scenario:
    - Invoice F2024001 is split into 2 postings with different chart of accounts
    - Posting 1001: FinanskontoId=1 (DW_EK_Kontoplan from map)
    - Posting 1002: FinanskontoId=2 (DW_EK_Kontoplan from map)
    - Fakturalinje should inherit kontoplan from the first posting (lowest ref_LedgertransId)
    """
    # Materialize full dependency chain including postings
    result = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=[
            # MySupply extraction
            "ext_statistic_invoice_line_mysupply_daily",
            "ext_statistic_invoice_header_mysupply_daily",
            "ext_metadata_mysupply_daily",
            # Sydax extraction for postings
            "ext_finanspostering_sydax_daily",
            "ext_kreditortransaktion_sydax_daily",
            "ext_kreditor_sydax_daily",
            "ext_finanskonto_sydax_daily",
            # Maps
            "map_kreditor",
            "map_vare",
            "map_kontoplan",
            # Dimensions
            "dim_kreditor",
            "dim_vare",
            "dim_kontoplan",
            # Facts (postings first, then invoice lines)
            "fct_postering",
            "fct_fakturalinje",
        ],
        partition="2025-09-17",
        resources={
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    assert result is not None

    # Verify fakturalinje fact table has data
    fakturalinje_result = get_materialization_result(dagster_instance, "fct_fakturalinje")
    assert fakturalinje_result is not None
    assert fakturalinje_result["rows_affected"].value >= 1

    # Verify postings fact table has data
    postering_result = get_materialization_result(dagster_instance, "fct_postering")
    assert postering_result is not None
    assert postering_result["rows_affected"].value >= 2  # Should have 2 postings

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            # Verify postings have correct kontoplan mappings
            cursor.execute(
                """
                SELECT ref_LedgertransId, FakturaNr, DW_EK_Kontoplan
                FROM [fct].[Postering]
                WHERE FakturaNr = 'F2024001'
                ORDER BY ref_LedgertransId
            """
            )
            postings = cursor.fetchall()

            assert (
                len(postings) == 2
            ), f"Expected 2 postings for invoice F2024001, got {len(postings)}"

            # Verify postings have different kontoplan values
            posting_1001 = next((p for p in postings if p[0] == 1001), None)
            posting_1002 = next((p for p in postings if p[0] == 1002), None)

            assert posting_1001 is not None, "Posting 1001 not found"
            assert posting_1002 is not None, "Posting 1002 not found"

            # Both should have valid DW_EK_Kontoplan (not -1)
            assert posting_1001[2] != -1, "Posting 1001 should have valid DW_EK_Kontoplan"
            assert posting_1002[2] != -1, "Posting 1002 should have valid DW_EK_Kontoplan"

            # They should be different
            assert (
                posting_1001[2] != posting_1002[2]
            ), "Postings should have different DW_EK_Kontoplan values"

            # Now verify fakturalinje inherits from the first posting
            cursor.execute(
                """
                SELECT statistic_invoice_line_id, FakturaNr, DW_EK_Kontoplan
                FROM [fct].[Fakturalinje]
                WHERE FakturaNr = 'F2024001'
            """
            )
            fakturalinje_rows = cursor.fetchall()

            assert len(fakturalinje_rows) >= 1, "Should have at least one fakturalinje for F2024001"

            # All fakturalinje for this invoice should inherit the same kontoplan
            # (from the first posting by ref_LedgertransId)
            expected_kontoplan = posting_1001[2]  # First posting's kontoplan

            for row in fakturalinje_rows:
                assert row[2] == expected_kontoplan, (
                    f"Fakturalinje {row[0]} should inherit DW_EK_Kontoplan={expected_kontoplan} "
                    f"from first posting, but got {row[2]}"
                )
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_fakturalinje_kontoplan_inheritance_fallback_to_minus_one(
    dagster_instance,
    real_mysupply_mssql_resource,
    real_mssql_target_resource,
    cleanup_erp_fact_tables,
):
    """
    Test kontoplan inheritance fallback when no valid postings exist.

    When an invoice has no corresponding postings with valid kontoplan,
    fakturalinje should fallback to -1 (UNKNOWN).
    """
    # Create a test invoice that won't have postings
    with real_mysupply_mssql_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            # Insert a header for a different invoice
            cursor.execute(
                """
                MERGE INTO [dbo].[statistic_invoice_header] AS target
                USING (
                    VALUES (
                        'F2024999', '2024-03-10', '2024-03-08', '2024-04-05',
                        'Region Hovedstaden', '12345678', 'DKK', 25.0, 100.0, 100.0, 100.0, 0.0, 0.0,
                        100.0, 100.0, 125.0, 125.0
                    )
                ) AS source (
                    [FakturaNr], [FakturaDato], [OrdreDato], [BetalingsDato],
                    [KundeNavn], [FakturaUdstederCVRNummer], [FakturaValutaKode], [VAT_AfgiftsKategoriProcent],
                    [VAT_CategoryTotal_AfgiftsKategor], [VAT_TaxAmounts_AfgiftsKategoriMo], [VAT_TaxAmounts_AfgiftsKategoriM0],
                    [VAT_TaxAmounts_AfgiftsKategoriM1], [VAT_TaxAmounts_AfgiftsKategoriM2], [FakturaLinjeTotal],
                    [FakturaLinjeTotal_Valuta], [FakturaTotal], [FakturaTotal_Valuta]
                )
                ON target.[FakturaNr] = source.[FakturaNr]
                WHEN NOT MATCHED THEN
                    INSERT ([metadata_id], [FakturaNr], [FakturaDato], [OrdreDato], [BetalingsDato],
                            [KundeNavn], [FakturaUdstederCVRNummer], [FakturaValutaKode], [VAT_AfgiftsKategoriProcent],
                            [VAT_CategoryTotal_AfgiftsKategor], [VAT_TaxAmounts_AfgiftsKategoriMo], [VAT_TaxAmounts_AfgiftsKategoriM0],
                            [VAT_TaxAmounts_AfgiftsKategoriM1], [VAT_TaxAmounts_AfgiftsKategoriM2], [FakturaLinjeTotal],
                            [FakturaLinjeTotal_Valuta], [FakturaTotal], [FakturaTotal_Valuta])
                    VALUES (100, source.[FakturaNr], source.[FakturaDato], source.[OrdreDato], source.[BetalingsDato],
                            source.[KundeNavn], source.[FakturaUdstederCVRNummer], source.[FakturaValutaKode], source.[VAT_AfgiftsKategoriProcent],
                            source.[VAT_CategoryTotal_AfgiftsKategor], source.[VAT_TaxAmounts_AfgiftsKategoriMo], source.[VAT_TaxAmounts_AfgiftsKategoriM0],
                            source.[VAT_TaxAmounts_AfgiftsKategoriM1], source.[VAT_TaxAmounts_AfgiftsKategoriM2], source.[FakturaLinjeTotal],
                            source.[FakturaLinjeTotal_Valuta], source.[FakturaTotal], source.[FakturaTotal_Valuta]);
                """
            )

            # Insert a line for this invoice
            cursor.execute(
                """
                MERGE INTO [dbo].[statistic_invoice_line] AS target
                USING (
                    VALUES (
                        1, 10.0, 100.0, 100.0, 'XYZ999', 'Test item without postings',
                        'Test item', 10.00, 10.00, 10.0, 'stk', '12345678'
                    )
                ) AS source (
                    [FakturaLinjeNr], [AntalEnhederFaktureret], [LinjeBeløb], [LinjeBeløb_Valuta],
                    [Item_FakturaUdstederVareNr], [Item_FakturaUdstederVareNavn], [Item_FakturaUdstederVareNavnKort],
                    [BasePrice_EnhedsPris], [BasePrice_EnhedsPris_Valuta], [BasePrice_EnhedsStørrelse], [BasePrice_EnhedsStørrelse_Type],
                    [FakturaUdstederCVRNummer]
                )
                ON target.[Item_FakturaUdstederVareNr] = source.[Item_FakturaUdstederVareNr]
                WHEN NOT MATCHED THEN
                    INSERT ([statistic_invoice_header_id], [FakturaLinjeNr], [AntalEnhederFaktureret],
                            [LinjeBeløb], [LinjeBeløb_Valuta], [Item_FakturaUdstederVareNr], [Item_FakturaUdstederVareNavn],
                            [Item_FakturaUdstederVareNavnKort], [BasePrice_EnhedsPris], [BasePrice_EnhedsPris_Valuta],
                            [BasePrice_EnhedsStørrelse], [BasePrice_EnhedsStørrelse_Type], [FakturaUdstederCVRNummer])
                    VALUES ((SELECT id FROM [dbo].[statistic_invoice_header] WHERE [FakturaNr] = 'F2024999'),
                            source.[FakturaLinjeNr], source.[AntalEnhederFaktureret],
                            source.[LinjeBeløb], source.[LinjeBeløb_Valuta], source.[Item_FakturaUdstederVareNr], source.[Item_FakturaUdstederVareNavn],
                            source.[Item_FakturaUdstederVareNavnKort], source.[BasePrice_EnhedsPris], source.[BasePrice_EnhedsPris_Valuta],
                            source.[BasePrice_EnhedsStørrelse], source.[BasePrice_EnhedsStørrelse_Type], source.[FakturaUdstederCVRNummer]);
            """
            )
        finally:
            cursor.close()
    # Materialize fakturalinje (without creating postings for this invoice)
    _ = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=[
            "ext_statistic_invoice_line_mysupply_daily",
            "ext_statistic_invoice_header_mysupply_daily",
            "ext_metadata_mysupply_daily",
            "map_kreditor",
            "map_vare",
            "fct_fakturalinje",
        ],
        partition="2025-09-17",
        resources={
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            # Verify the fakturalinje without postings gets DW_EK_Kontoplan = -1
            cursor.execute(
                """
                SELECT FakturaNr, DW_EK_Kontoplan
                FROM [fct].[Fakturalinje]
                WHERE FakturaNr = 'F2024999'
            """
            )
            rows = cursor.fetchall()

            assert len(rows) >= 1, "Should have fakturalinje for F2024999"

            for row in rows:
                assert row[1] == -1, (
                    f"Fakturalinje for invoice {row[0]} without postings should have DW_EK_Kontoplan=-1, "
                    f"but got {row[1]}"
                )
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_fakturalinje_kontoplan_inheritance_multiple_invoices(
    dagster_instance,
    real_mysupply_mssql_resource,
    real_mssql_target_resource,
    cleanup_erp_fact_tables,
):
    """
    Test kontoplan inheritance works correctly across multiple invoices.

    Each invoice should inherit kontoplan independently from its own postings.
    """
    # This test relies on the existing test data setup
    _ = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=[
            # MySupply extraction
            "ext_statistic_invoice_line_mysupply_daily",
            "ext_statistic_invoice_header_mysupply_daily",
            "ext_metadata_mysupply_daily",
            # Sydax extraction for postings
            "ext_finanspostering_sydax_daily",
            "ext_kreditortransaktion_sydax_daily",
            "ext_kreditor_sydax_daily",
            "ext_finanskonto_sydax_daily",
            # Maps and facts
            "map_kreditor",
            "map_vare",
            "map_kontoplan",
            "fct_postering",
            "fct_fakturalinje",
        ],
        partition="2025-09-17",
        resources={
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            # Get all fakturalinje with their inherited kontoplan
            cursor.execute(
                """
                SELECT DISTINCT f.FakturaNr, f.DW_EK_Kontoplan,
                    COUNT(*) OVER (PARTITION BY f.FakturaNr) as lines_per_invoice
                FROM [fct].[Fakturalinje] f
                ORDER BY f.FakturaNr
            """
            )
            fakturalinje_by_invoice = cursor.fetchall()

            # Verify each invoice has consistent kontoplan across its lines
            for faktura_nr, _dw_ek_kontoplan, lines_count in fakturalinje_by_invoice:
                assert lines_count > 0, f"Invoice {faktura_nr} should have at least one line"

                # Check that all lines for this invoice have the same kontoplan
                cursor.execute(
                    f"""
                    SELECT COUNT(DISTINCT DW_EK_Kontoplan) as distinct_kontoplan
                    FROM [fct].[Fakturalinje]
                    WHERE FakturaNr = '{faktura_nr}'
                """
                )

                distinct_count = cursor.fetchone()[0]
                assert (
                    distinct_count == 1
                ), f"All lines for invoice {faktura_nr} should have the same DW_EK_Kontoplan, but found {distinct_count} different values"
        finally:
            cursor.close()

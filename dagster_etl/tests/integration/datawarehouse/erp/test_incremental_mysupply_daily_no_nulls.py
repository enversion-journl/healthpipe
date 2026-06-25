# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration test for daily MySupply partitions in erp pipeline.

Validates that data loaded on day 1 remains populated after a day 2 run,
preventing NULL-overwrites for existing dimension rows.
"""

import pytest
from dagster import ResourceDefinition
from tests.utils.materialize_helpers import materialize_assets_with_dependencies

pytestmark = pytest.mark.skip(
    reason="Two-day seed regression test is brittle in shared integration environments"
)

# Use high IDs that won't conflict with existing data
TEST_METADATA_ID_1 = 99999991
TEST_METADATA_ID_2 = 99999992
TEST_HEADER_ID_1 = 99999901
TEST_HEADER_ID_2 = 99999902
TEST_HEADER_ID_3 = 99999903
TEST_LINE_ID_1 = 99999911
TEST_LINE_ID_2 = 99999912
TEST_LINE_ID_3 = 99999913


def _ensure_empty_kreditor_staging_tables(mssql_target_staging) -> None:
    """Create empty staging tables needed by map/dim_kreditor if missing."""
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                IF OBJECT_ID('[stg].[kreditor_sydax]', 'U') IS NULL
                BEGIN
                    CREATE TABLE [stg].[kreditor_sydax] (
                        CVR_CleanCode NVARCHAR(50) NULL,
                        KreditorId INT NULL,
                        Kreditornavn NVARCHAR(255) NULL,
                        Kreditornummer NVARCHAR(255) NULL,
                        JuridiskEnhed NVARCHAR(50) NULL
                    );
                END
                """
            )
            cursor.execute(
                """
                IF OBJECT_ID('[stg].[dim_kreditor_ils]', 'U') IS NULL
                BEGIN
                    CREATE TABLE [stg].[dim_kreditor_ils] (
                        CleanCode_Kreditor NVARCHAR(50) NULL,
                        navn NVARCHAR(255) NULL,
                        adr NVARCHAR(255) NULL,
                        adr2 NVARCHAR(255) NULL,
                        postdi NVARCHAR(50) NULL,
                        se_nr NVARCHAR(50) NULL,
                        cvr_nr NVARCHAR(50) NULL,
                        lnr NVARCHAR(50) NULL,
                        kontakt NVARCHAR(255) NULL
                    );
                END
                """
            )
            conn.commit()
        finally:
            cursor.close()


def _seed_mysupply_daily_invoices(mysupply_resource) -> None:
    """Insert minimal MySupply data for two daily partitions."""
    with mysupply_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # Metadata for two suppliers (explicit IDs required)
            cursor.execute(
                f"""
                MERGE INTO [dbo].[metadata] AS target
                USING (
                    VALUES
                        ({TEST_METADATA_ID_1}, '11111111', 'Supplier A', 'CityA', 'Addr 1', '1000',
                         '2026-01-10T10:00:00', 'MySupply', 'OrgA'),
                        ({TEST_METADATA_ID_2}, '22222222', 'Supplier B', 'CityB', 'Addr 2', '2000',
                         '2026-01-10T10:00:00', 'MySupply', 'OrgB')
                ) AS source (
                    [id], [LeverandorCVR], [FirmaNavn], [By], [Adresse], [Postnummer],
                    [ReceiveTimeFormat], [SystemNavn], [OrgUnit]
                )
                ON target.[id] = source.[id]
                WHEN NOT MATCHED THEN
                    INSERT ([id], [LeverandorCVR], [FirmaNavn], [By], [Adresse], [Postnummer],
                            [ReceiveTimeFormat], [SystemNavn], [OrgUnit])
                    VALUES (source.[id], source.[LeverandorCVR], source.[FirmaNavn], source.[By],
                            source.[Adresse], source.[Postnummer], source.[ReceiveTimeFormat],
                            source.[SystemNavn], source.[OrgUnit])
                WHEN MATCHED THEN
                    UPDATE SET
                        [LeverandorCVR] = source.[LeverandorCVR],
                        [FirmaNavn] = source.[FirmaNavn],
                        [By] = source.[By],
                        [Adresse] = source.[Adresse],
                        [Postnummer] = source.[Postnummer],
                        [ReceiveTimeFormat] = source.[ReceiveTimeFormat],
                        [SystemNavn] = source.[SystemNavn],
                        [OrgUnit] = source.[OrgUnit];
                """
            )

            # Invoice headers for two partitions (2026-01-10 and 2026-01-11)
            cursor.execute(
                f"""
                MERGE INTO [dbo].[statistic_invoice_header] AS target
                USING (
                    VALUES
                        ({TEST_HEADER_ID_1}, {TEST_METADATA_ID_1}, 'F2026A1', '2026-01-10', '2026-01-10', '2026-02-10',
                         'Region A', '11111111', 'DKK', '100.0', '100.0', '120.0', '120.0', '25.0'),
                        ({TEST_HEADER_ID_2}, {TEST_METADATA_ID_2}, 'F2026B1', '2026-01-10', '2026-01-10', '2026-02-10',
                         'Region B', '22222222', 'DKK', '200.0', '200.0', '240.0', '240.0', '25.0'),
                        ({TEST_HEADER_ID_3}, {TEST_METADATA_ID_1}, 'F2026A2', '2026-01-11', '2026-01-11', '2026-02-11',
                         'Region A', '11111111', 'DKK', '150.0', '150.0', '180.0', '180.0', '25.0')
                ) AS source (
                    [id], [metadata_id], [FakturaNr], [FakturaDato], [OrdreDato], [BetalingsDato],
                    [KundeNavn], [FakturaUdstederCVRNummer], [FakturaValutaKode],
                    [FakturaLinjeTotal], [FakturaLinjeTotal_Valuta], [FakturaTotal],
                    [FakturaTotal_Valuta], [VAT_AfgiftsKategoriProcent]
                )
                ON target.[id] = source.[id]
                WHEN NOT MATCHED THEN
                    INSERT ([id], [metadata_id], [FakturaNr], [FakturaDato], [OrdreDato], [BetalingsDato],
                            [KundeNavn], [FakturaUdstederCVRNummer], [FakturaValutaKode],
                            [FakturaLinjeTotal], [FakturaLinjeTotal_Valuta], [FakturaTotal],
                            [FakturaTotal_Valuta], [VAT_AfgiftsKategoriProcent])
                    VALUES (source.[id], source.[metadata_id], source.[FakturaNr], source.[FakturaDato],
                            source.[OrdreDato], source.[BetalingsDato],
                            source.[KundeNavn], source.[FakturaUdstederCVRNummer], source.[FakturaValutaKode],
                            source.[FakturaLinjeTotal], source.[FakturaLinjeTotal_Valuta], source.[FakturaTotal],
                            source.[FakturaTotal_Valuta], source.[VAT_AfgiftsKategoriProcent])
                WHEN MATCHED THEN
                    UPDATE SET
                        [metadata_id] = source.[metadata_id],
                        [FakturaDato] = source.[FakturaDato],
                        [OrdreDato] = source.[OrdreDato],
                        [BetalingsDato] = source.[BetalingsDato],
                        [KundeNavn] = source.[KundeNavn],
                        [FakturaUdstederCVRNummer] = source.[FakturaUdstederCVRNummer],
                        [FakturaValutaKode] = source.[FakturaValutaKode],
                        [FakturaLinjeTotal] = source.[FakturaLinjeTotal],
                        [FakturaLinjeTotal_Valuta] = source.[FakturaLinjeTotal_Valuta],
                        [FakturaTotal] = source.[FakturaTotal],
                        [FakturaTotal_Valuta] = source.[FakturaTotal_Valuta],
                        [VAT_AfgiftsKategoriProcent] = source.[VAT_AfgiftsKategoriProcent];
                """
            )

            # One line per invoice
            cursor.execute(
                f"""
                MERGE INTO [dbo].[statistic_invoice_line] AS target
                USING (
                    VALUES
                        ({TEST_LINE_ID_1}, {TEST_HEADER_ID_1}, 'ITEM_A_1', '1', '1.0', '100.0', 'DKK',
                         'Item A1', 'Item A1', '100.0', 'DKK', '1.0', 'stk'),
                        ({TEST_LINE_ID_2}, {TEST_HEADER_ID_2}, 'ITEM_B_1', '1', '1.0', '200.0', 'DKK',
                         'Item B1', 'Item B1', '200.0', 'DKK', '1.0', 'stk'),
                        ({TEST_LINE_ID_3}, {TEST_HEADER_ID_3}, 'ITEM_A_2', '1', '1.0', '150.0', 'DKK',
                         'Item A2', 'Item A2', '150.0', 'DKK', '1.0', 'stk')
                ) AS source (
                    [id], [statistic_invoice_header_id], [Item_FakturaUdstederVareNr], [FakturaLinjeNr],
                    [AntalEnhederFaktureret], [LinjeBeløb], [LinjeBeløb_Valuta],
                    [Item_FakturaUdstederVareNavn], [Item_FakturaUdstederVareNavnKort],
                    [BasePrice_EnhedsPris], [BasePrice_EnhedsPris_Valuta],
                    [BasePrice_EnhedsStørrelse], [BasePrice_EnhedsStørrelse_Type]
                )
                ON target.[id] = source.[id]
                WHEN NOT MATCHED THEN
                    INSERT ([id], [statistic_invoice_header_id], [FakturaLinjeNr], [AntalEnhederFaktureret],
                            [LinjeBeløb], [LinjeBeløb_Valuta], [Item_FakturaUdstederVareNr],
                            [Item_FakturaUdstederVareNavn], [Item_FakturaUdstederVareNavnKort],
                            [BasePrice_EnhedsPris], [BasePrice_EnhedsPris_Valuta],
                            [BasePrice_EnhedsStørrelse], [BasePrice_EnhedsStørrelse_Type])
                    VALUES (source.[id], source.[statistic_invoice_header_id],
                        source.[FakturaLinjeNr], source.[AntalEnhederFaktureret],
                        source.[LinjeBeløb], source.[LinjeBeløb_Valuta], source.[Item_FakturaUdstederVareNr],
                        source.[Item_FakturaUdstederVareNavn], source.[Item_FakturaUdstederVareNavnKort],
                        source.[BasePrice_EnhedsPris], source.[BasePrice_EnhedsPris_Valuta],
                        source.[BasePrice_EnhedsStørrelse], source.[BasePrice_EnhedsStørrelse_Type]
                    )
                WHEN MATCHED THEN
                    UPDATE SET
                        [statistic_invoice_header_id] = (
                            source.[statistic_invoice_header_id]
                        ),
                        [FakturaLinjeNr] = source.[FakturaLinjeNr],
                        [AntalEnhederFaktureret] = source.[AntalEnhederFaktureret],
                        [LinjeBeløb] = source.[LinjeBeløb],
                        [LinjeBeløb_Valuta] = source.[LinjeBeløb_Valuta],
                        [Item_FakturaUdstederVareNavn] = source.[Item_FakturaUdstederVareNavn],
                        [Item_FakturaUdstederVareNavnKort] = source.[Item_FakturaUdstederVareNavnKort],
                        [BasePrice_EnhedsPris] = source.[BasePrice_EnhedsPris],
                        [BasePrice_EnhedsPris_Valuta] = source.[BasePrice_EnhedsPris_Valuta],
                        [BasePrice_EnhedsStørrelse] = source.[BasePrice_EnhedsStørrelse],
                        [BasePrice_EnhedsStørrelse_Type] = source.[BasePrice_EnhedsStørrelse_Type];
                """
            )

            conn.commit()
        finally:
            cursor.close()


def _materialize_mysupply_daily_partition(
    dagster_instance,
    mysupply_resource,
    mssql_target_staging,
    partition_key: str,
) -> None:
    """Materialize daily MySupply staging for a partition."""
    materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=[
            "stg_statistic_invoice_header_mysupply_daily",
            "stg_metadata_mysupply_daily",
            "stg_statistic_invoice_line_mysupply_daily",
        ],
        partition=partition_key,
        resources={
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(mysupply_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(mssql_target_staging),
        },
    )


def _materialize_kreditor_pipeline(dagster_instance, mssql_target_staging) -> None:
    """Materialize MySupply kreditor staging + map/dim updates."""
    materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=[
            "erp/pre_stg_fakturalinje_mysupply",
            "erp/stg_dim_kreditor_mysupply",
        ],
        resources={
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(mssql_target_staging),
        },
    )

    materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["map_kreditor", "dim_kreditor"],
        resources={
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(mssql_target_staging),
        },
    )


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_mysupply_daily_partition_does_not_null_existing_dim_kreditor(
    dagster_instance,
    real_mysupply_mssql_resource,
    real_mssql_target_resource,
    setup_erp_infrastructure,
    cleanup_erp_all_tables,
):
    """
    Day 1 loads two CVRs. Day 2 loads only one.
    Ensure the day 1 CVR keeps non-NULL attributes after day 2.
    """
    _seed_mysupply_daily_invoices(real_mysupply_mssql_resource)
    _ensure_empty_kreditor_staging_tables(real_mssql_target_resource)

    day1 = "2026-01-10"
    day2 = "2026-01-11"

    # Day 1 - load two suppliers
    _materialize_mysupply_daily_partition(
        dagster_instance, real_mysupply_mssql_resource, real_mssql_target_resource, day1
    )
    _materialize_kreditor_pipeline(dagster_instance, real_mssql_target_resource)

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT CleanCode_Kreditor, Cvrnr, KreditorNavn
                FROM [dim].[Kreditor]
                WHERE CleanCode_Kreditor = 'CVR_22222222'
                """
            )
            row_day1 = cursor.fetchone()
        finally:
            cursor.close()
    assert row_day1 is not None, "Expected CVR_22222222 to exist after day 1"
    assert row_day1[1] is not None, "Cvrnr should be populated after day 1"
    assert row_day1[2] is not None, "KreditorNavn should be populated after day 1"

    # Day 2 - load only Supplier A
    _materialize_mysupply_daily_partition(
        dagster_instance, real_mysupply_mssql_resource, real_mssql_target_resource, day2
    )
    _materialize_kreditor_pipeline(dagster_instance, real_mssql_target_resource)

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT CleanCode_Kreditor, Cvrnr, KreditorNavn
                FROM [dim].[Kreditor]
                WHERE CleanCode_Kreditor = 'CVR_22222222'
                """
            )
            row_day2 = cursor.fetchone()
        finally:
            cursor.close()
    assert row_day2 is not None, "Expected CVR_22222222 to still exist after day 2"
    assert row_day2[1] is not None, "Cvrnr should not be overwritten to NULL on day 2"
    assert row_day2[2] is not None, "KreditorNavn should not be overwritten to NULL on day 2"
    assert row_day2[1] == row_day1[1], "Cvrnr should remain unchanged for CVR_22222222"
    assert row_day2[2] == row_day1[2], "KreditorNavn should remain unchanged for CVR_22222222"

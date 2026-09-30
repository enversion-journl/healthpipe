# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
MySupply DFM archive layer — 5 UPSERT archives from ext.mysupply_* into
arc.mysupply_*, keyed on `id` with `update_only_on_change=True`.

Each archive owns its own DFM checkpoint in meta.DFMState (scope=staging) and
advances it on success to the value captured by its sibling
dfm_*_mysupply_flow_start asset BEFORE the matching extract reads source —
closing the extract-start → merge-complete gap.

The source ext tables are ext.mysupply_* (source == arc target name).
"""

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource
from dagster import AssetsDefinition


def _arc_config(
    target_table: str,
    non_key_columns: tuple[str, ...],
) -> ArchiveMergeConfig:
    """Build a uniform ArchiveMergeConfig for a MySupply arc table.

    All 5 MySupply archives share the same shape — keyed on `id`, upsert with
    change-detect, no delete detection, gap-free checkpoint via flow_start.
    Only target_table and non_key_columns vary.

    All 5 opt into the single INSERT...SELECT fast path for their
    (empty-target) first load. Safe because MySupply source ``id`` is each
    table's INT primary key, so ext rows are unique on the merge key and
    cannot violate the arc PK.

    The `flow_start_key` table half is derived from `target_table` (the arc
    target name) — the matching extract module's
    `mysupply_flow_start_dfm_table()` writes to the same name. The round-trip
    is asserted in `tests/unit/test_mysupply_flow_start.py`.
    """
    return ArchiveMergeConfig(
        source_schema="ext",
        source_table=target_table,
        target_schema="arc",
        target_table=target_table,
        flow_start_key=("dfm", f"{target_table}_flow_start"),
        merge_keys=("id",),
        non_key_columns=non_key_columns,
        strategy="upsert",
        update_only_on_change=True,
        detect_deletes=False,
        upstream_is_full_load=False,
        enable_bulk_insert_on_first_load=True,
    )


# ---------------------------------------------------------------------------
# ArchiveMergeConfig constants (one per source table)
# ---------------------------------------------------------------------------

HEADER_ARCHIVE_CONFIG = _arc_config(
    target_table="mysupply_statistic_invoice_header",
    non_key_columns=(
        "metadata_id",
        "FakturaNr",
        "FakturaDato",
        "FakturaType",
        "FakturaValutaKode",
        "RegningsModtagerID",
        "RegningsModtagerID_Type",
        "KøbsOrdreNummer",
        "SalgsOrdreNummer",
        "OrdreDato",
        "KundeIdentification",
        "KundeIdentification_Type",
        "DimensionKontoStreng",
        "KundeNavn",
        "KundeAdresseTypeID",
        "KundeVejNavn",
        "KundeHusNr",
        "KundeByNavn",
        "KundePostNr",
        "KundeLandeKode",
        "KundeReferenceID",
        "KundeReferenceNavn",
        "KundeReferenceTelefonNr",
        "KundeReferenceFaxNr",
        "KundeReferenceE-Mail",
        "KundeReferenceRolle",
        "KundeReferenceID_Bogholder",
        "KundeReferenceNavn_Bogholder",
        "KundeReferenceTelefonNr_Bogholder",
        "KundeReferenceFaxNr_Bogholder",
        "KundeReferenceE-Mail_Bogholder",
        "KundeReferenceRolle_Bogholder",
        "KundeReferenceID_Budgetansvarlig",
        "KundeReferenceNavn_Budgetansvarlig",
        "KundeReferenceTelefonNr_Budgetansvarlig",
        "KundeReferenceFaxNr_Budgetansvarlig",
        "KundeReferenceE-Mail_Budgetansvarlig",
        "KundeReferenceRolle_Budgetansvarlig",
        "KundeReferenceID_Rekvirent",
        "KundeReferenceNavn_Rekvirent",
        "KundeReferenceTelefonNr_Rekvirent",
        "KundeReferenceFaxNr_Rekvirent",
        "KundeReferenceE-Mail_Rekvirent",
        "KundeReferenceRolle_Rekvirent",
        "Juridisk_KundeIdentification",
        "Juridisk_KundeIdentification_Type",
        "Juridisk_DimensionKontoStreng",
        "Juridisk_KundeNavn",
        "Juridisk_KundeAdresseTypeID",
        "Juridisk_KundeVejNavn",
        "Juridisk_KundeHusNr",
        "Juridisk_KundeByNavn",
        "Juridisk_KundePostNr",
        "Juridisk_KundeLandeKode",
        "Juridisk_KundeReferenceID",
        "Juridisk_KundeReferenceNavn",
        "Juridisk_KundeReferenceTelefonNr",
        "Juridisk_KundeReferenceFaxNr",
        "Juridisk_KundeReferenceE-Mail",
        "Juridisk_KundeReferenceRolle",
        "Juridisk_KundeReferenceID_Bogholder",
        "Juridisk_KundeReferenceNavn_Bogholder",
        "Juridisk_KundeReferenceTelefonNr_Bogholder",
        "Juridisk_KundeReferenceFaxNr_Bogholder",
        "Juridisk_KundeReferenceE-Mail_Bogholder",
        "Juridisk_KundeReferenceRolle_Bogholder",
        "Juridisk_KundeReferenceID_Budgetansvarlig",
        "Juridisk_KundeReferenceNavn_Budgetansvarlig",
        "Juridisk_KundeReferenceTelefonNr_Budgetansvarlig",
        "Juridisk_KundeReferenceFaxNr_Budgetansvarlig",
        "Juridisk_KundeReferenceE-Mail_Budgetansvarlig",
        "Juridisk_KundeReferenceRolle_Budgetansvarlig",
        "Juridisk_KundeReferenceID_Rekvirent",
        "Juridisk_KundeReferenceNavn_Rekvirent",
        "Juridisk_KundeReferenceTelefonNr_Rekvirent",
        "Juridisk_KundeReferenceFaxNr_Rekvirent",
        "Juridisk_KundeReferenceE-Mail_Rekvirent",
        "Juridisk_KundeReferenceRolle_Rekvirent",
        "DestinationParty_KundeLeveringsIdentifikation",
        "DestinationParty_KundeLeveringsIdentifikation_Type",
        "DestinationParty_KundeNavn",
        "DestinationParty_KundeReferenceIdentifikation",
        "DestinationParty_KundeReferenceNavn",
        "DestinationParty_KundeReferenceTelefonNr",
        "DestinationParty_KundeReferenceFaxNr",
        "DestinationParty_KundeReferenceEMail",
        "DestinationParty_KundeSprog",
        "DestinationParty_KundeLeveringsAdresseID",
        "DestinationParty_KundeLeveringsVejNavn",
        "DestinationParty_KundeLeveringsHusNr",
        "DestinationParty_KundeLeveringsByNavn",
        "DestinationParty_KundeLeveringsPostNr",
        "DestinationParty_KundeLeveringsLandeKode",
        "FakturaUdstederIdentifikation",
        "FakturaUdstederIdentifikation_Type",
        "KreditorIdentifikation",
        "FakturaUdstederNavn",
        "FakturaUdstederAdresseID_Vareafsendelse",
        "FakturaUdstederVejNavn_Vareafsendelse",
        "FakturaUdstederHusNr_Vareafsendelse",
        "FakturaUdstederByNavn_Vareafsendelse",
        "FakturaUdstederPostNr_Vareafsendelse",
        "FakturaUdstederLandKode_Vareafsendelse",
        "FakturaUdstederAdresseID_Betaling",
        "FakturaUdstederVejNavn_Betaling",
        "FakturaUdstederHusNr_Betaling",
        "FakturaUdstederByNavn_Betaling",
        "FakturaUdstederPostNr_Betaling",
        "FakturaUdstederLandKode_Betaling",
        "FakturaUdstederCVRNummer",
        "FakturaUdstederCVRNummer_Type",
        "FakturaUdstederReferenceIdenrifikation",
        "FakturaUdstederReferenceNavn",
        "FakturaUdstederReferenceTelefonNr",
        "FakturaUdstederReferenceFaxNr",
        "FakturaUdstederReferenceEMail",
        "KortArtsKode",
        "BetalingsDato",
        "BetalingsType",
        "BetalingsID",
        "Kreditnummer",
        "Kontonummer",
        "Kontotype",
        "AfdelingsID",
        "BankID",
        "BankNavn",
        "Batalingsbetingelser",
        "ToldKurs",
        "ToldKurs_Type",
        "Kontantrabatsats",
        "Strafrentesats",
        "KontantRabatDato",
        "StartRenteDato",
        "VAT_AfgiftsKategoriKode",
        "VAT_TaxAmounts_AfgiftsKategoriMomsGrundlag",
        "VAT_TaxAmounts_AfgiftsKategoriMomsGrundlag_Valuta",
        "VAT_TaxAmounts_AfgiftsKategoriMomsBeløb",
        "VAT_TaxAmounts_AfgiftsKategoriMomsBeløb_Valuta",
        "VAT_AfgiftsKategori",
        "VAT_AfgiftsKategoriProcent",
        "VAT_CategoryTotal_AfgiftsKategoriMomsGrundlag",
        "VAT_CategoryTotal_AfgiftsKategoriMomsGrundlag_Valuta",
        "VAT_CategoryTotal_AfgiftsKategoriMomsBeløb",
        "VAT_CategoryTotal_AfgiftsKategoriMomsBeløb_Valuta",
        "ZERO-RATED_AfgiftsKategoriKode",
        "ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsGrundlag",
        "ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsGrundlag_Valuta",
        "ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsBeløb",
        "ZERO-RATED_TaxAmounts_AfgiftsKategoriMomsBeløb_Valuta",
        "ZERO-RATED_AfgiftsKategori",
        "ZERO-RATED_AfgiftsKategoriProcent",
        "ZERO-RATED_CategoryTotal_AfgiftsKategoriMomsGrundlag",
        "ZERO-RATED_CategoryTotal_AfgiftsKategoriMomsGrundlag_Valuta",
        "ZERO-RATED_CategoryTotal_AfgiftsKategoriMomsBeløb",
        "ZERO-RATED_CategoryTotal_AfgiftsKategoriMomsBeløb_Valuta",
        "FakturaLinjeTotal",
        "FakturaLinjeTotal_Valuta",
        "FakturaTotal",
        "FakturaTotal_Valuta",
    ),
)

METADATA_ARCHIVE_CONFIG = _arc_config(
    target_table="mysupply_metadata",
    non_key_columns=(
        "Guid",
        "data_unit_identity_unique_name",
        "RowNumber",
        "Type",
        "FileName",
        "ReceivedId",
        "DestinationEP",
        "SystemNavn",
        "OrgUnit",
        "ReceiveTime",
        "ReceiveTimeFormat",
        "Kilde",
        "FakturaTotal",
        "OrdreRef",
        "LeverandorCVR",
        "SaelgerMomsNummer",
        "FirmaNavn",
        "Adresse",
        "Postnummer",
        "By",
    ),
)

LINE_ARCHIVE_CONFIG = _arc_config(
    target_table="mysupply_statistic_invoice_line",
    non_key_columns=(
        "statistic_invoice_header_id",
        "FakturaLinjeNr",
        "AntalEnhederFaktureret",
        "AntalEnhederFaktureret_Type",
        "AntalEnhederFaktureret_List",
        "LinjeBeløb",
        "LinjeBeløb_Valuta",
        "KøbsOrdreNummer",
        "SalgsOrdreNummer",
        "KundeLeveringsIdentifikation",
        "KundeLeveringsIdentifikation_Type",
        "DimensionsKontoStreng",
        "KundeNavn",
        "KundeReferenceIdentifikation",
        "KundeReferenceNavn",
        "KundeReferenceTelefonNr",
        "KundeReferenceFaxNr",
        "KundeReferenceEMail",
        "Kundesprog",
        "KundeLeveringsAdresseID",
        "KundeLeveringsVejNavn",
        "KundeLeveringsHusNr",
        "KundeLeveringsDueslag",
        "KundeLeveringsByNavn",
        "KundeLeveringsPostNr",
        "KundeLeveringsLandeKode",
        "KundeVareNr",
        "KundeVareBeskrivelse",
        "KundensVareNr",
        "KundensVareNr_Type",
        "KundensVareBeskrivelse",
        "FakturaUdstederensVareNr",
        "FakturaUdstederensVareNr_Type",
        "FakturaUdstederensVareBeskrivelse",
        "FabrikantensVareNr",
        "FabrikantensVareNr_Type",
        "FabrikantensVareBeskrivelse",
        "StandardiseretVareNr",
        "StandardiseretVareNr_Type",
        "StandardiseretVareBeskrivelse",
        "KatalogVareNr",
        "KatalogVareNr_Type",
        "KatalogVareBeskrivelse",
        "KatalogIdentifikation",
        "VareKlassifikation",
        "VareKlassifikation_Type",
        "VareKlassifikation_List",
        "AfgiftsKategori",
        "AfgiftsKategoriKode",
        "AfgiftsKategoriProcent",
        "EnhedsPris",
        "EnhedsPris_Valuta",
        "EnhedsStørrelse",
        "EnhedsStørrelse_Type",
        "EnhedsStørrelse_List",
        "LeveranceKrav",
        "LeveringsDatoTypeIdentifikation_1",
        "LeveringsDato_1",
        "LeveringsDatoTypeIdentifikation_2",
        "LeveringsDato_2",
        "LeveringsDatoTypeIdentifikation_3",
        "LeveringsDato_3",
        "LeveringsDatoTypeIdentifikation_4",
        "LeveringsDato_4",
        "Item_FakturaUdstederVareNr",
        "Item_FakturaUdstederVareNavn",
        "Item_KundensVareNr",
        "Item_KundensVareNr_Type",
        "Item_KundensVareBeskrivelse",
        "Item_FakturaUdstederensVareNr",
        "Item_FakturaUdstederensVareNr_Type",
        "Item_FakturaUdstederensVareBeskrivelse",
        "Item_FabrikantensVareNr",
        "Item_FabrikantensVareNr_Type",
        "Item_FabrikantensVareBeskrivelse",
        "Item_StandardiseretVareNr",
        "Item_StandardiseretVareNr_Type",
        "Item_StandardiseretVareBeskrivelse",
        "Item_KatalogVareNr",
        "Item_KatalogVareNr_Type",
        "Item_KatalogVareBeskrivelse",
        "Item_KatalogIdentifikation",
        "Item_VareKlassifikation",
        "Item_VareKlassifikation_Type",
        "Item_VareKlassifikation_List",
        "Item_AfgiftsKategori",
        "Item_AfgiftsKategoriKode",
        "Item_AfgiftsKategoriProcent",
        "Item_EnhedsPris",
        "Item_EnhedsPris_Valuta",
        "Item_EnhedsStørrelse",
        "Item_EnhedsStørrelse_Type",
        "Item_EnhedsStørrelse_List",
        "BasePrice_EnhedsPris",
        "BasePrice_EnhedsPris_Valuta",
        "BasePrice_EnhedsStørrelse",
        "BasePrice_EnhedsStørrelse_Type",
        "BasePrice_EnhedsStørrelse_List",
        "Item_FakturaUdstederVareNavnKort",
    ),
)

HEADER_ALLOWANCE_CHARGE_ARCHIVE_CONFIG = _arc_config(
    target_table="mysupply_statistic_invoice_header_allowance_charge",
    non_key_columns=(
        "statistic_invoice_header_id",
        "GebyrKategori",
        "GebyrKategoriIndikering",
        "GebyrKategoriÅrsag",
        "GebyrKategoriKvantitet",
        "GebyrKategoriKvantitet_Type",
        "GebyrKategoriKvantitet_List",
        "GebyrKategoriTotal",
        "GebyrKategoriFinanskonto",
    ),
)

LINE_ALLOWANCE_CHARGE_ARCHIVE_CONFIG = _arc_config(
    target_table="mysupply_statistic_invoice_line_allowance_charge",
    non_key_columns=(
        "statistic_invoice_line_id",
        "FakturaUdstederGebyrKategori",
        "FakturaUdstederGebyrKategoriIndikering",
        "FakturaUdstederGebyrKategoriÅrsag",
        "FakturaUdstederGebyrKategoriKvantitet",
        "FakturaUdstederGebyrKategoriKvantitet_Type",
        "FakturaUdstederGebyrKategoriKvantitet_List",
        "FakturaUdstederGebyrKategoriTotal",
        "FakturaUdstederGebyrKategoriFinanskonto",
    ),
)


# ---------------------------------------------------------------------------
# Asset factory
# ---------------------------------------------------------------------------


def _make_archive_asset(merge_config: ArchiveMergeConfig) -> AssetsDefinition:
    """Return a single @dg.asset for the given ArchiveMergeConfig.

    Same shape as ils_archive_configs._make_archive_asset / sydax_archive_configs._make_arc_asset.
    The inner function is named `_arc_asset` in every closure; Dagster uses
    the ``name=`` kwarg on the decorator, not the function name, so there is
    no collision.
    """
    entity = merge_config.target_table.removesuffix("_mysupply")

    @dg.asset(
        name=f"arc_{merge_config.target_table}",
        group_name="mysupply_archive",
        compute_kind="mssql",
        deps=[dg.AssetKey([f"ext_{merge_config.source_table}"])],
        tags={
            "domain": "erp",
            "layer": "archive",
            "source": "mysupply",
            "entity": entity,
        },
        description=(
            f"UPSERT ext.{merge_config.source_table} "
            f"→ arc.{merge_config.target_table} (change-detect)."
        ),
    )
    def _arc_asset(
        context: dg.AssetExecutionContext,
        mssql_target_staging: MSSQLResource,
        config: RunModeConfig,
    ) -> dg.MaterializeResult:
        result = execute_archive_merge(context, mssql_target_staging, merge_config, config)

        context.log.info(
            f"MERGEd into arc.{merge_config.target_table} — "
            f"inserted={result.rows_inserted}, updated={result.rows_updated}; "
            f"mode={config.mode}, checkpoint → {result.new_checkpoint}"
        )

        return dg.MaterializeResult(
            metadata=build_archive_metadata(
                result,
                config,
                source_table=f"ext.{merge_config.source_table}",
                target_table=f"arc.{merge_config.target_table}",
            )
        )

    return _arc_asset


# ---------------------------------------------------------------------------
# Asset definitions — one per config
# ---------------------------------------------------------------------------

arc_statistic_invoice_header_mysupply = _make_archive_asset(HEADER_ARCHIVE_CONFIG)
arc_metadata_mysupply = _make_archive_asset(METADATA_ARCHIVE_CONFIG)
arc_statistic_invoice_line_mysupply = _make_archive_asset(LINE_ARCHIVE_CONFIG)
arc_statistic_invoice_header_allowance_charge_mysupply = _make_archive_asset(
    HEADER_ALLOWANCE_CHARGE_ARCHIVE_CONFIG
)
arc_statistic_invoice_line_allowance_charge_mysupply = _make_archive_asset(
    LINE_ALLOWANCE_CHARGE_ARCHIVE_CONFIG
)


mysupply_archive_assets: list[AssetsDefinition] = [
    arc_statistic_invoice_header_mysupply,
    arc_metadata_mysupply,
    arc_statistic_invoice_line_mysupply,
    arc_statistic_invoice_header_allowance_charge_mysupply,
    arc_statistic_invoice_line_allowance_charge_mysupply,
]


__all__ = [
    # ArchiveMergeConfig constants
    "HEADER_ARCHIVE_CONFIG",
    "METADATA_ARCHIVE_CONFIG",
    "LINE_ARCHIVE_CONFIG",
    "HEADER_ALLOWANCE_CHARGE_ARCHIVE_CONFIG",
    "LINE_ALLOWANCE_CHARGE_ARCHIVE_CONFIG",
    # Assets
    "arc_statistic_invoice_header_mysupply",
    "arc_metadata_mysupply",
    "arc_statistic_invoice_line_mysupply",
    "arc_statistic_invoice_header_allowance_charge_mysupply",
    "arc_statistic_invoice_line_allowance_charge_mysupply",
    # Asset list
    "mysupply_archive_assets",
]

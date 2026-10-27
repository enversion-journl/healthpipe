# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SydAX archive layer — 21 UPSERT archives from ext.sydax_* into arc.sydax_*.

Every archive merges on a Kildesystem-prefixed ERP-stable key. Source IDENTITY
columns are preserved as non-key columns for observability but never used to
identify rows. The extract delta strategy is per-table (full load / RdwTs /
business timestamp) and the checkpoint is owned by a flow_start asset for the
delta tables — see the matching extract configs + the 2026-06-25 design spec.
"""

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource

# Per-bucket extract strategy (see the matching extract configs + design spec).
# Bucket A — small/static, extracted as a full load.
_FULL_LOAD_ARC = {
    "sydax_dranst",
    "sydax_art",
    "sydax_finanstransaktionstype",
    "sydax_gruppering_autoriseret",
    "sydax_funktion",
    "sydax_hovedkonto",
    "sydax_gruppering",
    "sydax_afdeling",
    "sydax_omkostningssted_hierarki",
    "sydax_budgetmodel",
    "sydax_budgetpoststatus",
    "sydax_budgetbeloebtype",
}
# Buckets B & C — delta extract whose checkpoint is owned by a flow_start asset.
_FLOW_START_ARC = {
    "sydax_kreditor",
    "sydax_finanskladde",
    "sydax_finanskonto_omkostningssted",
    "sydax_finanskonto",
    "sydax_finanskladdelinje",
    "sydax_kreditortransaktion",
    "sydax_finanspostering",
    "sydax_finanspostering_kreditortrans_relation",
    "sydax_budgetpost",
}

# ext.* is source-prefix orphaned for these 8 tables (sydax.py is the live ext
# writer under the suffix-style name; see overrides.yml ext: section) — the
# arc.* target keeps its clean name (ils_archive_configs.py precedent: the
# orphan marker is ext-schema only, arc is the live DFM archive).
_EXT_ORPHAN_SOURCE = {
    "sydax_afdeling": "sydax_afdeling_legacy_orphan",
    "sydax_hovedkonto": "sydax_hovedkonto_legacy_orphan",
    "sydax_gruppering": "sydax_gruppering_legacy_orphan",
    "sydax_art": "sydax_art_legacy_orphan",
    "sydax_funktion": "sydax_funktion_legacy_orphan",
    "sydax_dranst": "sydax_dranst_legacy_orphan",
    "sydax_kreditor": "sydax_kreditor_legacy_orphan",
    "sydax_finanstransaktionstype": "sydax_finanstransaktionstype_legacy_orphan",
}


def _arc_config(
    target_table: str,
    merge_keys: tuple[str, ...],
    non_key_columns: tuple[str, ...],
    *,
    enable_bulk_insert_on_first_load: bool = False,
) -> ArchiveMergeConfig:
    """Build a SydAX arc ArchiveMergeConfig with its per-bucket extract strategy.

    Checkpoint advancement is owned by a flow_start asset (Buckets B+C) or is
    irrelevant (Bucket A full loads), so no per-table ``checkpoint_query`` is
    needed — every SydAX table is either ``upstream_is_full_load`` or has a
    ``flow_start_key``.

    ``enable_bulk_insert_on_first_load`` opts a table into the single
    INSERT...SELECT fast path for its (empty-target) first load. Safe for the
    tables that enable it because their merge_keys are exactly the arc PK, so
    ext rows unique on the merge key cannot violate it. The large
    delta-extracted transaction/posting tables enable it; the small Bucket A
    tables use the plain MERGE.
    """
    # A column must never appear in both merge_keys and non_key_columns, or
    # build_archive_merge_sql lists it twice in the INSERT column list. This lets
    # callers promote a column into merge_keys (e.g. the StartDateTime version
    # discriminator) without also having to delete it from non_key_columns.
    non_key_columns = tuple(c for c in non_key_columns if c not in merge_keys)
    source_table = _EXT_ORPHAN_SOURCE.get(target_table, target_table)
    return ArchiveMergeConfig(
        source_schema="ext",
        source_table=source_table,
        target_schema="arc",
        target_table=target_table,
        merge_keys=merge_keys,
        non_key_columns=non_key_columns,
        checkpoint_query=None,
        flow_start_key=(
            ("dfm", f"{target_table}_flow_start") if target_table in _FLOW_START_ARC else None
        ),
        upstream_is_full_load=target_table in _FULL_LOAD_ARC,
        strategy="upsert",
        enable_bulk_insert_on_first_load=enable_bulk_insert_on_first_load,
    )


# ---------------------------------------------------------------------------
# ArchiveMergeConfig constants (one per source table)
# ---------------------------------------------------------------------------

AFDELING_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_afdeling",
    merge_keys=("Kildesystem", "RECID", "StartDateTime"),
    non_key_columns=(
        "Afdelingsnummer",
        "AfdelingId",
        "Afdelingstekst",
        "AktivFra",
        "AktivTil",
        "Suspenderet",
        "Ejer_id",
        "TilladIkkeManuelAngivelse",
        "E1",
        "O1",
        "ref_RECID",
        "ref_DirPartyTableId",
        "ref_DimensionAttributeValueId",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "CreatedOn",
        "Modified",
        "RECID",
    ),
)

FINANSKONTO_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_finanskonto",
    enable_bulk_insert_on_first_load=True,
    merge_keys=("Kildesystem", "FinanskontoId", "StartDateTime"),
    non_key_columns=(
        "Kontonummer",
        "ErLukket",
        "ve_RECID",
        "afd_RECID",
        "os_RECID",
        "hk_RECID",
        "ma_RECID",
        "grp_RECID",
        "art_RECID",
        "d1_RECID",
        "d2_RECID",
        "l_RECID",
        "ref_DIMENSIONATTRIBUTEVALUECOMBINATIONId",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "ref_DIMENSIONATTRIBUTEVALUECOMBINATION_RECID",
        "CreatedOn",
        "Modified",
    ),
)

FINANSKONTO_OMKOSTNINGSSTED_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_finanskonto_omkostningssted",
    enable_bulk_insert_on_first_load=True,
    merge_keys=("Kildesystem", "FinanskontoOmkostningsstedid", "StartDateTime"),
    non_key_columns=(
        "FinanskontoId",
        "Omkostningsstedid",
        "ref_LedgerTableId",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "CreatedOn",
        "Modified",
    ),
)

HOVEDKONTO_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_hovedkonto",
    merge_keys=("Kildesystem", "ref_RECID", "StartDateTime"),
    non_key_columns=(
        "HovedkontoId",
        "Hovedkontonummer",
        "Hovedkontotekst",
        "KontoplanId",
        "HovedkontotypeId",
        "RapporteringstypeId",
        "HovedkontoKategoriId",
        "DKForslag",
        "DKKrav",
        "Saldokontrol",
        "ModkontoId",
        "PrimokontoId",
        "SRUKode",
        "Afslut",
        "TvungenReference",
        "CloseType",
        "Autoriseret",
        "Standardkoncernkonto",
        "F1",
        "F2",
        "F3",
        "D1",
        "G1",
        "Valuta",
        "ValutaValidering",
        "BrugerId",
        "BrugerValidering",
        "FinansposteringstypeId",
        "KonteringValidering",
        "AktivFra",
        "AktivTil",
        "Suspenderet",
        "Ejer_id",
        "TilladIkkeManuelAngivelse",
        "ref_MainAccountId",
        "ref_DimensionAttributeValueId",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "CreatedOn",
        "Modified",
    ),
)

OMKOSTNINGSSTED_HIERARKI_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_omkostningssted_hierarki",
    merge_keys=("Kildesystem", "OmkostningsstedHierarkiId", "StartDateTime"),
    non_key_columns=(
        "max_OmkostningsstedId",
        "jeId1",
        "veId2",
        "afId2",
        "veId3",
        "afId3",
        "osId3",
        "veId4",
        "afId4",
        "osId4",
        "veId5",
        "afId5",
        "osId5",
        "veId6",
        "afId6",
        "osId6",
        "veId7",
        "afId7",
        "osId7",
        "veId8",
        "afId8",
        "osId8",
        "veId9",
        "afId9",
        "osId9",
        "veId10",
        "afId10",
        "osId10",
        "veId11",
        "afId11",
        "osId11",
        "veId12",
        "afId12",
        "osId12",
        "veId13",
        "afId13",
        "osId13",
        "veId14",
        "afId14",
        "osId14",
        "veId15",
        "afId15",
        "osId15",
        "veId16",
        "afId16",
        "osId16",
        "veId17",
        "afId17",
        "osId17",
        "veId18",
        "afId18",
        "osId18",
        "veId19",
        "afId19",
        "osId19",
        "veId20",
        "afId20",
        "osId20",
        "max_VirksomhedsenhedId",
        "max_AfdelingId",
        "nummer1",
        "nummer2",
        "nummer3",
        "nummer4",
        "nummer5",
        "nummer6",
        "nummer7",
        "nummer8",
        "nummer9",
        "nummer10",
        "nummer11",
        "nummer12",
        "nummer13",
        "nummer14",
        "nummer15",
        "nummer16",
        "nummer17",
        "nummer18",
        "nummer19",
        "nummer20",
        "tekst1",
        "tekst2",
        "tekst3",
        "tekst4",
        "tekst5",
        "tekst6",
        "tekst7",
        "tekst8",
        "tekst9",
        "tekst10",
        "tekst11",
        "tekst12",
        "tekst13",
        "tekst14",
        "tekst15",
        "tekst16",
        "tekst17",
        "tekst18",
        "tekst19",
        "tekst20",
        "Aktiv",
        "HierarkiId",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "Slettet",
        "CreatedOn",
        "Modified",
    ),
)

GRUPPERING_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_gruppering",
    merge_keys=("Kildesystem", "RECID", "StartDateTime"),
    non_key_columns=(
        "GrupperingId",
        "Grupperingsnummer",
        "Grupperingstekst",
        "GrupperingDimension",
        "AktivFra",
        "AktivTil",
        "Suspenderet",
        "Ejer_id",
        "TilladIkkeManuelAngivelse",
        "Ejerforhold",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "ref_DimensionAttributeValueId",
        "CreatedOn",
        "Modified",
    ),
)

GRUPPERING_AUTORISERET_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_gruppering_autoriseret",
    merge_keys=("Kildesystem", "ref_RECID", "StartDateTime"),
    non_key_columns=(
        "G1Id",
        "G1",
        "G1Tekst",
        "F1",
        "F2",
        "F3",
        "D1",
        "GyldigFra",
        "GyldigTil",
        "Autoriseret",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "ref_LedgerTextId",
        "CreatedOn",
        "Modified",
    ),
)

ART_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_art",
    merge_keys=("Kildesystem", "RECID", "StartDateTime"),
    non_key_columns=(
        "ArtId",
        "Artsnummer",
        "Artstekst",
        "AktivFra",
        "AktivTil",
        "Suspenderet",
        "Ejer_id",
        "TilladIkkeManuelAngivelse",
        "A1",
        "A2",
        "Momsretning",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "ref_DimensionAttributeValueId",
        "ArtDimension",
        "CreatedOn",
        "Modified",
    ),
)

FUNKTION_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_funktion",
    merge_keys=("Kildesystem", "ref_RECID", "StartDateTime"),
    non_key_columns=(
        "FunktionId",
        "F1",
        "F2",
        "F3",
        "FunktionTekst",
        "GyldigFra",
        "GyldigTil",
        "Autoriseret",
        "ref_LedgerTextId",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "CreatedOn",
        "Modified",
    ),
)

DRANST_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_dranst",
    merge_keys=("Kildesystem", "ref_RECID", "StartDateTime"),
    non_key_columns=(
        "DranstId",
        "D1",
        "D1Tekst",
        "GyldigFra",
        "GyldigTil",
        "Autoriseret",
        "ref_LedgerTextId",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "CreatedOn",
        "Modified",
    ),
)

KREDITOR_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_kreditor",
    merge_keys=("Kildesystem", "KreditorId", "StartDateTime"),
    non_key_columns=(
        "Kreditornummer",
        "Kreditornavn",
        "Soegenavn",
        "JuridiskEnhed",
        "ErBlokeret",
        "Oprettelsestidspunkt",
        "OprettetAf",
        "SidstAendretTidspunkt",
        "SidstAendretAf",
        "ref_VendtableId",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "DokumentId",
        "DataAreaId",
        "CreatedOn",
        "Modified",
    ),
)

FINANSPOSTERING_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_finanspostering",
    enable_bulk_insert_on_first_load=True,
    merge_keys=("Kildesystem", "ref_LedgertransId"),
    non_key_columns=(
        "FinansposteringId",
        "FinanskontoPrismeId",
        "FinanskontoId",
        "Bogfoeringsdato",
        "Posteringsbeloeb",
        "Valutakode",
        "PosteringsbeloebValuta",
        "Posteringstekst",
        "AfdelingPrismeId",
        "Bilagsnummer",
        "ErModpostering",
        "DebetKredit",
        "FinanskladdeId",
        "FinanstransaktionstypeId",
        "FinansposteringstypeId",
        "KontrolleretDato",
        "KontrolleretAf",
        "Oprettelsestidspunkt",
        "OprettetAf",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "BilagsnummerID",
        "Posteringsmoms",
        "PosteringsbeloebInklMoms",
        "DokumentId",
        "BærerId",
        "FormålId",
        "RefusionModtager",
        "Lager_Kasse",
        "Ekspeditionsnummer",
        "SDIdentifier",
        "Primosaldo",
        "DataAreaId",
        "FinansrelationId",
        "Oplysningspligt",
        "CreatedOn",
        "Modified",
        "Varemomsgruppe",
        "Bogføringstypetekst",
    ),
)

KREDITORTRANSAKTION_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_kreditortransaktion",
    merge_keys=("Kildesystem", "ref_VendTransId"),
    non_key_columns=(
        "KreditortransaktionId",
        "KreditorId",
        "Transaktionsdato",
        "Bilagsnummer",
        "Fakturanummer",
        "EANnummer",
        "Fakturadato",
        "Transaktionsbeloeb",
        "Valutakode",
        "TransaktionsbeloebValuta",
        "UdlignetBeloeb",
        "UdlignetBeloebValuta",
        "Transaktionstekst",
        "Forfaldsdato",
        "ErLukket",
        "LukketDato",
        "AfdelingPrismeId",
        "JuridiskEnhed",
        "Bankregistreringsnummer",
        "Bankkontonummer",
        "ErKorrektion",
        "KreditortransaktionstypeId",
        "ErGodkendt",
        "GodkendtDato",
        "GodkendtAf",
        "Oprettelsestidspunkt",
        "OprettetAf",
        "IsCounterpost",
        "CounterpostsThisKreditortransaktionId",
        "Betalingstekst",
        "BetalingReturkode",
        "BetalingYderligerInfo",
        "Virksomhedsenhed",
        "VirksomhedsenhedNavn",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "SidstUdlignetDato",
        "SidstUdlignetBilagsnummer",
        "DokumentId",
        "DataAreaId",
        "CreatedOn",
        "Modified",
    ),
)

FINANSPOSTERING_KREDITORTRANS_RELATION_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_finanspostering_kreditortrans_relation",
    enable_bulk_insert_on_first_load=True,
    merge_keys=("Kildesystem", "ref_LedgertransId", "KreditortransaktionId"),
    non_key_columns=(
        "FinansposteringKreditortransRelationId",
        "FinansposteringId",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "CreatedOn",
        "Modified",
    ),
)

FINANSKLADDE_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_finanskladde",
    enable_bulk_insert_on_first_load=True,
    merge_keys=("Kildesystem", "FinanskladdeId", "StartDateTime"),
    non_key_columns=(
        "Kladdenummer",
        "Kladdetekst",
        "Kladdenavn",
        "ErLukket",
        "ref_LedgerjournaltableId",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "DokumentId",
        "DataAreaId",
        "Maskinnummer",
        "CreatedOn",
        "Modified",
    ),
)

FINANSKLADDELINJE_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_finanskladdelinje",
    enable_bulk_insert_on_first_load=True,
    merge_keys=("Kildesystem", "ref_LedgertransId"),
    non_key_columns=(
        "FinanskladdelinjeId",
        "Linjenummer",
        "dato",
        "Beskrivelse",
        "Debet",
        "Kredit",
        "FinanskladdeId",
        "FinanstransaktionstypeId",
        "Ekspeditionsnummer",
        "Maskinnummer",
        "FinanskontoId",
        "BilagsnummerID",
        "DokumentId",
        "ref_RecId",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "CreatedOn",
        "Modified",
    ),
)

FINANSTRANSAKTIONSTYPE_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_finanstransaktionstype",
    merge_keys=("Kildesystem", "FinanstransaktionstypeId", "StartDateTime"),
    non_key_columns=(
        "Transaktionstypenummer",
        "Transaktionstypetekst",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "CreatedOn",
        "Modified",
    ),
)

BUDGETPOST_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_budgetpost",
    enable_bulk_insert_on_first_load=True,
    merge_keys=("Kildesystem", "ref_LedgerbudgetId"),
    non_key_columns=(
        "BudgetmodelId",
        "FinanskontoId",
        "BudgetpostStatusId",
        "BeloebstypeId",
        "Budgetdato",
        "BudgetAdoptedDate",
        "Oprettelsestidspunkt",
        "SidstAendretTidspunkt",
        "Budgetbeloeb",
        "BudgetbeloebValuta",
        "Valutakode",
        "DebetKredit",
        "Budgetkode",
        "Ændringstype",
        "ÆndringstypeBeskrivelse",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "CreatedOn",
        "Modified",
    ),
)

BUDGETMODEL_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_budgetmodel",
    merge_keys=("Kildesystem", "BudgetmodelId", "StartDateTime"),
    non_key_columns=(
        "ref_BudgetmodelId",
        "Budgetmodelnummer",
        "Budgetmodeltekst",
        "Budgetmodeltype",
        "ErLukket",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "CreatedOn",
        "Modified",
    ),
)

BUDGETPOSTSTATUS_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_budgetpoststatus",
    merge_keys=("Kildesystem", "BudgetpostStatusId", "StartDateTime"),
    non_key_columns=(
        "BudgetpostStatustekst",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "CreatedOn",
        "Modified",
    ),
)

BUDGETBELOEBTYPE_SYDAX_ARCHIVE_CONFIG = _arc_config(
    target_table="sydax_budgetbeloebtype",
    merge_keys=("Kildesystem", "BudgetbeløbTypeId", "StartDateTime"),
    non_key_columns=(
        "BudgetbeløbTypetekst",
        "RunId",
        "StartDateTime",
        "EndDateTime",
        "RdwTs",
        "CreatedOn",
        "Modified",
    ),
)


# ---------------------------------------------------------------------------
# Asset generation
# ---------------------------------------------------------------------------


def _make_arc_asset(cfg: ArchiveMergeConfig):
    """Build a Dagster asset that MERGEs ext.<table>_dfm into arc.<table>.

    All sydax archive assets share the same structure — only the config
    varies — so the asset function is generated from the config rather
    than repeated 17 times. Entity tag, asset name, and dep key are
    derived from cfg.target_table / cfg.source_table.
    """
    name = f"arc_{cfg.target_table}"
    ext_key = f"ext_{cfg.source_table}"
    entity = cfg.target_table.removesuffix("_sydax")
    merge_keys_str = ", ".join(cfg.merge_keys)

    @dg.asset(
        name=name,
        group_name="sydax_archive",
        compute_kind="mssql",
        deps=[dg.AssetKey([ext_key])],
        tags={"domain": "erp", "layer": "archive", "source": "sydax", "entity": entity},
        description=f"UPSERT ext.{cfg.source_table} → arc.{cfg.target_table} on ({merge_keys_str}).",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_staging: MSSQLResource,
        config: RunModeConfig,
    ) -> dg.MaterializeResult:
        result = execute_archive_merge(context, mssql_target_staging, cfg, config)

        context.log.info(
            f"MERGEd {result.row_count} rows into arc.{cfg.target_table}; "
            f"mode={config.mode}, checkpoint → {result.new_checkpoint}"
        )

        return dg.MaterializeResult(
            metadata=build_archive_metadata(
                result,
                config,
                source_table=f"ext.{cfg.source_table}",
                target_table=f"arc.{cfg.target_table}",
            )
        )

    _asset.__name__ = name  # type: ignore[attr-defined]
    _asset.__qualname__ = name  # type: ignore[attr-defined]
    return _asset


# Assets — one per config, in matrix order from the design spec.

arc_afdeling_sydax = _make_arc_asset(AFDELING_SYDAX_ARCHIVE_CONFIG)
arc_finanskonto_sydax = _make_arc_asset(FINANSKONTO_SYDAX_ARCHIVE_CONFIG)
arc_finanskonto_omkostningssted_sydax = _make_arc_asset(
    FINANSKONTO_OMKOSTNINGSSTED_SYDAX_ARCHIVE_CONFIG
)
arc_hovedkonto_sydax = _make_arc_asset(HOVEDKONTO_SYDAX_ARCHIVE_CONFIG)
arc_omkostningssted_hierarki_sydax = _make_arc_asset(OMKOSTNINGSSTED_HIERARKI_SYDAX_ARCHIVE_CONFIG)
arc_gruppering_sydax = _make_arc_asset(GRUPPERING_SYDAX_ARCHIVE_CONFIG)
arc_gruppering_autoriseret_sydax = _make_arc_asset(GRUPPERING_AUTORISERET_SYDAX_ARCHIVE_CONFIG)
arc_art_sydax = _make_arc_asset(ART_SYDAX_ARCHIVE_CONFIG)
arc_funktion_sydax = _make_arc_asset(FUNKTION_SYDAX_ARCHIVE_CONFIG)
arc_dranst_sydax = _make_arc_asset(DRANST_SYDAX_ARCHIVE_CONFIG)
arc_kreditor_sydax = _make_arc_asset(KREDITOR_SYDAX_ARCHIVE_CONFIG)
arc_finanspostering_sydax = _make_arc_asset(FINANSPOSTERING_SYDAX_ARCHIVE_CONFIG)
arc_kreditortransaktion_sydax = _make_arc_asset(KREDITORTRANSAKTION_SYDAX_ARCHIVE_CONFIG)
arc_finanspostering_kreditortrans_relation_sydax = _make_arc_asset(
    FINANSPOSTERING_KREDITORTRANS_RELATION_SYDAX_ARCHIVE_CONFIG
)
arc_finanskladde_sydax = _make_arc_asset(FINANSKLADDE_SYDAX_ARCHIVE_CONFIG)
arc_finanskladdelinje_sydax = _make_arc_asset(FINANSKLADDELINJE_SYDAX_ARCHIVE_CONFIG)
arc_finanstransaktionstype_sydax = _make_arc_asset(FINANSTRANSAKTIONSTYPE_SYDAX_ARCHIVE_CONFIG)
arc_budgetpost_sydax = _make_arc_asset(BUDGETPOST_SYDAX_ARCHIVE_CONFIG)
arc_budgetmodel_sydax = _make_arc_asset(BUDGETMODEL_SYDAX_ARCHIVE_CONFIG)
arc_budgetpoststatus_sydax = _make_arc_asset(BUDGETPOSTSTATUS_SYDAX_ARCHIVE_CONFIG)
arc_budgetbeloebtype_sydax = _make_arc_asset(BUDGETBELOEBTYPE_SYDAX_ARCHIVE_CONFIG)


sydax_archive_assets = [
    arc_afdeling_sydax,
    arc_finanskonto_sydax,
    arc_finanskonto_omkostningssted_sydax,
    arc_hovedkonto_sydax,
    arc_omkostningssted_hierarki_sydax,
    arc_gruppering_sydax,
    arc_gruppering_autoriseret_sydax,
    arc_art_sydax,
    arc_funktion_sydax,
    arc_dranst_sydax,
    arc_kreditor_sydax,
    arc_finanspostering_sydax,
    arc_kreditortransaktion_sydax,
    arc_finanspostering_kreditortrans_relation_sydax,
    arc_finanskladde_sydax,
    arc_finanskladdelinje_sydax,
    arc_finanstransaktionstype_sydax,
    arc_budgetpost_sydax,
    arc_budgetmodel_sydax,
    arc_budgetpoststatus_sydax,
    arc_budgetbeloebtype_sydax,
]


__all__ = [
    # ArchiveMergeConfig constants
    "AFDELING_SYDAX_ARCHIVE_CONFIG",
    "FINANSKONTO_SYDAX_ARCHIVE_CONFIG",
    "FINANSKONTO_OMKOSTNINGSSTED_SYDAX_ARCHIVE_CONFIG",
    "HOVEDKONTO_SYDAX_ARCHIVE_CONFIG",
    "OMKOSTNINGSSTED_HIERARKI_SYDAX_ARCHIVE_CONFIG",
    "GRUPPERING_SYDAX_ARCHIVE_CONFIG",
    "GRUPPERING_AUTORISERET_SYDAX_ARCHIVE_CONFIG",
    "ART_SYDAX_ARCHIVE_CONFIG",
    "FUNKTION_SYDAX_ARCHIVE_CONFIG",
    "DRANST_SYDAX_ARCHIVE_CONFIG",
    "KREDITOR_SYDAX_ARCHIVE_CONFIG",
    "FINANSPOSTERING_SYDAX_ARCHIVE_CONFIG",
    "KREDITORTRANSAKTION_SYDAX_ARCHIVE_CONFIG",
    "FINANSPOSTERING_KREDITORTRANS_RELATION_SYDAX_ARCHIVE_CONFIG",
    "FINANSKLADDE_SYDAX_ARCHIVE_CONFIG",
    "FINANSKLADDELINJE_SYDAX_ARCHIVE_CONFIG",
    "FINANSTRANSAKTIONSTYPE_SYDAX_ARCHIVE_CONFIG",
    "BUDGETPOST_SYDAX_ARCHIVE_CONFIG",
    "BUDGETMODEL_SYDAX_ARCHIVE_CONFIG",
    "BUDGETPOSTSTATUS_SYDAX_ARCHIVE_CONFIG",
    "BUDGETBELOEBTYPE_SYDAX_ARCHIVE_CONFIG",
    # Assets
    "arc_afdeling_sydax",
    "arc_finanskonto_sydax",
    "arc_finanskonto_omkostningssted_sydax",
    "arc_hovedkonto_sydax",
    "arc_omkostningssted_hierarki_sydax",
    "arc_gruppering_sydax",
    "arc_gruppering_autoriseret_sydax",
    "arc_art_sydax",
    "arc_funktion_sydax",
    "arc_dranst_sydax",
    "arc_kreditor_sydax",
    "arc_finanspostering_sydax",
    "arc_kreditortransaktion_sydax",
    "arc_finanspostering_kreditortrans_relation_sydax",
    "arc_finanskladde_sydax",
    "arc_finanskladdelinje_sydax",
    "arc_finanstransaktionstype_sydax",
    "arc_budgetpost_sydax",
    "arc_budgetmodel_sydax",
    "arc_budgetpoststatus_sydax",
    "arc_budgetbeloebtype_sydax",
    "sydax_archive_assets",
]

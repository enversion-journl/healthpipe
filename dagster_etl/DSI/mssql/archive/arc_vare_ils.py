# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Archive: ext.vare_ils_delta → arc.vare_ils

UPSERT archive with DFM checkpoint. Business logic mirrors arc_vare_ils.py.
"""

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource

VARE_ILS_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="vare_ils_delta",
    target_schema="arc",
    target_table="vare_ils",
    merge_keys=("prrowid",),
    non_key_columns=(
        "vnr",
        "navn",
        "navn2",
        "varestr",
        "navn_conc",
        "basisenh",
        "faelvnr",
        "faelnavn",
        "faelnavn2",
        "ean_vnr",
        "vareart",
        "varegrp",
        "varetype",
        "varelis",
        "momskd",
        "afgiftkd",
        "miljo",
        "ansvar",
        "adr_kode",
        "un_nr",
        "sik_fak",
        "holddage",
        "billed",
        "sw_edivare",
        "sw_udlkontr",
        "sw_batch",
        "sw_ovf_gs",
        "sw_opret_gs",
        "sw_ovf_dk200",
        "sw_opret_dk2000",
        "sw_synk",
        "sw_usyn_gs",
        "sw_gs",
        "aktiv",
        "oprdto",
        "oprtid",
        "oprbruger",
        "billedli",
        "sw_web",
        "unspc",
        "sampak",
        "txt1",
        "txt2",
        "bruger",
        "sysdato",
        "seqnr",
        "varetype2",
        "skat",
        "synonym_",
        "kategori",
        "sysklk",
        "varecpv",
        "pak1",
        "pak2",
        "pak3",
        "markering",
        "sw_steril",
        "navneng",
        "navneng2",
        "ejlukkes",
        "sw_farlig",
        "farligfarve",
        "pvnr",
        "pvarenavn",
        "pvarestr",
        "pnavn",
        "implantatside",
        "extordreenh",
        "unspsc2",
        "paktyp",
        "intrastat",
        "unspscint",
        "udbkat",
        "udbegnet",
        "appafh",
        "steril",
        "fraafttype",
        "sw_indberet",
        "Pro2SrcPDB",
        "pro2created",
        "pro2modified",
        "pmdmid",
        "sw_ikkeeu",
        "sw_sygehusapotek",
        "sw_kritiskvareliste",
        "sw_positivliste",
        "sw_hjemmepatient",
        "sw_obstekst",
    ),
    # CONVERT(..., 121) yields ISO; a plain CAST of `datetime` produces style-0
    # ("Jun 23 2026 12:00AM"), which the checkpoint validator rejects.
    checkpoint_query="SELECT CONVERT(NVARCHAR(50), MAX([sysdato]), 121) FROM [arc].[vare_ils]",
    strategy="upsert",
)


@dg.asset(
    name="arc_vare_ils",
    group_name="extract_ils",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_vare_ils_delta"])],
    tags={"domain": "erp", "layer": "archive", "source": "ils", "entity": "vare"},
    description="UPSERT ext.vare_ils_delta → arc.vare_ils.",
)
def arc_vare_ils(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    result = execute_archive_merge(context, mssql_target_staging, VARE_ILS_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.vare_ils; "
        f"mode={config.mode}, checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result, config, source_table="ext.vare_ils_delta", target_table="arc.vare_ils"
        )
    )

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
OS2MO CSV extraction assets for manual ConceptMap mappings.

Reads standardised CSV files from DSI/csv_files/data/ and makes them
available as Dagster assets so downstream ConceptMap builders can depend on them.
"""

from pathlib import Path

import dagster as dg
from DSI.connectors.csv_files.extractor import CsvExtractor
from DSI.connectors.csv_files.models import CsvExtractionConfig

CSV_DIR = Path(__file__).resolve().parent / "data"

_CONCEPTMAP_CSVS = {
    "okosted_to_sor": "Manual mappings for okosted codes to SOR codes",
    "rekvirent_to_sor": "Manual mappings for rekvirent codes to SOR codes",
    "lager_to_sor": "Manual mappings for lager codes to SOR codes",
    "ean_to_sor": "Manual mappings for ean codes to SOR codes",
}


def _build_csv_asset(name: str, description: str):
    @dg.asset(
        name=f"ext_os2mo_{name}",
        group_name="os2mo_extract",
        compute_kind="csv",
        tags={"layer": "extract", "source": "os2mo", "entity": name},
        description=description,
    )
    def _asset(context: dg.AssetExecutionContext) -> list[dict]:
        config = CsvExtractionConfig(
            file_path=str(CSV_DIR / f"{name}.csv"),
            description=description,
        )
        extractor = CsvExtractor(context=context, source_system="os2mo")
        return extractor.extract(config).data

    return _asset


ext_os2mo_okosted_to_sor = _build_csv_asset("okosted_to_sor", _CONCEPTMAP_CSVS["okosted_to_sor"])
ext_os2mo_rekvirent_to_sor = _build_csv_asset(
    "rekvirent_to_sor", _CONCEPTMAP_CSVS["rekvirent_to_sor"]
)
ext_os2mo_lager_to_sor = _build_csv_asset("lager_to_sor", _CONCEPTMAP_CSVS["lager_to_sor"])
ext_os2mo_ean_to_sor = _build_csv_asset("ean_to_sor", _CONCEPTMAP_CSVS["ean_to_sor"])

os2mo_assets = [
    ext_os2mo_okosted_to_sor,
    ext_os2mo_rekvirent_to_sor,
    ext_os2mo_lager_to_sor,
    ext_os2mo_ean_to_sor,
]

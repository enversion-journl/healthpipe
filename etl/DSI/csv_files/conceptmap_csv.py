# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Curated ConceptMap CSV extraction assets.

Reads the curated mapping CSV files from DSI/csv_files/data/ and makes them
available as Dagster assets so the ConceptMap builders can depend on them.
These files are the only mapping source for the ERP maps (ean/lager/
rekvirent/okosted -> SOR).

The assets were previously named ``ext_os2mo_*``: the long-term plan is to
replace the repo-tracked CSVs with a real OS2MO integration (GraphQL) and pull
the mappings from there, but that integration does not exist yet — hence the
honest ``ext_csv_*`` naming until it does.
"""

from pathlib import Path

import dagster as dg
from DSI.connectors.csv_files.extractor import CsvExtractor
from DSI.connectors.csv_files.models import CsvExtractionConfig

CSV_DIR = Path(__file__).resolve().parent / "data"

_CONCEPTMAP_CSVS = {
    "okosted_to_sor": "Curated mappings for okosted codes to SOR codes",
    "rekvirent_to_sor": "Curated mappings for rekvirent codes to SOR codes",
    "lager_to_sor": "Curated mappings for lager codes to SOR codes",
    "ean_to_sor": "Curated mappings for ean codes to SOR codes",
}


def _build_csv_asset(name: str, description: str):
    @dg.asset(
        name=f"ext_csv_{name}",
        group_name="conceptmap_csv_extract",
        compute_kind="csv",
        tags={"layer": "extract", "source": "csv", "entity": name},
        description=description,
    )
    def _asset(context: dg.AssetExecutionContext) -> list[dict]:
        config = CsvExtractionConfig(
            file_path=str(CSV_DIR / f"{name}.csv"),
            description=description,
        )
        extractor = CsvExtractor(context=context, source_system="csv")
        return extractor.extract(config).data

    return _asset


ext_csv_okosted_to_sor = _build_csv_asset("okosted_to_sor", _CONCEPTMAP_CSVS["okosted_to_sor"])
ext_csv_rekvirent_to_sor = _build_csv_asset(
    "rekvirent_to_sor", _CONCEPTMAP_CSVS["rekvirent_to_sor"]
)
ext_csv_lager_to_sor = _build_csv_asset("lager_to_sor", _CONCEPTMAP_CSVS["lager_to_sor"])
ext_csv_ean_to_sor = _build_csv_asset("ean_to_sor", _CONCEPTMAP_CSVS["ean_to_sor"])

conceptmap_csv_assets = [
    ext_csv_okosted_to_sor,
    ext_csv_rekvirent_to_sor,
    ext_csv_lager_to_sor,
    ext_csv_ean_to_sor,
]

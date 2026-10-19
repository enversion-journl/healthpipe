# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Harvest LPR3 Metadata and ExportLog; no data CSV rows are parsed or saved.

The archive is decompressed in full because SDS places its control files last.
Run from etl: python -m scripts.lpr3_schema DELIVERY.tar.bz2 --output schema.json
"""

import argparse
import json
from pathlib import Path

from DSI.csv_files._lpr3_control import harvest_schema


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("delivery", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--encoding", default="cp1252")
    args = parser.parse_args()
    schema = harvest_schema(args.delivery, encoding=args.encoding)
    # Write only after all control-file checks pass.
    args.output.write_text(
        json.dumps(schema, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"LPR3: harvested metadata for {len(schema)} tables; data rows were not read")


if __name__ == "__main__":
    main()

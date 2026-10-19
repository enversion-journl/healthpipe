# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Regenerate the LPR3 archive DDL from the single dataset contract.

Run from etl: python -m scripts.lpr3_generate_contract. The generated file is
reviewed and checked into Git; generation never connects to a database.
"""

from pathlib import Path

from DSI.csv_files.lpr3_datasets import archive_ddl


def main():
    root = Path(__file__).parents[1]
    (root / "infrastructure/ddl/tables/lpr3/00_archive.sql").write_text(archive_ddl())


if __name__ == "__main__":
    main()

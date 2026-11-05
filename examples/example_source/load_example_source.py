# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Load example_source.sql into the devcontainer SQL Server.

Connects as ``sa`` with the same variables as
``.devcontainer/startup_scripts/create_databases.py`` and runs each ``GO`` batch.
"""

import os
import re
import sys
from pathlib import Path

import pymssql

HOST = os.getenv("MSSQL_BOOTSTRAP_HOST", "mssql")
PORT = os.getenv("MSSQL_BOOTSTRAP_PORT", "1433")
SA_PASSWORD = os.getenv("MSSQL_SA_PASSWORD", "tempPassword1!")
SQL_FILE = Path(__file__).with_name("example_source.sql")


def main() -> int:
    batches = re.split(r"(?im)^\s*GO\s*$", SQL_FILE.read_text(encoding="utf-8"))
    conn = pymssql.connect(server=HOST, port=PORT, user="sa", password=SA_PASSWORD, autocommit=True)
    cursor = conn.cursor()
    for batch in batches:
        if batch.strip():
            cursor.execute(batch)
    cursor.execute("SELECT (SELECT COUNT(*) FROM dbo.sgh), (SELECT COUNT(*) FROM dbo.zz_oper)")
    sgh_rows, zz_oper_rows = cursor.fetchone()
    print(f"example_source.dbo.sgh: {sgh_rows} rows")
    print(f"example_source.dbo.zz_oper: {zz_oper_rows} rows")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

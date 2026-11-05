# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from . import config
from .checker import Report, check
from .ddl_parser import parse_ddl
from .lineage_reader import read_lineage

__all__ = ["run"]


def run() -> Report:
    """Run the verifier against the repo's real DDL and lineage files."""
    ddl = parse_ddl(config.DDL_TABLES_DIR, config.TARGET_SCHEMAS)
    lineage, malformed = read_lineage(config.LINEAGE_DIR, config.TARGET_SCHEMAS)
    return check(
        ddl,
        lineage,
        malformed=malformed,
        system_columns=config.SYSTEM_COLUMNS,
        ignored_tables=config.IGNORED_TABLES,
    )

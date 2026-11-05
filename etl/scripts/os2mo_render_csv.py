#!/usr/bin/env python

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Render the OS2mo codesystem files locally, without Dagster.

Builds each file with the same code as the ``os2mo_<codesystem>_export`` asset
— the same read, the same rows, the same CSV bytes — and writes it to a local
folder, so the format and data can be inspected or sent by hand. Read-only: it
writes nothing to the database and uploads nothing.

Reads the staging database from the ``MSSQL_TARGET_STAGING_*`` variables, as
the code location does (a ``.env`` in the repo root is picked up).

    python etl/scripts/os2mo_render_csv.py --out ~/os2mo
    python etl/scripts/os2mo_render_csv.py --codesystem okosted --out ~/os2mo
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# core.resources reads the source-system connections below at import, and
# parses or inspects some of them. The script never connects to those systems,
# so placeholders — like CI's .prod-db-env — let it import in an environment
# that only has the staging variables. A real value already set wins.
_SOURCE_SYSTEM_PLACEHOLDERS = {
    f"{system}_{field}": (
        ("1521" if system == "IFDB_PROD_ORACLE" else "1433") if field == "PORT" else "placeholder"
    )
    for system in ("MYSUPPLY_PROD_MSSQL", "SYDAX_PROD_MSSQL", "ILS_PROD_MSSQL", "IFDB_PROD_ORACLE")
    for field in ("SERVER", "PORT", "DATABASE", "USERNAME", "SCHEMA")
}
for _variable, _placeholder in _SOURCE_SYSTEM_PLACEHOLDERS.items():
    os.environ.setdefault(_variable, _placeholder)

from core.resources import MSSQLResource  # noqa: E402
from DSO.export.os2mo.codesystem_assets import read_export_set  # noqa: E402
from DSO.export.os2mo.codesystems import CODESYSTEM_EXPORTS  # noqa: E402
from utils.os2mo.csv_format import render_csv, write_csv  # noqa: E402

SPECS = {spec.asset_prefix: spec for spec in CODESYSTEM_EXPORTS}


STAGING_VARIABLES = (
    "MSSQL_TARGET_STAGING_HOST",
    "MSSQL_TARGET_STAGING_DB",
    "MSSQL_TARGET_STAGING_USER",
    "MSSQL_TARGET_STAGING_PASSWORD",
)


def staging_from_env() -> MSSQLResource:
    """The staging database, configured like ``mssql_target_staging``."""
    missing = [name for name in STAGING_VARIABLES if not os.getenv(name)]
    if missing:
        sys.exit(
            "Missing staging connection: " + ", ".join(missing) + ". Set them in the "
            ".env in the repo root (MSSQL_TARGET_STAGING_PORT defaults to 1433)."
        )
    return MSSQLResource(
        host=os.getenv("MSSQL_TARGET_STAGING_HOST", "localhost"),
        port=int(os.getenv("MSSQL_TARGET_STAGING_PORT", "1433")),
        database=os.getenv("MSSQL_TARGET_STAGING_DB", "DW"),
        user=os.getenv("MSSQL_TARGET_STAGING_USER", "sa"),
        password=os.environ["MSSQL_TARGET_STAGING_PASSWORD"],
    )


def render(mssql: MSSQLResource, codesystems: list[str], out_dir: Path) -> list[Path]:
    """Build and write one file per codesystem; return the written paths."""
    built_at = datetime.now().replace(microsecond=0)
    written = []
    for name in codesystems:
        spec = SPECS[name]
        export_set = read_export_set(mssql, spec)
        path = write_csv(render_csv(spec, export_set.rows), out_dir / spec.filename(built_at))
        notes = [
            f"{count} {what}"
            for count, what in (
                (export_set.codes_unreachable, "left out, not reachable from a root"),
                (export_set.codes_gone, "left out, gone from the source"),
            )
            if count
        ]
        if export_set.unreadable:
            notes.append(
                f"unreadable values (dates defaulted, flags empty): {export_set.unreadable}"
            )
        print(
            f"{spec.name}: {len(export_set.rows)} rows -> {path}"
            + (f"  ({'; '.join(notes)})" if notes else "")
        )
        written.append(path)
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--codesystem",
        choices=[*SPECS, "all"],
        default="all",
        help="Which codesystem to render (default: all).",
    )
    parser.add_argument(
        "--out", type=Path, default=Path("os2mo_export"), help="Output folder (created)."
    )
    args = parser.parse_args(argv)

    codesystems = list(SPECS) if args.codesystem == "all" else [args.codesystem]
    render(staging_from_env(), codesystems, args.out.expanduser())
    return 0


if __name__ == "__main__":
    sys.exit(main())

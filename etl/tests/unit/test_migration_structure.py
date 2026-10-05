# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Guard the centralized-migrations invariant: no per-domain migrations dirs."""

from infrastructure.utils.ddl_loader import get_ddl_path


def test_no_migrations_dirs_under_tables():
    tables_dir = get_ddl_path() / "tables"
    offenders = sorted(str(p) for p in tables_dir.rglob("migrations") if p.is_dir())
    assert offenders == [], (
        "Per-domain migration directories are forbidden; migrations belong in "
        f"infrastructure/ddl/migrations/ as yyyymmdd_name.sql. Found: {offenders}"
    )

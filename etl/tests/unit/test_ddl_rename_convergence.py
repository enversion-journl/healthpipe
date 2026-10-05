# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""A fresh database and a migrated one must end up with the same object names.

The base DDL under ``ddl/tables/`` is what a from-scratch provision creates; the
``sp_rename`` calls under ``ddl/migrations/`` are what an already-deployed
database gets rewritten to. If the base DDL still creates a constraint or index
name that a migration later renames away, the two paths diverge permanently:
a fresh database keeps ``PK_arc_encounter_ifdb`` while a migrated one carries
``PK_arc_ifdb_encounter``.

That divergence is also what turns an out-of-order DDL run into a confusing
failure — the collision surfaces as "There is already an object named
'PK_dfm_state'" rather than naming the table actually at fault.

So: no name that migrations rename away may still be created by ``tables/``.
Renames are chained in filename order, because a name can be renamed more than
once (``IX_FHIR_EncounterLocations_*`` -> ``..._Encounter_Locations_*`` ->
``..._Encounter_Location_*``); only the final name is acceptable.
"""

import re

import pytest
from infrastructure.utils.ddl_loader import get_ddl_path

pytestmark = pytest.mark.unit

# sp_rename 'schema.table.OLD', 'NEW', 'INDEX'   (also 'OBJECT' for check/default
# constraints). Table and column renames are out of scope here.
_SP_RENAME = re.compile(
    r"sp_rename\s+'([^']+)'\s*,\s*'([^']+)'\s*,\s*'(INDEX|OBJECT)'",
    re.IGNORECASE,
)


def _rename_chain() -> dict[str, str]:
    """Map every renamed-away object name to the name it finally lands on."""
    migrations = sorted((get_ddl_path() / "migrations").glob("*.sql"))

    final: dict[str, str] = {}
    for path in migrations:  # filename order is apply order
        for match in _SP_RENAME.finditer(path.read_text(encoding="utf-8")):
            old = match.group(1).split(".")[-1]  # schema.table.NAME -> NAME
            final[old] = match.group(2)

    # Collapse chains: X -> Y and Y -> Z becomes X -> Z.
    for _ in range(10):
        collapsed = {old: final.get(new, new) for old, new in final.items()}
        if collapsed == final:
            break
        final = collapsed

    return {old: new for old, new in final.items() if old != new}


def _base_ddl() -> dict[str, str]:
    tables = get_ddl_path() / "tables"
    return {
        str(path.relative_to(tables)): path.read_text(encoding="utf-8")
        for path in sorted(tables.glob("**/*.sql"))
    }


def test_base_ddl_never_creates_a_name_that_migrations_rename_away():
    chain = _rename_chain()
    assert chain, "no sp_rename pairs found — the migration scan is broken"

    base = _base_ddl()
    offenders: dict[str, tuple[str, list[str]]] = {}
    for old, new in chain.items():
        pattern = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(old)}(?![A-Za-z0-9_])")
        files = [name for name, sql in base.items() if pattern.search(sql)]
        if files:
            offenders[old] = (new, files)

    detail = "\n".join(
        f"  {old} -> {new}  in {', '.join(files)}"
        for old, (new, files) in sorted(offenders.items())
    )
    assert not offenders, (
        f"{len(offenders)} pre-rename name(s) are still created by the base DDL, so a "
        f"fresh database will not converge with a migrated one:\n{detail}"
    )

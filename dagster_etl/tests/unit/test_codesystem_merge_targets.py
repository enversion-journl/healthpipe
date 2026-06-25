# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""merge_codesystem_tables targets PascalCase [codesystem].* tables."""

from DSA.CodeSystem.utils import merge_codesystem_tables


class FakeCursor:
    """Records executed SQL; returns no rows for fetchall."""

    def __init__(self):
        self.executed: list[str] = []

    def execute(self, sql, *args):
        self.executed.append(sql)

    def executemany(self, sql, rows):
        self.executed.append(sql)

    def fetchall(self):
        return []


def test_merge_targets_pascalcase_tables():
    cursor = FakeCursor()
    merge_codesystem_tables(cursor, "sks", concepts=[], properties=[], hierarchy=[])
    joined = "\n".join(cursor.executed)
    assert "[codesystem].[SKSConcept]" in joined
    assert "[codesystem].[SKSProperties]" in joined
    assert "[codesystem].[SKSHierarchy]" in joined
    # No snake_case targets remain.
    assert "[codesystem].[sks_concept]" not in joined
    assert "[codesystem].[sks_properties]" not in joined
    assert "[codesystem].[sks_hierarchy]" not in joined

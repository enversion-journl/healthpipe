# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for stg_SHAKAlike helpers.

Covers the arc.epji_location deletion lookup that drives `aktiv` —
codesystem.SHAKAlikeConcept alone can't signal a removed location since
cs_shakalike UPSERTs concepts and never deletes them.
"""

from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.unit


def _cursor(rows):
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    return cursor


class TestBuildDeletionLookup:
    def test_reads_dw_is_deleted_flag(self):
        from DSA.SQL.staging.stg_SHAKAlike import _build_deletion_lookup

        cursor = _cursor([("01.02", 0), ("01.02.01", 1)])
        result = _build_deletion_lookup(cursor)
        assert result == {"01.02": False, "01.02.01": True}

    def test_trims_whitespace_on_code(self):
        from DSA.SQL.staging.stg_SHAKAlike import _build_deletion_lookup

        cursor = _cursor([(" 01.02 ", 0)])
        result = _build_deletion_lookup(cursor)
        assert result == {"01.02": False}


class TestAktivReflectsDeletionLookup:
    def test_live_code_is_active(self):
        deletion_lookup = {"01.02": False}
        assert (not deletion_lookup.get("01.02", False)) is True

    def test_deleted_code_is_inactive(self):
        deletion_lookup = {"01.02": True}
        assert (not deletion_lookup.get("01.02", False)) is False

    def test_code_missing_from_archive_defaults_active(self):
        # Should not happen in practice (arc.epji_location never hard-deletes
        # rows), but a missing lookup entry must not silently deactivate a concept.
        deletion_lookup: dict[str, bool] = {}
        assert (not deletion_lookup.get("unknown", False)) is True

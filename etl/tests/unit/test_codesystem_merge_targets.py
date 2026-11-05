# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""merge_codesystem_tables targets PascalCase [codesystem].* tables."""

import re

import pytest
from DSA.CodeSystem.utils import merge_codesystem_tables, touch_concepts

pytestmark = pytest.mark.unit


class FakeCursor:
    """Records executed SQL; returns zero action counts and no rows."""

    def __init__(self):
        self.executed: list[str] = []

    def execute(self, sql, *args):
        self.executed.append(sql)

    def executemany(self, sql, rows):
        self.executed.append(sql)

    def nextset(self):
        return True

    def fetchone(self):
        # Counts row from _run_merge_with_action_counts: (inserted, updated, deleted)
        return (0, 0, 0)

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


class TestChangeGuard:
    """update_only_on_change must see every real change — exactly.

    The database collation is case-insensitive, and `=` ignores trailing
    spaces, so a plain comparison would skip a corrected name for good.
    """

    def _merge_sql(self, update_only_on_change: bool) -> str:
        cursor = FakeCursor()
        merge_codesystem_tables(
            cursor,
            "okosted",
            concepts=[],
            properties=[],
            hierarchy=[],
            update_only_on_change=update_only_on_change,
        )
        return "\n".join(cursor.executed)

    def _concept_guard(self) -> str:
        """The condition on the concept MERGE's WHEN MATCHED branch."""
        sql = self._merge_sql(True)
        merge = next(
            part for part in sql.split("MERGE INTO") if "[codesystem].[OkostedConcept]" in part
        )
        match = re.search(r"WHEN\s+MATCHED\s+AND\s+\((.*?)\)\s+THEN", merge, re.S)
        assert match, "the concept MERGE has no change guard"
        return " ".join(match.group(1).split())

    def test_the_guard_updates_when_the_values_differ(self):
        # NOT NULL: NULLIF yields a value exactly when the two sides differ.
        assert re.search(r"\)\)\s+IS\s+NOT\s+NULL", self._concept_guard())

    def test_the_guard_compares_with_a_binary_collation(self):
        guard = self._concept_guard()
        assert (
            "NULLIF(target.[display] COLLATE Latin1_General_BIN2, "
            "source.[display] COLLATE Latin1_General_BIN2)" in guard
        )

    def test_the_guard_sees_trailing_spaces(self):
        assert re.search(
            r"OR\s+DATALENGTH\(target\.\[display\]\)\s*<>\s*DATALENGTH\(source\.\[display\]\)",
            self._concept_guard(),
        )

    def test_without_the_flag_every_match_updates(self):
        sql = self._merge_sql(False)
        assert "BIN2" not in sql
        assert "WHEN MATCHED THEN" in sql


class TestTouchConcepts:
    def test_the_codesystems_concept_table_is_stamped(self):
        cursor = FakeCursor()
        touch_concepts(cursor, "okosted", ["A", "B"])
        assert cursor.executed == [
            "UPDATE [codesystem].[OkostedConcept] "
            "SET [dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0)) "
            "WHERE [code] IN (?, ?)"
        ]

    def test_large_lists_are_chunked(self):
        cursor = FakeCursor()
        touch_concepts(cursor, "sor", [str(i) for i in range(2500)])
        assert len(cursor.executed) == 3


class TestCsOkosted:
    """cs_okosted's merge is what makes the OS2mo file's `updated` mean "changed"."""

    def _run(self, deleted_codes):
        from unittest.mock import MagicMock, patch

        import dagster as dg
        from DSA.CodeSystem.okosted import okosted

        mssql = MagicMock()
        cursor = mssql.get_connection.return_value.__enter__.return_value.cursor.return_value
        cursor.fetchall.return_value = []
        cursor.description = []
        result = {
            f"{kind}_{action}": 0
            for kind in ("concepts", "properties", "hierarchy")
            for action in ("inserted", "updated")
        }
        result["deleted_codes"] = deleted_codes
        with (
            patch.object(okosted, "merge_codesystem_tables", return_value=result) as merge,
            patch.object(okosted, "touch_concepts") as touch,
        ):
            okosted.cs_okosted(dg.build_asset_context(), mssql)
        return merge, touch, cursor

    def test_it_merges_only_on_change_and_deletes_stale_rows(self):
        merge, _, _ = self._run([])
        kwargs = merge.call_args.kwargs
        assert kwargs["update_only_on_change"] is True
        assert kwargs["delete_stale_properties"] is True
        assert kwargs["delete_stale_hierarchy"] is True
        assert kwargs.get("delete_stale", False) is False  # concepts are kept

    def test_codes_with_deleted_rows_are_stamped(self):
        _, touch, cursor = self._run(["4711"])
        touch.assert_called_once_with(cursor, "okosted", ["4711"])

    def test_nothing_deleted_stamps_nothing(self):
        _, touch, _ = self._run([])
        touch.assert_not_called()


class TestCsLager:
    """cs_lager's merge is what makes the OS2mo Lager file's `updated` mean "changed"."""

    # One description for both reads: the builder picks its columns by name.
    _COLUMNS = [("sgh",), ("lgr",), ("navn",), ("aktiv",)]
    _SGH = [("6000", None, "Sygehus Lillebælt", True)]
    _LGR = [("6000", "0100", "Centrallager", True)]

    def _run(self, deleted_codes, sgh_rows=_SGH, lgr_rows=_LGR):
        from unittest.mock import MagicMock, patch

        import dagster as dg
        from DSA.CodeSystem.lager import lager

        mssql = MagicMock()
        cursor = mssql.get_connection.return_value.__enter__.return_value.cursor.return_value
        cursor.fetchall.side_effect = [list(sgh_rows), list(lgr_rows)]
        cursor.description = self._COLUMNS
        result = {
            f"{kind}_{action}": 0
            for kind in ("concepts", "properties", "hierarchy")
            for action in ("inserted", "updated")
        }
        result["deleted_codes"] = deleted_codes
        with (
            patch.object(lager, "merge_codesystem_tables", return_value=result) as merge,
            patch.object(lager, "touch_concepts") as touch,
        ):
            lager.cs_lager(dg.build_asset_context(), mssql)
        return merge, touch, cursor

    def test_it_merges_only_on_change_and_deletes_stale_rows(self):
        merge, _, _ = self._run([])
        kwargs = merge.call_args.kwargs
        assert kwargs["update_only_on_change"] is True
        assert kwargs["delete_stale_properties"] is True
        assert kwargs["delete_stale_hierarchy"] is True
        assert kwargs.get("delete_stale", False) is False  # concepts are kept

    def test_codes_with_deleted_rows_are_stamped(self):
        _, touch, cursor = self._run(["SGH_6000_LGR_0100"])
        touch.assert_called_once_with(cursor, "lager", ["SGH_6000_LGR_0100"])

    def test_nothing_deleted_stamps_nothing(self):
        _, touch, _ = self._run([])
        touch.assert_not_called()

    @pytest.mark.parametrize("empty", ["sgh", "lgr"])
    def test_an_empty_archive_table_is_refused(self, empty):
        """With stale deletion on, an empty read would strip every lager's properties."""
        import dagster as dg

        sgh_rows = [] if empty == "sgh" else self._SGH
        lgr_rows = [] if empty == "lgr" else self._LGR
        with pytest.raises(dg.Failure, match=f"arc.ils_{empty}"):
            self._run([], sgh_rows=sgh_rows, lgr_rows=lgr_rows)

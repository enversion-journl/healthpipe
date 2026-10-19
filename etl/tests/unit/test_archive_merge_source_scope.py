# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for ArchiveMergeConfig.source_scope_predicate ([internal ticket] groundwork).

The archive merge could always narrow what it soft-DELETES
(delete_scope_predicate); nothing narrowed what it MERGES IN. DRG's tier-2
decision needs both, derived from the same slice set — narrowing the source
while delete detection still covers a slice makes WHEN NOT MATCHED BY SOURCE
soft-delete that entire slice (the [internal ticket] failure class).
"""

import pytest
from core.archive_merge import build_archive_merge_sql
from core.dfm_config import ArchiveMergeConfig

pytestmark = pytest.mark.unit

SOURCE_SCOPE = "[takstsystem] = 'T26' AND ISNULL([AKTIVITETSAAR], '') IN ('2024')"
DELETE_SCOPE = "tgt.[takstsystem] = 'T26' AND ISNULL(tgt.[AKTIVITETSAAR], '') IN ('2024')"


def _config(**overrides) -> ArchiveMergeConfig:
    base = dict(
        source_schema="ext",
        source_table="v_drg_kontakt",
        target_schema="arc",
        target_table="drg_kontakt",
        merge_keys=("takstsystem", "KONTAKT_VAERDI"),
        non_key_columns=("AKTIVITETSAAR", "leverance_dato"),
        strategy="upsert",
        update_only_on_change=True,
        detect_deletes=True,
        upstream_is_full_load=True,
    )
    base.update(overrides)
    return ArchiveMergeConfig(**base)


class TestSourceScope:
    def test_absent_by_default_source_is_the_bare_table(self):
        sql = build_archive_merge_sql(_config(), track_actions=False)
        assert "USING [ext].[v_drg_kontakt] AS src" in sql
        assert "SELECT * FROM" not in sql

    def test_scope_wraps_the_source_in_a_filtered_derived_table(self):
        sql = build_archive_merge_sql(
            _config(source_scope_predicate=SOURCE_SCOPE), track_actions=False
        )
        assert f"USING (SELECT * FROM [ext].[v_drg_kontakt] WHERE {SOURCE_SCOPE}) AS src" in sql

    def test_source_and_delete_scope_coexist(self):
        sql = build_archive_merge_sql(
            _config(source_scope_predicate=SOURCE_SCOPE, delete_scope_predicate=DELETE_SCOPE),
            track_actions=False,
        )
        assert SOURCE_SCOPE in sql
        assert DELETE_SCOPE in sql
        assert "WHEN NOT MATCHED BY SOURCE" in sql

    def test_scope_applies_before_dedup(self):
        """Dedup must pick its winner among the rows the run may actually
        merge, not among rows a rejected slice contributed."""
        sql = build_archive_merge_sql(
            _config(source_scope_predicate=SOURCE_SCOPE, dedup_source_on_keys=True),
            track_actions=False,
        )
        assert (
            f"FROM (SELECT * FROM [ext].[v_drg_kontakt] WHERE {SOURCE_SCOPE}) AS scoped_src" in sql
        )
        # The window function reads FROM the scoped derived table, so the
        # dedup winner is chosen among rows this run may merge.
        assert sql.count(SOURCE_SCOPE) == 1

    def test_dedup_without_scope_still_aliases_its_derived_table(self):
        sql = build_archive_merge_sql(_config(dedup_source_on_keys=True), track_actions=False)
        assert "FROM [ext].[v_drg_kontakt] AS scoped_src" in sql
        assert ") AS src" in sql

    def test_tracked_actions_variant_keeps_the_scope(self):
        sql = build_archive_merge_sql(
            _config(source_scope_predicate=SOURCE_SCOPE), track_actions=True
        )
        assert SOURCE_SCOPE in sql
        assert "OUTPUT $action" in sql


class TestFastLoadAliasing:
    """The empty-target fast load shares _build_source_expr, so a derived
    table from EITHER reason must stay syntactically valid there."""

    def test_plain_source_is_aliased(self):
        from core.archive_merge import _build_empty_target_fastload_sql

        sql = _build_empty_target_fastload_sql(_config(enable_bulk_insert_on_first_load=True))
        assert "FROM [ext].[v_drg_kontakt] AS src" in sql

    def test_scoped_source_is_aliased(self):
        from core.archive_merge import _build_empty_target_fastload_sql

        sql = _build_empty_target_fastload_sql(
            _config(enable_bulk_insert_on_first_load=True, source_scope_predicate=SOURCE_SCOPE)
        )
        assert f"FROM (SELECT * FROM [ext].[v_drg_kontakt] WHERE {SOURCE_SCOPE}) AS src" in sql

    def test_deduped_source_is_aliased(self):
        from core.archive_merge import _build_empty_target_fastload_sql

        sql = _build_empty_target_fastload_sql(
            _config(enable_bulk_insert_on_first_load=True, dedup_source_on_keys=True)
        )
        assert ") AS src" in sql

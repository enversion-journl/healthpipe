# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the DRG archive's slice-level delivery decision (tier 2).

Design: docs/source_systems/drg/drg_leverancedetektion_beslutningsnotat.md.

A delivery may only overwrite an activity-year slice when it carries data that
is STRICTLY newer than what the archive already reflects. The comparison is on
the delivery's real data cap (LPROPDATERINGSDATO), not on the packaging date —
an M file is packed days after the data it carries, so packaging dates order M
against U wrongly.
"""

import re
from datetime import date, datetime

import pytest
from DSI.csv_files import drg_archive
from DSI.csv_files.drg_archive import DeliveryManifest
from DSI.csv_files.drg_datasets import DATASETS

pytestmark = pytest.mark.unit

KONTAKT = next(d for d in DATASETS if d.ext_table == "drg_kontakt")
PSYK_KONTAKT = next(d for d in DATASETS if d.ext_table == "drgpsyk_kontakt")


class FakeCursor:
    def __init__(self, responses):
        self.responses = responses
        self.executed: list[str] = []
        self._current = None

    def execute(self, sql, *params):
        self.executed.append(" ".join(sql.split()))
        self._current = None
        for fragment, rows in self.responses.items():
            if fragment in self.executed[-1]:
                self._current = rows
                return

    def fetchall(self):
        return self._current or []

    def fetchone(self):
        return (self._current or [(0,)])[0]

    def close(self):
        pass


class FakeConn:
    def __init__(self, responses):
        self._responses = responses
        self.cursors: list[FakeCursor] = []

    def cursor(self):
        cursor = FakeCursor(self._responses)
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        pass


def _manifest(takstsystem="T26", cap="2026-08-10", kadence="M", cap_kilde="reel"):
    return {
        takstsystem: DeliveryManifest(
            leverancetype="DRG",
            takstsystem=takstsystem,
            kadence=kadence,
            leverance_ts=datetime(2026, 8, 18, 11, 45, 49),
            filnavn="Region_1083_DRG_T26_M_20260818114549.tar.bz2",
            leverance_dato=date(2026, 8, 18),
            cap=cap,
            cap_kilde=cap_kilde,
        )
    }


class TestDecideSlices:
    def test_first_load_of_a_slice_is_accepted(self):
        accepted, notes = drg_archive.decide_slices({"T26": ["2024", "2025"]}, _manifest(), {})
        assert accepted == {"T26": ["2024", "2025"]}
        assert notes == []

    def test_strictly_newer_cap_is_accepted(self):
        accepted, _ = drg_archive.decide_slices(
            {"T26": ["2024"]}, _manifest(cap="2026-08-10"), {("T26", "2024"): "2026-08-03"}
        )
        assert accepted == {"T26": ["2024"]}

    def test_equal_cap_is_skipped(self):
        """A rerun against the same delivery must change nothing."""
        accepted, notes = drg_archive.decide_slices(
            {"T26": ["2024"]}, _manifest(cap="2026-08-10"), {("T26", "2024"): "2026-08-10"}
        )
        assert accepted == {}
        assert "springer over" in notes[0]

    def test_older_cap_is_skipped(self):
        accepted, notes = drg_archive.decide_slices(
            {"T26": ["2025"]}, _manifest(cap="2026-08-10"), {("T26", "2025"): "2026-08-17"}
        )
        assert accepted == {}
        assert "2026-08-10" in notes[0] and "2026-08-17" in notes[0]

    def test_the_m_after_u_case_merges_only_the_year_u_does_not_carry(self):
        """Scenario 1 from the note: U (cap 17/8) landed first, then M (cap
        10/8). M's t-2 slice is new; its t-1/t slices are behind U."""
        accepted, notes = drg_archive.decide_slices(
            {"T26": ["2024", "2025", "2026"]},
            _manifest(cap="2026-08-10"),
            {("T26", "2025"): "2026-08-17", ("T26", "2026"): "2026-08-17"},
        )
        assert accepted == {"T26": ["2024"]}
        assert len(notes) == 2

    def test_takstsystem_absent_from_the_manifest_is_never_merged(self):
        """ext may still hold an earlier run's rows; the manifest is the
        contract for what THIS run loaded."""
        accepted, notes = drg_archive.decide_slices({"T25": ["2024"]}, _manifest("T26"), {})
        assert accepted == {}
        assert "ikke i manifestet" in notes[0]

    def test_delivery_without_a_cap_is_not_subject_to_the_comparison(self):
        """Psyk ships no LEVERANCEINFO — the packaging-date guard protects it."""
        accepted, notes = drg_archive.decide_slices(
            {"T26": [""]}, _manifest(cap=None, cap_kilde=None), {("T26", ""): "2026-08-17"}
        )
        assert accepted == {"T26": [""]}
        assert notes == []


class TestSlicePredicate:
    def test_source_and_delete_scope_select_the_same_slices(self):
        """The [internal ticket] invariant: narrowing the source while delete detection
        still covers a slice soft-deletes that slice. Compared on the literals
        each predicate actually selects, not by re-deriving one from the other."""
        accepted = {"T25": ["2024"], "T26": ["2024", "2025"]}
        source = drg_archive.build_slice_predicate(KONTAKT, accepted, alias="")
        delete = drg_archive.build_slice_predicate(KONTAKT, accepted, alias="tgt")

        literals = re.compile(r"'([^']*)'")
        assert literals.findall(source) == literals.findall(delete)
        # The ISNULL(..., '') sentinel is not a slice value.
        selected = sorted({v for v in literals.findall(source) if v})
        assert selected == ["2024", "2025", "T25", "T26"]

    def test_source_scope_is_unqualified(self):
        predicate = drg_archive.build_slice_predicate(KONTAKT, {"T26": ["2024"]}, alias="")
        assert predicate == "([takstsystem] = 'T26' AND ISNULL([AKTIVITETSAAR], '') IN ('2024'))"

    def test_src_alias_for_joins_against_the_view(self):
        predicate = drg_archive.build_slice_predicate(KONTAKT, {"T26": ["2024"]}, alias="src")
        assert "src.[takstsystem]" in predicate and "src.[AKTIVITETSAAR]" in predicate

    def test_dataset_without_activity_year_scopes_on_takstsystem(self):
        predicate = drg_archive.build_slice_predicate(PSYK_KONTAKT, {"T26": [""]}, alias="tgt")
        assert predicate == "tgt.[takstsystem] IN ('T26')"

    def test_no_slices_means_no_predicate(self):
        assert drg_archive.build_slice_predicate(KONTAKT, {}, alias="tgt") is None

    def test_values_are_escaped(self):
        predicate = drg_archive.build_slice_predicate(
            KONTAKT, {"T26": ["2025' OR 1=1 --"]}, alias="tgt"
        )
        assert "'2025'' OR 1=1 --'" in predicate


class TestRecordedCaps:
    def test_reads_the_max_cap_per_slice(self):
        conn = FakeConn(
            {
                "FROM [meta].[DRGLeverance]": [
                    ("T26", "2024", "2026-08-10"),
                    ("T26", "2025", "2026-08-17"),
                ]
            }
        )
        caps = drg_archive.recorded_caps(conn, KONTAKT)
        assert caps == {("T26", "2024"): "2026-08-10", ("T26", "2025"): "2026-08-17"}

    def test_empty_year_is_normalised(self):
        conn = FakeConn({"FROM [meta].[DRGLeverance]": [("T26", None, "2026-08-10")]})
        assert drg_archive.recorded_caps(conn, PSYK_KONTAKT) == {("T26", ""): "2026-08-10"}

    def test_derived_caps_are_not_a_watermark(self):
        """An afledt cap overstates the real one (the packaging date of an M
        file), so it must never become the cap the slice comparison rejects
        the next REAL delivery against."""
        conn = FakeConn({"FROM [meta].[DRGLeverance]": []})
        drg_archive.recorded_caps(conn, KONTAKT)
        assert "[cap_kilde] = ?" in conn.cursors[0].executed[0]


class TestCaplessTakstsystemer:
    def test_no_cap_needs_the_packaging_date_guard(self):
        assert drg_archive.capless_takstsystemer(_manifest(cap=None, cap_kilde=None)) == ["T26"]

    def test_a_derived_cap_also_needs_it(self):
        """The one case where the cap is untrustworthy must not be the one
        case where BOTH guards are off: a derived cap skips neither."""
        assert drg_archive.capless_takstsystemer(_manifest(cap_kilde="afledt")) == ["T26"]

    def test_a_real_cap_is_guarded_by_the_slice_comparison_instead(self):
        assert drg_archive.capless_takstsystemer(_manifest(cap_kilde="reel")) == []


class TestReadManifest:
    def test_maps_rows_per_takstsystem(self):
        conn = FakeConn(
            {
                "FROM [ext].[drg_leverance_manifest]": [
                    (
                        "DRG",
                        "T26",
                        "M",
                        datetime(2026, 8, 18, 11, 45, 49),
                        "Region_1083_DRG_T26_M_20260818114549.tar.bz2",
                        date(2026, 8, 18),
                        "2026-08-10",
                        "reel",
                    )
                ]
            }
        )
        manifest = drg_archive.read_manifest(conn, KONTAKT)
        assert manifest["T26"].cap == "2026-08-10"
        assert manifest["T26"].cap_kilde == "reel"
        assert manifest["T26"].kadence == "M"

    def test_empty_manifest_is_an_empty_mapping(self):
        assert drg_archive.read_manifest(FakeConn({}), KONTAKT) == {}

    def test_psyk_reads_its_own_manifest_table(self):
        conn = FakeConn({})
        drg_archive.read_manifest(conn, PSYK_KONTAKT)
        assert "drgpsyk_leverance_manifest" in conn.cursors[0].executed[0]


class TestPlanMergeTouchesExtOnlyWhenThereIsSomethingToMerge:
    """A run where the extract found no new delivery leaves ext ALONE — it does
    not even recreate the tables. Reading ext before checking the manifest
    therefore fails the archive asset on a missing object instead of declaring
    the intended no-op, and every downstream step reports 'dependencies were
    not executed'."""

    def test_empty_manifest_issues_no_query_against_ext(self):
        conn = FakeConn({})  # no manifest rows, and ext does not exist
        manifest, accepted, notes = drg_archive.plan_merge(conn, KONTAKT)

        assert manifest == {} and accepted == {} and notes == []
        executed = " ".join(sql for cursor in conn.cursors for sql in cursor.executed)
        assert "[ext].[drg_kontakt]" not in executed
        assert "[meta].[DRGLeverance]" not in executed

    def test_a_manifest_row_does_lead_to_ext_being_read(self):
        conn = FakeConn(
            {
                "FROM [ext].[drg_leverance_manifest]": [
                    (
                        "DRG",
                        "T26",
                        "M",
                        datetime(2026, 8, 18, 11, 45, 49),
                        "Region_1083_DRG_T26_M_20260818114549.tar.bz2",
                        date(2026, 8, 18),
                        "2026-08-10",
                        "reel",
                    )
                ],
                "GROUP BY [takstsystem], [AKTIVITETSAAR]": [("T26", "2024")],
            }
        )
        manifest, accepted, _ = drg_archive.plan_merge(conn, KONTAKT)

        assert set(manifest) == {"T26"}
        assert accepted == {"T26": ["2024"]}
        executed = " ".join(sql for cursor in conn.cursors for sql in cursor.executed)
        assert "[ext].[drg_kontakt]" in executed

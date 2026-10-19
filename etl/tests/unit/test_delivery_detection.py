# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for file-delivery detection.

Covers the contract from drg_leverancedetektion_beslutningsnotat.md:
  - the unit lives in the DFMState key, the identity in the value
  - one identity format readable across both delivery formats (DRG tar.bz2,
    HAIBA loose CSV), where string order == chronological order
  - "new delivery" loads, "same delivery" is a no-op
  - a missing cadence is a loud error, never a silent third bucket
  - the round trip works through the EXISTING DfmManager API — no DFM
    extension is needed, which is the point
"""

from contextlib import contextmanager
from datetime import date, datetime
from unittest.mock import MagicMock

import pytest
from core.dfm_manager import DfmManager
from DSI.connectors.delivery_detection import (
    DELIVERY_SCHEMA,
    DELIVERY_TARGET_TYPE,
    MAX_IDENTITY_LENGTH,
    MAX_UNIT_LENGTH,
    delivery_identity,
    delivery_unit,
    is_new_delivery,
)
from DSI.csv_files.drg import drg_delivery_unit

pytestmark = pytest.mark.unit


class FakeCursor:
    def __init__(self, fetchone_results=None):
        self.executed = []
        self.executed_params = []
        self._fetchone_results = list(fetchone_results or [])
        self._call_idx = 0

    def execute(self, sql, params=None):
        self.executed.append(sql)
        self.executed_params.append(params)

    def fetchone(self):
        if self._call_idx < len(self._fetchone_results):
            result = self._fetchone_results[self._call_idx]
            self._call_idx += 1
            return result
        return None

    def close(self):
        pass


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def commit(self):
        pass


def _make_dfm(fetchone_results=None):
    cursor = FakeCursor(fetchone_results)
    mssql = MagicMock()

    @contextmanager
    def _get_conn():
        yield FakeConnection(cursor)

    mssql.get_connection = _get_conn
    context = MagicMock()
    context.run_id = "test-run"
    return DfmManager(mssql, context), cursor


# ===========================================================================
# Unit naming
# ===========================================================================


class TestDeliveryUnit:
    def test_drg_unit_carries_leverancetype_takstsystem_and_kadence(self):
        """The leverancetype is the PREFIX, not a repeated middle part."""
        assert drg_delivery_unit("DRG", "T26", "M") == "drg_leverance_T26_M"
        assert drg_delivery_unit("DRGPSYK", "T26", "M") == "drgpsyk_leverance_T26_M"

    def test_the_psyk_unit_is_not_a_like_prefix_of_the_somatik_one(self):
        """A reset matching 'drg[_]leverance[_]%' must not also wipe psyk's
        rows, and one matching psyk must not wipe somatik's."""
        somatik = drg_delivery_unit("DRG", "T26", "M")
        psyk = drg_delivery_unit("DRGPSYK", "T26", "M")
        assert not psyk.startswith("drg_leverance_")
        assert not somatik.startswith("drgpsyk_leverance_")

    def test_somatik_cadences_are_separate_units(self):
        """A shared checkpoint would let M push past a pending U forever."""
        assert drg_delivery_unit("DRG", "T26", "M") != drg_delivery_unit("DRG", "T26", "U")

    def test_leverancetyper_are_separate_units(self):
        """Somatik and psyk come from different files and must advance apart."""
        assert drg_delivery_unit("DRG", "T26", "M") != drg_delivery_unit("DRGPSYK", "T26", "M")

    def test_a_second_source_builds_its_unit_the_same_way(self):
        """The shared module names no source system — each one passes its own
        parts, so adding a source never edits this module."""
        assert delivery_unit("haiba", "SSI_HIP", "cases") == "haiba_leverance_SSI_HIP_cases"
        assert delivery_unit("udredningsret", "1083") == "udredningsret_leverance_1083"

    def test_sources_cannot_collide(self):
        assert delivery_unit("drg", "X").startswith("drg_")
        assert delivery_unit("haiba", "X").startswith("haiba_")

    def test_missing_kadence_is_refused(self):
        """kadence is the JOB's; a file without a cadence letter is mapped to
        the M bucket by the monthly job's file filter, not by a None key."""
        with pytest.raises(ValueError, match="kadence"):
            drg_delivery_unit("DRG", "T24", None)

    def test_unknown_kadence_is_refused(self):
        with pytest.raises(ValueError, match="kadence"):
            drg_delivery_unit("DRG", "T26", "X")

    def test_unknown_leverancetype_is_refused(self):
        with pytest.raises(ValueError, match="leverancetype"):
            drg_delivery_unit("DRGSOMATIK", "T26", "M")

    def test_malformed_takstsystem_is_refused(self):
        with pytest.raises(ValueError, match="takstsystem"):
            drg_delivery_unit("DRG", "2026", "M")

    def test_new_takstsystem_needs_no_configuration(self):
        """Rollover: T27 is just another unit — no DDL, no registration."""
        assert drg_delivery_unit("DRG", "T27", "M") == "drg_leverance_T27_M"

    def test_none_part_is_refused(self):
        with pytest.raises(ValueError, match="None part"):
            delivery_unit("drg", None)

    def test_empty_parts_are_refused(self):
        with pytest.raises(ValueError, match="at least one unit part"):
            delivery_unit("drg")

    def test_unit_fits_the_dfmstate_column(self):
        assert len(drg_delivery_unit("DRGPSYK", "T26", "M")) <= MAX_UNIT_LENGTH

    def test_overlong_unit_is_refused(self):
        with pytest.raises(ValueError, match="meta.DFMState.table_name"):
            delivery_unit("drg", "X" * MAX_UNIT_LENGTH)


# ===========================================================================
# Identity
# ===========================================================================


class TestDeliveryIdentity:
    def test_drg_identity_is_the_packaging_timestamp(self):
        assert delivery_identity(datetime(2026, 8, 18, 11, 45, 49)) == "2026-08-18T11:45:49"

    def test_haiba_identity_is_delivery_date_then_mtime(self):
        identity = delivery_identity(date(2026, 8, 15), datetime(2026, 8, 16, 7, 12, 33))
        assert identity == "2026-08-15|2026-08-16T07:12:33"

    def test_both_formats_fit_the_dfmstate_column(self):
        drg = delivery_identity(datetime(2026, 8, 18, 11, 45, 49))
        haiba = delivery_identity(date(2026, 8, 15), datetime(2026, 8, 16, 7, 12, 33))
        assert len(drg) <= MAX_IDENTITY_LENGTH
        assert len(haiba) <= MAX_IDENTITY_LENGTH

    def test_string_order_equals_chronological_order_for_drg(self):
        older = delivery_identity(datetime(2026, 8, 17, 19, 24, 38))
        newer = delivery_identity(datetime(2026, 8, 18, 11, 45, 49))
        assert older < newer

    def test_same_day_files_are_ordered_by_time(self):
        """Full timestamp, not date: two files can land on the same date."""
        first = delivery_identity(datetime(2026, 8, 18, 3, 0, 0))
        second = delivery_identity(datetime(2026, 8, 18, 23, 0, 0))
        assert first < second

    def test_haiba_orders_by_delivery_date_before_mtime(self):
        older_date = delivery_identity(date(2026, 8, 15), datetime(2026, 12, 31, 23, 59, 59))
        newer_date = delivery_identity(date(2026, 8, 16), datetime(2026, 1, 1, 0, 0, 0))
        assert older_date < newer_date

    def test_haiba_same_date_falls_back_to_mtime(self):
        """A same-day re-drop is a new delivery only because mtime moved."""
        first = delivery_identity(date(2026, 8, 15), datetime(2026, 8, 15, 7, 0, 0))
        redropped = delivery_identity(date(2026, 8, 15), datetime(2026, 8, 15, 19, 0, 0))
        assert first < redropped

    def test_datetime_keeps_its_time_component(self):
        """datetime subclasses date — rendering it as a bare date would make
        every same-day delivery compare equal."""
        assert "T" in delivery_identity(datetime(2026, 8, 18, 11, 45, 49))

    def test_non_temporal_component_is_refused(self):
        with pytest.raises(TypeError, match="date or datetime"):
            delivery_identity("2026-08-18")

    def test_empty_identity_is_refused(self):
        with pytest.raises(ValueError, match="at least one component"):
            delivery_identity()


# ===========================================================================
# The decision itself
# ===========================================================================


class TestIsNewDelivery:
    def test_first_run_loads(self):
        assert is_new_delivery(None, "2026-08-18T11:45:49") is True

    def test_same_delivery_is_a_no_op(self):
        """The whole point: a rerun against the same file reads nothing."""
        identity = "2026-08-18T11:45:49"
        assert is_new_delivery(identity, identity) is False

    def test_newer_delivery_loads(self):
        assert is_new_delivery("2026-08-17T19:24:38", "2026-08-18T11:45:49") is True

    def test_older_delivery_is_skipped(self):
        assert is_new_delivery("2026-08-18T11:45:49", "2026-08-17T19:24:38") is False

    def test_haiba_new_and_same(self):
        stored = "2026-08-15|2026-08-16T07:12:33"
        assert is_new_delivery(stored, stored) is False
        assert is_new_delivery(stored, "2026-08-15|2026-08-16T09:00:00") is True

    def test_shape_change_is_loud(self):
        """Comparing a one-component identity with a two-component one is
        meaningless; returning True would re-ingest forever."""
        with pytest.raises(ValueError, match="shape changed"):
            is_new_delivery("2026-08-18T11:45:49", "2026-08-15|2026-08-16T07:12:33")

    def test_empty_identity_is_refused(self):
        with pytest.raises(ValueError, match="non-empty"):
            is_new_delivery(None, "")


# ===========================================================================
# DfmManager API
# ===========================================================================


class TestUsesTheExistingDfmApi:
    """No DfmManager extension: the delivery checkpoint is an ordinary
    DFMState row addressed through the framework's existing get/set."""

    def test_get_reads_the_dfm_schema_row(self):
        dfm, cursor = _make_dfm(fetchone_results=[("2026-08-18T11:45:49",)])
        unit = drg_delivery_unit("DRG", "T26", "M")

        value = dfm.get_checkpoint(DELIVERY_SCHEMA, unit, target_type=DELIVERY_TARGET_TYPE)

        assert value == "2026-08-18T11:45:49"
        assert cursor.executed_params[0] == ("staging", "archive", DELIVERY_SCHEMA, unit)

    def test_get_returns_none_for_an_unseen_unit(self):
        dfm, _ = _make_dfm(fetchone_results=[None])
        assert (
            dfm.get_checkpoint(
                DELIVERY_SCHEMA,
                drg_delivery_unit("DRG", "T27", "M"),
                target_type=DELIVERY_TARGET_TYPE,
            )
            is None
        )

    def test_set_binds_the_identity_as_the_checkpoint_value(self):
        dfm, cursor = _make_dfm()
        unit = drg_delivery_unit("DRG", "T26", "M")
        identity = delivery_identity(datetime(2026, 8, 18, 11, 45, 49))

        dfm.set_checkpoint(DELIVERY_SCHEMA, unit, value=identity, target_type=DELIVERY_TARGET_TYPE)

        assert "MERGE" in cursor.executed[0]
        assert identity in cursor.executed_params[0]
        assert unit in cursor.executed_params[0]
        assert "archive" in cursor.executed_params[0]

    def test_the_row_says_what_it_gates_not_what_schema_it_sits_under(self):
        """target_type 'dfm' derived from schema 'dfm' is information-free.
        The row is only advanced once the delivery has landed in arc.*."""
        assert DELIVERY_TARGET_TYPE == "archive"

    def test_reader_and_writer_must_agree_on_target_type(self):
        """target_type is part of the DFMState primary key: a reader passing
        the default would address a different row and read the checkpoint back
        as unset, silently re-ingesting every delivery."""
        dfm, cursor = _make_dfm(fetchone_results=[("2026-08-18T11:45:49",)])
        unit = drg_delivery_unit("DRG", "T26", "M")

        dfm.get_checkpoint(DELIVERY_SCHEMA, unit)

        assert cursor.executed_params[0][1] == "dfm" != DELIVERY_TARGET_TYPE

    def test_an_unknown_target_type_is_refused(self):
        """A typo would not fail against the database — it would create a
        second row under a key nothing else ever reads."""
        dfm, _ = _make_dfm(fetchone_results=[None])
        with pytest.raises(ValueError, match="not a known target type"):
            dfm.get_checkpoint(DELIVERY_SCHEMA, "x_leverance_T26_M", target_type="arkiv")

    def test_identity_fits_the_column_the_framework_already_has(self):
        """The reason no schema change is needed — and the reason the unit
        lives in the key rather than the value."""
        identity = delivery_identity(date(2026, 8, 15), datetime(2026, 8, 16, 7, 12, 33))
        assert len(identity) <= MAX_IDENTITY_LENGTH

    def test_round_trip_new_then_same(self):
        """Load once, then no-op — the sequence the design turns on."""
        unit = delivery_unit("haiba", "SSI_HIP", "cases")
        identity = delivery_identity(date(2026, 8, 15), datetime(2026, 8, 16, 7, 12, 33))

        dfm, _ = _make_dfm(fetchone_results=[None])
        assert is_new_delivery(dfm.get_checkpoint(DELIVERY_SCHEMA, unit), identity) is True

        dfm, _ = _make_dfm(fetchone_results=[(identity,)])
        assert is_new_delivery(dfm.get_checkpoint(DELIVERY_SCHEMA, unit), identity) is False

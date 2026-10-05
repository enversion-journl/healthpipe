# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the codesystem-sourced FHIR Organization build.

Covers the transform branches of Organization_SOR_dfm: active-status
derivation from the closingDate property, name/type fallbacks, and the
delta-vs-full_load window contract of the changed-code query.
"""

from datetime import datetime

import pytest
from DSA.FHIR.Organization.Organization_SOR_dfm import (
    build_changed_organization_query,
    build_organization_from_codesystem_row,
    determine_status,
)
from utils.fhir.identifier_systems import SYSTEM_SOR, SYSTEM_SOR_ENTITY_TYPE


def _row(**overrides):
    row = {
        "code": "265731000016003",
        "display": "Afdeling X",
        "entity_type": "klinisk enhed",
        "closing_date": None,
        "parent_code": None,
    }
    row.update(overrides)
    return row


@pytest.mark.unit
class TestBuildOrganizationFromCodesystemRow:
    def test_open_entity_is_active_with_sor_identifier(self):
        org = build_organization_from_codesystem_row(_row())
        assert org.active is True
        assert org.name == "Afdeling X"
        assert org.identifier[0].system == SYSTEM_SOR
        assert org.identifier[0].value == "265731000016003"

    def test_past_closing_date_is_inactive(self):
        org = build_organization_from_codesystem_row(_row(closing_date="2020-01-01 00:00:00"))
        assert org.active is False

    def test_future_closing_date_is_active(self):
        org = build_organization_from_codesystem_row(_row(closing_date="2099-12-31 23:59:59"))
        assert org.active is True

    def test_empty_closing_date_is_active(self):
        org = build_organization_from_codesystem_row(_row(closing_date=""))
        assert org.active is True

    def test_missing_display_falls_back_to_code(self):
        org = build_organization_from_codesystem_row(_row(display=None))
        assert org.name == "Organization 265731000016003"

    def test_entity_type_mapped_to_type_coding(self):
        org = build_organization_from_codesystem_row(_row())
        assert org.type[0].coding[0].system == SYSTEM_SOR_ENTITY_TYPE
        assert org.type[0].coding[0].code == "klinisk enhed"
        assert org.type[0].text == "klinisk enhed"

    def test_missing_entity_type_leaves_type_unset(self):
        org = build_organization_from_codesystem_row(_row(entity_type=None))
        assert org.type is None


@pytest.mark.unit
class TestDetermineStatus:
    def test_date_only_boundary_uses_datetime_comparison(self):
        # A date-only closing date "today at midnight" is in the past by the
        # time any run executes — must be Closed, not active.
        today = datetime.now().strftime("%Y-%m-%d")
        assert determine_status(today, now=datetime.now()) == "Closed"

    def test_invalid_date_is_active(self):
        assert determine_status("not-a-date") == "active"


@pytest.mark.unit
class TestChangedOrganizationQueryContract:
    """Pin the one critical contract: full_load reads everything, delta is
    windowed on the codesystem change timestamps."""

    def test_full_load_has_no_window_filter(self):
        sql = build_changed_organization_query(
            checkpoint="2026-01-01 00:00:00", upper_bound="2026-07-01 00:00:00", mode="full_load"
        )
        assert "dw_timestamp_update" not in sql

    def test_delta_filters_on_checkpoint_window(self):
        sql = build_changed_organization_query(
            checkpoint="2026-01-01 00:00:00", upper_bound="2026-07-01 00:00:00", mode="delta"
        )
        assert "> '2026-01-01 00:00:00'" in sql
        assert "<= '2026-07-01 00:00:00'" in sql
        # All three codesystem tables must contribute to the change signal —
        # a property- or hierarchy-only change must still repost the org.
        assert "SORConcept" in sql
        assert "SORProperties" in sql
        assert "SORHierarchy" in sql

    def test_first_run_delta_without_checkpoint_selects_everything_up_to_bound(self):
        sql = build_changed_organization_query(
            checkpoint=None, upper_bound="2026-07-01 00:00:00", mode="delta"
        )
        assert "> '" not in sql, "first run must have no lower bound"
        assert "<= '2026-07-01 00:00:00'" in sql
        assert "None" not in sql

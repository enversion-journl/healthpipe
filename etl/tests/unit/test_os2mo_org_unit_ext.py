# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the OS2mo read-back landing (ext.os2mo_org_unit)."""

import pytest
from DSI.os2mo.org_unit import EXT_COLUMNS, flatten_org_unit
from utils.os2mo.org_units import extract_org_units_page

pytestmark = pytest.mark.unit


def _unit(user_key, parent_user_key="6101000016001"):
    return {
        "uuid": "u-1",
        "user_key": user_key,
        "name": "PSY Afsnit",
        "validity": {"from": "2015-07-30T00:00:00+02:00", "to": None},
        "parent_response": {"uuid": "u-root", "current": {"user_key": parent_user_key}},
        "unit_type_response": {"uuid": "t-1", "current": {"name": "Klinisk enhed"}},
    }


class TestUserKeyIsJoinable:
    """The phase-1 units carry ="<code>" in OS2mo, which joins to nothing."""

    def test_the_wrapper_is_stripped(self):
        row = flatten_org_unit(_unit('="1553851000016009"'))
        assert row["user_key"] == "1553851000016009"

    def test_the_raw_value_is_kept_beside_it(self):
        row = flatten_org_unit(_unit('="1553851000016009"'))
        assert row["user_key_raw"] == '="1553851000016009"'

    def test_the_parent_key_is_cleaned_too(self):
        row = flatten_org_unit(_unit("2001", parent_user_key='="6101000016001"'))
        assert row["parent_user_key"] == "6101000016001"

    def test_a_bare_code_is_unchanged(self):
        row = flatten_org_unit(_unit("2001"))
        assert row["user_key"] == row["user_key_raw"] == "2001"

    def test_every_ext_column_is_filled(self):
        assert set(flatten_org_unit(_unit("2001"))) == set(EXT_COLUMNS)


class TestExtractOrgUnitsPage:
    def test_a_live_unit_is_read_from_current(self):
        units, cursor = extract_org_units_page(
            {
                "org_units": {
                    "objects": [{"current": {"uuid": "u-1"}, "validities": []}],
                    "page_info": {"next_cursor": "c1"},
                }
            }
        )
        assert units == [{"uuid": "u-1"}]
        assert cursor == "c1"

    def test_a_closed_unit_is_read_from_its_latest_validity(self):
        units, _ = extract_org_units_page(
            {
                "org_units": {
                    "objects": [
                        {
                            "current": None,
                            "validities": [
                                {
                                    "uuid": "u-1",
                                    "validity": {"from": "2015-01-01", "to": "2019-12-31"},
                                },
                                {
                                    "uuid": "u-1",
                                    "validity": {"from": "2020-01-01", "to": "2024-06-30"},
                                },
                            ],
                        }
                    ]
                }
            }
        )
        assert units == [{"uuid": "u-1", "validity": {"from": "2020-01-01", "to": "2024-06-30"}}]

    def test_an_empty_response_reads_nothing(self):
        assert extract_org_units_page({}) == ([], None)

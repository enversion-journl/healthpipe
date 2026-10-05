# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Delta lower-bound resolution for pre_stg_Fakturalinje_mysupply.

The pre_stg mysupply chain drives its delta off the MINIMUM checkpoint across
four downstream owners (dim.Faktura, dim.Fakturalinje, fct.Faktura,
fct.Fakturalinje). A None checkpoint means that owner is missing data entirely
(first run or a deliberate reset), so it MUST force a full rebuild. The bug this
guards against: filtering None out and taking min() of the rest, which silently
ignores a reset owner and makes the delta path reprocess nothing new — leaving
stg.pre_Fakturalinje_mysupply (and everything built on it) empty.
"""

import pytest
from DSA.SQL.staging.pre_stg_Fakturalinje_mysupply import resolve_min_checkpoint

pytestmark = pytest.mark.unit


def test_any_none_forces_full_rebuild():
    # Only fct.Fakturalinje was reset; the other three are still populated.
    owner_cps = [
        "2026-01-01 00:00:00",
        "2026-01-02 00:00:00",
        "2026-01-03 00:00:00",
        None,
    ]
    assert resolve_min_checkpoint(owner_cps) is None


def test_all_none_forces_full_rebuild():
    assert resolve_min_checkpoint([None, None, None, None]) is None


def test_all_present_takes_min():
    owner_cps = [
        "2026-01-03 00:00:00",
        "2026-01-01 00:00:00",  # oldest — no owner may miss a change before this
        "2026-01-02 00:00:00",
        "2026-01-04 00:00:00",
    ]
    assert resolve_min_checkpoint(owner_cps) == "2026-01-01 00:00:00"


def test_empty_forces_full_rebuild():
    assert resolve_min_checkpoint([]) is None

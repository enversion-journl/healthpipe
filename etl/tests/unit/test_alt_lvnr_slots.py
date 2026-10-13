# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Slot-listerne for alternative leverandoervarenumre er single-sourced.

laftdstam har lvnr##1-4, laftd har lvnr##1-3 (jf. arc-DDL). Slot ##1 er den
primaere og indgaar IKKE i alternativ-listerne — den haandteres af de
eksisterende branches.
"""

import pytest
from utils.sql.leverandoervare_ilsvare_sql import (
    LAFTD_ALT_LVNR_SLOTS,
    LAFTDSTAM_ALT_LVNR_SLOTS,
)

pytestmark = pytest.mark.unit


def test_laftdstam_alt_slots():
    assert LAFTDSTAM_ALT_LVNR_SLOTS == ["lvnr##2", "lvnr##3", "lvnr##4"]


def test_laftd_alt_slots():
    assert LAFTD_ALT_LVNR_SLOTS == ["lvnr##2", "lvnr##3"]


def test_primary_slot_not_in_alt_lists():
    assert "lvnr##1" not in LAFTDSTAM_ALT_LVNR_SLOTS
    assert "lvnr##1" not in LAFTD_ALT_LVNR_SLOTS

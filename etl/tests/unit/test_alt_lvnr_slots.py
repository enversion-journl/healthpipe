# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Kontrakt for de alternative lvnr-slots i utils.sql.leverandoervare_ilsvare_sql.

laftdstam har lvnr##1-4 (jf. arc-DDL); listen deler alternativerne ##2-4
mellem hub, dim og bridge. Plausibel bug: et slot fjernes/tilfoejes i listen
uden at kildesemantikken har aendret sig (se
etl/docs/contracts/beslutningslog_vareaftaler.md).
"""

import pytest
from utils.sql.leverandoervare_ilsvare_sql import LAFTDSTAM_ALT_LVNR_SLOTS

pytestmark = pytest.mark.unit


def test_laftdstam_alt_slots():
    assert LAFTDSTAM_ALT_LVNR_SLOTS == ["lvnr##2", "lvnr##3", "lvnr##4"]

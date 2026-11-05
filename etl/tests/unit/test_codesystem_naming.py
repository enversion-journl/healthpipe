# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Error contract for the codesystem table-name helper.

``codesystem_table_name`` is a lookup in two dicts plus concatenation, so
asserting its output for a known pair only transcribes the dicts back at
themselves (rule 3). What it does decide is *how it fails*: an unknown
codesystem and an unknown kind raise different exception types, and that
asymmetry is load-bearing for callers.
"""

import pytest
from DSA.CodeSystem.utils import codesystem_table_name

pytestmark = pytest.mark.unit


def test_unknown_name_raises_keyerror():
    with pytest.raises(KeyError):
        codesystem_table_name("bogus", "concept")


def test_unknown_kind_raises_valueerror():
    with pytest.raises(ValueError):
        codesystem_table_name("sks", "bogus")

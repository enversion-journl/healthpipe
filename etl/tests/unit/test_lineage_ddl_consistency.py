# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

import pytest
from data_lineage.tools.lineage_ddl_check import run

pytestmark = pytest.mark.unit


def test_lineage_matches_ddl():
    report = run()
    assert not report.has_findings, "\n" + report.render()

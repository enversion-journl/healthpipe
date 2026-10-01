# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for SQL Server GREATEST() capability probe."""

from unittest.mock import MagicMock

import pytest
from utils.sql.greatest_int3 import detect_mssql_greatest_support

pytestmark = pytest.mark.unit


class TestDetectMssqlGreatestSupport:
    def test_returns_true_when_probe_succeeds(self) -> None:
        cursor = MagicMock()
        cursor.fetchone.return_value = (3,)
        conn = MagicMock()
        conn.cursor.return_value = cursor

        assert detect_mssql_greatest_support(conn) is True
        cursor.execute.assert_called_once_with("SELECT GREATEST(1, 2, 3) AS v")
        cursor.close.assert_called_once()

    def test_returns_false_when_probe_fails(self) -> None:
        cursor = MagicMock()
        cursor.execute.side_effect = Exception(
            "GREATEST is not a recognized built-in function name"
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        assert detect_mssql_greatest_support(conn) is False

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Per-chunk log throttle: first chunk, periodic heartbeat, or slow chunk."""

import pytest
from DSI.connectors.mssql.extractor import _should_log_chunk


@pytest.mark.unit
def test_first_chunk_always_logs():
    assert _should_log_chunk(1, 0.1, 0.1, interval=100, slow_seconds=5.0) is True


@pytest.mark.unit
def test_periodic_heartbeat():
    assert _should_log_chunk(100, 0.1, 0.1, interval=100, slow_seconds=5.0) is True
    assert _should_log_chunk(50, 0.1, 0.1, interval=100, slow_seconds=5.0) is False


@pytest.mark.unit
def test_slow_fetch_or_insert_always_logs():
    assert _should_log_chunk(50, 6.0, 0.1, interval=100, slow_seconds=5.0) is True
    assert _should_log_chunk(50, 0.1, 7.0, interval=100, slow_seconds=5.0) is True


@pytest.mark.unit
def test_normal_fast_chunk_is_suppressed():
    assert _should_log_chunk(1242, 2.2, 0.5, interval=100, slow_seconds=5.0) is False


@pytest.mark.unit
def test_interval_one_logs_every_chunk():
    # escape hatch: EXT_LOG_CHUNK_INTERVAL=1 => every chunk logs
    assert _should_log_chunk(7, 0.1, 0.1, interval=1, slow_seconds=5.0) is True

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""fcntl counting semaphore: caps concurrent holders, releases on exit."""

import pytest
from core.parallel.source_semaphore import SourceConnectionSemaphore


@pytest.mark.unit
def test_acquire_release_round_trips(tmp_path):
    sem = SourceConnectionSemaphore("mysupply", capacity=2, base_dir=str(tmp_path))
    with sem.slot(timeout=2.0):
        with sem.slot(timeout=2.0):
            pass  # 2 held — at capacity
    # all released; can acquire again
    with sem.slot(timeout=2.0):
        pass


@pytest.mark.unit
def test_blocks_when_full_then_times_out(tmp_path):
    sem = SourceConnectionSemaphore("mysupply", capacity=1, base_dir=str(tmp_path))
    with sem.slot(timeout=2.0):
        with pytest.raises(TimeoutError):
            with sem.slot(timeout=0.3):
                pass  # second slot can't be had within the timeout


@pytest.mark.unit
def test_capacity_clamped_to_at_least_one(tmp_path):
    sem = SourceConnectionSemaphore("x", capacity=0, base_dir=str(tmp_path))
    assert sem.capacity == 1


@pytest.mark.unit
def test_slot_released_after_exception(tmp_path):
    sem = SourceConnectionSemaphore("y", capacity=1, base_dir=str(tmp_path))
    with pytest.raises(ValueError):
        with sem.slot(timeout=2.0):
            raise ValueError("boom")
    # slot must be free again despite the exception
    with sem.slot(timeout=2.0):
        pass

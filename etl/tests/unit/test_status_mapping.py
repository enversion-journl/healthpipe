# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from datetime import UTC, datetime, timedelta

import pytest
from DSA.FHIR.utils.status_mapping import (
    map_encounter_status,
    map_episode_of_care_status,
)

pytestmark = pytest.mark.unit


def test_map_episode_of_care_status_deleted_overrides():
    assert map_episode_of_care_status("active", is_deleted=True) == "entered-in-error"


def test_map_episode_of_care_status_from_source():
    assert map_episode_of_care_status("planned") == "planned"
    assert map_episode_of_care_status("FINISHED") == "finished"
    # Unknown source falls back to period-based logic


def test_map_episode_of_care_status_from_periods():
    now = datetime.now(UTC)
    future = now + timedelta(days=1)
    past = now - timedelta(days=1)

    assert map_episode_of_care_status(None, period_start=future) == "planned"
    assert map_episode_of_care_status(None, period_start=past, period_end=None) == "active"
    assert map_episode_of_care_status(None, period_start=past, period_end=past) == "finished"


def test_map_encounter_status():
    assert map_encounter_status("in-progress") == "in-progress"
    assert map_encounter_status("unknown-value") == "unknown"
    assert map_encounter_status(None) == "unknown"
    assert map_encounter_status("in-progress", is_deleted=True) == "entered-in-error"

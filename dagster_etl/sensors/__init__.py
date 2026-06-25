# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Sensors Module

All Dagster sensor definitions are consolidated here for discoverability and maintainability.

Sensors:
- dfm_base_job_sensor: Triggers downstream jobs on DFM base job success, and
  alarms (optionally best-effort triggers daily) on DFM base job failure
  (both built via factories in definitions.py to receive postfixed job objects)
"""

from sensors.dfm_base_job_sensor import (
    build_dfm_base_job_failure_sensor,
    build_dfm_base_job_success_sensor,
)

# Statically-defined sensors (the DFM sensors are built dynamically in
# definitions.py once jobs are postfixed). Currently none.
all_sensors = []

__all__ = [
    "build_dfm_base_job_success_sensor",
    "build_dfm_base_job_failure_sensor",
    "all_sensors",
]

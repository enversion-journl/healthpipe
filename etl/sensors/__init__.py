# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Sensors Module

All Dagster sensor definitions are consolidated here for discoverability and maintainability.

Sensors:
- nightly_failure_sensor: Reports failed scheduled/sensor-triggered runs as
  Azure DevOps work items for the 07:00 triage agent (monitors all runs
  in the code location, so it needs no postfixed job references)
- haiba_publish_sensor: Publishes the HAIBA dm layer to DSO after the nightly
  Borger chain has minted dw_ek_Borger for stub patients, gated on the
  archive's delivery watermark (built via a factory in definitions.py)

The DFM base-job sensors are gone: the IFDB → FHIR pipeline is one job
(``ifdb_dfm_daily_job``) on one schedule, so there is no downstream job to fan
out to and no partition key to hand it. A failure of that job is reported by
``nightly_failure_sensor`` like any other scheduled run.
"""

from sensors.haiba_publish_sensor import build_haiba_publish_sensor
from sensors.nightly_failure_sensor import build_nightly_failure_sensor

nightly_failure_report_sensor = build_nightly_failure_sensor()

# Statically-defined sensors (the HAIBA sensor is built dynamically in
# definitions.py once jobs are postfixed).
all_sensors = [nightly_failure_report_sensor]

__all__ = [
    "build_haiba_publish_sensor",
    "build_nightly_failure_sensor",
    "nightly_failure_report_sensor",
    "all_sensors",
]

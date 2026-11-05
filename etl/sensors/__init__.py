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
- drg_queue_sensor: Walks the DRG ingest queue, requesting the next
  (takstsystem, kadence) unit when the previous run succeeds; its FAILURE twin
  alarms with the units the failure leaves unprocessed (both built via
  factories in definitions.py)
- haiba_publish_sensor: Publishes the HAIBA dm layer to DSO after the nightly
  Borger chain has minted dw_ek_Borger for stub patients, gated on the
  archive's delivery watermark (built via a factory in definitions.py)
- os2mo_delivery_sensor: Delivers the codesystems to OS2mo after
  organisation_cs_job has rebuilt them (built via a factory in definitions.py)
- cpr_backlog_sensor: After a successful CPR ingest, requests one more ingest
  run while deliveries are still waiting on the share, so a backlog is caught
  up one delivery at a time in production-date order (factory in definitions.py)
- drg_publish_sensor: Rebuilds the DRG DSA layer and syncs it to DSO after the
  Borger chain; ungated and stateless because the publish is delta and the
  nightly run re-resolves dw_ek_Borger = -1 (built via a factory in
  definitions.py)
- udredningsret_publish_sensor: Same shape for udredningsret — publishes the
  DSA layer and syncs it to DSO after the Borger chain, gated on
  meta.UdredningsretLeverance's delivery watermark and on live forloeb still
  at dw_ek_Borger = -1 (built via a factory in definitions.py)
- release_infrastructure_sensor: Runs full_infrastructure_setup once per release-*/main-*
  deployment; every schedule there is gated on its success, and its FAILURE twin
  alarms (built via factories in definitions.py)
- ifdb_fhir_fuldload_sensor: Starts the IFDB → FHIR full load when
  ifdb_dfm_daily_job succeeds; stopped by default and switched on per
  environment (built via a factory in definitions.py)

The DFM base-job sensors are gone: the IFDB → FHIR pipeline is one job
(``ifdb_dfm_daily_job``) on one schedule, so there is no downstream job to fan
out to and no partition key to hand it. A failure of that job is reported by
``nightly_failure_sensor`` like any other scheduled run.
"""

from sensors.nightly_failure_sensor import build_nightly_failure_sensor

nightly_failure_report_sensor = build_nightly_failure_sensor()

# Statically-defined sensors (the HAIBA, DRG-publish and udredningsret sensors
# are built dynamically in definitions.py once jobs are postfixed).
all_sensors = [nightly_failure_report_sensor]

__all__ = ["nightly_failure_report_sensor", "all_sensors"]

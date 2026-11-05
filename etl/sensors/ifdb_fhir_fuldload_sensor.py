# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Sensor that starts the IFDB → FHIR full load as soon as the morning chain succeeds.

The full load runs until ``koer_indtil`` (05:30), so starting it right after
``ifdb_dfm_daily_job`` gives it the day and the night. A failed morning chain starts nothing
that day. The sensor is stopped by default and switched on per environment; it and
``ifdb_fhir_fuldload_schedule`` both skip while a full load is active.
"""

import dagster as dg
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)
from DSA.SQL.dfm.ifdb_fhir_fuldload import active_fuldload_run
from schedules.ifdb_fhir_fuldload_schedules import FULDLOAD_RUN_CONFIG

__all__ = ["SENSOR_NAME", "build_ifdb_fhir_fuldload_sensor", "evaluate"]

SENSOR_NAME = "ifdb_fhir_fuldload_after_daily_sensor"


def evaluate(context, *, fuldload_job_name: str) -> dg.RunRequest | dg.SkipReason:
    """Core sensor logic, kept plain so it unit-tests without the wrapper."""
    active = active_fuldload_run(context.instance, fuldload_job_name)
    if active is not None:
        return dg.SkipReason(f"{SENSOR_NAME}: full load run {active} is still active.")

    parent_run_id = context.dagster_run.run_id
    return dg.RunRequest(
        job_name=fuldload_job_name,
        run_key=f"ifdb_fhir_fuldload:{parent_run_id}",
        run_config=FULDLOAD_RUN_CONFIG,
        tags={"dagster/triggered_by": SENSOR_NAME, "dagster/parent_run_id": parent_run_id},
    )


def build_ifdb_fhir_fuldload_sensor(
    daily_job: dg.JobDefinition | UnresolvedAssetJobDefinition,
    fuldload_job: dg.JobDefinition | UnresolvedAssetJobDefinition,
) -> dg.SensorDefinition:
    """Build the sensor with the (possibly postfixed) daily and full load jobs."""

    @dg.run_status_sensor(
        run_status=dg.DagsterRunStatus.SUCCESS,
        monitored_jobs=[daily_job],
        request_jobs=[fuldload_job],
        name=SENSOR_NAME,
        # Which environment runs years of history, and when, is a human decision, not
        # something that follows a merge.
        default_status=dg.DefaultSensorStatus.STOPPED,
        description=(
            f"On {daily_job.name} SUCCESS: start {fuldload_job.name}, which runs windows "
            "until 05:30. Skips while a full load is active. Stopped by default; keep the "
            "nightly full load schedule off where this is on."
        ),
    )
    def ifdb_fhir_fuldload_after_daily_sensor(context: dg.RunStatusSensorContext):
        return evaluate(context, fuldload_job_name=fuldload_job.name)

    return ifdb_fhir_fuldload_after_daily_sensor

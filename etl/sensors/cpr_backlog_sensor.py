# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Sensor that orders one more run after a successful cpr_ingest_job when a delivery is waiting.

run_key is the file name; the state is read from meta.CPRLeverance and the share. The sensor
starts stopped in all environments and is switched on manually.
"""

import os

import dagster as dg
from core.resources import FileShareResource, MSSQLResource, mssql_python
from DSI.csv_files.cpr import naeste_leverance, sidst_indlaeste_leverance
from jobs.cpr_jobs import ingest_run_active

SENSOR_NAME = "cpr_backlog_sensor"

# A lost share (OSError, and ValueError from FileShareResource.root) or a lost database does not
# fail the tick.
TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (OSError, ValueError) + (
    (mssql_python.Error,) if mssql_python is not None else ()
)


def evaluate(context, *, job_name: str, mssql: MSSQLResource, share: FileShareResource):
    """The core of the sensor, kept pure so it can be unit tested without the decorator."""
    if ingest_run_active(context.instance, job_name):
        return dg.SkipReason(
            f"{SENSOR_NAME}: a {job_name} run is still active; the next success re-evaluates."
        )
    try:
        sidst, _ = sidst_indlaeste_leverance(mssql)
        naeste = naeste_leverance(share.root(), sidst)
    except TRANSIENT_ERRORS as exc:
        return dg.SkipReason(
            f"{SENSOR_NAME}: the backlog could not be determined ({type(exc).__name__}); "
            "retrying after the next successful run."
        )
    if naeste is None:
        return dg.SkipReason(f"{SENSOR_NAME}: no backlog — the archive is at {sidst or 'empty'}.")
    navn = os.path.basename(naeste)
    context.log.info(
        f"{SENSOR_NAME}: the delivery {navn} is waiting — ordering one run of {job_name}."
    )
    return dg.RunRequest(
        job_name=job_name,
        run_key=f"cpr_backlog:{navn}",
        tags={"dagster/triggered_by": SENSOR_NAME},
    )


def build_cpr_backlog_sensor(ingest_job):
    """The sensor for a (possibly postfixed) cpr_ingest_job."""

    @dg.run_status_sensor(
        name=SENSOR_NAME,
        run_status=dg.DagsterRunStatus.SUCCESS,
        monitored_jobs=[ingest_job],
        request_jobs=[ingest_job],
        # Manual start per environment: a backlog is not caught up before the environment is ready.
        default_status=dg.DefaultSensorStatus.STOPPED,
        description=(
            "After a successful CPR load: if one more delivery is on the share, one more "
            "run is ordered, so a backlog is caught up one day at a time in production "
            "date order. Starts stopped; switched on manually when the environment is ready."
        ),
    )
    def _sensor(
        context: dg.RunStatusSensorContext,
        mssql_target_staging: MSSQLResource,
        cpr_share: FileShareResource,
    ):
        return evaluate(
            context, job_name=ingest_job.name, mssql=mssql_target_staging, share=cpr_share
        )

    return _sensor


__all__ = ["SENSOR_NAME", "build_cpr_backlog_sensor", "evaluate"]

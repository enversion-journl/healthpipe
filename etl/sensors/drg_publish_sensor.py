# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Sensor tying the DRG publish job to the nightly Borger chain.

``drg_publish_job`` must run AFTER ``ifdb_dfm_daily_job``: the DRG ingest queue
posts stub Patients to Aidbox for unknown CPRs, and those only become a
dw_ek_Borger once that chain has minted them. The cross-job asset edge
(pre_stg_DRGKontakt -> dim_Borger) is not enforced by Dagster at runtime, so a
cron would only move the race — hence a run-status sensor on SUCCESS.

Ungated, unlike ``haiba_publish_sensor``: HAIBA's publish is a full multi-hour
rebuild worth skipping on empty nights, while the DRG publish is delta all the
way down, so a night without a delivery is a cheap no-op. Firing every night is
the point — the nightly run is what re-resolves contacts left at
dw_ek_Borger = -1 (``build_unresolved_borger_scope_sql``), and the fct MERGE
guard plus the 14-day window keep that retry set bounded. The sensor reads no
database and holds no state.
"""

import dagster as dg
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)
from schedules.schedule_default_status import get_sensor_default_status

SENSOR_NAME = "drg_publish_sensor"


def build_run_request(publish_job_name: str, parent_run_id: str) -> dg.RunRequest:
    """run_key is per triggering run: a retried tick dedupes, a later night
    publishes again (which is what makes the -1 re-resolution recur)."""
    return dg.RunRequest(
        job_name=publish_job_name,
        run_key=f"drg_publish:{parent_run_id}",
        tags={
            "dagster/triggered_by": SENSOR_NAME,
            "dagster/parent_run_id": parent_run_id,
        },
    )


def build_drg_publish_sensor(
    borger_chain_job: dg.JobDefinition | UnresolvedAssetJobDefinition,
    publish_job: dg.JobDefinition | UnresolvedAssetJobDefinition,
) -> dg.SensorDefinition:
    """Build the sensor with concrete job references. Must be called after
    branch-postfixing so it monitors and requests the correctly-named jobs."""

    @dg.run_status_sensor(
        run_status=dg.DagsterRunStatus.SUCCESS,
        monitored_jobs=[borger_chain_job],
        request_jobs=[publish_job],
        name=SENSOR_NAME,
        default_status=get_sensor_default_status(),
        description=(
            f"On {borger_chain_job.name} SUCCESS: rebuild the DRG DSA layer and "
            f"sync it to DSO, after the chain has minted dw_ek_Borger for the "
            f"ingest queue's stub patients. Ungated: the publish is delta, so a "
            f"night without a delivery is a no-op."
        ),
    )
    def drg_publish_sensor(context: dg.RunStatusSensorContext):
        parent_run_id = context.dagster_run.run_id
        context.log.info(
            f"{SENSOR_NAME}: {context.dagster_run.job_name} succeeded — "
            f"requesting {publish_job.name}."
        )
        return dg.SensorResult(run_requests=[build_run_request(publish_job.name, parent_run_id)])

    return drg_publish_sensor


__all__ = [
    "SENSOR_NAME",
    "build_drg_publish_sensor",
    "build_run_request",
]

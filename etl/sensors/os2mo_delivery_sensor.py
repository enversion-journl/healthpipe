# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Sensor that starts the OS2mo delivery after the codesystems are rebuilt.

``organisation_cs_job`` rebuilds ``codesystem.SOR*``, ``codesystem.Okosted*``
and ``conceptmap.OkostedToSOR``. The delivery runs on that job's success, so it
never reads a codesystem mid-rebuild, and a night whose rebuild failed delivers
nothing rather than yesterday's set.

The sensor skips when ``OS2MO_ENABLED`` is false: a deployment without an OS2mo
target has nothing to deliver to, and a run there would only fail.
"""

import dagster as dg
from core.parallel import env_bool
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)
from DSO.export.os2mo.codesystem_assets import upload_asset_name
from DSO.export.os2mo.codesystems import CODESYSTEM_EXPORTS
from schedules.schedule_default_status import get_sensor_default_status

__all__ = ["SENSOR_NAME", "build_os2mo_delivery_sensor", "delivery_run_config", "evaluate"]

SENSOR_NAME = "os2mo_delivery_sensor"


def delivery_run_config() -> dict:
    """Every upload with ``dry_run: false`` — the uploads default to a dry run.

    Keyed on the bare op names: the branch postfix renames asset keys, not ops.
    """
    return {
        "ops": {
            upload_asset_name(spec): {"config": {"dry_run": False}} for spec in CODESYSTEM_EXPORTS
        }
    }


def evaluate(context, *, delivery_job_name: str) -> dg.RunRequest | dg.SkipReason:
    """Core sensor logic, kept plain so it unit-tests without the wrapper."""
    if not env_bool("OS2MO_ENABLED", False):
        return dg.SkipReason(f"{SENSOR_NAME}: OS2MO_ENABLED is false in this deployment.")

    parent_run_id = context.dagster_run.run_id
    return dg.RunRequest(
        job_name=delivery_job_name,
        run_key=f"os2mo_delivery:{parent_run_id}",
        run_config=delivery_run_config(),
        tags={"dagster/triggered_by": SENSOR_NAME, "dagster/parent_run_id": parent_run_id},
    )


def build_os2mo_delivery_sensor(
    codesystem_job: dg.JobDefinition | UnresolvedAssetJobDefinition,
    delivery_job: dg.JobDefinition | UnresolvedAssetJobDefinition,
) -> dg.SensorDefinition:
    """Build the sensor with concrete job references.

    Must be called **after** branch-postfixing so the sensor monitors and
    requests the correctly-named jobs.
    """

    @dg.run_status_sensor(
        run_status=dg.DagsterRunStatus.SUCCESS,
        monitored_jobs=[codesystem_job],
        request_jobs=[delivery_job],
        name=SENSOR_NAME,
        default_status=get_sensor_default_status(),
        description=(
            f"On {codesystem_job.name} SUCCESS: deliver the codesystems to OS2mo "
            f"({delivery_job.name}). Skips when OS2MO_ENABLED is false."
        ),
    )
    def os2mo_delivery_sensor(context: dg.RunStatusSensorContext):
        return evaluate(context, delivery_job_name=delivery_job.name)

    return os2mo_delivery_sensor

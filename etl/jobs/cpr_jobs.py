# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Job for the CPR register's deliveries.

``cpr_ingest_job`` loads one delivery to ext, archives it, builds the union of
identities and then the Patient resources the delivery has changed. The base extract is run
manually with ``fil`` set in the run configuration.
"""

import dagster as dg

cpr_ingest_job = dg.define_asset_job(
    name="cpr_ingest_job",
    # ref_patient_identity runs between the archiving and the register's producer.
    selection=dg.AssetSelection.assets(
        "ext_cpr", "arc_cpr", "ref_patient_identity", "fhir_server_patient_cpr_dfm"
    ),
    description=(
        "Load the oldest unregistered CPR delivery from the DOL share to "
        "ext.cpr_*, merge it to arc.cpr_*, rebuild the union of identities, and "
        "rebuild the Patient resources the delivery has changed. One delivery per "
        "run, so the order holds."
    ),
)

_ACTIVE_STATUSES = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.CANCELING,
]


def ingest_run_active(instance: dg.DagsterInstance, job_name: str) -> bool:
    """True while a run of the (possibly postfixed) cpr_ingest_job is queued or running."""
    return bool(
        instance.get_run_records(
            filters=dg.RunsFilter(job_name=job_name, statuses=_ACTIVE_STATUSES), limit=1
        )
    )


__all__ = ["cpr_ingest_job", "ingest_run_active"]

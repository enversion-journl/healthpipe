# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SilkeborgData job — delivery share -> arc.* -> FHIR server -> fhir.* -> DSO.

The whole chain in ONE job, deliberately. Every layer is selected here, so
Dagster orders them by the asset graph and each link runs only after its
predecessor succeeded. Splitting the chain across jobs — the state this replaced,
where SOF and DSO rode along with the IFDB jobs — has no such guarantee: those
jobs know nothing about whether the SD archive and FHIR layers finished, so the
sync could publish a half-updated set of flat tables, or skip a delivery
entirely, and look green doing it.

Selected by asset group, so a new asset joins the chain by carrying the right
group and needs no change here:

* ``silkeborgdata_archive``    — delivery share -> ext -> arc
* ``fhir_silkeborgdata_dfm``   — arc -> FHIR resources
* ``fhir_silkeborgdata_sof``   — resources -> fhir.* flat tables
* ``fhir_silkeborgdata_dso``   — fhir.* -> the reporting database

Infrastructure is deliberately NOT selected — not staging_infra_fhir, not
dso_infra_fhir, not fhir_sof_infrastructure. None of the three is scoped to
SilkeborgData: the two DDL assets apply the whole tables/fhir directory, i.e. the
schema for every fhir.* table, and fhir_sof_infrastructure globs every
ViewDefinition JSON and re-materializes any whose definition differs from the
server's. Selecting them would make this job the only SCHEDULED thing in the repo
that applies them, so an unrelated migration or a changed IFDB ViewDefinition
would first land in production at 10:00 inside the SilkeborgData run — and fail
it, for a reason that has nothing to do with SilkeborgData. Infrastructure is a
deploy step and belongs in the setup jobs, which is where the DFM path keeps it.

Those assets are declared deps further down the chain, so Dagster treats them as
external and assumes they are current. That is the same posture as the IFDB DFM
jobs. After a deploy that changes DDL or a ViewDefinition, materialize them
manually before the next run.

A dedicated job rather than hanging the assets on erp_daily_dfm_job: SD is
payroll and personnel data with no downstream relation to the ERP dimensional
model, and an unreachable share would otherwise fail the whole daily ERP job.
All four existing archive domains keep their own job for the same reason.
"""

import dagster as dg

silkeborgdata_daily_job = dg.define_asset_job(
    name="silkeborgdata_daily_job",
    selection=dg.AssetSelection.groups(
        "silkeborgdata_archive",
        "fhir_silkeborgdata_dfm",
        "fhir_silkeborgdata_sof",
        "fhir_silkeborgdata_dso",
    ),
    description=(
        "SilkeborgData end to end: read new deliveries from the share, archive "
        "them in arc.silkeborgdata_*, post Practitioner / PractitionerRole to the "
        "FHIR server, extract the flat fhir.* tables and sync them to the "
        "reporting database. Delta is driven by DFM checkpoints at every layer. "
        "Runs daily; a day without a new delivery folder is a green no-op."
    ),
    tags={"domain": "silkeborgdata", "layer": "end_to_end"},
)

__all__ = ["silkeborgdata_daily_job"]

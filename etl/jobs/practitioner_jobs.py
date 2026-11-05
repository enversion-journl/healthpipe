# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Practitioner job — SilkeborgData and EPJI -> arc.* -> FHIR server -> fhir.* -> DSO.

Both Practitioner sources in ONE job, deliberately. Every layer is selected here,
so Dagster orders them by the asset graph and each link runs only after its
predecessor succeeded. Splitting the chain across jobs has no such guarantee: a
job that selects only the SOF and DSO layers knows nothing about whether the
archive and FHIR layers finished, so the sync could publish a half-updated set of
flat tables, or skip a delivery entirely, and look green doing it.

The two sources share sof_practitioner_dfm and dso_sync_fhir_practitioner_dfm,
which are source-agnostic and checkpoint-driven. One job runs them once a day,
after both producers. Two jobs would run them twice, and nothing in the deployment
stops two runs of the same asset from overlapping. The cost: a failure in either
source skips sof_practitioner_dfm and its DSO sync for that day; the other
source's resources are already on the FHIR server and the next run's checkpoint
picks them up.

Every layer is selected by asset group, so a new asset joins the chain by carrying
the right group and needs no change here:

* ``silkeborgdata_archive``     — delivery share -> ext -> arc
* ``fhir_silkeborgdata_dfm``    — arc -> FHIR resources
* ``epji_practitioner_archive`` — EPJI PRACTITIONER tables -> ext -> arc
* ``fhir_epji_dfm``             — arc -> FHIR resources
* ``fhir_practitioner_sof``     — resources -> fhir.* flat tables (both sources)
* ``fhir_practitioner_dso``     — fhir.* -> the reporting database (both sources)

EPJI LOCATION stays in epji_archive, which organisation_cs_job runs.

Infrastructure is deliberately NOT selected — not staging_infra_fhir, not
dso_infra_fhir, not fhir_sof_infrastructure. None of the three is scoped to
Practitioner: the two DDL assets apply the whole tables/fhir directory, i.e. the
schema for every fhir.* table, and fhir_sof_infrastructure globs every
ViewDefinition JSON and re-materializes any whose definition differs from the
server's. Selecting them would make this job the only SCHEDULED thing in the repo
that applies them, so an unrelated migration or a changed IFDB ViewDefinition
would first land in production at 10:00 inside the Practitioner run — and fail
it, for a reason that has nothing to do with Practitioner. Infrastructure is a
deploy step and belongs in the setup jobs, which is where the DFM path keeps it.

Those assets are declared deps further down the chain, so Dagster treats them as
external and assumes they are current. That is the same posture as the IFDB DFM
jobs. After a deploy that changes DDL or a ViewDefinition, materialize them
manually before the next run; the EPJI chain also needs staging_infra_source_epji
and fhir_ifdb_database_indexes.

A dedicated job rather than hanging the assets on erp_daily_dfm_job or the IFDB
jobs: Practitioner has no downstream relation to the ERP dimensional model, IFDB's
references are not resolved to Practitioner yet, and an unreachable share would
otherwise fail an unrelated daily job.
"""

import dagster as dg

practitioner_daily_job = dg.define_asset_job(
    name="practitioner_daily_job",
    selection=dg.AssetSelection.groups(
        "silkeborgdata_archive",
        "fhir_silkeborgdata_dfm",
        "epji_practitioner_archive",
        "fhir_epji_dfm",
        "fhir_practitioner_sof",
        "fhir_practitioner_dso",
    ),
    description=(
        "Practitioner end to end from both sources: read new SilkeborgData "
        "deliveries from the share and take a full snapshot of EPJI's PRACTITIONER "
        "tables, archive both in arc.*, post Practitioner / PractitionerRole to the "
        "FHIR server, extract the flat fhir.* tables and sync them to the reporting "
        "database. Delta is driven by DFM checkpoints at every layer. Runs daily; a "
        "day without a new SilkeborgData delivery folder is a no-op for that source."
    ),
    tags={"domain": "practitioner", "layer": "end_to_end"},
)

__all__ = ["practitioner_daily_job"]

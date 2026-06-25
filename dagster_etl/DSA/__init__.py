# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSA - Data Staging Area Layer

Contains transformation assets for FHIR and SQL-based pipelines.
This is where business logic and data transformations are applied.

Structure:
- SQL/staging/    -- pre_stg, stg_dim, stg_fct (all domains consolidated)
- SQL/dim/        -- DimUpd_* dimension update assets
- SQL/map/        -- MapUpd_* map update assets
- SQL/fct/        -- FctUpd_* fact update assets
- FHIR/{Resource}/ -- PascalCase resource dirs (Patient/, Encounter/, …)
- FHIR/SqlOnFhir/ -- SQL on FHIR view extraction + load
- FHIR/utils/     -- shared utilities (ifdb/, sql_on_fhir/)
"""

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dagster job definitions for the OS2mo (Magenta) integration.

One job runs both directions: build and upload a CSV file per delivered
codesystem, and read OS2mo's organisation back into the warehouse. The two are
independent — Magenta's import picks the files up on its own schedule — so the
read-back captures OS2mo as it stands, not the effect of tonight's files.

Started by ``os2mo_delivery_sensor`` when ``organisation_cs_job`` succeeds.
Kept out of that job so an unreachable Magenta cannot stop it.
"""

import dagster as dg

os2mo_codesystem_export_job = dg.define_asset_job(
    name="os2mo_codesystem_export_job",
    selection=dg.AssetSelection.groups("os2mo_export", "os2mo_import"),
    description=(
        "Daily full load of the delivered codesystems (SOR, Okosted, ...) to "
        "Magenta as CSV files uploaded through OS2mo's GraphQL upload_file, "
        "and a read-back of OS2mo's organisation into ext/arc."
    ),
    tags={"domain": "codesystem", "layer": "export", "source": "os2mo"},
)

__all__ = [
    "os2mo_codesystem_export_job",
]

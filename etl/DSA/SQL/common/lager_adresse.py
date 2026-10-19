# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The warehouse address columns — populated outside the CodeSystem.

Warehouse attributes reach dim.Lager along two paths. The default is the
CodeSystem: ``cs_lager`` writes the value as a property on the warehouse concept
and ``stg_dim_Lager`` pivots it into a column. Address fields are the exception:
``stg_dim_Lager`` joins them in from ``arc.ils_lgr`` after the bulk insert.

The exception is phrased on the *field* ("address details never enter the
property tables"), not per table, so it never has to be re-decided. On
dim.Rekvirent it is security-critical — ``codesystem.RekvirentProperties`` syncs
to DSO unmasked, and a requisitioner's address may be a home patient's. On
dim.Lager the address is organisational, and following the rule anyway costs
this module, an extra asset dependency on ``arc_ils_lgr`` and an extra UPDATE in
staging. That price is accepted deliberately: one uniform rule beats a cheaper
per-table judgement call.

Provenance is documented in ``data_lineage/dim/dim_lager.yml``.
"""

# (dim column, source column in arc.ils_lgr, SQL type in stg.dim_Lager)
LAGER_ADRESSE_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("adresse", "adr", "NVARCHAR(200)"),
    ("land", "land", "NVARCHAR(200)"),
    ("postnummer", "postdi", "NVARCHAR(200)"),
    ("telefon", "tlf", "NVARCHAR(200)"),
)

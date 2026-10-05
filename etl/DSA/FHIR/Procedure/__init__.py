# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

# Procedure-pipelinen fra IFDB AKTIVITETSRESULTAT produceres i DSA/FHIR/Aktivitetsresultat/.
from .Procedure_IFDB_AdminProc_dfm import fhir_server_procedure_admin_proc_dfm
from .Procedure_IFDB_RisYdelse_dfm import fhir_server_procedure_ris_ydelse_dfm

all_assets = [
    fhir_server_procedure_admin_proc_dfm,
    fhir_server_procedure_ris_ydelse_dfm,
]

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from .bridge_AftaleLager import bridge_AftaleLager
from .bridge_DRGDiagnoseGruppe import bridge_DRGDiagnoseGruppe
from .bridge_DRGProcedureGruppe import bridge_DRGProcedureGruppe
from .bridge_LeverandoervareILSVare import bridge_LeverandoervareILSVare

all_assets = [
    bridge_DRGDiagnoseGruppe,
    bridge_DRGProcedureGruppe,
    bridge_LeverandoervareILSVare,
    bridge_AftaleLager,
]

__all__ = [
    "bridge_DRGDiagnoseGruppe",
    "bridge_DRGProcedureGruppe",
    "all_assets",
    "bridge_LeverandoervareILSVare",
    "bridge_AftaleLager",
]

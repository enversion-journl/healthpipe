# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from .bridge_LeverandoervareILSVare import bridge_LeverandoervareILSVare

all_assets = [
    bridge_LeverandoervareILSVare,
]

__all__ = [
    "all_assets",
    "bridge_LeverandoervareILSVare",
]

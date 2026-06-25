# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Organisation-related DSI assets and static FHIR artifacts.

Includes runtime extraction for NemHandel EAN participants and static files
used by organisation CodeSystem/ConceptMap pipelines.
"""

from DSI.organisation.ean_nemhandel import extract_ean_nemhandel

all_extract_assets = [extract_ean_nemhandel]

__all__ = ["all_extract_assets"]

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""OS2mo (Magenta) GraphQL extraction — the return leg of the codesystem delivery.

Distinct from ``DSI/csv_files/os2mo.py``, which reads hand-maintained CSV
override files for the ConceptMap builders. This package reads OS2mo's live
organisation over the API.
"""

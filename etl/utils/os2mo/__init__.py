# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""OS2mo (Magenta) building blocks, free of database and network I/O.

- ``codesystem_export``: the export model — which codes we send, in which order,
  in which columns, for any codesystem described by a spec
- ``csv_format``: rendering an export set as the uploaded CSV file; ``write_csv``
  writes the local copy
- ``org_units``: the GraphQL read of OS2mo's org units, for the return leg
"""

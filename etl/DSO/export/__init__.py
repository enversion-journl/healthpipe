# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSO Export Module

Deliveries OUT of the warehouse to external parties, as opposed to ``load/``,
which syncs to our own DSO database. Each subpackage owns one recipient.

- os2mo/: codesystems (SOR, Okosted, ...) to Magenta's OS2mo, as CSV files
"""

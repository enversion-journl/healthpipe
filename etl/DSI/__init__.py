# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSI - Data Staging In Layer

1:1 raw extraction from source systems to the ext schema.
No business logic, no CleanCodes, no JOINs.

Structure:
- mssql/: MSSQL sources (ILS, SydAX, MySupply, KMD)
- oracle/: Oracle sources (IFDB extraction for FHIR ETL, EPJI SHAK-a-like)
- csv_files/: CSV/HTTP extracts (SOR, SKS, SHAK, DRG, HAIBA, OS2MO)
- organisation/: NemHandel ean extract and static FHIR JSON
"""

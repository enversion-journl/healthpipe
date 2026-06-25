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
- extract/erp/: ERP source system extraction (ILS, SydAX, MySupply)
- extract/IFDB/: Oracle IFDB extraction for FHIR ETL
- extract/FHIR/: FHIR patient rights extraction
- organisation/: organisation assets (e.g. NemHandel EAN extract) and static FHIR JSON
"""

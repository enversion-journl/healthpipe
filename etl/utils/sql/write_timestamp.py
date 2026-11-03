# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The write time stamped on dw_timestamp_insert and dw_timestamp_update, in UTC."""

# UTC, so a stamp is when the row was written, in one clock that only moves forward,
# whatever the server's own time zone.
WRITE_TIMESTAMP_UTC = "CAST(SYSUTCDATETIME() AS datetime2(0))"

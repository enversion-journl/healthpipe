# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared enums for the DSI extraction layer."""

from enum import Enum


class PartitionType(str, Enum):
    DAILY = "daily"
    MONTHLY = "monthly"
    FULL_LOAD = "full_load"


class TableType(str, Enum):
    MASTER_DATA = "master_data"
    MASTER_DATA_PARTITIONED = "master_data_partitioned"
    TRANSACTIONAL = "transactional"
    DEPENDENT = "dependent"
    BRIDGE = "bridge"

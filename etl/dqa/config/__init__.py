# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DQA subject configs."""

from dqa.config.arc_subjects import ARC_SUBJECTS
from dqa.config.dwh_subjects import DWH_SUBJECTS
from dqa.config.fhir_subjects import FHIR_SUBJECTS
from dqa.config.haiba_subjects import HAIBA_SUBJECTS
from dqa.config.udredningsret_subjects import UDREDNINGSRET_SUBJECTS

ALL_SUBJECTS = [
    *FHIR_SUBJECTS,
    *DWH_SUBJECTS,
    *HAIBA_SUBJECTS,
    *UDREDNINGSRET_SUBJECTS,
    *ARC_SUBJECTS,
]

__all__ = [
    "ALL_SUBJECTS",
    "ARC_SUBJECTS",
    "DWH_SUBJECTS",
    "FHIR_SUBJECTS",
    "HAIBA_SUBJECTS",
    "UDREDNINGSRET_SUBJECTS",
]

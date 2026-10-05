# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DQA remediation: capture dangling-reference business keys and replay the children.

- ``capture_referential_violations`` (capture.py) -- called by the
  ``referential_integrity`` check to enqueue violating business keys.
- ``dqa_remediation_replay`` / ``dqa_remediation_replay_job`` (replay.py) -- the
  operator-triggered semi-automatic replay (reads the queue, runs the DFM ids
  replay + SOF/DSO reproject, re-checks, flips status).
"""

from dqa.remediation.arc_replay import dqa_arc_ifdb_replay, dqa_arc_ifdb_replay_job
from dqa.remediation.arc_vanished_cleanup import (
    dqa_arc_ifdb_vanished_cleanup,
    dqa_arc_ifdb_vanished_cleanup_job,
)
from dqa.remediation.capture import (
    capture_kept_deleted_dups,
    capture_referential_violations,
)
from dqa.remediation.fhir_server_cleanup import (
    dqa_fhir_server_cleanup,
    dqa_fhir_server_cleanup_job,
)
from dqa.remediation.patient_delete_propagation import (
    dqa_patient_delete_propagation,
    dqa_patient_delete_propagation_job,
)
from dqa.remediation.replay import dqa_remediation_replay, dqa_remediation_replay_job

__all__ = [
    "capture_referential_violations",
    "capture_kept_deleted_dups",
    "dqa_remediation_replay",
    "dqa_remediation_replay_job",
    "dqa_arc_ifdb_replay",
    "dqa_arc_ifdb_replay_job",
    "dqa_arc_ifdb_vanished_cleanup",
    "dqa_arc_ifdb_vanished_cleanup_job",
    "dqa_fhir_server_cleanup",
    "dqa_fhir_server_cleanup_job",
    "dqa_patient_delete_propagation",
    "dqa_patient_delete_propagation_job",
]

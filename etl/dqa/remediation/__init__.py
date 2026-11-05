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

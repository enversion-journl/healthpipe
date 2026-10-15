# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL on FHIR: CarePlan + CarePlan_Activity extraction using DFM checkpoint.

CarePlan was the last IFDB flat table with a partitioned producer; with
``fhir_server_careplan_dfm`` on the DFM path its SOF and DSO assets move here
too, and the partitioned daily job retires with it.

Grain: ``careplan_view`` is one row per CarePlan resource (one per patient,
"alle markører"); ``careplan_activity_view`` uses ``forEach: activity`` to
project one row per marker, keyed by ``ifdb_eventmarker_id``.

Activity deletes
----------------
``careplan_activity_view`` runs with ``child_grain_reconcile`` (see
``SOF_RESOURCE_CONFIGS``): every activity row belonging to a CarePlan that
changed in this window is marked deleted before the MERGE, and the MERGE
resurrects the ones the resource still carries. That is what retires a marker
removed upstream — a plain MERGE only ever adds and updates, so a vanished
activity would otherwise stay live in ``fhir.CarePlan_Activity`` forever.

The producer re-posts a patient whose markers were ALL retired as a CarePlan
with zero activities, so a window whose only change is such a patient extracts 0
rows. The reconcile guard treats that as legitimate for any scoped delta — the
changed parents genuinely have no children left — and only refuses to mark on an
empty FULL load, where zero rows means a broken view.
"""

from DSA.FHIR.utils.sql_on_fhir.sof_dfm_factory import create_sof_dfm_asset

_PRODUCERS = ["fhir_sof_infrastructure", "fhir_server_careplan_dfm"]

sof_careplan_dfm = create_sof_dfm_asset(
    asset_name="sof_careplan_dfm",
    config_key="careplan_view",
    checkpoint_resource="CarePlan",
    deps=_PRODUCERS,
)

# The child view is derived from the parent's resources, so it follows the
# parent rather than re-reading the producer. Its reconcile reads the changed
# CarePlans straight from Aidbox, so it does not depend on the parent's window.
sof_careplan_activity_dfm = create_sof_dfm_asset(
    asset_name="sof_careplan_activity_dfm",
    config_key="careplan_activity_view",
    checkpoint_resource="CarePlan_Activity",
    deps=["fhir_sof_infrastructure", "sof_careplan_dfm"],
)

__all__ = ["sof_careplan_dfm", "sof_careplan_activity_dfm"]

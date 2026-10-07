# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Soft-deleted rows must come back when the source delivers them again.

THE BUG THIS CATCHES: a soft-deleted row still occupies the primary key, so the
source delivering that key again is a MATCH, not an INSERT. If the MERGE does not
clear ``dw_is_deleted`` on match, the row is updated with fresh data yet stays
invisible to every consumer filtering ``dw_is_deleted = 0`` -- an address that is
removed and later restored would never reappear, and nothing would fail.

The second half matters just as much: after a soft-delete the payload is often
byte-identical to what was deleted, so a diff predicate over payload columns
alone is false and the UPDATE never fires. The flag itself has to count as a
difference.
"""

import pytest
from DSA.FHIR.utils.sql_on_fhir.sof_utils import SOF_RESOURCE_CONFIGS, build_fhir_merge_sql

pytestmark = pytest.mark.unit

_COLUMNS = ["patient_id", "address_period_start", "address_line", "last_updated"]
_PRIMARY_KEYS = ["patient_id", "address_period_start"]


def _merge(**kwargs) -> str:
    return build_fhir_merge_sql("Patient_Address", _COLUMNS, _PRIMARY_KEYS, **kwargs)


def test_resurrection_clears_the_flag_and_survives_an_identical_payload():
    sql = _merge(resurrect_soft_deleted=True)

    assert "tgt.[dw_is_deleted] = 0" in sql, (
        "MERGE must clear dw_is_deleted on match, or a restored row stays "
        "invisible to consumers filtering dw_is_deleted = 0."
    )
    assert "OR tgt.[dw_is_deleted] <> 0" in sql, (
        "The flag must count as a difference; a resurrected row's payload is "
        "typically unchanged, so a payload-only diff predicate never fires."
    )


def test_flag_is_untouched_when_resurrection_is_off():
    """Where the view projects dw_is_deleted it is an ordinary payload column and
    round-trips on its own -- forcing it to 0 would discard the source's value."""
    sql = _merge(resurrect_soft_deleted=False)

    assert "tgt.[dw_is_deleted] = 0" not in sql
    assert "dw_is_deleted" not in sql


def test_patient_address_opts_in():
    """fhir.Patient_Address gets its flag from a reconcile rather than from the
    resource, so it is the case that needs the behaviour."""
    cfg = SOF_RESOURCE_CONFIGS["patient_address_view"]
    assert cfg.get("resurrect_soft_deleted") is True
    assert cfg.get("child_grain_reconcile") is True


def test_patient_view_does_not_opt_into_child_grain_reconcile():
    assert SOF_RESOURCE_CONFIGS["patient_view"].get("child_grain_reconcile") is not True


def test_child_grain_reconcile_always_resurrects():
    missing = [
        name
        for name, cfg in SOF_RESOURCE_CONFIGS.items()
        if cfg.get("child_grain_reconcile") and cfg.get("resurrect_soft_deleted") is not True
    ]
    assert missing == [], (
        "child_grain_reconcile without resurrect_soft_deleted leaves restored "
        f"codes invisible: {missing}"
    )


def test_organization_view_opts_into_resource_grain_reconcile():
    """fhir.Organization gets dw_is_deleted from a full-extract reconcile, not the resource."""
    cfg = SOF_RESOURCE_CONFIGS["organization_view"]
    assert cfg.get("resurrect_soft_deleted") is True
    assert cfg.get("resource_grain_reconcile") is True
    assert cfg.get("child_grain_reconcile") is not True

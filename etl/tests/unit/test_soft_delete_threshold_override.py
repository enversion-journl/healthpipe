# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Per-run override of the soft-delete guard.

A rebuild can retire more rows than the standing threshold. The override
applies to one run and does not become the default.
"""

import dagster as dg
import pytest
from core.dfm_config import RunModeConfig
from core.etl_thresholds import (
    DEFAULT_THRESHOLD,
    check_soft_delete_threshold,
    get_soft_delete_threshold,
)

pytestmark = pytest.mark.unit


def test_override_raises_the_bar_for_this_run_only():
    """The standing threshold must be unchanged by a run that overrode it."""
    assert get_soft_delete_threshold("dim.Borger") == 10000
    assert get_soft_delete_threshold("dim.Borger", 50_000) == 50_000
    assert get_soft_delete_threshold("dim.Borger") == 10000


def test_count_above_the_standing_threshold_passes_with_an_override():
    check_soft_delete_threshold("dim.Borger", 5469, override=10_000)


def test_count_above_the_override_still_fails():
    """An override is a raised bar, not a disabled guard."""
    with pytest.raises(dg.Failure):
        check_soft_delete_threshold("dim.Borger", 20_000, override=10_000)


def test_guard_still_fires_without_an_override():
    with pytest.raises(dg.Failure) as excinfo:
        check_soft_delete_threshold("dim.Borger", 54690)
    assert excinfo.value.metadata["threshold_overridden"].value is False
    assert "ops.dim_Borger.config.soft_delete_threshold" in excinfo.value.description
    assert "ops.Borger." not in excinfo.value.description


def test_remediation_names_the_ref_address_op():
    with pytest.raises(dg.Failure) as excinfo:
        check_soft_delete_threshold("ref.Patient_Address_To_Borger", 54690)
    assert (
        "ops.ref_patient_address_borger.config.soft_delete_threshold" in excinfo.value.description
    )


def test_config_defaults_to_no_override():
    """Scheduled runs supply no config, so they must get the standing threshold —
    this is what stops an override from leaking beyond the run it was set for."""
    assert RunModeConfig().soft_delete_threshold is None
    assert get_soft_delete_threshold("anything.Unlisted", None) == DEFAULT_THRESHOLD

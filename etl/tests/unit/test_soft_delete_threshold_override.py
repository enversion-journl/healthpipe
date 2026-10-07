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
    assert get_soft_delete_threshold("dim.Borger") == 1000
    assert get_soft_delete_threshold("dim.Borger", 10_000) == 10_000
    assert get_soft_delete_threshold("dim.Borger") == 1000


def test_count_above_the_standing_threshold_passes_with_an_override():
    check_soft_delete_threshold("dim.Borger", 5469, override=10_000)


def test_count_above_the_override_still_fails():
    """An override is a raised bar, not a disabled guard."""
    with pytest.raises(dg.Failure):
        check_soft_delete_threshold("dim.Borger", 20_000, override=10_000)


def test_guard_still_fires_without_an_override():
    with pytest.raises(dg.Failure) as excinfo:
        check_soft_delete_threshold("dim.Borger", 5469)
    assert excinfo.value.metadata["threshold_overridden"].value is False
    assert "ops.dim_Borger.config.soft_delete_threshold" in excinfo.value.description
    assert "ops.Borger." not in excinfo.value.description


def test_remediation_names_the_ref_address_op():
    with pytest.raises(dg.Failure) as excinfo:
        check_soft_delete_threshold("ref.Patient_Address_To_Borger", 5469)
    assert (
        "ops.ref_patient_address_borger.config.soft_delete_threshold" in excinfo.value.description
    )


def test_ratio_guard_fires_below_the_absolute_threshold():
    """50 of 200 is under 1000 but 25% > 10% — the ratio is what stops a small-table wipe."""
    with pytest.raises(dg.Failure) as excinfo:
        check_soft_delete_threshold("fhir.Organization", 50, live_row_count=200)
    assert excinfo.value.metadata["ratio_limit"].value == 0.10
    assert "10%" in excinfo.value.description


def test_ratio_guard_allows_a_small_fraction():
    check_soft_delete_threshold("fhir.Organization", 5, live_row_count=200)


def test_override_skips_the_ratio_guard():
    check_soft_delete_threshold("fhir.Organization", 50, override=100, live_row_count=200)


def test_config_defaults_to_no_override():
    """Scheduled runs supply no config, so they must get the standing threshold —
    this is what stops an override from leaking beyond the run it was set for."""
    assert RunModeConfig().soft_delete_threshold is None
    assert get_soft_delete_threshold("anything.Unlisted", None) == DEFAULT_THRESHOLD


def test_ratio_param_fires_for_an_unregistered_asset():
    """Callers with dynamic asset names (DRG archive, [internal ticket]) pass the ratio
    directly instead of registering 16 entries in the core dict."""
    with pytest.raises(dg.Failure) as excinfo:
        check_soft_delete_threshold("arc_drg_kontakt", 300, live_row_count=1000, ratio=0.20)
    assert excinfo.value.metadata["ratio_limit"].value == 0.20


def test_ratio_param_allows_a_fraction_below_the_limit():
    check_soft_delete_threshold("arc_drg_kontakt", 100, live_row_count=1000, ratio=0.20)


def test_ratio_param_without_live_count_skips_the_ratio_guard():
    """No denominator, no ratio — the absolute cap still applies alone."""
    check_soft_delete_threshold("arc_drg_kontakt", 300, ratio=0.20)

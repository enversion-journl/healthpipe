# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the DQA framework scaffolding: registry, model defaults, the
factory (anchors + bound checks + the per-run config param), and the
remediation-capture guard override. Pure/mock -- no DB.
"""

import pytest
from dqa.check_config import DqaCheckConfig
from dqa.config import ALL_SUBJECTS
from dqa.factory import build_all, build_subject
from dqa.model import Cadence, CheckSpecification, DqaSubject, Severity, Track

pytestmark = pytest.mark.unit


def test_subject_results_resource_key_defaults_to_none():
    s = DqaSubject(
        name="x", track=Track.FHIR, table="fhir.X", resource_key="r", identity_column="x_id"
    )
    assert s.results_resource_key is None
    assert s.deleted_column is None


def _subject(*checks: CheckSpecification) -> DqaSubject:
    return DqaSubject(
        name="fhir_thing",
        track=Track.FHIR,
        table="fhir.Thing",
        resource_key="mssql_target_staging",
        identity_column="thing_id",
        checks=tuple(checks),
    )


def test_build_subject_emits_anchor_per_cadence_and_named_checks():
    subj = _subject(
        CheckSpecification("not_null", Severity.ERROR, Cadence.DAILY, {"columns": ["thing_id"]}),
        CheckSpecification(
            "uniqueness", Severity.ERROR, Cadence.DAILY, {"grain": "thing_id"}, name_suffix="rid"
        ),
        CheckSpecification("custom_sql", Severity.WARN, Cadence.WEEKLY, {"sql": "SELECT 1"}),
    )
    assets, checks = build_subject(subj)

    anchor_keys = {a.key.to_user_string() for a in assets}
    assert anchor_keys == {"dqa_fhir_thing_daily", "dqa_fhir_thing_weekly"}

    check_names = {next(iter(c.check_keys)).name for c in checks}
    assert check_names == {
        "fhir_thing__not_null",
        "fhir_thing__uniqueness__rid",
        "fhir_thing__custom_sql",
    }


def test_check_config_max_keys_is_optional():
    # The factory binds `config: DqaCheckConfig` on each check (verified end-to-end);
    # here we pin the config contract: max_keys exists and is optional (None default),
    # so existing jobs need no run config.
    assert "max_keys" in DqaCheckConfig.model_fields
    assert DqaCheckConfig().max_keys is None


def test_build_all_has_unique_check_names_and_nonzero():
    assets, checks = build_all(ALL_SUBJECTS)
    assert assets and checks
    names = [next(iter(c.check_keys)).name for c in checks]
    assert len(names) == len(set(names)), "duplicate asset-check names across subjects"


def test_max_keys_override_contextvar():
    from dqa.remediation.capture import _max_keys, reset_max_keys_override, set_max_keys_override

    base = _max_keys()
    token = set_max_keys_override(base + 12345)
    try:
        assert _max_keys() == base + 12345
    finally:
        reset_max_keys_override(token)
    assert _max_keys() == base

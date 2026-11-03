# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The switch for running the SFI chain without consumers, and the asset that sets it."""

from types import SimpleNamespace

import pytest
from utils.fhir.sfi_consumers import CONSUMERS_ABSENT_TAG, sfi_consumers_absent

pytestmark = pytest.mark.unit


class _Dfm:
    def __init__(self, since=None):
        self.since = since
        self.set_calls: list[dict] = []

    def get_checkpoint(self, schema, table, *, scope="staging"):
        return (
            self.since
            if (schema, table, scope)
            == (
                "fhir",
                "SFI_datapoint_store",
                "sfi_consumers",
            )
            else None
        )

    def set_checkpoint(self, schema, table, **kwargs):
        self.set_calls.append({"schema": schema, "table": table, **kwargs})
        self.since = kwargs["value"]


def _ctx(tags=None):
    return SimpleNamespace(run=SimpleNamespace(tags=tags or {}), log=SimpleNamespace(info=print))


@pytest.mark.parametrize(
    ("since", "tag", "absent"),
    [
        ("2026-10-27 09:00:00", None, True),
        (None, None, False),
        (None, "true", True),
        ("2026-10-27 09:00:00", "false", False),
    ],
)
def test_the_dfm_row_switches_the_chain_and_a_run_tag_overrides_it(since, tag, absent):
    tags = {CONSUMERS_ABSENT_TAG: tag} if tag else {}
    assert sfi_consumers_absent(_ctx(tags), _Dfm(since)) is absent


def test_start_sets_the_switch_once_and_keeps_its_first_time(monkeypatch):
    import infrastructure.assets.sfi_consumers_absent as start

    dfm = _Dfm()
    monkeypatch.setattr(start, "DfmManager", lambda *a: dfm)
    run = start.sfi_consumers_absent_start.op.compute_fn.decorated_fn

    first = run(_ctx(), None).metadata["consumers_absent_since"].value
    second = run(_ctx(), None).metadata["consumers_absent_since"].value

    assert first == second and len(dfm.set_calls) == 1
    assert dfm.set_calls[0]["scope"] == "sfi_consumers"
    assert sfi_consumers_absent(_ctx(), dfm)

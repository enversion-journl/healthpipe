# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""LPR3 archive guard inputs that need no database."""

from types import SimpleNamespace

import pytest
from DSI.csv_files.lpr3_archive import missing_sources, soft_delete_overrides
from DSI.csv_files.lpr3_datasets import ARCHIVES_BY_TABLE

pytestmark = pytest.mark.unit


def test_empty_required_dimension_counts_as_missing_but_empty_fact_does_not():
    dataset = ARCHIVES_BY_TABLE["lpr3_kontaktlokation"]
    manifest = {prefix: SimpleNamespace(rows=1) for prefix in dataset.required_sources}
    assert missing_sources(dataset, manifest) == ()
    manifest["DimDato"] = SimpleNamespace(rows=0)
    manifest["fctKontaktLokation"] = SimpleNamespace(rows=0)
    assert missing_sources(dataset, manifest) == ("DimDato",)


def test_soft_delete_override_is_read_from_the_archive_op_config():
    context = SimpleNamespace(
        run=SimpleNamespace(
            run_config={"ops": {"arc_lpr3_kontakt": {"config": {"soft_delete_threshold": 5}}}}
        )
    )
    overrides = soft_delete_overrides(context)
    assert overrides["lpr3_kontakt"] == 5
    assert overrides["lpr3_forloeb"] is None

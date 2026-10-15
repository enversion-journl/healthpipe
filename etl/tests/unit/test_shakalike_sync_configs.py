# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SHAK-a-like is wired into the conceptmap DW_EK resolution."""

import pytest
from DSA.ConceptMap.utils import _load_conceptmap_dw_ek_update_sql, sql_update_conceptmap_dw_ek
from DSO.load.sync_configs import CODESYSTEM_SYNC_CONFIGS, CONCEPTMAP_SYNC_CONFIGS

pytestmark = pytest.mark.unit


def test_shakalike_dw_ek_resolution_block():
    assert "SHAKAlikeToSOR" in _load_conceptmap_dw_ek_update_sql()
    sql = sql_update_conceptmap_dw_ek("SHAKAlikeToSOR")
    assert "[conceptmap].[SHAKAlikeToSOR]" in sql
    assert "[map].[SHAKAlike]" in sql
    assert "[dw_ek_SHAKAlike]" in sql
    assert "[ref].[Organisation]" in sql


def test_shakalike_sync_configs():
    assert "codesystem_shakalike_concept" in CODESYSTEM_SYNC_CONFIGS
    assert CODESYSTEM_SYNC_CONFIGS["codesystem_shakalike_concept"].table == "SHAKAlikeConcept"
    cfg = CONCEPTMAP_SYNC_CONFIGS["conceptmap_shakalike_to_sor"]
    assert cfg.table == "SHAKAlikeToSOR"
    assert cfg.source_table == "vw_shakalike_to_sor"
    assert cfg.primary_key == ["dw_ek_SHAKAlike"]


def test_shakalike_is_on_organisation_job_not_fhir_put():
    import inspect

    from jobs import codesystem_jobs, organization_jobs

    org_src = inspect.getsource(organization_jobs)
    fhir_src = inspect.getsource(codesystem_jobs)
    assert "ext_epji_location" in org_src
    assert "cs_shakalike" in org_src
    assert "cm_shakalike_to_sor" in org_src
    assert "cs_shakalike" not in fhir_src
    assert "ext_epji_location" not in fhir_src

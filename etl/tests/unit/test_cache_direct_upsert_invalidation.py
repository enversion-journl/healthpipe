# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Direct-upsert cache invalidation regression tests."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from core.cache import CacheCoordinator
from core.resources import FHIRDatabaseResource, SqliteIdentifierCache

pytestmark = pytest.mark.unit

_PATIENT_SYSTEM = "urn:test:direct-upsert:patient"
_PATIENT_VALUE = "P1"
_OBS_SYSTEM = "urn:test:direct-upsert:observation"
_OBS_VALUE = "O1"


def _build_resource(tmp_path: Path) -> FHIRDatabaseResource:
    resource = FHIRDatabaseResource(
        host="localhost",
        port=5432,
        database="aidbox",
        user="postgres",
        password="secret",
    )
    resource._cache_coordinator = CacheCoordinator(cache_dir=str(tmp_path))
    return resource


def _seed_lookup(
    *,
    tmp_path: Path,
    resource: FHIRDatabaseResource,
    resource_type: str,
    system: str,
    value: str,
    fhir_id: str,
) -> str:
    db_path = SqliteIdentifierCache.cache_path(
        resource_type,
        frozenset([system]),
        cache_dir=str(tmp_path),
    )
    SqliteIdentifierCache.build_from_iter(
        iter([(system, value, fhir_id)]),
        db_path,
    )
    lookup = SqliteIdentifierCache(db_path)
    key = (resource_type, frozenset([system]))
    resource._identifier_caches[key] = lookup
    resource._identifier_cache = lookup
    resource._active_cache_key = key
    return db_path


def _patch_connection():
    cur = MagicMock()
    cur.__enter__.return_value = cur
    cur.__exit__.return_value = False
    cur.fetchone.return_value = (1,)

    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.cursor.return_value = cur

    get_connection_mock = MagicMock(return_value=conn)
    return patch.object(FHIRDatabaseResource, "get_connection", get_connection_mock)


def test_batch_upsert_resources_invalidate_true_keeps_file_for_reopen(tmp_path: Path) -> None:
    resource = _build_resource(tmp_path)
    db_path = _seed_lookup(
        tmp_path=tmp_path,
        resource=resource,
        resource_type="Patient",
        system=_PATIENT_SYSTEM,
        value=_PATIENT_VALUE,
        fhir_id="patient-uuid-1",
    )
    reader = SqliteIdentifierCache(db_path)
    payload = MagicMock()
    payload.id = None
    payload.json.return_value = '{"resourceType":"Patient"}'

    with _patch_connection():
        successes, failures, _failed_ids = resource.batch_upsert_resources(
            resources=[payload],
            resource_type="Patient",
            batch_size=100,
            invalidate_cache=True,
        )

    assert successes == 1
    assert failures == 0
    reader.close()  # force lazy reopen path
    assert os.path.exists(db_path)
    assert reader.get((_PATIENT_SYSTEM, _PATIENT_VALUE)) == "patient-uuid-1"
    assert (tmp_path / "patient.invalidated").exists()


def test_batch_upsert_resources_invalidate_false_skips_marker(tmp_path: Path) -> None:
    resource = _build_resource(tmp_path)
    db_path = _seed_lookup(
        tmp_path=tmp_path,
        resource=resource,
        resource_type="Patient",
        system=_PATIENT_SYSTEM,
        value=_PATIENT_VALUE,
        fhir_id="patient-uuid-1",
    )
    payload = MagicMock()
    payload.id = None
    payload.json.return_value = '{"resourceType":"Patient"}'

    with _patch_connection():
        successes, failures, _failed_ids = resource.batch_upsert_resources(
            resources=[payload],
            resource_type="Patient",
            batch_size=100,
            invalidate_cache=False,
        )

    assert successes == 1
    assert failures == 0
    assert os.path.exists(db_path)
    assert not (tmp_path / "patient.invalidated").exists()


def test_batch_upsert_resource_dicts_invalidate_true_keeps_file_for_reopen(
    tmp_path: Path,
) -> None:
    resource = _build_resource(tmp_path)
    db_path = _seed_lookup(
        tmp_path=tmp_path,
        resource=resource,
        resource_type="Observation",
        system=_OBS_SYSTEM,
        value=_OBS_VALUE,
        fhir_id="observation-uuid-1",
    )
    reader = SqliteIdentifierCache(db_path)
    resource_dicts = [("obs-1", {"resourceType": "Observation"})]

    with (
        patch.object(
            FHIRDatabaseResource,
            "_prepare_resource_dicts_for_upsert",
            return_value=(resource_dicts, 0, []),
        ),
        patch.object(FHIRDatabaseResource, "_resolve_dict_upsert_workers", return_value=1),
        patch.object(
            FHIRDatabaseResource,
            "_upsert_dicts_single_connection",
            return_value=(1, 0, []),
        ),
    ):
        successes, failures, _failed_ids = resource.batch_upsert_resource_dicts(
            resource_dicts=resource_dicts,
            resource_type="Observation",
            invalidate_cache=True,
        )

    assert successes == 1
    assert failures == 0
    reader.close()  # force lazy reopen path
    assert os.path.exists(db_path)
    assert reader.get((_OBS_SYSTEM, _OBS_VALUE)) == "observation-uuid-1"
    assert (tmp_path / "observation.invalidated").exists()


def test_batch_upsert_resource_dicts_invalidate_false_skips_marker(tmp_path: Path) -> None:
    resource = _build_resource(tmp_path)
    db_path = _seed_lookup(
        tmp_path=tmp_path,
        resource=resource,
        resource_type="Observation",
        system=_OBS_SYSTEM,
        value=_OBS_VALUE,
        fhir_id="observation-uuid-1",
    )
    resource_dicts = [("obs-1", {"resourceType": "Observation"})]

    with (
        patch.object(
            FHIRDatabaseResource,
            "_prepare_resource_dicts_for_upsert",
            return_value=(resource_dicts, 0, []),
        ),
        patch.object(FHIRDatabaseResource, "_resolve_dict_upsert_workers", return_value=1),
        patch.object(
            FHIRDatabaseResource,
            "_upsert_dicts_single_connection",
            return_value=(1, 0, []),
        ),
    ):
        successes, failures, _failed_ids = resource.batch_upsert_resource_dicts(
            resource_dicts=resource_dicts,
            resource_type="Observation",
            invalidate_cache=False,
        )

    assert successes == 1
    assert failures == 0
    assert os.path.exists(db_path)
    assert not (tmp_path / "observation.invalidated").exists()

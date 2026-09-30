# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

import os
import pickle
from dataclasses import replace
from unittest.mock import MagicMock

import dagster as dg
import pytest
from core.extract import ExtractBatchingPolicy, materialize_row_batches_to_pickle_artifact
from core.resources import IFDBOracleResource
from DSI.connectors.oracle.extractor import OracleExtractor
from DSI.extract.IFDB.aktivitetsresultat import AKTIVITETSRESULTAT_CONFIG

pytestmark = pytest.mark.integration

RUN_REAL_ORACLE = pytest.mark.skipif(
    os.getenv("RUN_BATCHED_EXTRACT_ORACLE_TEST", "false").lower() not in {"1", "true", "yes", "on"},
    reason="Set RUN_BATCHED_EXTRACT_ORACLE_TEST=1 to run against dev Oracle",
)


def _oracle_resource() -> IFDBOracleResource:
    return IFDBOracleResource(
        host=os.getenv("IFDB_PROD_ORACLE_SERVER", "oracle"),
        port=int(os.getenv("IFDB_PROD_ORACLE_PORT", "1521")),
        username=os.getenv("IFDB_PROD_ORACLE_USERNAME", "myuser"),
        password=os.getenv("IFDB_PROD_ORACLE_PASSWORD", "tempPassword1!"),
        service_name=os.getenv("IFDB_PROD_ORACLE_DATABASE", "ORACLE_DB"),
    )


def _context() -> dg.AssetExecutionContext:
    context = MagicMock(spec=dg.AssetExecutionContext)
    context.log = MagicMock()
    context.has_partition_key = False
    context.partition_key = None
    context.run.tags = {}
    context.add_output_metadata = MagicMock()
    return context


@RUN_REAL_ORACLE
def test_batched_extract_streams_real_oracle_rows_to_plain_pickle_list(tmp_path):
    config = replace(
        AKTIVITETSRESULTAT_CONFIG,
        additional_where="ROWNUM <= 5",
        batching=None,
    )
    policy = ExtractBatchingPolicy(
        enabled=True,
        fetch_size=2,
        env_prefix="TEST_BATCHED_EXTRACT_ORACLE",
        log_every_batches=1,
    )
    context = _context()

    with _oracle_resource().get_connection() as conn:
        extractor = OracleExtractor(conn, context=context, source_system="IFDB")
        batch_source = extractor.make_row_batch_source(
            config,
            None,
            "full_load",
            policy,
        )
        artifact = materialize_row_batches_to_pickle_artifact(
            batch_source=batch_source,
            policy=policy,
            context=context,
            source_label=config.table,
            temp_dir=str(tmp_path),
        )

    with open(artifact.temp_path, "rb") as file:
        rows = pickle.load(file)

    assert type(rows) is list
    assert 0 < len(rows) <= 5
    assert artifact.row_count == len(rows)
    assert artifact.batch_count >= 1
    assert rows[0]["OBJEKT_ID"] is not None
    assert "XML" in rows[0]

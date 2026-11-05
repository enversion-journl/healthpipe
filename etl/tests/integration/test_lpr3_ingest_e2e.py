# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The registered job from all 47 synthetic files through 38 SQL archives.

Uses the disposable local MSSQL fixture, the real DFM checkpoint and the real
Dagster asset graph. Freshness is tested separately from historical fixtures.
"""

import dagster as dg
import pytest
from core.dfm_manager import DfmManager
from core.resources import FileShareResource
from DSI.connectors.delivery_detection import DELIVERY_SCHEMA, DELIVERY_TARGET_TYPE
from DSI.csv_files.lpr3 import DELIVERY_UNIT, lpr3_extract_assets
from DSI.csv_files.lpr3_archive import lpr3_archive_assets, scalar
from DSI.csv_files.lpr3_datasets import ARCHIVE_DATASETS, SOURCES_BY_PREFIX
from jobs.lpr3_jobs import lpr3_ingest_job
from tests.fixtures.lpr3 import build_delivery, source_row

pytestmark = pytest.mark.integration


def test_registered_job_archives_every_source_before_advancing_checkpoint(request, tmp_path):
    database = request.getfixturevalue("archive_db")

    def contacts(numbers, changed=False):
        rows = {
            prefix: [
                source_row(
                    SOURCES_BY_PREFIX[prefix],
                    number,
                    **(
                        {"DW_IsIndirectError": "0"}
                        if prefix == "DimKontakt" and number == 1 and changed
                        else {}
                    ),
                )
                for number in numbers
            ]
            for prefix in ("DimKontakt", "fctKontakt")
        }
        hf = SOURCES_BY_PREFIX["DimHelbredsForloeb"]
        rows[hf.source_prefix] = [
            source_row(hf),
            source_row(hf, DW_SK_HelbredsForloeb="-1", DW_EK_HelbredsForloeb="-1"),
        ]
        return rows

    def checkpoint():
        return DfmManager(database, dg.build_asset_context()).get_checkpoint(
            DELIVERY_SCHEMA, DELIVERY_UNIT, target_type=DELIVERY_TARGET_TYPE
        )

    build_delivery(tmp_path, rows=contacts((1, 2)))
    definitions = dg.Definitions(
        assets=[*lpr3_extract_assets, *lpr3_archive_assets],
        jobs=[lpr3_ingest_job],
        resources={
            "mssql_target_staging": database,
            "lpr3_share": FileShareResource(base_dir=str(tmp_path)),
        },
    )
    job = definitions.resolve_job_def("lpr3_ingest_job")
    selection = [
        asset.key
        for asset in [*lpr3_extract_assets, *lpr3_archive_assets]
        if asset.key != dg.AssetKey("lpr3_leverance_alder")
    ]
    with dg.DagsterInstance.ephemeral() as instance:
        result = job.execute_in_process(instance=instance, asset_selection=selection)
        assert result.success
        with database.get_connection() as conn:
            for dataset in ARCHIVE_DATASETS:
                assert scalar(conn, f"SELECT COUNT_BIG(*) FROM arc.[{dataset.arc_table}]") == (
                    2 if dataset.entity in ("kontakt", "helbredsforloeb") else 1
                )
            assert (
                scalar(
                    conn,
                    "SELECT COUNT_BIG(*) FROM arc.lpr3_helbredsforloeb WHERE noeglemetode='sentinel' AND helbredsforloeb_uuid IS NULL AND forloeb_foerste_uuid IS NULL",
                )
                == 1
            )
            assert (
                scalar(conn, "SELECT COUNT_BIG(*) FROM meta.LPR3Leverance WHERE status='archived'")
                == 47
            )
        assert checkpoint() == "2026-09-15T19:56:03"

        build_delivery(
            tmp_path,
            stamp="20260921190000",
            cutoff="2026-09-20",
            cadence="U",
            rows=contacts((1, 3), changed=True),
        )
        second = job.execute_in_process(instance=instance, asset_selection=selection)
        assert second.success
        assert checkpoint() == "2026-09-21T19:00:00"
        with database.get_connection() as conn:
            assert (
                scalar(
                    conn,
                    "SELECT MIN(raekker_arc) FROM meta.LPR3Leverance WHERE run_id=? AND arc_tabel='lpr3_kontakt'",
                    (second.run_id,),
                )
                == 2
            )
            assert (
                scalar(conn, "SELECT COUNT_BIG(*) FROM arc.lpr3_kontakt WHERE dw_is_deleted=0") == 2
            )
            assert (
                scalar(conn, "SELECT COUNT_BIG(*) FROM arc.lpr3_kontakt WHERE dw_is_deleted=1") == 1
            )
            cursor = conn.cursor()
            cursor.execute(
                "SELECT raekker_indsat,raekker_opdateret,raekker_soft_slettet FROM meta.LPR3Leverance WHERE kildetabel='DimKontakt' AND leverance_tidsstempel='2026-09-21T19:00:00'"
            )
            assert tuple(cursor.fetchone()) == (1, 1, 1)
            cursor.close()
            assert (
                scalar(
                    conn,
                    "SELECT COUNT_BIG(*) FROM meta.LPR3Leverance WHERE run_id=? AND status='archived'",
                    (second.run_id,),
                )
                == 47
            )

        # A deliberate replay exercises every merge and returns 0/0/0. A
        # subsequent automatic run skips extraction without touching archives.
        pinned = {
            "ops": {"lpr3_leverance_plan": {"config": {"delivery_on_or_before": "2026-09-21"}}}
        }
        replay = job.execute_in_process(
            instance=instance, run_config=pinned, asset_selection=selection
        )
        assert replay.success
        with database.get_connection() as conn:
            assert (
                scalar(
                    conn,
                    "SELECT COUNT_BIG(*) FROM meta.LPR3Leverance WHERE run_id=? AND status='archived'",
                    (replay.run_id,),
                )
                == 47
            )
            assert (
                scalar(
                    conn,
                    "SELECT SUM(raekker_indsat+raekker_opdateret+raekker_soft_slettet) FROM meta.LPR3Leverance WHERE run_id=?",
                    (replay.run_id,),
                )
                == 0
            )
        noop = job.execute_in_process(instance=instance, asset_selection=selection)
        assert noop.success
        assert checkpoint() == "2026-09-21T19:00:00"
        with database.get_connection() as conn:
            assert scalar(conn, "SELECT COUNT_BIG(*) FROM ext.lpr3_leverance_manifest") == 0
            assert (
                scalar(
                    conn,
                    "SELECT COUNT_BIG(*) FROM meta.LPR3Leverance WHERE run_id=?",
                    (noop.run_id,),
                )
                == 0
            )

        # Older cutoff is refused before even replacing the current landing.
        older = {
            "ops": {"lpr3_leverance_plan": {"config": {"delivery_on_or_before": "2026-09-15"}}}
        }
        rejected = job.execute_in_process(
            instance=instance, run_config=older, asset_selection=selection, raise_on_error=False
        )
        assert not rejected.success
        assert checkpoint() == "2026-09-21T19:00:00"
        with database.get_connection() as conn:
            assert scalar(conn, "SELECT COUNT_BIG(*) FROM ext.lpr3_leverance_manifest") == 0

        # Missing input skips affected targets, and the final gate prevents
        # accepting this incomplete snapshot as the unit's checkpoint.
        build_delivery(
            tmp_path,
            stamp="20260922190000",
            cutoff="2026-09-21",
            rows=contacts((1, 3), changed=True),
            omit=("DimAdresse",),
        )
        missing = job.execute_in_process(
            instance=instance, asset_selection=selection, raise_on_error=False
        )
        assert not missing.success
        assert checkpoint() == "2026-09-21T19:00:00"
        with database.get_connection() as conn:
            assert scalar(conn, "SELECT COUNT_BIG(*) FROM ext.lpr3_leverance_manifest") == 46
            assert (
                scalar(conn, "SELECT COUNT_BIG(*) FROM arc.lpr3_adresse WHERE dw_is_deleted=0") == 1
            )

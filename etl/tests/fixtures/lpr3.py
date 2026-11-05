"""Synthetic LPR3 deliveries and the disposable local MSSQL fixtures shared by the LPR3 tests."""

import csv
import io
import os
import tarfile
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from DSI.csv_files import lpr3
from DSI.csv_files._lpr3_keys import identity_table
from DSI.csv_files._lpr3_run import RUN_TABLE, claim_run, owned_landing
from DSI.csv_files._sds_tar import execute_statements
from DSI.csv_files.lpr3_datasets import (
    ARCHIVE_DATASETS,
    METADATA_HEADER,
    SOURCE_DATASETS,
    SOURCES_BY_PREFIX,
)
from infrastructure.utils.ddl_loader import execute_sql_file, execute_sql_files_from_directory


def source_row(dataset, number=1, **overrides):
    values = {}
    for column in dataset.columns:
        kind = column.base_type
        if kind in ("bigint", "int", "smallint", "tinyint"):
            value = str(number)
        elif kind == "bit":
            value = "0" if column.name in ("DW_IsDeleted", "DW_IsInferred") else "1"
        elif kind == "uniqueidentifier":
            value = f"00000000-0000-4000-8000-{number:012d}"
        elif kind == "varbinary":
            value = f"{number:032x}"
        elif kind == "datetime2":
            value = "2026-09-01 12:34:56.1234567"
        elif kind == "date":
            value = "2026-09-01"
        elif kind == "time":
            value = "12:34:56.1234567"
        elif kind == "decimal":
            value = "1,5" if column.scale else "1"
        else:
            value = "Test æøå"[: column.wire_width]
        values[column.name] = value
    assert set(overrides) <= set(values)
    values.update(overrides)
    return [values[name] for name in dataset.source_columns]


def csv_bytes(rows):
    buffer = io.StringIO(newline="")
    csv.writer(buffer, delimiter=";", quoting=csv.QUOTE_ALL).writerows(rows)
    return buffer.getvalue().encode("cp1252")


def build_delivery(
    directory,
    *,
    stamp="20260915195603",
    cutoff="2026-09-10",
    cadence="M",
    rows=None,
    omit=(),
    row_count_delta=None,
    byte_count_delta=None,
    alter_members=None,
):
    """Write a synthetic LPR3 tar.bz2 with data before Metadata/ExportLog and optional faults."""
    rows = rows or {}
    row_count_delta, byte_count_delta = row_count_delta or {}, byte_count_delta or {}
    data, metadata, export = {}, {}, [["FilNavn", "AntalRaekker", "FilStoerrelseBytes"]]
    for dataset in SOURCE_DATASETS:
        prefix = dataset.source_prefix
        filename = dataset.data_filename(cutoff)
        records = rows.get(prefix, [source_row(dataset)])
        content = csv_bytes([dataset.source_columns, *records])
        if prefix not in omit:
            data[filename] = content
        export.append(
            [
                filename,
                len(records) + row_count_delta.get(prefix, 0),
                len(content) + byte_count_delta.get(prefix, 0),
            ]
        )
        metadata["Metadata_" + filename] = csv_bytes([METADATA_HEADER, *dataset.metadata_records])
    members = {
        **data,
        **metadata,
        f"SDP_ExportLog_{cutoff.replace('-', '')}.csv": csv_bytes(export),
    }
    if alter_members:
        alter_members(members)
    path = directory / f"Region1083_LPRGrunddata_{cadence}_{stamp}.tar.bz2"
    with tarfile.open(path, "w:bz2") as tar:
        for name, content in members.items():
            info = tarfile.TarInfo("1083/" + name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    return path


@pytest.fixture
def lpr3_db(real_mssql_target_staging_resource):
    resource = real_mssql_target_staging_resource
    if os.getenv("ENABLE_TEST_WRITES", "false").lower() != "true":
        pytest.skip("requires ENABLE_TEST_WRITES=true")
    if resource.host != "mssql" or resource.database != "staging":
        pytest.fail("This destructive LPR3 fixture requires local mssql/staging")
    execute_sql_file(resource, "tables/meta/02_dfm_state.sql")
    execute_sql_file(resource, "tables/meta/03_dfm_run_log.sql")
    execute_sql_file(resource, "migrations/20261005_01_dfm_run_log_source_columns.sql")

    def cleanup():
        with resource.get_connection() as conn:
            cursor = conn.cursor()
            for table in [*(d.ext_table for d in SOURCE_DATASETS), RUN_TABLE, lpr3.MANIFEST_TABLE]:
                cursor.execute(f"DROP TABLE IF EXISTS [ext].[{table}]")
            conn.commit()
            cursor.close()

    cleanup()
    yield resource
    cleanup()


def context(run_id, *, previous_finished=True):
    instance = MagicMock()
    instance.get_run_by_id.return_value = SimpleNamespace(is_finished=previous_finished)
    return SimpleNamespace(run_id=run_id, instance=instance, log=MagicMock())


def prepare(resource, path, run_id):
    archive = next(a for a in lpr3.discover_archives(str(path.parent)) if a.path == str(path))
    plan = lpr3.preflight(archive, None)
    plan.update(run_id=run_id, advance_checkpoint=True)
    ctx = context(run_id)
    with resource.get_connection() as conn:
        with claim_run(conn, ctx):
            execute_statements(conn, lpr3.manifest_create_sql())
            for prefix in plan["missing"]:
                execute_statements(
                    conn, f"DROP TABLE IF EXISTS [ext].[{SOURCES_BY_PREFIX[prefix].ext_table}]"
                )
    return plan, ctx


def extract(resource, path, run_id):
    plan, ctx = prepare(resource, path, run_id)
    groups = []
    for prefixes in lpr3.GROUPS:
        with resource.get_connection() as conn:
            with owned_landing(conn, ctx.run_id):
                groups.append(lpr3.load_group(conn, ctx, plan, prefixes))
    with resource.get_connection() as conn:
        with owned_landing(conn, ctx.run_id):
            lpr3.write_manifest(conn, plan, groups)
    return plan


@pytest.fixture
def archive_db(request):
    resource = request.getfixturevalue("lpr3_db")

    def cleanup():
        with resource.get_connection() as conn:
            cursor = conn.cursor()
            for dataset in ARCHIVE_DATASETS:
                cursor.execute(f"DROP VIEW IF EXISTS [ext].[v_{dataset.arc_table}]")
                for schema, table in (
                    ("ext", identity_table(dataset.entity)),
                    ("ext", dataset.source_table),
                    ("arc", dataset.arc_table),
                ):
                    cursor.execute(f"DROP TABLE IF EXISTS [{schema}].[{table}]")
            cursor.execute("DROP TABLE IF EXISTS meta.LPR3Leverance")
            cursor.execute(
                "DELETE FROM meta.DFMState WHERE (schema_name='arc' AND table_name LIKE 'lpr3[_]%') OR (schema_name='dfm' AND table_name='lpr3_leverance_1083')"
            )
            conn.commit()
            cursor.close()

    cleanup()
    execute_sql_files_from_directory(resource, "sources/lpr3")
    yield resource
    cleanup()

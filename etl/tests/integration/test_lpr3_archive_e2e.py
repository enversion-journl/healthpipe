# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""LPR3 typed archive DDL, exact-version FK resolution and shared SQL merge.

All source rows are synthetic. The reused fixture refuses destructive work
unless the target is the disposable devcontainer mssql/staging database.
"""

import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID

import dagster as dg
import pytest
from core.archive_merge import execute_archive_merge
from core.dfm_config import RunModeConfig
from DSI.csv_files._lpr3_keys import (
    framed_hash,
    identity_table,
    relational_identity_sql,
    resolved_view_sql,
    simple_identity_select,
)
from DSI.csv_files._sds_tar import execute_statements
from DSI.csv_files.lpr3_archive import (
    _run_archive,
    assert_gate_complete,
    build_identity_map,
    build_merge_config,
    execute_safe,
    guard_resolved_rows,
    guard_soft_deletes,
    guard_source_counts,
    guard_source_index,
    missing_sources,
    precheck_soft_deletes,
    read_manifest,
    record_delivery,
    refresh_surrogates_sql,
    reload_alarm,
    scalar,
    source_index_sql,
)
from DSI.csv_files.lpr3_datasets import ARCHIVE_DATASETS, ARCHIVES_BY_TABLE, SOURCES_BY_PREFIX
from infrastructure.utils.ddl_loader import execute_sql_file
from tests.fixtures.lpr3 import build_delivery, extract, source_row

pytestmark = pytest.mark.integration


def prepare_source(conn, dataset):
    targets = {
        dataset.entity,
        *(r.target_entity for r in dataset.references if not r.missing_dimension),
    }
    for entity in sorted(targets):
        target = ARCHIVES_BY_TABLE[f"lpr3_{entity}"]
        name = identity_table(entity)
        if target.canonical_versions:
            build_identity_map(conn, target)
            continue
        execute_statements(
            conn,
            f"DROP TABLE IF EXISTS [ext].[{name}]; SELECT * INTO [ext].[{name}] FROM ({simple_identity_select(target)}) d; CREATE UNIQUE CLUSTERED INDEX [UX_{name}] ON [ext].[{name}] ([source_sk]);",
        )
    execute_statements(conn, resolved_view_sql(dataset))
    execute_statements(
        conn,
        f"DROP TABLE IF EXISTS [ext].[{dataset.source_table}]; SELECT * INTO [ext].[{dataset.source_table}] FROM [ext].[v_{dataset.arc_table}]",
    )


def test_all_38_archive_tables_ddl_is_idempotent_and_matches_spec(archive_db):
    execute_sql_file(archive_db, "tables/lpr3/00_archive.sql")
    with archive_db.get_connection() as conn:
        cursor = conn.cursor()
        for dataset in ARCHIVE_DATASETS:
            cursor.execute(
                "SELECT data_compression_desc FROM sys.partitions "
                "WHERE object_id=OBJECT_ID(?) AND index_id=1",
                (f"arc.{dataset.arc_table}",),
            )
            assert cursor.fetchone()[0] == "PAGE"
            cursor.execute(
                "SELECT name FROM sys.columns WHERE object_id=OBJECT_ID(?) ORDER BY column_id",
                (f"arc.{dataset.arc_table}",),
            )
            names = [row[0] for row in cursor.fetchall()]
            assert names == [
                *dataset.arc_columns,
                "kadence",
                "leverance_tidsstempel",
                "leverance_dato",
                "dw_is_deleted",
                "dw_timestamp_insert",
                "dw_timestamp_update",
            ]
            cursor.execute(
                "SELECT c.name FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id WHERE i.object_id=OBJECT_ID(?) AND i.is_primary_key=1 ORDER BY ic.key_ordinal",
                (f"arc.{dataset.arc_table}",),
            )
            assert tuple(row[0] for row in cursor.fetchall()) == dataset.merge_keys
        cursor.close()


@pytest.mark.parametrize(
    "entity,ek",
    [
        ("kontaktlokation", "DW_EK_KontaktLokation"),
        ("betalingsoplysning", "DW_EK_Betalingsoplysning"),
    ],
)
def test_binary_id_survives_source_rekey_and_preserves_typed_relationships(
    archive_db, tmp_path, entity, ek
):
    dataset = ARCHIVES_BY_TABLE[f"lpr3_{entity}"]
    extract(archive_db, build_delivery(tmp_path), "archive-test")
    with archive_db.get_connection() as conn:
        manifest = read_manifest(conn, "archive-test")
        assert not missing_sources(dataset, manifest)
        guard_source_counts(conn, dataset, manifest)
        prepare_source(conn, dataset)
        assert guard_resolved_rows(conn, dataset, manifest) == 1
        execute_statements(conn, source_index_sql(dataset))
        guard_source_index(conn, dataset)
        assert guard_soft_deletes(conn, dataset) == (0, 0)
    config = build_merge_config(dataset)
    run = RunModeConfig(advance_checkpoint=False)
    first = execute_archive_merge(dg.build_asset_context(), archive_db, config, run)
    assert (first.rows_inserted, first.rows_updated, first.rows_soft_deleted) == (1, 0, 0)
    with archive_db.get_connection() as conn:
        assert (
            scalar(conn, f"SELECT DATALENGTH([{entity}_id]) FROM arc.[{dataset.arc_table}]") == 16
        )
        assert (
            scalar(
                conn, f"SELECT CONVERT(NVARCHAR(36),kontakt_uuid) FROM arc.[{dataset.arc_table}]"
            )
            == "00000000-0000-4000-8000-000000000001"
        )
        before = scalar(conn, f"SELECT dw_timestamp_update FROM arc.[{dataset.arc_table}]")
        execute_statements(
            conn,
            f"UPDATE ext.[{dataset.source_table}] SET [{ek}]=999,DW_ID_Audit_Insert=999",
        )
    rekey = execute_archive_merge(dg.build_asset_context(), archive_db, config, run)
    assert (rekey.rows_inserted, rekey.rows_updated, rekey.rows_soft_deleted) == (0, 0, 0)
    with archive_db.get_connection() as conn:
        execute_statements(conn, refresh_surrogates_sql(dataset))
        assert scalar(conn, f"SELECT [{ek}] FROM arc.[{dataset.arc_table}]") == 999
        assert scalar(conn, f"SELECT dw_timestamp_update FROM arc.[{dataset.arc_table}]") == before
        execute_statements(conn, f"UPDATE ext.[{dataset.source_table}] SET DW_IsIndirectError=0")
    changed = execute_archive_merge(dg.build_asset_context(), archive_db, config, run)
    assert changed.rows_updated == 1


def test_short_varbinary_source_id_survives_extract_and_archive_without_padding(
    archive_db, tmp_path
):
    source = SOURCES_BY_PREFIX["DimResultatIndberetning"]
    dataset = ARCHIVES_BY_TABLE["lpr3_resultatindberetning"]
    value = "0123456789ab"  # Synthetic six-byte ID, like the real source's short shape.
    path = build_delivery(
        tmp_path, rows={source.source_prefix: [source_row(source, ResultatIndberetning_ID=value)]}
    )
    extract(archive_db, path, "short-binary")
    with archive_db.get_connection() as conn:
        assert (
            scalar(conn, "SELECT ResultatIndberetning_ID FROM ext.lpr3_dimresultatindberetning")
            == value
        )
        prepare_source(conn, dataset)
    result = execute_archive_merge(
        dg.build_asset_context(),
        archive_db,
        build_merge_config(dataset),
        RunModeConfig(advance_checkpoint=False),
    )
    assert result.rows_inserted == 1
    with archive_db.get_connection() as conn:
        assert (
            scalar(
                conn,
                "SELECT DATALENGTH(ResultatIndberetning_ID) FROM arc.lpr3_resultatindberetning",
            )
            == 6
        )
        assert (
            scalar(
                conn,
                "SELECT CONVERT(VARCHAR(32),ResultatIndberetning_ID,2) FROM arc.lpr3_resultatindberetning",
            ).lower()
            == value
        )


@pytest.mark.parametrize(
    "entity,field",
    [
        ("forloebegenskaber", "TekniskIndberetter"),
        ("forloebegenskaber", "ForloebLabel_Tekst"),
        ("markoer", "Markoer_Tekst"),
    ],
)
def test_small_content_dimensions_keep_same_code_versions_with_different_business_fields(
    archive_db, tmp_path, entity, field
):
    dataset = ARCHIVES_BY_TABLE[f"lpr3_{entity}"]
    source = SOURCES_BY_PREFIX[dataset.sources[0]]
    sk = next(c for c in source.source_columns if c.startswith("DW_SK_"))
    ek = next(c for c in source.source_columns if c.startswith("DW_EK_"))
    rows = [source_row(source, **{sk: str(n), ek: str(n), field: f"Variant {n}"}) for n in (1, 2)]
    extract(
        archive_db, build_delivery(tmp_path, rows={source.source_prefix: rows}), "content-grain"
    )
    key = dataset.merge_keys[0]
    table = identity_table(entity)
    with archive_db.get_connection() as conn:
        assert build_identity_map(conn, dataset) == 2
        assert scalar(conn, f"SELECT COUNT(DISTINCT [{key}]) FROM ext.[{table}]") == 2
        prepare_source(conn, dataset)
    result = execute_archive_merge(
        dg.build_asset_context(), archive_db, build_merge_config(dataset), RunModeConfig()
    )
    assert result.rows_inserted == 2
    with archive_db.get_connection() as conn:
        # A source rekey cannot change these content identities.
        execute_statements(
            conn,
            f"UPDATE ext.[{source.ext_table}] SET [{sk}]=CONVERT(BIGINT,[{sk}])+100,"
            f"[{ek}]=CONVERT(BIGINT,[{ek}])+100",
        )
        assert build_identity_map(conn, dataset) == 2
        assert (
            scalar(
                conn,
                f"SELECT COUNT(*) FROM ext.[{table}] i JOIN arc.[{dataset.arc_table}] a "
                f"ON i.[{key}]=a.[{key}]",
            )
            == 2
        )


def test_canonical_properties_preserve_both_source_references_and_replay(archive_db, tmp_path):
    dimension = SOURCES_BY_PREFIX["DimKontaktEgenskaber"]
    dataset = ARCHIVES_BY_TABLE["lpr3_kontaktegenskaber"]
    dim_rows = [
        source_row(dimension),
        source_row(dimension, DW_SK_KontaktEgenskaber="2", DW_EK_KontaktEgenskaber="2"),
    ]
    contacts = SOURCES_BY_PREFIX["DimKontakt"]
    facts = SOURCES_BY_PREFIX["fctKontakt"]
    rows = {
        dimension.source_prefix: dim_rows,
        contacts.source_prefix: [source_row(contacts, n) for n in (1, 2)],
        facts.source_prefix: [
            source_row(facts, DW_SK_Kontakt=str(n), DW_SK_KontaktEgenskaber=str(n)) for n in (1, 2)
        ],
    }
    extract(archive_db, build_delivery(tmp_path, rows=rows), "canonical-test")
    with archive_db.get_connection() as conn:
        prepare_source(conn, dataset)
        manifest = read_manifest(conn, "canonical-test")
        assert guard_resolved_rows(conn, dataset, manifest) == 2
        assert scalar(conn, f"SELECT COUNT(*) FROM ext.[{dataset.source_table}]") == 1
        assert scalar(conn, f"SELECT antal_kilderaekker FROM ext.[{dataset.source_table}]") == 2
        provenance = json.loads(
            scalar(conn, f"SELECT kildeidentiteter FROM ext.[{dataset.source_table}]")
        )
        assert provenance == [
            {"DW_EK_KontaktEgenskaber": 1},
            {"DW_EK_KontaktEgenskaber": 2},
        ]
        # Build every lookup required by the real contact view, including groups.
        contact = ARCHIVES_BY_TABLE["lpr3_kontakt"]
        for entity in {"kontakt", *(r.target_entity for r in contact.references)}:
            build_identity_map(conn, ARCHIVES_BY_TABLE[f"lpr3_{entity}"])
        execute_statements(conn, resolved_view_sql(contact))
        assert scalar(conn, "SELECT COUNT_BIG(*) FROM ext.v_lpr3_kontakt") == 2
        assert (
            scalar(
                conn,
                "SELECT COUNT(DISTINCT kontaktegenskaber_noegle) FROM ext.v_lpr3_kontakt",
            )
            == 1
        )
        assert (
            scalar(
                conn,
                "SELECT COUNT(*) FROM ext.v_lpr3_kontakt WHERE kontaktegenskaber_noegle IS NULL",
            )
            == 0
        )
    config = build_merge_config(dataset)
    for expected in (1, 0):
        result = execute_archive_merge(
            dg.build_asset_context(), archive_db, config, RunModeConfig()
        )
        assert (result.rows_inserted, result.rows_updated, result.rows_soft_deleted) == (
            expected,
            0,
            0,
        )
    with archive_db.get_connection() as conn:
        key = scalar(conn, "SELECT kontaktegenskaber_noegle FROM arc.lpr3_kontaktegenskaber")
        before = scalar(conn, "SELECT dw_timestamp_update FROM arc.lpr3_kontaktegenskaber")
        execute_statements(
            conn,
            "UPDATE ext.lpr3_dimkontaktegenskaber SET "
            "DW_SK_KontaktEgenskaber=CONVERT(BIGINT,DW_SK_KontaktEgenskaber)+100,"
            "DW_EK_KontaktEgenskaber=CONVERT(BIGINT,DW_EK_KontaktEgenskaber)+100",
        )
        prepare_source(conn, dataset)
    rekey = execute_archive_merge(dg.build_asset_context(), archive_db, config, RunModeConfig())
    assert (rekey.rows_inserted, rekey.rows_updated, rekey.rows_soft_deleted) == (0, 0, 0)
    with archive_db.get_connection() as conn:
        execute_statements(conn, refresh_surrogates_sql(dataset))
        assert (
            scalar(conn, "SELECT kontaktegenskaber_noegle FROM arc.lpr3_kontaktegenskaber") == key
        )
        assert scalar(conn, "SELECT dw_timestamp_update FROM arc.lpr3_kontaktegenskaber") == before
        assert json.loads(
            scalar(conn, "SELECT kildeidentiteter FROM arc.lpr3_kontaktegenskaber")
        ) == [{"DW_EK_KontaktEgenskaber": 101}, {"DW_EK_KontaktEgenskaber": 102}]
        # Removing one duplicate must not delete the surviving dimension identity.
        execute_statements(
            conn, "DELETE FROM ext.lpr3_dimkontaktegenskaber WHERE DW_SK_KontaktEgenskaber='102'"
        )
        prepare_source(conn, dataset)
    delta = execute_archive_merge(dg.build_asset_context(), archive_db, config, RunModeConfig())
    assert (delta.rows_inserted, delta.rows_updated, delta.rows_soft_deleted) == (0, 1, 0)
    with archive_db.get_connection() as conn:
        execute_statements(conn, refresh_surrogates_sql(dataset))
        assert scalar(conn, "SELECT antal_kilderaekker FROM arc.lpr3_kontaktegenskaber") == 1
        assert scalar(conn, "SELECT kildeidentiteter FROM arc.lpr3_kontaktegenskaber") is None


@pytest.mark.parametrize(
    "column,value",
    [("DW_ValidTo", "2027-01-01 00:00:00"), ("DW_IsCurrent", "0"), ("DW_ID_Audit_Insert", "2")],
)
def test_canonical_properties_refuse_different_validity_flags_or_audit(
    archive_db, tmp_path, column, value
):
    source = SOURCES_BY_PREFIX["DimKontaktEgenskaber"]
    dataset = ARCHIVES_BY_TABLE["lpr3_kontaktegenskaber"]
    rows = [
        source_row(source),
        source_row(
            source,
            DW_SK_KontaktEgenskaber="2",
            DW_EK_KontaktEgenskaber="2",
            **{column: value},
        ),
    ]
    extract(archive_db, build_delivery(tmp_path, rows={source.source_prefix: rows}), "conflict")
    with archive_db.get_connection() as conn:
        with pytest.raises(ValueError, match="nonidentical canonical versions"):
            build_identity_map(conn, dataset)
        assert scalar(conn, f"SELECT COUNT(*) FROM arc.[{dataset.arc_table}]") == 0


def test_duplicate_pair_key_and_stale_manifest_fail_before_archive(archive_db, tmp_path):
    dataset = ARCHIVES_BY_TABLE["lpr3_kontaktlokation"]
    extract(archive_db, build_delivery(tmp_path), "archive-test")
    with archive_db.get_connection() as conn:
        with pytest.raises(ValueError, match="stale"):
            read_manifest(conn, "other-run")
        manifest = read_manifest(conn, "archive-test")
        execute_statements(conn, "UPDATE ext.lpr3_fctkontaktlokation SET DW_SK_KontaktLokation='2'")
        with pytest.raises(ValueError, match="parity"):
            guard_source_counts(conn, dataset, manifest)
        assert scalar(conn, "SELECT COUNT_BIG(*) FROM arc.lpr3_kontaktlokation") == 0


@pytest.mark.parametrize("entity,allowed", [("kontakt", 5), ("markoerregistrering", 30)])
def test_soft_delete_ratio_checks_expected_rows_before_any_write(archive_db, entity, allowed):
    dataset = ARCHIVES_BY_TABLE[f"lpr3_{entity}"]
    key = dataset.merge_keys[0]
    with archive_db.get_connection() as conn:
        execute_statements(
            conn,
            f"""WITH n AS (SELECT TOP (1000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects)
INSERT INTO arc.[{dataset.arc_table}] ([{key}],kadence,leverance_tidsstempel,leverance_dato)
SELECT CONVERT(UNIQUEIDENTIFIER,CONVERT(BINARY(16),n)),N'M','2026-09-15','2026-09-10' FROM n;
SELECT TOP ({999-allowed}) [{key}] INTO ext.[{dataset.source_table}] FROM arc.[{dataset.arc_table}];""",
        )
        with pytest.raises(dg.Failure, match="exceeds threshold"):
            guard_soft_deletes(conn, dataset)
        assert (
            scalar(
                conn, f"SELECT COUNT_BIG(*) FROM arc.[{dataset.arc_table}] WHERE dw_is_deleted=1"
            )
            == 0
        )
        execute_statements(
            conn,
            f"INSERT INTO ext.[{dataset.source_table}] SELECT TOP (1) [{key}] FROM arc.[{dataset.arc_table}] a WHERE NOT EXISTS (SELECT 1 FROM ext.[{dataset.source_table}] s WHERE s.[{key}]=a.[{key}])",
        )
        assert guard_soft_deletes(conn, dataset) == (1000, allowed)
        execute_statements(conn, f"TRUNCATE TABLE ext.[{dataset.source_table}]")
        with pytest.raises(dg.Failure, match="exceeds threshold"):
            guard_soft_deletes(conn, dataset, override=999)
        assert guard_soft_deletes(conn, dataset, override=1000) == (1000, 1000)


def test_soft_delete_precheck_fails_before_any_merge_unless_overridden(archive_db):
    dataset = ARCHIVES_BY_TABLE["lpr3_kontakt"]
    identities = f"ext.[{identity_table(dataset.entity)}]"
    context = SimpleNamespace(log=SimpleNamespace(info=lambda message: None))
    with archive_db.get_connection() as conn:
        execute_statements(
            conn,
            f"""WITH n AS (SELECT TOP (1000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects)
INSERT INTO arc.[{dataset.arc_table}] (kontakt_uuid,kadence,leverance_tidsstempel,leverance_dato)
SELECT CONVERT(UNIQUEIDENTIFIER,CONVERT(BINARY(16),n)),N'U','2026-09-22','2026-09-20' FROM n;
DROP TABLE IF EXISTS {identities};
SELECT TOP (994) CONVERT(BIGINT,1) source_sk,kontakt_uuid INTO {identities} FROM arc.[{dataset.arc_table}];""",
        )
        with pytest.raises(dg.Failure, match="before any archive merge") as failure:
            precheck_soft_deletes(conn, context, {"kontakt": 994}, {})
        assert f"arc_{dataset.arc_table}: 6 expected soft-deletes" in failure.value.description
        precheck_soft_deletes(conn, context, {"kontakt": 994}, {dataset.arc_table: 6})
        assert (
            scalar(
                conn, f"SELECT COUNT_BIG(*) FROM arc.[{dataset.arc_table}] WHERE dw_is_deleted=1"
            )
            == 0
        )


def test_empty_nullable_reference_never_resolves_to_source_key_zero(archive_db, tmp_path):
    dataset = ARCHIVES_BY_TABLE["lpr3_procedureregistrering"]
    rows = {
        "DimDataKilde": [source_row(SOURCES_BY_PREFIX["DimDataKilde"], 0)],
        "fctProcedureRegistrering": [
            source_row(SOURCES_BY_PREFIX["fctProcedureRegistrering"], DW_SK_Datakilde="")
        ],
    }
    extract(archive_db, build_delivery(tmp_path, rows=rows), "archive-test")
    with archive_db.get_connection() as conn:
        prepare_source(conn, dataset)
        assert scalar(conn, f"SELECT datakilde_noegle FROM ext.[{dataset.source_table}]") is None


def test_hash_framing_distinguishes_null_empty_delimiters_and_long_unicode(archive_db):
    expressions = [
        ["N'a|b'", "N'c'"],
        ["N'a'", "N'b|c'"],
        ["NULL", "N'x'"],
        ["N''", "N'x'"],
        ["REPLICATE(CAST(N'æ' AS NVARCHAR(MAX)),5000)+N'x'"],
        ["REPLICATE(CAST(N'æ' AS NVARCHAR(MAX)),5000)+N'y'"],
    ]
    with archive_db.get_connection() as conn:
        values = [
            scalar(conn, "SELECT " + framed_hash("lpr3:test:v1", components))
            for components in expressions
        ]
    assert len(set(values)) == len(expressions)
    assert all(len(value) == 32 for value in values)


@pytest.mark.parametrize("entity", ["helbredsforloeb", "sygehusophold"])
def test_relational_fallback_matches_measured_python_hash_and_preserves_all_boundaries(
    archive_db, tmp_path, entity
):
    stay = entity == "sygehusophold"
    member = "Kontakt" if stay else "Forloeb"
    own = "Sygehusophold" if stay else "HelbredsForloeb"
    fact = "FctSygehusophold" if stay else "FctHelbredsforloeb"
    bridge = "FctBridge_Sygehusophold" if stay else "FctbridgeForloebsRelation"
    rows = {
        f"Dim{member}": [
            source_row(SOURCES_BY_PREFIX[f"Dim{member}"], n) for n in (10, 11, 12, 13)
        ],
        f"Dim{own}": [source_row(SOURCES_BY_PREFIX[f"Dim{own}"], n) for n in (1, 2, 3, 4)],
        fact: [
            source_row(
                SOURCES_BY_PREFIX[fact], n, **{f"DW_SK_{member}_Foerste": "10" if n == 1 else "999"}
            )
            for n in (1, 2, 3, 4)
        ],
        bridge: [],
    }
    for own_id, member_id in [(1, 10), (2, 11), (3, 12), (3, 13), (4, 10)]:
        overrides = {
            ("DW_SK_Sygehusophold" if stay else "DW_SK_Helbredsforloeb"): str(own_id),
            f"DW_SK_{member}": str(member_id),
        }
        if not stay:
            overrides["DW_SK_Forloeb_Forrige"] = "-1" if own_id == 1 else "999"
        rows[bridge].append(source_row(SOURCES_BY_PREFIX[bridge], **overrides))
    extract(archive_db, build_delivery(tmp_path, rows=rows), "archive-test")
    dataset = ARCHIVES_BY_TABLE[f"lpr3_{entity}"]
    with archive_db.get_connection() as conn:
        target = ARCHIVES_BY_TABLE[f"lpr3_{member.lower()}"]
        execute_statements(
            conn,
            f"SELECT * INTO ext.[{identity_table(member.lower())}] FROM ({simple_identity_select(target)}) d",
        )
        cursor = conn.cursor()
        try:
            for sql in relational_identity_sql(dataset):
                cursor.execute(sql)
            conn.commit()
            cursor.execute(
                f"SELECT source_sk,[{entity}_noegle],[{entity}_uuid],noeglemetode FROM ext.[{identity_table(entity)}] ORDER BY source_sk"
            )
            actual = cursor.fetchall()
        finally:
            cursor.close()

    def digest(n):
        return hashlib.sha256(UUID(f"00000000-0000-4000-8000-{n:012d}").bytes).digest()

    namespace = f"lpr3:{entity}:{'contact-set' if stay else 'boundary-set'}:v1:".encode()
    expected = [
        digest(10),
        digest(11),
        hashlib.sha256(namespace + b"".join(sorted((digest(12), digest(13))))).digest(),
        hashlib.sha256(namespace + digest(10)).digest(),
    ]
    assert [row[1] for row in actual] == expected
    assert [row[3] for row in actual] == [
        "first" if stay else "root",
        "single_contact" if stay else "visible_boundary",
        "contact_set" if stay else "boundary_set",
        "contact_set" if stay else "boundary_set",
    ]
    assert actual[2][2] is actual[3][2] is None
    with archive_db.get_connection() as conn:
        with pytest.raises(ValueError, match="UUID coverage below 99.9%"):
            build_identity_map(conn, dataset)


def test_time_business_minute_key_preserves_unknown_midnight_and_rekeyed_references(
    archive_db, tmp_path
):
    time_source = SOURCES_BY_PREFIX["DimTidspunkt"]
    time_dataset = ARCHIVES_BY_TABLE["lpr3_tidspunkt"]
    contact = ARCHIVES_BY_TABLE["lpr3_kontakt"]
    rows = {
        "DimTidspunkt": [
            source_row(time_source, DW_SK_Tidspunkt="-1", MinutID="-1", Tidspunkt=""),
            source_row(time_source, DW_SK_Tidspunkt="77", MinutID="0", Tidspunkt="00:00:00"),
            source_row(time_source, DW_SK_Tidspunkt="78", MinutID="1234", Tidspunkt="12:34:00"),
        ],
        "DimKontakt": [source_row(SOURCES_BY_PREFIX["DimKontakt"], n) for n in (1, 2, 3)],
        "fctKontakt": [
            source_row(SOURCES_BY_PREFIX["fctKontakt"], n, DW_SK_Tidspunkt_Start=sk)
            for n, sk in ((1, "-1"), (2, "77"), (3, "78"))
        ],
    }
    extract(archive_db, build_delivery(tmp_path, rows=rows), "archive-test")
    with archive_db.get_connection() as conn:
        build_identity_map(conn, time_dataset)
        prepare_source(conn, time_dataset)
        assert guard_resolved_rows(conn, time_dataset, read_manifest(conn, "archive-test")) == 3
    merged = execute_archive_merge(
        dg.build_asset_context(), archive_db, build_merge_config(time_dataset), RunModeConfig()
    )
    assert merged.rows_inserted == 3
    with archive_db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT tidspunkt_minut_id,Tidspunkt FROM arc.lpr3_tidspunkt ORDER BY tidspunkt_minut_id"
        )
        actual = cursor.fetchall()
        assert [(r[0], None if r[1] is None else str(r[1])) for r in actual] == [
            (-1, None),
            (0, "00:00:00"),
            (1234, "12:34:00"),
        ]
        for rekey in (False, True):
            if rekey:
                execute_statements(
                    conn,
                    f"UPDATE ext.[{time_source.ext_table}] SET DW_SK_Tidspunkt=N'999' WHERE DW_SK_Tidspunkt=N'78'; UPDATE ext.[{SOURCES_BY_PREFIX['fctKontakt'].ext_table}] SET DW_SK_Tidspunkt_Start=N'999' WHERE DW_SK_Tidspunkt_Start=N'78';",
                )
            prepare_source(conn, contact)
            cursor.execute(
                f"SELECT tidspunkt_start_minut_id FROM ext.[{contact.source_table}] ORDER BY kontakt_uuid"
            )
            assert [r[0] for r in cursor.fetchall()] == [None, 0, 1234]
        cursor.close()


def test_versioned_reference_uses_activity_sk_and_sentinel_resolves_null(archive_db, tmp_path):
    borger = SOURCES_BY_PREFIX["DimBorger"]
    rows = {
        "DimBorger": [
            source_row(
                borger,
                1,
                CPRnummer="SYNTHETIC",
                DW_EK_Borger="7",
                DW_IsCurrent="0",
                DW_ValidFrom="2025-01-01 00:00:00.0000000",
            ),
            source_row(
                borger,
                2,
                CPRnummer="SYNTHETIC",
                DW_EK_Borger="7",
                DW_IsCurrent="1",
                DW_ValidFrom="2026-01-01 00:00:00.0000000",
            ),
        ],
        "fctKontaktLokation": [
            source_row(SOURCES_BY_PREFIX["fctKontaktLokation"], DW_SK_Organisation_Ophold="-1")
        ],
    }
    extract(archive_db, build_delivery(tmp_path, rows=rows), "archive-test")
    with archive_db.get_connection() as conn:
        prepare_source(conn, ARCHIVES_BY_TABLE["lpr3_kontaktlokation"])
        assert (
            str(scalar(conn, "SELECT borger_gyldig_fra FROM ext.lpr3_kontaktlokation_noegler"))
            == "2025-01-01 00:00:00"
        )
        assert (
            scalar(conn, "SELECT organisation_ophold_noegle FROM ext.lpr3_kontaktlokation_noegler")
            is None
        )
        assert (
            scalar(
                conn, "SELECT organisation_ophold_gyldig_fra FROM ext.lpr3_kontaktlokation_noegler"
            )
            is None
        )


def test_gate_requires_all_sources_all_current_run_merges_and_records_reload_alarm(
    archive_db, tmp_path
):
    extract(archive_db, build_delivery(tmp_path), "archive-test")
    result = SimpleNamespace(row_count=1, rows_inserted=1, rows_updated=0, rows_soft_deleted=0)
    with archive_db.get_connection() as conn:
        manifest = read_manifest(conn, "archive-test")
        with pytest.raises(dg.Failure, match="not all 38"):
            assert_gate_complete(conn, manifest, "archive-test")
        for dataset in ARCHIVE_DATASETS:
            record_delivery(conn, dataset, manifest, result=result, full_load=True, seconds=0.001)
        assert_gate_complete(conn, manifest, "archive-test")
        # Merely starting a replay cannot downgrade a successfully archived
        # delivery or lose its cutoff protection if that replay then fails.
        replay_manifest = {p: replace(d, run_id="failed-replay") for p, d in manifest.items()}
        for dataset in ARCHIVE_DATASETS:
            record_delivery(conn, dataset, replay_manifest)
        assert_gate_complete(conn, manifest, "archive-test")
        with pytest.raises(dg.Failure, match="not all 38"):
            assert_gate_complete(conn, replay_manifest, "failed-replay")
        with pytest.raises(dg.Failure, match="not all 38"):
            assert_gate_complete(conn, manifest, "different-run")
        incomplete = {key: item for key, item in manifest.items() if key != "DimAdresse"}
        with pytest.raises(dg.Failure, match="missing source"):
            assert_gate_complete(conn, incomplete, "archive-test")
        previous = manifest["DimKontakt"]
        later = replace(
            previous, timestamp=previous.timestamp.replace(day=16), audit_min=0, audit_distinct=0
        )
        assert reload_alarm(conn, later)
        later = replace(later, audit_min=previous.audit_min, audit_distinct=previous.audit_distinct)
        assert not reload_alarm(conn, later)
        assert (
            scalar(
                conn,
                "SELECT COUNT_BIG(*) FROM meta.LPR3Leverance WHERE status='archived' AND merge_metode='bulk'",
            )
            == 47
        )


def test_archive_lifecycle_records_commit_and_replay_and_removes_work_copy(archive_db, tmp_path):
    dataset = ARCHIVES_BY_TABLE["lpr3_kontaktlokation"]
    with dg.build_asset_context() as context:
        extract(archive_db, build_delivery(tmp_path), context.run_id)
        with archive_db.get_connection() as conn:
            prepare_source(conn, dataset)
        first = _run_archive(context, archive_db, dataset)
        second = _run_archive(context, archive_db, dataset)
    assert first.metadata["rows_inserted"] == 1
    assert (
        second.metadata["rows_inserted"],
        second.metadata["rows_updated"],
        second.metadata["rows_soft_deleted"],
    ) == (0, 0, 0)
    with archive_db.get_connection() as conn:
        assert scalar(conn, f"SELECT OBJECT_ID(N'ext.{dataset.source_table}',N'U')") is None
        assert (
            scalar(
                conn,
                "SELECT COUNT_BIG(*) FROM meta.LPR3Leverance WHERE status='archived' AND arc_tabel='lpr3_kontaktlokation' AND referencer_uoploest=0 AND raekker_arc=1",
            )
            == 2
        )


@pytest.mark.parametrize("stem", ["Diagnose", "Procedure"])
def test_group_fallback_preserves_unresolved_and_colliding_complete_groups(
    archive_db, tmp_path, stem
):
    entity = stem.lower() + "gruppe"
    dim = SOURCES_BY_PREFIX[f"Dim{stem}Gruppe"]
    bridge = SOURCES_BY_PREFIX[f"fctBridge{stem}"]
    rows = {
        dim.source_prefix: [source_row(dim, n) for n in (1, 2, 3)],
        bridge.source_prefix: [
            source_row(bridge, **{f"DW_SK_{stem}_Parent": "-1"}),
            source_row(
                bridge,
                **{
                    f"DW_SK_{stem}gruppe": "3",
                    f"DW_SK_{stem}": "999",
                    f"DW_SK_{stem}_Parent": "-1",
                },
            ),
        ],
    }
    extract(archive_db, build_delivery(tmp_path, rows=rows), "groups-test")
    dataset = ARCHIVES_BY_TABLE[f"lpr3_{entity}"]
    with archive_db.get_connection() as conn:
        assert build_identity_map(conn, dataset) == 3
        table = f"[ext].[{identity_table(entity)}]"
        original = scalar(conn, f"SELECT [{entity}_noegle] FROM {table} WHERE source_sk=1")
        assert (
            scalar(conn, f"SELECT COUNT(*) FROM {table} WHERE noeglemetode='technical_unresolved'")
            == 2
        )
        # Renumbering a resolved group leaves the code-based key unchanged.
        execute_statements(
            conn,
            f"UPDATE ext.[{dim.ext_table}] SET [DW_EK_{stem}gruppe]=100 WHERE [DW_SK_{stem}gruppe]='1'",
        )
        build_identity_map(conn, dataset)
        assert scalar(conn, f"SELECT [{entity}_noegle] FROM {table} WHERE source_sk=1") == original
        # Equal membership with different dimension audits must remain separate.
        execute_statements(
            conn,
            f"UPDATE ext.[{bridge.ext_table}] SET [DW_SK_{stem}]='1' WHERE [DW_SK_{stem}gruppe]='3'",
        )
        assert build_identity_map(conn, dataset) == 3
        assert (
            scalar(conn, f"SELECT COUNT(*) FROM {table} WHERE noeglemetode='technical_collision'")
            == 2
        )
        assert scalar(conn, f"SELECT COUNT(DISTINCT [{entity}_noegle]) FROM {table}") == 3
        collision_key = scalar(conn, f"SELECT [{entity}_noegle] FROM {table} WHERE source_sk=1")
        assert collision_key != original
        build_identity_map(conn, dataset)
        assert (
            scalar(conn, f"SELECT [{entity}_noegle] FROM {table} WHERE source_sk=1")
            == collision_key
        )


@pytest.mark.parametrize("stem", ["Diagnose", "Procedure"])
def test_identical_complete_groups_share_identity_and_weighted_bridge_without_losing_versions(
    archive_db, tmp_path, stem
):
    dim = SOURCES_BY_PREFIX[f"Dim{stem}Gruppe"]
    code = SOURCES_BY_PREFIX[f"Dim{stem}"]
    bridge = SOURCES_BY_PREFIX[f"fctBridge{stem}"]
    entity = stem.lower() + "gruppe"
    group_sk = f"DW_SK_{stem}gruppe"
    dataset = ARCHIVES_BY_TABLE[f"lpr3_{entity}"]
    rows = {
        dim.source_prefix: [
            source_row(dim, **{group_sk: str(n), f"DW_EK_{stem}gruppe": str(n)}) for n in (1, 2)
        ],
        code.source_prefix: [
            source_row(code),
            source_row(code, **{f"DW_SK_{stem}": "2", "DW_ValidFrom": "2025-01-01 00:00:00"}),
        ],
        bridge.source_prefix: [
            source_row(bridge, **{group_sk: str(n), f"DW_SK_{stem}_Parent": "-1"}) for n in (1, 2)
        ],
    }
    extract(archive_db, build_delivery(tmp_path, rows=rows), "canonical-groups")
    with archive_db.get_connection() as conn:
        build_identity_map(conn, ARCHIVES_BY_TABLE[f"lpr3_{stem.lower()}"])
        assert build_identity_map(conn, dataset) == 2
        table = identity_table(entity)
        assert scalar(conn, f"SELECT COUNT(DISTINCT [{entity}_noegle]) FROM ext.[{table}]") == 1
        original = scalar(conn, f"SELECT [{entity}_noegle] FROM ext.[{table}] WHERE source_sk=1")
        manifest = read_manifest(conn, "canonical-groups")
        for target in (dataset, ARCHIVES_BY_TABLE[f"lpr3_bridge_{stem.lower()}"]):
            execute_statements(conn, resolved_view_sql(target))
            execute_statements(
                conn, f"SELECT * INTO ext.[{target.source_table}] FROM ext.[v_{target.arc_table}]"
            )
            assert guard_resolved_rows(conn, target, manifest) == 2
            assert scalar(conn, f"SELECT COUNT(*) FROM ext.[{target.source_table}]") == 1
            assert scalar(conn, f"SELECT antal_kilderaekker FROM ext.[{target.source_table}]") == 2
        # Same code at a different source version is not identical membership.
        execute_statements(
            conn,
            f"UPDATE ext.[{bridge.ext_table}] SET [DW_SK_{stem}]='2' WHERE [{group_sk}]='2'",
        )
        build_identity_map(conn, dataset)
        assert scalar(conn, f"SELECT COUNT(DISTINCT [{entity}_noegle]) FROM ext.[{table}]") == 2
        assert (
            scalar(conn, f"SELECT [{entity}_noegle] FROM ext.[{table}] WHERE source_sk=1")
            == original
        )
        # Identical sets with different source multiplicity are not identical
        # complete graphs. The conservative technical-collision fallback must still apply.
        execute_statements(
            conn,
            f"UPDATE ext.[{bridge.ext_table}] SET [DW_SK_{stem}]='1'; "
            f"INSERT INTO ext.[{bridge.ext_table}] SELECT * FROM ext.[{bridge.ext_table}] WHERE [{group_sk}]='2'",
        )
        build_identity_map(conn, dataset)
        assert (
            scalar(
                conn, f"SELECT COUNT(*) FROM ext.[{table}] WHERE noeglemetode='technical_collision'"
            )
            == 2
        )


def test_collapsed_bridge_audit_counts_all_unresolved_source_references(archive_db, tmp_path):
    bridge = SOURCES_BY_PREFIX["fctBridgeProcedure"]
    row = source_row(bridge, DW_SK_Procedure="999", DW_SK_Procedure_Parent="-1")
    dataset = ARCHIVES_BY_TABLE["lpr3_bridge_procedure"]
    with dg.build_asset_context() as context:
        extract(
            archive_db,
            build_delivery(tmp_path, rows={bridge.source_prefix: [row, row, row]}),
            context.run_id,
        )
        with archive_db.get_connection() as conn:
            for entity in ("procedure", "proceduregruppe"):
                build_identity_map(conn, ARCHIVES_BY_TABLE[f"lpr3_{entity}"])
        result = _run_archive(context, archive_db, dataset)
    assert result.metadata["rows_inserted"] == 1
    assert result.metadata["source_rows_represented"] == 3
    assert result.metadata["unresolved_references"] == 3
    with archive_db.get_connection() as conn:
        assert scalar(conn, "SELECT antal_kilderaekker FROM arc.lpr3_bridge_procedure") == 3
        assert scalar(conn, "SELECT procedure_noegle FROM arc.lpr3_bridge_procedure") is None
        assert (
            scalar(
                conn,
                "SELECT referencer_uoploest FROM meta.LPR3Leverance WHERE arc_tabel='lpr3_bridge_procedure'",
            )
            == 3
        )


def test_procedure_bridge_collapses_only_identical_source_rows_and_reconciles_total(
    archive_db, tmp_path
):
    bridge = SOURCES_BY_PREFIX["fctBridgeProcedure"]
    row = source_row(bridge, DW_SK_Procedure_Parent="-1")
    extract(
        archive_db,
        build_delivery(tmp_path, rows={bridge.source_prefix: [row, row, row]}),
        "bridge-test",
    )
    dataset = ARCHIVES_BY_TABLE["lpr3_bridge_procedure"]
    with archive_db.get_connection() as conn:
        for entity in ("procedure", "proceduregruppe"):
            build_identity_map(conn, ARCHIVES_BY_TABLE[f"lpr3_{entity}"])
        manifest = read_manifest(conn, "bridge-test")
        execute_statements(conn, resolved_view_sql(dataset))
        execute_statements(
            conn, f"SELECT * INTO ext.[{dataset.source_table}] FROM ext.[v_{dataset.arc_table}]"
        )
        assert guard_resolved_rows(conn, dataset, manifest) == 3
        assert scalar(conn, f"SELECT COUNT(*) FROM ext.[{dataset.source_table}]") == 1
        assert scalar(conn, f"SELECT antal_kilderaekker FROM ext.[{dataset.source_table}]") == 3
        # Even an audit-only difference is not byte-identical. Its duplicate
        # business key must fail rather than silently discarding source data.
        execute_statements(
            conn,
            f"UPDATE TOP (1) ext.[{bridge.ext_table}] SET DW_ID_Audit_Insert='2'; DROP TABLE ext.[{dataset.source_table}]; SELECT * INTO ext.[{dataset.source_table}] FROM ext.[v_{dataset.arc_table}]",
        )
        with pytest.raises(ValueError, match="not unique/non-null"):
            guard_resolved_rows(conn, dataset, manifest)


@pytest.mark.parametrize(
    "stem,natural_column", [("Borger", "CPRnummer"), ("Organisation", "SORKode")]
)
def test_ambiguous_natural_identity_preserves_entities_and_all_history(
    archive_db, tmp_path, stem, natural_column
):
    source = SOURCES_BY_PREFIX[f"Dim{stem}"]
    extra = {"Kodetype": "SOR"} if stem == "Organisation" else {}
    rows = [source_row(source, n, **extra) for n in (1, 2)]
    # A prior version of entity 1 has a different natural value: the exception
    # must follow that entity's entire supplied history, not just its collision.
    rows += [
        source_row(
            source,
            3,
            **extra,
            **{f"DW_EK_{stem}": "1", natural_column: "123", "DW_ValidFrom": "2025-01-01 00:00:00"},
        )
    ]
    rows += [source_row(source, 4, **extra, **{natural_column: "456"})]
    # Different ValidFrom versions can share the natural identity legitimately.
    rows += [
        source_row(
            source, 5, **extra, **{natural_column: "456", "DW_ValidFrom": "2024-01-01 00:00:00"}
        )
    ]
    extract(archive_db, build_delivery(tmp_path, rows={source.source_prefix: rows}), "natural-test")
    entity = stem.lower()
    dataset = ARCHIVES_BY_TABLE[f"lpr3_{entity}"]
    table = f"ext.[{identity_table(entity)}]"
    with archive_db.get_connection() as conn:
        assert build_identity_map(conn, dataset) == 5
        assert (
            scalar(conn, f"SELECT COUNT(*) FROM {table} WHERE noeglemetode='technical_collision'")
            == 3
        )
        assert (
            scalar(
                conn,
                f"SELECT COUNT(DISTINCT [{dataset.merge_keys[0]}]) FROM {table} WHERE source_sk IN (1,3)",
            )
            == 1
        )
        original = scalar(conn, f"SELECT [{dataset.merge_keys[0]}] FROM {table} WHERE source_sk=4")
        execute_statements(
            conn,
            f"UPDATE ext.[{source.ext_table}] SET [DW_EK_{stem}]='400' WHERE [DW_SK_{stem}]='4'",
        )
        build_identity_map(conn, dataset)
        assert (
            scalar(conn, f"SELECT [{dataset.merge_keys[0]}] FROM {table} WHERE source_sk=4")
            == original
        )
        for ref in dataset.references:
            if not ref.missing_dimension:
                build_identity_map(conn, ARCHIVES_BY_TABLE[f"lpr3_{ref.target_entity}"])
        execute_statements(conn, resolved_view_sql(dataset))
        assert scalar(conn, f"SELECT COUNT(*) FROM ext.[v_{dataset.arc_table}]") == 5


@pytest.mark.parametrize("stem", ["Diagnose", "Procedure"])
def test_national_bridge_missing_references_have_distinct_row_keys_and_null_clinical_fks(
    archive_db, tmp_path, stem
):
    bridge = SOURCES_BY_PREFIX[f"fctBridge{stem}"]
    rows = [
        source_row(bridge, **{f"DW_SK_{stem}": str(n), f"DW_SK_{stem}_Parent": "-1"})
        for n in (1, 999, 1000)
    ]
    extract(
        archive_db,
        build_delivery(tmp_path, rows={bridge.source_prefix: rows}),
        "missing-reference-test",
    )
    dataset = ARCHIVES_BY_TABLE[f"lpr3_bridge_{stem.lower()}"]
    with archive_db.get_connection() as conn:
        for entity in (stem.lower(), stem.lower() + "gruppe"):
            build_identity_map(conn, ARCHIVES_BY_TABLE[f"lpr3_{entity}"])
        execute_statements(conn, resolved_view_sql(dataset))
        execute_statements(
            conn, f"SELECT * INTO ext.[{dataset.source_table}] FROM ext.[v_{dataset.arc_table}]"
        )
        assert (
            guard_resolved_rows(conn, dataset, read_manifest(conn, "missing-reference-test")) == 3
        )
        table = f"ext.[{dataset.source_table}]"
        assert scalar(conn, f"SELECT COUNT(DISTINCT [{dataset.merge_keys[0]}]) FROM {table}") == 3
        assert (
            scalar(
                conn,
                f"SELECT COUNT(*) FROM {table} WHERE noeglemetode='technical_reference' AND [{stem.lower()}_noegle] IS NULL AND [{stem.lower()}_gyldig_fra] IS NULL",
            )
            == 2
        )
        assert scalar(conn, f"SELECT SUM(_lpr3_uoploeste_referencer) FROM {table}") == 2
        # Numeric spelling of a source ID must not change a technical row key.
        before = scalar(
            conn,
            f"SELECT CHECKSUM_AGG(BINARY_CHECKSUM([{dataset.merge_keys[0]}])) FROM ext.[v_{dataset.arc_table}]",
        )
        execute_statements(
            conn,
            f"UPDATE ext.[{bridge.ext_table}] SET [DW_SK_{stem}]='0999' WHERE [DW_SK_{stem}]='999'",
        )
        assert (
            scalar(
                conn,
                f"SELECT CHECKSUM_AGG(BINARY_CHECKSUM([{dataset.merge_keys[0]}])) FROM ext.[v_{dataset.arc_table}]",
            )
            == before
        )


def test_identity_batch_surfaces_deferred_sql_error_and_rolls_back(archive_db):
    with archive_db.get_connection() as conn:
        with pytest.raises(ValueError, match="SQL batch failed; source values suppressed"):
            execute_safe(
                conn,
                "CREATE TABLE #lpr3_atomic (value INT NOT NULL); INSERT INTO #lpr3_atomic VALUES (1); INSERT INTO #lpr3_atomic VALUES (NULL);",
            )
        assert scalar(conn, "SELECT OBJECT_ID('tempdb..#lpr3_atomic')") is None

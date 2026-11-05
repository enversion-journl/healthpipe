# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SOF-målebænk: varianter, kopiens layout og opsummering (rene funktioner)."""

from __future__ import annotations

import pytest
from adhoc import sof_merge_benchmark as b
from DSA.FHIR.utils.sql_on_fhir.sof_utils import SOF_DEFAULT_MERGE_BATCH_ROWS

pytestmark = pytest.mark.unit


def test_variants_map_to_plans():
    single_on = b.plan_for_variant("single_on", 250_000)
    assert single_on.active is False and single_on.batch_rows == 0 and not single_on.disable_indexes
    batched_off = b.plan_for_variant("batched_off", 100_000)
    assert batched_off.active and batched_off.batch_rows == 100_000 and batched_off.disable_indexes
    single_off = b.plan_for_variant("single_off", 100_000)
    assert single_off.active and single_off.batch_rows == 0 and single_off.disable_indexes
    assert b.VARIANTS["batched_on"] == (SOF_DEFAULT_MERGE_BATCH_ROWS, False)
    with pytest.raises(ValueError):
        b.validate_variants(["heap"])
    assert b.validate_variants(["single_on", "single_on", "batched_off"]) == [
        "single_on",
        "batched_off",
    ]


def test_copy_layout_recreates_pk_defaults_and_indexes_with_copy_scoped_names():
    index_rows = [
        ("PK_FHIR_QuestionnaireResponse", 1, True, True, "[qr_id] ASC", None),
        ("IX_FHIR_QR_PatientAuthored", 2, False, False, "[patient_id] ASC, [authored] ASC", None),
        (
            "IX_FHIR_QR_CanonicalVersion",
            2,
            False,
            False,
            "[questionnaire_canonical] ASC, [questionnaire_version] ASC",
            "[patient_id], [authored], [source_model_name]",
        ),
        ("UX_x", 2, True, False, "[a] ASC", None),
    ]
    default_rows = [("dw_is_deleted", "((0))"), ("dw_timestamp_insert", "(sysdatetime())")]
    computed_rows = [
        ("dw_timestamp_effective", "(isnull([dw_timestamp_update],[dw_timestamp_insert]))", True)
    ]
    statements = b.copy_layout_statements(
        "QuestionnaireResponse_bench", index_rows, default_rows, computed_rows
    )
    # Beregnede kolonner genskabes FØRST (SELECT INTO gjorde dem til almindelige kolonner,
    # som MERGE'n ellers skulle udfylde), før defaults og indeks.
    assert statements[0] == (
        "ALTER TABLE [fhir].[QuestionnaireResponse_bench] DROP COLUMN [dw_timestamp_effective]; "
        "ALTER TABLE [fhir].[QuestionnaireResponse_bench] ADD [dw_timestamp_effective] AS "
        "(isnull([dw_timestamp_update],[dw_timestamp_insert])) PERSISTED;"
    )
    assert statements[1] == (
        "ALTER TABLE [fhir].[QuestionnaireResponse_bench] ADD CONSTRAINT "
        "[DF_QuestionnaireResponse_bench_dw_is_deleted] DEFAULT ((0)) FOR [dw_is_deleted];"
    )
    assert (
        "ALTER TABLE [fhir].[QuestionnaireResponse_bench] ADD CONSTRAINT [PK_QuestionnaireResponse_bench] "
        "PRIMARY KEY CLUSTERED ([qr_id] ASC);" in statements
    )
    assert (
        "CREATE NONCLUSTERED INDEX [IX_FHIR_QR_CanonicalVersion] ON [fhir].[QuestionnaireResponse_bench] "
        "([questionnaire_canonical] ASC, [questionnaire_version] ASC) INCLUDE ([patient_id], [authored], [source_model_name]);"
        in statements
    )
    assert (
        "CREATE UNIQUE NONCLUSTERED INDEX [UX_x] ON [fhir].[QuestionnaireResponse_bench] ([a] ASC);"
        in statements
    )
    assert all(
        "PK_FHIR_QuestionnaireResponse" not in s for s in statements
    ), "måltabellens PK-navn må ikke genbruges"


def test_bench_table_name_and_summary():
    assert b.bench_table_name("QuestionnaireResponse") == "QuestionnaireResponse_bench"
    results = {
        "single_on": {
            "rows": 1_000_000,
            "wall_s": 900.0,
            "timings": {
                "fetch_s": 100,
                "stage_s": 20,
                "create_index_s": 5,
                "merge_s": 600.0,
                "merge_batches": 0,
                "index_disable_s": 0,
                "index_rebuild_s": 0,
            },
        }
    }
    table = b.summary_table(results)
    assert "| single_on | 1000000 | 120 | 5 | 600.0 | 0 | 0 | 0 | 900.0 | 0.60 |" in table


def test_copy_sql_supports_partial_copy():
    assert b.copy_sql("QuestionnaireResponse", "QuestionnaireResponse_bench") == (
        "SELECT * INTO [fhir].[QuestionnaireResponse_bench] FROM [fhir].[QuestionnaireResponse]"
    )
    assert b.copy_sql(
        "QuestionnaireResponse", "QuestionnaireResponse_bench", 12_000_000
    ).startswith("SELECT TOP (12000000) * INTO")


def test_preflight_scales_the_estimate_with_copy_rows():
    class Cur:
        def __init__(self):
            self.sql = []

        def execute(self, sql):
            self.sql.append(sql)

        def fetchone(self):
            if "dm_os_volume_stats" in self.sql[-1]:
                return (391.0,)
            return (96938.0, 37_376_631)

    log = type("L", (), {"info": lambda *a, **k: None})()
    full = b.preflight(
        Cur(),
        b.SofMergeBenchmarkConfig(window_from="a", window_to="b"),
        "QuestionnaireResponse",
        log,
    )
    third = b.preflight(
        Cur(),
        b.SofMergeBenchmarkConfig(window_from="a", window_to="b", copy_rows=12_000_000),
        "QuestionnaireResponse",
        log,
    )
    assert full["estimate_gb"] == pytest.approx(104.1, abs=0.2)
    assert third["estimate_gb"] == pytest.approx(33.4, abs=0.3) and third["copy_rows"] == 12_000_000

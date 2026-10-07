# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DQA coverage for the HAIBA dm domain: subjects, jobs, sensor spec, and the two
new reusable check types (row_parity, freshness).

The regressions these pin down: a subject silently losing its parity or
konverteringstab check, a check drifting to WEEKLY cadence (which would never
run — the domain's job is requested per publish, not per Saturday), the
sentinel caps loosening for the date keys, CPR-bearing columns leaking into
check params, and the two new check types changing their pass/fail semantics.

Mock cursors only -- no DB.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from dqa.config.haiba_subjects import HAIBA_SUBJECTS
from dqa.jobs import dqa_jobs
from dqa.model import Cadence, CheckSpecification, DqaSubject, Severity, Track
from dqa.registry import CHECK_TYPES
from dqa.sensors import DQA_AFTER_ETL_SENSOR_SPECS

pytestmark = pytest.mark.unit


def _make_resource(cursor):
    resource = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    resource.get_connection.return_value.__enter__ = MagicMock(return_value=conn)
    resource.get_connection.return_value.__exit__ = MagicMock(return_value=False)
    return resource


def _subject(table="dm.HAIBA_CasesSSI", **kw):
    return DqaSubject(
        name="dwh_haiba_cases_ssi",
        track=Track.DWH,
        table=table,
        resource_key="mssql_target_staging",
        identity_column="dw_ek_HAIBA_CasesSSI",
        **kw,
    )


def _by_name() -> dict[str, DqaSubject]:
    return {s.name: s for s in HAIBA_SUBJECTS}


# --- subject configuration --------------------------------------------------------


def test_all_subjects_exist_with_source_haiba():
    names = {s.name for s in HAIBA_SUBJECTS}
    assert names == {
        "dwh_haiba_agg_infektion",
        "dwh_haiba_agg_ortopaedi",
        "dwh_haiba_cases_infektion",
        "dwh_haiba_cases_ssi",
        "dwh_haiba_arc_agg_infektion",
        "dwh_haiba_arc_agg_ortopaedi",
        "dwh_haiba_arc_cases_infektion",
        "dwh_haiba_arc_cases_ssi",
        "dwh_haiba_leverance",
    }
    assert all(s.track is Track.DWH and s.source == "haiba" for s in HAIBA_SUBJECTS)


def test_every_check_is_daily_cadence():
    """The domain's DQA job is requested per publish; a WEEKLY check would sit
    in a job no sensor or cron ever requests."""
    for s in HAIBA_SUBJECTS:
        for spec in s.checks:
            assert spec.cadence is Cadence.DAILY, f"{s.name}: {spec.check_type} is not DAILY"


def test_dm_subjects_have_parity_and_konverteringstab():
    for s in HAIBA_SUBJECTS:
        if not s.table.startswith("dm."):
            continue
        kinds = [spec.name_suffix for spec in s.checks if spec.check_type == "row_parity"]
        assert set(kinds) == {"vs_arc", "vs_egenskaber"}, s.name
        assert any(spec.check_type == "konverteringstab" for spec in s.checks), s.name


def test_lineage_referential_edges_are_present():
    s = _by_name()["dwh_haiba_cases_ssi"]
    ri = {
        spec.params["column"]: spec.params["parent_table"]
        for spec in s.checks
        if spec.check_type == "referential_integrity"
    }
    assert ri["dw_ek_Borger"] == "dim.Borger"
    assert ri["dw_ek_Organisation_producent"] == "dim.Organisation"
    assert ri["dw_ek_Dato_operation"] == "dim.Dato"
    # Relation keys stamped by haiba_kontakt resolve against the FHIR tables.
    assert ri["encounter_id_producent"] == "fhir.Encounter"


def test_cases_infektion_checks_both_relation_key_kinds():
    s = _by_name()["dwh_haiba_cases_infektion"]
    ri = {
        spec.params["column"]: (spec.params["parent_table"], spec.params["parent_key"])
        for spec in s.checks
        if spec.check_type == "referential_integrity"
    }
    assert ri["encounter_id_opholdsadresse"] == ("fhir.Encounter", "encounter_id")
    assert ri["location_id_opholdsadresse"] == ("fhir.Location", "location_id")


def test_ikke_koert_check_covers_every_role_and_is_error():
    s = _by_name()["dwh_haiba_cases_ssi"]
    spec = next(sp for sp in s.checks if sp.name_suffix == "ikke_koert")
    assert spec.severity is Severity.ERROR
    for role in ("producent", "kontaktansvarlig", "reo_producent", "reo_kontaktansvarlig"):
        assert f"match_kvalitet_{role}" in spec.params["sql"]


def test_date_sentinels_are_capped_and_org_roles_are_not():
    s = _by_name()["dwh_haiba_cases_ssi"]
    spec = next(sp for sp in s.checks if sp.check_type == "sentinel_rate")
    caps = spec.params["max_rate"]
    assert caps["dw_ek_Dato_operation"] <= 0.01
    assert caps["dw_ek_Borger"] <= 0.05
    assert not any(c.startswith("dw_ek_Organisation") for c in caps)
    # Uncapped org roles are still measured.
    assert "dw_ek_Organisation_producent" in spec.params["columns"]


def test_no_check_can_surface_cpr_or_clean_code_values():
    """dm is CPR-free by construction, and the arc PKs carry raw CPR. No check
    may SELECT such a value into examples — scanning the cprnr COLUMN in a
    counts-only aggregate (the encoding check) is fine, exposing rows is not."""
    for s in HAIBA_SUBJECTS:
        for spec in s.checks:
            assert "clean_code" not in repr(spec.params), f"{s.name}: {spec.name_suffix}"
            examples_sql = spec.params.get("examples_sql", "")
            assert "cprnr" not in examples_sql, f"{s.name}: {spec.name_suffix}"
            if s.table.startswith("dm."):
                # The CPR column does not even exist in dm — a mention is a bug.
                assert "cprnr" not in repr(spec.params), f"{s.name}: {spec.name_suffix}"
            elif "cprnr" in repr(spec.params):
                # arc checks may scan the column, but only as a counting
                # aggregate with no example rows.
                assert "examples_sql" not in spec.params, f"{s.name}: {spec.name_suffix}"
                assert "COUNT" in spec.params.get("checked_sql", "") or "SUM(" in spec.params.get(
                    "sql", ""
                ), f"{s.name}: {spec.name_suffix}"


def test_leverance_subject_watches_all_four_feeds():
    s = _by_name()["dwh_haiba_leverance"]
    (spec,) = s.checks
    assert spec.check_type == "freshness"
    assert set(spec.params["expected_groups"]) == {
        "haiba_agg_infektion",
        "haiba_agg_ortopaedi",
        "haiba_cases_infektion",
        "haiba_cases_ssi",
    }


# --- jobs + sensor wiring ----------------------------------------------------------


def test_haiba_daily_job_exists_and_no_weekly_job():
    names = {j.name for j in dqa_jobs}
    assert "dqa_dwh_haiba_daily_job" in names
    assert "dqa_dwh_haiba_weekly_job" not in names


def test_sensor_spec_monitors_the_publish_job():
    spec = next(s for s in DQA_AFTER_ETL_SENSOR_SPECS if s["name"] == "dqa_haiba_after_publish")
    assert spec["monitored"] == ["haiba_publish_job"]
    assert spec["daily_job"] == "dqa_dwh_haiba_daily_job"
    assert spec["weekly_jobs"] == []


# --- row_parity check type ---------------------------------------------------------


def _parity_spec(**overrides):
    params = {"other_table": "arc.haiba_cases_ssi"}
    params.update(overrides)
    return CheckSpecification("row_parity", Severity.ERROR, Cadence.DAILY, params)


def test_row_parity_passes_on_equal_counts():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(100,), (100,)]
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    outcome = CHECK_TYPES["row_parity"](MagicMock(), _subject(), _parity_spec(), resources)
    assert outcome.passed
    assert outcome.violations == 0
    assert outcome.extra["other_rows"] == 100


def test_row_parity_fails_with_the_absolute_difference():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(100,), (97,)]
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    outcome = CHECK_TYPES["row_parity"](MagicMock(), _subject(), _parity_spec(), resources)
    assert not outcome.passed
    assert outcome.violations == 3


def test_row_parity_does_not_filter_soft_deleted_rows():
    """Both sides carry dw_is_deleted; filtering would hide the drift."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(1,), (1,)]
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    CHECK_TYPES["row_parity"](
        MagicMock(), _subject(deleted_column="dw_is_deleted"), _parity_spec(), resources
    )
    sql = "\n".join(str(c.args[0]) for c in cursor.execute.call_args_list)
    assert "dw_is_deleted" not in sql


# --- freshness check type ----------------------------------------------------------


def _freshness_spec(**overrides):
    params = {
        "watermark_column": "leverance_dato",
        "max_age_days": 14,
        "group_column": "kildetabel",
        "expected_groups": ["a", "b"],
    }
    params.update(overrides)
    return CheckSpecification("freshness", Severity.WARN, Cadence.DAILY, params)


def _meta_subject():
    return DqaSubject(
        name="dwh_haiba_leverance",
        track=Track.DWH,
        table="meta.HAIBALeverance",
        resource_key="mssql_target_staging",
        identity_column="kildetabel",
    )


def test_freshness_passes_when_all_groups_are_recent():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("a", 3), ("b", 7)]
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    outcome = CHECK_TYPES["freshness"](MagicMock(), _meta_subject(), _freshness_spec(), resources)
    assert outcome.passed
    assert outcome.extra["age_days"] == {"a": 3, "b": 7}


def test_freshness_fails_on_a_stale_group():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("a", 3), ("b", 21)]
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    outcome = CHECK_TYPES["freshness"](MagicMock(), _meta_subject(), _freshness_spec(), resources)
    assert not outcome.passed
    assert outcome.violations == 1
    assert outcome.examples[0]["group"] == "b"


def test_freshness_fails_on_a_missing_expected_group():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("a", 3)]
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    outcome = CHECK_TYPES["freshness"](MagicMock(), _meta_subject(), _freshness_spec(), resources)
    assert not outcome.passed
    assert outcome.extra["missing_groups"] == ["b"]


def test_freshness_fails_on_an_empty_watermark_table():
    cursor = MagicMock()
    cursor.fetchone.return_value = (None,)
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    outcome = CHECK_TYPES["freshness"](
        MagicMock(),
        _meta_subject(),
        _freshness_spec(group_column=None, expected_groups=None),
        resources,
    )
    assert not outcome.passed


def test_freshness_skips_a_group_with_a_null_watermark():
    """A group whose MAX watermark is NULL has no datable row: skip it rather
    than crash on int(None). The HAIBA columns are NOT NULL, but the check is
    advertised as reusable."""
    cursor = MagicMock()
    cursor.fetchall.return_value = [("a", 3), ("b", None)]
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    outcome = CHECK_TYPES["freshness"](
        MagicMock(), _meta_subject(), _freshness_spec(expected_groups=None), resources
    )
    assert outcome.passed
    assert outcome.extra["age_days"] == {"a": 3}


def test_freshness_reads_the_table_override():
    """The watermark can live in a table other than the subject's own — a
    per-type freshness reads the delivery register, not the fact table."""
    cursor = MagicMock()
    cursor.fetchall.return_value = [("a", 1)]
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    CHECK_TYPES["freshness"](
        MagicMock(),
        _meta_subject(),
        _freshness_spec(table="meta.Other", expected_groups=None),
        resources,
    )
    sql = "\n".join(str(c.args[0]) for c in cursor.execute.call_args_list)
    assert "[meta].[Other]" in sql


# --- arc-subjects: kontroltotal, freshness pr. type, encoding ------------------------


def test_kontroltotal_fails_only_on_aligned_deliveries():
    """Out-of-step deliveries (HAIBAcpr and HAIBAagg drop on different dates —
    verified on dev: 2026-08-08 vs 2026-08-15 gave drift on every type) must
    land in extra, never as violations; a diff on ALIGNED dates is the real
    alarm the profiling promised is exact."""
    s = _by_name()["dwh_haiba_arc_cases_infektion"]
    spec = next(sp for sp in s.checks if sp.name_suffix == "cases_vs_agg")
    assert spec.severity is Severity.ERROR
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("BACT", 100, 100, "2026-08-15", "2026-08-15"),  # aligned, equal -> ok
        ("UTI", 90, 95, "2026-08-15", "2026-08-15"),  # aligned, differs -> violation
        ("COHA", 50, 53, "2026-08-08", "2026-08-15"),  # out of step -> extra only
    ]
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    outcome = spec.fn(MagicMock(), s, spec, resources)
    assert not outcome.passed
    assert outcome.violations == 1
    assert outcome.examples == ["UTI"]
    assert set(outcome.extra["ude_af_takt"]) == {"COHA"}
    # One type in step, so the gate is not inert.
    assert outcome.extra["ingen_typer_i_takt"] is False


def test_kontroltotal_flags_a_permanently_inert_gate():
    """When every type is out of step the gate compares nothing yet still passes
    (out-of-step is legitimate between weekly drops). A standing flag makes that
    inert state visible instead of a silently green ERROR check."""
    s = _by_name()["dwh_haiba_arc_cases_infektion"]
    spec = next(sp for sp in s.checks if sp.name_suffix == "cases_vs_agg")
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("BACT", 100, 90, "2026-08-08", "2026-08-15"),
        ("UTI", 90, 80, "2026-08-08", "2026-08-15"),
    ]
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    outcome = spec.fn(MagicMock(), s, spec, resources)
    assert outcome.passed  # no aligned pair, so no violation
    assert outcome.violations == 0
    assert outcome.extra["ingen_typer_i_takt"] is True


def test_kontroltotal_sql_gates_on_the_register_not_arc_leverance():
    from dqa.config.haiba_subjects import _KONTROLTOTAL_SQL as sql

    assert "WHEN N'CDI_COHA' THEN N'COHA'" in sql
    assert "WHEN N'CDI_HOHA' THEN N'HOHA'" in sql
    assert "N'Kontaktansvar'" in sql and "N'1083'" in sql
    assert "FULL OUTER JOIN" in sql
    # The delivery-date gate reads the per-type register, not MAX(arc.leverance).
    assert "[meta].[HAIBALeverance]" in sql
    assert "MAX([leverance_dato]) AS [lev]" in sql


def test_arc_freshness_reads_the_per_type_register():
    """Per-type freshness must read meta.HAIBALeverance, not arc.leverance_dato:
    that column only advances on a value change, and the near append-only case
    feed would read as stale per type. No expected_groups: SDS adds and retires
    types without notice; a retired type stops being registered and leaves."""
    for name, arc_table in [
        ("dwh_haiba_arc_agg_infektion", "haiba_agg_infektion"),
        ("dwh_haiba_arc_cases_ssi", "haiba_cases_ssi"),
    ]:
        s = _by_name()[name]
        spec = next(sp for sp in s.checks if sp.check_type == "freshness")
        assert spec.params["table"] == "meta.HAIBALeverance"
        assert spec.params["group_column"] == "hai_type"
        assert spec.params["where"] == f"[kildetabel] = N'{arc_table}'"
        assert "expected_groups" not in spec.params


def test_encoding_check_scans_all_text_columns_without_examples():
    """The cases PK carries CPR, so the U+FFFD check must return counts only —
    no examples_sql — while still covering every text column. Active rows only,
    so a retracted historical row cannot keep the check red forever."""
    s = _by_name()["dwh_haiba_arc_cases_ssi"]
    spec = next(sp for sp in s.checks if sp.name_suffix == "encoding_ufffd")
    assert "examples_sql" not in spec.params
    assert "NCHAR(65533)" in spec.params["sql"]
    assert "[dw_is_deleted] = 0" in spec.params["sql"]
    assert "[dw_is_deleted] = 0" in spec.params["checked_sql"]
    # BIN2 collation is required: U+FFFD is collation-ignorable in the default
    # collation, so a plain LIKE matches every non-empty cell (false positives
    # measured on dev). The check must count literal replacement characters.
    assert "COLLATE Latin1_General_100_BIN2" in spec.params["sql"]
    for column in ("cprnr", "findings9", "reo_producent", "banr"):
        assert f"[{column}]" in spec.params["sql"]


def test_arc_subject_carries_the_deleted_column():
    """not_null/uniqueness/referential_integrity read subject.deleted_column;
    leaving it unset is a trap for the next check added to the arc subject."""
    for name in ("dwh_haiba_arc_agg_infektion", "dwh_haiba_arc_cases_ssi"):
        assert _by_name()[name].deleted_column == "dw_is_deleted"


# --- konverteringstab (inline fn) ----------------------------------------------------


def test_konverteringstab_sums_the_rebuilds_own_counter():
    s = _by_name()["dwh_haiba_cases_ssi"]
    spec = next(sp for sp in s.checks if sp.check_type == "konverteringstab")
    cursor = MagicMock()
    cursor.description = [("tab_SSI_acute_90",), ("tab_operation_date",)]
    cursor.fetchone.return_value = (2, 0)
    resources = SimpleNamespace(mssql_target_staging=_make_resource(cursor))
    outcome = spec.fn(MagicMock(), s, spec, resources)
    assert not outcome.passed
    assert outcome.violations == 2
    assert outcome.extra["tab_pr_kolonne"]["tab_SSI_acute_90"] == 2

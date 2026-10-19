# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for IFDB senge/kapacitet FHIR transforms and grain protection.

Pins behaviour that would silently break fhir.Observation, fhir.Location or
fhir.Encounter_Location if the new resources leaked into the existing views.
"""

from __future__ import annotations

import json
import pathlib

import pytest
from core.dfm_config import RunModeConfig
from dagster import AssetKey, Failure
from DSA.FHIR.Encounter.Encounter_IFDB_dfm import (
    CHILD_BACKFILL_DONE,
    CHILD_BACKFILL_SUBJECT,
    CHILD_DELTA_WATERMARK_SUBJECT,
    CHILD_TOUCHED_ENCOUNTERS_CEILING,
    EncounterDfmConfig,
    _build_encounter_query,
    _child_watermark_mode,
    _er_epoke,
    _guard_child_affected_delta,
    _skal_rykke_child_vandmaerke,
    _transform_encounters_to_fhir,
    fhir_server_encounter_dfm,
)
from DSA.FHIR.Location.Location_IFDB_Bed_dfm import (
    _transform_beds_to_fhir,
    fhir_server_location_bed_dfm,
)
from DSA.FHIR.Location.Location_IFDB_WardUnit_dfm import (
    _transform_ward_unit_to_fhir,
    fhir_server_location_ward_unit_dfm,
)
from DSA.FHIR.Observation.Observation_IFDB_WardUnitCapacity_dfm import (
    _transform_capacity_to_fhir,
    fhir_server_observation_ward_unit_capacity_dfm,
)
from DSA.FHIR.SqlOnFhir.Location_dfm import sof_location_dfm
from DSA.FHIR.SqlOnFhir.Observation_dfm import sof_observation_dfm
from DSA.FHIR.SqlOnFhir.WardUnitBed_dfm import (
    sof_bed_dfm,
    sof_encounter_absence_dfm,
    sof_encounter_bed_space_dfm,
    sof_ward_unit_capacity_dfm,
    sof_ward_unit_dfm,
)
from DSA.FHIR.utils.ifdb.ward_unit_bed import (
    CHILD_AFFECTED_ENCOUNTER_TABLES,
    CODE_LOCATION_PHYSICAL_TYPE_BED,
    KAPACITET_TYPE_DISPONIBLE,
    KAPACITET_TYPE_NORMEREDE,
    admenhed_dictionary_sql,
    archive_datetime_to_fhir,
    build_absence_extension,
    build_admenhed_extension,
    build_capacity_identifier_value,
    child_affected_encounter_count_sql,
    child_affected_encounter_ids_sql,
    child_backfill_remaining_sql,
    child_backfill_slice_sql,
    fold_latest_bed_rows,
    parse_antal,
)
from DSA.FHIR.utils.sql_on_fhir.sof_utils import SOF_RESOURCE_CONFIGS, VIEW_DEFINITIONS_DIR
from utils.fhir.fhir_extensions import (
    EXTENSION_BED_SPACE_WARD_UNIT_CODE,
    EXTENSION_ENCOUNTER_ABSENCE,
    EXTENSION_SENGEENHED_ADMENHED,
)
from utils.fhir.identifier_systems import (
    CODE_OBSERVATION_CATEGORY_BED_CAPACITY,
    SYSTEM_IFDB_BED,
    SYSTEM_IFDB_SENGEENHED,
    SYSTEM_IFDB_SENGEENHED_KAPACITET,
    SYSTEM_IFDB_SFI,
    SYSTEM_RSD_OBSERVATION_CATEGORY,
    SYSTEM_RSD_WARD_UNIT_CAPACITY_TYPE,
    SYSTEM_SKS,
    SYSTEM_SOR,
)
from utils.fhir.index_config import IDENTIFIER_UNIQUE_INDEX_SPECS

pytestmark = pytest.mark.unit


class _Log:
    def __init__(self) -> None:
        self.warnings: list[str] = []
        self.infos: list[str] = []

    def warning(self, msg: str, *args: object, **kwargs: object) -> None:
        self.warnings.append(str(msg))

    def info(self, msg: str, *args: object, **kwargs: object) -> None:
        self.infos.append(str(msg))


class _Ctx:
    def __init__(self) -> None:
        self.log = _Log()


class _FakeCursor:
    def __init__(self, count: int) -> None:
        self._count = count
        self.executed: list[str] = []

    def execute(self, sql: str, *args: object) -> None:
        self.executed.append(sql)

    def fetchone(self) -> tuple[int]:
        return (self._count,)

    def close(self) -> None:
        return None


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


class _FakeMssql:
    """Staging resource that answers the child-affected COUNT with a fixed size."""

    def __init__(self, count: int) -> None:
        self.cursor = _FakeCursor(count)

    def get_connection(self) -> _FakeConn:
        return _FakeConn(self.cursor)


def _load_view(name: str) -> dict:
    with open(VIEW_DEFINITIONS_DIR / f"{name}.json", encoding="utf-8") as handle:
        return json.load(handle)


def _where_paths(view: dict) -> list[str]:
    return [item.get("path", "") for item in view.get("where", [])]


# ---------------------------------------------------------------------------
# Grain protection — red if anyone removes where[] / the bd exclusion
# ---------------------------------------------------------------------------


class TestGrainProtection:
    def test_observation_view_excludes_kapacitet_only(self) -> None:
        """The filter must subtract capacity, never whitelist a code system.

        Only the SFI datapoints carry urn:dk:ifdb:sfi. The root
        AKTIVITETSRESULTAT observation is emitted losslessly with SKS codings
        alone, so a whitelist on the SFI system silently drops every one of
        them out of fhir.Observation.
        """
        paths = _where_paths(_load_view("observation_view"))
        assert paths, "observation_view must keep a filter that subtracts capacity"
        assert all(
            SYSTEM_RSD_WARD_UNIT_CAPACITY_TYPE in path and path.endswith("exists().not()")
            for path in paths
        ), paths
        assert SYSTEM_IFDB_SFI not in "".join(paths), paths
        assert SYSTEM_SKS not in "".join(paths), paths

    def test_location_view_whitelists_sor(self) -> None:
        paths = _where_paths(_load_view("location_view"))
        assert any(SYSTEM_SOR in path and "exists()" in path for path in paths), paths
        text = json.dumps(_load_view("location_view"))
        assert SYSTEM_IFDB_SENGEENHED not in text
        assert SYSTEM_IFDB_BED not in text

    def test_encounter_location_view_excludes_bd(self) -> None:
        view = _load_view("encounter_location_view")
        foreach = [block.get("forEachOrNull") for block in view["select"]]
        assert any(
            item and "physicalType.coding.where(code='bd').exists().not()" in item
            for item in foreach
        ), foreach

    def test_kapacitet_view_excludes_sfi(self) -> None:
        view = _load_view("ward_unit_capacity_view")
        paths = _where_paths(view)
        assert any(SYSTEM_RSD_WARD_UNIT_CAPACITY_TYPE in path for path in paths)
        assert SYSTEM_IFDB_SFI not in json.dumps(view)

    def test_sengeenhed_and_bed_views_use_own_identifier_systems(self) -> None:
        sengeenhed = _load_view("ward_unit_view")
        bed = _load_view("bed_view")
        assert any(SYSTEM_IFDB_SENGEENHED in path for path in _where_paths(sengeenhed))
        assert any(SYSTEM_IFDB_BED in path for path in _where_paths(bed))
        assert SYSTEM_SOR not in "".join(_where_paths(sengeenhed))
        assert SYSTEM_SOR not in "".join(_where_paths(bed))

    def test_encounter_bed_space_view_selects_bd_only(self) -> None:
        view = _load_view("encounter_bed_space_view")
        foreach = [block.get("forEach") for block in view["select"]]
        assert any(
            item and "physicalType.coding.where(code='bd').exists()" in item for item in foreach
        ), foreach


# ---------------------------------------------------------------------------
# Transform contracts
# ---------------------------------------------------------------------------


class TestKapacitetTransform:
    def test_identifier_is_deterministic(self) -> None:
        first = build_capacity_identifier_value(
            KAPACITET_TYPE_DISPONIBLE, "12345", "2025-01-15T08:30:00"
        )
        second = build_capacity_identifier_value(
            KAPACITET_TYPE_DISPONIBLE, "12345", "2025-01-15T08:30:00"
        )
        assert first == second

    @pytest.mark.parametrize("fra", ["2025-01-15T08:30:00", "15JAN2025:08:30:00"])
    def test_identifier_format_is_pinned(self, fra: str) -> None:
        """The full string, not a prefix.

        Every capacity resource's identity is this value. It used to be built
        from str(datetime) — a space instead of a T — so a later "cleanup" of
        the formatting would have minted new identifiers for rows that already
        exist and left the unique identifier index holding duplicates. Both of
        the source's datetime shapes must land on the same key.
        """
        assert (
            build_capacity_identifier_value(KAPACITET_TYPE_DISPONIBLE, "12345", fra)
            == "disponible:12345:2025-01-15T08:30:00+00:00"
        )

    def test_archive_datetime_is_an_iso_string(self) -> None:
        value = archive_datetime_to_fhir("15JAN2025:08:30:00")
        assert isinstance(value, str)
        assert value == "2025-01-15T08:30:00+00:00"

    def test_antal_zero_survives(self) -> None:
        assert parse_antal("0") == 0
        assert parse_antal(0) == 0
        rows = [
            {
                "kapacitet_type": KAPACITET_TYPE_DISPONIBLE,
                "SENGEENHED_ID": "SE1",
                "FRA": "2025-01-01T00:00:00",
                "TIL": "2025-06-30T00:00:00",
                "ANTAL": "0",
                "dw_is_deleted": 0,
            }
        ]
        observations = _transform_capacity_to_fhir(rows, _Ctx())
        assert len(observations) == 1
        assert observations[0].valueInteger == 0
        assert observations[0].identifier[0].system == SYSTEM_IFDB_SENGEENHED_KAPACITET
        assert observations[0].code.coding[0].system == SYSTEM_RSD_WARD_UNIT_CAPACITY_TYPE
        assert observations[0].subject.identifier.system == SYSTEM_IFDB_SENGEENHED

    def test_open_period_omits_end(self) -> None:
        rows = [
            {
                "kapacitet_type": KAPACITET_TYPE_DISPONIBLE,
                "SENGEENHED_ID": "SE1",
                "FRA": "2025-10-01T00:00:00",
                "TIL": None,
                "ANTAL": "16",
                "dw_is_deleted": 0,
            }
        ]
        observations = _transform_capacity_to_fhir(rows, _Ctx())
        assert observations[0].effectivePeriod.start is not None
        assert observations[0].effectivePeriod.end is None
        assert archive_datetime_to_fhir(None) is None


class TestSengeenhedTransform:
    def test_dictionary_miss_omits_managing_organization_and_logs(self) -> None:
        ctx = _Ctx()
        rows = [
            {
                "OBJEKT_ID": "SE1",
                "SLETTET": "0",
                "ADMENHED_KODE": "UNKNOWN",
                "ADMENHED_NAVN": "Testafsnit",
                "ADMENHED_ORGKODE": None,
            }
        ]
        locations = _transform_ward_unit_to_fhir(rows, {}, ctx)
        assert len(locations) == 1
        assert locations[0].managingOrganization is None
        assert locations[0].name == "Testafsnit"
        assert locations[0].identifier[0].system == SYSTEM_IFDB_SENGEENHED
        assert locations[0].physicalType is None
        assert any("UNKNOWN" in msg or "dictionary" in msg.lower() for msg in ctx.log.warnings)

    def test_dictionary_hit_sets_managing_organization(self) -> None:
        rows = [
            {
                "OBJEKT_ID": "SE1",
                "SLETTET": "0",
                "ADMENHED_KODE": "K1",
                "ADMENHED_NAVN": "Hit",
                "ADMENHED_ORGKODE": "ORG",
            }
        ]
        locations = _transform_ward_unit_to_fhir(rows, {"K1": "123456789"}, _Ctx())
        assert locations[0].managingOrganization is not None
        assert locations[0].managingOrganization.identifier.system == SYSTEM_SOR
        assert locations[0].managingOrganization.identifier.value == "123456789"


class TestBedTransform:
    def test_latest_period_wins_dedup(self) -> None:
        rows = [
            {
                "BED_ID": "B1",
                "BED": "old",
                "ROOM": "R-old",
                "LOCATION_SORCODE": "111",
                "PERIOD_START_REGISTRATION": "2024-01-01T00:00:00",
            },
            {
                "BED_ID": "B1",
                "BED": "new",
                "ROOM": "R-new",
                "LOCATION_SORCODE": "222",
                "PERIOD_START_REGISTRATION": "2025-06-01T12:00:00",
            },
        ]
        folded = fold_latest_bed_rows(rows)
        assert len(folded) == 1
        assert folded[0]["BED"] == "new"
        assert folded[0]["ROOM"] == "R-new"
        assert folded[0]["LOCATION_SORCODE"] == "222"

    def test_fold_streams_and_survives_an_unreadable_timestamp(self) -> None:
        """One unreadable NVARCHAR value must not take the asset down.

        The fallback sort key used to be a naive datetime.min while parsed keys
        are aware, so the comparison raised TypeError on the first bad row.
        """
        rows = iter(
            [
                {"BED_ID": "B1", "BED": "god", "PERIOD_START_REGISTRATION": "02JAN2025:10:00:00"},
                {"BED_ID": "B1", "BED": "skrald", "PERIOD_START_REGISTRATION": "ikke-en-dato"},
            ]
        )
        folded = fold_latest_bed_rows(rows)
        assert [row["BED"] for row in folded] == ["god"]

    def test_fold_takes_any_iterable(self) -> None:
        """A generator, so the caller can stream 1,3 mio. rows through it."""

        def _generate():
            yield {"BED_ID": "B1", "PERIOD_START_REGISTRATION": "01JAN2025:00:00:00"}
            yield {"BED_ID": "B2", "PERIOD_START_REGISTRATION": "01JAN2025:00:00:00"}

        assert len(fold_latest_bed_rows(_generate())) == 2

    def test_live_row_beats_deleted_row_regardless_of_period(self) -> None:
        rows = [
            {
                "BED_ID": "B1",
                "BED": "slettet-nyere",
                "PERIOD_START_REGISTRATION": "02JUN2025:10:00:00",
                "dw_is_deleted": 1,
            },
            {
                "BED_ID": "B1",
                "BED": "levende-aeldre",
                "PERIOD_START_REGISTRATION": "02JAN2025:10:00:00",
                "dw_is_deleted": 0,
            },
        ]
        folded = fold_latest_bed_rows(rows)
        assert [row["BED"] for row in folded] == ["levende-aeldre"]

    def test_bed_is_deleted_only_when_no_live_row_remains(self) -> None:
        """The archive flag is fhir.Bed's only route to dw_is_deleted = 1."""
        deleted_only = fold_latest_bed_rows(
            [
                {
                    "BED_ID": "B1",
                    "BED": "Seng 1",
                    "PERIOD_START_REGISTRATION": "02JAN2025:10:00:00",
                    "dw_is_deleted": 1,
                }
            ]
        )
        locations = _transform_beds_to_fhir(deleted_only, _Ctx())
        assert len(locations) == 1, "en slettet seng skal stadig udsendes, ikke droppes"
        flags = [
            ext.valueInteger for ext in locations[0].extension if ext.url == "urn:fhir:isDeleted"
        ]
        assert flags == [1]

    def test_bed_view_projects_the_delete_flag(self) -> None:
        columns = {
            column["name"]: column["path"]
            for block in _load_view("bed_view")["select"]
            for column in block["column"]
        }
        assert "dw_is_deleted" in columns, "uden projektion naar flaget aldrig fhir.Bed"
        assert "urn:fhir:isDeleted" in columns["dw_is_deleted"]
        cfg = SOF_RESOURCE_CONFIGS["bed_view"]
        assert cfg.get("boolean_false_default_columns") == ["dw_is_deleted"]

    def test_part_of_omitted_when_sor_is_null(self) -> None:
        locations = _transform_beds_to_fhir(
            [
                {
                    "BED_ID": "B1",
                    "BED": "Seng 1",
                    "ROOM": "Stue A",
                    "LOCATION_SORCODE": None,
                    "PERIOD_START_REGISTRATION": "2025-01-01T00:00:00",
                }
            ],
            _Ctx(),
        )
        assert len(locations) == 1
        assert locations[0].partOf is None
        assert locations[0].identifier[0].system == SYSTEM_IFDB_BED
        assert locations[0].physicalType.coding[0].code == CODE_LOCATION_PHYSICAL_TYPE_BED
        assert locations[0].name == "Seng 1"

    def test_bed_location_carries_current_ward_unit_code(self) -> None:
        locations = _transform_beds_to_fhir(
            [
                {
                    "BED_ID": "B1",
                    "BED": "Seng 1",
                    "ROOM": None,
                    "LOCATION_SORCODE": None,
                    "LOCATION_CODE": " 01.04.05.2.1 ",
                    "PERIOD_START_REGISTRATION": "2025-01-01T00:00:00",
                }
            ],
            _Ctx(),
        )
        ward = [e for e in locations[0].extension if e.url == EXTENSION_BED_SPACE_WARD_UNIT_CODE]
        assert len(ward) == 1
        assert ward[0].valueString == "01.04.05.2.1"


class TestEncounterEnrichment:
    def _encounter_row(self) -> dict:
        return {
            "OBJECT_ID": "ENC1",
            "PATIENT_ID": "P1",
            "STATUS_TEXT": "indlagt",
            "DELETED": 0,
            "PERIOD_START": "01JAN2025:00:00:00",
            "PERIOD_END": None,
            "CLASS_TEXT": "indlæggelse",
        }

    def test_bed_entry_has_physical_type_bd_on_the_entry(self) -> None:
        encounters = _transform_encounters_to_fhir(
            [self._encounter_row()],
            {},
            _Ctx(),
            beds_by_encounter={
                "ENC1": [
                    {
                        "BED_ID": "B1",
                        "LOCATION_CODE": "01.04.05.2.1",
                        "PERIOD_START_REGISTRATION": "02JAN2025:10:00:00",
                        "PERIOD_END_REGISTRATION": None,
                    }
                ]
            },
        )
        assert len(encounters) == 1
        assert encounters[0].location is not None
        entry = encounters[0].location[0]
        assert entry.location.identifier.system == SYSTEM_IFDB_BED
        assert entry.physicalType is not None
        assert entry.physicalType.coding[0].code == CODE_LOCATION_PHYSICAL_TYPE_BED
        # Enhedskoden ligger paa selve opholdet — joinnoeglen til WardUnit.admin_unit_code.
        ward = [e for e in (entry.extension or []) if e.url == EXTENSION_BED_SPACE_WARD_UNIT_CODE]
        assert len(ward) == 1
        assert ward[0].valueString == "01.04.05.2.1"

    def test_bed_entry_without_location_code_has_no_ward_extension(self) -> None:
        encounters = _transform_encounters_to_fhir(
            [self._encounter_row()],
            {},
            _Ctx(),
            beds_by_encounter={
                "ENC1": [
                    {
                        "BED_ID": "B1",
                        "LOCATION_CODE": None,
                        "PERIOD_START_REGISTRATION": "02JAN2025:10:00:00",
                        "PERIOD_END_REGISTRATION": None,
                    }
                ]
            },
        )
        entry = encounters[0].location[0]
        assert not [
            e for e in (entry.extension or []) if e.url == EXTENSION_BED_SPACE_WARD_UNIT_CODE
        ]

    def test_absence_is_pattern_a_with_fragment_urls(self) -> None:
        extension = build_absence_extension(
            {
                "PERIOD_START": "2025-03-01T08:00:00",
                "PERIOD_END": "2025-03-03T12:00:00",
                "TYPE_CODE": "SKS.a.admALCF01",
                "TYPE_TEXT": "Orlov",
                "EXPECTED_RETURN": "2025-03-03T12:00:00",
            }
        )
        assert extension is not None
        assert extension.url == EXTENSION_ENCOUNTER_ABSENCE
        urls = {sub.url for sub in extension.extension}
        assert f"{EXTENSION_ENCOUNTER_ABSENCE}#period" in urls
        assert f"{EXTENSION_ENCOUNTER_ABSENCE}#type" in urls
        assert f"{EXTENSION_ENCOUNTER_ABSENCE}#expectedReturn" in urls
        type_ext = next(sub for sub in extension.extension if sub.url.endswith("#type"))
        # Kildens praefiks strippes og koden baerer SKS-systemet, som for diagnoser.
        assert type_ext.valueCoding.system == SYSTEM_SKS
        assert type_ext.valueCoding.code == "ALCF01"
        assert type_ext.valueCoding.display == "Orlov"

    def test_absence_type_code_without_source_prefix_passes_through(self) -> None:
        extension = build_absence_extension(
            {"PERIOD_START": "2025-03-01T08:00:00", "TYPE_CODE": " ALCF05 ", "TYPE_TEXT": None}
        )
        type_ext = next(sub for sub in extension.extension if sub.url.endswith("#type"))
        assert type_ext.valueCoding.code == "ALCF05"
        assert type_ext.valueCoding.display is None

    def test_absence_without_type_code_emits_no_type_subextension(self) -> None:
        extension = build_absence_extension(
            {"PERIOD_START": "2025-03-01T08:00:00", "TYPE_CODE": "  "}
        )
        assert not any(sub.url.endswith("#type") for sub in extension.extension)

        encounters = _transform_encounters_to_fhir(
            [self._encounter_row()],
            {},
            _Ctx(),
            absences_by_encounter={
                "ENC1": [
                    {
                        "PERIOD_START": "2025-03-01T08:00:00",
                        "PERIOD_END": None,
                        "TYPE_CODE": "SKS.a.admALCF01",
                        "TYPE_TEXT": "Orlov",
                        "EXPECTED_RETURN": None,
                    }
                ]
            },
        )
        absence_exts = [
            ext for ext in encounters[0].extension if ext.url == EXTENSION_ENCOUNTER_ABSENCE
        ]
        assert len(absence_exts) == 1

    def test_live_verify_summary_is_aggregate_only(self) -> None:
        encounters = _transform_encounters_to_fhir(
            [self._encounter_row()],
            {},
            _Ctx(),
            beds_by_encounter={
                "ENC1": [
                    {
                        "BED_ID": "B1",
                        "PERIOD_START_REGISTRATION": "02JAN2025:10:00:00",
                        "PERIOD_END_REGISTRATION": None,
                    }
                ]
            },
            absences_by_encounter={
                "ENC1": [
                    {
                        "PERIOD_START": "2025-03-01T08:00:00",
                        "PERIOD_END": None,
                        "TYPE_CODE": "SKS.a.admALCF01",
                        "TYPE_TEXT": "Orlov",
                        "EXPECTED_RETURN": None,
                    }
                ]
            },
        )
        without_absence = _transform_encounters_to_fhir(
            [self._encounter_row()],
            {},
            _Ctx(),
            beds_by_encounter={
                "ENC1": [
                    {
                        "BED_ID": "B1",
                        "PERIOD_START_REGISTRATION": "02JAN2025:10:00:00",
                        "PERIOD_END_REGISTRATION": None,
                    }
                ]
            },
        )
        assert len(encounters) == 1
        enc = encounters[0]
        bed_entries = [
            loc
            for loc in (enc.location or [])
            if loc.location.identifier and loc.location.identifier.system == SYSTEM_IFDB_BED
        ]
        assert len(bed_entries) == 1
        assert bed_entries[0].physicalType.coding[0].code == CODE_LOCATION_PHYSICAL_TYPE_BED
        absence_exts = [
            ext for ext in (enc.extension or []) if ext.url == EXTENSION_ENCOUNTER_ABSENCE
        ]
        assert len(absence_exts) == 1
        assert any(sub.url.endswith("#period") for sub in absence_exts[0].extension)
        # Fravaer aendrer aldrig kontaktens status (ingen onleave-afledning).
        assert enc.status == without_absence[0].status
        assert enc.status != "onleave"


class TestBerorteKontakterSql:
    def test_delta_unions_child_object_ids(self) -> None:
        checkpoint = "2025-06-01 00:00:00"
        sql = _build_encounter_query(
            checkpoint, RunModeConfig(mode="delta"), child_watermark=checkpoint
        )
        assert "arc.ifdb_encounter" in sql.replace("[", "").replace("]", "")
        assert "dw_is_deleted" not in sql
        assert "OR [OBJECT_ID] IN" in sql
        child_sql = child_affected_encounter_ids_sql(checkpoint)
        assert child_sql in sql
        flattened = child_sql.replace("[", "").replace("]", "")
        assert "arc.ifdb_encounter_bed_space_history" in flattened
        assert "arc.ifdb_absence" in flattened
        assert checkpoint in child_sql
        assert "UNION" in child_sql
        assert CHILD_AFFECTED_ENCOUNTER_TABLES == (
            "arc.ifdb_encounter_bed_space_history",
            "arc.ifdb_absence",
        )

    def test_full_load_does_not_union_children(self) -> None:
        sql = _build_encounter_query(None, RunModeConfig(mode="full_load"))
        assert "UNION" not in sql
        assert "ifdb_absence" not in sql

    def test_count_sql_wraps_the_same_union(self) -> None:
        checkpoint = "2025-06-01 00:00:00"
        count_sql = child_affected_encounter_count_sql(checkpoint)
        assert child_affected_encounter_ids_sql(checkpoint) in count_sql
        assert "COUNT(*)" in count_sql


class TestChildVandmaerke:
    """Child-deltaet maaler paa sit eget vandmaerke, ikke paa Encounter-checkpointet.

    Uden det laaser mekanismerne hinanden: efter deploy matcher unionen hele
    historikken, vaernet fejler koerslen, og fordi koerslen fejler rykker
    checkpointet ikke -- saa vaernet beregner det samme i morgen. For evigt.
    """

    def test_uden_vandmaerke_ingen_child_union(self) -> None:
        sql = _build_encounter_query(
            "2025-06-01 00:00:00", RunModeConfig(mode="delta"), child_watermark=None
        )
        assert "UNION" not in sql
        assert "ifdb_absence" not in sql

    def test_unionen_maaler_paa_vandmaerket_ikke_checkpointet(self) -> None:
        sql = _build_encounter_query(
            "2025-01-01 00:00:00",
            RunModeConfig(mode="delta"),
            child_watermark="2025-06-01 00:00:00",
        )
        # Child-grenen skal bruge vandmaerket; kontaktens eget delta bruger
        # checkpointet. Begge datoer optraeder, men child-unionen kun med den ene.
        child_del = sql.split("OR [OBJECT_ID] IN")[1]
        assert "2025-06-01 00:00:00" in child_del
        assert "2025-01-01 00:00:00" not in child_del

    def test_vandmaerket_har_eget_navn(self) -> None:
        assert CHILD_DELTA_WATERMARK_SUBJECT == "Encounter_ChildDelta"
        assert CHILD_DELTA_WATERMARK_SUBJECT != CHILD_BACKFILL_SUBJECT
        assert CHILD_DELTA_WATERMARK_SUBJECT != "Encounter"


class TestChildAffectedCeiling:
    """The first delta after the child archives load matches the whole history.

    archive_merge stamps dw_timestamp_update = SYSDATETIME() on INSERT, so the
    union is not "fremadrettet berigelse" but the entire backlog. The guard
    keeps that sweep out of a nightly run whose checkpoint would move past it.
    """

    checkpoint = "2025-06-01 00:00:00"

    def test_uden_vandmaerke_afvises_hele_historikken(self) -> None:
        """Er efterslaebet ikke afviklet, maa deltaet ikke tage det."""
        ctx, mssql = _Ctx(), _FakeMssql(915_070)
        with pytest.raises(Failure) as excinfo:
            _guard_child_affected_delta(
                ctx,
                self.checkpoint,
                EncounterDfmConfig(mode="delta"),
                mssql,
                child_watermark=None,
            )
        beskrivelse = str(excinfo.value.description)
        assert "915,070" in beskrivelse
        assert "20260929_fhir_encounter_child_delta_reset" in beskrivelse
        assert "dfm_reset" in beskrivelse

    def test_routine_delta_passes_and_logs_the_size(self) -> None:
        ctx, mssql = _Ctx(), _FakeMssql(1_200)
        _guard_child_affected_delta(
            ctx,
            self.checkpoint,
            EncounterDfmConfig(mode="delta"),
            mssql,
            child_watermark=self.checkpoint,
        )
        assert any("1,200" in msg for msg in ctx.log.infos), ctx.log.infos
        assert not ctx.log.warnings

    def test_backlog_sweep_is_refused_with_its_size(self) -> None:
        ctx = _Ctx()
        mssql = _FakeMssql(CHILD_TOUCHED_ENCOUNTERS_CEILING + 1)
        with pytest.raises(Failure) as excinfo:
            _guard_child_affected_delta(
                ctx,
                self.checkpoint,
                EncounterDfmConfig(mode="delta"),
                mssql,
                child_watermark=self.checkpoint,
            )
        description = str(excinfo.value.description)
        assert f"{CHILD_TOUCHED_ENCOUNTERS_CEILING + 1:,}" in description
        # Beskeden skal sige, hvad koden goer: kontakter beroert VIA child-
        # arkiverne -- ikke "Encounter-deltaet er for stort".
        assert "sengeophold/fravaer" in description
        assert "dfm_reset" in description, "den vanlige vej til at tage historikken med"
        assert "FHIR_ENCOUNTER_CHILD_TOUCHED_CEILING" in description

    def test_vaernet_kender_ikke_backfill_flaget(self) -> None:
        """Noedudgangen i vaernet var doed kode: med child_affected_backfill=True
        short-circuitter assetet til efterslaebet, foer vaernet naas. Vaernet
        maa derfor ikke lade, som om flaget aendrer noget her.
        """
        ctx = _Ctx()
        mssql = _FakeMssql(CHILD_TOUCHED_ENCOUNTERS_CEILING + 1)
        with pytest.raises(Failure):
            _guard_child_affected_delta(
                ctx,
                self.checkpoint,
                EncounterDfmConfig(mode="delta", child_affected_backfill=True),
                mssql,
                child_watermark=self.checkpoint,
            )
        assert not ctx.log.warnings

    def test_bevidst_nulstilling_passerer_loftet(self) -> None:
        """Et vandmaerke sat med meta.usp_dfm_reset er vejen til at tage alt med.

        last_mode = 'reset' er en logget, bevidst handling -- ikke den
        utilsigtede flodboelge fra et genindlaest arkiv, som loftet skal fange.
        """
        ctx = _Ctx()
        mssql = _FakeMssql(927_745)
        _guard_child_affected_delta(
            ctx,
            self.checkpoint,
            EncounterDfmConfig(mode="delta"),
            mssql,
            child_watermark="1900-01-01 00:00:00",
            child_watermark_mode="reset",
        )
        assert any("927,745" in m and "usp_dfm_reset" in m for m in ctx.log.infos), ctx.log.infos

    def test_delvis_nulstilling_over_loftet_afvises(self) -> None:
        """Undtagelsen gaelder kun epoken.

        Nulstiller nogen til et par dage tilbage, og arkivet genindlaeses foer
        naeste koersel, ville undtagelsen ellers lade hele historikken passere --
        praecis det, loftet skal stoppe.
        """
        ctx = _Ctx()
        mssql = _FakeMssql(CHILD_TOUCHED_ENCOUNTERS_CEILING + 1)
        with pytest.raises(Failure):
            _guard_child_affected_delta(
                ctx,
                self.checkpoint,
                EncounterDfmConfig(mode="delta"),
                mssql,
                child_watermark="2026-09-20 00:00:00",
                child_watermark_mode="reset",
            )

    @pytest.mark.parametrize(
        "vaerdi, forventet",
        [
            ("1900-01-01 00:00:00", True),
            ("1900-01-01", True),
            ("1900-01-01 00:00:00.000", True),
            ("1900-01-02 00:00:00", False),
            ("2026-09-20 00:00:00", False),
            ("", False),
            (None, False),
            ("ikke-en-dato", False),
        ],
    )
    def test_epoken_genkendes_i_de_former_usp_dfm_reset_kan_faa(self, vaerdi, forventet) -> None:
        assert _er_epoke(vaerdi) is forventet

    def test_almindeligt_vandmaerke_over_loftet_afvises_stadig(self) -> None:
        """Kun 'reset' aabner; et fremrykket vandmaerke (last_mode = delta) goer ikke."""
        ctx = _Ctx()
        mssql = _FakeMssql(CHILD_TOUCHED_ENCOUNTERS_CEILING + 1)
        with pytest.raises(Failure):
            _guard_child_affected_delta(
                ctx,
                self.checkpoint,
                EncounterDfmConfig(mode="delta"),
                mssql,
                child_watermark=self.checkpoint,
                child_watermark_mode="delta",
            )

    def test_scheduled_default_does_not_opt_into_the_backfill(self) -> None:
        assert EncounterDfmConfig().child_affected_backfill is False

    def test_guard_is_silent_outside_delta(self) -> None:
        ctx, mssql = _Ctx(), _FakeMssql(10**9)
        _guard_child_affected_delta(
            ctx,
            self.checkpoint,
            EncounterDfmConfig(mode="full_load"),
            mssql,
            child_watermark=self.checkpoint,
        )
        _guard_child_affected_delta(
            ctx,
            None,
            EncounterDfmConfig(mode="delta"),
            mssql,
            child_watermark=self.checkpoint,
        )
        assert mssql.cursor.executed == []


class TestKapacitetModellering:
    """Kategori og kodesystem: en aftager skal kunne filtrere uden forhåndsviden."""

    def _row(self) -> dict:
        return {
            "kapacitet_type": KAPACITET_TYPE_DISPONIBLE,
            "SENGEENHED_ID": "SE1",
            "FRA": "2025-01-01T00:00:00",
            "TIL": None,
            "ANTAL": "12",
            "dw_is_deleted": 0,
        }

    def test_kapacitet_baerer_en_kategori(self) -> None:
        observations = _transform_capacity_to_fhir([self._row()], _Ctx())
        codings = [
            coding
            for category in (observations[0].category or [])
            for coding in (category.coding or [])
        ]
        assert [(c.system, c.code) for c in codings] == [
            (SYSTEM_RSD_OBSERVATION_CATEGORY, CODE_OBSERVATION_CATEGORY_BED_CAPACITY)
        ]

    def test_kategorien_projiceres_i_viewet(self) -> None:
        columns = {
            column["name"]: column["path"]
            for block in _load_view("ward_unit_capacity_view")["select"]
            for column in block["column"]
        }
        assert "category_code" in columns
        assert SYSTEM_RSD_OBSERVATION_CATEGORY in columns["category_code"]

    def test_kapacitetstypens_kodesaet_er_lukket(self) -> None:
        """To koder, defineret i kode. Ændres sættet, skal lineage følge med."""
        assert (KAPACITET_TYPE_DISPONIBLE, KAPACITET_TYPE_NORMEREDE) == (
            "disponible",
            "normerede",
        )

    def test_kapacitetstypen_bruger_husets_kodesystem_navnerum(self) -> None:
        """Egne kodninger hoerer i FHIR-navnerummet, ikke i identifier-navnerummet.

        Kilden baerer ikke kapacitetstypen som kode, saa saettet er vores eget.
        Det deler navnerum med de oevrige egne kodninger (observation-category,
        procedure-category, patient-rights-type), mens urn:source:ifdb: er
        forbeholdt identifier-systemer -- hvor en noegle kommer fra. De to blev
        blandet sammen, da kodesystemet blev lagt i identifier-navnerummet.
        """
        assert SYSTEM_RSD_WARD_UNIT_CAPACITY_TYPE.startswith("http://example.org/fhir/CodeSystem/")
        assert not SYSTEM_RSD_WARD_UNIT_CAPACITY_TYPE.startswith("urn:source:ifdb:")
        assert SYSTEM_IFDB_SENGEENHED_KAPACITET.startswith(
            "urn:source:ifdb:"
        ), "identifier-systemet bliver i kilde-navnerummet"

    def test_kapacitetstypen_har_intet_kodesystem_katalog(self) -> None:
        """Bevidst fravalg: to vaerdier, der er deres egen displaytekst.

        Repoets kodesystemer er udtrukne kataloger med mange koder, et planlagt
        job og en aftagerflade. Et haandskrevet saet paa to vaerdier havde ingen
        af delene, og et katalog, intet planlagt job fylder, synkroniserer nul
        raekker uden at fejle. Samme behandling som kategorien bed-capacity.
        """
        from DSA.CodeSystem.utils import CODESYSTEM_PASCAL

        assert "sengeenhed_kapacitet" not in CODESYSTEM_PASCAL


class TestExtensionKonvention:
    """Én konvention: fragment-URL'er, ikke bare sub-navne.

    SoF-stien extension.extension.where(url='kode') filtrerer IKKE på
    parent-URL'en, så et generisk sub-navn på én extension kan opsamles af en
    anden extensions sti på samme ressource.
    """

    def test_admenhed_bruger_fragment_urler(self) -> None:
        ext = build_admenhed_extension("1.2.3", "Afsnit A", "ORG1")
        assert ext is not None
        urls = sorted(sub.url for sub in ext.extension)
        assert urls == [
            f"{EXTENSION_SENGEENHED_ADMENHED}#kode",
            f"{EXTENSION_SENGEENHED_ADMENHED}#navn",
            f"{EXTENSION_SENGEENHED_ADMENHED}#orgkode",
        ]

    def test_viewet_laeser_de_samme_fragment_urler(self) -> None:
        paths = " ".join(
            column["path"]
            for block in _load_view("ward_unit_view")["select"]
            for column in block["column"]
        )
        for field in ("kode", "navn", "orgkode"):
            assert f"{EXTENSION_SENGEENHED_ADMENHED}#{field}" in paths
            assert f"where(url='{field}')" not in paths


class TestDqaDaekning:
    """Værnene om joinnøglen og afstemningen mod arkivet."""

    def _subject(self, name: str):
        from dqa.config.fhir_subjects import FHIR_SUBJECTS

        return next(s for s in FHIR_SUBJECTS if s.name == name)

    def test_ward_unit_haandhaever_en_aktiv_pr_kode(self) -> None:
        checks = self._subject("fhir_ward_unit").checks
        invariant = [c for c in checks if c.name_suffix == "en_aktiv_pr_admin_unit_code"]
        assert len(invariant) == 1
        sql = invariant[0].params["sql"]
        assert "status] = 'active'" in sql
        assert "dw_is_deleted] = 0" in sql
        assert "admin_unit_code" in sql

    @pytest.mark.parametrize(
        ("subject", "archive"),
        [
            ("fhir_ward_unit", "arc.ifdb_sengeenhed"),
            ("fhir_bed", "arc.ifdb_encounter_bed_space_history"),
        ],
    )
    def test_afstemmes_mod_arkivet(self, subject: str, archive: str) -> None:
        checks = self._subject(subject).checks
        sources = [
            c.params.get("source_table") for c in checks if c.check_type == "source_reconciliation"
        ]
        assert archive in sources, sources

    @pytest.mark.parametrize(
        ("subject", "grain"),
        [
            ("fhir_encounter_bed_space", "encounter_id, location_id, location_period_start"),
            ("fhir_encounter_absence", "encounter_id, period_start"),
        ],
    )
    def test_child_grains_har_daglig_uniqueness(self, subject: str, grain: str) -> None:
        checks = self._subject(subject).checks
        grains = [c.params.get("grain") for c in checks if c.check_type == "uniqueness"]
        assert grain in grains, grains

    @pytest.mark.parametrize(
        ("table", "column"),
        [
            ("WardUnit", "admin_unit_code"),
            ("Bed", "ward_unit_code"),
            ("Encounter_BedSpace", "ward_unit_code"),
        ],
    )
    def test_joinnoeglen_er_indekseret(self, table: str, column: str) -> None:
        """Koblingen belægning <-> kapacitet skal kunne slås op, ikke scannes."""
        ddl = (
            pathlib.Path(__file__).resolve().parents[2]
            / "infrastructure"
            / "ddl"
            / "tables"
            / "fhir"
            / "01_tables.sql"
        ).read_text(encoding="utf-8")
        assert f"ON [fhir].[{table}] ([{column}] ASC)" in ddl


class TestAdmenhedOrdbog:
    """Organisationskoblingen skal komme fra sengeopholdene, ikke fra aktivitetsdata.

    LOCATION_CODE og LOCATION_SORCODE staar paa samme raekke i
    ENCOUNTER_BED_SPACE_HISTORY -- samme kodeomraade som ADMENHED_KODE, og
    samme SOR-kolonne der saetter Bed.partOf. Et opslag hoestet fra
    aktivitetsresultat/ris_ydelse ville udlede en organisationsrelation af et
    kodesammentraef i et andet domaene.
    """

    def test_ordbogen_hoestes_fra_sengeopholdene(self) -> None:
        flat = admenhed_dictionary_sql().replace("[", "").replace("]", "")
        assert "arc.ifdb_encounter_bed_space_history" in flat
        assert "LOCATION_CODE" in flat
        assert "LOCATION_SORCODE" in flat

    def test_ordbogen_bruger_ikke_aktivitetsdata(self) -> None:
        flat = admenhed_dictionary_sql().replace("[", "").replace("]", "")
        assert "aktivitetsresultat" not in flat
        assert "ris_ydelse" not in flat

    def test_ordbogen_aggregerer_i_sql(self) -> None:
        """Faa hundrede par ud af 1,3 mio. raekker -- ikke en scanning i Python."""
        sql = admenhed_dictionary_sql()
        assert "GROUP BY" in sql
        assert "dw_is_deleted" in sql


class TestArkivDedup:
    """PK-løse kildetabeller skal deduplikeres før MERGE.

    merge_keys er en grain vi påstår, ikke en kilden håndhæver. Uden dedup
    vælter en enkelt dublet hele den natlige arkiv-MERGE på arc-tabellens
    primærnøgle.
    """

    @pytest.mark.parametrize(
        "config_name",
        [
            "SENGEENHED_DISPONIBLE_ARCHIVE_CONFIG",
            "SENGEENHED_NORMEREDE_ARCHIVE_CONFIG",
            "ENCOUNTER_BED_SPACE_HISTORY_ARCHIVE_CONFIG",
            "ABSENCE_ARCHIVE_CONFIG",
        ],
    )
    def test_pk_loese_kilder_deduplikeres(self, config_name: str) -> None:
        from DSI.oracle import ifdb_archive

        assert getattr(ifdb_archive, config_name).dedup_source_on_keys is True

    def test_sengeenhed_har_kildens_egen_noegle(self) -> None:
        """SENGEENHED har PK i Oracle — dedup ville kun sløre en reel dublet."""
        from DSI.oracle import ifdb_archive

        assert ifdb_archive.SENGEENHED_ARCHIVE_CONFIG.dedup_source_on_keys is False


class TestEfterslaeb:
    """Efterslaebet er en engangsoperation pr. miljoe, koert i skiver.

    Markoeren i meta.DFMState er det, der goer den sikker at lade staa i en
    runbook: naar den er DONE, er koerslen en no-op, uanset hvor mange gange
    den startes.
    """

    def test_skiven_ordnes_paa_noeglen_ikke_paa_tid(self) -> None:
        """Arkivets timestamps er ens for hele bulk-loadet og kan ikke ordne."""
        sql = child_backfill_slice_sql(None, 100)
        assert "ORDER BY [OBJECT_ID] ASC" in sql
        assert "TOP (100)" in sql
        assert "dw_timestamp_update" not in sql

    def test_skiven_fortsaetter_efter_markoeren(self) -> None:
        sql = child_backfill_slice_sql("ENC-123", 50)
        assert sql.count("[OBJECT_ID] > 'ENC-123'") == len(CHILD_AFFECTED_ENCOUNTER_TABLES)

    def test_foerste_skive_har_ingen_nedre_graense(self) -> None:
        assert ">" not in child_backfill_slice_sql(None, 10).split("ORDER BY")[0].replace(
            "TOP (10)", ""
        )

    def test_markoeren_escapes(self) -> None:
        assert "''" in child_backfill_slice_sql("EN'C", 10)
        assert "''" in child_backfill_remaining_sql("EN'C")

    def test_skiven_daekker_begge_child_arkiver(self) -> None:
        flat = child_backfill_slice_sql(None, 10).replace("[", "").replace("]", "")
        for table in CHILD_AFFECTED_ENCOUNTER_TABLES:
            assert table in flat

    def test_efterslaebet_filtrerer_ikke_slettede_child_raekker(self) -> None:
        """En soft-slettet child-raekke skal ogsaa re-emittere sin kontakt."""
        assert "dw_is_deleted" not in child_backfill_slice_sql(None, 10)

    def test_ugyldig_skivestoerrelse_afvises(self) -> None:
        with pytest.raises(ValueError):
            child_backfill_slice_sql(None, 0)

    def test_resterende_taeller_efter_markoeren(self) -> None:
        sql = child_backfill_remaining_sql("ENC-9")
        assert "COUNT(*)" in sql
        assert sql.count("[OBJECT_ID] > 'ENC-9'") == len(CHILD_AFFECTED_ENCOUNTER_TABLES)

    def test_migrationen_armerer_med_den_noegleform_dfm_bruger(self) -> None:
        """Migrationen og assetet skal laese samme raekke.

        Noegleformen kommer fra DfmManagers egen mapping: schema fhir_server
        -> target_type fhir_resource. Er de uenige, armerer migrationen en
        raekke, som koerslen aldrig finder.
        """
        from core.dfm_manager import SCHEMA_TO_TARGET_TYPE

        sql = (
            pathlib.Path(__file__).resolve().parents[2]
            / "infrastructure"
            / "ddl"
            / "migrations"
            / "20260911_fhir_encounter_child_backfill.sql"
        ).read_text(encoding="utf-8")
        assert SCHEMA_TO_TARGET_TYPE["fhir_server"] == "fhir_resource"
        for needle in (
            "'staging'",
            "'fhir_resource'",
            "'fhir_server'",
            f"'{CHILD_BACKFILL_SUBJECT}'",
        ):
            assert needle in sql, needle
        # Idempotent og staging-gatet som praecedensen.
        assert "IF NOT EXISTS" in sql
        assert "OBJECT_ID(N'[meta].[DFMState]'" in sql
        # Migrationen maa ikke selv forsoege dataarbejdet.
        assert "DELETE FROM [meta].[DFMState]" not in sql

    def test_markoeren_har_eget_navn_og_sentinel(self) -> None:
        """Markoeren maa ikke kollidere med Encounter-checkpointet."""
        assert CHILD_BACKFILL_SUBJECT == "Encounter_ChildBackfill"
        assert CHILD_BACKFILL_SUBJECT != "Encounter"
        assert CHILD_BACKFILL_DONE == "DONE"

    def test_efterslaeb_med_ids_mode_afvises(self) -> None:
        """Kombinationen er misvisende: mode-filtrene bliver aldrig laest."""
        cfg = EncounterDfmConfig(
            mode="ids",
            id_filter_column="OBJECT_ID",
            id_filter_values=["X"],
            child_affected_backfill=True,
        )
        assert cfg.child_affected_backfill is True
        assert cfg.mode == "ids", "configen selv er gyldig -- assetet afviser kombinationen"

    def test_efterslaebet_er_default_slaaet_fra(self) -> None:
        cfg = EncounterDfmConfig()
        assert cfg.child_affected_backfill is False
        assert cfg.child_backfill_restart is False
        assert cfg.child_backfill_slice > 0

    def test_en_start_koerer_til_faerdig_som_standard(self) -> None:
        """En runbook, der siger «start den 37 gange», bliver ikke fulgt."""
        assert EncounterDfmConfig().child_backfill_max_slices == 0

    def test_loftet_kan_afgraense_koerslen(self) -> None:
        """Til at maale varigheden af én skive, foer man lader den loebe."""
        assert EncounterDfmConfig(child_backfill_max_slices=1).child_backfill_max_slices == 1

    def test_restart_nulstiller_markoeren_til_null_ikke_til_nu(self) -> None:
        """set_checkpoint(value=None) betyder SYSDATETIME(). En restart, der skrev
        et tidsstempel som markoer, begyndte gennemloebet midt i noegleordningen
        (alt efter '2026-...') i stedet for forfra -- set paa dev. Nulstilling
        skal derfor vaere en eksplicit NULL paa den eksisterende raekke."""
        from core.dfm_manager import DfmManager

        log: list[tuple[str, tuple]] = []

        class _Cursor:
            def execute(self, sql: str, params: tuple = ()) -> None:
                log.append((sql, params))

            def close(self) -> None:
                pass

        class _Conn:
            committed = 0

            def cursor(self) -> _Cursor:
                return _Cursor()

            def commit(self) -> None:
                _Conn.committed += 1

            def __enter__(self) -> _Conn:
                return self

            def __exit__(self, *exc: object) -> bool:
                return False

        class _Mssql:
            def get_connection(self) -> _Conn:
                return _Conn()

        DfmManager(_Mssql(), context=None).clear_checkpoint(  # type: ignore[arg-type]
            "fhir_server", CHILD_BACKFILL_SUBJECT, mode="ids"
        )
        ((sql, params),) = log
        flat = " ".join(sql.split())
        assert "SET [checkpoint_value] = NULL" in flat
        assert "COALESCE" not in flat and "SYSDATETIME(), 121" not in flat
        assert flat.startswith("UPDATE") and "INSERT" not in flat, "kun eksisterende raekke"
        assert params[-4:] == ("staging", "fhir_resource", "fhir_server", CHILD_BACKFILL_SUBJECT)
        assert _Conn.committed == 1

    def test_restart_stien_bruger_clear_checkpoint(self) -> None:
        import inspect

        from DSA.FHIR.Encounter import Encounter_IFDB_dfm as modul

        kode = "\n".join(
            linje
            for linje in inspect.getsource(modul._run_child_backfill).splitlines()
            if not linje.strip().startswith("#")
        )
        assert "clear_checkpoint(" in kode
        assert "value=None" not in kode, "set_checkpoint(value=None) skriver SYSDATETIME, ikke NULL"


class TestChildVandmaerkeFremrykning:
    """Hvornaar child-vandmaerket maa rykke -- reglen, der goer undtagelsen til een koersel."""

    def test_nulstilling_med_nul_hentede_rykker(self) -> None:
        """Ellers bliver last_mode staaende paa 'reset', og loftet er slaaet fra."""
        assert _skal_rykke_child_vandmaerke(EncounterDfmConfig(mode="delta"), 0, "reset")

    def test_almindelig_delta_med_nul_hentede_rykker_ikke(self) -> None:
        assert not _skal_rykke_child_vandmaerke(EncounterDfmConfig(mode="delta"), 0, "delta")

    def test_delta_med_hentede_rykker(self) -> None:
        assert _skal_rykke_child_vandmaerke(EncounterDfmConfig(mode="delta"), 10, "delta")

    def test_full_load_med_hentede_rykker(self) -> None:
        """full_load tager alle kontakter, saa child-raekkerne er laest."""
        assert _skal_rykke_child_vandmaerke(EncounterDfmConfig(mode="full_load"), 10, None)

    def test_ids_koersel_bruger_ikke_nulstillingen_op(self) -> None:
        """ids laeser ikke child-raekkerne, heller ikke med advance_checkpoint=True."""
        cfg = EncounterDfmConfig(
            mode="ids",
            id_filter_column="OBJECT_ID",
            id_filter_values=["X"],
            advance_checkpoint=True,
        )
        assert not _skal_rykke_child_vandmaerke(cfg, 0, "reset")
        assert not _skal_rykke_child_vandmaerke(cfg, 10, "reset")
        assert not _skal_rykke_child_vandmaerke(cfg, 10, "delta")

    def test_uden_fremrykning_rykker_intet(self) -> None:
        cfg = EncounterDfmConfig(mode="delta", advance_checkpoint=False)
        assert not _skal_rykke_child_vandmaerke(cfg, 10, "reset")


class TestChildVandmaerkeOpslag:
    """_child_watermark_mode skal slaa op med DfmManagers egne noegler, ikke haandskrevne."""

    def test_bruger_tilstandstabellen_og_mappingen_fra_dfm_manager(self) -> None:
        from core.dfm_manager import SCHEMA_TO_TARGET_TYPE, DfmManager

        kald: list[tuple[str, tuple]] = []

        class _Cursor:
            def execute(self, sql: str, params: tuple = ()) -> None:
                kald.append((sql, params))

            def fetchone(self):
                return ("reset",)

            def close(self) -> None:
                pass

        class _Conn:
            def cursor(self) -> _Cursor:
                return _Cursor()

            def __enter__(self) -> _Conn:
                return self

            def __exit__(self, *exc: object) -> bool:
                return False

        class _Mssql:
            def get_connection(self) -> _Conn:
                return _Conn()

        assert _child_watermark_mode(_Mssql()) == "reset"  # type: ignore[arg-type]
        ((sql, params),) = kald
        assert DfmManager.STATE_TABLE in sql
        assert "'fhir_resource'" not in sql and "'staging'" not in sql, "noeglerne er parametre"
        assert params == (
            "staging",
            SCHEMA_TO_TARGET_TYPE["fhir_server"],
            "fhir_server",
            CHILD_DELTA_WATERMARK_SUBJECT,
        )


class TestChildDeltaNulstilling:
    """Migrationen 20260929 er den vanlige vej: usp_dfm_reset, saa det ordinaere delta tager resten.

    Testene laeser SQL'en, men kontrollerer grenstrukturen -- hvilken handling
    staar i hvilken gren -- ikke blot at et ord forekommer.
    """

    @staticmethod
    def _sql() -> str:
        sti = (
            pathlib.Path(__file__).resolve().parents[2]
            / "infrastructure"
            / "ddl"
            / "migrations"
            / "20260929_fhir_encounter_child_delta_reset.sql"
        )
        return sti.read_text(encoding="utf-8")

    @classmethod
    def _grene(cls) -> tuple[str, str, str]:
        """(Encounter har ikke koert, allerede beriget, nulstil) -- i den raekkefoelge."""
        sql = cls._sql()
        ikke_koert = "AND [schema_name] = ''fhir_server'' AND [table_name] = ''Encounter'')"
        beriget = "ELSE IF EXISTS (SELECT 1 FROM [meta].[DFMState]"
        nulstil = "        ELSE\n        BEGIN"
        a, b, c = sql.index(ikke_koert), sql.index(beriget), sql.index(nulstil)
        assert a < b < c, "grenene skal staa: ikke koert -> beriget -> nulstil"
        return sql[a:b], sql[b:c], sql[c:]

    def test_kun_nulstillingsgrenen_roerer_vandmaerket(self) -> None:
        ikke_koert, beriget, nulstil = self._grene()
        reset_vm = f"@table_name = ''{CHILD_DELTA_WATERMARK_SUBJECT}''"
        assert reset_vm in nulstil
        assert (
            reset_vm not in ikke_koert
        ), "uden Encounter-koersel er foerste koersel en fuld indlaesning"
        assert reset_vm not in beriget, "et beriget miljoe maa ikke genposteres"
        assert "@new_value = ''1900-01-01 00:00:00''" in nulstil
        assert "[meta].[usp_dfm_reset] @schema_name = ''fhir_server''" in nulstil
        assert "UPDATE [meta].[DFMState]" not in self._sql(), "via proceduren, ikke en UPDATE"

    def test_markoeren_stemples_done_hvor_flaget_ellers_kunne_koere(self) -> None:
        """Begge grene, der ikke allerede er DONE, goer det flagstyrede efterslaeb til en no-op."""
        ikke_koert, beriget, nulstil = self._grene()
        stempel = (
            f"@table_name = ''{CHILD_BACKFILL_SUBJECT}'',\n"
            f"                 @new_value = ''{CHILD_BACKFILL_DONE}''"
        )
        assert stempel in nulstil
        assert f"@table_name = ''{CHILD_BACKFILL_SUBJECT}''" in ikke_koert
        assert f"@new_value = ''{CHILD_BACKFILL_DONE}''" in ikke_koert
        assert "usp_dfm_reset" not in beriget

    def test_beriget_grenen_kraever_done(self) -> None:
        _, beriget, _ = self._grene()
        assert f"[table_name] = ''{CHILD_BACKFILL_SUBJECT}''" in beriget
        assert f"[checkpoint_value] = ''{CHILD_BACKFILL_DONE}''" in beriget

    def test_epoken_er_den_samme_som_assetets(self) -> None:
        from DSA.FHIR.Encounter import Encounter_IFDB_dfm as modul

        assert f"''{modul._CHILD_EPOCH}''" in self._sql()
        assert _er_epoke(
            modul._CHILD_EPOCH
        ), "vaernets undtagelse skal genkende migrationens vaerdi"

    def test_fejler_hoejt_hvis_tilstanden_findes_men_proceduren_mangler(self) -> None:
        sql = self._sql()
        betingelse = (
            "IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL\n"
            "   AND OBJECT_ID(N'[meta].[usp_dfm_reset]', 'P') IS NULL\n"
            "    THROW 50012,"
        )
        assert betingelse in sql


class TestChildGrainReconcile:
    """Without the reconcile the child-affected delta buys nothing.

    A removed bed stay or absence simply stops being emitted; only the
    reconcile retires the row that fhir.Encounter_BedSpace / _Absence still
    holds.
    """

    @pytest.mark.parametrize("view", ["encounter_bed_space_view", "encounter_absence_view"])
    def test_child_views_retire_stale_rows(self, view: str) -> None:
        cfg = SOF_RESOURCE_CONFIGS[view]
        assert cfg.get("child_grain_reconcile") is True
        # Reconcile marks every live row for the parent deleted before the
        # MERGE; without resurrect the surviving rows stay flagged.
        assert cfg.get("resurrect_soft_deleted") is True
        assert cfg["id_column"] == "encounter_id"


class TestPipelineRegistration:
    def test_ward_unit_depends_on_archive_and_dictionary_source(self) -> None:
        """Sengeopholdene er dep: SOR-koblingen hoestes fra dem."""
        key = AssetKey.from_user_string("fhir_server_location_ward_unit_dfm")
        assert fhir_server_location_ward_unit_dfm.asset_deps[key] == {
            AssetKey.from_user_string("arc_sengeenhed_ifdb"),
            AssetKey.from_user_string("arc_encounter_bed_space_history_ifdb"),
        }

    def test_kapacitet_depends_on_archives_and_sengeenhed(self) -> None:
        key = AssetKey.from_user_string("fhir_server_observation_ward_unit_capacity_dfm")
        assert fhir_server_observation_ward_unit_capacity_dfm.asset_deps[key] == {
            AssetKey.from_user_string("arc_sengeenhed_disponible_ifdb"),
            AssetKey.from_user_string("arc_sengeenhed_normerede_ifdb"),
            AssetKey.from_user_string("fhir_server_location_ward_unit_dfm"),
        }

    def test_bed_depends_on_history_and_department_location(self) -> None:
        key = AssetKey.from_user_string("fhir_server_location_bed_dfm")
        assert fhir_server_location_bed_dfm.asset_deps[key] == {
            AssetKey.from_user_string("arc_encounter_bed_space_history_ifdb"),
            AssetKey.from_user_string("fhir_server_location_dfm"),
        }

    def test_encounter_depends_on_bed_and_absence(self) -> None:
        key = AssetKey.from_user_string("fhir_server_encounter_dfm")
        deps = fhir_server_encounter_dfm.asset_deps[key]
        assert AssetKey.from_user_string("arc_encounter_bed_space_history_ifdb") in deps
        assert AssetKey.from_user_string("arc_absence_ifdb") in deps
        assert AssetKey.from_user_string("fhir_server_location_bed_dfm") in deps

    def test_sof_location_does_not_wait_for_new_location_types(self) -> None:
        key = AssetKey.from_user_string("sof_location_dfm")
        deps = sof_location_dfm.asset_deps[key]
        assert AssetKey.from_user_string("fhir_server_location_ward_unit_dfm") not in deps
        assert AssetKey.from_user_string("fhir_server_location_bed_dfm") not in deps

    def test_sof_observation_does_not_wait_for_kapacitet(self) -> None:
        key = AssetKey.from_user_string("sof_observation_dfm")
        deps = sof_observation_dfm.asset_deps[key]
        assert (
            AssetKey.from_user_string("fhir_server_observation_ward_unit_capacity_dfm") not in deps
        )

    def test_new_sof_assets_wait_for_their_producers(self) -> None:
        assert sof_ward_unit_dfm.asset_deps[AssetKey.from_user_string("sof_ward_unit_dfm")] == {
            AssetKey.from_user_string("fhir_sof_infrastructure"),
            AssetKey.from_user_string("fhir_server_location_ward_unit_dfm"),
        }
        assert sof_ward_unit_capacity_dfm.asset_deps[
            AssetKey.from_user_string("sof_ward_unit_capacity_dfm")
        ] == {
            AssetKey.from_user_string("fhir_sof_infrastructure"),
            AssetKey.from_user_string("fhir_server_observation_ward_unit_capacity_dfm"),
        }
        assert sof_bed_dfm.asset_deps[AssetKey.from_user_string("sof_bed_dfm")] == {
            AssetKey.from_user_string("fhir_sof_infrastructure"),
            AssetKey.from_user_string("fhir_server_location_bed_dfm"),
        }
        assert sof_encounter_bed_space_dfm.asset_deps[
            AssetKey.from_user_string("sof_encounter_bed_space_dfm")
        ] == {
            AssetKey.from_user_string("fhir_sof_infrastructure"),
            AssetKey.from_user_string("fhir_server_encounter_dfm"),
        }
        assert sof_encounter_absence_dfm.asset_deps[
            AssetKey.from_user_string("sof_encounter_absence_dfm")
        ] == {
            AssetKey.from_user_string("fhir_sof_infrastructure"),
            AssetKey.from_user_string("fhir_server_encounter_dfm"),
        }

    def test_unique_index_specs_cover_new_identifier_systems(self) -> None:
        specs = set(IDENTIFIER_UNIQUE_INDEX_SPECS)
        assert ("Location", SYSTEM_IFDB_SENGEENHED) in specs
        assert ("Location", SYSTEM_IFDB_BED) in specs
        assert ("Observation", SYSTEM_IFDB_SENGEENHED_KAPACITET) in specs


def test_view_definition_files_exist() -> None:
    for name in (
        "ward_unit_view.json",
        "ward_unit_capacity_view.json",
        "bed_view.json",
        "encounter_bed_space_view.json",
        "encounter_absence_view.json",
    ):
        assert (VIEW_DEFINITIONS_DIR / name).is_file()


@pytest.mark.parametrize("view_name", ["encounter_bed_space_view", "bed_view"])
def test_bed_views_project_ward_unit_code(view_name: str) -> None:
    """Joinnoeglen fra belaegning til kapacitet skal naa den flade tabel."""
    import json

    view = json.loads((VIEW_DEFINITIONS_DIR / f"{view_name}.json").read_text())

    def columns(select):
        out = []
        for s in select:
            out += [c for c in s.get("column", [])]
            out += columns(s.get("select", []))
        return out

    ward = [c for c in columns(view["select"]) if c["name"] == "ward_unit_code"]
    assert len(ward) == 1
    assert "urn:source:ifdb:bed_space_ward_unit_code" in ward[0]["path"]

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from DSA.FHIR.Aktivitetsresultat.references import (
    _ensure_reference_dependencies,
)
from DSA.FHIR.Procedure.Procedure_IFDB_AdminProc_dfm import (
    _post_adm_procedure_to_fhir,
)
from DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse_dfm import _post_ris_ydelse_to_fhir
from DSA.FHIR.utils.ifdb.dependency_resolver import (
    _fetch_rows_by_ids_from_ifdb,
    resolve_missing_encounters,
)
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.observation import Observation
from fhir.resources.R4B.procedure import Procedure
from fhir.resources.R4B.reference import Reference
from utils.fhir.fhir_extensions import EXTENSION_EPISODE_OF_CARE
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
)

pytestmark = pytest.mark.unit


def _mock_context() -> MagicMock:
    ctx = MagicMock(spec=dg.AssetExecutionContext)
    ctx.log = MagicMock()
    return ctx


def _procedure_with_dependencies(
    patient_id: str,
    encounter_id: str,
    episode_id: str,
) -> Procedure:
    return Procedure.construct(
        subject=Reference.construct(
            identifier=Identifier.construct(
                system=SYSTEM_IFDB_PATIENT_ID,
                value=patient_id,
            )
        ),
        encounter=Reference.construct(
            identifier=Identifier.construct(
                system=SYSTEM_IFDB_OBJECT_ID,
                value=encounter_id,
            )
        ),
        extension=[
            Extension.construct(
                url=EXTENSION_EPISODE_OF_CARE,
                valueReference=Reference.construct(
                    identifier=Identifier.construct(
                        system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                        value=episode_id,
                    )
                ),
            )
        ],
    )


def _observation_with_dependencies(
    patient_id: str,
    encounter_id: str,
    episode_id: str,
) -> Observation:
    return Observation.construct(
        subject=Reference.construct(
            identifier=Identifier.construct(
                system=SYSTEM_IFDB_PATIENT_ID,
                value=patient_id,
            )
        ),
        encounter=Reference.construct(
            identifier=Identifier.construct(
                system=SYSTEM_IFDB_OBJECT_ID,
                value=encounter_id,
            )
        ),
        extension=[
            Extension.construct(
                url=EXTENSION_EPISODE_OF_CARE,
                valueReference=Reference.construct(
                    identifier=Identifier.construct(
                        system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                        value=episode_id,
                    )
                ),
            )
        ],
    )


class TestResolveMissingEncounters:
    @patch("DSA.FHIR.Encounter.Encounter_IFDB_dfm._post_encounters_to_fhir")
    @patch("DSA.FHIR.Encounter.Encounter_IFDB_dfm._transform_encounters_to_fhir")
    @patch("DSA.FHIR.Encounter.Encounter_IFDB_dfm._fetch_absence_data_for_batch")
    @patch("DSA.FHIR.Encounter.Encounter_IFDB_dfm._fetch_bed_space_data_for_batch")
    @patch("DSA.FHIR.Encounter.Encounter_IFDB_dfm._fetch_location_data_for_batch")
    @patch("DSA.FHIR.utils.ifdb.dependency_resolver._fetch_encounters_from_archive")
    @patch("DSA.FHIR.utils.ifdb.dependency_resolver.batch_resolve_references")
    def test_fetches_posts_and_reresolves_missing_encounters(
        self,
        mock_batch_resolve: MagicMock,
        mock_fetch_rows: MagicMock,
        mock_fetch_locations: MagicMock,
        mock_fetch_beds: MagicMock,
        mock_fetch_absences: MagicMock,
        mock_transform: MagicMock,
        mock_post: MagicMock,
    ) -> None:
        mock_batch_resolve.side_effect = [
            {"ENC-1": "enc-uuid-1"},
            {"ENC-1": "enc-uuid-1", "ENC-2": "enc-uuid-2"},
        ]
        mock_fetch_rows.return_value = [
            {"OBJECT_ID": "ENC-2", "PATIENT_ID": "PAT-2", "EPISODE_OF_CARE_ID": "EOC-2"}
        ]
        mock_fetch_locations.return_value = {"ENC-2": []}
        mock_fetch_beds.return_value = {"ENC-2": []}
        mock_fetch_absences.return_value = {"ENC-2": []}
        mock_transform.return_value = ["encounter-resource"]

        context = _mock_context()
        fhir_database = MagicMock()
        mssql = MagicMock()

        resolved = resolve_missing_encounters(
            encounter_ids={"ENC-1", "ENC-2"},
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
        )

        assert resolved == {"ENC-1": "enc-uuid-1", "ENC-2": "enc-uuid-2"}
        assert (
            mock_batch_resolve.call_args_list[0].kwargs["identifier_system"]
            == SYSTEM_IFDB_OBJECT_ID
        )
        assert (
            mock_batch_resolve.call_args_list[1].kwargs["identifier_system"]
            == SYSTEM_IFDB_OBJECT_ID
        )
        assert mock_batch_resolve.call_args_list[1].kwargs["force_reload"] is True
        mock_fetch_rows.assert_called_once()
        mock_fetch_locations.assert_called_once_with(["ENC-2"], mssql)
        mock_transform.assert_called_once()
        mock_post.assert_called_once_with(
            context=context,
            encounters=["encounter-resource"],
            fhir_database=fhir_database,
            extracted_count=1,
            mssql=mssql,
            resolve_missing_dependencies=True,
        )


def _ensure_result(**kw):
    from DSA.FHIR.Patient.patient_service import EnsureResult

    return EnsureResult(**kw)


@patch("DSA.FHIR.utils.ifdb.dependency_resolver._fetch_bom_ifdb_patientinfo_by_cpr")
@patch("DSA.FHIR.utils.ifdb.dependency_resolver.ensure_patients")
def test_by_cpr_looks_unresolved_cprs_up_in_bom_and_builds_them_as_epj(
    mock_ensure: MagicMock, mock_bom: MagicMock
) -> None:
    """Two passes: the entry point first, then BOM rows for what is left."""
    from DSA.FHIR.utils.ifdb.dependency_resolver import resolve_missing_patients_by_cpr

    mock_ensure.side_effect = [
        _ensure_result(by_cpr={"0101010101": "f-1"}, existing=1, unresolved_cprs=("0202020202",)),
        _ensure_result(by_cpr={"0202020202": "f-2"}, written=1),
    ]
    mock_bom.return_value = [{"CPRNR": "0202020202", "PATIENTINFOOID": 7}]
    fhir_database = MagicMock()

    resolved = resolve_missing_patients_by_cpr(
        cprs={"0101010101", "0202020202"},
        mssql=MagicMock(),
        fhir_database=fhir_database,
        context=_mock_context(),
        ifdb_oracle=MagicMock(),
    )

    assert resolved == {"0101010101": "f-1", "0202020202": "f-2"}
    assert mock_bom.call_args.args[0] == ["0202020202"]
    second = mock_ensure.call_args_list[1].kwargs
    assert set(second["cprs"]) == {"0202020202"}
    assert second["bom_rows_by_cpr"]["0202020202"][0]["PATIENTINFOOID"] == 7
    fhir_database.mark_stale.assert_called_once_with("Patient")


@patch("DSA.FHIR.utils.ifdb.dependency_resolver._fetch_bom_ifdb_patientinfo_by_cpr")
@patch("DSA.FHIR.utils.ifdb.dependency_resolver.ensure_patients")
def test_by_cpr_reads_the_archive_without_oracle(
    mock_ensure: MagicMock, mock_bom: MagicMock
) -> None:
    """Without Oracle only Oracle is skipped; the archive's BOM rows are still read."""
    from DSA.FHIR.utils.ifdb.dependency_resolver import resolve_missing_patients_by_cpr

    mock_ensure.return_value = _ensure_result(by_cpr={}, unresolved_cprs=("0202020202",))
    mock_bom.return_value = []
    resolved = resolve_missing_patients_by_cpr(
        cprs={"0202020202"},
        mssql=MagicMock(),
        fhir_database=MagicMock(),
        context=_mock_context(),
        ifdb_oracle=None,
    )
    assert resolved == {}
    mock_bom.assert_called_once()
    assert mock_bom.call_args.args[1] is None
    assert mock_ensure.call_count == 1


@patch("DSA.FHIR.utils.ifdb.dependency_resolver._fetch_bom_ifdb_patientinfo_by_cpr")
@patch("DSA.FHIR.utils.ifdb.dependency_resolver.ensure_patients")
def test_by_cpr_passes_every_bom_row_per_cpr(mock_ensure: MagicMock, mock_bom: MagicMock) -> None:
    """Two patientinfo rows for one CPR both reach the entry point."""
    from DSA.FHIR.utils.ifdb.dependency_resolver import resolve_missing_patients_by_cpr

    mock_ensure.side_effect = [
        _ensure_result(by_cpr={}, unresolved_cprs=("0202020202",)),
        _ensure_result(by_cpr={"0202020202": "f-2"}, written=1),
    ]
    mock_bom.return_value = [
        {"CPRNR": "0202020202", "PATIENTINFOOID": 9},
        {"CPRNR": "0202020202", "PATIENTINFOOID": 7},
    ]
    resolve_missing_patients_by_cpr(
        cprs={"0202020202"},
        mssql=MagicMock(),
        fhir_database=MagicMock(),
        context=_mock_context(),
        ifdb_oracle=MagicMock(),
    )
    second = mock_ensure.call_args_list[1].kwargs
    assert [r["PATIENTINFOOID"] for r in second["bom_rows_by_cpr"]["0202020202"]] == [9, 7]


class TestAktivitetsresultatDependencies:
    @patch("DSA.FHIR.Patient.patient_service.ensure_patients")
    @patch("DSA.FHIR.utils.ifdb.dependency_resolver.resolve_missing_encounters")
    @patch("DSA.FHIR.utils.ifdb.dependency_resolver.resolve_missing_episodes_of_care")
    def test_dict_pipeline_ensures_all_reference_dependencies(
        self,
        mock_resolve_eoc: MagicMock,
        mock_resolve_enc: MagicMock,
        mock_resolve_pat: MagicMock,
    ) -> None:
        rows = [
            {"PATIENT_ID": "PAT-1", "ENCOUNTER_ID": "ENC-1", "EPISODE_OF_CARE_ID": "EOC-1"},
            {"PATIENT_ID": "PAT-1", "ENCOUNTER_ID": "ENC-2", "EPISODE_OF_CARE_ID": "EOC-2"},
        ]

        context = _mock_context()
        fhir_database = MagicMock()
        mssql = MagicMock()
        mock_resolve_eoc.return_value = {"EOC-1": "eoc-fhir-1", "EOC-2": "eoc-fhir-2"}
        mock_resolve_enc.return_value = {"ENC-1": "enc-fhir-1", "ENC-2": "enc-fhir-2"}
        mock_resolve_pat.return_value = _ensure_result(by_ifdb_id={"PAT-1": "pat-fhir-1"})

        resolved = _ensure_reference_dependencies(
            rows,
            mssql,
            fhir_database,
            context,
        )

        # Cache invalidation is owned centrally by the dependency resolver +
        # FHIRDatabaseResource stale-tracking (cb494398); the resolvers are
        # therefore called without a mutation_report kwarg.
        mock_resolve_eoc.assert_called_once_with(
            eoc_ids={"EOC-1", "EOC-2"},
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
        )
        mock_resolve_enc.assert_called_once_with(
            encounter_ids={"ENC-1", "ENC-2"},
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
        )
        mock_resolve_pat.assert_called_once_with(
            ifdb_patient_ids={"PAT-1"},
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
            need="reference",
        )
        assert resolved["EpisodeOfCare"][(SYSTEM_IFDB_EPISODE_OF_CARE_ID, "EOC-1")] == "eoc-fhir-1"
        assert resolved["Encounter"][(SYSTEM_IFDB_OBJECT_ID, "ENC-2")] == "enc-fhir-2"
        assert resolved["Patient"][(SYSTEM_IFDB_PATIENT_ID, "PAT-1")] == "pat-fhir-1"


class TestProcedureDependencies:
    @patch("DSA.FHIR.Procedure.Procedure_IFDB_AdminProc_dfm.post_resources_direct")
    @patch("DSA.FHIR.Procedure.Procedure_IFDB_AdminProc_dfm.prepare_resources_for_upsert")
    @patch("DSA.FHIR.Patient.patient_service.ensure_patients")
    @patch("DSA.FHIR.utils.ifdb.dependency_resolver.resolve_missing_encounters")
    @patch("DSA.FHIR.utils.ifdb.dependency_resolver.resolve_missing_episodes_of_care")
    def test_administrative_procedure_post_resolves_all_dependencies(
        self,
        mock_resolve_eoc: MagicMock,
        mock_resolve_enc: MagicMock,
        mock_resolve_pat: MagicMock,
        mock_prepare: MagicMock,
        mock_post: MagicMock,
    ) -> None:
        procedures = [
            _procedure_with_dependencies("PAT-1", "ENC-1", "EOC-1"),
            _procedure_with_dependencies("PAT-1", "ENC-2", "EOC-2"),
        ]
        mock_prepare.return_value = procedures
        mock_post.return_value = "posted"

        context = _mock_context()
        fhir_database = MagicMock()
        mssql = MagicMock()

        result = _post_adm_procedure_to_fhir(
            context=context,
            procedures=procedures,
            fhir_database=fhir_database,
            extracted_count=2,
            mssql=mssql,
        )

        assert result == "posted"
        mock_resolve_eoc.assert_called_once_with(
            eoc_ids={"EOC-1", "EOC-2"},
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
        )
        mock_resolve_enc.assert_called_once_with(
            encounter_ids={"ENC-1", "ENC-2"},
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
        )
        mock_resolve_pat.assert_called_once_with(
            ifdb_patient_ids={"PAT-1"},
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
            need="reference",
        )
        assert mock_prepare.call_args.kwargs["resource_type"] == "Procedure"

    @patch("DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse_dfm.post_resources_direct")
    @patch("DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse_dfm.prepare_resources_for_upsert")
    @patch("DSA.FHIR.Patient.patient_service.ensure_patients")
    @patch("DSA.FHIR.utils.ifdb.dependency_resolver.resolve_missing_encounters")
    @patch("DSA.FHIR.utils.ifdb.dependency_resolver.resolve_missing_episodes_of_care")
    def test_ris_ydelse_post_resolves_all_dependencies(
        self,
        mock_resolve_eoc: MagicMock,
        mock_resolve_enc: MagicMock,
        mock_resolve_pat: MagicMock,
        mock_prepare: MagicMock,
        mock_post: MagicMock,
    ) -> None:
        procedures = [
            _procedure_with_dependencies("PAT-1", "ENC-1", "EOC-1"),
            _procedure_with_dependencies("PAT-2", "ENC-2", "EOC-2"),
        ]
        mock_prepare.return_value = procedures
        mock_post.return_value = "posted"

        context = _mock_context()
        fhir_database = MagicMock()
        mssql = MagicMock()

        result = _post_ris_ydelse_to_fhir(
            context=context,
            procedures=procedures,
            fhir_database=fhir_database,
            extracted_count=2,
            mssql=mssql,
        )

        assert result == "posted"
        mock_resolve_eoc.assert_called_once_with(
            eoc_ids={"EOC-1", "EOC-2"},
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
        )
        mock_resolve_enc.assert_called_once_with(
            encounter_ids={"ENC-1", "ENC-2"},
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
        )
        mock_resolve_pat.assert_called_once_with(
            ifdb_patient_ids={"PAT-1", "PAT-2"},
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
            need="reference",
        )
        assert mock_prepare.call_args.kwargs["resource_type"] == "Procedure"


class TestDependencyResolverHelpers:
    def test_fetch_rows_by_ids_returns_empty_without_connection_when_no_ids(self) -> None:
        ifdb_oracle = MagicMock()

        rows = _fetch_rows_by_ids_from_ifdb(
            ids=[],
            table="IFDB.TEST_TABLE",
            columns=["OBJECT_ID", "VALUE"],
            where_column="OBJECT_ID",
            ifdb_oracle=ifdb_oracle,
            context=_mock_context(),
        )

        assert rows == []
        ifdb_oracle.get_connection.assert_not_called()

    @patch("DSA.FHIR.utils.ifdb.oracle_lookup.fetch_rows_as_dicts")
    def test_fetch_rows_by_ids_chunks_queries_and_aggregates_results(
        self,
        mock_fetch_rows_as_dicts: MagicMock,
    ) -> None:
        ifdb_oracle = MagicMock()
        connection = MagicMock()
        cursor = MagicMock()

        ifdb_oracle.get_connection.return_value.__enter__.return_value = connection
        ifdb_oracle.get_connection.return_value.__exit__.return_value = False
        connection.cursor.return_value = cursor

        # One payload per chunk.
        mock_fetch_rows_as_dicts.side_effect = [
            [{"OBJECT_ID": "ID0000"}],
            [{"OBJECT_ID": "ID1004"}],
        ]

        # 1005 IDs forces two chunks (999 + 6).
        ids = [f"ID{i:04d}" for i in range(1005)]
        rows = _fetch_rows_by_ids_from_ifdb(
            ids=ids,
            table="IFDB.TEST_TABLE",
            columns=["OBJECT_ID", "VALUE"],
            where_column="OBJECT_ID",
            ifdb_oracle=ifdb_oracle,
            context=_mock_context(),
        )

        assert rows == [{"OBJECT_ID": "ID0000"}, {"OBJECT_ID": "ID1004"}]
        assert cursor.execute.call_count == 2
        cursor.close.assert_called_once()

        first_call = cursor.execute.call_args_list[0]
        second_call = cursor.execute.call_args_list[1]

        first_query, first_params = first_call.args
        second_query, second_params = second_call.args

        # Helper now binds parameters positionally (`:1, :2, ...`) instead of
        # interpolating raw quoted strings — the table/column identifiers stay
        # in the SQL while ID values travel via the driver's bind path.
        assert "SELECT OBJECT_ID, VALUE FROM IFDB.TEST_TABLE WHERE OBJECT_ID IN (" in first_query
        assert ":1, :2" in first_query
        # IDs are deduped + sorted lexicographically before chunking.
        sorted_ids = sorted(ids)
        assert first_params == sorted_ids[:999]
        assert second_params == sorted_ids[999:]


# --- CPR resolver: archive lookup falls back per CPR, not all-or-nothing ----------


def test_patientinfo_cpr_fallback_is_per_cpr_not_all_or_nothing(monkeypatch):
    """Partial archive coverage must not drop the uncovered CPRs.

    Regression: the archive result was returned whenever it was non-empty, so a
    batch where the archive held 1 of 3 CPRs silently lost the other 2.
    """
    from DSA.FHIR.utils.ifdb import dependency_resolver as dr

    monkeypatch.setattr(
        dr,
        "chunked_in_fetch",
        lambda ids, **kw: [{"PATIENTINFOOID": "1", "CPRNR": "111111-1111"}],
    )
    oracle_calls: list[list[str]] = []

    def fake_oracle(*, ids, **kw):
        oracle_calls.append(list(ids))
        return [{"PATIENTINFOOID": "2", "CPRNR": c} for c in ids]

    monkeypatch.setattr(dr, "_fetch_rows_by_ids_from_ifdb", fake_oracle)

    monkeypatch.setattr(dr, "DfmManager", lambda *args: MagicMock())

    rows = dr._fetch_bom_ifdb_patientinfo_by_cpr(
        ["111111-1111", "222222-2222", "333333-3333"],
        SimpleNamespace(user="ifdb", host="ifdb-host"),
        _mock_context(),
        mssql=object(),
    )

    # Only the CPRs the archive did not cover go to Oracle...
    assert oracle_calls == [["222222-2222", "333333-3333"]]
    # ...and every CPR comes back.
    assert {r["CPRNR"] for r in rows} == {"111111-1111", "222222-2222", "333333-3333"}


def test_patientinfo_cpr_fallback_skips_oracle_when_archive_covers_all(monkeypatch):
    from DSA.FHIR.utils.ifdb import dependency_resolver as dr

    monkeypatch.setattr(
        dr,
        "chunked_in_fetch",
        lambda ids, **kw: [{"PATIENTINFOOID": "1", "CPRNR": c} for c in ids],
    )

    def fail_oracle(**kw):  # pragma: no cover - must not run
        raise AssertionError("Oracle must not be queried when the archive covers every CPR")

    monkeypatch.setattr(dr, "_fetch_rows_by_ids_from_ifdb", fail_oracle)

    rows = dr._fetch_bom_ifdb_patientinfo_by_cpr(
        ["111111-1111"], object(), _mock_context(), mssql=object()
    )
    assert [r["CPRNR"] for r in rows] == ["111111-1111"]


class TestArchiveFetcherContract:
    """Pin the arc-switch contract: soft-delete semantics + column superset.

    The three resolver tables archive with source_delete_column (SLETTET/
    DELETED), so a soft-deleted row still exists in Oracle. The fetchers must
    pass include_deleted=True to preserve Oracle-lookup semantics 1:1 — the
    transform tombstones the row and required references stay resolvable.
    """

    @pytest.mark.parametrize(
        ("fetcher_name", "expected_table"),
        [
            ("_fetch_encounters_from_archive", "arc.ifdb_encounter"),
            ("_fetch_episodes_of_care_from_archive", "arc.ifdb_episode_of_care"),
        ],
    )
    def test_archive_fetchers_pass_include_deleted(
        self, fetcher_name: str, expected_table: str
    ) -> None:
        import DSA.FHIR.utils.ifdb.dependency_resolver as dr

        fetcher = getattr(dr, fetcher_name)
        with patch.object(dr, "chunked_in_fetch", return_value=[]) as mock_fetch:
            fetcher(["ID-1"], mssql=MagicMock())

        assert mock_fetch.call_count == 1
        kwargs = mock_fetch.call_args.kwargs
        assert kwargs["table"] == expected_table
        assert kwargs["include_deleted"] is True

    def test_arc_columns_are_superset_of_legacy_extract_configs(self) -> None:
        """The resolver reads *_COLUMNS from arc; nothing the legacy Oracle
        configs delivered may be missing (estimat §5.2 superset requirement)."""
        from DSI.oracle.ifdb import ENCOUNTER_CONFIG, EPISODE_OF_CARE_CONFIG, PATIENT_CONFIG
        from DSI.oracle.ifdb_extract import ENCOUNTER_COLUMNS, EOC_COLUMNS, PATIENT_COLUMNS

        assert set(PATIENT_CONFIG.columns) <= set(PATIENT_COLUMNS)
        assert set(ENCOUNTER_CONFIG.columns) <= set(ENCOUNTER_COLUMNS)
        assert set(EPISODE_OF_CARE_CONFIG.columns) <= set(EOC_COLUMNS)

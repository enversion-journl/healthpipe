# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from DSA.FHIR.Aktivitetsresultat.references import (
    _ensure_reference_dependencies,
)
from DSA.FHIR.Procedure.Procedure_IFDB_AdminProc import (
    _post_adm_procedure_to_fhir,
)
from DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse import _post_ris_ydelse_to_fhir
from DSA.FHIR.utils.ifdb.dependency_resolver import (
    _fetch_rows_by_ids_from_ifdb,
    _raise_if_unresolved,
    resolve_missing_encounters,
    resolve_missing_patients,
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
    @patch("DSA.FHIR.Encounter.Encounter_IFDB_dfm._fetch_location_data")
    @patch("DSA.FHIR.utils.ifdb.dependency_resolver._fetch_encounters_from_ifdb")
    @patch("DSA.FHIR.utils.ifdb.dependency_resolver.batch_resolve_references")
    def test_fetches_posts_and_reresolves_missing_encounters(
        self,
        mock_batch_resolve: MagicMock,
        mock_fetch_rows: MagicMock,
        mock_fetch_locations: MagicMock,
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
        mock_transform.return_value = ["encounter-resource"]

        context = _mock_context()
        fhir_database = MagicMock()
        ifdb_oracle = MagicMock()

        resolved = resolve_missing_encounters(
            encounter_ids={"ENC-1", "ENC-2"},
            ifdb_oracle=ifdb_oracle,
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
        mock_fetch_locations.assert_called_once_with(["ENC-2"], ifdb_oracle, context)
        mock_transform.assert_called_once()
        mock_post.assert_called_once_with(
            context=context,
            encounters=["encounter-resource"],
            fhir_database=fhir_database,
            extracted_count=1,
            ifdb_oracle=ifdb_oracle,
            resolve_missing_dependencies=True,
        )


@patch("DSA.FHIR.utils.ifdb.dependency_resolver._persist_cpr_matched_identifiers")
@patch("DSA.FHIR.utils.ifdb.dependency_resolver._fetch_patients_from_ifdb")
@patch("DSA.FHIR.utils.ifdb.dependency_resolver.batch_resolve_references")
def test_resolve_missing_patients_force_reloads_after_cpr_bridge(
    mock_batch_resolve: MagicMock,
    mock_fetch_rows: MagicMock,
    mock_persist_bridge: MagicMock,
) -> None:
    mock_batch_resolve.side_effect = [
        {},  # initial IFDB PATIENT_ID resolve
        {"0101011234": "fhir-pat-1"},  # CPR resolve
        {"NEW-PATIENT-ID": "fhir-pat-1"},  # force-reloaded IFDB PATIENT_ID resolve
    ]
    mock_fetch_rows.return_value = [{"PATIENT_ID": "NEW-PATIENT-ID", "CPR": "0101011234"}]
    mock_persist_bridge.return_value = 1

    context = _mock_context()
    fhir_database = MagicMock()
    ifdb_oracle = MagicMock()

    resolved = resolve_missing_patients(
        patient_ids={"NEW-PATIENT-ID"},
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
        context=context,
    )

    assert resolved == {"NEW-PATIENT-ID": "fhir-pat-1"}
    assert mock_batch_resolve.call_args_list[2].kwargs["force_reload"] is True
    fhir_database.clear_identifier_caches.assert_not_called()


class TestAktivitetsresultatDependencies:
    @patch("DSA.FHIR.utils.ifdb.dependency_resolver.resolve_missing_patients")
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
        ifdb_oracle = MagicMock()
        mock_resolve_eoc.return_value = {"EOC-1": "eoc-fhir-1", "EOC-2": "eoc-fhir-2"}
        mock_resolve_enc.return_value = {"ENC-1": "enc-fhir-1", "ENC-2": "enc-fhir-2"}
        mock_resolve_pat.return_value = {"PAT-1": "pat-fhir-1"}

        resolved = _ensure_reference_dependencies(
            rows,
            ifdb_oracle,
            fhir_database,
            context,
        )

        # Cache invalidation is owned centrally by the dependency resolver +
        # FHIRDatabaseResource stale-tracking (cb494398); the resolvers are
        # therefore called without a mutation_report kwarg.
        mock_resolve_eoc.assert_called_once_with(
            eoc_ids={"EOC-1", "EOC-2"},
            ifdb_oracle=ifdb_oracle,
            fhir_database=fhir_database,
            context=context,
        )
        mock_resolve_enc.assert_called_once_with(
            encounter_ids={"ENC-1", "ENC-2"},
            ifdb_oracle=ifdb_oracle,
            fhir_database=fhir_database,
            context=context,
        )
        mock_resolve_pat.assert_called_once_with(
            patient_ids={"PAT-1"},
            ifdb_oracle=ifdb_oracle,
            fhir_database=fhir_database,
            context=context,
        )
        assert resolved["EpisodeOfCare"][(SYSTEM_IFDB_EPISODE_OF_CARE_ID, "EOC-1")] == "eoc-fhir-1"
        assert resolved["Encounter"][(SYSTEM_IFDB_OBJECT_ID, "ENC-2")] == "enc-fhir-2"
        assert resolved["Patient"][(SYSTEM_IFDB_PATIENT_ID, "PAT-1")] == "pat-fhir-1"


class TestProcedureDependencies:
    @patch("DSA.FHIR.Procedure.Procedure_IFDB_AdminProc.post_resources_direct")
    @patch("DSA.FHIR.Procedure.Procedure_IFDB_AdminProc.prepare_resources_for_upsert")
    @patch("DSA.FHIR.utils.ifdb.dependency_resolver.resolve_missing_patients")
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
        ifdb_oracle = MagicMock()

        result = _post_adm_procedure_to_fhir(
            context=context,
            procedures=procedures,
            fhir_database=fhir_database,
            extracted_count=2,
            ifdb_oracle=ifdb_oracle,
        )

        assert result == "posted"
        mock_resolve_eoc.assert_called_once_with(
            eoc_ids={"EOC-1", "EOC-2"},
            ifdb_oracle=ifdb_oracle,
            fhir_database=fhir_database,
            context=context,
        )
        mock_resolve_enc.assert_called_once_with(
            encounter_ids={"ENC-1", "ENC-2"},
            ifdb_oracle=ifdb_oracle,
            fhir_database=fhir_database,
            context=context,
        )
        mock_resolve_pat.assert_called_once_with(
            patient_ids={"PAT-1"},
            ifdb_oracle=ifdb_oracle,
            fhir_database=fhir_database,
            context=context,
        )
        assert mock_prepare.call_args.kwargs["resource_type"] == "Procedure"

    @patch("DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse.post_resources_direct")
    @patch("DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse.prepare_resources_for_upsert")
    @patch("DSA.FHIR.utils.ifdb.dependency_resolver.resolve_missing_patients")
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
        ifdb_oracle = MagicMock()

        result = _post_ris_ydelse_to_fhir(
            context=context,
            procedures=procedures,
            fhir_database=fhir_database,
            extracted_count=2,
            ifdb_oracle=ifdb_oracle,
        )

        assert result == "posted"
        mock_resolve_eoc.assert_called_once_with(
            eoc_ids={"EOC-1", "EOC-2"},
            ifdb_oracle=ifdb_oracle,
            fhir_database=fhir_database,
            context=context,
        )
        mock_resolve_enc.assert_called_once_with(
            encounter_ids={"ENC-1", "ENC-2"},
            ifdb_oracle=ifdb_oracle,
            fhir_database=fhir_database,
            context=context,
        )
        mock_resolve_pat.assert_called_once_with(
            patient_ids={"PAT-1", "PAT-2"},
            ifdb_oracle=ifdb_oracle,
            fhir_database=fhir_database,
            context=context,
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

    def test_raise_if_unresolved_does_not_raise_when_all_required_are_resolved(self) -> None:
        _raise_if_unresolved(
            required_ids={"A", "B"},
            resolved={"A": "uuid-a", "B": "uuid-b"},
            entity_label="patients",
        )

    def test_raise_if_unresolved_raises_with_count_and_truncation(self) -> None:
        required_ids = {f"ID-{i}" for i in range(6)}

        with pytest.raises(ValueError) as exc_info:
            _raise_if_unresolved(
                required_ids=required_ids,
                resolved={},
                entity_label="patients",
            )

        msg = str(exc_info.value)
        assert "Could not resolve 6 required patients:" in msg
        assert "..." in msg

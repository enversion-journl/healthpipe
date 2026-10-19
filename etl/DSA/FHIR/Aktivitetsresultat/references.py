# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Reference lookup and dependency resolution for AKTIVITETSRESULTAT."""

import time
import uuid
from typing import Any

import dagster as dg
from core.resources import (
    FHIRDatabaseResource,
    IdentifierLookup,
    MSSQLResource,
)
from DSA.FHIR.Questionnaire.Questionnaire_IFDB import (
    QUESTIONNAIRE_IDENTIFIER_SYSTEM,
    build_questionnaire_identifier_value,
)
from DSA.FHIR.utils.ifdb.aktivitetsresultat_identifiers import _sfi_identifier_value
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id as _normalize_oracle_id
from DSA.FHIR.utils.ifdb.xml_child_reconcile import FAMILY_SFI_OBS, FAMILY_XML_CONDITION
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SOR,
)
from utils.fhir.index_config import IDENTIFIER_UNIQUE_INDEX_SET


def resolve_procedure_organizations(
    procedure_dicts: list[tuple[str, dict]], fhir_database: FHIRDatabaseResource
) -> None:
    """Opløs chunkens Procedure.performer.actor uden at ændre kliniker-id'er.

    Dict-stien bruger ikke den generelle Pydantic-resolver. Slå kun chunkens
    unikke SOR-id'er op, og bevar identifieren, hvis organisationen ikke findes.
    """
    actors = [
        (actor["identifier"]["value"], actor)
        for _, procedure in procedure_dicts
        for performer in procedure.get("performer", [])
        if (actor := performer.get("actor", {})).get("identifier", {}).get("system") == SYSTEM_SOR
        and actor["identifier"].get("value")
        and not actor.get("id")
    ]
    if not actors:
        return
    resolved = fhir_database.resolve_identifier_values(
        resource_type="Organization",
        identifier_system=SYSTEM_SOR,
        values=sorted({value for value, _ in actors}),
    )
    for value, actor in actors:
        if resource_id := resolved.get(value):
            actor.pop("identifier", None)
            actor.pop("type", None)
            actor.update(id=resource_id, resourceType="Organization")


class ResolvedRefLookup:
    """Pre-resolved FHIR reference lookups for zero-overhead dict construction.

    Wraps identifier lookups provided by ``FHIRDatabaseResource`` and provides O(1)
    convenience methods that return Aidbox-native reference dicts.

    Observation, Procedure, Questionnaire and QuestionnaireResponse *self*-id
    stability is handled by the
    identifier-cache lookup pattern shared with the rest of the FHIR
    transforms: existing Aidbox UUIDs are reused on re-runs so
    ``INSERT ... ON CONFLICT (id) DO UPDATE`` hits the same row;
    cache misses fall back to a fresh ``uuid4()`` and Aidbox auto-
    generates the persisted UUID.
    """

    __slots__ = (
        "_patient",
        "_encounter",
        "_episode",
        "_observation",
        "_procedure",
        "_condition",
        "_questionnaire",
        "_questionnaire_response",
        "_minted_observation_sfi_ids",
        "_minted_procedure_ids",
        "_minted_xml_condition_ids",
        "_minted_questionnaire_ids",
        "_minted_questionnaire_response_ids",
        "_chunk_merged_keys",
        "lookup_profile",
        "miss_counts",
    )

    def __init__(
        self,
        patient: IdentifierLookup,
        encounter: IdentifierLookup,
        episode: IdentifierLookup,
        observation: IdentifierLookup,
        procedure: IdentifierLookup | None = None,
        condition: IdentifierLookup | None = None,
        questionnaire: IdentifierLookup | None = None,
        questionnaire_response: IdentifierLookup | None = None,
    ) -> None:
        self._patient = patient
        self._encounter = encounter
        self._episode = episode
        self._observation = observation
        self._procedure = procedure or {}
        self._condition = condition or {}
        self._questionnaire = questionnaire or {}
        self._questionnaire_response = questionnaire_response or {}
        self._minted_observation_sfi_ids: dict[str, str] = {}
        self._minted_procedure_ids: dict[str, str] = {}
        self._minted_xml_condition_ids: dict[str, str] = {}
        self._minted_questionnaire_ids: dict[str, str] = {}
        self._minted_questionnaire_response_ids: dict[str, str] = {}
        # Opslag flettet ind for den aktuelle chunk (eksisterende ressourcer) —
        # frigives sammen med de mintede id'er, se release_chunk_scoped_ids.
        self._chunk_merged_keys: list[tuple[IdentifierLookup, tuple[str, str]]] = []
        self.lookup_profile: dict[str, Any] = {}
        # Tracks cache misses so SoF NULL-joins can be diagnosed without log-spam.
        self.miss_counts: dict[str, int] = {"Patient": 0, "Encounter": 0, "EpisodeOfCare": 0}

    def lookups_by_resource_type(self) -> dict[str, IdentifierLookup]:
        """Expose backing lookups for core cache diagnostics without leaking internals."""
        return {
            "Patient": self._patient,
            "Encounter": self._encounter,
            "EpisodeOfCare": self._episode,
            "Observation": self._observation,
            "Procedure": self._procedure,
            "Condition": self._condition,
            "Questionnaire": self._questionnaire,
            "QuestionnaireResponse": self._questionnaire_response,
        }

    def patient_ref(self, patient_id: Any) -> dict | None:
        if patient_id is None:
            return None
        pid = _normalize_oracle_id(patient_id)
        fhir_id = self._patient.get((SYSTEM_IFDB_PATIENT_ID, pid))
        if fhir_id:
            return {"id": fhir_id, "resourceType": "Patient"}
        self.miss_counts["Patient"] += 1
        return {"identifier": {"system": SYSTEM_IFDB_PATIENT_ID, "value": pid}}

    def encounter_ref(self, encounter_id: Any) -> dict | None:
        if encounter_id is None:
            return None
        eid = _normalize_oracle_id(encounter_id)
        fhir_id = self._encounter.get((SYSTEM_IFDB_OBJECT_ID, eid))
        if fhir_id:
            return {"id": fhir_id, "resourceType": "Encounter"}
        self.miss_counts["Encounter"] += 1
        return {"identifier": {"system": SYSTEM_IFDB_OBJECT_ID, "value": eid}}

    def episode_ref(self, episode_id: Any) -> dict | None:
        if episode_id is None:
            return None
        eid = _normalize_oracle_id(episode_id)
        fhir_id = self._episode.get((SYSTEM_IFDB_EPISODE_OF_CARE_ID, eid))
        if fhir_id:
            return {"id": fhir_id, "resourceType": "EpisodeOfCare"}
        self.miss_counts["EpisodeOfCare"] += 1
        return {
            "identifier": {
                "system": SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                "value": eid,
            }
        }

    def observation_id(self, objekt_id: Any) -> str:
        if objekt_id is not None:
            oid = _normalize_oracle_id(objekt_id)
            if oid is not None:
                # DIAG: surface sqlite "unable to open database file" on the
                # observation cache distinctly from the procedure cache so we
                # know which one disappeared.
                try:
                    existing = self._observation.get((SYSTEM_IFDB_AKTIVITETSRESULTAT, oid))
                except Exception as _exc:
                    raise type(_exc)(
                        f"[CACHE_DIAG] Observation cache get failed for OBJEKT_ID={objekt_id}: {_exc}"
                    ) from _exc
                if existing:
                    return existing
        return str(uuid.uuid4())

    def observation_id_sfi(self, objekt_id: Any, element_nummer: int | None) -> str:
        """Resolve an Aidbox UUID for a per-SFI-element Observation.

        Cache key is the per-element identifier ``{OBJEKT_ID}#sfi#{element_nummer}``
        under ``SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI``. Existing rows reuse the
        persisted Aidbox id through the lookup cache. Cache misses mint one
        UUID per source SFI key for the current run, so repeated rows for the
        same source datapoint still upsert the same FHIR resource id.
        """
        key_value = None
        if objekt_id is not None and element_nummer is not None:
            oid = _normalize_oracle_id(objekt_id)
            if oid is not None:
                key_value = _sfi_identifier_value(oid, element_nummer)
                try:
                    existing = self._observation.get(
                        (
                            SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
                            key_value,
                        )
                    )
                except Exception as _exc:
                    raise type(_exc)(
                        f"[CACHE_DIAG] Observation cache get failed for SFI "
                        f"OBJEKT_ID={objekt_id} element_nummer={element_nummer}: {_exc}"
                    ) from _exc
                if existing:
                    return existing
        key_value = key_value or FAMILY_SFI_OBS.child_value(
            _normalize_oracle_id(objekt_id) or "", element_nummer or ""
        )
        if key_value not in self._minted_observation_sfi_ids:
            self._minted_observation_sfi_ids[key_value] = str(uuid.uuid4())
        return self._minted_observation_sfi_ids[key_value]

    def release_chunk_scoped_ids(self) -> int:
        """Frigiv chunkens id-tilstand, når chunken er postet.

        To ting lever kun chunken ud:
        - id'er mintet for *nye* ressourcer (QuestionnaireResponse, Procedure,
          SFI-observation, XML-diagnose) — de sikrer kun, at gentagne forekomster
          af samme kildenøgle inden for chunken får samme uuid;
        - opslag flettet ind for *eksisterende* ressourcer via
          ``merge_identifiers`` — næste chunk slår sine egne identifikatorer op
          i databasen først, så de gamle behøves ikke.

        Kildenøglen (OBJEKT_ID, evt. med elementnummer) er arkivets flettenøgle og
        dermed unik pr. række, så en nøgle lander aldrig i to chunks. Uden
        frigivelse vokser begge dele med én post pr. række i hele perioden —
        nye rækker fylder memoerne, genpostede rækker fylder de flettede opslag —
        og barneprocessen dræbes ved store perioder.

        De opslag, der blev opløst før chunk-løkken (partitionens eksisterende
        selv-id'er og relationer), røres ikke. Questionnaire-memoen deles på
        tværs af chunks (samme model/version i mange rækker) og beholdes; den er
        lille. Returnerer antal frigivne poster.
        """
        released = (
            len(self._minted_observation_sfi_ids)
            + len(self._minted_procedure_ids)
            + len(self._minted_xml_condition_ids)
            + len(self._minted_questionnaire_response_ids)
            + len(self._chunk_merged_keys)
        )
        self._minted_observation_sfi_ids.clear()
        self._minted_procedure_ids.clear()
        self._minted_xml_condition_ids.clear()
        self._minted_questionnaire_response_ids.clear()
        for lookup, key in self._chunk_merged_keys:
            lookup.pop(key, None)
        self._chunk_merged_keys.clear()
        return released

    def merge_identifiers(
        self,
        resource_type: str,
        identifier_system: str,
        values_to_ids: dict[str, str],
    ) -> int:
        """Flet opløste identifiers for ``resource_type`` ind i partitionens opslag.

        Én metode for alle familier (Observation, Condition, Procedure, …), så
        fletningen ikke kan drifte mellem dem. Kun rene dict-opslag kan flettes;
        de delte caches er skrivebeskyttede her og giver 0. Nøgler, opslaget
        allerede kender, røres ikke. De tilføjede nøgler huskes, så
        ``release_chunk_scoped_ids`` kan frigive dem igen efter chunken.
        Returnerer antal nye poster.
        """
        if not values_to_ids:
            return 0

        lookup = self.lookups_by_resource_type()[resource_type]
        if not isinstance(lookup, dict):
            return 0

        added = 0
        for source_value, fhir_id in values_to_ids.items():
            key = (identifier_system, str(source_value))
            if key not in lookup:
                lookup[key] = fhir_id
                self._chunk_merged_keys.append((lookup, key))
                added += 1
        return added

    def procedure_id(self, objekt_id: Any) -> str:
        if objekt_id is not None:
            oid = _normalize_oracle_id(objekt_id)
            if oid is not None:
                try:
                    existing = self._procedure.get((SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE, oid))
                except Exception as _exc:
                    raise type(_exc)(
                        f"[CACHE_DIAG] Procedure cache get failed for OBJEKT_ID={objekt_id}: {_exc}"
                    ) from _exc
                if existing:
                    return existing
                if oid not in self._minted_procedure_ids:
                    self._minted_procedure_ids[oid] = str(uuid.uuid4())
                return self._minted_procedure_ids[oid]
        return str(uuid.uuid4())

    def condition_id(self, objekt_id: Any) -> str:
        if objekt_id is not None:
            oid = _normalize_oracle_id(objekt_id)
            if oid is not None:
                try:
                    existing = self._condition.get((SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION, oid))
                except Exception as _exc:
                    raise type(_exc)(
                        f"[CACHE_DIAG] Condition cache get failed for OBJEKT_ID={objekt_id}: {_exc}"
                    ) from _exc
                if existing:
                    return existing
        return str(uuid.uuid4())

    def xml_condition_id(self, objekt_id: Any, element_nummer: Any) -> str:
        oid = _normalize_oracle_id(objekt_id)
        if oid is None or element_nummer is None:
            return str(uuid.uuid4())
        identifier_value = FAMILY_XML_CONDITION.child_value(oid, element_nummer)
        try:
            existing = self._condition.get(
                (
                    SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION,
                    identifier_value,
                )
            )
        except Exception as _exc:
            raise type(_exc)(
                f"[CACHE_DIAG] XML Condition cache get failed for "
                f"OBJEKT_ID={objekt_id} element_nummer={element_nummer}: {_exc}"
            ) from _exc
        if existing:
            return existing
        if identifier_value not in self._minted_xml_condition_ids:
            self._minted_xml_condition_ids[identifier_value] = str(uuid.uuid4())
        return self._minted_xml_condition_ids[identifier_value]

    def questionnaire_id(self, model_name: Any, model_version: Any) -> str:
        identifier_value = build_questionnaire_identifier_value(
            None if model_name is None else str(model_name),
            None if model_version is None else str(model_version),
        )
        try:
            existing = self._questionnaire.get((QUESTIONNAIRE_IDENTIFIER_SYSTEM, identifier_value))
        except Exception as _exc:
            raise type(_exc)(
                f"[CACHE_DIAG] Questionnaire cache get failed for "
                f"model={model_name} version={model_version}: {_exc}"
            ) from _exc
        if existing:
            return existing
        if identifier_value not in self._minted_questionnaire_ids:
            self._minted_questionnaire_ids[identifier_value] = str(uuid.uuid4())
        return self._minted_questionnaire_ids[identifier_value]

    def questionnaire_response_id(self, objekt_id: Any) -> str:
        if objekt_id is not None:
            oid = _normalize_oracle_id(objekt_id)
            if oid is not None:
                try:
                    existing = self._questionnaire_response.get(
                        (
                            SYSTEM_IFDB_AKTIVITETSRESULTAT,
                            oid,
                        )
                    )
                except Exception as _exc:
                    raise type(_exc)(
                        f"[CACHE_DIAG] QuestionnaireResponse cache get failed "
                        f"for OBJEKT_ID={objekt_id}: {_exc}"
                    ) from _exc
                if existing:
                    return existing
        key = _normalize_oracle_id(objekt_id) or ""
        if key not in self._minted_questionnaire_response_ids:
            self._minted_questionnaire_response_ids[key] = str(uuid.uuid4())
        return self._minted_questionnaire_response_ids[key]


def _as_identifier_lookup(
    identifier_system: str,
    resolved_values: dict[str, str],
) -> IdentifierLookup:
    return {
        (identifier_system, str(source_value)): fhir_id
        for source_value, fhir_id in resolved_values.items()
        if source_value is not None and fhir_id
    }


def _resolve_relations_targeted(
    rows: list[dict[str, Any]],
    fhir_database: FHIRDatabaseResource,
    dependency_lookups: dict[str, IdentifierLookup] | None,
) -> tuple[IdentifierLookup, IdentifierLookup, IdentifierLookup, dict[str, Any]]:
    """Resolve Patient/Encounter/EpisodeOfCare references via targeted SQL.

    If ``dependency_lookups`` already contains resolved maps (populated by
    ``_ensure_reference_dependencies``), those are reused directly — zero
    extra Postgres queries.  Otherwise, the unique-index-backed targeted
    SQL path is used (sub-second even for tens of thousands of IDs).
    """
    logger = dg.get_dagster_logger()
    dep = dependency_lookups or {}

    patient_cache: IdentifierLookup = dep.get("Patient", {})
    encounter_cache: IdentifierLookup = dep.get("Encounter", {})
    episode_cache: IdentifierLookup = dep.get("EpisodeOfCare", {})

    if patient_cache and encounter_cache and episode_cache:
        logger.info(
            "[TargetedLookup] Reusing dependency lookups: "
            "Patient=%d, Encounter=%d, EpisodeOfCare=%d",
            len(patient_cache),
            len(encounter_cache),
            len(episode_cache),
        )
        profile = {
            "strategy": "dependency_cached",
            "timing_seconds": 0.0,
            "Patient": {
                "requested": len(patient_cache),
                "resolved": len(patient_cache),
                "misses": 0,
            },
            "Encounter": {
                "requested": len(encounter_cache),
                "resolved": len(encounter_cache),
                "misses": 0,
            },
            "EpisodeOfCare": {
                "requested": len(episode_cache),
                "resolved": len(episode_cache),
                "misses": 0,
            },
        }
        return patient_cache, encounter_cache, episode_cache, profile

    patient_ids = sorted(
        {pid for r in rows if (pid := _normalize_oracle_id(r.get("PATIENT_ID"))) is not None}
    )
    encounter_ids = sorted(
        {eid for r in rows if (eid := _normalize_oracle_id(r.get("ENCOUNTER_ID"))) is not None}
    )
    episode_ids = sorted(
        {
            eid
            for r in rows
            if (eid := _normalize_oracle_id(r.get("EPISODE_OF_CARE_ID"))) is not None
        }
    )

    targeted_start = time.perf_counter()
    if not patient_cache and patient_ids:
        patient_cache = _as_identifier_lookup(
            SYSTEM_IFDB_PATIENT_ID,
            fhir_database.resolve_identifier_values(
                resource_type="Patient",
                identifier_system=SYSTEM_IFDB_PATIENT_ID,
                values=patient_ids,
            ),
        )
    if not encounter_cache and encounter_ids:
        encounter_cache = _as_identifier_lookup(
            SYSTEM_IFDB_OBJECT_ID,
            fhir_database.resolve_identifier_values(
                resource_type="Encounter",
                identifier_system=SYSTEM_IFDB_OBJECT_ID,
                values=encounter_ids,
            ),
        )
    if not episode_cache and episode_ids:
        episode_cache = _as_identifier_lookup(
            SYSTEM_IFDB_EPISODE_OF_CARE_ID,
            fhir_database.resolve_identifier_values(
                resource_type="EpisodeOfCare",
                identifier_system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                values=episode_ids,
            ),
        )
    targeted_elapsed = time.perf_counter() - targeted_start

    patient_profile = {
        "requested": len(patient_ids),
        "resolved": len(patient_cache),
        "misses": max(len(patient_ids) - len(patient_cache), 0),
    }
    encounter_profile = {
        "requested": len(encounter_ids),
        "resolved": len(encounter_cache),
        "misses": max(len(encounter_ids) - len(encounter_cache), 0),
    }
    episode_profile = {
        "requested": len(episode_ids),
        "resolved": len(episode_cache),
        "misses": max(len(episode_ids) - len(episode_cache), 0),
    }
    logger.info(
        "[TargetedLookup] Relation lookups in %.2fs: "
        "Patient=%d/%d, Encounter=%d/%d, EpisodeOfCare=%d/%d",
        targeted_elapsed,
        len(patient_cache),
        len(patient_ids),
        len(encounter_cache),
        len(encounter_ids),
        len(episode_cache),
        len(episode_ids),
    )
    return (
        patient_cache,
        encounter_cache,
        episode_cache,
        {
            "strategy": "targeted",
            "timing_seconds": targeted_elapsed,
            "Patient": patient_profile,
            "Encounter": encounter_profile,
            "EpisodeOfCare": episode_profile,
        },
    )


def _collect_identifier_values(rows: list[dict[str, Any]], extractor) -> list[str]:
    """Collect sorted unique values from rows using a row extractor."""
    values: set[str] = set()
    for row in rows:
        value = extractor(row)
        if value is None:
            continue
        values.add(str(value))
    return sorted(values)


def _log_self_id_lookup_profile(
    logger,
    resource_type: str,
    identifier_system: str,
    strategy: str,
    requested: int,
    resolved: int,
    elapsed: float,
) -> dict[str, Any]:
    """Log and return lookup metadata for one resource/system pair."""
    misses = max(requested - resolved, 0)
    logger.info(
        "[SelfIdLookup] %s/%s strategy=%s resolved=%d requested=%d misses=%d in %.2fs",
        resource_type,
        identifier_system,
        strategy,
        resolved,
        requested,
        misses,
        elapsed,
    )
    return {
        "resource_type": resource_type,
        "identifier_system": identifier_system,
        "strategy": strategy,
        "resolved": resolved,
        "requested": requested,
        "misses": misses,
        "elapsed": elapsed,
    }


def _resolve_indexed_self_ids(
    *,
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    identifier_system: str,
    requested_values: list[str],
) -> tuple[dict[str, str], dict[str, Any]]:
    """Resolve indexed self-id values via targeted PostgreSQL lookup."""
    logger = dg.get_dagster_logger()
    start = time.perf_counter()
    resolved_values = fhir_database.resolve_identifier_values(
        resource_type=resource_type,
        identifier_system=identifier_system,
        values=requested_values,
    )
    elapsed = time.perf_counter() - start
    return resolved_values, _log_self_id_lookup_profile(
        logger,
        resource_type,
        identifier_system,
        "targeted",
        len(requested_values),
        len(resolved_values),
        elapsed,
    )


def _resolve_full_cache_self_ids(
    *,
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    identifier_system: str,
    requested_values: list[str],
) -> tuple[dict[str, str], dict[str, Any]]:
    """Fallback to full shared cache preload for non-indexed self IDs."""
    logger = dg.get_dagster_logger()
    start = time.perf_counter()
    if not requested_values:
        elapsed = time.perf_counter() - start
        return {}, _log_self_id_lookup_profile(
            logger,
            resource_type,
            identifier_system,
            "full-cache",
            0,
            0,
            elapsed,
        )

    lookup = fhir_database.get_identifier_lookup(
        resource_type=resource_type,
        identifier_systems=[identifier_system],
        force_reload=False,
    )
    elapsed = time.perf_counter() - start
    resolved: dict[str, str] = {
        value: lookup[(identifier_system, value)]
        for value in requested_values
        if (identifier_system, value) in lookup
    }
    logger.info(
        "[SelfIdLookup] %s/%s strategy=full-cache warmup loaded %d/entries in %.2fs",
        resource_type,
        identifier_system,
        len(lookup),
        elapsed,
    )
    return resolved, _log_self_id_lookup_profile(
        logger,
        resource_type,
        identifier_system,
        "full-cache",
        len(requested_values),
        len(resolved),
        elapsed,
    )


def _resolve_self_ids(
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    identifier_system: str,
    requested_values: list[str],
) -> tuple[dict[str, str], dict[str, Any]]:
    """Resolve ids by targeted lookup when indexed, full cache otherwise."""
    if (resource_type, identifier_system) in IDENTIFIER_UNIQUE_INDEX_SET:
        return _resolve_indexed_self_ids(
            fhir_database=fhir_database,
            resource_type=resource_type,
            identifier_system=identifier_system,
            requested_values=requested_values,
        )

    return _resolve_full_cache_self_ids(
        fhir_database=fhir_database,
        resource_type=resource_type,
        identifier_system=identifier_system,
        requested_values=requested_values,
    )


def _pre_resolve_references(
    rows: list[dict[str, Any]],
    fhir_database: FHIRDatabaseResource,
    dependency_lookups: dict[str, IdentifierLookup] | None = None,
    include_questionnaire_lookups: bool = False,
) -> ResolvedRefLookup:
    """Batch-resolve all FHIR reference IDs before dict construction.

    Relation references (Patient, Encounter, EpisodeOfCare) are resolved
    via targeted SQL using the unique partial indexes — only the IDs
    present in the current partition are queried, avoiding full-table
    SQLite cache builds.  When ``dependency_lookups`` is populated by
    ``_ensure_reference_dependencies``, those maps are reused directly
    (zero extra queries).

    Self-id lookups (Observation, Procedure, Questionnaire,
    QuestionnaireResponse) still use the shared identifier cache for
    O(1) idempotent-upsert ID stability.
    """
    logger = dg.get_dagster_logger()

    resolve_start = time.perf_counter()
    patient_cache, encounter_cache, episode_cache, relation_profile = _resolve_relations_targeted(
        rows,
        fhir_database,
        dependency_lookups,
    )

    observation_values = _collect_identifier_values(
        rows,
        lambda row: _normalize_oracle_id(row.get("OBJEKT_ID")),
    )
    procedure_values = observation_values.copy()
    condition_values = observation_values.copy()
    questionnaire_response_values = (
        _collect_identifier_values(
            rows,
            lambda row: _normalize_oracle_id(row.get("OBJEKT_ID")),
        )
        if include_questionnaire_lookups
        else []
    )
    questionnaire_model_values = (
        _collect_identifier_values(
            rows,
            lambda row: build_questionnaire_identifier_value(
                row.get("MODELLERINGSNAVN"),
                row.get("MODELLERINGSVERSION"),
            ),
        )
        if include_questionnaire_lookups
        else []
    )

    logger.info(
        "[SelfIdLookup] unique identifiers partition counts: "
        "Observation=%d Procedure=%d Condition=%d Questionnaire=%d QuestionnaireResponse=%d",
        len(observation_values),
        len(procedure_values),
        len(condition_values),
        len(questionnaire_model_values),
        len(questionnaire_response_values),
    )

    lookup_profiles: list[dict[str, Any]] = []
    self_resolve_start = time.perf_counter()

    observation_resolved, profile = _resolve_self_ids(
        fhir_database=fhir_database,
        resource_type="Observation",
        identifier_system=SYSTEM_IFDB_AKTIVITETSRESULTAT,
        requested_values=observation_values,
    )
    lookup_profiles.append(profile)
    observation_cache = _as_identifier_lookup(
        SYSTEM_IFDB_AKTIVITETSRESULTAT,
        observation_resolved,
    )

    procedure_resolved, profile = _resolve_self_ids(
        fhir_database=fhir_database,
        resource_type="Procedure",
        identifier_system=SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
        requested_values=procedure_values,
    )
    lookup_profiles.append(profile)
    procedure_cache = _as_identifier_lookup(
        SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
        procedure_resolved,
    )

    condition_resolved, profile = _resolve_self_ids(
        fhir_database=fhir_database,
        resource_type="Condition",
        identifier_system=SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION,
        requested_values=condition_values,
    )
    lookup_profiles.append(profile)
    condition_cache = _as_identifier_lookup(
        SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION,
        condition_resolved,
    )

    questionnaire_cache = {}
    if include_questionnaire_lookups:
        questionnaire_resolved, profile = _resolve_self_ids(
            fhir_database=fhir_database,
            resource_type="Questionnaire",
            identifier_system=QUESTIONNAIRE_IDENTIFIER_SYSTEM,
            requested_values=questionnaire_model_values,
        )
        lookup_profiles.append(profile)
        questionnaire_cache = _as_identifier_lookup(
            QUESTIONNAIRE_IDENTIFIER_SYSTEM,
            questionnaire_resolved,
        )

        questionnaire_response_resolved, profile = _resolve_self_ids(
            fhir_database=fhir_database,
            resource_type="QuestionnaireResponse",
            identifier_system=SYSTEM_IFDB_AKTIVITETSRESULTAT,
            requested_values=questionnaire_response_values,
        )
        lookup_profiles.append(profile)
        questionnaire_response_cache = _as_identifier_lookup(
            SYSTEM_IFDB_AKTIVITETSRESULTAT,
            questionnaire_response_resolved,
        )
    else:
        questionnaire_response_cache = {}

    self_lookup_elapsed = time.perf_counter() - self_resolve_start
    lookup_profile = {
        "relation": relation_profile,
        "self_id": {
            "timing_seconds": self_lookup_elapsed,
            "lookups": lookup_profiles,
        },
    }
    resolved_count = len(observation_cache) + len(procedure_cache) + len(condition_cache)
    if include_questionnaire_lookups:
        resolved_count += len(questionnaire_cache) + len(questionnaire_response_cache)

    logger.info(
        "[SelfIdLookup] partition warmup in %.2fs: %d cached self-ids",
        self_lookup_elapsed,
        resolved_count,
    )

    resolved_lookup = ResolvedRefLookup(
        patient=patient_cache,
        encounter=encounter_cache,
        episode=episode_cache,
        observation=observation_cache,
        procedure=procedure_cache,
        condition=condition_cache,
        questionnaire=questionnaire_cache,
        questionnaire_response=questionnaire_response_cache,
    )
    resolved_lookup.lookup_profile = lookup_profile
    resolved_lookup.miss_counts = {
        "Patient": relation_profile.get("Patient", {}).get("misses", 0),
        "Encounter": relation_profile.get("Encounter", {}).get("misses", 0),
        "EpisodeOfCare": relation_profile.get("EpisodeOfCare", {}).get("misses", 0),
    }

    resolve_elapsed = time.perf_counter() - resolve_start
    logger.info(
        "[PreResolve] completed in %.2fs: relation=%s, self_id=%s",
        resolve_elapsed,
        relation_profile.get("strategy", "dependency-cached"),
        ", ".join(
            [
                f"{p['resource_type']}/{p['identifier_system']}={p['strategy']} "
                f"(resolved={p['resolved']}, requested={p['requested']})"
                for p in lookup_profiles
            ]
        ),
    )
    return resolved_lookup


def _ensure_reference_dependencies(
    rows: list[dict[str, Any]],
    mssql: MSSQLResource | None,
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext,
) -> dict[str, IdentifierLookup]:
    """Ensure referenced Patient, Encounter and EpisodeOfCare resources exist.

    Cache invalidation is handled automatically by FHIRDatabaseResource
    stale-tracking: dependency resolvers call ``fhir_database.mark_stale()``
    when they mutate resources, and ``resolve_identifier_values`` checks
    ``is_stale()`` before returning cached results.

    Returns already-resolved identifier lookups for callers that need them.
    The AKTIVITETSRESULTAT dict pipeline performs its own target cache
    pre-warm after dependencies are ensured.
    """
    dependency_lookups: dict[str, IdentifierLookup] = {
        "Patient": {},
        "Encounter": {},
        "EpisodeOfCare": {},
    }
    if not mssql:
        return dependency_lookups

    patient_ids: set[str] = set()
    encounter_ids: set[str] = set()
    episode_ids: set[str] = set()
    for row_dict in rows:
        pid = row_dict.get("PATIENT_ID")
        pid_norm = _normalize_oracle_id(pid)
        if pid_norm is not None:
            patient_ids.add(pid_norm)
        encounter_id = row_dict.get("ENCOUNTER_ID")
        encounter_id_norm = _normalize_oracle_id(encounter_id)
        if encounter_id_norm is not None:
            encounter_ids.add(encounter_id_norm)
        episode_id = row_dict.get("EPISODE_OF_CARE_ID")
        episode_id_norm = _normalize_oracle_id(episode_id)
        if episode_id_norm is not None:
            episode_ids.add(episode_id_norm)

    if not patient_ids and not encounter_ids and not episode_ids:
        return dependency_lookups

    from DSA.FHIR.Patient.patient_service import NEED_REFERENCE, ensure_patients
    from DSA.FHIR.utils.ifdb.dependency_resolver import (
        resolve_missing_encounters,
        resolve_missing_episodes_of_care,
    )

    logger = dg.get_dagster_logger()
    if episode_ids:
        logger.info("[DEPENDENCY] Ensuring %d episodes of care exist", len(episode_ids))
        resolved_episodes = resolve_missing_episodes_of_care(
            eoc_ids=episode_ids,
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
        )
        dependency_lookups["EpisodeOfCare"] = _as_identifier_lookup(
            SYSTEM_IFDB_EPISODE_OF_CARE_ID,
            resolved_episodes,
        )
    if encounter_ids:
        logger.info("[DEPENDENCY] Ensuring %d encounters exist", len(encounter_ids))
        resolved_encounters = resolve_missing_encounters(
            encounter_ids=encounter_ids,
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
        )
        dependency_lookups["Encounter"] = _as_identifier_lookup(
            SYSTEM_IFDB_OBJECT_ID,
            resolved_encounters,
        )
    if patient_ids:
        logger.info("[DEPENDENCY] Ensuring %d patients exist", len(patient_ids))
        resolved_patients = ensure_patients(
            ifdb_patient_ids=patient_ids,
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
            need=NEED_REFERENCE,
        ).by_ifdb_id
        dependency_lookups["Patient"] = _as_identifier_lookup(
            SYSTEM_IFDB_PATIENT_ID,
            resolved_patients,
        )

    return dependency_lookups

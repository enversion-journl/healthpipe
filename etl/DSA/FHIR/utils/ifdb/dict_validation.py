# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""FHIR dict validation helpers for Aktivitetsresultat Observation, Procedure and Condition dicts."""

import copy
from typing import Any

from DSA.FHIR.utils.ifdb.aktivitetsresultat_config import _DICT_VALIDATE_SAMPLE
from fhir.resources.R4B.observation import Observation


def _aidbox_ref_to_fhir_ref(d: dict) -> dict:
    """Convert Aidbox reference ``{id, resourceType}`` to standard FHIR ``{reference}``."""
    if "resourceType" in d and "id" in d and "reference" not in d:
        rt = d["resourceType"]
        rid = d["id"]
        return {"reference": f"{rt}/{rid}"}
    return d


def _aidbox_dict_to_fhir_for_validation(obs_dict: dict) -> dict:
    """Deep-copy an Aidbox-formatted resource dict and convert references to standard FHIR.

    The dict pipeline builds Aidbox-native references (``{id, resourceType}``) for
    zero-overhead posting.  Pydantic ``parse_obj`` requires standard FHIR references
    (``{reference: "Type/id"}``), so this helper converts them before validation.
    """
    d = copy.deepcopy(obs_dict)
    _REF_FIELDS = ("subject", "encounter", "performer", "basedOn", "partOf")
    for field in _REF_FIELDS:
        if field in d and isinstance(d[field], dict):
            d[field] = _aidbox_ref_to_fhir_ref(d[field])
        elif field in d and isinstance(d[field], list):
            d[field] = [
                _aidbox_ref_to_fhir_ref(item) if isinstance(item, dict) else item
                for item in d[field]
            ]
    for ext in d.get("extension", []):
        if isinstance(ext, dict) and "valueReference" in ext:
            ext["valueReference"] = _aidbox_ref_to_fhir_ref(ext["valueReference"])
    return d


def _validate_observation_dicts_sample(
    dicts: list[tuple[str, dict]],
    logger: Any,
) -> None:
    """Validate a random sample of built dicts against the Pydantic Observation model.

    Catches structural FHIR errors (wrong types, missing required fields, invalid
    enums) without paying Pydantic cost for every resource.  Controlled via
    ``IFDB_DICT_VALIDATE_SAMPLE`` (default 50, 0 disables).
    """
    if _DICT_VALIDATE_SAMPLE <= 0 or not dicts:
        return

    import random

    sample_size = min(_DICT_VALIDATE_SAMPLE, len(dicts))
    sample = random.sample(dicts, sample_size)
    failures: list[str] = []

    for resource_id, obs_dict in sample:
        try:
            fhir_dict = _aidbox_dict_to_fhir_for_validation(obs_dict)
            Observation.parse_obj(fhir_dict)
        except Exception as exc:
            failures.append(f"OBJEKT_ID={resource_id}: {exc}")

    if failures:
        logger.error(
            "FHIR sample validation failed for %d/%d dicts:\n%s",
            len(failures),
            sample_size,
            "\n".join(failures[:5]),
        )
        raise ValueError(
            f"FHIR sample validation: {len(failures)}/{sample_size} dicts invalid. "
            f"First: {failures[0][:200]}"
        )
    else:
        logger.info("FHIR sample validation passed (%d/%d dicts)", sample_size, len(dicts))


def _validate_procedure_dicts_sample(
    dicts: list[tuple[str, dict]],
    logger: Any,
) -> None:
    """Validate a random sample of Procedure dicts against the Pydantic model."""
    if _DICT_VALIDATE_SAMPLE <= 0 or not dicts:
        return

    import random

    from fhir.resources.R4B.procedure import Procedure as ProcedureModel

    sample_size = min(_DICT_VALIDATE_SAMPLE, len(dicts))
    sample = random.sample(dicts, sample_size)
    failures: list[str] = []

    for resource_id, proc_dict in sample:
        try:
            fhir_dict = _aidbox_dict_to_fhir_for_validation(proc_dict)
            ProcedureModel.parse_obj(fhir_dict)
        except Exception as exc:
            failures.append(f"OBJEKT_ID={resource_id}: {exc}")

    if failures:
        logger.error(
            "Procedure sample validation failed for %d/%d dicts:\n%s",
            len(failures),
            sample_size,
            "\n".join(failures[:5]),
        )
        raise ValueError(
            f"Procedure sample validation: {len(failures)}/{sample_size} dicts invalid. "
            f"First: {failures[0][:200]}"
        )
    else:
        logger.info("Procedure sample validation passed (%d/%d dicts)", sample_size, len(dicts))


def _validate_condition_dicts_sample(
    dicts: list[tuple[str, dict]],
    logger: Any,
) -> None:
    """Validate a random sample of Condition dicts against the Pydantic model."""
    if _DICT_VALIDATE_SAMPLE <= 0 or not dicts:
        return

    import random

    from fhir.resources.R4B.condition import Condition as ConditionModel

    sample_size = min(_DICT_VALIDATE_SAMPLE, len(dicts))
    sample = random.sample(dicts, sample_size)
    failures: list[str] = []

    for resource_id, cond_dict in sample:
        try:
            fhir_dict = _aidbox_dict_to_fhir_for_validation(cond_dict)
            ConditionModel.parse_obj(fhir_dict)
        except Exception as exc:
            failures.append(f"OBJEKT_ID={resource_id}: {exc}")

    if failures:
        logger.error(
            "Condition sample validation failed for %d/%d dicts:\n%s",
            len(failures),
            sample_size,
            "\n".join(failures[:5]),
        )
        raise ValueError(
            f"Condition sample validation: {len(failures)}/{sample_size} dicts invalid. "
            f"First: {failures[0][:200]}"
        )
    else:
        logger.info("Condition sample validation passed (%d/%d dicts)", sample_size, len(dicts))

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Chunkens id-tilstand lever kun chunken ud — ellers vokser hukommelsen med perioden.

Fejl disse tests fanger:
- memoerne for nye ressourcer frigives ikke mellem chunks (én post pr. ny
  ressource i hele perioden -> barneprocessen dræbes ved store backfills)
- opslag flettet ind for eksisterende ressourcer frigives ikke (samme vækst
  ved genpost af allerede postede rækker)
- frigivelsen rammer Questionnaire-memoen eller partitionens opslag fra før
  chunk-løkken
- frigivelsen ødelægger stabiliteten inden for chunken
"""

from __future__ import annotations

import pytest
from DSA.FHIR.Aktivitetsresultat.references import ResolvedRefLookup
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION,
)

pytestmark = pytest.mark.unit


def _tom_lookup() -> ResolvedRefLookup:
    return ResolvedRefLookup(patient={}, encounter={}, episode={}, observation={})


def test_samme_noegle_i_samme_chunk_giver_samme_id():
    r = _tom_lookup()
    assert r.questionnaire_response_id("42") == r.questionnaire_response_id("42")
    assert r.procedure_id("42") == r.procedure_id("42")
    assert r.observation_id_sfi("42", 7) == r.observation_id_sfi("42", 7)
    assert r.xml_condition_id("42", 3) == r.xml_condition_id("42", 3)


def test_frigivelse_toemmer_raekke_memoerne_og_beholder_questionnaire():
    r = _tom_lookup()
    q_id = r.questionnaire_id("SKEMA", "3")
    for oid in ("1", "2", "3"):
        r.questionnaire_response_id(oid)
        r.procedure_id(oid)
        r.observation_id_sfi(oid, 1)
        r.xml_condition_id(oid, 1)

    assert r.release_chunk_scoped_ids() == 12
    assert r.release_chunk_scoped_ids() == 0
    assert r.questionnaire_id("SKEMA", "3") == q_id


def test_flettede_chunk_opslag_frigives_men_partitionens_opslag_bevares():
    r = ResolvedRefLookup(
        patient={},
        encounter={},
        episode={},
        observation={(SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI, "gammel#sfi#1"): "obs-fra-start"},
        procedure={(SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE, "gammel"): "proc-fra-start"},
    )
    lookups = r.lookups_by_resource_type()
    assert (
        r.merge_observation_identifiers(SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI, {"a#sfi#1": "o1"}) == 1
    )
    assert (
        r.merge_condition_identifiers(SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION, {"a/1": "c1"})
        == 1
    )
    assert r.merge_procedure_identifiers(SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE, {"a": "p1"}) == 1
    # inden for chunken: opslaget virker
    assert lookups["Observation"][(SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI, "a#sfi#1")] == "o1"

    assert r.release_chunk_scoped_ids() == 3
    # chunkens opslag er væk — næste chunk slår selv op i databasen
    assert (SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI, "a#sfi#1") not in lookups["Observation"]
    assert (SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION, "a/1") not in lookups["Condition"]
    assert (SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE, "a") not in lookups["Procedure"]
    # partitionens opslag fra før chunk-løkken bevares
    assert r.observation_id_sfi("gammel", 1) == "obs-fra-start"
    assert r.procedure_id("gammel") == "proc-fra-start"
    assert r.release_chunk_scoped_ids() == 0


def test_eksisterende_ressourcer_paavirkes_ikke_af_frigivelse():
    r = ResolvedRefLookup(
        patient={},
        encounter={},
        episode={},
        observation={},
        procedure={(SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE, "42"): "proc-uuid"},
        questionnaire_response={(SYSTEM_IFDB_AKTIVITETSRESULTAT, "42"): "qr-uuid"},
    )
    r.release_chunk_scoped_ids()
    assert r.procedure_id("42") == "proc-uuid"
    assert r.questionnaire_response_id("42") == "qr-uuid"
    assert r.release_chunk_scoped_ids() == 0

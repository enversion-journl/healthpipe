# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Kontrakttests for genkørsels-nulstillingen.

Fejl disse tests fanger:
- et fremmed spors tabel eller checkpoint sniger sig ind i arbejdslisterne
- SFI-lageret får (forkert) sit eget checkpoint, eller mister sin vandmærke-række
- allow-listen og Aidbox-arbejdslisten glider fra hinanden
- bekræftelses-spærren holder op med at afvise refleks-launches
"""

from __future__ import annotations

import logging

import dagster as dg
import pytest
from adhoc.sfi_genkoersel_nulstilling import (
    AIDBOX_RESSOURCETABELLER,
    FHIR_SERVER_CHECKPOINTS,
    FHIR_SERVER_CHECKPOINTS_UDEN_SEED,
    FLADE_FAMILIETABELLER,
    IDENTIFIER_ALLOWLIST,
    SFI_STORE_TABEL,
    SfiGenkoerselConfig,
    _kraev_server_pind,
    aidbox_arbejdsliste,
    dfmstate_where,
    pg_truncate_statement,
    sfi_genkoersel_nulstilling,
)
from definitions import defs

pytestmark = pytest.mark.unit

FREMMEDE_SPOR = (
    "patient",
    "encounter",
    "episodeofcare",
    "location",
    "organization",
    "servicerequest",
    "documentreference",
    "appointment",
    "careplan",
)


def test_aidbox_arbejdsliste_er_grund_plus_history():
    liste = aidbox_arbejdsliste()
    assert len(liste) == 2 * len(AIDBOX_RESSOURCETABELLER)
    for t in AIDBOX_RESSOURCETABELLER:
        assert t in liste
        assert f"{t}_history" in liste


def test_truncate_statement_citerer_og_udelader_cascade():
    statement = pg_truncate_statement(aidbox_arbejdsliste())
    # "procedure" og "condition" er nøgleord i Postgres — citaterne er værnet.
    assert '"procedure"' in statement
    assert '"questionnaireresponse_history"' in statement
    assert "CASCADE" not in statement
    for fremmed in FREMMEDE_SPOR:
        assert f'"{fremmed}"' not in statement


def test_allowlist_daekker_praecis_aidbox_tabellerne():
    assert set(IDENTIFIER_ALLOWLIST) == set(AIDBOX_RESSOURCETABELLER)
    for systemer in IDENTIFIER_ALLOWLIST.values():
        assert systemer  # en tom allow-liste ville afvise alt
    # Besluttet: senge/kapacitet-sporets prøvedata ryger med i tømningen og
    # genindlæses af dét spors egen pipeline — ikke af vores genkørsel.
    assert "urn:source:ifdb:sengeenhed_kapacitet" in IDENTIFIER_ALLOWLIST["observation"]


def test_dfmstate_where_rammer_begge_scopes_og_alle_poster():
    where = dfmstate_where()
    assert "N'staging'" in where
    # DSO-syncens vandmærker bor i stagings DFMState under scope 'dso' —
    # overlever de, ser DSO-synken de genopbyggede rækker som allerede synkede.
    assert "[scope] = N'dso'" in where
    for navn in FHIR_SERVER_CHECKPOINTS:
        assert f"N'{navn}'" in where
    for navn in FHIR_SERVER_CHECKPOINTS_UDEN_SEED:
        assert f"N'{navn}'" in where
    # ServiceRequest er med som leverandør af condition-tabellens femte
    # diagnosevej — glemmes dens full_load, redder det slettede checkpoint
    # genopbygningen ved næste delta.
    assert "N'ServiceRequest'" in where
    for navn in FLADE_FAMILIETABELLER:
        assert f"N'{navn}'" in where
    # Et tømt SFI-lager uden checkpoint genopbygges ved næste refresh.
    assert (
        f"[scope] = N'sfi_store' AND [schema_name] = N'fhir' AND [table_name] = N'{SFI_STORE_TABEL}'"
        in where
    )
    assert "Patient" not in where
    assert "Encounter" not in where


def test_dfmstate_where_kun_sof_freder_de_seedbare_fhir_server_poster():
    # Ved seeding slettes SOF- og DSO-rækkerne; de seedbare fhir_server-rækker
    # MERGE's til seed-værdien, så der aldrig findes et vindue uden checkpoint.
    where = dfmstate_where(kun_sof=True)
    for navn in FLADE_FAMILIETABELLER:
        assert f"N'{navn}'" in where
    assert "[scope] = N'dso'" in where
    for navn in FHIR_SERVER_CHECKPOINTS:
        assert f"N'{navn}'" not in where
    # Kapacitets-checkpointet er ikke vores spor og seedes aldrig: observation-
    # tabellen tømmes med, så sporet skal læse hele sit arkiv igen.
    for navn in FHIR_SERVER_CHECKPOINTS_UDEN_SEED:
        assert f"N'{navn}'" in where


def test_uden_seed_posterne_er_ikke_blandt_de_seedbare():
    assert not set(FHIR_SERVER_CHECKPOINTS_UDEN_SEED) & set(FHIR_SERVER_CHECKPOINTS)


def test_forkert_bekraeftelse_afvises_foer_noget_roeres():
    # Attrap-resources: spærren skal ramme, før nogen forbindelse overhovedet åbnes.
    ctx = dg.build_asset_context(
        resources={
            "mssql_target_staging": object(),
            "mssql_target_dso": object(),
            "fhir_database": object(),
        }
    )
    with pytest.raises(dg.Failure, match="bekraeft"):
        sfi_genkoersel_nulstilling(ctx, SfiGenkoerselConfig(bekraeft="forkert", dry_run=True))


def test_server_pind_skelner_miljoeer():
    # Dev og QA deler databasenavne; kun servernavnet skiller dem. Pinden
    # kræves ved rigtig kørsel og håndhæves også i dry-run, når den er sat.
    log = logging.getLogger("test")
    identiteter = {
        "staging": ("DEVSRV1", "healthpipe_staging"),
        "dso": ("DEVSRV2", "healthpipe_dso"),
    }

    # Tom pind: dry-run advarer kun; rigtig kørsel afvises.
    _kraev_server_pind(log, {"staging": "", "dso": ""}, identiteter, dry_run=True)
    with pytest.raises(dg.Failure, match="bekraeft_staging_server"):
        _kraev_server_pind(log, {"staging": "", "dso": ""}, identiteter, dry_run=False)

    # Forkert pind afvises — også i dry-run.
    with pytest.raises(dg.Failure, match="dso"):
        _kraev_server_pind(log, {"staging": "devsrv1", "dso": "QASRV"}, identiteter, dry_run=True)

    # Korrekt pind passerer, uafhængigt af store/små bogstaver.
    _kraev_server_pind(log, {"staging": "devsrv1", "dso": "DEVSRV2"}, identiteter, dry_run=False)


def test_asset_er_registreret_i_definitions():
    graph = defs.resolve_asset_graph()
    noegler = {key.to_user_string() for key in graph.get_all_asset_keys()}
    assert any("sfi_genkoersel_nulstilling" in noegle for noegle in noegler)

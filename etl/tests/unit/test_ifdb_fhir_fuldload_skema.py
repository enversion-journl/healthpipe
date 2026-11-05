# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Det daglige skema for fuldindlæsningen: tidsrum, config, spærre og standardtilstand.

Fejl disse tests fanger:
- skemaets run_config passer ikke til asset'ets config, så hver kørsel dør ved
  opstart (op-nøgle eller feltnavn ændret)
- skemaet tændes automatisk ved merge, også på production, hvor ingen har taget
  stilling til, om historikken skal køres
- tidsrummet flyttes ind oven i den daglige kæde 06:00
- tidsbudgettet fjernes, så én kørsel kun tager ét vindue
- skemaet starter en fuldindlæsning oven i en, sensoren allerede har startet
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import dagster as dg
import pytest
from DSA.SQL.dfm.ifdb_fhir_fuldload import FuldloadConfig
from schedules.ifdb_fhir_fuldload_schedules import FULDLOAD_RUN_CONFIG
from schedules.ifdb_fhir_fuldload_schedules import ifdb_fhir_fuldload_schedule as skema

pytestmark = pytest.mark.unit


def _asset_config() -> dict:
    return FULDLOAD_RUN_CONFIG["ops"]["ifdb_fhir_fuldload"]["config"]


def _evaluer(monkeypatch, aktiv_run_id: str | None = None):
    records = [SimpleNamespace(dagster_run=SimpleNamespace(run_id=aktiv_run_id))]
    opslag = MagicMock(return_value=records if aktiv_run_id else [])
    with dg.instance_for_test() as instance:
        monkeypatch.setattr(type(instance), "get_run_records", opslag)
        return skema(dg.build_schedule_context(instance=instance)), opslag


def test_skemaet_starter_efter_den_daglige_kaede_og_stopper_foer_den_naeste():
    assert skema.cron_schedule == "0 10 * * *"
    assert skema.execution_timezone == "Europe/Copenhagen"
    # Den daglige kæde starter 06:00. koer_indtil er det klokkeslæt, kørslen skal være
    # færdig; et vindue begynder kun, hvis det når det, så der er en halv times luft.
    assert _asset_config()["koer_indtil"] < "06:00"


def test_tidsbudgettet_styrer_kørslen_ikke_antallet_af_vinduer():
    config = _asset_config()
    assert (
        config["max_vinduer"] >= 10
    ), "Med standard på 1 tager en kørsel kun ét vindue, og tidsbudgettet er pynt."


def test_run_config_passer_paa_asset_ets_egen_config():
    """Op-nøgle og feltnavne skal svare til asset'et, ellers dør hver kørsel
    ved opstart — og det opdages først dagen efter."""
    config = _asset_config()
    kendte = set(FuldloadConfig.model_fields)
    ukendte = set(config) - kendte
    assert not ukendte, f"skemaet sætter felter, asset'et ikke har: {ukendte}"
    # Værdierne skal også kunne opløses af Dagsters egen validering.
    FuldloadConfig(**config)


def test_skemaet_starter_med_tidsbudgettet(monkeypatch):
    anmodning, _ = _evaluer(monkeypatch)
    assert isinstance(anmodning, dg.RunRequest)
    assert anmodning.run_config == FULDLOAD_RUN_CONFIG


def test_skemaet_springer_over_mens_en_fuldindlaesning_koerer(monkeypatch):
    resultat, opslag = _evaluer(monkeypatch, aktiv_run_id="fuldload-7")
    assert isinstance(resultat, dg.SkipReason) and "fuldload-7" in resultat.skip_message
    assert opslag.call_args.kwargs["filters"].job_name == "ifdb_fhir_fuldload_job"


def test_spaerren_kigger_paa_det_postfixede_job(monkeypatch):
    monkeypatch.setenv("DAGSTER_BRANCH_POSTFIX", "feature-x")
    _, opslag = _evaluer(monkeypatch)
    assert opslag.call_args.kwargs["filters"].job_name == "ifdb_fhir_fuldload_job_feature_x"


def test_skemaet_er_slukket_som_udgangspunkt():
    """Fuldindlæsningen kører år af historik igennem. Hvilket miljø der skal det,
    og hvornår, er en beslutning et menneske træffer — ikke noget, der følger med
    en merge til development eller production."""
    assert skema.default_status == dg.DefaultScheduleStatus.STOPPED


def test_skemaet_peger_paa_fuldindlaesningens_eget_job():
    assert skema.job_name == "ifdb_fhir_fuldload_job"

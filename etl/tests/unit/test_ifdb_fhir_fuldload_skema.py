# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Aftenskemaet for fuldindlæsningen: tidsrum, config og standardtilstand.

Fejl disse tests fanger:
- skemaets run_config passer ikke til asset'ets config, så hver natkørsel dør ved
  opstart (op-nøgle eller feltnavn ændret)
- skemaet tændes automatisk ved merge, også på production, hvor ingen har taget
  stilling til, om historikken skal køres
- tidsrummet flyttes ind oven i den daglige kæde 06:00
- tidsbudgettet fjernes, så én natkørsel kun tager ét vindue
"""

from __future__ import annotations

import dagster as dg
import pytest
from infrastructure.assets.ifdb_fhir_fuldload import FuldloadConfig
from schedules.ifdb_fhir_fuldload_schedules import FULDLOAD_RUN_CONFIG
from schedules.ifdb_fhir_fuldload_schedules import ifdb_fhir_fuldload_nightly_schedule as skema

pytestmark = pytest.mark.unit


def _asset_config() -> dict:
    return FULDLOAD_RUN_CONFIG["ops"]["ifdb_fhir_fuldload"]["config"]


def test_skemaet_koerer_om_natten_og_er_ude_af_vejen_foer_den_daglige_kaede():
    assert skema.cron_schedule == "0 22 * * *"
    assert skema.execution_timezone == "Europe/Copenhagen"
    # Den daglige kæde starter 06:00. koer_indtil er det klokkeslæt, natten skal være
    # færdig; et vindue begynder kun, hvis det når det, så der er en halv times luft.
    assert _asset_config()["koer_indtil"] < "06:00"


def test_tidsbudgettet_styrer_natten_ikke_antallet_af_vinduer():
    config = _asset_config()
    assert (
        config["max_vinduer"] >= 10
    ), "Med standard på 1 tager en natkørsel kun ét vindue, og tidsbudgettet er pynt."


def test_run_config_passer_paa_asset_ets_egen_config():
    """Op-nøgle og feltnavne skal svare til asset'et, ellers dør hver natkørsel
    ved opstart — og det opdages først næste morgen."""
    config = _asset_config()
    kendte = set(FuldloadConfig.model_fields)
    ukendte = set(config) - kendte
    assert not ukendte, f"skemaet sætter felter, asset'et ikke har: {ukendte}"
    # Værdierne skal også kunne opløses af Dagsters egen validering.
    FuldloadConfig(**config)


def test_skemaet_er_slukket_som_udgangspunkt():
    """Fuldindlæsningen kører år af historik igennem. Hvilket miljø der skal det,
    og hvornår, er en beslutning et menneske træffer — ikke noget, der følger med
    en merge til development eller production."""
    assert skema.default_status == dg.DefaultScheduleStatus.STOPPED


def test_skemaet_peger_paa_fuldindlaesningens_eget_job():
    assert skema.job_name == "ifdb_fhir_fuldload_job"

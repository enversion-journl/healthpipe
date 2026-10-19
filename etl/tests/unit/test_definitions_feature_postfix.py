# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Code-locationen skal kunne indlæses som feature-deployment.

På en feature-deployment postfixes asset-, job- og skemanavne, men op-nøglerne i
et jobs standard-config gør ikke. Et job, der sætter sin config med en postfixet
op-nøgle, afvises først, når Dagster bygger jobbets config-skema — og det gør
code-serveren ved indlæsning, så hele locationen falder, og ingen af dens skemaer
kører. ``get_repository_def()`` alene bygger ikke skemaerne og fanger det ikke.

Postfixet læses ved import, så indlæsningen skal ske i en frisk proces.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

ETL_DIR = Path(__file__).resolve().parents[2]

_INDLAES = (
    "import definitions\n"
    "fejl = []\n"
    "for job in definitions.defs.get_repository_def().get_all_jobs():\n"
    "    try:\n"
    "        job.run_config_schema\n"
    "    except Exception as e:\n"
    "        fejl.append(f'{job.name}: {type(e).__name__}')\n"
    "assert not fejl, fejl\n"
)


_KOE_RUN_CONFIG = (
    "from datetime import date\n"
    "import definitions\n"
    "from dagster._config import validate_config\n"
    "from schedules.drg_schedules import EXT_ASSET, KADENCER, queue_run_request\n"
    "repo = definitions.defs.get_repository_def()\n"
    "fejl = []\n"
    "for skema in repo.schedule_defs:\n"
    "    job = repo.get_job(skema.job_name)\n"
    "    noder = {node.name for node in job.graph.nodes}\n"
    "    for leverancetype, kadencer in KADENCER.items():\n"
    "        if EXT_ASSET[leverancetype] not in noder:\n"
    "            continue\n"
    "        for kadence in kadencer:\n"
    "            anmodning = queue_run_request(leverancetype, ['T26', kadence], [], date.today())\n"
    "            validering = validate_config(job.run_config_schema.config_type, anmodning.run_config)\n"
    "            if not validering.success:\n"
    "                fejl.append(f'{job.name} {leverancetype}/{kadence}: {validering.errors[0].message}')\n"
    "assert not fejl, fejl\n"
)


def _koer_med_feature_postfix(script: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "DAGSTER_BRANCH_POSTFIX": "feature-development"}
    return subprocess.run(
        [sys.executable, "-c", script],
        cwd=ETL_DIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )


@pytest.mark.unit
def test_definitions_indlaeses_med_feature_postfix() -> None:
    resultat = _koer_med_feature_postfix(_INDLAES)
    assert resultat.returncode == 0, resultat.stderr[-3000:]


@pytest.mark.unit
def test_drg_koeens_run_config_passer_til_jobbet_med_feature_postfix() -> None:
    resultat = _koer_med_feature_postfix(_KOE_RUN_CONFIG)
    assert resultat.returncode == 0, resultat.stderr[-3000:]

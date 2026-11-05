# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dagligt skema for fuldindlæsningen af IFDB → FHIR-historikken."""

import dagster as dg
from core.branch_postfix import get_postfixed_name
from DSA.SQL.dfm.ifdb_fhir_fuldload import active_fuldload_run, ifdb_fhir_fuldload_job

# Tidsrummet: 10:00, når morgenkæden 06:00 er færdig, til 05:30, hvor den skal være
# færdig. Et nyt vindue begynder kun, hvis det når at blive færdigt inden da, regnet på
# kørslens længste vindue; et halvt vindue er ikke en tilstand, systemet kender. Kører
# den daglige kæde alligevel, begynder hverken posten eller sof_dso, og vinduet tages
# forfra ved næste start. ifdb_fhir_fuldload_after_daily_sensor starter i stedet lige
# efter en grøn morgenkæde; skemaet er til et miljø, hvor man vil have et fast klokkeslæt.
FULDLOAD_START = "0 10 * * *"
FULDLOAD_STOP = "05:30"

# Modulkonstant, ikke kun et argument: testene og sensoren skal kunne se præcis det,
# skemaet kører med.
FULDLOAD_RUN_CONFIG = {
    "ops": {
        "ifdb_fhir_fuldload": {
            "config": {
                # Tidsbudgettet styrer, ikke antallet: loftet er sat højt nok til
                # aldrig at være det, der stopper kørslen.
                "koer_indtil": FULDLOAD_STOP,
                "max_vinduer": 99,
            }
        }
    }
}


@dg.schedule(
    name="ifdb_fhir_fuldload_schedule",
    job=ifdb_fhir_fuldload_job,
    cron_schedule=FULDLOAD_START,
    execution_timezone="Europe/Copenhagen",
    # Bevidst IKKE get_schedule_default_status(): den tænder automatisk på både
    # development og production. Fuldindlæsningen kører år af historik igennem,
    # og hvilket miljø der skal det, og hvornår, er en beslutning et menneske
    # træffer — ikke noget, der følger med en merge. Tændes i Dagster pr. miljø.
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description=(
        "Fuldindlæsning af IFDB → FHIR-historikken, hver dag fra 10:00, når morgenkæden er "
        "færdig; et nyt vindue begynder kun, hvis det efter kørslens længste vindue når at "
        "blive færdigt inden 05:30. Springer over, mens en fuldindlæsning kører. Asset'et "
        "tager selv den ældste måned, der mangler, og melder færdig, når historikken er "
        "inde. Slukket som udgangspunkt og tændes bevidst pr. miljø."
    ),
)
def ifdb_fhir_fuldload_schedule(context: dg.ScheduleEvaluationContext):
    active = active_fuldload_run(context.instance, get_postfixed_name(ifdb_fhir_fuldload_job.name))
    if active is not None:
        return dg.SkipReason(f"Fuldindlæsningen {active} kører stadig.")
    return dg.RunRequest(run_config=FULDLOAD_RUN_CONFIG)

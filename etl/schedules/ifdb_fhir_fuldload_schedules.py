# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Natligt skema for fuldindlæsningen af IFDB → FHIR-historikken."""

import dagster as dg
from infrastructure.assets.ifdb_fhir_fuldload import ifdb_fhir_fuldload_job

# Tidsrummet: 22:00, hvor ingen sidder og arbejder i miljøet, til 05:30, hvor den
# skal være færdig. Et nyt vindue begynder kun, hvis det når at blive færdigt
# inden da, regnet på nattens længste vindue; et halvt vindue er ikke en
# tilstand, systemet kender. Kører den daglige kæde alligevel, begynder hverken
# posten eller sof_dso, og vinduet tages forfra næste nat.
#
# Grænsen er sat af den daglige kæde 06:00, ikke af de øvrige natjobs. Målt på dev
# tager de tilsammen under en time fordelt over hele natten (linked_server 0,2 min,
# erp 8,4, dim_dato 0,1, organisation 7,7, security 0,2, haiba 8,5).
FULDLOAD_START = "0 22 * * *"
FULDLOAD_STOP = "05:30"

# Modulkonstant, ikke kun et argument: ScheduleDefinition gemmer sin run_config
# bag en funktion, så den ikke kan læses tilbage. Testene skal kunne se præcis
# det, natten kører med.
FULDLOAD_RUN_CONFIG = {
    "ops": {
        "ifdb_fhir_fuldload": {
            "config": {
                # Tidsbudgettet styrer, ikke antallet: loftet er sat højt nok til
                # aldrig at være det, der stopper natten.
                "koer_indtil": FULDLOAD_STOP,
                "max_vinduer": 99,
            }
        }
    }
}

ifdb_fhir_fuldload_nightly_schedule = dg.ScheduleDefinition(
    name="ifdb_fhir_fuldload_nightly_schedule",
    job=ifdb_fhir_fuldload_job,
    cron_schedule=FULDLOAD_START,
    execution_timezone="Europe/Copenhagen",
    # Bevidst IKKE get_schedule_default_status(): den tænder automatisk på både
    # development og production. Fuldindlæsningen kører år af historik igennem,
    # og hvilket miljø der skal det, og hvornår, er en beslutning et menneske
    # træffer — ikke noget, der følger med en merge. Tændes i Dagster pr. miljø.
    default_status=dg.DefaultScheduleStatus.STOPPED,
    run_config=FULDLOAD_RUN_CONFIG,
    description=(
        "Fuldindlæsning af IFDB → FHIR-historikken, hver nat fra 22:00; et nyt vindue "
        "begynder kun, hvis det efter nattens længste vindue når at blive færdigt inden "
        "05:30. Asset'et tager selv den ældste måned, der mangler, kører hele kæden "
        "for den, og rykker sin markør, når vinduet er grønt. Er historikken inde, "
        "melder den færdig og gør ingenting. Skemaet er slukket som udgangspunkt og "
        "tændes bevidst pr. miljø."
    ),
)

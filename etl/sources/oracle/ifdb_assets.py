# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import dagster as dg
from core.resources import IFDBOracleResource


def _observe_table_metadata(
    context: dg.AssetExecutionContext, ifdb_oracle: IFDBOracleResource, table_name: str
) -> dg.ObserveResult:
    """
    Helper function to observe table metadata from Oracle database.
    Executes a count query to get row count.
    """
    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        cursor.close()

        return dg.ObserveResult(
            metadata={
                "row_count": row_count,
                "table_name": table_name,
                "database": "IFDB Oracle",
            }
        )


# Define all IFDB Oracle table names
IFDB_TABLES_NAMES = [
    "ABSENCE",
    "ADGANGSLOG",
    "ADGANGS_OBJEKTER",
    "ADMINISTRATIVE_PROCEDURE",
    "ADM_PROCEDUREKODE",
    "ADM_PROCEDUREKODE_TILLAEGSKODE",
    "ADM_PROCEDURE_SUPPL_CODES",
    "AFDELINGSFORLOEB",
    "AKTIVITETSRESULTAT",
    "AKTIVITETSUDFOERELSE",
    "AKTIVITETSUDFOERELSE_SPECIALE",
    "ANDENCAVE",
    "BETALERINTERVAL",
    "BETALERREGISTER",
    "BLACKLIST_DATAWAREHOUSE",
    "BLACKLIST_PIDB",
    "BLACKLIST_PIDB_REV",
    "BOM_AFLYSTOPERATION",
    "BOM_PERIODE",
    "CARE_DISCHARGE_REPORT",
    "CARE_DISCHARGE_REPORT_RECEIVER",
    "CARETEAM",
    "CARETEAMPARTICIPANT",
    "DIAGNOSE",
    "DIAGNOSE_RESUME",
    "DIAGNOSE_TILLAEGSKODE",
    "DISPENSED_MEDICATION",
    "ENCOUNTER",
    "ENCOUNTER_BED_SPACE_HISTORY",
    "EPIKRISE",
    "EPIKRISE_DIAGNOSE",
    "EPIKRISE_DIAGNOSE_TILKODE",
    "EPIKRISE_MODTAGER",
    "EPIKRISE_OPERATION",
    "EPIKRISE_OPERATION_TILKODE",
    "EPIKRISE_TEKST",
    "EPISODE_OF_CARE",
    "EVENT_FLOW",
    "EVENTMARKER",
    "FAERDIGBEHANDLING",
    "FEJLADMINISTRATION",
    "GRAVIDITETSPERIODE",
    "HEALTHISSUE",
    "HEALTHY_COMPANION_PERIOD",
    "HENVISNING",
    "HENVISNING_HISTORIK",
    "HOSPITAL",
    "IMPORTANT_INFORMATION",
    "JOB_ERRORS",
    "JOB_LOG",
    "JOB_STATUS",
    "KA_PRODUKT_VED_HAENDELSE",
    "KA_PRODUKT_VED_SKADESMEKANISME",
    "KOMMUNE_ADVIS",
    "KOMMUNE_ADVIS_DETAIL",
    "KONTAKTAARSAG",
    "KONTAKT_AFDELINGSFORLOEB_REF",
    "KONTAKT_AMB_OPHOLD_HISTORIK",
    "KONTAKT_AMBULANT",
    "KONTAKT_FORSKNING",
    "KONTAKT_INDLAEGGELSE",
    "KONTAKT_INDL_FRAVAER",
    "KONTAKT_INDL_OPHOLD_HISTORIK",
    "KONTAKTPERIODE",
    "KONTAKT_SKADE",
    "KONTAKT_SKADE_ASSISTANCE",
    "LOCATION",
    "MEDADMSTATUSHISTORY",
    "MEDDELELSESBREV",
    "MEDICATION_BAG",
    "MEDICATIONORDERPLAN",
    "MEDICATIONORDERPLANDOSE",
    "MEDICINADMINISTRATION",
    "MEDICINADMINISTRATION_MIXTURE_INGREDIENT",
    "MEDICINCAVE",
    "MEDICINKORTLOG",
    "MEDICINORDINATION",
    "MEDICINORDINATION_MIXTURE_INGREDIENT",
    "MIGRATION_HISTORY",
    "NEGATIVE_CONSENT",
    "ORD_BEGRUNDELSE_SPECIALE",
    "ORDINATIONSBEGRUNDELSE",
    "PAS_DIAGNOSE",
    "PAS_DIAGNOSE_ENCOUNTER_REF",
    "PAS_DIAGNOSE_HENVISNING_REF",
    "PAS_DIAGNOSE_PERIODE_REF",
    "PAS_DIAGNOSE_TILLAEGSKODE",
    "PASYDELSE",
    "PATIENT_CONTACT_INFO",
    "PATIENT_MIDLERTIDIG_ADRESSE",
    "PATIENT_OPLYSNINGER_COLUMNA",
    "PATIENT_OPLYSNINGER_OFFICIELLE",
    "PATIENT_TIDLIGERE_ADRESSE",
    "PAYMENTINFORMATION",
    "PAYOR",
    "PIDB_INDLAEGGELSER",
    "PIDB_PERSONOPLYSNINGER",
    "PLANLAGT_AKTIVITET",
    "POSTNUMMER",
    "PROBLEM_BASED_PLAN",
    "REFERRAL_ASSESSMENT_STAMP_FIELD",
    "REFERRALINFORMATION",
    "REFERRAL_REASON_ADD_CODES",
    "REHABILITATION_PLAN",
    "REHABILITATION_PLAN_RECEIVER",
    "RIS_YDELSE",
    "SENGEENHED",
    "SENGEENHED_DISPONIBLE",
    "SENGEENHED_NORMEREDE",
    "TECHNICAL_CONTACT",
    "TEMP_TABLE_BILLING_ID_TIME",
    "TEMP_TABLE_BILLING_PROCEDURE",
    "TEMP_TABLE_CONNECT_BY_KA_PIDS",
    "TEMP_TABLE_CONNECT_BY_KI_PIDS",
    "TEMP_TABLE_CONTACT_ID_TIME",
    "TEMP_TABLE_REFERRAL_ID_TIME",
    "TILKNYTTET_PERSON",
    "TILKNYTTET_PERSON_TELEFON_NR",
    "TILKNYTTET_SYGEHUS_BEHANDLER",
    "TILSTEDEVAERELSE",
    "UOFFICIEL_KONTAKT",
]


# Generate observable source assets for all IFDB Oracle tables
def create_ifdb_observable_assets():
    """Dynamically create observable source assets for all IFDB Oracle tables."""
    assets = []

    for table_name in IFDB_TABLES_NAMES:
        asset_name = f"ifdb_{table_name.lower()}_source"

        # Create a closure to capture table_name
        def make_observable_fn(tbl_name):
            @dg.observable_source_asset(
                name=f"ifdb_{tbl_name.lower()}_source",
                group_name="ifdb_oracle_sources",
                description=f"Observable source asset for {tbl_name} table from IFDB Oracle database",
            )
            def observable_function(
                context: dg.AssetExecutionContext, ifdb_oracle: IFDBOracleResource
            ) -> dg.ObserveResult:
                """Observe table metadata from IFDB Oracle database."""
                return _observe_table_metadata(context, ifdb_oracle, tbl_name)

            return observable_function

        assets.append(make_observable_fn(table_name))

    return assets


# Create all observable assets
ifdb_observable_assets = create_ifdb_observable_assets()

# Export individual assets for backward compatibility and specific use cases
ifdb_encounter_source = next(
    asset
    for asset in ifdb_observable_assets
    if asset.key.to_user_string() == "ifdb_encounter_source"
)
ifdb_episode_of_care_source = next(
    asset
    for asset in ifdb_observable_assets
    if asset.key.to_user_string() == "ifdb_episode_of_care_source"
)
ifdb_eventmarker_source = next(
    asset
    for asset in ifdb_observable_assets
    if asset.key.to_user_string() == "ifdb_eventmarker_source"
)
ifdb_healthissue_source = next(
    asset
    for asset in ifdb_observable_assets
    if asset.key.to_user_string() == "ifdb_healthissue_source"
)
ifdb_patient_oplysninger_columna_source = next(
    asset
    for asset in ifdb_observable_assets
    if asset.key.to_user_string() == "ifdb_patient_oplysninger_columna_source"
)
ifdb_patient_oplysninger_officielle_source = next(
    asset
    for asset in ifdb_observable_assets
    if asset.key.to_user_string() == "ifdb_patient_oplysninger_officielle_source"
)
ifdb_location_source = next(
    asset
    for asset in ifdb_observable_assets
    if asset.key.to_user_string() == "ifdb_location_source"
)

# Using AssetSpec to declare external assets for key IFDB tables
# This provides a clear signal to the planner and matches the MSSQL pattern
# These are used as dependencies for transform assets

ifdb_encounter_source_spec = dg.AssetSpec(
    key="ifdb_encounter_source",
    group_name="ifdb_oracle_sources",
    description="The IFDB.ENCOUNTER table in Oracle IFDB database.",
)

ifdb_episode_of_care_source_spec = dg.AssetSpec(
    key="ifdb_episode_of_care_source",
    group_name="ifdb_oracle_sources",
    description="The IFDB.EPISODE_OF_CARE table in Oracle IFDB database.",
)

ifdb_patient_oplysninger_columna_source_spec = dg.AssetSpec(
    key="ifdb_patient_oplysninger_columna_source",
    group_name="ifdb_oracle_sources",
    description="The IFDB.PATIENT_OPLYSNINGER_COLUMNA table in Oracle IFDB database.",
)

ifdb_patient_oplysninger_officielle_source_spec = dg.AssetSpec(
    key="ifdb_patient_oplysninger_officielle_source",
    group_name="ifdb_oracle_sources",
    description="The IFDB.PATIENT_OPLYSNINGER_OFFICIELLE table in Oracle IFDB database.",
)

ifdb_location_source_spec = dg.AssetSpec(
    key="ifdb_location_source",
    group_name="ifdb_oracle_sources",
    description="The IFDB.LOCATION table in Oracle IFDB database.",
)

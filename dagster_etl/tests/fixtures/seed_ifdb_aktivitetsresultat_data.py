"""
Performance test data seeder for IFDB Oracle - Aktivitetsresultat Pipeline.

Generates synthetic AKTIVITETSRESULTAT records with realistic XML CLOB content
containing clinical form data (elements, klynger, selektioner, repetitioner).
Data is distributed evenly across 53 monthly partitions (2021-09 to 2026-01).

Prerequisites:
    - Patient data must be seeded first (run seed_ifdb_performance_data.py)
    - Encounter data should be seeded (run seed_ifdb_encounter_data.py)
    - Oracle IFDB container must be running and healthy

Usage:
    # From dagster_etl directory
    python -m tests.fixtures.seed_ifdb_aktivitetsresultat_data

    # With custom row count (default: 500k)
    python -m tests.fixtures.seed_ifdb_aktivitetsresultat_data --rows 100000

    # Force re-seed (truncate existing data)
    python -m tests.fixtures.seed_ifdb_aktivitetsresultat_data --force
"""

import argparse
import logging
import os
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    import oracledb
except ImportError:
    print("ERROR: oracledb package not installed. Run: pip install oracledb")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)8s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

DEFAULT_TOTAL_ROWS = 500_000
BATCH_SIZE = 5_000
COMMIT_INTERVAL = 25_000

START_PARTITION = datetime(2021, 9, 1)
END_PARTITION = datetime(2026, 1, 31)

ORACLE_CONFIG = {
    "host": os.getenv("IFDB_PROD_ORACLE_SERVER", "oracle"),
    "port": int(os.getenv("IFDB_PROD_ORACLE_PORT", "1521")),
    "service_name": os.getenv("IFDB_PROD_ORACLE_DATABASE", "ORACLE_DB"),
    "user": os.getenv("IFDB_PROD_ORACLE_USERNAME", "myuser"),
    "password": os.getenv("IFDB_PROD_ORACLE_PASSWORD", "tempPassword1!"),
}

# =============================================================================
# Danish Clinical Test Data
# =============================================================================

AKTIVITET_TYPES = [
    ("ZZ3170", "Blodtryksmåling"),
    ("ZZ3173", "Temperaturmåling"),
    ("ZZ3175", "Pulsmåling"),
    ("ZZ3180", "Vægtmåling"),
    ("ZZ0050", "Klinisk observation"),
    ("ZZ0055", "Sygeplejefaglig vurdering"),
    ("ZZ0060", "Ernæringsvurdering"),
    ("ZZ0065", "Smerteregistrering"),
    ("ZZ0070", "Funktionsevnevurdering"),
    ("ZZ0075", "Bevidsthedsniveau"),
]

RESULTAT_TYPES = [
    ("RESBLOD", "Blodtryksresultat"),
    ("RESTEMP", "Temperaturresultat"),
    ("RESPULS", "Pulsresultat"),
    ("RESVAEGT", "Vægtresultat"),
    ("RESKLIN", "Klinisk resultat"),
    ("RESSYG", "Sygeplejefagligt resultat"),
    ("RESERN", "Ernæringsresultat"),
    ("RESSME", "Smerteresultat"),
]

OVERSKRIFTER = [
    "Blodtryk",
    "Vitale værdier",
    "Temperatur",
    "Puls",
    "Vægt og højde",
    "Klinisk observation",
    "Sygeplejefaglig vurdering",
    "Ernæringsvurdering",
    "Smerteregistrering",
    "Funktionsevnevurdering",
    "Bevidsthedsniveau (GCS)",
    "Respirationsfrekvens",
    "Iltmætning",
    "Blodsukker",
    "Vandladning",
]

DOMAENE_NAVNE = [
    "Medicin",
    "Sygepleje",
    "Observation",
    "Behandling",
    "Dokumentation",
]

MODELLERINGS_NAVNE = [
    "SP_VitaleVærdier_v2",
    "SP_Ernæring_v1",
    "SP_Smerte_v3",
    "SP_Funktion_v1",
    "SP_Bevidsthed_v2",
    "SP_Respiration_v1",
]

BRUGER_ROLLER = [
    ("Sygeplejerske", "Sygeplejerske"),
    ("Læge", "Læge"),
    ("SOSU", "Social- og sundhedsassistent"),
    ("Fysioterapeut", "Fysioterapeut"),
    ("Ergoterapeut", "Ergoterapeut"),
]

HOSPITALS = [
    ("OUH001", "Odense Universitetshospital", "461001", "1234567890123"),
    ("SLB001", "Sygehus Lillebælt - Vejle", "630001", "1234567890125"),
    ("SLB002", "Sygehus Lillebælt - Kolding", "621001", "1234567890126"),
    ("SHS001", "Sydvestjysk Sygehus - Esbjerg", "561001", "1234567890127"),
    ("SAS001", "Sygehus Sønderjylland - Aabenraa", "580001", "1234567890129"),
]

PERSONALE_NAVNE = [
    "Anna Hansen",
    "Bo Nielsen",
    "Camilla Jensen",
    "Daniel Pedersen",
    "Emma Andersen",
    "Frederik Larsen",
    "Gitte Sørensen",
    "Henrik Rasmussen",
    "Ida Christensen",
    "Jakob Petersen",
    "Karen Olsen",
    "Lars Madsen",
]

# =============================================================================
# XML Generation - Realistic Clinical Form Structures
# =============================================================================

_XML_TEMPLATES = None


def _get_xml_templates() -> list:
    """Build XML template generators lazily."""
    global _XML_TEMPLATES
    if _XML_TEMPLATES is not None:
        return _XML_TEMPLATES

    _XML_TEMPLATES = [
        _xml_blodtryk,
        _xml_vitale_vaerdier,
        _xml_smerteregistrering,
        _xml_ernaering,
        _xml_bevidsthed_gcs,
        _xml_medicin_repetition,
        _xml_diagnose_selektion,
        _xml_funktionsevne,
        _xml_respiration,
        _xml_personale_observation,
    ]
    return _XML_TEMPLATES


def _xml_kontekst(
    ts: datetime, bruger: str, pers_id: str, rolle: str, adm_enhed: str, adm_enhed_id: str
) -> str:
    iso = ts.isoformat()
    return (
        f"<Kontekst>"
        f"<Oprettet><Bruger>{bruger}</Bruger><PersId>{pers_id}</PersId>"
        f"<Rolle>{rolle}</Rolle><AdministrativEnhed>{adm_enhed}</AdministrativEnhed>"
        f"<AdministrativEnhedId>{adm_enhed_id}</AdministrativEnhedId>"
        f"<Tidspunkt>{iso}</Tidspunkt><Tidsstempel>{int(ts.timestamp())}</Tidsstempel>"
        f"</Oprettet>"
        f"<Aendret><Bruger>{bruger}</Bruger><PersId>{pers_id}</PersId>"
        f"<Rolle>{rolle}</Rolle><Tidspunkt>{iso}</Tidspunkt>"
        f"<Tidsstempel>{int(ts.timestamp())}</Tidsstempel></Aendret>"
        f"<Status>Godkendt</Status>"
        f"</Kontekst>"
    )


def _elem(navn: str, vaerdi: str, eid: str = "", kode: str = "", etype: str = "string") -> str:
    parts = [f'<Element type="{etype}">']
    parts.append(f"<Navn>{navn}</Navn>")
    parts.append(f"<Vaerdi>{vaerdi}</Vaerdi>")
    parts.append(f"<Ledetekst>{navn}</Ledetekst>")
    if eid:
        parts.append(f"<ElementId>{eid}</ElementId>")
    if kode:
        parts.append(f"<Kode>{kode}</Kode>")
    parts.append("</Element>")
    return "".join(parts)


def _xml_blodtryk(ts: datetime, bruger_info: tuple) -> str:
    bruger, pid, rolle, adm, adm_id = bruger_info
    sys_val = random.randint(90, 180)
    dia_val = random.randint(50, 110)
    return (
        "<Afdelingsydelsesresultat>"
        "<Overskrift>Blodtryk</Overskrift>"
        "<Ledetekst>Vitale værdier</Ledetekst>"
        "<Type>Observation</Type>"
        "<Kladde>false</Kladde>"
        + _xml_kontekst(ts, bruger, pid, rolle, adm, adm_id)
        + _elem("Systolisk", str(sys_val), "E001", "ZZ3170", "number")
        + _elem("Diastolisk", str(dia_val), "E002", "ZZ3172", "number")
        + _elem(
            "Måleposition", random.choice(["Siddende", "Liggende", "Stående"]), "E003", "", "string"
        )
        + "</Afdelingsydelsesresultat>"
    )


def _xml_vitale_vaerdier(ts: datetime, bruger_info: tuple) -> str:
    bruger, pid, rolle, adm, adm_id = bruger_info
    temp = round(random.uniform(36.0, 40.5), 1)
    puls = random.randint(40, 140)
    resp = random.randint(8, 30)
    sat = random.randint(85, 100)
    return (
        "<Afdelingsydelsesresultat>"
        "<Overskrift>Vitale værdier</Overskrift>"
        "<Type>Observation</Type>"
        "<Kladde>false</Kladde>"
        + _xml_kontekst(ts, bruger, pid, rolle, adm, adm_id)
        + '<Klynge type="sekvens"><Navn>VitaleGruppe</Navn>'
        + "<Ledetekst>Vitale målinger</Ledetekst>"
        + _elem("Temperatur", str(temp), "E010", "ZZ3173", "number")
        + _elem("Puls", str(puls), "E011", "ZZ3175", "number")
        + _elem("Respirationsfrekvens", str(resp), "E012", "ZZ3177", "number")
        + _elem("Iltmætning", str(sat), "E013", "ZZ3179", "number")
        + "</Klynge>"
        + "</Afdelingsydelsesresultat>"
    )


def _xml_smerteregistrering(ts: datetime, bruger_info: tuple) -> str:
    bruger, pid, rolle, adm, adm_id = bruger_info
    score = random.randint(0, 10)
    lokation = random.choice(["Ryg", "Mave", "Hoved", "Bryst", "Ben", "Arm", "Skulder"])
    karakter = random.choice(["Skarp", "Dunkende", "Brændende", "Trykkende", "Stikkende"])
    return (
        "<Afdelingsydelsesresultat>"
        "<Overskrift>Smerteregistrering</Overskrift>"
        "<Type>Observation</Type>"
        "<Kladde>false</Kladde>"
        + _xml_kontekst(ts, bruger, pid, rolle, adm, adm_id)
        + '<Klynge type="sekvens"><Navn>SmerteVurdering</Navn>'
        + "<Ledetekst>Smertevurdering</Ledetekst>"
        + _elem("NRS-score", str(score), "E020", "", "number")
        + _elem("Smertelokalisation", lokation, "E021", "", "string")
        + _elem("Smertekarakter", karakter, "E022", "", "string")
        + _elem(
            "Varighed", random.choice(["Akut", "Kronisk", "Intermitterende"]), "E023", "", "string"
        )
        + "</Klynge>"
        + "</Afdelingsydelsesresultat>"
    )


def _xml_ernaering(ts: datetime, bruger_info: tuple) -> str:
    bruger, pid, rolle, adm, adm_id = bruger_info
    vaegt = round(random.uniform(40.0, 150.0), 1)
    hoejde = random.randint(140, 200)
    bmi = round(vaegt / ((hoejde / 100) ** 2), 1)
    return (
        "<Afdelingsydelsesresultat>"
        "<Overskrift>Ernæringsvurdering</Overskrift>"
        "<Type>Observation</Type>"
        "<Kladde>false</Kladde>"
        + _xml_kontekst(ts, bruger, pid, rolle, adm, adm_id)
        + '<Klynge type="sekvens"><Navn>Antropometri</Navn>'
        + "<Ledetekst>Antropometriske målinger</Ledetekst>"
        + _elem("Vægt", str(vaegt), "E030", "", "number")
        + _elem("Højde", str(hoejde), "E031", "", "number")
        + _elem("BMI", str(bmi), "E032", "", "number")
        + "</Klynge>"
        + '<Klynge type="selektion"><Navn>Ernæringsrisiko</Navn>'
        + "<Ledetekst>Ernæringsrisikovurdering</Ledetekst>"
        + "<ValgLedetekst>"
        + random.choice(["Lav risiko", "Moderat risiko", "Høj risiko"])
        + "</ValgLedetekst>"
        + _elem("NRS2002", str(random.randint(0, 7)), "E033", "", "number")
        + "</Klynge>"
        + "</Afdelingsydelsesresultat>"
    )


def _xml_bevidsthed_gcs(ts: datetime, bruger_info: tuple) -> str:
    bruger, pid, rolle, adm, adm_id = bruger_info
    eye = random.randint(1, 4)
    verbal = random.randint(1, 5)
    motor = random.randint(1, 6)
    return (
        "<Afdelingsydelsesresultat>"
        "<Overskrift>Bevidsthedsniveau (GCS)</Overskrift>"
        "<Type>Observation</Type>"
        "<Kladde>false</Kladde>"
        + _xml_kontekst(ts, bruger, pid, rolle, adm, adm_id)
        + '<Klynge type="sekvens"><Navn>GCS</Navn>'
        + "<Ledetekst>Glasgow Coma Scale</Ledetekst>"
        + _elem("Øjenåbning", str(eye), "E040", "", "number")
        + _elem("Verbal respons", str(verbal), "E041", "", "number")
        + _elem("Motorisk respons", str(motor), "E042", "", "number")
        + _elem("GCS Total", str(eye + verbal + motor), "E043", "", "number")
        + "</Klynge>"
        + "</Afdelingsydelsesresultat>"
    )


def _xml_medicin_repetition(ts: datetime, bruger_info: tuple) -> str:
    bruger, pid, rolle, adm, adm_id = bruger_info
    meds = [
        ("Metoprolol", "50mg", "C07AB02"),
        ("Amlodipine", "5mg", "C08CA01"),
        ("Simvastatin", "40mg", "C10AA01"),
        ("Metformin", "500mg", "A10BA02"),
        ("Omeprazol", "20mg", "A02BC01"),
        ("Furosemid", "40mg", "C03CA01"),
        ("Paracetamol", "1g", "N02BE01"),
        ("Ibuprofen", "400mg", "M01AE01"),
    ]
    num_meds = random.randint(1, 4)
    selected = random.sample(meds, num_meds)
    parts = [
        "<Afdelingsydelsesresultat>",
        "<Overskrift>Medicingennemgang</Overskrift>",
        "<Type>Observation</Type>",
        "<Kladde>false</Kladde>",
        _xml_kontekst(ts, bruger, pid, rolle, adm, adm_id),
    ]
    for med_name, dose, atc in selected:
        parts.append("<Repetition>")
        parts.append(_elem("Lægemiddel", med_name, "", atc, "string"))
        parts.append(_elem("Dosis", dose, "", "", "string"))
        parts.append(
            _elem(
                "Frekvens",
                random.choice(
                    ["1 gang dagligt", "2 gange dagligt", "3 gange dagligt", "Ved behov"]
                ),
                "",
                "",
                "string",
            )
        )
        parts.append("</Repetition>")
    parts.append("</Afdelingsydelsesresultat>")
    return "".join(parts)


def _xml_diagnose_selektion(ts: datetime, bruger_info: tuple) -> str:
    bruger, pid, rolle, adm, adm_id = bruger_info
    diagnoses = [
        ("I10", "Hypertension"),
        ("E11", "Type 2 diabetes"),
        ("J18", "Pneumoni"),
        ("K80", "Galdestenssygdom"),
        ("S72", "Femurfraktur"),
        ("I21", "Akut myokardieinfarkt"),
        ("J44", "KOL"),
        ("N39", "Urinvejsinfektion"),
    ]
    diag = random.choice(diagnoses)
    return (
        "<Afdelingsydelsesresultat>"
        "<Overskrift>Diagnose</Overskrift>"
        "<Type>Observation</Type>"
        "<Kladde>false</Kladde>"
        + _xml_kontekst(ts, bruger, pid, rolle, adm, adm_id)
        + '<Klynge type="selektion"><Navn>DiagnoseValg</Navn>'
        + "<Ledetekst>Vælg diagnose</Ledetekst>"
        + f"<ValgLedetekst>Valgt: {diag[1]}</ValgLedetekst>"
        + _elem("DiagnoseKode", diag[0], "E050", diag[0], "string")
        + _elem("DiagnoseTekst", diag[1], "E051", "", "string")
        + "</Klynge>"
        + "</Afdelingsydelsesresultat>"
    )


def _xml_funktionsevne(ts: datetime, bruger_info: tuple) -> str:
    bruger, pid, rolle, adm, adm_id = bruger_info
    areas = ["Mobilitet", "Personlig pleje", "Ernæring", "Kommunikation", "Socialt liv"]
    parts = [
        "<Afdelingsydelsesresultat>",
        "<Overskrift>Funktionsevnevurdering</Overskrift>",
        "<Type>Observation</Type>",
        "<Kladde>false</Kladde>",
        _xml_kontekst(ts, bruger, pid, rolle, adm, adm_id),
    ]
    for area in random.sample(areas, random.randint(2, 4)):
        score = random.randint(0, 4)
        parts.append(f'<Klynge type="sekvens"><Navn>{area}</Navn>')
        parts.append(f"<Ledetekst>{area}svurdering</Ledetekst>")
        parts.append(_elem("Funktionsniveau", str(score), "", "", "number"))
        parts.append(
            _elem(
                "Bemærkning",
                f"Patient {'klarer sig selv' if score < 2 else 'har behov for hjælp'}",
                "",
                "",
                "string",
            )
        )
        parts.append("</Klynge>")
    parts.append("</Afdelingsydelsesresultat>")
    return "".join(parts)


def _xml_respiration(ts: datetime, bruger_info: tuple) -> str:
    bruger, pid, rolle, adm, adm_id = bruger_info
    return (
        "<Afdelingsydelsesresultat>"
        "<Overskrift>Respirationsovervågning</Overskrift>"
        "<Type>Observation</Type>"
        "<Kladde>false</Kladde>"
        + _xml_kontekst(ts, bruger, pid, rolle, adm, adm_id)
        + _elem("Respirationsfrekvens", str(random.randint(8, 30)), "E070", "ZZ3177", "number")
        + _elem("Iltmætning", str(random.randint(85, 100)), "E071", "ZZ3179", "number")
        + _elem(
            "Ilttilskud",
            random.choice(["Ingen", "Næsebrille 2L", "Maske 5L", "CPAP"]),
            "E072",
            "",
            "string",
        )
        + _elem(
            "Vejrtrækningstype",
            random.choice(["Normal", "Overfladisk", "Dyspnø", "Takypnø"]),
            "E073",
            "",
            "string",
        )
        + "</Afdelingsydelsesresultat>"
    )


def _xml_personale_observation(ts: datetime, bruger_info: tuple) -> str:
    bruger, pid, rolle, adm, adm_id = bruger_info
    personale = random.choice(PERSONALE_NAVNE)
    return (
        "<Afdelingsydelsesresultat>"
        "<Overskrift>Klinisk observation</Overskrift>"
        "<Type>Observation</Type>"
        "<Kladde>false</Kladde>"
        + _xml_kontekst(ts, bruger, pid, rolle, adm, adm_id)
        + '<Element type="string"><Navn>PersonaleRolle</Navn>'
        + f"<Vaerdi>{rolle}</Vaerdi></Element>"
        + '<Element type="string"><Navn>Personale</Navn>'
        + f"<Vaerdi>{personale}</Vaerdi><PersId>{pid}</PersId></Element>"
        + _elem("Observation", "Patient er mobiliseret og er i bedring", "E080", "", "string")
        + _elem(
            "Opfølgning",
            random.choice(["Fortsæt plan", "Revurdering nødvendig", "Udskrivelse mulig"]),
            "E081",
            "",
            "string",
        )
        + "</Afdelingsydelsesresultat>"
    )


def generate_xml(ts: datetime, bruger_info: tuple) -> str:
    templates = _get_xml_templates()
    return random.choice(templates)(ts, bruger_info)


# =============================================================================
# Data Generation
# =============================================================================


def generate_partition_dates(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    partitions = []
    current = start
    while current <= end:
        if current.month == 12:
            next_month = datetime(current.year + 1, 1, 1)
        else:
            next_month = datetime(current.year, current.month + 1, 1)
        month_end = next_month - timedelta(days=1)
        partitions.append((current, month_end))
        current = next_month
    return partitions


def random_datetime_in_range(start: datetime, end: datetime) -> datetime:
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


def _make_bruger_info() -> tuple[str, str, str, str, str]:
    hospital = random.choice(HOSPITALS)
    rolle_kode, rolle_navn = random.choice(BRUGER_ROLLER)
    pers = random.choice(PERSONALE_NAVNE)
    pers_id = f"P{random.randint(10000, 99999)}"
    return pers, pers_id, rolle_kode, hospital[1], hospital[0]


def generate_aktivitetsresultat_batch(
    start_id: int,
    batch_size: int,
    patient_ids: list[int],
    encounter_ids: list[int] | None,
    episode_ids: list[int] | None,
    partitions: list[tuple[datetime, datetime]],
    rows_per_partition: int,
) -> list[tuple]:
    batch = []

    for i in range(batch_size):
        objekt_id = start_id + i

        partition_idx = objekt_id // rows_per_partition
        if partition_idx >= len(partitions):
            partition_idx = len(partitions) - 1
        partition_start, partition_end = partitions[partition_idx]
        opdateret_tid = random_datetime_in_range(partition_start, partition_end)

        patient_id = random.choice(patient_ids)
        encounter_id = random.choice(encounter_ids) if encounter_ids else None
        episode_id = random.choice(episode_ids) if episode_ids else None

        akt_type = random.choice(AKTIVITET_TYPES)
        res_type = random.choice(RESULTAT_TYPES)
        overskrift = random.choice(OVERSKRIFTER)
        domaene = random.choice(DOMAENE_NAVNE)
        modellering = random.choice(MODELLERINGS_NAVNE)
        hospital = random.choice(HOSPITALS)
        rolle_kode, rolle_navn = random.choice(BRUGER_ROLLER)
        personale = random.choice(PERSONALE_NAVNE)
        pers_id = f"P{random.randint(10000, 99999)}"

        slettet = 1 if random.random() < 0.02 else 0
        kladde = 1 if random.random() < 0.05 else 0

        bruger_info = (personale, pers_id, rolle_kode, hospital[1], hospital[0])
        xml_content = generate_xml(opdateret_tid, bruger_info)

        modtidspunkt = opdateret_tid - timedelta(seconds=random.randint(0, 600))
        udfoert_tidspunkt = opdateret_tid - timedelta(minutes=random.randint(0, 120))
        period_start = opdateret_tid - timedelta(hours=random.randint(0, 48))
        period_end = (
            opdateret_tid - timedelta(minutes=random.randint(0, 30))
            if random.random() < 0.7
            else None
        )

        record = (
            objekt_id,
            modtidspunkt,
            slettet,
            patient_id,
            objekt_id + 1000000,
            akt_type[0],
            akt_type[1],
            res_type[0],
            res_type[1],
            overskrift,
            f"/{domaene}/{modellering}",
            domaene,
            modellering,
            "2.0",
            xml_content,
            encounter_id,
            pers_id,
            personale,
            rolle_kode,
            hospital[0],
            hospital[1],
            hospital[2],
            udfoert_tidspunkt,
            kladde,
            opdateret_tid,
            encounter_id,
            episode_id,
            period_start,
            period_end,
            hospital[3],
            0,
            rolle_navn,
            "STRUCTURED",
            f"MOID{objekt_id}",
        )
        batch.append(record)

    return batch


# =============================================================================
# Database Operations
# =============================================================================


def get_oracle_connection():
    dsn = oracledb.makedsn(
        ORACLE_CONFIG["host"],
        ORACLE_CONFIG["port"],
        service_name=ORACLE_CONFIG["service_name"],
    )
    logger.info(f"Connecting to Oracle at {ORACLE_CONFIG['host']}:{ORACLE_CONFIG['port']}")
    return oracledb.connect(
        user=ORACLE_CONFIG["user"],
        password=ORACLE_CONFIG["password"],
        dsn=dsn,
    )


def get_patient_ids(connection) -> list[int]:
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT PATIENT_ID FROM IFDB.PATIENT_OPLYSNINGER_COLUMNA")
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()


def get_encounter_ids(connection) -> list[int] | None:
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT COUNT(*) FROM all_tables WHERE owner='IFDB' AND table_name='ENCOUNTER'"
        )
        if cursor.fetchone()[0] == 0:
            return None
        cursor.execute("SELECT OBJECT_ID FROM IFDB.ENCOUNTER WHERE ROWNUM <= 100000")
        ids = [row[0] for row in cursor.fetchall()]
        return ids if ids else None
    except oracledb.DatabaseError:
        return None
    finally:
        cursor.close()


def get_episode_ids(connection) -> list[int] | None:
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT COUNT(*) FROM all_tables WHERE owner='IFDB' AND table_name='EPISODE_OF_CARE'"
        )
        if cursor.fetchone()[0] == 0:
            return None
        cursor.execute("SELECT OBJECT_ID FROM IFDB.EPISODE_OF_CARE WHERE ROWNUM <= 100000")
        ids = [row[0] for row in cursor.fetchall()]
        return ids if ids else None
    except oracledb.DatabaseError:
        return None
    finally:
        cursor.close()


def ensure_table_exists(connection) -> bool:
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            SELECT COUNT(*) FROM all_tables
            WHERE owner = 'IFDB' AND table_name = 'AKTIVITETSRESULTAT'
        """
        )
        if cursor.fetchone()[0] > 0:
            logger.info("AKTIVITETSRESULTAT table already exists")
            return True

        logger.info("Creating IFDB.AKTIVITETSRESULTAT table...")
        cursor.execute(
            """
            CREATE TABLE IFDB.AKTIVITETSRESULTAT (
                OBJEKT_ID NUMBER(19) PRIMARY KEY,
                MODTIDSPUNKT TIMESTAMP,
                SLETTET NUMBER(1),
                PATIENT_ID NUMBER(19),
                UDFOERELSE_ID NUMBER(19),
                AKTIVITETTYPE_KODE VARCHAR2(50),
                AKTIVITETTYPE_KODETEKST VARCHAR2(500),
                RESULTATTYPE_KODE VARCHAR2(50),
                RESULTATTYPE_KODETEKST VARCHAR2(500),
                OVERSKRIFT VARCHAR2(500),
                HIERARKISK_PLACERING VARCHAR2(500),
                DOMAENENAVN VARCHAR2(200),
                MODELLERINGSNAVN VARCHAR2(200),
                MODELLERINGSVERSION VARCHAR2(50),
                XML CLOB,
                TILSTEDEVAERELSE_ID NUMBER(19),
                UDFOERT_PERSID VARCHAR2(50),
                UDFOERT_PERSNAVN VARCHAR2(200),
                UDFOERT_BRUGERROLLE VARCHAR2(100),
                UDFOERT_ADMENHED_KODE VARCHAR2(50),
                UDFOERT_ADMENHED_NAVN VARCHAR2(500),
                UDFOERT_ADMENHED_ORGKODE VARCHAR2(50),
                UDFOERT_TIDSPUNKT TIMESTAMP,
                KLADDE NUMBER(1),
                OPDATERET_TID TIMESTAMP,
                ENCOUNTER_ID NUMBER(19),
                EPISODE_OF_CARE_ID NUMBER(19),
                PERIOD_START TIMESTAMP,
                PERIOD_END TIMESTAMP,
                UDFOERT_ADMENHED_SORCODE VARCHAR2(50),
                LPR3_IGNORED NUMBER(1),
                UDFOERT_BRUGERROLLE_NAME VARCHAR2(200),
                RESULT_TYPE VARCHAR2(50),
                RESULT_MOID VARCHAR2(100)
            )
        """
        )
        cursor.execute(
            """
            CREATE INDEX IFDB.IDX_AKTRES_OPDATERET_TID
            ON IFDB.AKTIVITETSRESULTAT(OPDATERET_TID)
        """
        )
        cursor.execute(
            """
            CREATE INDEX IFDB.IDX_AKTRES_PATIENT_ID
            ON IFDB.AKTIVITETSRESULTAT(PATIENT_ID)
        """
        )
        connection.commit()
        logger.info("AKTIVITETSRESULTAT table created")
        return True

    except oracledb.DatabaseError as e:
        logger.error(f"Database error: {e}")
        return False
    finally:
        cursor.close()


def get_row_count(connection) -> int:
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM IFDB.AKTIVITETSRESULTAT")
        return cursor.fetchone()[0]
    except oracledb.DatabaseError:
        return 0
    finally:
        cursor.close()


def truncate_table(connection):
    cursor = connection.cursor()
    try:
        logger.info("Clearing IFDB.AKTIVITETSRESULTAT...")
        try:
            cursor.execute("TRUNCATE TABLE IFDB.AKTIVITETSRESULTAT")
        except oracledb.DatabaseError:
            cursor.execute("DELETE FROM IFDB.AKTIVITETSRESULTAT")
        connection.commit()
    finally:
        cursor.close()


def insert_batch(connection, cursor, batch: list[tuple]):
    insert_sql = """
        INSERT INTO IFDB.AKTIVITETSRESULTAT (
            OBJEKT_ID, MODTIDSPUNKT, SLETTET, PATIENT_ID, UDFOERELSE_ID,
            AKTIVITETTYPE_KODE, AKTIVITETTYPE_KODETEKST,
            RESULTATTYPE_KODE, RESULTATTYPE_KODETEKST,
            OVERSKRIFT, HIERARKISK_PLACERING, DOMAENENAVN,
            MODELLERINGSNAVN, MODELLERINGSVERSION, XML,
            TILSTEDEVAERELSE_ID, UDFOERT_PERSID, UDFOERT_PERSNAVN,
            UDFOERT_BRUGERROLLE, UDFOERT_ADMENHED_KODE,
            UDFOERT_ADMENHED_NAVN, UDFOERT_ADMENHED_ORGKODE,
            UDFOERT_TIDSPUNKT, KLADDE, OPDATERET_TID,
            ENCOUNTER_ID, EPISODE_OF_CARE_ID,
            PERIOD_START, PERIOD_END, UDFOERT_ADMENHED_SORCODE,
            LPR3_IGNORED, UDFOERT_BRUGERROLLE_NAME,
            RESULT_TYPE, RESULT_MOID
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, :9, :10,
            :11, :12, :13, :14, :15, :16, :17, :18, :19, :20,
            :21, :22, :23, :24, :25, :26, :27, :28, :29, :30,
            :31, :32, :33, :34
        )
    """
    cursor.executemany(insert_sql, batch)


# =============================================================================
# Main Seeding Logic
# =============================================================================


def seed_aktivitetsresultat_data(total_rows: int, force: bool = False):
    logger.info("=" * 70)
    logger.info("IFDB Aktivitetsresultat Data Seeder")
    logger.info("=" * 70)
    logger.info(f"Target rows: {total_rows:,}")

    partitions = generate_partition_dates(START_PARTITION, END_PARTITION)
    rows_per_partition = total_rows // len(partitions)

    logger.info(f"Partitions: {len(partitions)} months")
    logger.info(f"Rows per partition: ~{rows_per_partition:,}")
    logger.info("=" * 70)

    try:
        connection = get_oracle_connection()
        logger.info("Connected to Oracle successfully")
    except Exception as e:
        logger.error(f"Failed to connect to Oracle: {e}")
        sys.exit(1)

    try:
        patient_ids = get_patient_ids(connection)
        if not patient_ids:
            logger.error("No patients found! Run seed_ifdb_performance_data.py first.")
            sys.exit(1)
        logger.info(f"Found {len(patient_ids):,} patients")

        encounter_ids = get_encounter_ids(connection)
        episode_ids = get_episode_ids(connection)
        logger.info(f"Encounter IDs available: {len(encounter_ids) if encounter_ids else 0:,}")
        logger.info(f"Episode IDs available: {len(episode_ids) if episode_ids else 0:,}")

        if not ensure_table_exists(connection):
            logger.error("Failed to ensure table exists")
            sys.exit(1)

        current_count = get_row_count(connection)
        logger.info(f"Current row count: {current_count:,}")

        if current_count >= total_rows and not force:
            logger.info("Table already has sufficient data (>= target)")
            logger.info("Use --force to re-seed data")
            return

        if current_count > 0 and force:
            truncate_table(connection)

        cursor = connection.cursor()
        start_time = time.time()
        rows_inserted = 0
        last_commit = 0

        logger.info(f"Starting data generation ({BATCH_SIZE:,}/batch, commit/{COMMIT_INTERVAL:,})")

        try:
            while rows_inserted < total_rows:
                remaining = total_rows - rows_inserted
                current_batch_size = min(BATCH_SIZE, remaining)

                batch = generate_aktivitetsresultat_batch(
                    start_id=rows_inserted + 1,
                    batch_size=current_batch_size,
                    patient_ids=patient_ids,
                    encounter_ids=encounter_ids,
                    episode_ids=episode_ids,
                    partitions=partitions,
                    rows_per_partition=rows_per_partition,
                )

                insert_batch(connection, cursor, batch)
                rows_inserted += current_batch_size

                if rows_inserted - last_commit >= COMMIT_INTERVAL:
                    connection.commit()
                    last_commit = rows_inserted

                    elapsed = time.time() - start_time
                    rate = rows_inserted / elapsed if elapsed > 0 else 0
                    eta = (total_rows - rows_inserted) / rate if rate > 0 else 0

                    logger.info(
                        f"Progress: {rows_inserted:,}/{total_rows:,} "
                        f"({100 * rows_inserted / total_rows:.1f}%) | "
                        f"Rate: {rate:.0f}/sec | ETA: {eta / 60:.1f} min"
                    )

            connection.commit()

        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            connection.rollback()
            raise
        finally:
            cursor.close()

        elapsed = time.time() - start_time
        final_count = get_row_count(connection)

        logger.info("")
        logger.info("=" * 70)
        logger.info("SEEDING COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Total rows: {final_count:,}")
        logger.info(f"Time: {elapsed / 60:.1f} minutes")
        logger.info(f"Rate: {final_count / elapsed:.0f} rows/second")

        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    TO_CHAR(OPDATERET_TID, 'YYYY-MM') as partition_month,
                    COUNT(*) as row_count
                FROM IFDB.AKTIVITETSRESULTAT
                GROUP BY TO_CHAR(OPDATERET_TID, 'YYYY-MM')
                ORDER BY partition_month
            """
            )
            rows = cursor.fetchall()
            logger.info(f"\nPartition Distribution ({len(rows)} months):")
            for i, (month, count) in enumerate(rows):
                if i < 3 or i >= len(rows) - 3:
                    logger.info(f"  {month}: {count:,} rows")
                elif i == 3:
                    logger.info("  ...")
        finally:
            cursor.close()

        logger.info("=" * 70)

    finally:
        connection.close()
        logger.info("Connection closed")


def main():
    parser = argparse.ArgumentParser(
        description="Seed IFDB Oracle with Aktivitetsresultat performance test data"
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_TOTAL_ROWS,
        help=f"Total number of rows to generate (default: {DEFAULT_TOTAL_ROWS:,})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-seed by truncating existing data",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Oracle host (overrides IFDB_ORACLE_HOST env var)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Oracle port (overrides IFDB_ORACLE_PORT env var)",
    )

    args = parser.parse_args()

    if args.host:
        ORACLE_CONFIG["host"] = args.host
    if args.port:
        ORACLE_CONFIG["port"] = args.port

    seed_aktivitetsresultat_data(args.rows, args.force)


if __name__ == "__main__":
    main()

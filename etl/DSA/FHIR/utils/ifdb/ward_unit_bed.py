# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Pure helpers for IFDB senge/kapacitet FHIR transforms.

Kept out of the DFM modules so unit tests can pin identifier determinism,
period handling and bed dedup without constructing Dagster assets.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from DSA.FHIR.utils.code_normalization import normalize_sks_coding_code
from DSA.FHIR.utils.fhir_utils import fhir_datetime
from DSA.FHIR.utils.ifdb.transforms import (
    is_true_flag,
    normalize_oracle_id,
    parse_clob_datetime,
)
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.period import Period
from utils.fhir.fhir_extensions import EXTENSION_ENCOUNTER_ABSENCE, EXTENSION_SENGEENHED_ADMENHED
from utils.fhir.identifier_systems import SYSTEM_LOCATION_PHYSICAL_TYPE, SYSTEM_SKS
from utils.sql_safety import safe_table_ref, validate_checkpoint_value

KAPACITET_TYPE_DISPONIBLE = "disponible"
KAPACITET_TYPE_NORMEREDE = "normerede"

CODE_LOCATION_PHYSICAL_TYPE_BED = "bd"

CHILD_AFFECTED_ENCOUNTER_TABLES: tuple[str, ...] = (
    "arc.ifdb_encounter_bed_space_history",
    "arc.ifdb_absence",
)

# Sengeopholdene bærer selv både enhedskoden og organisationen: LOCATION_CODE
# er samme kodeområde som SENGEENHED.ADMENHED_KODE (målt til 100 % direkte
# match), og LOCATION_SORCODE er den SOR-kode, der også sætter Bed.partOf.
# Koblingen enhedskode → organisation kommer derfor fra rækker, der beskriver
# senge i netop den sengeenhed — ikke fra klinisk aktivitetsdata i et andet
# domæne.
_ADMENHED_DICTIONARY_TABLE = "arc.ifdb_encounter_bed_space_history"


def parse_archive_datetime(raw: Any) -> datetime | None:
    """Parse an archive datetime (CLOB or ISO) to a timezone-aware datetime.

    The archive columns are NVARCHAR and carry both of the source's shapes
    ("16MAY2025:11:39:05" and ISO), so string ordering says nothing about
    chronology — everything that compares or formats one of these values goes
    through here. Returns None for empty or unreadable input so callers can
    omit the field rather than invent a sentinel.
    """
    if raw is None:
        return None
    if isinstance(raw, datetime):
        parsed: datetime | None = raw
    else:
        text = str(raw).strip()
        if not text:
            return None
        parsed = parse_clob_datetime(text)
        if parsed is None:
            try:
                parsed = datetime.fromisoformat(text)
            except (ValueError, TypeError):
                # fhir_datetime accepts a few shapes fromisoformat rejects; it
                # returns a datetime despite its str annotation.
                try:
                    coerced = fhir_datetime(text)
                except Exception:
                    return None
                parsed = coerced if isinstance(coerced, datetime) else None
    if parsed is None:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def archive_datetime_to_fhir(raw: Any) -> str | None:
    """An archive datetime as a FHIR dateTime string, or None.

    Explicitly ISO 8601 with a ``T`` separator: the value also lands in
    business keys, where ``str(datetime)`` would silently use a space.
    """
    parsed = parse_archive_datetime(raw)
    return parsed.isoformat() if parsed is not None else None


def parse_antal(raw: Any) -> int | None:
    """Parse SENGEENHED_*.ANTAL to int. ``0`` is valid and must survive."""
    if raw is None:
        return None
    text = str(raw).strip()
    if text == "":
        return None
    try:
        value = Decimal(text)
    except (InvalidOperation, ValueError):
        return None
    if value != value.to_integral_value():
        return None
    return int(value)


def build_capacity_identifier_value(kapacitet_type: str, sengeenhed_id: Any, fra_raw: Any) -> str:
    """Deterministic Observation identifier: ``{type}:{sengeenhed_id}:{fra_iso}``.

    The timestamp is formatted here rather than borrowed from whatever a
    datetime happens to stringify as. Every capacity resource's identity hangs
    on this string: a changed format mints new identifiers for rows that
    already exist, and the unique identifier index then holds duplicates.
    ``2025-01-15T08:30:00+00:00`` — pinned in full in the tests.
    """
    unit_id = normalize_oracle_id(sengeenhed_id) or str(sengeenhed_id).strip()
    fra_iso = archive_datetime_to_fhir(fra_raw) or str(fra_raw).strip()
    return f"{kapacitet_type}:{unit_id}:{fra_iso}"


def archive_datetime_to_dt(raw: Any) -> datetime | None:
    """Parse an archive datetime to a timezone-aware datetime, or None."""
    return parse_archive_datetime(raw)


def period_sort_key(raw: Any) -> datetime:
    """Sort key for latest-wins dedup.

    Unparseable values sort first, as an aware ``datetime.min``: the parsed
    keys are aware, and mixing the two raises TypeError on the comparison —
    one unreadable NVARCHAR value would take the whole asset down.
    """
    parsed = parse_archive_datetime(raw)
    return parsed if parsed is not None else datetime.min.replace(tzinfo=UTC)


def bed_physical_type() -> CodeableConcept:
    """R4 Location / Encounter.location physicalType for a bed space."""
    return CodeableConcept(
        coding=[
            Coding(
                system=SYSTEM_LOCATION_PHYSICAL_TYPE,
                code=CODE_LOCATION_PHYSICAL_TYPE_BED,
                display="Bed",
            )
        ]
    )


def build_admenhed_extension(
    kode: Any,
    navn: Any,
    orgkode: Any,
) -> Extension | None:
    """Lossless ADMENHED triple (Mønster A + fragment URLs). Omits empty subs.

    The sub-URLs carry the parent URL as a fragment, same as the absence
    extension. Bare sub-names ("kode", "navn") are the collision-prone form:
    the SoF path ``extension.extension.where(url='kode')`` does not filter on
    the parent URL, so any other extension on the same resource that used the
    same bare name would be picked up too.
    """
    subs: list[Extension] = []
    if kode is not None and str(kode).strip():
        subs.append(
            Extension(
                url=f"{EXTENSION_SENGEENHED_ADMENHED}#kode",
                valueString=str(kode).strip(),
            )
        )
    if navn is not None and str(navn).strip():
        subs.append(
            Extension(
                url=f"{EXTENSION_SENGEENHED_ADMENHED}#navn",
                valueString=str(navn).strip(),
            )
        )
    if orgkode is not None and str(orgkode).strip():
        subs.append(
            Extension(
                url=f"{EXTENSION_SENGEENHED_ADMENHED}#orgkode",
                valueString=str(orgkode).strip(),
            )
        )
    if not subs:
        return None
    return Extension(url=EXTENSION_SENGEENHED_ADMENHED, extension=subs)


def build_absence_extension(row: dict[str, Any]) -> Extension | None:
    """One parent absence extension (Mønster A + fragment URLs).

    Requires PERIOD_START. PERIOD_END / EXPECTED_RETURN / TYPE_* are optional.
    """
    period_start = archive_datetime_to_fhir(row.get("PERIOD_START"))
    if period_start is None:
        return None

    period_end = archive_datetime_to_fhir(row.get("PERIOD_END"))
    subs: list[Extension] = [
        Extension(
            url=f"{EXTENSION_ENCOUNTER_ABSENCE}#period",
            valuePeriod=Period(start=period_start, end=period_end),
        )
    ]

    # TYPE_CODE er en SKS-administrationskode med kildens konstante praefiks
    # (fx "SKS.a.admALCF01" for orlov). Samme normalisering som diagnoser og
    # procedurer: praefikset strippes, og koden baerer SKS-systemet, saa
    # fravaerstyper kan joines mod alt andet SKS i platformen.
    type_code = normalize_sks_coding_code(row.get("TYPE_CODE"))
    if type_code:
        type_text = row.get("TYPE_TEXT")
        display = None
        if type_text is not None and str(type_text).strip():
            display = str(type_text).strip()
        subs.append(
            Extension(
                url=f"{EXTENSION_ENCOUNTER_ABSENCE}#type",
                valueCoding=Coding(system=SYSTEM_SKS, code=type_code, display=display),
            )
        )

    expected = archive_datetime_to_fhir(row.get("EXPECTED_RETURN"))
    if expected is not None:
        subs.append(
            Extension(
                url=f"{EXTENSION_ENCOUNTER_ABSENCE}#expectedReturn",
                valueDateTime=expected,
            )
        )

    return Extension(url=EXTENSION_ENCOUNTER_ABSENCE, extension=subs)


def bed_row_is_live(row: dict[str, Any]) -> bool:
    """Whether a bed-history row still counts as live in the archive."""
    return not is_true_flag(row.get("dw_is_deleted"))


def fold_latest_bed_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """One row per BED_ID; a live row always wins, then latest period wins.

    Takes any iterable and holds only the winners, so the caller can stream a
    cursor straight through it: the bed history is 1,3 mio. rows against
    roughly 5.000 distinct beds, and materialising the input first is the
    difference between a few MB and a couple of GB in the run process.

    Live beats deleted before period does, which is what carries the bed's
    delete signal: the winner is deleted only when the bed has no live history
    row left, and a bed that comes back is picked up again on the next run.
    A deleted bed keeps the attributes of its last known row rather than
    dropping out of the result.

    The sort key is kept next to the winner rather than re-parsed per row —
    dedup over the full history would otherwise parse every timestamp twice.
    """
    latest: dict[str, tuple[tuple[int, datetime], dict[str, Any]]] = {}
    for row in rows:
        bed_id = normalize_oracle_id(row.get("BED_ID"))
        if not bed_id:
            continue
        incoming_key = (
            1 if bed_row_is_live(row) else 0,
            period_sort_key(row.get("PERIOD_START_REGISTRATION")),
        )
        previous = latest.get(bed_id)
        if previous is None or incoming_key > previous[0]:
            folded = dict(row)
            folded["BED_ID"] = bed_id
            latest[bed_id] = (incoming_key, folded)
    return [folded for _, folded in latest.values()]


def child_affected_encounter_ids_sql(checkpoint: str) -> str:
    """UNION of OBJECT_ID from bed-history and absence after ``checkpoint``.

    No ``dw_is_deleted`` filter: a newly soft-deleted absence must still
    re-process the encounter so the extension list is rewritten.
    """
    validate_checkpoint_value(checkpoint)
    safe_cp = checkpoint.replace("'", "''")
    parts = [
        (
            f"SELECT [OBJECT_ID] FROM {safe_table_ref(table)} "
            f"WHERE [dw_timestamp_update] > '{safe_cp}'"
        )
        for table in CHILD_AFFECTED_ENCOUNTER_TABLES
    ]
    return " UNION ".join(parts)


def child_affected_encounter_count_sql(checkpoint: str) -> str:
    """COUNT of the distinct encounters the child-affected delta would re-emit.

    Same UNION as :func:`child_affected_encounter_ids_sql`, so the count is the
    exact size of the sweep the caller is about to run.
    """
    return f"SELECT COUNT(*) FROM ({child_affected_encounter_ids_sql(checkpoint)}) AS [affected]"


def child_backfill_slice_sql(cursor_value: str | None, slice_size: int) -> str:
    """Naeste skive af efterslaebet: distinkte OBJECT_ID efter ``cursor_value``.

    Efterslaebet er ENGANGS pr. miljoe, men det koeres i skiver, saa en
    afbrudt koersel ikke efterlader et hul. Ordningen er total og
    deterministisk (OBJECT_ID ASC), saa markoeren «sidste behandlede
    OBJECT_ID» daekker praecis det, der er gjort: intet kan springes over, og
    intet behandles to gange. Derfor sorteres paa noeglen frem for paa et
    tidsstempel -- arkivets timestamps er ens for hele bulk-loadet og kan ikke
    ordne raekkerne.

    BETINGELSEN, fuldstaendigheden hviler paa: OBJECT_ID er NVARCHAR, saa
    ``>`` bruger databasens kollation. Det farlige er ikke omordning -- ordningen
    er total under enhver kollation -- men UAFGJORTE sammenligninger: under en
    case-/accent-insensitiv kollation kan to forskellige noegler sammenlignes
    ens, og saa springer ``> markoer`` den over, der staar lige med markoeren.
    Det forsvinder, naar noeglerne er RENE CIFRE: cifre har hverken versaler
    eller accenter, saa to forskellige noegler kan ikke gaa lige. Bredden maa
    gerne variere -- saa er leksikografisk orden blot ikke numerisk orden
    ('9' > '10'), og det er ligegyldigt her, hvor ordningen kun skal vaere
    total og fri for uafgjorte. Maalt paa dev's fulde arkiver (sengeophold og
    fravaer): 0 noegler med ikke-cifre (``LIKE '%[^0-9]%'``), laengde 10-12
    tegn -- altsaa netop IKKE fast bredde. Hverken eksplicit COLLATE (hardcodet
    kollation, som repoet har en regel imod) eller numerisk cast (kaster paa
    foerste ikke-numeriske noegle og goer praedikatet ikke-SARGable) er svaret.

    Ingen ``dw_is_deleted``-filtrering: en soft-slettet child-raekke skal ogsaa
    faa sin kontakt re-emitteret, saa extension-listen bliver skrevet om.
    """
    if slice_size < 1:
        raise ValueError(f"slice_size skal vaere mindst 1, fik {slice_size}")
    parts = []
    for table in CHILD_AFFECTED_ENCOUNTER_TABLES:
        where = ""
        if cursor_value is not None:
            safe = cursor_value.replace("'", "''")
            where = f" WHERE [OBJECT_ID] > '{safe}'"
        parts.append(f"SELECT [OBJECT_ID] FROM {safe_table_ref(table)}{where}")
    union = " UNION ".join(parts)
    return (
        f"SELECT TOP ({int(slice_size)}) [OBJECT_ID] FROM ({union}) AS [child] "
        f"ORDER BY [OBJECT_ID] ASC"
    )


def child_backfill_remaining_sql(cursor_value: str | None) -> str:
    """Hvor mange kontakter efterslaebet har tilbage efter ``cursor_value``."""
    parts = []
    for table in CHILD_AFFECTED_ENCOUNTER_TABLES:
        where = ""
        if cursor_value is not None:
            safe = cursor_value.replace("'", "''")
            where = f" WHERE [OBJECT_ID] > '{safe}'"
        parts.append(f"SELECT [OBJECT_ID] FROM {safe_table_ref(table)}{where}")
    return f"SELECT COUNT(*) FROM ({' UNION '.join(parts)}) AS [child]"


def admenhed_dictionary_sql() -> str:
    """ADMENHED_KODE → SOR harvest from the bed-stay archive.

    Returns the distinct (enhedskode, SOR) pairs the source itself carries on
    a single row, so the aggregate is a few hundred rows rather than the 1,3
    mio. it is built from. A code seen with more than one SOR comes back as
    several rows: the caller drops it rather than pick a winner, because the
    only ordering key available is an NVARCHAR registration timestamp in two
    different formats — see parse_archive_datetime.
    """
    return (
        f"SELECT [LOCATION_CODE] AS [ADMENHED_KODE], [LOCATION_SORCODE] AS [SORCODE] "
        f"FROM {safe_table_ref(_ADMENHED_DICTIONARY_TABLE)} "
        f"WHERE [LOCATION_CODE] IS NOT NULL "
        f"AND [LOCATION_SORCODE] IS NOT NULL "
        f"AND [dw_is_deleted] = 0 "
        f"GROUP BY [LOCATION_CODE], [LOCATION_SORCODE]"
    )

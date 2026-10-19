# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Parent-scoped retire of XML-derived child FHIR resources.

Callers post the children that exist in the current XML, then pass the parent
oids and the identifier values they just posted. Live Aidbox resources whose
identifier matches ``{oid}{infix}%`` but is not in that expected set are set to
``entered-in-error`` + ``urn:fhir:isDeleted=1``. No DELETE.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from utils.fhir.identifier_systems import _SAFE_IDENTIFIER_SYSTEM_RE
from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED

_ALLOWED_TABLES = frozenset({"procedure", "observation", "condition"})
_UPDATE_BATCH = 500
_CONDITION_VER_STATUS_SYSTEM = "http://terminology.hl7.org/CodeSystem/condition-ver-status"
_WARN_RETIRE_THRESHOLD = 10_000

StatusKind = Literal["status", "verificationStatus"]


@dataclass(frozen=True)
class RetireCeiling:
    """Værn mod masse-pensionering: over loftet springes retire over med ERROR.

    Loftet er ``max(floor, fraction * levende)`` pr. familie pr. kald. Tallene
    er bevidst u-kalibrerede sikkerhedsgrænser, ikke statistik: gulvet ligger
    langt over enhver plausibel rutinedags oprydning (så små chunks aldrig
    rammer det), andelen fanger systematisk tømning på store kørsler — det
    mønster en fremtidig skip-betingelse eller regelfejl ville efterlade.

    En legitim massehændelse skal ses af et menneske og køres igen med hævet
    loft (run-tags, se posting_config). ``fraction=1.0`` slår loftet fra:
    retire kan aldrig overstige antallet af levende børn.
    """

    floor: int = 1000
    fraction: float = 0.5

    def limit(self, live_count: int) -> int:
        return max(self.floor, int(self.fraction * live_count))


DEFAULT_RETIRE_CEILING = RetireCeiling()


@dataclass(frozen=True)
class ChildFamily:
    """One XML-child identifier family (same resource type + infix)."""

    resource_type: str
    identifier_system: str
    infix: str
    status_kind: StatusKind = "status"

    def table_name(self) -> str:
        table = self.resource_type.lower()
        if table not in _ALLOWED_TABLES:
            raise ValueError(f"Unsupported child resource type {self.resource_type!r}")
        return table

    def prefix(self, parent_oid: str) -> str:
        return f"{parent_oid}{self.infix}"

    def child_value(self, parent_oid: str, seq: object) -> str:
        """Identifier-vaerdien for eet barn — formatets ENESTE ejer.

        Alle steder, der danner en barne-identifier, skal gaa gennem denne,
        saa dannelse og opslag (prefix/prefix_bounds) aldrig kan drifte fra
        hinanden. ``seq`` er loebenummer eller elementnummer; kaldestedet ejer
        nummereringen, familien ejer formatet.
        """
        return f"{self.prefix(parent_oid)}{seq}"

    def like_pattern(self, parent_oid: str) -> str:
        return f"{self.prefix(parent_oid)}%"

    def prefix_bounds(self, parent_oid: str) -> tuple[str, str]:
        """Byte-ordens-interval, der indeholder praecis denne foraelders boern.

        Boernenes identifier-vaerdi er praefikset efterfulgt af et loebenummer,
        saa intervallet fra praefikset til praefikset med sidste tegn plus en
        rammer alle boern og intet andet. Byte-orden er den, ``text_pattern_ops``
        bruger, saa intervallet er eksakt uanset databasens collation -- og til
        forskel fra et LIKE-moenster, der foerst kendes ved koersel, kan et
        interval afvikles som et indeksopslag.
        """
        if not parent_oid:
            # Et tomt oid ville give praefikset "{infix}" og dermed ramme alle
            # foraeldres boern paa en gang -- en oprydning, der pensionerede
            # hele bestanden. reconcile_children filtrerer dem fra, men graensen
            # skal ikke kunne bygges ved et uheld.
            raise ValueError("Cannot build prefix bounds for an empty parent oid")
        lo = self.prefix(parent_oid)
        return lo, lo[:-1] + chr(ord(lo[-1]) + 1)

    def is_child_of(self, identifier_value: str, parent_oid: str) -> bool:
        prefix = self.prefix(parent_oid)
        return identifier_value.startswith(prefix) and identifier_value != prefix


def fhir_ids_to_retire(
    live: Iterable[tuple[str, str]],
    expected_identifiers: set[str],
) -> list[str]:
    """Return Aidbox ids whose identifier is live but not in the current XML set."""
    retired: list[str] = []
    seen: set[str] = set()
    for fhir_id, ident in live:
        if ident in expected_identifiers:
            continue
        if fhir_id in seen:
            continue
        seen.add(fhir_id)
        retired.append(fhir_id)
    return retired


def identifier_values_for_system(resource: Any, system: str) -> list[str]:
    """Read identifier values for ``system`` from a dict, (id, dict), or Pydantic resource."""
    payload = resource[1] if isinstance(resource, tuple) and len(resource) == 2 else resource
    identifiers: Any
    if isinstance(payload, dict):
        identifiers = payload.get("identifier") or []
    else:
        identifiers = getattr(payload, "identifier", None) or []
    values: list[str] = []
    for ident in identifiers:
        if isinstance(ident, dict):
            ident_system = ident.get("system")
            ident_value = ident.get("value")
        else:
            ident_system = getattr(ident, "system", None)
            ident_value = getattr(ident, "value", None)
        if ident_system == system and ident_value:
            values.append(str(ident_value))
    return values


def expected_child_identifiers(
    resources: Sequence[Any],
    family: ChildFamily,
) -> set[str]:
    """Identifier values from posted resources that belong to ``family``."""
    expected: set[str] = set()
    infix = family.infix
    for resource in resources:
        for value in identifier_values_for_system(resource, family.identifier_system):
            if infix in value:
                expected.add(value)
    return expected


def reconcile_children(
    *,
    fhir_database: Any,
    family: ChildFamily,
    parent_oids: Sequence[str],
    expected_identifiers: set[str],
    logger: Any | None = None,
    retire_ceiling: RetireCeiling | None = DEFAULT_RETIRE_CEILING,
) -> int:
    """Retire live children for ``parent_oids`` that are missing from ``expected_identifiers``.

    ``retire_ceiling`` er værnet mod masse-pensionering; ``None`` slår det fra.
    """
    oids = [oid for oid in parent_oids if oid]
    if not oids:
        return 0

    live = _fetch_live_children(fhir_database, family, oids)
    to_retire = fhir_ids_to_retire(live, expected_identifiers)
    if retire_ceiling is not None:
        live_count = len({fhir_id for fhir_id, _ in live})
        limit = retire_ceiling.limit(live_count)
        if len(to_retire) > limit:
            if logger is not None:
                logger.error(
                    "XML child reconcile %s: retire ceiling exceeded — %d to retire of"
                    " %d live for %d parents (limit=%d, floor=%d, fraction=%.2f)."
                    " Skipping retire; a deliberate mass cleanup must re-run with a"
                    " raised ceiling.",
                    family.resource_type,
                    len(to_retire),
                    live_count,
                    len(oids),
                    limit,
                    retire_ceiling.floor,
                    retire_ceiling.fraction,
                )
            return 0
    if logger is not None and len(to_retire) >= _WARN_RETIRE_THRESHOLD:
        logger.warning(
            "XML child reconcile %s: retiring %d resources for %d parents",
            family.resource_type,
            len(to_retire),
            len(oids),
        )
    updated = _mark_entered_in_error(fhir_database, family, to_retire)
    if logger is not None:
        logger.info(
            "XML child reconcile %s: %d live, %d expected, %d retired",
            family.resource_type,
            len(live),
            len(expected_identifiers),
            updated,
        )
    return updated


def _fetch_live_children(
    fhir_database: Any,
    family: ChildFamily,
    parent_oids: Sequence[str],
) -> list[tuple[str, str]]:
    table = family.table_name()
    system = family.identifier_system
    if not _SAFE_IDENTIFIER_SYSTEM_RE.match(system):
        raise ValueError(
            f"Identifier system {system!r} contains characters unsafe for "
            "SQL-literal interpolation"
        )
    already_deleted = json.dumps(
        {"extension": [{"url": EXTENSION_FHIR_IS_DELETED, "valueInteger": 1}]}
    )
    # Systemet interpoleres som SQL-literal, ikke som parameter: et
    # udtryksindeks matcher kun et ordret identisk udtryk, og en bunden
    # parameter er uigennemsigtig for planlæggeren. Værdien er valideret
    # ovenfor. Den partielle WHERE-klausul er den samme som indeksets, så
    # planlæggeren kan matche det.
    ident_expr = f"extract_identifier_value(o.resource, '{system}')"
    system_fragment = json.dumps([{"system": system}])
    status_filter = _live_status_sql(family.status_kind)
    # ÉN forælder pr. forespørgsel — med vilje. En mængdebaseret form (VALUES-
    # join over 1.000 intervaller) målte 125 ms, men planlæggeren opgav
    # opslagsplanen, da tabellens statistik ændrede sig efter en natlig
    # indlæsning, og flippede til fuld indeksscanning med alle intervaller
    # afprøvet pr. række — målt til at hænge i timevis, to dage i træk. LATERAL
    # er også efterprøvet og fejlplanlægges tilsvarende. Et enkelt interval med
    # to parametre kan ikke fejlplanlægges: opslag i præfiks-indekset er den
    # eneste fornuftige plan, uanset statistik — målt til 0,05 ms også som
    # GENERISK plan efter gentagne kørsler.
    #
    # PREPARE én gang, EXECUTE pr. forælder: planlægningen (målt 4,4 ms pr.
    # opslag — 60-90× dyrere end selve eksekveringen) betales én gang i stedet
    # for tusindvis af gange pr. chunk.
    # Ét interval-opslag PR. FORÆLDER, afsendt i driverens pipeline-tilstand.
    #
    # Formen er destillatet af fem efterprøvede forsøg: LIKE-mønstre kan ikke
    # bruge indekset; mængdeformer (VALUES-join, LATERAL) flipper til fuld
    # scanning, når statistikken driver; batch-nøglerums-intervallet læser hele
    # familien, når chunkens forældre er spredt (delta-vinduer indeholder
    # opdateringer af gamle rækker); og sekventielle enkelt-opslag drukner i
    # rundture (målt 15,4 ms/opslag mod 0,06 ms i pipeline — 256×).
    #
    # Pipeline-tilstanden sender grupper af opslag uden at vente på svar
    # imellem, så rundtur-klassen forsvinder, mens hvert opslag forbliver ét
    # interval med to parametre — den eneste form, planlæggeren ikke kan
    # fejlplanlægge, uanset statistik.
    sql = (
        f"SELECT o.id, {ident_expr} "
        f"FROM {table} o "
        f"WHERE o.resource->'identifier' @> '{system_fragment}'::jsonb "
        f"AND {ident_expr} ~>=~ %s AND {ident_expr} ~<~ %s "
        f"AND {status_filter} "
        f"AND NOT (o.resource @> %s::jsonb)"
    )
    found: list[tuple[str, str]] = []
    with fhir_database.get_connection() as conn:
        # Driveren forbereder forespørgslen server-side fra første kørsel, så
        # planlægningen betales én gang pr. forbindelse.
        if hasattr(conn, "prepare_threshold"):
            conn.prepare_threshold = 0
        with conn.cursor() as cur:
            # Fail-fast: en fremtidig plan- eller låseændring skal fejle
            # højlydt på minutter, ikke hænge tavst i timevis. Nulstilles, da
            # forbindelsen kan være fra en pulje.
            cur.execute("SET statement_timeout = '120s'")
        try:
            pipeline = getattr(conn, "pipeline", None)
            if callable(pipeline):
                gruppe_stoerrelse = 500
                for i in range(0, len(parent_oids), gruppe_stoerrelse):
                    gruppe = parent_oids[i : i + gruppe_stoerrelse]
                    cursors = []
                    with conn.pipeline():
                        for oid in gruppe:
                            lo, hi = family.prefix_bounds(oid)
                            gcur = conn.cursor()
                            gcur.execute(sql, [lo, hi, already_deleted])
                            cursors.append(gcur)
                    for gcur in cursors:
                        for fhir_id, ident in gcur.fetchall():
                            if fhir_id and ident:
                                found.append((str(fhir_id), str(ident)))
                        gcur.close()
            else:
                # Uden pipeline (fx attrapper i test): sekventielle opslag med
                # samme forespørgselsform.
                with conn.cursor() as cur:
                    for oid in parent_oids:
                        lo, hi = family.prefix_bounds(oid)
                        cur.execute(sql, [lo, hi, already_deleted])
                        for fhir_id, ident in cur.fetchall():
                            if fhir_id and ident:
                                found.append((str(fhir_id), str(ident)))
        finally:
            # Oprydningen må ikke maskere den oprindelige fejl: er forbindelsen
            # død efter et fejlet opslag, fejler RESET også — og dén fejl er
            # støj, mens den første er årsagen.
            try:
                with conn.cursor() as cur:
                    cur.execute("RESET statement_timeout")
            except Exception:
                pass
    return found


def _live_status_sql(status_kind: StatusKind) -> str:
    if status_kind == "verificationStatus":
        return (
            "COALESCE(o.resource#>>'{verificationStatus,coding,0,code}', '') <> 'entered-in-error'"
        )
    return "COALESCE(o.resource->>'status', '') <> 'entered-in-error'"


def _mark_entered_in_error(
    fhir_database: Any,
    family: ChildFamily,
    fhir_ids: Sequence[str],
) -> int:
    if not fhir_ids:
        return 0
    table = family.table_name()
    ext_url = EXTENSION_FHIR_IS_DELETED
    marker_fragment = json.dumps([{"url": ext_url, "valueInteger": 1}])
    status_sql, status_params = _status_set_sql(family.status_kind)
    updated_total = 0
    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            for offset in range(0, len(fhir_ids), _UPDATE_BATCH):
                batch = list(fhir_ids[offset : offset + _UPDATE_BATCH])
                cur.execute(f"SELECT COALESCE(MAX(txid), 0) FROM {table};")
                base_txid = cur.fetchone()[0]
                for j, row_id in enumerate(batch):
                    txid = base_txid + j + 1
                    cur.execute(
                        f"""
                        UPDATE {table}
                        SET resource = jsonb_set(
                            {status_sql},
                            '{{extension}}',
                            COALESCE(
                                (
                                    SELECT jsonb_agg(elem)
                                    FROM jsonb_array_elements(
                                        COALESCE(resource->'extension', '[]'::jsonb)
                                    ) AS elem
                                    WHERE elem->>'url' != %s
                                ),
                                '[]'::jsonb
                            ) || %s::jsonb
                        ),
                        txid = %s,
                        ts = CURRENT_TIMESTAMP,
                        status = 'updated'::resource_status
                        WHERE id = %s
                        """,
                        [*status_params, ext_url, marker_fragment, txid, row_id],
                    )
                    updated_total += cur.rowcount
                conn.commit()
    return updated_total


def _status_set_sql(status_kind: StatusKind) -> tuple[str, list[Any]]:
    if status_kind == "verificationStatus":
        payload = json.dumps(
            {
                "coding": [
                    {
                        "system": _CONDITION_VER_STATUS_SYSTEM,
                        "code": "entered-in-error",
                    }
                ]
            }
        )
        # En diagnose i entered-in-error må ikke have clinicalStatus (FHIR con-5),
        # og uden clinicalStatus må den heller ikke have abatement (con-4).
        # Samme form som byggeren giver en slettet diagnose.
        base = (
            "(resource - 'clinicalStatus' - 'abatementDateTime' - 'abatementAge'"
            " - 'abatementPeriod' - 'abatementRange' - 'abatementString')"
        )
        return f"jsonb_set({base}, '{{verificationStatus}}', %s::jsonb)", [payload]
    return "jsonb_set(resource, '{status}', %s::jsonb)", [json.dumps("entered-in-error")]

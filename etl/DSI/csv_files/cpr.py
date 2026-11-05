# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Load of the CPR register's file extracts (base extract and daily change extracts) to ext.cpr_*.

All record types are in the same fixed-width file; one pass distributes the rows across one
ext table per record type. The layout is chosen on record type and length, a known type with
an unknown length fails, and the end record's counter must match. Logs and errors carry file
name, record type and counts, never field values.
"""

# No "from __future__ import annotations": dg.Config cannot be resolved with deferred annotations.

import functools
import os
import re
from collections import Counter, defaultdict
from collections.abc import Iterator

import dagster as dg
from core.dfm_manager import DfmManager
from core.mssql_application_lock import application_lock
from core.parallel import env_int
from core.resources import FileShareResource, MSSQLResource
from DSI.csv_files._sds_tar import (
    ExtWriter,
    execute_statements,
    ext_create_sql,
    run_writers,
    upsert_register,
)
from DSI.csv_files.cpr_layouts import LAYOUTS, Layout
from DSI.csv_files.cpr_publication import CPR_PUBLICATION_LOCK, register_exists
from pydantic import Field

# d<YYMMDD>.<task number>; the task number is also in the start record.
FILNAVN = re.compile(r"^d(?P<dato>\d{6})\.(?P<opgave>[a-z]\d{6})$", re.IGNORECASE)
OPGAVE_GRUNDUDTRAEK = "l208110"
OPGAVE_AENDRING = "l208109"

STARTRECORD = "000"
SLUTRECORD = "999"
PERSONRECORD = "001"
DATA_FREMADRETTET = "996"
# Error records come after the end record and do not count in the control total.
FEJLRECORD = "910"
# The counter has eight digits; with more records, CPR writes 99999999.
TAELLER_LOFT = 99_999_999

INSERT_CHUNK_SIZE = 1000

# The delivery date is known only after the start record; rows get this and are fixed at the end.
PLADSHOLDER_DATO = "1900-01-01"

# Share of unknown records above which the run fails.
UKENDT_ANDEL_GRAENSE = 0.01


class Arkivtabel:
    """An archived record type and the key its rows are unique on."""

    def __init__(
        self,
        tabel: str,
        recordtype: str,
        noegle: tuple,
        erstattes: bool = False,
        personkolonne: str = "pnr",
        sidst_set: bool = False,
        akkumulerer: bool = False,
    ):
        self.tabel = tabel
        self.recordtype = recordtype
        self.noegle = noegle
        # True where the source does not identify the rows uniquely: replaced per person.
        self.erstattes = erstattes
        # The column in position 4-13 that carries the person; pnrgaeld for 022.
        self.personkolonne = personkolonne
        # True when the delivery date must move every time the person is in a delivery.
        self.sidst_set = sidst_set
        # True for an event log: new rows are appended, nothing is deleted.
        self.akkumulerer = akkumulerer


# The record type in position 1-3; the variant is chosen on the length (vaelg_layout).
ARKIVTABELLER = (
    Arkivtabel("cpr_001_person", "001", ("pnr",)),
    Arkivtabel("cpr_002_adresse", "002", ("pnr",)),
    Arkivtabel("cpr_003_klarskriftadresse", "003", ("pnr",)),
    Arkivtabel("cpr_004_beskyttelse", "004", ("pnr", "beskyttype")),
    Arkivtabel("cpr_005_udrejse", "005", ("pnr",)),
    Arkivtabel("cpr_008_navn", "008", ("pnr",)),
    Arkivtabel("cpr_010_statsborgerskab", "010", ("pnr",)),
    Arkivtabel("cpr_012_civilstand", "012", ("pnr",)),
    Arkivtabel("cpr_015_foraeldre", "015", ("pnr",)),
    # PNRGAELD is the current number and carries the person; PNR is the historical one.
    Arkivtabel(
        "cpr_022_historisk_personnummer",
        "022",
        ("pnr", "pnrgaeld"),
        personkolonne="pnrgaeld",
    ),
    # No combination of source columns is unique.
    Arkivtabel(
        "cpr_023_historisk_adresse",
        "023",
        ("pnr", "tilflydto", "fraflydto"),
        erstattes=True,
    ),
    # The declared key is not unique.
    Arkivtabel(
        "cpr_034_historisk_beskyttelse",
        "034",
        ("pnr", "beskyttype", "start_dt_beskyttelse"),
        erstattes=True,
    ),
    Arkivtabel("cpr_037_personid", "037", ("pnr",)),
    Arkivtabel("cpr_038_sogn", "038", ("pnr",)),
    # The events that affect the person during the day; the log is kept.
    Arkivtabel(
        "cpr_099_haendelse",
        "099",
        ("pnr", "timestampu", "haendelse", "afledtmrk", "nglkonst"),
        akkumulerer=True,
    ),
    # Whether the person is delivered in future, and when she was last delivered.
    Arkivtabel("cpr_996_data_fremadrettet", "996", ("pnr",), sidst_set=True),
)
TABEL_PR_RECORDTYPE = {t.recordtype: t for t in ARKIVTABELLER}
TABEL_PR_NAVN = {t.tabel: t for t in ARKIVTABELLER}

# Danish characters are transliterated so field names are not chopped up at an underscore.
_TRANSLITTERATION = str.maketrans({"æ": "ae", "ø": "oe", "å": "aa", "é": "e", "ü": "u"})


@functools.cache
def kolonnenavn(feltnavn: str) -> str:
    """CPR's field name as a column name: lower case and underscores."""
    return re.sub(r"[^a-z0-9]+", "_", feltnavn.lower().translate(_TRANSLITTERATION)).strip("_")


def _byg_variantindeks() -> dict:
    """Record type code -> {record length: layout}."""
    indeks: dict = defaultdict(dict)
    for navn, layout in LAYOUTS.items():
        if layout.recordlaengde is None:
            continue
        kode = navn[:3]
        indeks[kode][layout.recordlaengde] = layout
    return dict(indeks)


VARIANTER = _byg_variantindeks()


class UkendtRecordlaengde(ValueError):
    """A known record type with a length no layout has; the message carries no line content."""


def vaelg_layout(kode: str, laengde: int) -> Layout | None:
    """The layout for a record chosen on record type and length; None for an unknown type."""
    varianter = VARIANTER.get(kode)
    if varianter is None:
        return None
    layout = varianter.get(laengde)
    if layout is None:
        raise UkendtRecordlaengde(
            f"record type {kode} has length {laengde}; known lengths are "
            f"{sorted(varianter)}. The delivery probably uses a different "
            f"variant than the one the layout table knows."
        )
    return layout


def filnavnsdato(navn: str) -> str | None:
    """The production date in the file name as YYYYMMDD, or None for a foreign name."""
    m = FILNAVN.match(navn)
    return "20" + m.group("dato") if m else None


def filart(navn: str) -> str:
    """'grundudtraek', 'aendring' or a description of what the file otherwise is."""
    m = FILNAVN.match(navn)
    if not m:
        return "ukendt filnavnsmønster"
    opgave = m.group("opgave").lower()
    if opgave == OPGAVE_GRUNDUDTRAEK:
        return "grundudtraek"
    if opgave == OPGAVE_AENDRING:
        return "aendring"
    return f"andet opgavenummer ({opgave})"


def vaelg_filer(rod: str, maks: int = 0) -> list:
    """Delivery files in date order, oldest first."""
    kandidater = []
    for navn in sorted(os.listdir(rod)):
        sti = os.path.join(rod, navn)
        if not os.path.isfile(sti):
            continue
        m = FILNAVN.match(navn)
        if m is None:
            continue
        kandidater.append((m.group("dato"), navn, sti))
    kandidater.sort()
    if maks:
        kandidater = kandidater[-maks:]
    return [sti for _, _, sti in kandidater]


class BlokdelingFejlede(ValueError):
    """A blocked file has no unambiguous split into records; the message carries no file content."""


def _snitlaengde(rest: str, varianter: dict, ved_filslut: bool) -> int | None:
    """Next record's length: the candidate length followed by a known three-character record type.

    Longest first. None means that more must be read, or that the file is wrong.
    """
    for laengde in sorted(varianter, reverse=True):
        if ved_filslut and len(rest) == laengde:
            return laengde
        if len(rest) >= laengde + 3 and rest[laengde : laengde + 3] in VARIANTER:
            return laengde
    return None


def linjer(sti: str, encoding: str) -> Iterator[str]:
    """Records from a delivery file, including a blocked file without line breaks."""
    with open(sti, encoding=encoding, errors="strict", newline="") as fh:
        foerste = fh.read(65_536)
        blokket = "\n" not in foerste and "\r" not in foerste
        fh.seek(0)
        if not blokket:
            for linje in fh:
                yield linje.rstrip("\r\n")
            return
        navn = os.path.basename(sti)
        rest = ""
        while True:
            klump = fh.read(1_048_576)
            rest += klump
            ved_filslut = not klump
            while len(rest) >= 3:
                varianter = VARIANTER.get(rest[0:3])
                if not varianter:
                    # Unknown type in a blocked file: no length, so the next record's start is lost.
                    raise BlokdelingFejlede(
                        f"{navn}: unknown record type in blocked file; cannot determine "
                        f"where the next record begins."
                    )
                laengde = _snitlaengde(rest, varianter, ved_filslut)
                if laengde is None:
                    if ved_filslut:
                        raise BlokdelingFejlede(
                            f"{navn}: none of the record type's documented lengths "
                            f"resynchronises the file. Either the variant is new, or "
                            f"the file is truncated."
                        )
                    break
                yield rest[:laengde]
                rest = rest[laengde:]
            if ved_filslut:
                if rest.strip():
                    raise BlokdelingFejlede(
                        f"{navn}: {len(rest)} characters left after the last whole record. "
                        f"The file is truncated."
                    )
                return


def udskaer(linje: str, layout: Layout) -> dict:
    """Field values from a record without the record type; empty values are '', not None."""
    ud = {}
    for felt in layout.felter:
        navn = kolonnenavn(felt.navn)
        if navn == "recordtype":
            continue
        ud[navn] = linje[felt.position - 1 : felt.position - 1 + felt.laengde].strip()
    return ud


class Leverance:
    """One file's metadata and counts, as they are after the pass."""

    def __init__(self, sti: str):
        self.sti = sti
        self.navn = os.path.basename(sti)
        self.art = filart(self.navn)
        self.opgavenr = ""
        self.produktionsdato = ""
        self.forrige_produktionsdato = ""
        self.records_i_slutrecord: int | None = None
        self.records_laest = 0
        self.pr_recordtype: Counter = Counter()
        self.ukendte_recordtyper: Counter = Counter()
        # The person record comes exactly once per person.
        self.personer_beroert = 0
        # Record type 996 per (PERSONKREDS, ABONNEMENT): whether the person is delivered in future.
        self.data_fremadrettet: Counter = Counter()

    @property
    def ukendte_records(self) -> int:
        """Number of records with a record type the layout table does not know — the count only."""
        return sum(self.ukendte_recordtyper.values())

    @property
    def ukendt_andel(self) -> float:
        i_alt = self.records_laest or 1
        return self.ukendte_records / i_alt

    @property
    def records_i_taelleren(self) -> int:
        """The records read that the end record's counter covers (error records excluded)."""
        return self.records_laest - self.pr_recordtype.get(FEJLRECORD, 0)

    @property
    def kontroltotal_differens(self) -> int | None:
        """The end record's counter minus what it must cover. Zero when the file is whole."""
        if self.records_i_slutrecord is None:
            return None
        return self.records_i_slutrecord - self.records_i_taelleren

    def kontroltotal_stemmer(self) -> bool | None:
        """End record's counter against what was read; at the ceiling 99,999,999 only downwards."""
        if self.records_i_slutrecord is None:
            return None
        if self.records_i_slutrecord == TAELLER_LOFT:
            return self.records_i_taelleren >= TAELLER_LOFT
        return self.kontroltotal_differens == 0


def gennemloeb(sti: str, encoding: str, leverance: "Leverance") -> Iterator[tuple]:
    """Read one file and yield (archive table, row); ``leverance`` is complete once exhausted."""
    for linje in linjer(sti, encoding):
        if not linje.strip():
            continue
        kode = linje[0:3]
        laengde = len(linje)

        if kode == STARTRECORD:
            layout = vaelg_layout(kode, laengde)
            if layout is not None:
                v = udskaer(linje, layout)
                leverance.opgavenr = v.get("opgavenr", "")
                leverance.produktionsdato = v.get("proddto", "")
                leverance.forrige_produktionsdato = v.get("proddtoforrig", "")
            continue
        if kode == SLUTRECORD:
            layout = vaelg_layout(kode, laengde)
            if layout is not None:
                taeller = udskaer(linje, layout).get("taeller", "")
                if taeller.isdigit():
                    leverance.records_i_slutrecord = int(taeller)
            continue

        leverance.records_laest += 1
        layout = vaelg_layout(kode, laengde)
        if layout is None:
            leverance.ukendte_recordtyper[kode] += 1
            continue

        leverance.pr_recordtype[kode] += 1
        if kode == PERSONRECORD:
            leverance.personer_beroert += 1
        elif kode == DATA_FREMADRETTET:
            v = udskaer(linje, layout)
            leverance.data_fremadrettet[
                f"personkreds={v.get('personkreds', '')}|abonnement={v.get('abonnement', '')}"
            ] += 1

        tabel = TABEL_PR_RECORDTYPE.get(kode)
        if tabel is not None:
            yield tabel.tabel, udskaer(linje, layout)


def kontroller_leverance(leverance: Leverance) -> None:
    """Fail hard if the file is not as it should be."""
    if leverance.art not in ("grundudtraek", "aendring"):
        raise ValueError(
            f"{leverance.navn}: {leverance.art}. Only base extracts and daily "
            f"change extracts can be loaded."
        )
    if not leverance.produktionsdato:
        raise ValueError(
            f"{leverance.navn}: the start record is missing or could not be read. "
            f"The file is incomplete or not a CPR extract."
        )
    filnavnets_dato = filnavnsdato(leverance.navn)
    if filnavnets_dato != leverance.produktionsdato:
        raise ValueError(
            f"{leverance.navn}: the file name says {filnavnets_dato}, the start record "
            f"{leverance.produktionsdato}. The watermark follows the start record and "
            f"the file selection the file name, so the file would be loaded again every night."
        )
    if leverance.records_i_slutrecord is None:
        raise ValueError(
            f"{leverance.navn}: the end record is missing or carries no readable "
            f"counter. Without it the file's completeness cannot be determined, and a "
            f"truncated file would be archived as complete."
        )
    if leverance.kontroltotal_stemmer() is False:
        raise ValueError(
            f"{leverance.navn}: the end record states "
            f"{leverance.records_i_slutrecord} records, but "
            f"{leverance.records_i_taelleren} were read (error records excluded). "
            f"The file is incomplete."
        )
    if leverance.ukendt_andel > UKENDT_ANDEL_GRAENSE:
        # Numbers only: the unknown codes themselves are not disclosed, here or in metadata.
        raise ValueError(
            f"{leverance.navn}: {leverance.ukendt_andel:.1%} of the records "
            f"({leverance.ukendte_records} records, {len(leverance.ukendte_recordtyper)} "
            f"different record types) have an unknown record type. Above the limit of "
            f"{UKENDT_ANDEL_GRAENSE:.0%} the file is presumably something other than expected."
        )


def kontroller_kaede(
    leverance: "Leverance",
    sidst_indlaest: str | None,
    sidst_var_grundudtraek: bool = False,
) -> tuple | None:
    """Does the delivery follow on from the one last loaded?

    The archive begins with a base extract, and a delivery older than the archive is rejected. A
    gap after a base extract is returned as a blind window (from, to); a gap in operation fails.
    """
    if sidst_indlaest is None:
        if leverance.art != "grundudtraek":
            raise ValueError(
                f"{leverance.navn}: the archive is empty, and the first load must be "
                f"a base extract. A change extract covers only the persons who "
                f"changed that day."
            )
        return None
    if leverance.produktionsdato == sidst_indlaest:
        return None
    if leverance.produktionsdato < sidst_indlaest:
        raise ValueError(
            f"{leverance.navn}: the delivery is from {leverance.produktionsdato}, but "
            f"the archive is at {sidst_indlaest}. An older delivery would roll the "
            f"persons in the file back; set spring_kaedekontrol for a deliberate "
            f"rebuild."
        )
    if leverance.art == "grundudtraek":
        return None
    if not leverance.forrige_produktionsdato:
        return None
    if leverance.forrige_produktionsdato == sidst_indlaest:
        return None
    if sidst_var_grundudtraek:
        return (sidst_indlaest, leverance.forrige_produktionsdato)
    raise ValueError(
        f"{leverance.navn}: gap in the delivery chain. The file points to "
        f"{leverance.forrige_produktionsdato} as the previous production date, "
        f"but the archive is at {sidst_indlaest}. The delivery in between "
        f"must be loaded first."
    )


def kolonner_for_tabel(tabel: str) -> tuple:
    """The archive table's columns in the layout's order, from the longest variant."""
    arkivtabel = TABEL_PR_NAVN[tabel]
    varianter = VARIANTER[arkivtabel.recordtype]
    layout = varianter[max(varianter)]
    return tuple(kolonnenavn(f.navn) for f in layout.felter if kolonnenavn(f.navn) != "recordtype")


def _ext_create_sql(tabel: str) -> str:
    """DROP and CREATE for one ext table; the key columns are NOT NULL."""
    noegle = TABEL_PR_NAVN[tabel].noegle
    return ext_create_sql(tabel, (), [(k, 500, k in noegle) for k in kolonner_for_tabel(tabel)])


def _genskab_ext(conn) -> None:
    """Drop and recreate all ext tables, also those the delivery does not touch; does not commit."""
    for arkivtabel in ARKIVTABELLER:
        execute_statements(conn, _ext_create_sql(arkivtabel.tabel), commit=False)


def sidst_indlaeste_leverance(mssql: MSSQLResource, *, kun_arkiverede: bool = True) -> tuple:
    """Production date and kind of the newest delivery where all the tables are archived.

    With ``kun_arkiverede=False`` a delivery that is loaded to ext but not yet archived also
    counts.
    """
    arkiveret = "HAVING COUNT(*) = COUNT([arkiveret_tidspunkt])" if kun_arkiverede else ""
    if not register_exists(mssql):
        return None, False
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT TOP 1 CONVERT(CHAR(8), [leverance_dato], 112), MAX([indlaesningstype])
                  FROM [meta].[CPRLeverance]
                 GROUP BY [leverance_dato]
                {arkiveret}
                 ORDER BY [leverance_dato] DESC
                """
            )
            raekke = cursor.fetchone()
            if not raekke or not raekke[0]:
                return None, False
            return raekke[0], raekke[1] == "grundudtraek"
        finally:
            cursor.close()


def indlaest_filnavn(mssql: MSSQLResource, dato: str | None) -> str | None:
    """The file name the delivery register has for a production date, or None."""
    if not dato or not register_exists(mssql):
        return None
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT MAX([filnavn]) FROM [meta].[CPRLeverance] "
                "WHERE [leverance_dato] = CONVERT(DATE, ?, 112)",
                (dato,),
            )
            raekke = cursor.fetchone()
            return raekke[0] if raekke and raekke[0] else None
        finally:
            cursor.close()


def kontroller_ingen_anden_fil_paa_datoen(rod: str, dato: str | None, filnavn: str | None) -> None:
    """Fail if the share has another file with the same production date as the loaded one."""
    if not dato or not filnavn:
        return
    andre = sorted(
        navn
        for sti in vaelg_filer(rod)
        if filnavnsdato(navn := os.path.basename(sti)) == dato and navn != filnavn
    )
    if andre:
        raise ValueError(
            f"The share has another file with production date {dato} than the loaded one "
            f"({filnavn}): {', '.join(andre)}. The register can hold only one per date; "
            f"clarify with DOL which one applies."
        )


def naeste_leverance(rod: str, sidst_indlaest: str | None) -> str | None:
    """The oldest file not yet loaded, or None."""
    ventende = _ventende(rod, sidst_indlaest)
    return ventende[0][1] if ventende else None


def efterslaeb(rod: str, sidst_indlaest: str | None) -> list[str]:
    """The production dates (YYYYMMDD) of the deliveries on the share that the archive does not
    yet know — oldest first. Empty when the archive is up to date."""
    return [dato for dato, _ in _ventende(rod, sidst_indlaest)]


def _ventende(rod: str, sidst_indlaest: str | None) -> list[tuple[str, str]]:
    """(date, path) for the deliveries after the watermark, oldest first; two on one date fail."""
    ventende = [
        (dato, sti)
        for sti in vaelg_filer(rod)
        if (dato := filnavnsdato(os.path.basename(sti)))
        and (sidst_indlaest is None or dato > sidst_indlaest)
    ]
    datoer = [dato for dato, _ in ventende]
    dubletter = sorted({dato for dato in datoer if datoer.count(dato) > 1})
    if dubletter:
        raise ValueError(
            f"Several deliveries with the same production date on the share "
            f"({', '.join(dubletter)}). The register can hold only one per date; "
            f"clarify with DOL which one applies."
        )
    return ventende


class CprExtractConfig(dg.Config):
    """Control of a single load."""

    fil: str | None = Field(
        default=None,
        description=(
            "Load exactly this file instead of the oldest one missing. "
            "For reloading a single delivery. The chain check still "
            "applies unless spring_kaedekontrol is set."
        ),
    )
    spring_kaedekontrol: bool = Field(
        default=False,
        description=(
            "Load even though the delivery does not follow on from the last one loaded. "
            "Only for a deliberate rebuild; a gap otherwise means that the archive is "
            "missing a day and cannot rely on being up to date."
        ),
    )


@dg.asset(
    name="ext_cpr",
    group_name="cpr",
    deps=["staging_infra_source_cpr"],
    compute_kind="fastbredde",
    # Same key as arc_cpr; requires a limit in the instance's configuration.
    tags={"domain": "cpr", "layer": "extract", "source": "cpr", "dagster/concurrency_key": "cpr"},
    description=(
        "Load one CPR delivery from the DOL share to ext.cpr_* — one table per "
        "record type, distributed in one pass. Oldest unloaded delivery "
        "first, so the change extracts come in production-date order."
    ),
)
def ext_cpr(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    cpr_share: FileShareResource,
    config: CprExtractConfig,
) -> dg.MaterializeResult:
    with application_lock(mssql_target_staging, CPR_PUBLICATION_LOCK):
        rod = cpr_share.root()
        encoding = cpr_share.encoding
        sidst, sidst_var_grundudtraek = sidst_indlaeste_leverance(mssql_target_staging)
        kontroller_ingen_anden_fil_paa_datoen(
            rod, sidst, indlaest_filnavn(mssql_target_staging, sidst)
        )

        sti = os.path.join(rod, config.fil) if config.fil else naeste_leverance(rod, sidst)
        if sti is None:
            context.log.info("No new delivery to load; the archive is at %s", sidst)
            # Ext is emptied without a new file too, so the previous delivery is not archived again.
            with mssql_target_staging.get_connection() as conn:
                _genskab_ext(conn)
                # get_connection does not commit.
                conn.commit()
            return dg.MaterializeResult(
                metadata={
                    "leverance": "ingen ny",
                    "arkivet_staar_paa": sidst or "tomt",
                    "leverancer_i_efterslaeb": 0,
                }
            )
        if not os.path.isfile(sti):
            raise ValueError(f"{os.path.basename(sti)} does not exist in the delivery directory.")

        # Log-only DFM run that borrows the archive's checkpoint.
        dfm = DfmManager(mssql_target_staging, context)
        run_ctx = dfm.start_dfm_run(
            "ext",
            "cpr",
            checkpoint_from=("arc", ARKIVTABELLER[0].tabel),
            dagster_run_id=context.run_id,
            mode="full_load",
        )
        try:
            leverance = Leverance(sti)
            skrevet: Counter = Counter()
            with mssql_target_staging.get_connection() as conn:
                _genskab_ext(conn)
                kolonner = {t.tabel: kolonner_for_tabel(t.tabel) for t in ARKIVTABELLER}
                skrivere = {
                    tabel: ExtWriter(
                        conn,
                        tabel,
                        (*felter, "leverance_dato"),
                        context,
                        chunk_size=INSERT_CHUNK_SIZE,
                        row_word="rows",
                    )
                    for tabel, felter in kolonner.items()
                }

                def _indlaes() -> tuple | None:
                    for tabel, raekke in gennemloeb(sti, encoding, leverance):
                        skrivere[tabel].add(
                            (*(raekke.get(k, "") for k in kolonner[tabel]), PLADSHOLDER_DATO)
                        )
                    kontroller_leverance(leverance)
                    if config.spring_kaedekontrol:
                        return None
                    return kontroller_kaede(leverance, sidst, sidst_var_grundudtraek)

                blindt_vindue = run_writers(skrivere, _indlaes)
                skrevet.update({tabel: s.total for tabel, s in skrivere.items() if s.total})

                cursor = conn.cursor()
                try:
                    for arkivtabel in ARKIVTABELLER:
                        cursor.execute(
                            f"UPDATE [ext].[{arkivtabel.tabel}] "
                            "SET [leverance_dato] = CONVERT(DATE, ?, 112)",
                            (leverance.produktionsdato,),
                        )
                    conn.commit()
                finally:
                    cursor.close()

            if blindt_vindue:
                context.log.warning(
                    "Blind window: there are no change extracts for %s to %s. "
                    "Persons who changed in that window and not since are left with "
                    "their picture from the base extract.",
                    blindt_vindue[0],
                    blindt_vindue[1],
                )
            registrer_leverance(mssql_target_staging, leverance, blindt_vindue)
            if not leverance.personer_beroert:
                # Nothing to archive; without the stamp the delivery would be left as pending.
                marker_arkiveret(mssql_target_staging, leverance.produktionsdato)
                context.log.info("%s has no persons and is stamped as archived", leverance.navn)
        except Exception as exc:
            dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
            raise

        i_alt = sum(skrevet.values())
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=i_alt)
        venter = efterslaeb(rod, leverance.produktionsdato)
        if venter:
            context.log.warning(
                "Backlog: %d delivery(ies) waiting on the share after %s; the oldest is %s.",
                len(venter),
                leverance.produktionsdato,
                venter[0],
            )
        context.log.info(
            "%s: %d records read, %d rows written to %d tables",
            leverance.navn,
            leverance.records_laest,
            i_alt,
            len(skrevet),
        )
        return dg.MaterializeResult(
            metadata={
                "leverance": leverance.navn,
                "art": leverance.art,
                "produktionsdato": leverance.produktionsdato,
                "forrige_produktionsdato": leverance.forrige_produktionsdato,
                "records_laest": leverance.records_laest,
                "personer": leverance.personer_beroert,
                "raekker_skrevet": i_alt,
                "pr_tabel": dg.MetadataValue.json(dict(skrevet)),
                "recordtyper": dg.MetadataValue.json(dict(leverance.pr_recordtype)),
                "data_fremadrettet": dg.MetadataValue.json(dict(leverance.data_fremadrettet)),
                # Unknown codes are counted in total; the codes themselves are not disclosed.
                "ukendte_records": leverance.ukendte_records,
                "ukendte_recordtyper_antal": len(leverance.ukendte_recordtyper),
                "ukendt_andel_pct": round(leverance.ukendt_andel * 100, 3),
                "kontroltotal_stemmer": str(leverance.kontroltotal_stemmer()),
                "kontroltotal_differens": leverance.kontroltotal_differens,
                "fejlrecords": leverance.pr_recordtype.get(FEJLRECORD, 0),
                "blindt_vindue": (
                    f"{blindt_vindue[0]} til {blindt_vindue[1]}" if blindt_vindue else "intet"
                ),
                "leverancer_i_efterslaeb": len(venter),
                "naeste_ventende_leverance": venter[0] if venter else "ingen",
            }
        )


# Warns when the backlog on the share is larger than the limit.
EFTERSLAEB_GRAENSE = env_int("CPR_EFTERSLAEB_GRAENSE", 3, minimum=1)


@dg.asset_check(
    name="cpr_efterslaeb_er_lille",
    asset="ext_cpr",
    description=(
        "Warns when there are more unloaded CPR deliveries on the share than the "
        "limit allows. Count and dates of deliveries — never content."
    ),
    compute_kind="fastbredde",
)
def cpr_efterslaeb_er_lille(
    mssql_target_staging: MSSQLResource, cpr_share: FileShareResource
) -> dg.AssetCheckResult:
    """How many deliveries wait after the latest loaded one, and how old is the oldest?"""
    rod = cpr_share.root()
    # Same starting point as ext_cpr's metadata: the loaded file, archived or not.
    sidst, _ = sidst_indlaeste_leverance(mssql_target_staging, kun_arkiverede=False)
    venter = efterslaeb(rod, sidst)
    metadata = {
        "leverancer_i_efterslaeb": len(venter),
        "graense": EFTERSLAEB_GRAENSE,
        "sidst_indlaeste_leverance": sidst or "ingen",
        "aeldste_ventende": venter[0] if venter else "ingen",
        "nyeste_ventende": venter[-1] if venter else "ingen",
    }
    return dg.AssetCheckResult(
        passed=len(venter) <= EFTERSLAEB_GRAENSE,
        severity=dg.AssetCheckSeverity.WARN,
        metadata=metadata,
    )


cpr_extract_checks = [cpr_efterslaeb_er_lille]


cpr_extract_assets = [ext_cpr]


def marker_arkiveret(mssql: MSSQLResource, leverance_dato: str) -> None:
    """Stamp the delivery's rows as archived. Only a completed run stamps."""
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE [meta].[CPRLeverance]
                   SET [arkiveret_tidspunkt] = CAST(SYSDATETIME() AS datetime2(0)),
                       [dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
                 WHERE [leverance_dato] = CONVERT(date, ?, 112)
                """,
                (leverance_dato,),
            )
            conn.commit()
        finally:
            cursor.close()


def registrer_leverance(
    mssql: MSSQLResource, leverance: "Leverance", blindt_vindue: tuple | None = None
) -> None:
    """Write or update the delivery's row per archive table in the control table."""
    indlaesningstype = "grundudtraek" if leverance.art == "grundudtraek" else "aendring"
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for arkivtabel in ARKIVTABELLER:
                upsert_register(
                    cursor,
                    "CPRLeverance",
                    ("kildetabel", "leverance_dato"),
                    {
                        "kildetabel": arkivtabel.tabel,
                        "leverance_dato": leverance.produktionsdato,
                        "opgavenr": leverance.opgavenr,
                        "forrige_produktionsdato": leverance.forrige_produktionsdato or None,
                        "filnavn": leverance.navn,
                        "indlaesningstype": indlaesningstype,
                        "records_i_fil": leverance.records_i_slutrecord,
                        "records_indlaest": leverance.records_laest,
                        "blindt_vindue_fra": blindt_vindue[0] if blindt_vindue else None,
                        "blindt_vindue_til": blindt_vindue[1] if blindt_vindue else None,
                    },
                    casts={
                        "leverance_dato": "CONVERT(DATE, ?, 112)",
                        "forrige_produktionsdato": "TRY_CONVERT(DATE, ?, 112)",
                        "blindt_vindue_fra": "TRY_CONVERT(DATE, ?, 112)",
                        "blindt_vindue_til": "TRY_CONVERT(DATE, ?, 112)",
                    },
                    insert_only=("forrige_produktionsdato",),
                )
            conn.commit()
        finally:
            cursor.close()

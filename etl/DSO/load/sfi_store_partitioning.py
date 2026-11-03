# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Månedspartitioneret columnstore for fhir.SFI_datapoint_store og SWITCH-indlæsning.

Storen er en clustered columnstore på månedspartitioner (occurred_at_dt) med to smalle,
alignede B-træer: den unikke datapoint-nøgle og patientopslag. Aftagernes aggregeringer
(perioder, elementkoder, modeller) læses fra columnstoren i batch-tilstand; punktopslag
pr. patient går via B-træet.

Et backfill-vindue skrives til arbejdstabellen som heap, columnstoren og B-træerne bygges
over vinduets egne rækker, og partitionerne skiftes ind i storen som metadata. Prisen er
uafhængig af historikkens størrelse. Rækker til fyldte partitioner (efterslæbere, NULL-
datoer) skrives direkte i storen. Slettede rækker og åbne deltastore-rækkegrupper ryddes
med REORGANIZE efter hver kørsel.

Hvis det ikke performer: en aftagerforespørgsel, der seeker (én patient, én kontakt), får
et alignet B-træ med occurred_at_dt som sidste nøgle; en scanning, der ikke rammer
segment-eliminering, får en sortering af columnstoren på den kolonne (CREATE ... ORDER).
Bærer DELETE-joinet tiden, så bær occurred_at_dt med fra byggeviewet ind i joinet.
Hvis SWITCH afvises på serveren, står fejlteksten i loggen, og rækkerne flyttes med INSERT.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Literal

PARTITION_COLUMN = "occurred_at_dt"
PARTITION_FUNCTION = "pf_SFI_datapoint_store_Maaned"
PARTITION_SCHEME = "ps_SFI_datapoint_store_Maaned"
SEQUENCE_SCHEMA = "fhir"
SEQUENCE_NAME = "seq_SFI_datapoint_store"
STORE_TABLE = "[fhir].[SFI_datapoint_store]"
WORK_TABLE = "[fhir].[SFI_datapoint_store_backfill]"
# Holdetabel til efterslæbere: de få rækker, der allerede ligger i en partition, et vindue
# skal skiftes ind i (daglige leverancer med dato i en måned, der endnu ikke er backfillet).
LEFTOVER_TABLE = "[fhir].[SFI_datapoint_store_efterslaeb]"
# En partition med højst så mange rækker regnes som tom for SWITCH: rækkerne flyttes til
# holdetabellen før vinduet og lægges tilbage bagefter, hvis deres ressource ikke blev
# genbygget. Målt: oktober 2022 havde 204 efterslæbere og kostede 43 min via indeks-
# frakobling mod ~8 min via SWITCH.
NEARLY_EMPTY_MAX_ROWS = 100_000
# Første grænse = historikkens start (migreringsbulken 2021-Q4). RANGE RIGHT: NULL og
# alt før første grænse lander i partition 1; alt fra og med sidste grænse i den sidste.
FIRST_BOUNDARY = date(2021, 10, 1)
LAST_BOUNDARY = date(2035, 12, 1)
EMPTY_PARTITIONS_TEMP = "#empty_partitions"
BATCH_ROWS_TEMP = "#batch_rows"
COLUMNSTORE_INDEX = "CCI_FHIR_SFI_datapoint_store"
SWITCH_LOCK_TIMEOUT_MS = 120_000
SWITCH_RETRIES = 5
# Andel af en stikprøve af vinduets ressourcer, der skal falde i tomme partitioner,
# før SWITCH-stien vælges frem for indeks-frakobling. Et rent historikvindue giver
# ~1,0; en genkørsel af et allerede indlæst vindue giver 0,0.
SWITCH_MIN_EMPTY_SHARE = 0.5
PROBE_SAMPLE_SIZE = 10_000


def monthly_boundaries(first: date = FIRST_BOUNDARY, last: date = LAST_BOUNDARY) -> list[date]:
    """Den første i hver måned fra ``first`` til og med ``last``."""
    if first.day != 1 or last.day != 1:
        raise ValueError("partitionsgrænser skal være den første i måneden")
    out: list[date] = []
    year, month = first.year, first.month
    while (year, month) <= (last.year, last.month):
        out.append(date(year, month, 1))
        month += 1
        if month == 13:
            year, month = year + 1, 1
    return out


def boundary_literals(boundaries: list[date] | None = None) -> str:
    """Grænserne som T-SQL-literaler til CREATE PARTITION FUNCTION."""
    return ", ".join(f"N'{d.isoformat()}T00:00:00'" for d in (boundaries or monthly_boundaries()))


def partition_number_expr(column_ref: str) -> str:
    return f"$PARTITION.{PARTITION_FUNCTION}({column_ref})"


@dataclass(frozen=True)
class StoreIndex:
    """Ét B-træ på storen. Samme definition bruges af DDL'en (kontrakttest) og af
    arbejdstabellen, fordi SWITCH kræver identiske indeks på begge sider. På en
    columnstore skal alle B-træer være alignede; ``aligned=False`` findes kun til
    målebænkens rowstore-sammenligning."""

    name: str
    key_columns: tuple[str, ...]
    include_columns: tuple[str, ...] = ()
    unique: bool = False
    aligned: bool = True

    def create_sql(self, table: str) -> str:
        unique = "UNIQUE " if self.unique else ""
        keys = ", ".join(f"[{c}] ASC" for c in self.key_columns)
        include = ""
        if self.include_columns:
            include = "\n    INCLUDE (" + ", ".join(f"[{c}]" for c in self.include_columns) + ")"
        options = "DATA_COMPRESSION = PAGE"
        if self.aligned:
            options += ", STATISTICS_INCREMENTAL = ON"
            on_clause = f"[{PARTITION_SCHEME}]([{PARTITION_COLUMN}])"
        else:
            on_clause = "[PRIMARY]"
        return (
            f"CREATE {unique}NONCLUSTERED INDEX [{self.name}]\n"
            f"    ON {table} ({keys}){include}\n"
            f"    WITH ({options})\n"
            f"    ON {on_clause};"
        )


# Rækkefølgen er byggerækkefølgen på arbejdstabellen: det unikke indeks først, så en
# dublet i et vindue fejler, før der er brugt tid på resten. Kun to B-træer: alt, der
# aggregerer, læses fra columnstoren, og hvert B-træ koster en skrivning pr. række.
STORE_INDEXES: tuple[StoreIndex, ...] = (
    StoreIndex(
        name="UX_FHIR_SFI_datapoint_store_DatapointKey",
        key_columns=("datapoint_key", PARTITION_COLUMN),
        unique=True,
    ),
    # Punktopslag pr. patient: uden B-træ koster et patientopslag en scanning af alle
    # rækkegrupper (målt 351 ms ved 25 mio. rækker, lineært voksende).
    StoreIndex(
        name="IX_FHIR_SFI_datapoint_store_PatientOccurredDt",
        key_columns=("patient_id", PARTITION_COLUMN),
    ),
)

ALIGNED_INDEXES: tuple[StoreIndex, ...] = tuple(i for i in STORE_INDEXES if i.aligned)
NON_ALIGNED_INDEXES: tuple[StoreIndex, ...] = tuple(i for i in STORE_INDEXES if not i.aligned)


def partition_function_sql() -> str:
    """Vogtet CREATE PARTITION FUNCTION — samme tekst som basis-DDL'en (kontrakttest)."""
    return f"""
        IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = N'{PARTITION_FUNCTION}')
        BEGIN
            CREATE PARTITION FUNCTION [{PARTITION_FUNCTION}] (DATETIME2(0))
            AS RANGE RIGHT FOR VALUES ({boundary_literals()});
        END
    """


def partition_scheme_sql() -> str:
    return f"""
        IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = N'{PARTITION_SCHEME}')
        BEGIN
            CREATE PARTITION SCHEME [{PARTITION_SCHEME}]
                AS PARTITION [{PARTITION_FUNCTION}] ALL TO ([PRIMARY]);
        END
    """


def columnstore_index_name(table: str = STORE_TABLE) -> str:
    """CCI_FHIR_<tabelnavn>: storen og arbejdstabellen har hver sit navn."""
    bare = table.split(".")[-1].strip("[]")
    return f"CCI_FHIR_{bare}"


def columnstore_index_sql(table: str = STORE_TABLE, *, drop_existing: bool = False) -> str:
    """Den klyngede columnstore på partitionsskemaet — samme tekst i DDL'en (kontrakttest).
    ``drop_existing`` konverterer et eksisterende klynget rowstore-indeks med samme navn
    (målebænkens kopier, der starter som heap uden partitionering)."""
    options = "\n    WITH (DROP_EXISTING = ON)" if drop_existing else ""
    return (
        f"CREATE CLUSTERED COLUMNSTORE INDEX [{columnstore_index_name(table)}]\n"
        f"    ON {table}{options}\n"
        f"    ON [{PARTITION_SCHEME}]([{PARTITION_COLUMN}]);"
    )


def reorganize_sql(table: str = STORE_TABLE) -> str:
    """Luk åbne deltastore-rækkegrupper og fjern slettede rækker (rækkegrupper med over
    ti procent slettede flettes). Online og proportional med det, der skal ryddes."""
    return (
        f"ALTER INDEX [{columnstore_index_name(table)}] ON {table} "
        f"REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);"
    )


def columnstore_exists_sql(table: str = STORE_TABLE) -> str:
    return f"""
        SELECT CASE WHEN EXISTS (
            SELECT 1 FROM sys.indexes i
            WHERE i.object_id = OBJECT_ID(N'{table}') AND i.type = 5
        ) THEN 1 ELSE 0 END
    """


def clustered_key_sql(table: str, constraint_name: str) -> str:
    """Et klynget rowstore-nøgleindeks til målebænkens rowstore-sammenligning (kopier
    skabt med SELECT INTO): unik på (id, partitionskolonne), PAGE-komprimeret, på skemaet."""
    return (
        f"CREATE UNIQUE CLUSTERED INDEX [{constraint_name}]\n"
        f"    ON {table} ([sfi_datapoint_id] ASC, [{PARTITION_COLUMN}] ASC)\n"
        f"    WITH (DATA_COMPRESSION = PAGE)\n"
        f"    ON [{PARTITION_SCHEME}]([{PARTITION_COLUMN}]);"
    )


# ---------------------------------------------------------------------------
# Metadata-SQL
# ---------------------------------------------------------------------------


def table_is_partitioned_sql(table: str = STORE_TABLE) -> str:
    """1 hvis tabellens klyngede indeks (rowstore eller columnstore) eller heap ligger på
    et partitionsskema."""
    return f"""
        SELECT CASE WHEN EXISTS (
            SELECT 1
            FROM sys.indexes i
            JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
            WHERE i.object_id = OBJECT_ID(N'{table}')
              AND i.index_id IN (0, 1)
        ) THEN 1 ELSE 0 END
    """


def table_exists_sql(table: str) -> str:
    return f"SELECT CASE WHEN OBJECT_ID(N'{table}', N'U') IS NULL THEN 0 ELSE 1 END"


def empty_partitions_temp_sql(table: str = STORE_TABLE) -> str:
    """Materialisér storens tomme og næsten tomme partitioner (højst
    NEARLY_EMPTY_MAX_ROWS rækker) i sessionen; returnerer antallet.

    dm_db_partition_stats.row_count vedligeholdes af DML og er den tælling, der er
    aktuel nok til routing. Er den alligevel forældet, fejler SWITCH på en ikke-tom
    partition, og faldet tilbage til almindelig INSERT holder resultatet korrekt. De
    næsten tomme partitioners rækker flyttes til holdetabellen, før vinduet indlæses
    (SwitchLoadScope).
    """
    return f"""
        SET NOCOUNT ON;
        IF OBJECT_ID(N'tempdb..{EMPTY_PARTITIONS_TEMP}') IS NOT NULL
            DROP TABLE {EMPTY_PARTITIONS_TEMP};

        SELECT ps.partition_number
        INTO {EMPTY_PARTITIONS_TEMP}
        FROM sys.dm_db_partition_stats AS ps
        WHERE ps.object_id = OBJECT_ID(N'{table}')
          AND ps.index_id IN (0, 1)
          AND ps.row_count <= {NEARLY_EMPTY_MAX_ROWS};

        CREATE UNIQUE CLUSTERED INDEX [IX_empty_partitions]
            ON {EMPTY_PARTITIONS_TEMP} ([partition_number]);

        SELECT COUNT(*) AS empty_partitions FROM {EMPTY_PARTITIONS_TEMP};
    """


def populated_partitions_sql(table: str) -> str:
    """Fyldte partitioner for heap (index_id 0, arbejdstabellen under indlæsning) eller
    klynget indeks (1, storen)."""
    return f"""
        SELECT ps.partition_number, ps.row_count
        FROM sys.dm_db_partition_stats AS ps
        WHERE ps.object_id = OBJECT_ID(N'{table}')
          AND ps.index_id IN (0, 1)
          AND ps.row_count > 0
        ORDER BY ps.partition_number
    """


def leftover_partitions_sql(table: str = STORE_TABLE) -> str:
    """(partition, rækker) for de partitioner i #empty_partitions, der ikke er helt tomme."""
    return f"""
        SELECT ps.partition_number, ps.row_count
        FROM sys.dm_db_partition_stats AS ps
        INNER JOIN {EMPTY_PARTITIONS_TEMP} AS e ON e.[partition_number] = ps.partition_number
        WHERE ps.object_id = OBJECT_ID(N'{table}')
          AND ps.index_id IN (0, 1)
          AND ps.row_count > 0
        ORDER BY ps.partition_number
    """


def move_leftovers_sql(
    partition_number: int,
    *,
    insert_columns: str,
    store: str = STORE_TABLE,
    leftover: str = LEFTOVER_TABLE,
) -> str:
    """Flyt en næsten tom partitions rækker (med id) til holdetabellen og tøm partitionen,
    så den kan modtage et SWITCH."""
    part = partition_number_expr(f"[{PARTITION_COLUMN}]")
    return f"""
        INSERT INTO {leftover} (
            [sfi_datapoint_id],
            {insert_columns}
        )
        SELECT
            [sfi_datapoint_id],
            {insert_columns}
        FROM {store}
        WHERE {part} = {int(partition_number)};

        TRUNCATE TABLE {store} WITH (PARTITIONS ({int(partition_number)}));
    """


def restore_leftovers_sql(
    *,
    insert_columns: str,
    numbered_table: str | None,
    store: str = STORE_TABLE,
    leftover: str = LEFTOVER_TABLE,
) -> str:
    """Læg efterslæbere tilbage i storen (med deres id) og tøm holdetabellen. Rækker, hvis
    ressource står i vinduets kandidatsæt, er genbygget af vinduet og lægges ikke tilbage
    (DELETE+INSERT-semantikken). Uden kandidatsæt lægges alt tilbage (reparation)."""
    where = ""
    if numbered_table:
        where = f"""
        WHERE NOT EXISTS (
            SELECT 1 FROM {numbered_table} AS c
            WHERE c.[source_resource] = l.[source_resource]
              AND c.[source_resource_id] = l.[source_resource_id]
        )"""
    return f"""
        INSERT INTO {store} (
            [sfi_datapoint_id],
            {insert_columns}
        )
        SELECT
            [sfi_datapoint_id],
            {insert_columns}
        FROM {leftover} AS l{where};

        SELECT @@ROWCOUNT AS restored_rows;

        TRUNCATE TABLE {leftover};
    """


def nonclustered_indexes_sql(table: str) -> str:
    """(navn, is_disabled) for alle nonclustered B-træer på tabellen."""
    return f"""
        SELECT i.name, i.is_disabled
        FROM sys.indexes i
        WHERE i.object_id = OBJECT_ID(N'{table}')
          AND i.type = 2
        ORDER BY i.name
    """


def all_indexes_sql(table: str) -> str:
    """(navn, type, is_disabled) for B-træer (2) og columnstore (5) på tabellen, B-træer
    først: de droppes før columnstoren, så tabellen ender som heap."""
    return f"""
        SELECT i.name, i.type, i.is_disabled
        FROM sys.indexes i
        WHERE i.object_id = OBJECT_ID(N'{table}')
          AND i.type IN (2, 5)
        ORDER BY i.type, i.name
    """


def probe_empty_share_sql(numbered_table: str, total_resources: int) -> str:
    """Stikprøve: andel af vinduets ressourcer, hvis NUVÆRENDE kildedato falder i en
    tom partition. Skridtet holder stikprøven på ~PROBE_SAMPLE_SIZE rækker uanset
    vinduets størrelse og undgår at sortere hele nummereringstabellen.

    Datoen udledes som i byggeviewet (QR.authored / Observation.effective_date med
    samme TRY_CONVERT-kæde), så partitionsnummeret er det samme, som rækkerne får.
    """
    stride = max(1, int(total_resources) // PROBE_SAMPLE_SIZE)
    return f"""
        SET NOCOUNT ON;
        SELECT
            COUNT_BIG(*) AS sampled,
            SUM(CASE WHEN e.[partition_number] IS NOT NULL THEN 1 ELSE 0 END) AS in_empty
        FROM {numbered_table} AS c
        {source_date_join_sql("c")}
        LEFT JOIN {EMPTY_PARTITIONS_TEMP} AS e
            ON e.[partition_number] = {partition_number_expr("d.dt")}
        WHERE c.[row_id] % {stride} = 0;
    """


def source_date_expr(qr_alias: str = "qr", obs_alias: str = "o") -> str:
    """Ressourcens NUVÆRENDE kildedato som i byggeviewet (QR.authored / Observation.
    effective_date, samme TRY_CONVERT-kæde), så partitionsnummeret er det samme, som
    rækkerne får. Bruges af stikprøven og af sletningens datointerval."""
    raw = f"COALESCE({qr_alias}.[authored], {obs_alias}.[effective_date])"
    return (
        "COALESCE("
        f"TRY_CONVERT(DATETIME2(0), {raw}, 126), "
        f"TRY_CONVERT(DATETIME2(0), {raw}, 121), "
        f"TRY_CONVERT(DATETIME2(0), {raw}, 23))"
    )


def source_date_join_sql(resources_alias: str) -> str:
    """Joins, der giver hver ressource i ``resources_alias`` sin nuværende kildedato som
    ``d.dt``."""
    return f"""LEFT JOIN [fhir].[QuestionnaireResponse] AS qr
            ON {resources_alias}.[source_resource] = N'QuestionnaireResponse'
           AND qr.[qr_id] = {resources_alias}.[source_resource_id]
        LEFT JOIN [fhir].[Observation] AS o
            ON {resources_alias}.[source_resource] = N'Observation'
           AND o.[observation_id] = {resources_alias}.[source_resource_id]
        CROSS APPLY (SELECT {source_date_expr()} AS dt) AS d"""


def batch_date_window_sql(batch_table: str) -> str:
    """Portionens datointerval (og om den har ressourcer uden dato) som variabler.

    Storen har ikke noget B-træ på ressource-id, så sletningen er en scanning af
    columnstoren; et join på datoen fra en temp-tabel giver ingen eliminering, fordi
    optimizeren ikke kender datoerne før joinet. Et interval i variabler gør: både
    partitions- og rækkegruppe-eliminering rammer, så kun portionens måneder læses.
    Intervallet er kun smalt, når portionerne er dannet i datoorden
    (``changed_numbered_sql``); én gammel dato i portionen åbner ellers hele historikken.
    """
    return f"""
        DECLARE @dt_lo DATETIME2(0), @dt_hi DATETIME2(0), @dt_null_count INT;
        SELECT
            @dt_lo = MIN(d.dt),
            @dt_hi = MAX(d.dt),
            @dt_null_count = SUM(CASE WHEN d.dt IS NULL THEN 1 ELSE 0 END)
        FROM {batch_table} AS br
        {source_date_join_sql("br")};
    """


def changed_numbered_sql(changed_table: str, numbered_table: str) -> str:
    """Nummerér de ændrede ressourcer i kildedatoens orden (uden dato til sidst), så hver
    portion dækker et smalt datointerval, og sletningen kun læser portionens måneder."""
    return f"""
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY
                    CASE WHEN d.dt IS NULL THEN 1 ELSE 0 END,
                    d.dt,
                    c.[source_resource],
                    c.[source_resource_id]
            ) AS row_id,
            c.[source_resource],
            c.[source_resource_id]
        INTO {numbered_table}
        FROM {changed_table} AS c
        {source_date_join_sql("c")};
    """


def scoped_delete_sql(store: str, batch_table: str) -> str:
    """Slet portionens ressourcer i storen inden for portionens datointerval; ressourcer
    uden dato rammer partition 1 (IS NULL). Efterlader @deleted_rows til INSERT-halen.

    Rækker fra en ressource, hvis dato er RETTET i kilden, kan ligge uden for
    intervallet og overleve her; afstemningen sletter uden dato og samler dem op.
    """
    join = f"""INNER JOIN {batch_table} AS br
            ON br.[source_resource] = tgt.[source_resource]
           AND br.[source_resource_id] = tgt.[source_resource_id]"""
    return f"""
        DECLARE @deleted_rows BIGINT = 0;
        IF @dt_lo IS NOT NULL
        BEGIN
            DELETE tgt
            FROM {store} AS tgt
            {join}
            WHERE tgt.[{PARTITION_COLUMN}] >= @dt_lo
              AND tgt.[{PARTITION_COLUMN}] <= @dt_hi;
            SET @deleted_rows = @@ROWCOUNT;
        END
        IF @dt_null_count > 0
        BEGIN
            DELETE tgt
            FROM {store} AS tgt
            {join}
            WHERE tgt.[{PARTITION_COLUMN}] IS NULL;
            SET @deleted_rows += @@ROWCOUNT;
        END
    """


# ---------------------------------------------------------------------------
# Skrive-SQL
# ---------------------------------------------------------------------------


def routed_insert_sql(
    *,
    insert_columns: str,
    batch_select_sql: str,
    store: str = STORE_TABLE,
    work: str = WORK_TABLE,
) -> str:
    """Hale til en portions-batch i SWITCH-tilstand.

    Byggeforespørgslen materialiseres ÉN gang i ``#batch_rows`` og deles derefter:
    rækker til partitioner, der var tomme ved vinduets start, går til arbejdstabellen;
    resten (efterslæbere med dato i en fyldt partition, NULL-datoer efter første
    vindue) går direkte i storen som hidtil. Forudsætter ``@deleted_rows`` fra
    DELETE-trinnet og ``#empty_partitions`` fra vinduets start.
    """
    part = partition_number_expr(f"r.[{PARTITION_COLUMN}]")
    return f"""
        IF OBJECT_ID(N'tempdb..{BATCH_ROWS_TEMP}') IS NOT NULL
            DROP TABLE {BATCH_ROWS_TEMP};

        SELECT *
        INTO {BATCH_ROWS_TEMP}
        FROM (
            {batch_select_sql}
        ) AS rows_;

        INSERT INTO {work} (
            {insert_columns}
        )
        SELECT
            {insert_columns}
        FROM {BATCH_ROWS_TEMP} AS r
        WHERE EXISTS (
            SELECT 1 FROM {EMPTY_PARTITIONS_TEMP} AS e
            WHERE e.[partition_number] = {part}
        );

        DECLARE @work_rows BIGINT = @@ROWCOUNT;

        INSERT INTO {store} (
            {insert_columns}
        )
        SELECT
            {insert_columns}
        FROM {BATCH_ROWS_TEMP} AS r
        WHERE NOT EXISTS (
            SELECT 1 FROM {EMPTY_PARTITIONS_TEMP} AS e
            WHERE e.[partition_number] = {part}
        );

        DECLARE @store_rows BIGINT = @@ROWCOUNT;

        DROP TABLE {BATCH_ROWS_TEMP};

        SELECT
            @deleted_rows AS deleted_rows,
            @work_rows + @store_rows AS inserted_rows,
            @work_rows AS work_rows,
            @store_rows AS store_rows;
    """


def switch_partition_sql(
    partition_number: int, *, source: str = WORK_TABLE, target: str = STORE_TABLE
) -> str:
    return (
        f"ALTER TABLE {source} SWITCH PARTITION {int(partition_number)} "
        f"TO {target} PARTITION {int(partition_number)};"
    )


def fallback_insert_sql(
    partition_number: int,
    *,
    insert_columns: str,
    source: str = WORK_TABLE,
    target: str = STORE_TABLE,
) -> str:
    """Når SWITCH ikke kan gennemføres for én partition: flyt rækkerne som almindelig
    INSERT (samme pris som i dag) og tøm partitionen i arbejdstabellen."""
    part = partition_number_expr(f"[{PARTITION_COLUMN}]")
    return f"""
        INSERT INTO {target} (
            {insert_columns}
        )
        SELECT
            {insert_columns}
        FROM {source}
        WHERE {part} = {int(partition_number)};

        TRUNCATE TABLE {source} WITH (PARTITIONS ({int(partition_number)}));
    """


def update_statistics_sql(
    partition_numbers: list[int],
    table: str = STORE_TABLE,
    indexes: tuple[StoreIndex, ...] = ALIGNED_INDEXES,
) -> str:
    """Statistik for de skiftede partitioner, ét statement pr. alignet indeks.

    SWITCH opdaterer ikke statistik, og ændringstælleren, der udløser autoopdatering,
    rører sig ikke ved et skift. ON PARTITIONS kræver inkrementel statistik, og det har
    kun de alignede B-træer (columnstoren og ikke-alignede indeks kan ikke være
    inkrementelle — målt på serveren: "This type of statistics is not supported to be
    incremental").
    """
    numbers = ", ".join(str(int(p)) for p in sorted(set(partition_numbers)))
    return "\n".join(
        f"UPDATE STATISTICS {table} ([{ix.name}]) WITH RESAMPLE ON PARTITIONS ({numbers});"
        for ix in indexes
    )


def is_lock_timeout(error: BaseException) -> bool:
    text = str(error)
    return "1222" in text or "Lock request time out" in text


# ---------------------------------------------------------------------------
# SWITCH-scope omkring portionsløkken
# ---------------------------------------------------------------------------


@dataclass
class SwitchReport:
    empty_partitions: int = 0
    work_rows: int = 0
    store_rows: int = 0
    switched_partitions: list[int] = field(default_factory=list)
    fallback_partitions: list[int] = field(default_factory=list)
    index_build_ms: dict[str, int] = field(default_factory=dict)
    switch_ms: int = 0
    statistics_ms: int = 0
    leftover_partitions: list[int] = field(default_factory=list)
    leftover_rows: int = 0
    restored_rows: int = 0
    finalized: bool = False

    def as_metadata(self) -> dict[str, int | str]:
        return {
            "backfill_empty_partitions": self.empty_partitions,
            "backfill_work_rows": self.work_rows,
            "backfill_store_rows": self.store_rows,
            "switch_partitions": len(self.switched_partitions),
            "switch_fallback_partitions": len(self.fallback_partitions),
            "switch_index_build_ms": sum(self.index_build_ms.values()),
            "switch_ms": self.switch_ms,
            "switch_statistics_ms": self.statistics_ms,
            "switch_leftover_partitions": len(self.leftover_partitions),
            "switch_leftover_rows": self.leftover_rows,
            "switch_restored_rows": self.restored_rows,
            "switch_partition_numbers": ", ".join(str(p) for p in self.switched_partitions) or "-",
        }


def _ms_since(started_ns: int) -> int:
    return (time.monotonic_ns() - started_ns) // 1_000_000


class SwitchLoadScope:
    """Indlæs et backfill-vindue via arbejdstabellen og skift det ind til sidst.

    Bruges INDEN FOR refresh-låsen, omkring portionsløkken, med ``routed=True``-
    batches. ``__enter__`` gør arbejdstabellen klar (tom heap uden indeks), materialiserer
    storens tomme og næsten tomme partitioner og flytter de næsten tommes efterslæbere til
    holdetabellen. ``__exit__`` uden fejl bygger columnstoren og B-træerne på
    arbejdstabellen (identiske med storens, alle alignede), skifter hver fyldt partition
    ind (fald tilbage til INSERT pr. partition ved fejl), lægger de efterslæbere tilbage,
    hvis ressource ikke blev genbygget, opdaterer statistik og rydder op. Fejler løkken,
    lægges alle efterslæbere tilbage; dør processen, gør næste kørsel det.

    Fejler løkken, efterlades arbejdstabellens rækker: de er aldrig blevet synlige i
    storen, watermarket er ikke rykket, og næste kørsel tømmer tabellen og genopbygger
    det samme delta. Slettede storerækker fra de gennemførte portioner genskabes af
    samme genkørsel — samme semantik som i dag.
    """

    def __init__(
        self,
        cursor: Any,
        conn: Any,
        log: Any,
        *,
        insert_columns: str,
        store: str = STORE_TABLE,
        work: str = WORK_TABLE,
        leftover: str = LEFTOVER_TABLE,
        numbered_table: str | None = None,
        indexes: tuple[StoreIndex, ...] = STORE_INDEXES,
    ) -> None:
        self._cursor = cursor
        self._conn = conn
        self._log = log
        self._insert_columns = insert_columns
        self._store = store
        self._work = work
        self._leftover = leftover
        self._numbered_table = numbered_table
        self._indexes = indexes
        self.report = SwitchReport()
        self.active = True

    # -- forberedelse --------------------------------------------------------

    def __enter__(self) -> SwitchLoadScope:
        prepare_work_table(self._cursor, self._conn, self._log, work=self._work)
        self._cursor.execute(empty_partitions_temp_sql(self._store))
        row = fetch_single_row(self._cursor, result_name="empty partitions")
        self.report.empty_partitions = int(row[0])
        self._conn.commit()
        self._move_leftovers()
        self._log.info(
            "SWITCH-tilstand: %d tomme partitioner i %s modtager vinduets rækker via %s; "
            "efterslæbere til fyldte partitioner skrives direkte.",
            self.report.empty_partitions,
            self._store,
            self._work,
        )
        return self

    def _move_leftovers(self) -> None:
        """Næsten tomme partitioners rækker flyttes til holdetabellen, så SWITCH kan ske."""
        self._cursor.execute(leftover_partitions_sql(self._store))
        leftovers = [(int(p), int(rows)) for p, rows in self._cursor.fetchall()]
        if not leftovers:
            return
        for partition_number, rows in leftovers:
            self._cursor.execute(
                move_leftovers_sql(
                    partition_number,
                    insert_columns=self._insert_columns,
                    store=self._store,
                    leftover=self._leftover,
                )
            )
            self.report.leftover_partitions.append(partition_number)
            self.report.leftover_rows += rows
        self._conn.commit()
        self._log.info(
            "Flyttede %d efterslæbere fra %d næsten tomme partitioner til %s, så vinduet kan "
            "skiftes ind; de lægges tilbage efter skiftet.",
            self.report.leftover_rows,
            len(leftovers),
            self._leftover,
        )

    def note_batch(self, work_rows: int, store_rows: int) -> None:
        self.report.work_rows += int(work_rows)
        self.report.store_rows += int(store_rows)

    # -- afslutning ----------------------------------------------------------

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        if exc_type is not None:
            try:
                self._conn.rollback()
            except Exception:  # noqa: BLE001 — oprydningen må ikke maskere den oprindelige fejl
                pass
            self._log.warning(
                "Portionsløkken fejlede i SWITCH-tilstand: %d rækker står i %s og skiftes IKKE ind. "
                "Næste kørsel tømmer arbejdstabellen og genopbygger deltaet.",
                self.report.work_rows,
                self._work,
            )
            if self.report.leftover_rows:
                self._restore_leftovers(numbered_table=None)
            return False
        self.finalize()
        return False

    def _restore_leftovers(self, *, numbered_table: str | None) -> None:
        try:
            self._cursor.execute(
                restore_leftovers_sql(
                    insert_columns=self._insert_columns,
                    numbered_table=numbered_table,
                    store=self._store,
                    leftover=self._leftover,
                )
            )
            row = fetch_single_row(self._cursor, result_name="restored leftovers")
            self.report.restored_rows = int(row[0])
            self._conn.commit()
            self._log.info(
                "Lagde %d af %d efterslæbere tilbage i %s (resten er genbygget af vinduet).",
                self.report.restored_rows,
                self.report.leftover_rows,
                self._store,
            )
        except (
            Exception
        ) as error:  # noqa: BLE001 — rækkerne står i holdetabellen; næste kørsel lægger dem tilbage
            try:
                self._conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            self._log.warning(
                "Efterslæbere kunne ikke lægges tilbage (%s) — de står i %s og lægges tilbage af næste kørsel.",
                error,
                self._leftover,
            )

    def finalize(self) -> None:
        populated = self._populated_work_partitions()
        if not populated:
            self._log.info("SWITCH-tilstand: ingen rækker i %s — intet at skifte ind.", self._work)
            self._cleanup_work_table()
            self.report.finalized = True
            return

        total_rows = sum(rows for _, rows in populated)
        self._log.info(
            "SWITCH-tilstand: %d rækker i %d partitioner i %s klar — bygger indeks.",
            total_rows,
            len(populated),
            self._work,
        )

        # 1. Columnstoren først (komprimerer vinduets rækker i ét træk), så B-træerne
        #    (unikt først: en dublet fejler her, før noget er skiftet ind). Alt er
        #    alignet, så ingen indeks skal slås fra omkring skiftet.
        builds = [(columnstore_index_name(self._work), columnstore_index_sql(self._work))]
        builds += [(index.name, index.create_sql(self._work)) for index in self._indexes]
        for name, statement in builds:
            started = time.monotonic_ns()
            self._cursor.execute(statement)
            self._conn.commit()
            self.report.index_build_ms[name] = _ms_since(started)
            self._log.info(
                "Byggede [%s] på %s på %d ms.", name, self._work, self.report.index_build_ms[name]
            )

        # 2. Skift partition for partition; fald tilbage til INSERT ved fejl.
        started = time.monotonic_ns()
        self._cursor.execute(f"SET LOCK_TIMEOUT {SWITCH_LOCK_TIMEOUT_MS}")
        for partition_number, rows in populated:
            if self._switch_with_retries(partition_number):
                self.report.switched_partitions.append(partition_number)
                self._log.info(
                    "Skiftede partition %d (%d rækker) ind i %s.",
                    partition_number,
                    rows,
                    self._store,
                )
            else:
                self._cursor.execute(
                    fallback_insert_sql(
                        partition_number,
                        insert_columns=self._insert_columns,
                        source=self._work,
                        target=self._store,
                    )
                )
                self._conn.commit()
                self.report.fallback_partitions.append(partition_number)
                self._log.warning(
                    "Partition %d (%d rækker) flyttet med INSERT i stedet for SWITCH.",
                    partition_number,
                    rows,
                )
        self._cursor.execute("SET LOCK_TIMEOUT -1")
        self.report.switch_ms = _ms_since(started)

        # 3. Efterslæbere tilbage i de nu fyldte partitioner — kun dem, vinduet ikke har
        #    genbygget.
        if self.report.leftover_rows:
            self._restore_leftovers(numbered_table=self._numbered_table)

        # 4. Statistik for de skiftede partitioner (ikke-fatal).
        if self.report.switched_partitions:
            started = time.monotonic_ns()
            try:
                self._cursor.execute(
                    update_statistics_sql(self.report.switched_partitions, self._store)
                )
                self._conn.commit()
            except Exception as error:  # noqa: BLE001 — statistik er en optimering
                self._log.warning(
                    "Partitionsvis statistikopdatering fejlede (%s) — kører tabelvis.", error
                )
                try:
                    self._conn.rollback()
                    self._cursor.execute(f"UPDATE STATISTICS {self._store}")
                    self._conn.commit()
                except Exception as fallback_error:  # noqa: BLE001
                    self._log.warning("Statistikopdatering sprunget over: %s", fallback_error)
            self.report.statistics_ms = _ms_since(started)

        self._cleanup_work_table()
        self.report.finalized = True
        self._log.info(
            "SWITCH-tilstand afsluttet: %d partitioner skiftet, %d flyttet med INSERT; "
            "indeksbygning %d ms, skift %d ms, statistik %d ms.",
            len(self.report.switched_partitions),
            len(self.report.fallback_partitions),
            sum(self.report.index_build_ms.values()),
            self.report.switch_ms,
            self.report.statistics_ms,
        )

    # -- hjælpere ------------------------------------------------------------

    def _populated_work_partitions(self) -> list[tuple[int, int]]:
        self._cursor.execute(populated_partitions_sql(self._work))
        return [(int(p), int(rows)) for p, rows in self._cursor.fetchall()]

    def _switch_with_retries(self, partition_number: int) -> bool:
        sql = switch_partition_sql(partition_number, source=self._work, target=self._store)
        for attempt in range(1, SWITCH_RETRIES + 1):
            try:
                self._cursor.execute(sql)
                self._conn.commit()
                return True
            except Exception as error:  # noqa: BLE001 — fejlen afgør genforsøg eller fald tilbage
                try:
                    self._conn.rollback()
                except Exception:  # noqa: BLE001
                    pass
                if is_lock_timeout(error) and attempt < SWITCH_RETRIES:
                    self._log.warning(
                        "SWITCH af partition %d ventede forgæves på lås (forsøg %d/%d) — prøver igen.",
                        partition_number,
                        attempt,
                        SWITCH_RETRIES,
                    )
                    continue
                self._log.warning(
                    "SWITCH af partition %d kunne ikke gennemføres: %s", partition_number, error
                )
                return False
        return False

    def _cleanup_work_table(self) -> None:
        prepare_work_table(self._cursor, self._conn, self._log, work=self._work, quiet=True)


def prepare_work_table(
    cursor: Any, conn: Any, log: Any, *, work: str = WORK_TABLE, quiet: bool = False
) -> None:
    """Tøm arbejdstabellen og drop alle dens indeks (B-træer først, så columnstoren),
    så næste indlæsning er ren tilføjelse til en heap. Rækker fra en afbrudt kørsel er
    aldrig blevet synlige og bygges igen af den kørsel, der finder dem."""
    cursor.execute(populated_partitions_sql(work))
    leftover = sum(int(rows) for _, rows in cursor.fetchall())
    if leftover and not quiet:
        log.warning(
            "%s indeholder %d rækker fra en afbrudt kørsel — tømmes; deltaet genopbygges i denne kørsel.",
            work,
            leftover,
        )
    cursor.execute(f"TRUNCATE TABLE {work}")
    cursor.execute(all_indexes_sql(work))
    for name, _index_type, _disabled in cursor.fetchall():
        cursor.execute(f"DROP INDEX [{name}] ON {work}")
    conn.commit()


def restore_all_leftovers(
    cursor: Any,
    conn: Any,
    log: Any,
    *,
    insert_columns: str,
    store: str = STORE_TABLE,
    leftover: str = LEFTOVER_TABLE,
) -> int:
    """Reparation ved kørselsstart: efterslæbere fra en dræbt kørsel lægges tilbage i
    storen. Returnerer antal rækker (0 = intet at gøre)."""
    cursor.execute(populated_partitions_sql(leftover))
    rows = sum(int(n) for _, n in cursor.fetchall())
    if not rows:
        return 0
    cursor.execute(
        restore_leftovers_sql(
            insert_columns=insert_columns, numbered_table=None, store=store, leftover=leftover
        )
    )
    row = fetch_single_row(cursor, result_name="restored leftovers")
    conn.commit()
    log.warning(
        "%s indeholdt %d efterslæbere fra en afbrudt kørsel — lagt tilbage i %s.",
        leftover,
        int(row[0]),
        store,
    )
    return int(row[0])


def table_is_partitioned(cursor: Any, table: str = STORE_TABLE) -> bool:
    cursor.execute(table_is_partitioned_sql(table))
    row = cursor.fetchone()
    return bool(row and int(row[0]) == 1)


def table_exists(cursor: Any, table: str) -> bool:
    cursor.execute(table_exists_sql(table))
    row = cursor.fetchone()
    return bool(row and int(row[0]) == 1)


def columnstore_exists(cursor: Any, table: str = STORE_TABLE) -> bool:
    cursor.execute(columnstore_exists_sql(table))
    row = cursor.fetchone()
    return bool(row and int(row[0]) == 1)


def reorganize_columnstore(cursor: Any, conn: Any, log: Any, table: str = STORE_TABLE) -> int:
    """REORGANIZE efter en kørsel: ikke-fatal, returnerer millisekunder (0 = sprunget over)."""
    if not columnstore_exists(cursor, table):
        return 0
    started = time.monotonic_ns()
    try:
        cursor.execute(reorganize_sql(table))
        conn.commit()
    except Exception as error:  # noqa: BLE001 — oprydning i rækkegrupper er en optimering
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        log.warning("REORGANIZE af columnstoren på %s fejlede: %s", table, error)
        return _ms_since(started)
    elapsed = _ms_since(started)
    log.info("REORGANIZE af columnstoren på %s på %d ms.", table, elapsed)
    return elapsed


def fetch_single_row(cursor: Any, *, result_name: str) -> Any:
    """Første tabulære resultat på cursoren (NOCOUNT-batches kan skubbe tomme
    resultatsæt foran)."""
    nextset = getattr(cursor, "nextset", None)
    while True:
        if getattr(cursor, "description", None):
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(f"Expected one row from {result_name}, but result set was empty")
            return row
        if nextset is None or not nextset():
            break
    raise RuntimeError(f"Could not locate tabular result set for {result_name}")

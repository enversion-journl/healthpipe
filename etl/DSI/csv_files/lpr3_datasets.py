# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""LPR3 source specification generated from SDS Metadata.

`lpr3_metadata.json` preserves the source schema harvested by
`python -m scripts.lpr3_schema DELIVERY --output PATH`. Historical file sizes
are omitted; runtime byte reconciliation reads each delivery's ExportLog.
Column order, SQL types, widths, nullability and collation remain unchanged.

All 47 input tables have separate ext tables named after the actual SDS table,
including Dim/fct prefixes. Removing those prefixes in ext would collide for
the nine pairs. They are removed only in arc, combining each pair into one of
38 targets. Key policies and every source-column disposition live here as well.
"""

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

METADATA_HEADER = (
    "column_ordinal",
    "column_name",
    "system_type_name",
    "is_nullable",
    "max_length",
    "precision",
    "scale",
    "collation_name",
)
_TYPE_RE = re.compile(r"(?P<base>[a-z][a-z0-9]*)(?:\((?P<width>\d+)\))?\Z")


@dataclass(frozen=True)
class SourceColumn:
    ordinal: int
    name: str
    source_type: str
    nullable: bool
    max_length: int
    precision: int
    scale: int
    collation: str

    @property
    def base_type(self) -> str:
        return self.source_type.split("(", 1)[0]

    @property
    def sql_type(self) -> str:
        if self.base_type == "decimal":
            return f"DECIMAL({self.precision},{self.scale})"
        if self.base_type in ("datetime2", "time"):
            return f"{self.base_type.upper()}({self.scale})"
        return self.source_type.upper()

    @property
    def wire_width(self) -> int:
        """CSV text width, shared by all readers and ext DDL (no silent truncation)."""
        kind = self.base_type
        if kind in ("nvarchar", "nchar"):
            return self.max_length // 2
        if kind in ("varchar", "char"):
            return self.max_length
        if kind == "varbinary":
            return self.max_length * 2
        if kind == "decimal":
            return self.precision + 2  # sign and decimal separator
        return {
            "bigint": 20,
            "int": 11,
            "smallint": 6,
            "tinyint": 3,
            "bit": 1,
            "date": 10,
            "datetime2": 27,
            "time": 16,
            "uniqueidentifier": 36,
        }[kind]

    @property
    def metadata_record(self) -> tuple[str, ...]:
        return (
            str(self.ordinal),
            self.name,
            self.source_type,
            str(int(self.nullable)),
            str(self.max_length),
            str(self.precision),
            str(self.scale),
            self.collation,
        )


@dataclass(frozen=True)
class LPR3SourceDataset:
    source_prefix: str
    regional: bool
    columns: tuple[SourceColumn, ...]
    expected_rows: int

    @property
    def ext_table(self) -> str:
        return f"lpr3_{self.source_prefix.lower()}"

    @property
    def source_columns(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    @property
    def metadata_records(self) -> tuple[tuple[str, ...], ...]:
        return tuple(column.metadata_record for column in self.columns)

    @property
    def arc_table(self) -> str:
        stem = re.sub(r"^(?:Dim|Fct|fct)", "", self.source_prefix).lower()
        if stem.startswith("bridge"):
            stem = "bridge_" + stem.removeprefix("bridge").lstrip("_")
        return "lpr3_" + stem

    def data_filename(self, cutoff: str) -> str:
        suffix = "_1083" if self.regional else ""
        return f"{self.source_prefix}{suffix}_{cutoff.replace('-', '')}.csv"


def source_datasets(metadata: dict) -> tuple[LPR3SourceDataset, ...]:
    datasets = []
    for prefix, item in sorted(metadata.items()):
        if tuple(item["metadata_header"]) != METADATA_HEADER:
            raise ValueError("LPR3 Metadata header differs from the supported SDS contract")
        columns = []
        for record in item["columns"]:
            if len(record) != len(METADATA_HEADER):
                raise ValueError("LPR3 Metadata column record has an invalid width")
            ordinal, name, kind, nullable, width, precision, scale, collation = record
            if not _TYPE_RE.fullmatch(kind) or nullable not in ("0", "1"):
                raise ValueError("Unsupported LPR3 metadata type or nullability")
            column = SourceColumn(
                int(ordinal),
                name,
                kind,
                nullable == "1",
                int(width),
                int(precision),
                int(scale),
                collation,
            )
            if column.wire_width <= 0 or column.wire_width > 4000:
                raise ValueError("Unsupported LPR3 CSV width")
            columns.append(column)
        if [c.ordinal for c in columns] != list(range(1, len(columns) + 1)):
            raise ValueError("LPR3 Metadata ordinals are not contiguous in source order")
        if len({c.name.casefold() for c in columns}) != len(columns):
            raise ValueError("LPR3 Metadata has duplicate column names")
        datasets.append(
            LPR3SourceDataset(prefix, item["regional"], tuple(columns), int(item["rows_exportlog"]))
        )
    return tuple(datasets)


SOURCE_DATASETS = source_datasets(
    json.loads(Path(__file__).with_name("lpr3_metadata.json").read_text(encoding="utf-8"))
)
SOURCES_BY_PREFIX = {dataset.source_prefix: dataset for dataset in SOURCE_DATASETS}
# Stamped on every ext and arc row; identifies the delivery a row came from.
DELIVERY_COLUMNS = ("kadence", "leverance_tidsstempel", "leverance_dato")

# Nine 1:1 Dim/fct pairs each become one arc target, 38 targets in total. The source's technical
# unknown row must be accounted for explicitly where it has no fact partner.
_archive_sources = defaultdict(list)
for _source in SOURCE_DATASETS:
    _archive_sources[_source.arc_table].append(_source.source_prefix)
ARCHIVE_SOURCES = {name: tuple(sources) for name, sources in sorted(_archive_sources.items())}


def extract_groups(workers: int = 4) -> tuple[tuple[str, ...], ...]:
    """Each group makes its own complete forward tar pass; procedure has its own."""
    if not 2 <= workers <= 8:
        raise ValueError("LPR3 extraction requires between two and eight groups")
    procedure = ("DimProcedureRegistrering", "fctProcedureRegistrering")
    groups: list[list[str]] = [list(procedure), *([] for _ in range(workers - 1))]
    sizes = [sum(SOURCES_BY_PREFIX[p].expected_rows for p in procedure), *([0] * (workers - 1))]
    for dataset in sorted(SOURCE_DATASETS, key=lambda d: (-d.expected_rows, d.source_prefix)):
        if dataset.source_prefix in procedure:
            continue
        index = min(range(1, workers), key=lambda i: sizes[i])
        groups[index].append(dataset.source_prefix)
        sizes[index] += dataset.expected_rows
    return tuple(tuple(group) for group in groups)


@dataclass(frozen=True)
class ArchiveColumn:
    name: str
    sql_type: str
    nullable: bool = True
    source_prefix: str | None = None
    source_column: str | None = None


@dataclass(frozen=True)
class KeyPolicy:
    key_class: str
    method: str
    components: tuple[str, ...] = ()
    versioned: bool = False


# Only source identities actually present in Metadata may be used. In
# particular, the binary-ID key exception is limited to these two source IDs.
KEY_POLICIES = {
    **{
        entity: KeyPolicy("A", "uuid", ("LPR_Entity_ID",))
        for entity in (
            "kontakt",
            "forloeb",
            "procedureregistrering",
            "markoerregistrering",
            "resultatindberetning",
        )
    },
    "kontaktlokation": KeyPolicy("A", "binary_id", ("Opholdadresse_ID",)),
    "betalingsoplysning": KeyPolicy("A", "binary_id", ("Betalingsoplysning_ID",)),
    "helbredsforloeb": KeyPolicy("B", "forloeb_roots"),
    "sygehusophold": KeyPolicy("B", "first_contact"),
    "diagnosegruppe": KeyPolicy("B", "code_set"),
    "proceduregruppe": KeyPolicy("B", "code_set"),
    "resultat": KeyPolicy("B", "report_number"),
    "adresse": KeyPolicy("B", "address"),
    "borger": KeyPolicy("C", "natural", ("CPRnummer",), True),
    "organisation": KeyPolicy("C", "organisation", versioned=True),
    "diagnose": KeyPolicy(
        "C",
        "clinical_code",
        (
            "Diagnose_Kode",
            "Diagnose_Type_Kode",
            "Diagnose_SKS_Type",
            "SenereAfkraeftet",
            "Diagnose_Type_Tekst",
        ),
        True,
    ),
    "procedure": KeyPolicy(
        "C",
        "clinical_code",
        ("Procedure_Kode", "Procedure_Type_Kode", "Procedure_SKS_Type", "Procedure_Type_Tekst"),
        True,
    ),
    "vaerdisks": KeyPolicy("C", "natural", ("Vaerdi_Kode", "Vaerdi_SKS_Type"), True),
    "dato": KeyPolicy("technical", "value", ("Dato",)),
    "tidspunkt": KeyPolicy("technical", "value", ("MinutID",)),
    "alder": KeyPolicy("technical", "natural", ("Aar", "Dage")),
    "sundhedsklynge": KeyPolicy("technical", "value", ("Sundhedsklynge Kode",)),
    "borgerdemografi": KeyPolicy(
        "technical", "natural", ("KoenID", "Aldersgruppe", "RegionID", "KommuneKode"), True
    ),
    "datakilde": KeyPolicy(
        "technical", "natural", ("ArkivKilde", "LprIndberetningsSystem", "LprType"), True
    ),
}


def entity_key_policy(entity: str) -> KeyPolicy:
    if entity in KEY_POLICIES:
        return KEY_POLICIES[entity]
    if entity.startswith("bridge_"):
        return KeyPolicy("bridge", "resolved_relations")
    source = SOURCES_BY_PREFIX[ARCHIVE_SOURCES[f"lpr3_{entity}"][0]]
    if entity in ("forloebegenskaber", "kontaktegenskaber", "markoer"):
        # These small content dimensions contain distinct rows with the same
        # codes/types and ValidFrom. Reporting parties and delivered labels
        # are part of their grain; omitting them loses source distinctions.
        return KeyPolicy(
            "technical",
            "natural",
            tuple(c.name for c in source.columns if not c.name.startswith("DW_")),
            "DW_ValidFrom" in source.source_columns,
        )
    components = tuple(c.name for c in source.columns if c.name.endswith(("_Kode", "_SKS_Type")))
    if not components:
        raise ValueError(f"LPR3 archive key policy missing for {entity}")
    return KeyPolicy("technical", "natural", components, "DW_ValidFrom" in source.source_columns)


@dataclass(frozen=True)
class ResolvedReference:
    source_prefix: str
    source_column: str
    target_entity: str
    columns: tuple[ArchiveColumn, ...]
    # Explicitly missing in the 2026 source contract, rather than guessed from
    # an unrelated dimension. The source key remains measurable in ext.
    missing_dimension: bool = False
    sentinel: str = "-1 resolves to NULL in every reference component"


@dataclass(frozen=True)
class ColumnDisposition:
    source_prefix: str
    source_column: str
    action: str
    reason: str


@dataclass(frozen=True)
class LPR3ArchiveDataset:
    arc_table: str
    sources: tuple[str, ...]
    key_policy: KeyPolicy
    columns: tuple[ArchiveColumn, ...]
    merge_keys: tuple[str, ...]
    references: tuple[ResolvedReference, ...]
    dispositions: tuple[ColumnDisposition, ...]

    @property
    def entity(self):
        return self.arc_table.removeprefix("lpr3_")

    @property
    def source_table(self):
        return f"{self.arc_table}_noegler"

    @property
    def arc_columns(self):
        return tuple(c.name for c in self.columns)

    @property
    def surrogate_columns(self):
        return tuple(
            c
            for c in self.arc_columns
            if c.casefold().startswith(("dw_ek_", "dw_id_audit_")) or c == "kildeidentiteter"
        )

    @property
    def canonical_versions(self):
        return self.entity in ("kontaktegenskaber", "diagnosegruppe", "proceduregruppe")

    @property
    def change_detect_columns(self):
        ignored = {*self.merge_keys, *self.surrogate_columns}
        return tuple(c for c in self.arc_columns if c not in ignored)

    @property
    def required_sources(self):
        targets = {r.target_entity for r in self.references if not r.missing_dimension}
        return tuple(
            sorted(
                {
                    *self.sources,
                    *(p for e in {*targets, self.entity} for p in key_source_dependencies(e)),
                }
            )
        )


def key_source_dependencies(entity):
    """Inputs of an identity, including relational fallback and group-set inputs."""
    additional = {
        "helbredsforloeb": ("FctHelbredsforloeb", "DimForloeb", "FctbridgeForloebsRelation"),
        "sygehusophold": ("FctSygehusophold", "DimKontakt", "FctBridge_Sygehusophold"),
        "diagnosegruppe": ("fctBridgeDiagnose", "DimDiagnose"),
        "proceduregruppe": ("fctBridgeProcedure", "DimProcedure"),
        "resultat": ("fctResultat", "DimResultatIndberetning"),
    }
    own = tuple(p for p in ARCHIVE_SOURCES[f"lpr3_{entity}"] if p.startswith("Dim"))
    return (*own, *additional.get(entity, ()))


def identity_columns(entity: str, role: str = "") -> tuple[ArchiveColumn, ...]:
    """Types/names are shared by an identity and all role-preserving references."""
    policy = entity_key_policy(entity)
    prefix = entity + (f"_{role.lower()}" if role else "")
    if policy.method == "uuid":
        return (ArchiveColumn(f"{prefix}_uuid", "UNIQUEIDENTIFIER", False),)
    if policy.method == "binary_id":
        return (ArchiveColumn(f"{prefix}_id", "VARBINARY(16)", False),)
    if policy.method == "value":
        source = SOURCES_BY_PREFIX[ARCHIVE_SOURCES[f"lpr3_{entity}"][0]]
        column = next(c for c in source.columns if c.name == policy.components[0])
        suffix = {"dato": "dato", "tidspunkt": "minut_id", "sundhedsklynge": "kode"}[entity]
        return (ArchiveColumn(f"{prefix}_{suffix}", column.sql_type, False),)
    columns: tuple[ArchiveColumn, ...] = (ArchiveColumn(f"{prefix}_noegle", "BINARY(32)", False),)
    if policy.versioned:
        columns += (ArchiveColumn(f"{prefix}_gyldig_fra", "DATETIME2(7)", False),)
    return columns


def resolve_reference(prefix, column):
    stem = column.removeprefix("DW_SK_")
    entities = sorted(
        (s.removeprefix("lpr3_") for s in ARCHIVE_SOURCES if not s.startswith("lpr3_bridge_")),
        key=len,
        reverse=True,
    )
    for entity in [*entities, "sygehusopholdegenskaber"]:
        if stem.casefold() == entity or stem.casefold().startswith(entity + "_"):
            role = stem[len(entity) :].lstrip("_")
            missing = entity == "sygehusopholdegenskaber"
            columns = (
                (ArchiveColumn("sygehusopholdegenskaber_noegle", "BINARY(32)"),)
                if missing
                else identity_columns(entity, role)
            )
            return ResolvedReference(
                prefix,
                column,
                entity,
                tuple(ArchiveColumn(c.name, c.sql_type) for c in columns),
                missing,
            )
    raise ValueError(f"Unmapped LPR3 reference column: {prefix}.{column}")


def archive_dataset(table, sources):
    entity = table.removeprefix("lpr3_")
    policy = entity_key_policy(entity)
    identity = identity_columns(entity)
    columns = list(identity)
    if entity in ("helbredsforloeb", "sygehusophold"):
        columns += [
            ArchiveColumn(f"{entity}_uuid", "UNIQUEIDENTIFIER"),
            ArchiveColumn("noeglemetode", "VARCHAR(32)", False),
        ]
    if entity in (
        "diagnosegruppe",
        "proceduregruppe",
        "borger",
        "organisation",
        "bridge_diagnose",
        "bridge_procedure",
    ):
        columns += [ArchiveColumn("noeglemetode", "VARCHAR(32)", False)]
    if entity in ("bridge_diagnose", "bridge_procedure"):
        columns += [ArchiveColumn("antal_kilderaekker", "BIGINT", False)]
    if entity in ("kontaktegenskaber", "diagnosegruppe", "proceduregruppe"):
        columns += [
            ArchiveColumn("antal_kilderaekker", "BIGINT", False),
            ArchiveColumn("kildeidentiteter", "NVARCHAR(MAX)"),
        ]
    references, dispositions = [], []
    seen = {c.name.casefold() for c in columns}
    for prefix in sources:
        for c in SOURCES_BY_PREFIX[prefix].columns:
            name = c.name.casefold()
            if name in ("dw_isdeleted", "dw_isinferred"):
                action, reason = (
                    "omit",
                    "Constant source flags; archive absence drives soft-delete",
                )
            elif name == f"dw_sk_{entity}":
                action, reason = (
                    "resolve",
                    "Delivery-local entity key: pair join and business identity lookup only",
                )
            elif name.startswith("dw_sk_"):
                reference = resolve_reference(prefix, c.name)
                references.append(reference)
                for derived in reference.columns:
                    if derived.name.casefold() in seen:
                        raise ValueError("Duplicate resolved LPR3 column")
                    seen.add(derived.name.casefold())
                    columns.append(derived)
                action, reason = "resolve", reference.sentinel
                if reference.missing_dimension:
                    reason += "; dimension absent from all 47 supplied tables"
            elif name in seen:
                action, reason = (
                    "omit",
                    "duplicate Dim/fct audit column; the dimension is authoritative",
                )
            else:
                if entity.startswith("bridge_") and name == "dw_ek_sundhedsklynge":
                    derived = identity_columns("sundhedsklynge")
                    reference = ResolvedReference(
                        prefix,
                        c.name,
                        "sundhedsklynge",
                        tuple(ArchiveColumn(col.name, col.sql_type) for col in derived),
                    )
                    references.append(reference)
                    columns.extend(reference.columns)
                    seen.update(col.name.casefold() for col in reference.columns)
                columns.append(ArchiveColumn(c.name, c.sql_type, True, prefix, c.name))
                seen.add(name)
                action, reason = "keep", "SDS source attribute, spelling and SQL type preserved"
            dispositions.append(ColumnDisposition(prefix, c.name, action, reason))
    return LPR3ArchiveDataset(
        table,
        sources,
        policy,
        tuple(columns),
        tuple(c.name for c in identity),
        tuple(references),
        tuple(dispositions),
    )


ARCHIVE_DATASETS = tuple(archive_dataset(t, s) for t, s in ARCHIVE_SOURCES.items())
ARCHIVES_BY_TABLE = {dataset.arc_table: dataset for dataset in ARCHIVE_DATASETS}


def archive_ddl() -> str:
    """Idempotent DDL generated from this contract; no data-dependent types."""
    blocks = [
        "-- Generated by DSI.csv_files.lpr3_datasets.archive_ddl(); do not edit by hand.\n"
        "-- SDS SQL widths/types are preserved. Derived hashes use BINARY(32).\n"
        "-- DW_SK never leaves ext; the binary-ID key exceptions are documented in lpr3_ingest_noegler.md."
    ]
    for dataset in ARCHIVE_DATASETS:
        table = dataset.arc_table
        declarations = [
            f"        [{c.name}] {c.sql_type} {'NULL' if c.nullable else 'NOT NULL'}"
            for c in dataset.columns
        ]
        declarations += [
            "        [kadence] NCHAR(1) NOT NULL",
            "        [leverance_tidsstempel] DATETIME2(0) NOT NULL",
            "        [leverance_dato] DATE NOT NULL",
            f"        [dw_is_deleted] BIT NOT NULL CONSTRAINT [DF_{table}_dw_is_deleted] DEFAULT (0)",
            f"        [dw_timestamp_insert] DATETIME2(0) NOT NULL CONSTRAINT [DF_{table}_dw_timestamp_insert] DEFAULT (CAST(SYSDATETIME() AS DATETIME2(0)))",
            f"        [dw_timestamp_update] DATETIME2(0) NOT NULL CONSTRAINT [DF_{table}_dw_timestamp_update] DEFAULT (CAST(SYSDATETIME() AS DATETIME2(0)))",
            f"        CONSTRAINT [PK_arc_{table}] PRIMARY KEY CLUSTERED ("
            + ", ".join(f"[{key}]" for key in dataset.merge_keys)
            + ") WITH (DATA_COMPRESSION=PAGE)",
        ]
        blocks.append(
            f"IF OBJECT_ID(N'[arc].[{table}]', N'U') IS NULL\nBEGIN\n"
            f"    CREATE TABLE [arc].[{table}] (\n" + ",\n".join(declarations) + "\n    );\nEND\nGO"
        )
    return "\n\n".join(blocks) + "\n"

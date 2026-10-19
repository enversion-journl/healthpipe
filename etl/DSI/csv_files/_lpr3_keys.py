# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Typed, role-preserving SQL resolution of delivery-local LPR3 identities.

Only the private ext identity maps retain source SKs. Archive rows use the
business identities in lpr3_datasets. Hashes frame every component by byte
length, distinguish NULL from empty, and use NVARCHAR(MAX) to avoid truncation.
Binary IDs are decoded before use, so hex letter casing cannot change a key.
"""

from DSI.csv_files.lpr3_datasets import ARCHIVES_BY_TABLE, DELIVERY_COLUMNS, SOURCES_BY_PREFIX


def typed_source(column, alias):
    expression = f"{alias}.[{column.name}]"
    if column.base_type in ("nvarchar", "varchar", "nchar", "char"):
        return f"CONVERT({column.sql_type},{expression})"
    expression = f"NULLIF({expression},N'')"
    if column.base_type == "decimal":
        expression = f"REPLACE({expression},N',',N'.')"
    style = ",2" if column.base_type == "varbinary" else ""
    return f"CONVERT({column.sql_type},{expression}{style})"


def framed_hash(namespace, expressions):
    # Identifiers/namespace are closed repository contracts, never source data.
    parts = [f"CAST(N'{namespace}' AS NVARCHAR(MAX))"]
    for expression in expressions:
        value = f"CONVERT(NVARCHAR(MAX),{expression})"
        parts.append(
            f"CASE WHEN {expression} IS NULL THEN N'|-1:' ELSE "
            f"CONCAT(N'|',DATALENGTH({value}),N':',{value}) END"
        )
    return "CONVERT(BINARY(32),HASHBYTES('SHA2_256',CONCAT(" + ",".join(parts) + ")))"


def identity_table(entity):
    return f"lpr3_{entity}_identitet"


def dimension_source(dataset):
    return next(SOURCES_BY_PREFIX[p] for p in dataset.sources if p.startswith("Dim"))


def source_identity_column(dataset):
    source = dimension_source(dataset)
    for kind in ("SK", "EK"):
        for column in source.columns:
            if column.name.casefold() == f"dw_{kind.lower()}_{dataset.entity}":
                return column.name
    raise ValueError("LPR3 dimension has no declared delivery-local identity")


def simple_identity_select(dataset):
    """Source UUIDs, binary source IDs and measured natural identities; relation keys differ."""
    source = dimension_source(dataset)
    policy = dataset.key_policy
    columns = {c.name: c for c in source.columns}
    key = source_identity_column(dataset)
    components = [typed_source(columns[name], "s") for name in policy.components]
    if policy.method == "uuid":
        # The source's -1 unknown row has no reported UUID. This reserved arc
        # identity preserves that row; references to source -1 still become
        # NULL. The original LPR_Entity_ID is never overwritten in payload.
        value = f"CASE WHEN s.[{key}]=N'-1' THEN CONVERT(UNIQUEIDENTIFIER,'00000000-0000-0000-0000-000000000000') ELSE {components[0]} END"
    elif policy.method in ("binary_id", "value"):
        value = components[0]
    elif policy.method in ("natural", "clinical_code"):
        value = framed_hash(f"lpr3:{dataset.entity}:v1", components)
    elif policy.method == "address":
        address = [
            "Type",
            "Kommunekode",
            "Vejkode",
            "Vejnavn",
            "Husnummer",
            "Etage",
            "Dør",
            "Postnummer",
        ]
        fallback = framed_hash("lpr3:adresse:fields:v1", [f"s.[{c}]" for c in address])
        dawa = framed_hash(
            "lpr3:adresse:dawa-unit:v1", ["s.[Type]", "LOWER(s.[AdgangsadresseID])", "s.[kvhx_key]"]
        )
        value = (
            f"CASE WHEN NULLIF(s.[AdgangsadresseID],N'') IS NULL THEN {fallback} ELSE {dawa} END"
        )
    elif policy.method == "organisation":
        variants = {
            "SOR": ("Kodetype", "SORKode"),
            "YDER": ("Kodetype", "Ydernummer"),
            "SHAK": ("Kodetype", "ShakSghKode", "ShakAfdkode", "ShakAfsKode"),
        }
        clauses = [
            f"WHEN s.[Kodetype]=N'{kind}' THEN {framed_hash('lpr3:organisation:v1', [f's.[{c}]' for c in names])}"
            for kind, names in variants.items()
        ]
        fallback = framed_hash(
            "lpr3:organisation:other:v1",
            [
                "s.[Kodetype]",
                "s.[SORKode]",
                "s.[ShakSghKode]",
                "s.[ShakAfdkode]",
                "s.[ShakAfsKode]",
                "s.[Ydernummer]",
            ],
        )
        value = "CASE " + " ".join(clauses) + f" ELSE {fallback} END"
    else:
        raise ValueError(f"LPR3 {dataset.entity} requires its measured relational key builder")
    if dataset.entity in ("borger", "organisation"):
        # Mark every historical version of an ambiguous entity. Repeated
        # versions of ONE DW_EK are not a collision; distinct DW_EKs sharing a
        # natural identity AND ValidFrom are. No delivery-local SK enters the final key.
        ek = next(c.name for c in source.columns if c.name.casefold() == f"dw_ek_{dataset.entity}")
        fallback = framed_hash(f"lpr3:{dataset.entity}:collision-source-id:v1", ["source_ek"])
        return f"""SELECT source_sk,
CASE WHEN ambiguous=1 THEN {fallback} ELSE natural_key END [{dataset.merge_keys[0]}],
valid_from [{dataset.merge_keys[1]}],
CONVERT(VARCHAR(32),CASE WHEN ambiguous=1 THEN 'technical_collision' ELSE 'natural' END) noeglemetode
FROM (SELECT *,MAX(collision) OVER (PARTITION BY source_ek) ambiguous
FROM (SELECT *,CASE WHEN MIN(source_ek) OVER (PARTITION BY natural_key,valid_from) <> MAX(source_ek) OVER (PARTITION BY natural_key,valid_from) THEN 1 ELSE 0 END collision
FROM (SELECT CONVERT(BIGINT,s.[{key}]) source_sk,CONVERT(BIGINT,s.[{ek}]) source_ek,
{value} natural_key,CONVERT(DATETIME2(7),s.[DW_ValidFrom]) valid_from
FROM [ext].[{source.ext_table}] s) candidates) collisions) identities"""
    select = [f"CONVERT(BIGINT,s.[{key}]) AS [source_sk]", f"{value} AS [{dataset.merge_keys[0]}]"]
    if dataset.canonical_versions:
        ek = next(c.name for c in source.columns if c.name.startswith("DW_EK_"))
        select.append(f"CONVERT(BIGINT,s.[{ek}]) AS source_ek")
    if policy.versioned:
        select.append(f"CONVERT(DATETIME2(7),s.[DW_ValidFrom]) AS [{dataset.merge_keys[1]}]")
    return "SELECT " + ",\n".join(select) + f"\nFROM [ext].[{source.ext_table}] s"


def resolved_view_sql(dataset):
    """Build a typed projection, joining every FK to its exact source version."""
    aliases = {prefix: f"s{i}" for i, prefix in enumerate(dataset.sources)}
    primary = next((p for p in dataset.sources if not p.startswith("Dim")), dataset.sources[0])
    joins = []
    unresolved = []
    reference_parts = []
    if len(dataset.sources) == 2:
        dim = dimension_source(dataset)
        fact = SOURCES_BY_PREFIX[primary]
        sk = source_identity_column(dataset)
        fact_sk = next(c.name for c in fact.columns if c.name.casefold() == sk.casefold())
        join_kind = "FULL OUTER JOIN" if dataset.entity == "helbredsforloeb" else "LEFT JOIN"
        joins.append(
            f"{join_kind} [ext].[{dim.ext_table}] {aliases[dim.source_prefix]} ON {aliases[dim.source_prefix]}.[{sk}]={aliases[primary]}.[{fact_sk}]"
        )
    select = {}
    if dataset.entity == "bridge_procedure":
        select["antal_kilderaekker"] = f"{aliases[primary]}.[antal_kilderaekker]"
    elif dataset.entity == "bridge_diagnose":
        select["antal_kilderaekker"] = "CONVERT(BIGINT,1)"
    if dataset.key_policy.key_class != "bridge":
        dim = dimension_source(dataset)
        dim_alias = aliases[dim.source_prefix]
        joins.append(
            f"LEFT JOIN [ext].[{identity_table(dataset.entity)}] own_key ON own_key.[source_sk]=CONVERT(BIGINT,{dim_alias}.[{source_identity_column(dataset)}])"
        )
        for key in dataset.merge_keys:
            select[key] = f"own_key.[{key}]"
        for column in (f"{dataset.entity}_uuid", "noeglemetode"):
            if column in dataset.arc_columns and column not in select:
                select[column] = f"own_key.[{column}]"
        if dataset.canonical_versions:
            ek = next(c.name for c in dim.columns if c.name.startswith("DW_EK_"))
            # One source row represents the identical payload; provenance lists
            # every DW_EK. Source SKs remain solely in the private ext lookup.
            select["antal_kilderaekker"] = "own_key.antal_kilderaekker"
            select["kildeidentiteter"] = (
                "CASE WHEN own_key.antal_kilderaekker>1 THEN ("
                f"SELECT mapped.source_ek [{ek}] "
                f"FROM ext.[{identity_table(dataset.entity)}] mapped "
                "WHERE mapped.canonical_source_sk=own_key.source_sk "
                "ORDER BY mapped.source_ek FOR JSON PATH) END"
            )
    for index, ref in enumerate(dataset.references):
        alias = f"r{index}"
        raw = f"{aliases[ref.source_prefix]}.[{ref.source_column}]"
        if ref.missing_dimension:
            select.update({c.name: f"CAST(NULL AS {c.sql_type})" for c in ref.columns})
            unresolved.append(f"CASE WHEN CONVERT(BIGINT,NULLIF({raw},N''))<>-1 THEN 1 ELSE 0 END")
            continue
        target = ARCHIVES_BY_TABLE[f"lpr3_{ref.target_entity}"]
        joins.append(
            f"LEFT JOIN [ext].[{identity_table(ref.target_entity)}] {alias} ON {alias}.[source_sk]=NULLIF(CONVERT(BIGINT,NULLIF({raw},N'')),-1)"
        )
        for output, key in zip(ref.columns, target.merge_keys, strict=True):
            select[output.name] = f"{alias}.[{key}]"
        missing = (
            f"CONVERT(BIGINT,NULLIF({raw},N''))<>-1 AND {alias}.[{target.merge_keys[0]}] IS NULL"
        )
        unresolved.append(f"CASE WHEN {missing} THEN 1 ELSE 0 END")
        # An unresolved national reference contributes only to row identity.
        # Resolved clinical FKs above remain NULL when the dimension is absent.
        resolved_part = framed_hash(
            f"lpr3:{dataset.entity}:{ref.source_column}:resolved:v1",
            [select[c.name] for c in ref.columns],
        )
        technical_part = framed_hash(
            f"lpr3:{dataset.entity}:{ref.source_column}:unresolved:v1",
            [f"CONVERT(BIGINT,{raw})"],
        )
        reference_parts.append(
            f"CASE WHEN {missing} THEN {technical_part} ELSE {resolved_part} END"
        )
    for column in dataset.columns:
        if column.source_prefix:
            source = SOURCES_BY_PREFIX[column.source_prefix]
            original = next(c for c in source.columns if c.name == column.source_column)
            select[column.name] = typed_source(original, aliases[column.source_prefix])
    if dataset.key_policy.key_class == "bridge":
        key_columns = [column.name for ref in dataset.references for column in ref.columns]
        key_columns += {
            "bridge_forloeb": ["Forloebstype"],
            "bridge_forloebsrelation": ["RelationsType", "Hierarki_Niveau"],
            "bridge_adressesundhedsklynge": [
                "Husnummer_Lige_Start",
                "Husnummer_Lige_Slut",
                "Husnummer_Ulige_Start",
                "Husnummer_Ulige_Slut",
            ],
        }.get(dataset.entity, [])
        key = framed_hash(f"lpr3:{dataset.entity}:v1", [select[c] for c in key_columns])
        if dataset.entity in ("bridge_diagnose", "bridge_procedure"):
            missing = "(" + " + ".join(unresolved) + ")>0"
            fallback = framed_hash(f"lpr3:{dataset.entity}:technical-reference:v1", reference_parts)
            key = f"CASE WHEN {missing} THEN {fallback} ELSE {key} END"
            select["noeglemetode"] = (
                f"CONVERT(VARCHAR(32),CASE WHEN {missing} THEN 'technical_reference' ELSE 'resolved_relations' END)"
            )
        select[dataset.merge_keys[0]] = key
    missing = set(dataset.arc_columns) - set(select)
    if missing:
        raise ValueError(f"LPR3 {dataset.entity} requires its bridge/grain projection")
    projection = [f"{select[c]} AS [{c}]" for c in dataset.arc_columns]
    for column in DELIVERY_COLUMNS:
        stamp = f"{aliases[primary]}.[{column}]"
        if dataset.entity == "helbredsforloeb":
            # SDS supplies one unknown dimension row without a fact. Preserve
            # that source row and its stamps; all absent fact attributes stay NULL.
            stamp = f"COALESCE({stamp},{aliases[dim.source_prefix]}.[{column}])"
        projection.append(f"{stamp} AS [{column}]")
    projection.append("(" + " + ".join(unresolved or ["0"]) + ") AS [_lpr3_uoploeste_referencer]")
    source = f"[ext].[{SOURCES_BY_PREFIX[primary].ext_table}]"
    prefix = ""
    suffix = ""
    if dataset.canonical_versions:
        suffix = "\nWHERE own_key.source_sk=own_key.canonical_source_sk"
    if dataset.entity == "bridge_procedure":
        # SQL's usual string comparison ignores case/trailing spaces. Only
        # byte-identical SOURCE rows may collapse, so compare their UTF-16
        # bytes, including source audit fields, rather than collated strings.
        partition = ",".join(
            f"CONVERT(VARBINARY(MAX),[{c}])" for c in SOURCES_BY_PREFIX[primary].source_columns
        )
        prefix = f"WITH bridge_source AS (SELECT *,COUNT_BIG(*) OVER (PARTITION BY {partition}) antal_kilderaekker,ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY (SELECT NULL)) identical_row FROM {source})\n"
        source = "bridge_source"
        suffix = f"\nWHERE {aliases[primary]}.[identical_row]=1"
    statement = (
        "SELECT "
        + ",\n".join(projection)
        + f"\nFROM {source} {aliases[primary]}\n"
        + "\n".join(joins)
        + suffix
    )
    if dataset.entity in ("bridge_diagnose", "bridge_procedure"):
        # Canonical groups can make formerly distinct source relations identical.
        # Preserve source multiplicity, and retain unequal resolved payloads so
        # the uniqueness guard still rejects them rather than choosing a row.
        columns = [c for c in dataset.arc_columns if c != "antal_kilderaekker"]
        columns += [*DELIVERY_COLUMNS, "_lpr3_uoploeste_referencer"]
        keys = ",".join(f"[{c}]" for c in columns)
        ctes = prefix.removeprefix("WITH ").rstrip() + ",\n" if prefix else ""
        statement = (
            f"WITH {ctes}resolved_rows AS ({statement})\nSELECT {keys},"
            f"SUM(antal_kilderaekker) antal_kilderaekker FROM resolved_rows GROUP BY {keys}"
        )
        prefix = ""
    return f"CREATE OR ALTER VIEW [ext].[v_{dataset.arc_table}] AS\n{prefix}{statement};"


def uuid_digest(expression):
    """SHA256 of the UUID's RFC 4122 byte form."""
    return f"HASHBYTES('SHA2_256',CONVERT(BINARY(16),REPLACE(CONVERT(CHAR(36),{expression}),'-',''),2))"


def relational_identity_sql(dataset):
    """Root/first plus a measured complete-boundary-set fallback, no source ID input.

    Temporary numeric edges avoid repeated casts and wide raw-string indexes.
    Candidate source IDs identify joins only and never enter a business hash.
    Execute all batches on one connection; #tables disappear with that session.
    """
    entity = dataset.entity
    if entity not in ("helbredsforloeb", "sygehusophold"):
        raise ValueError("Unsupported LPR3 relational identity")
    stay = entity == "sygehusophold"
    member = "kontakt" if stay else "forloeb"
    member_key = f"{member}_uuid"
    members = f"[ext].[{identity_table(member)}]"
    fact = "lpr3_fctsygehusophold" if stay else "lpr3_fcthelbredsforloeb"
    bridge = "lpr3_bridge_unused" if stay else "lpr3_fctbridgeforloebsrelation"
    sk = "DW_SK_Sygehusophold" if stay else "DW_SK_HelbredsForloeb"
    first = "DW_SK_Kontakt_Foerste" if stay else "DW_SK_Forloeb_Foerste"
    statements = []
    if not stay:
        statements += [
            f"""SELECT CONVERT(BIGINT,DW_SK_Helbredsforloeb) entity,
CONVERT(BIGINT,NULLIF(DW_SK_Forloeb_Forrige,N'')) previous_key,
CONVERT(BIGINT,DW_SK_Forloeb) current_key INTO #lpr3_edges FROM [ext].[{bridge}];
CREATE INDEX ix_lpr3_edges ON #lpr3_edges(entity,current_key) INCLUDE(previous_key);
IF EXISTS (SELECT entity FROM #lpr3_edges WHERE previous_key=-1 GROUP BY entity HAVING COUNT_BIG(*)>1)
    THROW 51000,'LPR3 helbredsforloeb has multiple root rows',1;"""
        ]
    root_join = (
        f"LEFT JOIN #lpr3_edges root ON root.entity=CONVERT(BIGINT,f.[{sk}]) AND root.previous_key=-1 "
        f"LEFT JOIN {members} r ON r.source_sk=NULLIF(root.current_key,-1)"
        if not stay
        else ""
    )
    candidate = f"c.[{member_key}]" if stay else f"COALESCE(r.[{member_key}],c.[{member_key}])"
    origin = (
        "CASE WHEN c.kontakt_uuid IS NOT NULL THEN 'first' ELSE 'missing' END"
        if stay
        else "CASE WHEN r.forloeb_uuid IS NOT NULL THEN 'root' WHEN c.forloeb_uuid IS NOT NULL THEN 'first' ELSE 'missing' END"
    )
    statements += [
        f"""SELECT CONVERT(BIGINT,f.[{sk}]) source_sk,
{candidate} candidate_uuid, CONVERT(VARCHAR(32),{origin}) noeglemetode
INTO #lpr3_candidates FROM [ext].[{fact}] f
LEFT JOIN {members} c ON c.source_sk=NULLIF(CONVERT(BIGINT,NULLIF(f.[{first}],N'')),-1)
{root_join};
CREATE UNIQUE CLUSTERED INDEX ix_lpr3_candidates_sk ON #lpr3_candidates(source_sk);
CREATE INDEX ix_lpr3_candidates_uuid ON #lpr3_candidates(candidate_uuid);"""
    ]
    if stay:
        boundary = f"""SELECT DISTINCT u.source_sk,c.kontakt_uuid member_uuid
FROM #lpr3_candidates u JOIN ext.lpr3_fctbridge_sygehusophold b ON CONVERT(BIGINT,b.DW_SK_Sygehusophold)=u.source_sk
JOIN {members} c ON c.source_sk=NULLIF(CONVERT(BIGINT,NULLIF(b.DW_SK_Kontakt,N'')),-1)
WHERE u.candidate_uuid IS NULL AND c.kontakt_uuid IS NOT NULL"""
    else:
        boundary = f"""SELECT DISTINCT u.source_sk,c.forloeb_uuid member_uuid
FROM #lpr3_candidates u JOIN #lpr3_edges b ON b.entity=u.source_sk
JOIN {members} c ON c.source_sk=NULLIF(b.current_key,-1)
LEFT JOIN {members} p ON p.source_sk=NULLIF(b.previous_key,-1)
WHERE u.candidate_uuid IS NULL AND c.forloeb_uuid IS NOT NULL AND p.forloeb_uuid IS NULL
UNION
SELECT u.source_sk,p.forloeb_uuid
FROM #lpr3_candidates u JOIN #lpr3_edges b ON b.entity=u.source_sk
JOIN {members} p ON p.source_sk=NULLIF(b.previous_key,-1)
WHERE u.candidate_uuid IS NULL AND p.forloeb_uuid IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM #lpr3_edges i WHERE i.entity=b.entity AND i.current_key=b.previous_key)"""
    namespace = f"lpr3:{entity}:{'contact-set' if stay else 'boundary-set'}:v1:"
    single_origin, set_origin = (
        ("single_contact", "contact_set") if stay else ("visible_boundary", "boundary_set")
    )
    statements += [
        f"""SELECT source_sk,member_uuid,{uuid_digest('member_uuid')} member_hash
INTO #lpr3_boundary FROM ({boundary}) b;
CREATE UNIQUE CLUSTERED INDEX ix_lpr3_boundary ON #lpr3_boundary(source_sk,member_hash);
CREATE INDEX ix_lpr3_boundary_uuid ON #lpr3_boundary(member_uuid);
SELECT b.source_sk,COUNT_BIG(*) members,
MAX(CONVERT(CHAR(36),b.member_uuid)) single_uuid,
MAX(CASE WHEN EXISTS_UUID.candidate_uuid IS NOT NULL OR duplicates.n>1 THEN 1 ELSE 0 END) shared,
CONVERT(BINARY(32),HASHBYTES('SHA2_256',CONVERT(VARBINARY(MAX),'{namespace}')+
CONVERT(VARBINARY(MAX),STRING_AGG(CONVERT(VARCHAR(MAX),b.member_hash,2),'') WITHIN GROUP (ORDER BY b.member_hash),2))) set_key
INTO #lpr3_boundary_sets
FROM #lpr3_boundary b
LEFT JOIN (SELECT DISTINCT candidate_uuid FROM #lpr3_candidates WHERE candidate_uuid IS NOT NULL) EXISTS_UUID ON EXISTS_UUID.candidate_uuid=b.member_uuid
JOIN (SELECT member_uuid,COUNT_BIG(*) n FROM #lpr3_boundary GROUP BY member_uuid) duplicates ON duplicates.member_uuid=b.member_uuid
GROUP BY b.source_sk;
CREATE UNIQUE CLUSTERED INDEX ix_lpr3_boundary_sets ON #lpr3_boundary_sets(source_sk);"""
    ]
    final_uuid = "COALESCE(c.candidate_uuid,CASE WHEN b.members=1 AND b.shared=0 THEN CONVERT(UNIQUEIDENTIFIER,b.single_uuid) END)"
    unknown = (
        "\nUNION ALL SELECT CONVERT(BIGINT,-1),"
        "CONVERT(BINARY(32),HASHBYTES('SHA2_256','lpr3:helbredsforloeb:sentinel:v1')),"
        "CAST(NULL AS UNIQUEIDENTIFIER),'sentinel' FROM ext.lpr3_dimhelbredsforloeb "
        "WHERE DW_SK_HelbredsForloeb=N'-1'"
        if not stay
        else ""
    )
    statements += [
        f"""SELECT c.source_sk,
CONVERT(BINARY(32),COALESCE({uuid_digest(final_uuid)},b.set_key)) [{entity}_noegle],
{final_uuid} [{entity}_uuid],
CONVERT(VARCHAR(32),CASE WHEN c.candidate_uuid IS NOT NULL THEN c.noeglemetode
WHEN b.members=1 AND b.shared=0 THEN '{single_origin}'
WHEN b.members>0 THEN '{set_origin}' ELSE 'missing' END) noeglemetode
INTO [ext].[{identity_table(entity)}]
FROM #lpr3_candidates c LEFT JOIN #lpr3_boundary_sets b ON b.source_sk=c.source_sk{unknown};
DROP TABLE #lpr3_boundary_sets;
DROP TABLE #lpr3_boundary;
DROP TABLE #lpr3_candidates;"""
    ]
    if not stay:
        statements += ["DROP TABLE #lpr3_edges;"]
    return statements


def group_identity_sql(dataset):
    """Complete versioned memberships, with technical fallbacks for unsafe cohorts.

    Only complete groups present in Dim participate in collision detection.
    Groups share an identity only when full membership multiplicity, audits and
    dimension validity/flags/audits also agree. Source SKs never enter its hash.
    """
    stem = {"diagnosegruppe": "Diagnose", "proceduregruppe": "Procedure"}[dataset.entity]
    group_sk = f"DW_SK_{stem}gruppe"
    code_sk = f"DW_SK_{stem}"
    parent_sk = f"DW_SK_{stem}_Parent"
    codes = ARCHIVES_BY_TABLE[f"lpr3_{stem.lower()}"]
    code_hash = framed_hash("lpr3:group-code-version:v2", [f"[{c}]" for c in codes.merge_keys])
    member_hash = framed_hash(
        "lpr3:group-member:v2", ["c.code_key", "p.code_key", "CONVERT(INT,b.AntalRaekker)"]
    )
    member_payload = framed_hash(
        "lpr3:group-member-payload:v2",
        [member_hash, "CONVERT(INT,b.DW_ID_Audit_Insert)", "CONVERT(INT,b.DW_ID_Audit_Update)"],
    )
    dimension_payload = framed_hash(
        "lpr3:group-payload:v2",
        ["p.member_payload"]
        + [
            typed_source(c, "d")
            for c in dimension_source(dataset).columns
            if not c.name.startswith(("DW_SK_", "DW_EK_"))
        ],
    )
    fallback = framed_hash(
        f"lpr3:{dataset.entity}:unresolved-source-id:v1",
        [f"CONVERT(BIGINT,d.[DW_EK_{stem}gruppe])"],
    )
    collision = framed_hash(f"lpr3:{dataset.entity}:collision-source-id:v1", ["source_ek"])
    return [
        f"""SELECT source_sk,{code_hash} code_key
INTO #lpr3_codes FROM ({simple_identity_select(codes)}) codes WHERE source_sk<>-1;
CREATE UNIQUE CLUSTERED INDEX ix_lpr3_codes ON #lpr3_codes(source_sk);
SELECT CONVERT(BIGINT,b.[{group_sk}]) group_sk,
CASE WHEN c.source_sk IS NULL OR (b.[{parent_sk}]<>N'-1' AND p.source_sk IS NULL) THEN 1 ELSE 0 END unresolved,
{member_hash} member_key,{member_payload} member_payload_key
INTO #lpr3_group_members FROM [ext].[lpr3_fctbridge{stem.lower()}] b
LEFT JOIN #lpr3_codes c ON c.source_sk=NULLIF(CONVERT(BIGINT,NULLIF(b.[{code_sk}],N'')),-1)
LEFT JOIN #lpr3_codes p ON p.source_sk=NULLIF(CONVERT(BIGINT,NULLIF(b.[{parent_sk}],N'')),-1);
SELECT group_sk,MAX(unresolved) unresolved INTO #lpr3_group_status FROM #lpr3_group_members GROUP BY group_sk;
CREATE UNIQUE CLUSTERED INDEX ix_lpr3_group_status ON #lpr3_group_status(group_sk);
SELECT group_sk,CONVERT(BINARY(32),HASHBYTES('SHA2_256',STRING_AGG(CONVERT(VARCHAR(MAX),member_key,2),'|') WITHIN GROUP (ORDER BY member_key))) set_key
INTO #lpr3_group_sets FROM (SELECT DISTINCT group_sk,member_key FROM #lpr3_group_members WHERE unresolved=0) members GROUP BY group_sk;
CREATE UNIQUE CLUSTERED INDEX ix_lpr3_group_sets ON #lpr3_group_sets(group_sk);
SELECT group_sk,HASHBYTES('SHA2_256',STRING_AGG(CONVERT(VARCHAR(MAX),member_payload_key,2),'|') WITHIN GROUP (ORDER BY member_payload_key)) member_payload
INTO #lpr3_group_payloads FROM #lpr3_group_members WHERE unresolved=0 GROUP BY group_sk;
CREATE UNIQUE CLUSTERED INDEX ix_lpr3_group_payloads ON #lpr3_group_payloads(group_sk);
SELECT CONVERT(BIGINT,d.[{group_sk}]) source_sk,
CONVERT(BIGINT,d.[DW_EK_{stem}gruppe]) source_ek,
CASE WHEN s.unresolved=0 THEN g.set_key END complete_key,
{fallback} unresolved_key,{dimension_payload} payload_signature
INTO #lpr3_group_candidates FROM [ext].[lpr3_dim{dataset.entity}] d
LEFT JOIN #lpr3_group_status s ON s.group_sk=CONVERT(BIGINT,d.[{group_sk}])
LEFT JOIN #lpr3_group_sets g ON g.group_sk=s.group_sk
LEFT JOIN #lpr3_group_payloads p ON p.group_sk=s.group_sk;
SELECT identities.*,
MIN(source_sk) OVER (PARTITION BY [{dataset.entity}_noegle]) canonical_source_sk,
COUNT_BIG(*) OVER (PARTITION BY [{dataset.entity}_noegle]) antal_kilderaekker
INTO [ext].[{identity_table(dataset.entity)}]
FROM (SELECT source_sk,source_ek,
CASE WHEN complete_key IS NULL THEN unresolved_key WHEN conflicting=1 THEN {collision} ELSE complete_key END [{dataset.entity}_noegle],
CONVERT(VARCHAR(32),CASE WHEN complete_key IS NULL THEN 'technical_unresolved' WHEN conflicting=1 THEN 'technical_collision' ELSE 'code_set' END) noeglemetode
FROM (SELECT *,CASE WHEN MIN(payload_signature) OVER (PARTITION BY complete_key)<>MAX(payload_signature) OVER (PARTITION BY complete_key) THEN 1 ELSE 0 END conflicting
FROM #lpr3_group_candidates) candidates) identities;
DROP TABLE #lpr3_group_candidates;
DROP TABLE #lpr3_group_payloads;
DROP TABLE #lpr3_group_sets;
DROP TABLE #lpr3_group_status;
DROP TABLE #lpr3_group_members;
DROP TABLE #lpr3_codes;"""
    ]


def result_identity_sql(dataset):
    key = framed_hash(
        "lpr3:resultat:v1", ["r.resultatindberetning_uuid", "CONVERT(BIGINT,f.Resultat_Nr)"]
    )
    return f"""SELECT CONVERT(BIGINT,f.DW_SK_Resultat) source_sk,
CASE WHEN r.resultatindberetning_uuid IS NOT NULL AND NULLIF(f.Resultat_Nr,N'') IS NOT NULL THEN {key} END resultat_noegle
FROM ext.lpr3_fctresultat f LEFT JOIN ext.lpr3_resultatindberetning_identitet r
ON r.source_sk=NULLIF(CONVERT(BIGINT,NULLIF(f.DW_SK_Resultatindberetning,N'')),-1)"""

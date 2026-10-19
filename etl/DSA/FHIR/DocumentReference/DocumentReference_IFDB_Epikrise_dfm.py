# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR DocumentReference asset for IFDB Epikrise, using DFM (Data Flow Management).

Single-asset pattern (mirrors ``Encounter_IFDB_dfm`` / ``EpisodeOfCare_IFDB_dfm``):
fetch from ``arc.ifdb_epikrise`` in batches, transform to FHIR DocumentReference,
and post to the FHIR server. Recipients are joined per-batch from
``arc.ifdb_epikrise_modtager`` and carried as an extension (Mønster A).

DocumentReference-only (no Composition, §7 #2). The epikrise text/diagnoses are
NOT modelled in FHIR (§7 #1) — every DocumentReference carries a single
metadata-only ``content`` element (R4B requires content 1..*).

``KORREKTION_AF_ID`` is a self-type ``relatesTo`` reference, resolved with the
2-pass upsert + retry pattern (à la ``EpisodeOfCare.previousEpisodeOfCare``).
"""

from typing import Any

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.DocumentReference.document_reference_common import (
    SOURCE_TABLE_EPIKRISE,
    append_subfield,
    build_metadata_only_content,
    build_recipients_extension,
    build_source_table_meta,
    fetch_child_rows_for_batch,
    map_document_reference_status,
)
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from DSA.FHIR.utils.ifdb.fhir_posting import prepare_resources_for_upsert
from DSA.FHIR.utils.ifdb.patient_cpr_fallback import build_patient_cpr_fallback
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.ifdb.transforms import (
    is_true_flag,
    normalize_oracle_id,
    parse_clob_datetime,
    to_fhir_instant,
)
from DSA.FHIR.utils.reference_resolver import (
    NestedReferenceFieldConfig,
    ReferenceFieldConfig,
    ResolveValuesFn,
    batch_resolve_references,
)
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.documentreference import (
    DocumentReference,
    DocumentReferenceContext,
    DocumentReferenceRelatesTo,
)
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.reference import Reference
from utils.fhir.fhir_extensions import (
    EXTENSION_DOCUMENT_REFERENCE_PRIORITY,
    EXTENSION_DOCUMENT_REFERENCE_SIGNATURE,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_EPIKRISE,
    SYSTEM_IFDB_EPIKRISE_MODTAGER,
    SYSTEM_IFDB_EPIKRISETYPE,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

BATCH_SIZE = 10_000

# Recipient child columns pulled from arc.ifdb_epikrise_modtager per batch.
_MODTAGER_COLUMNS = ["OBJEKT_ID", "EPIKRISE_ID", "MODTAGERTYPE", "NAVN", "ADRESSE"]

# Signature roles (Mønster B): IFDB column prefix → role suffix on the signature
# extension base URL. Order is deterministic for stable output.
_SIGNATURE_ROLES: list[tuple[str, str]] = [
    ("UNDERSKRIVER", "underskriver"),  # mode=legal,        100% populated
    ("GODKENDER", "godkender"),  # mode=professional, ~81% (omitted when empty)
    ("REGISTRANT", "registrant"),  # registrant,        100% populated
]

# =============================================================================
# Reference resolution configuration
# =============================================================================

# Top-level references resolved by the standard resolver.
EPIKRISE_REFERENCE_CONFIGS = [
    ReferenceFieldConfig(
        field_name="subject",
        target_resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        is_required=True,
        description="Patient reference from EPIKRISE.PATIENT_ID",
    ),
    ReferenceFieldConfig(
        field_name="author",
        target_resource_type="Organization",
        identifier_system=SYSTEM_SOR,
        is_required=False,
        description="Sender organization from EPIKRISE.AFSENDER_ADMENHED_SORKODE (via SOR)",
    ),
]

# Nested reference: context.encounter[] may hold an Encounter (via the generic
# IFDB object-id system — the repo standard for referencing an Encounter) and/or
# an EpisodeOfCare (R4B allows both Reference types in context.encounter).
EPIKRISE_NESTED_REFERENCE_CONFIGS = [
    NestedReferenceFieldConfig(
        field_path="context.encounter",
        identifier_system_to_type={
            SYSTEM_IFDB_OBJECT_ID: "Encounter",
            SYSTEM_IFDB_EPISODE_OF_CARE_ID: "EpisodeOfCare",
        },
        is_required=False,
        description="DocumentReference.context.encounter → Encounter / EpisodeOfCare",
    ),
]


# =============================================================================
# Transform helpers
# =============================================================================


def _build_type(row: dict[str, Any]) -> CodeableConcept | None:
    """Epikrise type CodeableConcept from local IFDB code system."""
    code = row.get("EPIKRISETYPE_KODE")
    if not code or not str(code).strip():
        return None
    display = row.get("EPIKRISETYPE_KODETEKST")
    return CodeableConcept(
        coding=[
            Coding(
                system=SYSTEM_IFDB_EPIKRISETYPE,
                code=str(code).strip(),
                display=str(display).strip() if display else None,
            )
        ]
    )


def _map_doc_status(row: dict[str, Any], context: dg.AssetExecutionContext) -> str | None:
    """Map EPIKRISESTATUS to R4B docStatus: only Sendt/Sent → final, else NULL + warning."""
    status_text = row.get("EPIKRISESTATUS_KODETEKST")
    normalized = str(status_text).strip() if status_text else ""
    if normalized in ("Sendt", "Sent"):
        return "final"
    if normalized:
        context.log.warning(
            f"Unmapped EPIKRISESTATUS '{normalized}' (OBJEKT_ID={row.get('OBJEKT_ID')}) "
            "→ docStatus left NULL"
        )
    return None


def _build_priority_extension(row: dict[str, Any]) -> Extension | None:
    """PRIORITET → priority extension. Empty string AND NULL are treated as absent (§7 #8)."""
    priority = row.get("PRIORITET")
    if priority is None:
        return None
    value = str(priority).strip()
    if not value:
        return None
    return Extension(url=EXTENSION_DOCUMENT_REFERENCE_PRIORITY, valueString=value)


def _build_signature_role_extensions(row: dict[str, Any]) -> list[Extension]:
    """Signature roles as top-level distinctly-named extensions (Mønster B).

    Persons are carried as display/identifier-style value strings (SYSTEM_IFDB_STAFF
    semantics) — no Reference(Practitioner), which does not exist yet (§7 #4).
    """
    extensions: list[Extension] = []
    for col_prefix, suffix in _SIGNATURE_ROLES:
        subfields: list[Extension] = []
        append_subfield(subfields, "persId", normalize_oracle_id(row.get(f"{col_prefix}_PERSID")))
        append_subfield(subfields, "persName", row.get(f"{col_prefix}_PERSNAVN"))
        append_subfield(subfields, "brugerrolle", row.get(f"{col_prefix}_BRUGERROLLE"))
        append_subfield(subfields, "brugerrolleName", row.get(f"{col_prefix}_BRUGERROLLE_NAME"))
        append_subfield(subfields, "admEnhedKode", row.get(f"{col_prefix}_ADMENHED_KODE"))
        append_subfield(subfields, "admEnhedNavn", row.get(f"{col_prefix}_ADMENHED_NAVN"))
        append_subfield(subfields, "admEnhedSorkode", row.get(f"{col_prefix}_ADMENHED_SORKODE"))
        append_subfield(subfields, "time", row.get(f"{col_prefix}_TIDSPUNKT"), is_date=True)

        # Omit a role entirely when it has no data (e.g. GODKENDER on ~19% of rows).
        if subfields:
            extensions.append(
                Extension(
                    url=f"{EXTENSION_DOCUMENT_REFERENCE_SIGNATURE}#{suffix}",
                    extension=subfields,
                )
            )
    return extensions


def _build_recipient_extension(modtager_rows: list[dict[str, Any]]) -> Extension | None:
    """Epikrise recipients → recipients extension (Mønster A)."""
    recipient_subfields: list[list[Extension]] = []
    for row in modtager_rows:
        subfields: list[Extension] = []
        # recipientKey — stable per-recipient identity for the SOF child PK
        # (mirrors CarePlan activity-source-identifier). EPIKRISE_MODTAGER.OBJEKT_ID.
        objekt_id = row.get("OBJEKT_ID")
        if objekt_id and str(objekt_id).strip():
            subfields.append(
                Extension(
                    url="recipientKey",
                    valueIdentifier=Identifier(
                        system=SYSTEM_IFDB_EPIKRISE_MODTAGER,
                        value=normalize_oracle_id(objekt_id),
                    ),
                )
            )
        append_subfield(subfields, "type", row.get("MODTAGERTYPE"))
        append_subfield(subfields, "name", row.get("NAVN"))
        append_subfield(subfields, "address", row.get("ADRESSE"))
        recipient_subfields.append(subfields)
    return build_recipients_extension(recipient_subfields)


def _transform_epikrise_to_fhir(
    rows: list[dict[str, Any]],
    modtagere_by_epikrise: dict[str, list[dict[str, Any]]],
    context: dg.AssetExecutionContext,
) -> list[DocumentReference]:
    """Transform arc.ifdb_epikrise rows into FHIR DocumentReference resources."""
    documents: list[DocumentReference] = []

    for row in rows:
        objekt_id = row.get("OBJEKT_ID")
        deleted_value = int(is_true_flag(row.get("SLETTET")))

        identifier = Identifier(
            system=SYSTEM_IFDB_EPIKRISE,
            value=normalize_oracle_id(objekt_id),
        )

        subject = Reference(
            identifier=Identifier(
                system=SYSTEM_IFDB_PATIENT_ID,
                value=normalize_oracle_id(row.get("PATIENT_ID")),
            )
        )

        # author → sender organization via SOR (resolved later; optional)
        author = None
        sender_sorkode = row.get("AFSENDER_ADMENHED_SORKODE")
        if sender_sorkode and str(sender_sorkode).strip():
            author = [
                Reference(
                    identifier=Identifier(system=SYSTEM_SOR, value=str(sender_sorkode).strip())
                )
            ]

        # context.encounter[] — Encounter (object-id system) and/or EpisodeOfCare
        encounter_refs: list[Reference] = []
        encounter_id = row.get("ENCOUNTER_ID")
        if encounter_id and str(encounter_id).strip():
            encounter_refs.append(
                Reference(
                    identifier=Identifier(
                        system=SYSTEM_IFDB_OBJECT_ID,
                        value=normalize_oracle_id(encounter_id),
                    )
                )
            )
        eoc_id = row.get("EPISODE_OF_CARE_ID")
        if eoc_id and str(eoc_id).strip():
            encounter_refs.append(
                Reference(
                    identifier=Identifier(
                        system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                        value=normalize_oracle_id(eoc_id),
                    )
                )
            )
        document_context = (
            DocumentReferenceContext(encounter=encounter_refs) if encounter_refs else None
        )

        # relatesTo (self-type) — resolved post-upsert (2-pass)
        relates_to = None
        korrektion_af_id = row.get("KORREKTION_AF_ID")
        if korrektion_af_id and str(korrektion_af_id).strip():
            relates_to = [
                DocumentReferenceRelatesTo(
                    code="replaces",
                    target=Reference(
                        identifier=Identifier(
                            system=SYSTEM_IFDB_EPIKRISE,
                            value=normalize_oracle_id(korrektion_af_id),
                        )
                    ),
                )
            ]

        # date (R4B instant — requires timezone)
        date_raw = row.get("FOERSTE_AFSENDELSESTIDSPUNKT")
        date_value = (
            to_fhir_instant(parse_clob_datetime(date_raw) or date_raw) if date_raw else None
        )

        # Extensions (all top-level): signature roles + recipients + priority + isDeleted
        extensions: list[Extension] = []
        extensions.extend(_build_signature_role_extensions(row))
        recipients_ext = _build_recipient_extension(modtagere_by_epikrise.get(str(objekt_id), []))
        if recipients_ext:
            extensions.append(recipients_ext)
        priority_ext = _build_priority_extension(row)
        if priority_ext:
            extensions.append(priority_ext)
        extensions.append(build_is_deleted_extension(deleted_value))

        documents.append(
            DocumentReference(
                meta=build_source_table_meta(SOURCE_TABLE_EPIKRISE),
                identifier=[identifier],
                status=map_document_reference_status(deleted_value),
                docStatus=_map_doc_status(row, context),
                type=_build_type(row),
                subject=subject,
                date=date_value,
                author=author,
                context=document_context,
                relatesTo=relates_to,
                content=build_metadata_only_content(),
                extension=extensions,
            )
        )

    return documents


def _resolve_relatesto_self_references(
    documents: list[DocumentReference],
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext,
    max_attempts: int = 3,
    initial_delay: float = 0.5,
) -> None:
    """Resolve relatesTo[].target self-references after pass-1 upsert (retry w/ backoff).

    Drops a relatesTo entry whose target cannot be resolved (is_required=False
    semantics) — KORREKTION_AF_ID may point at an epikrise outside the loaded
    window.
    """
    import time

    target_identifiers: dict[str, str] = {}
    for doc in documents:
        for rel in doc.relatesTo or []:
            tgt = getattr(rel, "target", None)
            if tgt is not None and getattr(tgt, "identifier", None):
                target_identifiers[str(tgt.identifier.value)] = ""

    if not target_identifiers:
        return

    context.log.info(
        f"Resolving {len(target_identifiers)} unique relatesTo (KORREKTION_AF_ID) self-references"
    )

    resolved_ids: dict[str, str] = {}
    delay = initial_delay
    for attempt in range(1, max_attempts + 1):
        unresolved = {k: "" for k in target_identifiers if k not in resolved_ids}
        if not unresolved:
            break
        resolved_ids.update(
            batch_resolve_references(
                identifiers=unresolved,
                resource_type="DocumentReference",
                identifier_system=SYSTEM_IFDB_EPIKRISE,
                fhir_database=fhir_database,
            )
        )
        if all(k in resolved_ids for k in target_identifiers) or attempt == max_attempts:
            break
        time.sleep(delay)
        delay *= 2

    missing: set[str] = set()
    for doc in documents:
        if not doc.relatesTo:
            continue
        kept: list[DocumentReferenceRelatesTo] = []
        for rel in doc.relatesTo:
            tgt = getattr(rel, "target", None)
            if tgt is not None and getattr(tgt, "identifier", None):
                fhir_id = resolved_ids.get(str(tgt.identifier.value))
                if fhir_id:
                    tgt.id = fhir_id
                    tgt.type = "DocumentReference"
                    tgt.identifier = None
                    kept.append(rel)
                else:
                    missing.add(str(tgt.identifier.value))
            else:
                kept.append(rel)
        doc.relatesTo = kept or None

    if missing:
        context.log.warning(
            f"relatesTo target unresolved for {len(missing)} KORREKTION_AF_ID(s) "
            f"— entries dropped. Examples: {list(missing)[:10]}"
        )


def _post_epikrise_to_fhir(
    context: dg.AssetExecutionContext,
    documents: list[DocumentReference],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    reference_fallback_fn: ResolveValuesFn | None = None,
) -> int:
    """Resolve references, upsert (2-pass for relatesTo), return posted count.

    ``reference_fallback_fn`` is an optional patient-id → CPR fallback for
    subject resolution: when a PATIENT_ID is unresolved (e.g. a soft-deleted
    CPR-duplicate whose active twin is posted under the same CPR), it bridges
    via CPR (see ``build_patient_cpr_fallback``).
    """
    logger = dg.get_dagster_logger()
    if not documents:
        return 0

    documents = prepare_resources_for_upsert(
        resources=documents,
        fhir_database=fhir_database,
        resource_type="DocumentReference",
        preferred_id_systems=[SYSTEM_IFDB_EPIKRISE],
        reference_configs=EPIKRISE_REFERENCE_CONFIGS,
        nested_reference_configs=EPIKRISE_NESTED_REFERENCE_CONFIGS,
        reference_fallback_fn=reference_fallback_fn,
        force_reload=True,
        logger=logger,
    )

    # Pass 1: upsert all (generates FHIR ids). Strict — abort before checkpoint
    # advancement if any resource fails to persist.
    successes, failures, failed_ids = fhir_database.batch_upsert_resources(
        resources=documents,
        resource_type="DocumentReference",
    )
    if failures > 0:
        raise dg.Failure(
            description=(
                f"Failed to post {failures} of {len(documents)} DocumentReference "
                f"(epikrise) resources (pass 1) — checkpoint NOT advanced. "
                f"Failed IDs: {failed_ids[:10]}"
            )
        )

    # Pass 2: resolve relatesTo self-references and re-upsert the changed ones.
    _resolve_relatesto_self_references(documents, fhir_database, context)
    docs_with_refs = [
        doc
        for doc in documents
        if doc.relatesTo
        and any(getattr(getattr(rel, "target", None), "id", None) for rel in doc.relatesTo)
    ]
    if docs_with_refs:
        logger.info(
            f"Updating {len(docs_with_refs)} DocumentReference records with resolved relatesTo"
        )
        fhir_database.batch_upsert_resources(
            resources=docs_with_refs,
            resource_type="DocumentReference",
        )

    logger.info(
        f"Summary: {extracted_count} ext -> {len(documents)} trans -> {successes} posted "
        f"({failures} failed)"
    )
    return successes


# ---------------------------------------------------------------------------
# Single DFM asset — fetch batches, transform, post
# ---------------------------------------------------------------------------


@dg.asset(
    name="fhir_server_documentreference_epikrise_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=[
        "arc_epikrise_ifdb",
        "arc_epikrise_modtager_ifdb",
        "fhir_server_patient_dfm",
        "fhir_server_encounter_dfm",
        "fhir_server_eoc_dfm",
    ],
    tags={
        TAG_CHUNK_SIZE: "400",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Fetch epikriser from arc.ifdb_epikrise in batches, transform to FHIR "
        "DocumentReference (recipients joined from arc.ifdb_epikrise_modtager), "
        "and post to the FHIR server. Checkpoint owner for fhir_server.DocumentReference (epikrise)."
    ),
)
def fhir_server_documentreference_epikrise_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch → transform → post for Epikrise DocumentReference (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "DocumentReference_Epikrise",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    # Patient-id → CPR fallback for subject resolution: resolves
    # soft-deleted/CPR-duplicate PATIENT_IDs to the active patient under the same
    # CPR. Built once and reused for every batch (mirrors Encounter/EoC/CarePlan).
    patient_cpr_fallback = build_patient_cpr_fallback(mssql_target_staging, fhir_database)

    try:
        # include_deleted=True so soft-deleted epikriser are tomb-stoned
        # (status=entered-in-error) rather than left active in Aidbox forever.
        query = build_fhir_archive_query(
            "arc.ifdb_epikrise", run_ctx.checkpoint, config, include_deleted=True
        )
        context.log.info(f"Querying arc.ifdb_epikrise (mode={config.mode})")

        total_fetched = 0
        total_posted = 0
        batch_num = 0

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]

                while True:
                    raw_rows = cursor.fetchmany(BATCH_SIZE)
                    if not raw_rows:
                        break

                    batch_num += 1
                    batch = [dict(zip(columns, row, strict=False)) for row in raw_rows]
                    total_fetched += len(batch)
                    context.log.info(
                        f"Batch {batch_num}: processing {len(batch)} epikriser "
                        f"(total so far: {total_fetched})"
                    )

                    epikrise_ids = [str(r["OBJEKT_ID"]) for r in batch if r.get("OBJEKT_ID")]
                    modtagere_by_epikrise = fetch_child_rows_for_batch(
                        mssql_target_staging,
                        "arc.ifdb_epikrise_modtager",
                        "EPIKRISE_ID",
                        epikrise_ids,
                        _MODTAGER_COLUMNS,
                    )

                    documents = _transform_epikrise_to_fhir(batch, modtagere_by_epikrise, context)
                    if documents:
                        total_posted += _post_epikrise_to_fhir(
                            context,
                            documents,
                            fhir_database,
                            len(batch),
                            reference_fallback_fn=patient_cpr_fallback,
                        )
            finally:
                cursor.close()

        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted in {batch_num} batches"
        )

        new_checkpoint = advance_fhir_checkpoint(
            dfm, run_ctx, "DocumentReference_Epikrise", config, total_fetched
        )
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=new_checkpoint, rows_affected=total_posted
        )

        return dg.MaterializeResult(
            metadata={
                "posted_count": total_posted,
                "fetched_count": total_fetched,
                "batch_count": batch_num,
                "mode": config.mode,
            }
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

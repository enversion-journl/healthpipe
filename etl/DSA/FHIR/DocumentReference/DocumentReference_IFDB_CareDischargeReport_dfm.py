# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR DocumentReference asset for IFDB Udskrivningsrapport (CARE_DISCHARGE_REPORT),
using DFM (Data Flow Management).

Single-asset pattern (mirrors ``Encounter_IFDB_dfm``): fetch from
``arc.ifdb_care_discharge_report`` in batches, transform to FHIR
DocumentReference, and post to the FHIR server. Recipients are joined per-batch
from ``arc.ifdb_care_discharge_report_receiver`` and carried as an extension
(Mønster A), including the per-recipient transmission status.

DocumentReference-only (no Composition); a single metadata-only ``content``
element satisfies R4B content 1..*. The main report row has no document status,
so ``docStatus`` is left NULL (receiver STATE_KODE is transmission status,
carried in the recipient extension — not docStatus).
"""

from typing import Any

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.DocumentReference.document_reference_common import (
    SOURCE_TABLE_CARE_DISCHARGE_REPORT,
    advance_fhir_checkpoint,
    append_subfield,
    build_metadata_only_content,
    build_recipients_extension,
    build_source_table_meta,
    fetch_child_rows_for_batch,
    map_document_reference_status,
)
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.ifdb.fhir_posting import post_resources_direct, prepare_resources_for_upsert
from DSA.FHIR.utils.ifdb.patient_cpr_fallback import build_patient_cpr_fallback
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.ifdb.transforms import (
    is_true_flag,
    normalize_oracle_id,
    parse_clob_datetime,
    to_fhir_instant,
)
from DSA.FHIR.utils.reference_resolver import ReferenceFieldConfig, ResolveValuesFn
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.documentreference import DocumentReference
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.reference import Reference
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_CARE_DISCHARGE_REPORT,
    SYSTEM_IFDB_CARE_DISCHARGE_REPORT_RECEIVER,
    SYSTEM_IFDB_DOCUMENT_TYPE,
    SYSTEM_IFDB_NOTIFICATION_IDENTIFIER,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

BATCH_SIZE = 10_000

_RECEIVER_COLUMNS = [
    "OBJECT_ID",
    "RECEIVER_AFDELING_KODE",
    "RECEIVER_AFDELING_SORKODE",
    "CCRECEIVER",
    "STATE_KODE",
    "STATE_KODETEXT",
    "TRANSMISSION_TIME",
]

# Fixed local document type for udskrivningsrapport (LOINC unavailable, §7 #10).
_DOCUMENT_TYPE = CodeableConcept(
    coding=[
        Coding(
            system=SYSTEM_IFDB_DOCUMENT_TYPE,
            code="care_discharge_report",
            display="Udskrivningsrapport",
        )
    ]
)

CARE_DISCHARGE_REPORT_REFERENCE_CONFIGS = [
    ReferenceFieldConfig(
        field_name="subject",
        target_resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        is_required=True,
        description="Patient reference from CARE_DISCHARGE_REPORT.PATIENT_ID",
    ),
    ReferenceFieldConfig(
        field_name="author",
        target_resource_type="Organization",
        identifier_system=SYSTEM_SOR,
        is_required=False,
        description="Sender organization from CARE_DISCHARGE_REPORT.SENDER_AFDELING_SORKODE (via SOR)",
    ),
]


def _build_recipient_extension(receiver_rows: list[dict[str, Any]]) -> Extension | None:
    """Udskrivningsrapport recipients → recipients extension (Mønster A).

    Recipient organization is kept as an identifier-based Reference (system=SOR);
    it is extension metadata, not resolved to a FHIR Organization id.
    """
    recipient_subfields: list[list[Extension]] = []
    for row in receiver_rows:
        subfields: list[Extension] = []

        # recipientKey — stable per-recipient identity for the SOF child PK
        # (mirrors CarePlan activity-source-identifier). The receiver has no own
        # id; its identity is the arc merge-key tail RECEIVER_AFDELING_KODE|CCRECEIVER.
        afdeling_kode = row.get("RECEIVER_AFDELING_KODE")
        if afdeling_kode and str(afdeling_kode).strip():
            recipient_key = (
                f"{str(afdeling_kode).strip()}|{int(is_true_flag(row.get('CCRECEIVER')))}"
            )
            subfields.append(
                Extension(
                    url="recipientKey",
                    valueIdentifier=Identifier(
                        system=SYSTEM_IFDB_CARE_DISCHARGE_REPORT_RECEIVER,
                        value=recipient_key,
                    ),
                )
            )

        sorkode = row.get("RECEIVER_AFDELING_SORKODE")
        if sorkode and str(sorkode).strip():
            subfields.append(
                Extension(
                    url="organization",
                    valueReference=Reference(
                        identifier=Identifier(system=SYSTEM_SOR, value=str(sorkode).strip())
                    ),
                )
            )

        cc = row.get("CCRECEIVER")
        if cc is not None:
            subfields.append(Extension(url="ccFlag", valueBoolean=bool(is_true_flag(cc))))

        append_subfield(subfields, "statusCode", row.get("STATE_KODE"))
        append_subfield(subfields, "statusText", row.get("STATE_KODETEXT"))
        append_subfield(subfields, "timestamp", row.get("TRANSMISSION_TIME"), is_date=True)

        recipient_subfields.append(subfields)

    return build_recipients_extension(recipient_subfields)


def _transform_care_discharge_reports_to_fhir(
    rows: list[dict[str, Any]],
    receivers_by_report: dict[str, list[dict[str, Any]]],
) -> list[DocumentReference]:
    """Transform arc.ifdb_care_discharge_report rows into FHIR DocumentReference resources."""
    documents: list[DocumentReference] = []

    for row in rows:
        object_id = row.get("OBJECT_ID")
        deleted_value = int(is_true_flag(row.get("DELETED")))

        identifiers = [
            Identifier(
                system=SYSTEM_IFDB_CARE_DISCHARGE_REPORT,
                value=normalize_oracle_id(object_id),
            )
        ]
        notification_id = row.get("NOTIFICATION_IDENTIFIER")
        if notification_id and str(notification_id).strip():
            identifiers.append(
                Identifier(
                    system=SYSTEM_IFDB_NOTIFICATION_IDENTIFIER,
                    value=str(notification_id).strip(),
                )
            )

        subject = Reference(
            identifier=Identifier(
                system=SYSTEM_IFDB_PATIENT_ID,
                value=normalize_oracle_id(row.get("PATIENT_ID")),
            )
        )

        author = None
        sender_sorkode = row.get("SENDER_AFDELING_SORKODE")
        if sender_sorkode and str(sender_sorkode).strip():
            author = [
                Reference(
                    identifier=Identifier(system=SYSTEM_SOR, value=str(sender_sorkode).strip())
                )
            ]

        date_raw = row.get("MODIFICATION_TIME")
        date_value = (
            to_fhir_instant(parse_clob_datetime(date_raw) or date_raw) if date_raw else None
        )

        extensions: list[Extension] = []
        recipients_ext = _build_recipient_extension(receivers_by_report.get(str(object_id), []))
        if recipients_ext:
            extensions.append(recipients_ext)
        extensions.append(build_is_deleted_extension(deleted_value))

        documents.append(
            DocumentReference(
                meta=build_source_table_meta(SOURCE_TABLE_CARE_DISCHARGE_REPORT),
                identifier=identifiers,
                status=map_document_reference_status(deleted_value),
                type=_DOCUMENT_TYPE,
                subject=subject,
                date=date_value,
                author=author,
                content=build_metadata_only_content(),
                extension=extensions,
            )
        )

    return documents


def _post_care_discharge_reports_to_fhir(
    context: dg.AssetExecutionContext,
    documents: list[DocumentReference],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    reference_fallback_fn: ResolveValuesFn | None = None,
) -> dg.MaterializeResult:
    """Resolve references and upsert (no self-references → single pass).

    ``reference_fallback_fn`` is an optional patient-id → CPR fallback for
    subject resolution: when a PATIENT_ID is unresolved (e.g. a soft-deleted
    CPR-duplicate whose active twin is posted under the same CPR), it bridges
    via CPR (see ``build_patient_cpr_fallback``).
    """
    logger = dg.get_dagster_logger()

    documents = prepare_resources_for_upsert(
        resources=documents,
        fhir_database=fhir_database,
        resource_type="DocumentReference",
        preferred_id_systems=[SYSTEM_IFDB_CARE_DISCHARGE_REPORT],
        reference_configs=CARE_DISCHARGE_REPORT_REFERENCE_CONFIGS,
        reference_fallback_fn=reference_fallback_fn,
        force_reload=True,
        logger=logger,
    )

    return post_resources_direct(
        context=context,
        resources=documents,
        fhir_database=fhir_database,
        resource_type="DocumentReference",
        source_name="ifdb_oracle",
        extracted_count=extracted_count,
    )


# ---------------------------------------------------------------------------
# Single DFM asset — fetch batches, transform, post
# ---------------------------------------------------------------------------


@dg.asset(
    name="fhir_server_documentreference_care_discharge_report_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=[
        "arc_care_discharge_report_ifdb",
        "arc_care_discharge_report_receiver_ifdb",
        "fhir_server_patient_dfm",
    ],
    tags={
        TAG_CHUNK_SIZE: "400",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Fetch udskrivningsrapporter from arc.ifdb_care_discharge_report in batches, "
        "transform to FHIR DocumentReference (recipients joined from "
        "arc.ifdb_care_discharge_report_receiver), and post to the FHIR server. "
        "Checkpoint owner for fhir_server.DocumentReference (udskrivningsrapport)."
    ),
)
def fhir_server_documentreference_care_discharge_report_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch → transform → post for Udskrivningsrapport DocumentReference (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "DocumentReference_CareDischargeReport",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    # Patient-id → CPR fallback for subject resolution: resolves
    # soft-deleted/CPR-duplicate PATIENT_IDs to the active patient under the same
    # CPR. Built once and reused for every batch (mirrors Encounter/EoC/CarePlan).
    patient_cpr_fallback = build_patient_cpr_fallback(mssql_target_staging, fhir_database)

    try:
        query = build_fhir_archive_query(
            "arc.ifdb_care_discharge_report", run_ctx.checkpoint, config, include_deleted=True
        )
        context.log.info(f"Querying arc.ifdb_care_discharge_report (mode={config.mode})")

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
                        f"Batch {batch_num}: processing {len(batch)} udskrivningsrapporter "
                        f"(total so far: {total_fetched})"
                    )

                    report_ids = [str(r["OBJECT_ID"]) for r in batch if r.get("OBJECT_ID")]
                    receivers_by_report = fetch_child_rows_for_batch(
                        mssql_target_staging,
                        "arc.ifdb_care_discharge_report_receiver",
                        "OBJECT_ID",
                        report_ids,
                        _RECEIVER_COLUMNS,
                    )

                    documents = _transform_care_discharge_reports_to_fhir(
                        batch, receivers_by_report
                    )
                    if documents:
                        _post_care_discharge_reports_to_fhir(
                            context,
                            documents,
                            fhir_database,
                            len(batch),
                            reference_fallback_fn=patient_cpr_fallback,
                        )
                        total_posted += len(documents)
            finally:
                cursor.close()

        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted in {batch_num} batches"
        )

        new_checkpoint = advance_fhir_checkpoint(
            dfm,
            mssql_target_staging,
            "DocumentReference_CareDischargeReport",
            config,
            total_fetched,
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

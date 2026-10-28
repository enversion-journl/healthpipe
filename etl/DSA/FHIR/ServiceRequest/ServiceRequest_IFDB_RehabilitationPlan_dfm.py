# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR ServiceRequest asset for IFDB REHABILITATION_PLAN,
using DFM (Data Flow Management).

Single-asset pattern (mirrors ``DocumentReference_IFDB_CareDischargeReport_dfm``):
fetch from ``arc.ifdb_rehabilitation_plan`` in batches, transform to FHIR
ServiceRequest, and post to the FHIR server. Receivers (modtagere) are joined
per-batch from ``arc.ifdb_rehabilitation_plan_receiver`` and carried as a
recipients extension (Mønster A), including per-recipient transmission status.

A genoptræningsplan is a binding, statutory request for rehabilitation sent from
hospital to municipality/provider at discharge (sundhedsloven §84/§140), so it is
modelled as a ServiceRequest with ``intent=order``. It is the
first ServiceRequest source on the DFM path (the HENVISNING/RIS_YDELSE
sources remain partitioned); coexistence is safe because posting isolates on the
per-source identifier system and the SOF flatten-back merges by the Aidbox id.
"""

from typing import Any

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource

# Generic (resource-agnostic) DFM helpers shared with the DocumentReference
# transforms — a scalar sub-field builder, a per-batch child-row fetch, and the
# fhir_server checkpoint advance. Reused rather than duplicated.
from DSA.FHIR.DocumentReference.document_reference_common import (
    append_subfield,
    fetch_child_rows_for_batch,
)
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from DSA.FHIR.utils.ifdb.fhir_posting import post_resources_direct, prepare_resources_for_upsert
from DSA.FHIR.utils.ifdb.patient_cpr_fallback import build_patient_cpr_fallback
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.ifdb.transforms import (
    is_true_flag,
    normalize_oracle_id,
    parse_clob_datetime_safe,
)
from DSA.FHIR.utils.reference_resolver import ReferenceFieldConfig, ResolveValuesFn
from DSI.oracle.ifdb_extract import REHABILITATION_PLAN_RECEIVER_COLUMNS
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.reference import Reference
from fhir.resources.R4B.servicerequest import ServiceRequest
from utils.fhir.fhir_extensions import (
    EXTENSION_REHABILITATION_PLAN_RECEIVERS,
    EXTENSION_REHABILITATION_PLAN_SOURCE_META,
    EXTENSION_SOURCE_TABLE,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_NOTIFICATION_IDENTIFIER,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_REHABILITATION_PLAN,
    SYSTEM_IFDB_REHABILITATION_PLAN_RECEIVER,
    SYSTEM_IFDB_REHABILITATION_PLAN_TYPE,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

BATCH_SIZE = 10_000

# source_table discriminator value projected to [fhir].[ServiceRequest].source_table
# via the source-meta extension's sourceTable sub-field. UPPERCASE source-table name,
# matching the existing HENVISNING/RIS_YDELSE values — analyst source_table IN (…)
# filters are case-sensitive under the DWH collation.
SOURCE_TABLE_REHABILITATION_PLAN = "REHABILITATION_PLAN"

# Fixed intent (R4B 1..1, no source column): a rehabilitation plan is a binding, statutory order
# for rehabilitation; same value as the RIS_YDELSE source.
_INTENT = "order"

# Wrapper URL inside the receivers parent extension (Mønster A pattern; url="receiver").
_RECEIVER_WRAPPER_URL = "receiver"

REHABILITATION_PLAN_REFERENCE_CONFIGS = [
    ReferenceFieldConfig(
        field_name="subject",
        target_resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        is_required=True,
        description="Patient reference from REHABILITATION_PLAN.PATIENT_ID",
    ),
    ReferenceFieldConfig(
        field_name="requester",
        target_resource_type="Organization",
        identifier_system=SYSTEM_SOR,
        is_required=False,
        description="Sender organization from REHABILITATION_PLAN.SENDER_AFDELING_SORKODE (via SOR)",
    ),
]


def _map_status(deleted_value: int) -> str:
    """DELETED soft-delete flag → R4B ServiceRequest status.

    Active rows are ``active``; a soft-deleted source row is tomb-stoned as
    ``entered-in-error`` (repo-wide convention, same as HENVISNING/RIS).
    """
    return "entered-in-error" if deleted_value != 0 else "active"


def _build_code(row: dict[str, Any]) -> CodeableConcept | None:
    """TYPE_KODETEKST → code.coding.

    There is no separate code column in the source — the code IS the kodetekst,
    so the raw text is used verbatim as both ``code`` and ``display`` (no invented
    codes), plus ``code.text``.
    """
    raw = row.get("TYPE_KODETEKST")
    if raw is None or not str(raw).strip():
        return None
    text = str(raw).strip()
    return CodeableConcept(
        coding=[Coding(system=SYSTEM_IFDB_REHABILITATION_PLAN_TYPE, code=text, display=text)],
        text=text,
    )


def _build_source_extensions(row: dict[str, Any]) -> list[Extension]:
    """The sourceTable discriminator plus the lossless source-meta extension.

    ``sourceTable`` is a FIRST-CLASS top-level extension (EXTENSION_SOURCE_TABLE),
    matching HENVISNING/RIS_YDELSE — the SOF view reads it in one anchored hop
    (``extension('…/sourceTable').valueString``) and no longer scans nested
    source-meta sub-fields. The source-meta extension keeps the fields that have no
    native ServiceRequest home: sender department code/kodetekst and plan version.
    """
    parts: list[Extension] = []
    append_subfield(parts, "senderAfdelingKode", row.get("SENDER_AFDELING_KODE"))
    append_subfield(parts, "senderAfdelingKodetekst", row.get("SENDER_AFDELING_KODETEKST"))
    append_subfield(parts, "planVersion", row.get("PLAN_VERSION"))
    return [
        Extension(url=EXTENSION_SOURCE_TABLE, valueString=SOURCE_TABLE_REHABILITATION_PLAN),
        Extension(url=EXTENSION_REHABILITATION_PLAN_SOURCE_META, extension=parts),
    ]


def _text(raw: Any) -> str:
    """The stripped string value, or an empty string for NULL."""
    return "" if raw is None else str(raw).strip()


def _derive_receiver_type(yder_nummer: str, afdeling_kode: str) -> str:
    """Afledt modtagertype: yder if a ydernummer is set, else afdeling if a
    department code is set, else ukendt."""
    if yder_nummer:
        return "yder"
    if afdeling_kode:
        return "afdeling"
    return "ukendt"


def _build_receivers_extension(receiver_rows: list[dict[str, Any]]) -> Extension | None:
    """Genoptræningsplan receivers → receivers extension.

    One same-url ``"receiver"`` wrapper per modtager under a single parent
    extension. The recipient organization is kept identifier-based (system=SOR);
    it is extension metadata, not resolved to a FHIR Organization id (nested
    wrapper — the resolver cannot reach it). Returns None when there are no
    receivers so the caller can omit the extension entirely.
    """
    wrappers: list[Extension] = []
    seen_keys: set[str] = set()
    for row in receiver_rows:
        yder_nummer = _text(row.get("RECEIVER_YDER_NUMMER"))
        afdeling_kode = _text(row.get("RECEIVER_AFDELING_KODE"))

        subfields: list[Extension] = []

        # The arc PK (OBJECT_ID, modtager_noegle) makes receiverKey unique per plan; a duplicate
        # would abort the SOF MERGE with error 8672.
        recipient_key = _text(row.get("modtager_noegle"))
        if recipient_key in seen_keys:
            raise ValueError(
                f"Rehabilitation plan {_text(row.get('OBJECT_ID'))!r} has receiverKey "
                f"{recipient_key!r} more than once"
            )
        seen_keys.add(recipient_key)

        subfields.append(
            Extension(
                url="receiverKey",
                valueIdentifier=Identifier(
                    system=SYSTEM_IFDB_REHABILITATION_PLAN_RECEIVER,
                    value=recipient_key,
                ),
            )
        )

        subfields.append(
            Extension(
                url="receiverType",
                valueString=_derive_receiver_type(yder_nummer, afdeling_kode),
            )
        )

        append_subfield(subfields, "yderNummer", yder_nummer)
        append_subfield(subfields, "yderNavn", row.get("RECEIVER_YDER_NAVN"))
        append_subfield(subfields, "afdelingKode", afdeling_kode)
        append_subfield(subfields, "afdelingKodetekst", row.get("RECEIVER_AFDELING_KODETEKST"))

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

        append_subfield(subfields, "statusCode", row.get("STATE_KODE"))
        append_subfield(subfields, "statusText", row.get("STATE_KODETEXT"))
        append_subfield(subfields, "timestamp", row.get("TRANSMISSION_TIME"), is_date=True)

        wrappers.append(Extension(url=_RECEIVER_WRAPPER_URL, extension=subfields))

    if not wrappers:
        return None
    return Extension(url=EXTENSION_REHABILITATION_PLAN_RECEIVERS, extension=wrappers)


def _transform_rehabilitation_plans_to_fhir(
    rows: list[dict[str, Any]],
    receivers_by_plan: dict[str, list[dict[str, Any]]],
) -> list[ServiceRequest]:
    """Transform arc.ifdb_rehabilitation_plan rows into FHIR ServiceRequest resources."""
    requests: list[ServiceRequest] = []

    for row in rows:
        object_id = row.get("OBJECT_ID")
        deleted_value = int(is_true_flag(row.get("DELETED")))

        identifiers = [
            Identifier(
                system=SYSTEM_IFDB_REHABILITATION_PLAN,
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

        requester = None
        sender_sorkode = row.get("SENDER_AFDELING_SORKODE")
        if sender_sorkode and str(sender_sorkode).strip():
            requester = Reference(
                identifier=Identifier(system=SYSTEM_SOR, value=str(sender_sorkode).strip())
            )

        authored_on = parse_clob_datetime_safe(row.get("MODIFICATION_TIME"))

        extensions: list[Extension] = _build_source_extensions(row)
        receivers_ext = _build_receivers_extension(receivers_by_plan.get(str(object_id), []))
        if receivers_ext:
            extensions.append(receivers_ext)
        extensions.append(build_is_deleted_extension(deleted_value))

        requests.append(
            ServiceRequest(
                identifier=identifiers,
                status=_map_status(deleted_value),
                intent=_INTENT,
                code=_build_code(row),
                subject=subject,
                authoredOn=authored_on,
                requester=requester,
                extension=extensions,
            )
        )

    return requests


def _post_rehabilitation_plans_to_fhir(
    context: dg.AssetExecutionContext,
    requests: list[ServiceRequest],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    reference_fallback_fn: ResolveValuesFn | None = None,
) -> dg.MaterializeResult:
    """Resolve references and upsert (no self-references → single pass).

    Upsert is keyed on the rehabilitation_plan identifier system only, so it never
    matches or overwrites the HENVISNING/RIS_YDELSE ServiceRequests.
    ``reference_fallback_fn`` is the patient-id → CPR fallback for subject resolution.
    """
    logger = dg.get_dagster_logger()

    requests = prepare_resources_for_upsert(
        resources=requests,
        fhir_database=fhir_database,
        resource_type="ServiceRequest",
        preferred_id_systems=[SYSTEM_IFDB_REHABILITATION_PLAN],
        reference_configs=REHABILITATION_PLAN_REFERENCE_CONFIGS,
        reference_fallback_fn=reference_fallback_fn,
        force_reload=True,
        logger=logger,
    )

    return post_resources_direct(
        context=context,
        resources=requests,
        fhir_database=fhir_database,
        resource_type="ServiceRequest",
        source_name="ifdb_oracle",
        extracted_count=extracted_count,
    )


# ---------------------------------------------------------------------------
# Single DFM asset — fetch batches, transform, post
# ---------------------------------------------------------------------------


@dg.asset(
    name="fhir_server_rehabilitation_plan_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=[
        "arc_rehabilitation_plan_ifdb",
        "arc_rehabilitation_plan_receiver_ifdb",
        "fhir_server_patient_dfm",
    ],
    tags={
        TAG_CHUNK_SIZE: "400",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Fetch genoptræningsplaner from arc.ifdb_rehabilitation_plan in batches, "
        "transform to FHIR ServiceRequest (intent=order; receivers joined from "
        "arc.ifdb_rehabilitation_plan_receiver as a Mønster A extension), and post "
        "to the FHIR server. Checkpoint owner for fhir_server.RehabilitationPlan."
    ),
)
def fhir_server_rehabilitation_plan_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch → transform → post for Genoptræningsplan ServiceRequest (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "RehabilitationPlan",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    # Patient-id → CPR fallback for subject resolution (mirrors CDR/Encounter):
    # bridges soft-deleted/CPR-duplicate PATIENT_IDs to the active patient.
    patient_cpr_fallback = build_patient_cpr_fallback(mssql_target_staging, fhir_database)

    try:
        query = build_fhir_archive_query(
            "arc.ifdb_rehabilitation_plan", run_ctx.checkpoint, config, include_deleted=True
        )
        context.log.info(f"Querying arc.ifdb_rehabilitation_plan (mode={config.mode})")

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
                        f"Batch {batch_num}: processing {len(batch)} genoptræningsplaner "
                        f"(total so far: {total_fetched})"
                    )

                    plan_ids = [str(r["OBJECT_ID"]) for r in batch if r.get("OBJECT_ID")]
                    receivers_by_plan = fetch_child_rows_for_batch(
                        mssql_target_staging,
                        "arc.ifdb_rehabilitation_plan_receiver",
                        "OBJECT_ID",
                        plan_ids,
                        list(REHABILITATION_PLAN_RECEIVER_COLUMNS),
                    )

                    requests = _transform_rehabilitation_plans_to_fhir(batch, receivers_by_plan)
                    if requests:
                        _post_rehabilitation_plans_to_fhir(
                            context,
                            requests,
                            fhir_database,
                            len(batch),
                            reference_fallback_fn=patient_cpr_fallback,
                        )
                        total_posted += len(requests)
            finally:
                cursor.close()

        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted in {batch_num} batches"
        )

        new_checkpoint = advance_fhir_checkpoint(
            dfm,
            run_ctx,
            "RehabilitationPlan",
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

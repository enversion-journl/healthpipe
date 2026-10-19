# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""FHIR Aktivitetsresultat/SFI multi-asset (DFM) — arc.* input handle switch.

Reads ``arc.ifdb_aktivitetsresultat``, spools the rows to pickle sidecars and
hands a ``BatchedExtractRows`` view to the existing chunked pipeline in one
call per slice — the same streaming input the legacy monthly/bulk assets fed the
orchestrator via ``streaming_batched_extract_io_manager`` (estimat §6.4). The
orchestrator's own 25k chunk loop, shared caches (Questionnaire store, UDF/
speciale lookups, ResolvedRefLookup) and one-time setup therefore run once per
slice — not once per fetch batch. A plain delta run (no ``lookback_days``, no
``delta_column``) is cut into slices of at most ``_DELTA_SLICE_MAX_ROWS`` rows,
each with its own checkpoint, up to the safe read point at the run's start;
other modes are one slice.

The spool is required because the orchestrator iterates the full input several
times BEFORE its chunk loop (``_ensure_reference_dependencies`` +
``_pre_resolve_references``), so a one-shot cursor stream cannot feed it.
"""

import json
import os
import pickle
import tempfile
from pathlib import Path
from typing import Any

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.extract.batched_rows import BatchedExtractRows
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.Aktivitetsresultat.orchestrator import _run_aktivitetsresultat_pipeline_chunked
from DSA.FHIR.Aktivitetsresultat.questionnaire_outputs import _metadata_int
from DSA.FHIR.Aktivitetsresultat.routing_glue import _sfi_mode_enabled
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query, next_delta_slice_bound
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS

# Rows per pickle sidecar while spooling the archive cursor to disk. This is a
# fetch/IO knob only — the pipeline chunk stays _AKTIVITETSRESULTAT_CHUNK_SIZE
# (25k, env IFDB_AKTIVITETSRESULTAT_CHUNK_SIZE): BatchedExtractRows.iter_batches
# coalesces sidecars to that size. Legacy extract sidecars held 50k rows.
_SPOOL_BATCH_SIZE = 10_000

_ARCHIVE_TABLE = "arc.ifdb_aktivitetsresultat"

# Rows per delta slice. The orchestrator resolves every reference of its input
# up front, so its memory grows with the input; a slice never splits one archive MERGE.
_DELTA_SLICE_MAX_ROWS = 8_000_000

# Optional override for where the spool directory is created (e.g. a large
# volume during initial full loads). Defaults to the system temp dir.
_SPOOL_DIR_ENV = "IFDB_AKTIVITETSRESULTAT_SPOOL_DIR"

_AKTIVITETSRESULTAT_OUT = "aktivitetsresultat"
_QUESTIONNAIRE_OUT = "questionnaire"
_QUESTIONNAIRE_RESPONSE_OUT = "questionnaire_response"

_DFM_OUTS = {
    _AKTIVITETSRESULTAT_OUT: dg.AssetOut(
        key=dg.AssetKey("fhir_server_aktivitetsresultat_dfm"),
        group_name="fhir_ifdb_dfm",
    ),
    _QUESTIONNAIRE_OUT: dg.AssetOut(
        key=dg.AssetKey("fhir_server_questionnaire_dfm"),
        group_name="fhir_ifdb_dfm",
    ),
    _QUESTIONNAIRE_RESPONSE_OUT: dg.AssetOut(
        key=dg.AssetKey("fhir_server_questionnaire_response_dfm"),
        group_name="fhir_ifdb_dfm",
    ),
}

_OBS_COUNT_KEYS = (
    "posted_count",
    "procedure_posted_count",
    "condition_posted_count",
    "failed_count",
    "unique_violation_recovered_count",
    "extracted_count",
)
_SFI_COUNT_KEYS = ("posted_count", "failed_count", "unique_violation_recovered_count")


def _zero_sfi_metadata(context: dg.AssetExecutionContext, method: str) -> dict[str, Any]:
    return {
        "posted_count": dg.MetadataValue.int(0),
        "failed_count": dg.MetadataValue.int(0),
        "unique_violation_recovered_count": dg.MetadataValue.int(0),
        "partition_key": dg.MetadataValue.text("N/A"),
        "method": dg.MetadataValue.text(method),
    }


def _read_counts(result: dg.MaterializeResult | None, keys: tuple[str, ...]) -> dict[str, int]:
    """Read integer metadata keys from a pipeline result (0 when absent/None).

    Orkestreringen sætter alle nøglerne uanset chunk-antal — også ved kørsler
    på under én chunk — så 0 betyder her kun, at resultatet mangler.
    """
    if result is None:
        return {key: 0 for key in keys}
    return {key: _metadata_int(result, key) for key in keys}


def _spool_archive_to_sidecars(
    cursor: Any,
    columns: list[str],
    spool_dir: Path,
) -> tuple[int, int]:
    """Stream the archive cursor to ``BatchedExtractRows``-format sidecars.

    Writes ``batch-%06d.pickle`` files plus the ``manifest.json`` (version 1)
    that ``core.extract.batched_rows.BatchedExtractRows`` reads. Deliberately
    local instead of reusing ``write_batches_to_pickle_list``: that writer also
    produces a monolithic pickle we would immediately delete (double disk IO),
    and shared core code stays untouched in a resource PR. The roundtrip unit
    test pins the format contract against ``BatchedExtractRows``.

    Returns ``(row_count, batch_count)``. Only one fetch batch is held in
    memory at a time.
    """
    row_count = 0
    batch_files: list[str] = []
    while True:
        raw_rows = cursor.fetchmany(_SPOOL_BATCH_SIZE)
        if not raw_rows:
            break
        batch = [dict(zip(columns, row, strict=False)) for row in raw_rows]
        filename = f"batch-{len(batch_files) + 1:06d}.pickle"
        with open(spool_dir / filename, "wb") as file:
            pickle.dump(batch, file, protocol=pickle.HIGHEST_PROTOCOL)
        batch_files.append(filename)
        row_count += len(batch)

    manifest = {
        "version": 1,
        "row_count": row_count,
        "batch_count": len(batch_files),
        "batch_files": batch_files,
    }
    (spool_dir / "manifest.json").write_text(json.dumps(manifest))
    return row_count, len(batch_files)


def _spool_and_post(
    context: dg.AssetExecutionContext,
    query: str,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
) -> tuple[
    int,
    int,
    dg.MaterializeResult | None,
    dg.MaterializeResult | None,
    dg.MaterializeResult | None,
]:
    """Spool one query's rows to disk and run the pipeline once over them."""
    obs_result: dg.MaterializeResult | None = None
    q_result: dg.MaterializeResult | None = None
    qr_result: dg.MaterializeResult | None = None

    with tempfile.TemporaryDirectory(
        prefix="arc_aktivitetsresultat_spool_",
        dir=os.environ.get(_SPOOL_DIR_ENV) or None,
    ) as spool_dir_str:
        spool_dir = Path(spool_dir_str)

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                total_fetched, spool_batches = _spool_archive_to_sidecars(
                    cursor, columns, spool_dir
                )
            finally:
                cursor.close()

        context.log.info(
            f"Spooled {total_fetched} aktivitetsresultater to {spool_batches} sidecar batches"
        )

        if total_fetched > 0:
            rows = BatchedExtractRows(spool_dir)
            if _sfi_mode_enabled():
                result = _run_aktivitetsresultat_pipeline_chunked(
                    extracted_data=rows,
                    context=context,
                    ifdb_oracle=None,
                    fhir_database=fhir_database,
                    mssql_target_staging=mssql_target_staging,
                    emit_questionnaire_outputs=True,
                )
                # NB: MaterializeResult is a NamedTuple (tuple subclass),
                # so a bare isinstance(result, tuple) cannot tell a single
                # result from the 3-tuple — check for the single first.
                if isinstance(result, dg.MaterializeResult) or not (
                    isinstance(result, tuple) and len(result) == 3
                ):
                    raise TypeError(
                        "Expected unified SFI pipeline to return three materialization results"
                    )
                obs_result, q_result, qr_result = result
            else:
                legacy_result = _run_aktivitetsresultat_pipeline_chunked(
                    extracted_data=rows,
                    context=context,
                    ifdb_oracle=None,
                    fhir_database=fhir_database,
                    mssql_target_staging=mssql_target_staging,
                )
                if not isinstance(legacy_result, dg.MaterializeResult):
                    raise TypeError("Expected single materialization result in legacy mode")
                obs_result = legacy_result

    return total_fetched, spool_batches, obs_result, q_result, qr_result


@dg.multi_asset(
    name="fhir_server_aktivitetsresultat_sfi_dfm",
    compute_kind="fhir",
    outs=_DFM_OUTS,
    deps=[
        "arc_aktivitetsresultat_ifdb",
        "arc_aktivitetsudfoerelse_ifdb",
        "arc_aktivitetsudfoerelse_speciale_ifdb",
        "fhir_server_patient_dfm",
        "fhir_server_encounter_dfm",
        "fhir_server_eoc_dfm",
    ],
    op_tags={TAG_CHUNK_SIZE: "500", TAG_MAX_WORKERS: "4"},
    description=(
        "Spool AKTIVITETSRESULTAT from arc.ifdb_aktivitetsresultat to disk, "
        "run the SFI pipeline once per delta slice over the streamed batches (Observation/"
        "Procedure/Condition + Questionnaire/QR when SFI mode is on), and post "
        "to the FHIR server. Checkpoint owner for "
        "fhir_server.Aktivitetsresultat_SFI."
    ),
)
def fhir_server_aktivitetsresultat_sfi_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
):
    """Spool arc rows → one pipeline call per delta slice → post (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "Aktivitetsresultat_SFI",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        obs_totals = dict.fromkeys(_OBS_COUNT_KEYS, 0)
        q_totals = dict.fromkeys(_SFI_COUNT_KEYS, 0)
        qr_totals = dict.fromkeys(_SFI_COUNT_KEYS, 0)
        total_fetched = 0
        spool_batches = 0
        new_checkpoint: str | None = None
        lower = run_ctx.checkpoint
        sliced = config.mode == "delta" and not (config.lookback_days or config.delta_column)
        # Rows of an archive MERGE in flight are stamped after this point; no
        # slice may pass it, or a later slice's checkpoint would skip them.
        ceiling = dfm.safe_reader_checkpoint(run_ctx.started_at) if sliced else None

        while True:
            upper: str | None = None
            if sliced:
                with mssql_target_staging.get_connection() as conn:
                    upper = next_delta_slice_bound(
                        conn, _ARCHIVE_TABLE, lower, _DELTA_SLICE_MAX_ROWS, ceiling
                    )
                if upper is None:
                    break
            query = build_fhir_archive_query(
                _ARCHIVE_TABLE,
                lower,
                config,
                include_deleted=True,
                delta_upper_bound=upper,
            )
            context.log.info(
                f"Querying {_ARCHIVE_TABLE} (mode={config.mode}"
                + (f", up to {upper})" if upper else ")")
            )
            fetched, batches, obs_result, q_result, qr_result = _spool_and_post(
                context, query, mssql_target_staging, fhir_database
            )
            if sliced and fetched > _DELTA_SLICE_MAX_ROWS:
                context.log.warning(
                    f"One archive MERGE at {upper} holds {fetched} rows, above the slice "
                    f"limit of {_DELTA_SLICE_MAX_ROWS}; it is read as one slice."
                )
            total_fetched += fetched
            spool_batches += batches
            for totals, result, keys in (
                (obs_totals, obs_result, _OBS_COUNT_KEYS),
                (q_totals, q_result, _SFI_COUNT_KEYS),
                (qr_totals, qr_result, _SFI_COUNT_KEYS),
            ):
                for key, value in _read_counts(result, keys).items():
                    totals[key] += value

            slice_checkpoint = advance_fhir_checkpoint(
                dfm, run_ctx, "Aktivitetsresultat_SFI", config, fetched, read_bound=upper
            )
            new_checkpoint = slice_checkpoint or new_checkpoint
            if not sliced:
                break
            lower = upper

        context.log.info(
            f"Done: {total_fetched} fetched, "
            f"{obs_totals['posted_count']} obs / "
            f"{obs_totals['procedure_posted_count']} proc / "
            f"{obs_totals['condition_posted_count']} cond posted "
            f"({spool_batches} spool batches)"
        )

        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=new_checkpoint,
            rows_affected=obs_totals["posted_count"]
            + obs_totals["procedure_posted_count"]
            + obs_totals["condition_posted_count"]
            + q_totals["posted_count"]
            + qr_totals["posted_count"],
        )

        obs_meta = {
            "posted_count": dg.MetadataValue.int(obs_totals["posted_count"]),
            "procedure_posted_count": dg.MetadataValue.int(obs_totals["procedure_posted_count"]),
            "condition_posted_count": dg.MetadataValue.int(obs_totals["condition_posted_count"]),
            "failed_count": dg.MetadataValue.int(obs_totals["failed_count"]),
            # Rækker genoprettet efter unique-violation; forventet 0, da id'erne
            # pre-resolves — et tal over 0 peger på et hul i opslaget.
            "unique_violation_recovered_count": dg.MetadataValue.int(
                obs_totals["unique_violation_recovered_count"]
            ),
            "fetched_count": dg.MetadataValue.int(total_fetched),
            # Spool sidecar count — the pipeline's own 25k chunk count is in
            # the orchestrator's log/metadata ("chunk_count") when multi-chunk.
            "batch_count": dg.MetadataValue.int(spool_batches),
            "mode": dg.MetadataValue.text(config.mode),
            "source": dg.MetadataValue.text("arc.ifdb_aktivitetsresultat"),
            "method": dg.MetadataValue.text("dfm_arc_streamed"),
        }
        if _sfi_mode_enabled():
            q_meta = {
                "posted_count": dg.MetadataValue.int(q_totals["posted_count"]),
                "failed_count": dg.MetadataValue.int(q_totals["failed_count"]),
                "unique_violation_recovered_count": dg.MetadataValue.int(
                    q_totals["unique_violation_recovered_count"]
                ),
                "mode": dg.MetadataValue.text(config.mode),
                "method": dg.MetadataValue.text("dfm_arc_streamed"),
            }
            qr_meta = {
                "posted_count": dg.MetadataValue.int(qr_totals["posted_count"]),
                "failed_count": dg.MetadataValue.int(qr_totals["failed_count"]),
                "unique_violation_recovered_count": dg.MetadataValue.int(
                    qr_totals["unique_violation_recovered_count"]
                ),
                "mode": dg.MetadataValue.text(config.mode),
                "method": dg.MetadataValue.text("dfm_arc_streamed"),
            }
        else:
            q_meta = _zero_sfi_metadata(context, "sfi_legacy_noop")
            qr_meta = _zero_sfi_metadata(context, "sfi_legacy_noop")

        yield dg.Output(value=None, output_name=_AKTIVITETSRESULTAT_OUT, metadata=obs_meta)
        yield dg.Output(value=None, output_name=_QUESTIONNAIRE_OUT, metadata=q_meta)
        yield dg.Output(value=None, output_name=_QUESTIONNAIRE_RESPONSE_OUT, metadata=qr_meta)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

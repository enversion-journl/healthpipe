# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Daily full load of the warehouse's codesystems to Magenta (OS2mo), as CSV files.

Two assets per codesystem in :data:`CODESYSTEM_EXPORTS`, built by one factory:

``os2mo_<codesystem>_export``
    Reads ``codesystem.<Name>*`` (and its ConceptMaps), builds the delivery set,
    lands it in ``export.CodesystemToOs2mo`` — this codesystem's rows replaced
    every run, because a delivery set is state and not an archive — and renders
    the CSV. Needs no OS2mo access at all.

``os2mo_<codesystem>_upload``
    Uploads that exact CSV through OS2mo's GraphQL ``upload_file`` mutation.
    That is the whole delivery: what Magenta's import does with the file is
    theirs, including deltas and codes that fall out of the set.

The upload consumes the export asset's output, so the table, the local file and
the bytes on the wire are provably the same set.

Run history lives in ``meta.DFMRunLog``: ``export`` for building a set,
``os2mo`` for uploading it. Both are log-only — a full load every night has no
delta position to hold — so neither writes a ``meta.DFMState`` row.
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Any, NamedTuple

import dagster as dg
from core.dfm_manager import DfmManager
from core.org_config import DATA_DIR
from core.os2mo import OS2moConfigurationError
from core.os2mo.client import DEFAULT_FILE_STORE
from core.resources import MSSQLResource, OS2moResource
from DSO.export.os2mo.codesystems import CODESYSTEM_EXPORTS
from DSO.export.os2mo.source_query import read_mappings, read_source_rows, write_export_table
from utils.os2mo.codesystem_export import (
    CodesystemExportSpec,
    build_export_rows,
    map_slots_needed,
    unreadable_values,
)
from utils.os2mo.csv_format import encode_csv, render_csv, write_csv

__all__ = [
    "DELIVERY_DFM_SCHEMA",
    "EXPORT_DFM_SCHEMA",
    "OS2moExportConfig",
    "OS2moUploadConfig",
    "ExportSet",
    "build_codesystem_assets",
    "read_export_set",
    "export_asset_name",
    "os2mo_export_assets",
    "upload_asset_name",
]

EXPORT_GROUP = "os2mo_export"

# How many data rows of the file the export asset shows in its metadata.
CSV_PREVIEW_ROWS = 20

# DFM schemas: "export" is the staging schema holding the delivery sets; "os2mo"
# is a LOGICAL target — the upload, which has no table of ours — the same shape
# as "fhir_server" standing for a push to Aidbox.
EXPORT_DFM_SCHEMA = "export"
DELIVERY_DFM_SCHEMA = "os2mo"


class OS2moExportConfig(dg.Config):
    """Config for building a delivery set.

    ``output_dir`` only moves the local copy of the file; the uploaded bytes do
    not depend on it.
    """

    output_dir: str | None = None


class OS2moUploadConfig(dg.Config):
    """Config for the upload.

    ``dry_run`` defaults to True: reaching an external system is something a
    person — or the delivery sensor — should ask for explicitly, not something a
    default does. A dry run builds and checks everything and sends nothing,
    and needs no OS2mo configuration at all.
    """

    dry_run: bool = True
    file_store: str = DEFAULT_FILE_STORE
    force: bool = True


def export_asset_name(spec: CodesystemExportSpec) -> str:
    return f"os2mo_{spec.asset_prefix}_export"


def upload_asset_name(spec: CodesystemExportSpec) -> str:
    return f"os2mo_{spec.asset_prefix}_upload"


def csv_preview(csv_text: str, rows: int = CSV_PREVIEW_ROWS) -> str:
    """The header and the first ``rows`` records, as a Markdown code block.

    The file lives inside the code-server's container, out of reach for most
    people checking a run, so the start of it is shown where they look: on the
    materialization. Split on the file's CRLF record separator, so a value with
    an embedded newline stays on its record.
    """
    lines = csv_text.split("\r\n")
    shown = [line for line in lines[: rows + 1] if line]
    total = sum(1 for line in lines[1:] if line)
    footer = f"\n… {total - rows} more rows" if total > rows else ""
    return "```\n" + "\n".join(shown) + "\n```" + footer


def _describe_target(os2mo: OS2moResource) -> str:
    """The endpoint, or why there is not one. Never raises."""
    try:
        return os2mo.graphql_url
    except OS2moConfigurationError:
        return "not configured"


class ExportSet(NamedTuple):
    """One codesystem's export rows, and what was left out on the way."""

    rows: list[dict[str, Any]]
    codes_read: int
    codes_mapped: int
    # Concepts with no properties left: the source no longer has them.
    codes_gone: int
    # Present but unreadable dates and flags, per source field.
    unreadable: dict[str, int]

    @property
    def codes_unreachable(self) -> int:
        """Codes read but not in the set: they sit in a parent cycle."""
        return self.codes_read - self.codes_gone - len(self.rows)


def read_export_set(mssql: MSSQLResource, spec: CodesystemExportSpec) -> ExportSet:
    """Read a codesystem and its ConceptMaps and build the export rows.

    The one definition of "what goes in the file", shared by the export asset
    and ``scripts/os2mo_render_csv.py``.
    """
    source_rows = read_source_rows(mssql, spec)
    mappings = read_mappings(mssql, spec)
    # A concept outlives its source: when the codesystem merge drops a code's
    # properties and parent edge, the concept row stays. Delivered, it would
    # be an open-ended root with no dates and no status.
    present = [row for row in source_rows if row.get("has_properties", 1)]
    return ExportSet(
        rows=build_export_rows(spec, present, mappings),
        codes_read=len(source_rows),
        codes_mapped=len(mappings),
        codes_gone=len(source_rows) - len(present),
        unreadable=unreadable_values(present),
    )


def _build_delivery(
    context: dg.AssetExecutionContext,
    spec: CodesystemExportSpec,
    mssql: MSSQLResource,
    config: OS2moExportConfig,
) -> dg.Output[dict[str, Any]]:
    """Read, build, land in the export table and render — inside a DFM run."""
    dfm = DfmManager(mssql, context)
    with dfm.connection():
        run_ctx = dfm.start_dfm_run(
            EXPORT_DFM_SCHEMA,
            f"{spec.name}ToOs2mo",
            dagster_run_id=context.run_id,
            mode="full_load",
        )
        try:
            export_set = read_export_set(mssql, spec)
            export_rows = export_set.rows
            context.log.info(
                f"Read {export_set.codes_read} {spec.name} codes and mappings for "
                f"{export_set.codes_mapped} of them"
            )

            if not export_rows:
                raise dg.Failure(
                    description=f"The {spec.name} export set is empty — refusing to deliver it."
                )

            # Every code read should be reachable from a root; one that is not
            # sits in a parent cycle and cannot be placed in the tree.
            if export_set.codes_gone:
                context.log.warning(
                    f"{export_set.codes_gone} {spec.name} codes have no properties left "
                    "(gone from the source) and are left out of the file."
                )
            if export_set.unreadable:
                context.log.warning(
                    f"{spec.name} values the file renders as empty because they could not "
                    f"be read: {export_set.unreadable}"
                )
            unreachable = export_set.codes_unreachable
            if unreachable:
                context.log.warning(
                    f"{unreachable} {spec.name} codes are not reachable from any root "
                    "and are left out of the file."
                )

            # One stamp for the whole set: "when was this built", not history.
            built_at = datetime.now().replace(microsecond=0)
            written = write_export_table(mssql, spec, export_rows, built_at)
            context.log.info(f"Wrote {written} {spec.name} rows to export.CodesystemToOs2mo")

            csv_text = render_csv(spec, export_rows)
            filename = spec.filename(built_at)
            path = Path(config.output_dir or os.path.join(DATA_DIR, "os2mo")) / filename
            write_csv(csv_text, path)
            context.log.info(f"Wrote {len(export_rows)} rows to {path}")
        except Exception as exc:
            dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
            raise

        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=len(export_rows))

    return dg.Output(
        {"filename": filename, "csv": csv_text, "rows": len(export_rows)},
        metadata={
            "codes_exported": len(export_rows),
            "codes_read": export_set.codes_read,
            "codes_unreachable": unreachable,
            "codes_gone": export_set.codes_gone,
            "unreadable_values": dg.MetadataValue.json(export_set.unreadable),
            "codes_with_mappings": sum(1 for row in export_rows if row.get("_mappings")),
            "map_slots": map_slots_needed(spec, export_rows),
            "target_table": "export.CodesystemToOs2mo",
            "csv_path": str(path),
            "csv_filename": filename,
            "csv_preview": dg.MetadataValue.md(csv_preview(csv_text)),
            "built_at": built_at.isoformat(),
        },
    )


def _upload_delivery(
    context: dg.AssetExecutionContext,
    spec: CodesystemExportSpec,
    delivery: dict[str, Any],
    os2mo: OS2moResource,
    mssql: MSSQLResource,
    config: OS2moUploadConfig,
) -> dg.MaterializeResult:
    """Upload the rendered file, logged as a DFM run when it is real.

    A dry run opens no DFM run: meta.DFMRunLog answers *did we deliver, and did
    it work*, and a rehearsal is not a delivery.
    """
    content = encode_csv(delivery["csv"])
    target = _describe_target(os2mo)
    metadata: dict[str, Any] = {
        "filename": delivery["filename"],
        "rows": delivery["rows"],
        "bytes": len(content),
        "file_store": config.file_store,
        "graphql_url": target,
        "dry_run": config.dry_run,
    }

    if config.dry_run:
        context.log.info(
            f"dry_run — would upload {delivery['filename']} ({delivery['rows']} rows, "
            f"{len(content)} bytes) to {config.file_store} on {target}"
        )
        return dg.MaterializeResult(metadata=metadata)

    dfm = DfmManager(mssql, context)
    with dfm.connection():
        run_ctx = dfm.start_dfm_run(
            DELIVERY_DFM_SCHEMA,
            f"{spec.name}File",
            dagster_run_id=context.run_id,
            mode="full_load",
        )
        try:
            os2mo.upload_file(
                delivery["filename"], content, file_store=config.file_store, force=config.force
            )
        except Exception as exc:
            dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
            raise
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=delivery["rows"])

    context.log.info(f"Uploaded {delivery['filename']} to {config.file_store} on {target}")
    return dg.MaterializeResult(metadata=metadata)


def build_codesystem_assets(spec: CodesystemExportSpec) -> list[dg.AssetsDefinition]:
    """The export and upload asset for one codesystem."""
    export_name = export_asset_name(spec)

    @dg.asset(
        name=export_name,
        group_name=EXPORT_GROUP,
        compute_kind="mssql",
        deps=[*spec.source_deps, "staging_infra_export"],
        tags={"domain": "codesystem", "layer": "export", "source": spec.asset_prefix},
        description=(
            f"Build the {spec.name} delivery set from codesystem.{spec.name}* into "
            f"export.CodesystemToOs2mo and render {spec.filename_pattern}."
        ),
    )
    def export_asset(
        context: dg.AssetExecutionContext,
        mssql_target_staging: MSSQLResource,
        config: OS2moExportConfig,
    ) -> dg.Output[dict[str, Any]]:
        return _build_delivery(context, spec, mssql_target_staging, config)

    @dg.asset(
        name=upload_asset_name(spec),
        group_name=EXPORT_GROUP,
        compute_kind="graphql",
        ins={"delivery": dg.AssetIn(key=dg.AssetKey(export_name))},
        tags={"domain": "codesystem", "layer": "export", "source": spec.asset_prefix},
        description=(
            f"Upload {spec.filename_pattern} to OS2mo's file store over the pinned GraphQL "
            "endpoint (upload_file). Full load; Magenta's import handles deltas."
        ),
    )
    def upload_asset(
        context: dg.AssetExecutionContext,
        delivery: dict[str, Any],
        os2mo: OS2moResource,
        mssql_target_staging: MSSQLResource,
        config: OS2moUploadConfig,
    ) -> dg.MaterializeResult:
        return _upload_delivery(context, spec, delivery, os2mo, mssql_target_staging, config)

    return [export_asset, upload_asset]


os2mo_export_assets: list[dg.AssetsDefinition] = [
    asset for spec in CODESYSTEM_EXPORTS for asset in build_codesystem_assets(spec)
]

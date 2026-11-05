# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Patient stubs for CPRs that only the DRG deliveries know about.

The tier split and the deliberate non-fix of tier B are inherited decisions —
see cpr_stub_asset.py and Patient_HAIBA.py's module docstring. The resources
are written through the Patient entry point (patient_service.ensure_patients),
which tags them SDSDRG. Two things are specific to DRG:

  * **One asset per leverancetype.** Somatik and psyk come from different files
    and run in different jobs. A shared asset would depend on both
    archives, so a somatik failure would skip psyk's stub creation and
    re-introduce exactly the coupling the job split removes.
  * **Scoped to the takstsystemer this run loaded.** A run handles ONE
    takstsystem, so an unscoped DISTINCT would rescan the whole ~20M-row
    archive on every run. The scope comes from the delivery manifest — the
    contract for what the run actually loaded — and because ``takstsystem`` is
    the LEADING column of the arc primary key it is a range seek rather than a
    full clustered scan. An empty manifest means no new CPRs, so the archive is
    not opened at all.

GDPR: CPR values never leave the staging/FHIR databases; logs and metadata
carry counts only.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.FHIR.Patient.cpr_stub_asset import CprStubConfig, build_cpr_stub_asset
from DSA.FHIR.Patient.cpr_stub_asset import classify_cprs as _classify_cprs
from DSA.FHIR.Patient.patient_ownership import SOURCE_DRG
from DSI.csv_files.drg_archive import read_manifest_by_type

# One per leverancetype: they come from different files, run in different jobs
# and must not be able to block each other.
SOURCES = {
    "DRG": ("drg_kontakt", "CPR"),
    "DRGPSYK": ("drgpsyk_kontakt", "CPRnummer"),
}

CPR_SQL = """
-- No length cast: CAST(... AS NVARCHAR(10)) would silently truncate a
-- malformed overlong value and create a stub under the wrong identity.
SELECT DISTINCT LTRIM(RTRIM([{cpr_column}])) AS cpr
FROM [arc].[{kontakt_table}]
WHERE [dw_is_deleted] = 0 AND [{cpr_column}] IS NOT NULL
  AND LTRIM(RTRIM([{cpr_column}])) <> ''
  AND [takstsystem] IN ({takstsystemer})
"""


def render_cpr_sql(leverancetype: str, takstsystemer: list[str]) -> str | None:
    """The CPR query for one leverancetype, scoped to this run's takstsystemer.

    Returns None for an empty scope: no manifest means the run loaded nothing,
    so there are no new CPRs and the archive must not be opened.

    The takstsystemer are interpolated as literals rather than bound, because
    ``classify_cprs`` takes no parameters. That is safe ONLY because each value
    is validated here against the same shape the filename regex enforces —
    anything else is a programming error and fails loudly instead of reaching
    the database.
    """
    if not takstsystemer:
        return None
    kontakt_table, cpr_column = SOURCES[leverancetype]
    for takstsystem in takstsystemer:
        if not (len(takstsystem) == 3 and takstsystem[0] == "T" and takstsystem[1:].isdigit()):
            raise ValueError(
                f"takstsystem {takstsystem!r} does not look like 'T26'; refusing to "
                f"build the CPR query from it"
            )
    values = ", ".join(f"'{t}'" for t in sorted(takstsystemer))
    return CPR_SQL.format(kontakt_table=kontakt_table, cpr_column=cpr_column, takstsystemer=values)


def _cpr_sql_for_run(leverancetype: str):
    """Per-run CPR SQL, scoped to the deliveries this run actually loaded."""

    def _resolve(context: dg.AssetExecutionContext, mssql: MSSQLResource) -> str | None:
        with mssql.get_connection() as conn:
            manifest = read_manifest_by_type(conn, leverancetype)
        takstsystemer = sorted(manifest)
        if not takstsystemer:
            context.log.info(f"{leverancetype}: tomt manifest — ingen nye CPR'er at vurdere")
            return None
        context.log.info(f"{leverancetype}: vurderer CPR'er for {', '.join(takstsystemer)}")
        return render_cpr_sql(leverancetype, takstsystemer)

    return _resolve


def classify_cprs(
    mssql: MSSQLResource, leverancetype: str, takstsystemer: list[str]
) -> dict[str, list[str]]:
    """Split one leverancetype's distinct CPRs into tiers A, B and C.

    Scoped to the takstsystemer given; an empty scope opens no connection.
    """
    sql = render_cpr_sql(leverancetype, takstsystemer)
    if sql is None:
        return {"A": [], "B": [], "C": []}
    return _classify_cprs(mssql, sql)


def _build_asset(leverancetype: str, suffix: str):
    kontakt_table, _ = SOURCES[leverancetype]
    return build_cpr_stub_asset(
        name=f"fhir_patient_{suffix}_drg",
        group_name="drg_borger",
        deps=[f"arc_{kontakt_table}"],
        tags={"domain": "drg", "layer": "dsa", "source": "drg"},
        description=(
            f"Create stub Patient resources for CPRs in the {leverancetype} "
            f"deliveries that FHIR does not know, so their contacts can resolve a "
            f"dw_ek_Borger in the next staging pass. Scoped to the takstsystemer "
            f"this run loaded. Written through the Patient entry point; tagged SDSDRG."
        ),
        cpr_sql=_cpr_sql_for_run(leverancetype),
        stub_source=SOURCE_DRG,
        source_label=leverancetype,
        source_name=f"drg_{suffix}_stubs",
    )


fhir_patient_somatik_drg = _build_asset("DRG", "somatik")
fhir_patient_psyk_drg = _build_asset("DRGPSYK", "psyk")
drg_borger_assets = [fhir_patient_somatik_drg, fhir_patient_psyk_drg]


__all__ = [
    "SOURCES",
    "CprStubConfig",
    "classify_cprs",
    "drg_borger_assets",
    "fhir_patient_psyk_drg",
    "fhir_patient_somatik_drg",
    "render_cpr_sql",
]

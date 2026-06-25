# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DFM extract configs for SydAX archive layer.

Adds 17 ``ext.<table>_sydax_dfm`` extract assets alongside the existing
partitioned ``ext.*_sydax`` flow. Each asset writes a delta slice filtered
on ``COALESCE([Modified], [CreatedOn])`` and feeds the matching
``arc.<table>_sydax`` archive asset.

The bridge table (FinansposteringKreditortransRelation) uses
``select_from_expr`` to JOIN Finanspostering and expose
``ref_LedgertransId`` so the archive can merge on the ERP-stable key
tuple ``(Kildesystem, ref_LedgertransId, KreditortransaktionId)``.

Tables with a nullable ERP-stable key use ``select_from_expr`` with a
WHERE clause that filters out NULL key rows. The factory then appends
its own delta filter via AND.
"""

import dagster as dg
from core.resources import SydaxMSSQLResource
from DSI.connectors.extract_config import DfmExtractConfig
from DSI.connectors.factory import create_dfm_extraction_assets

_DELTA_EXPR = "COALESCE([Modified], [CreatedOn])"
_LOOKBACK = 3


def _checkpoint_for(target_table: str) -> tuple[str, str]:
    # target ext.<x>_sydax_dfm  --> checkpoint from arc.<x>_sydax
    return ("arc", target_table.removesuffix("_dfm"))


# ---------------------------------------------------------------------------
# Plain delta configs (no special WHERE / JOIN)
# ---------------------------------------------------------------------------

_PLAIN_DELTA = [
    # source_schema, source_table, target_table
    ("Finans", "Afdeling", "afdeling_sydax_dfm"),
    ("Finans", "Finanskonto", "finanskonto_sydax_dfm"),
    ("Finans", "FinanskontoOmkostningssted", "finanskonto_omkostningssted_sydax_dfm"),
    ("Finans", "OmkostningsstedHierarki", "omkostningssted_hierarki_sydax_dfm"),
    ("Kreditor", "Kreditor", "kreditor_sydax_dfm"),
    ("Finans", "Finanskladde", "finanskladde_sydax_dfm"),
    ("Finans", "Finanstransaktionstype", "finanstransaktionstype_sydax_dfm"),
]

_PLAIN_DELTA_CONFIGS = [
    DfmExtractConfig(
        source_schema=schema,
        source_table=table,
        target_table=target,
        delta_expression=_DELTA_EXPR,
        checkpoint_type="datetime",
        lookback_days=_LOOKBACK,
        checkpoint_from=_checkpoint_for(target),
        description=f"SydAX {schema}.{table} delta extract for arc.{target.removesuffix('_dfm')}.",
    )
    for schema, table, target in _PLAIN_DELTA
]


# ---------------------------------------------------------------------------
# Configs with a nullable ERP-stable key — filter NULLs at extract time
# ---------------------------------------------------------------------------

# (source_schema, source_table, target_table, stable_key_column)
_NULL_FILTERED = [
    ("Finans", "Hovedkonto", "hovedkonto_sydax_dfm", "ref_RECID"),
    ("Finans", "Gruppering", "gruppering_sydax_dfm", "RECID"),
    ("Finans", "GrupperingAutoriseret", "gruppering_autoriseret_sydax_dfm", "ref_RECID"),
    ("Finans", "Art", "art_sydax_dfm", "RECID"),
    ("Finans", "Funktion", "funktion_sydax_dfm", "ref_RECID"),
    ("Finans", "Dranst", "dranst_sydax_dfm", "ref_RECID"),
    ("Finans", "Finanspostering", "finanspostering_sydax_dfm", "ref_LedgertransId"),
    ("Kreditor", "Kreditortransaktion", "kreditortransaktion_sydax_dfm", "ref_VendTransId"),
    ("Finans", "Finanskladdelinje", "finanskladdelinje_sydax_dfm", "ref_LedgertransId"),
]

_NULL_FILTERED_CONFIGS = [
    DfmExtractConfig(
        source_schema=schema,
        source_table=table,
        target_table=target,
        delta_expression=_DELTA_EXPR,
        checkpoint_type="datetime",
        lookback_days=_LOOKBACK,
        checkpoint_from=_checkpoint_for(target),
        select_from_expr=(f"SELECT * FROM [{schema}].[{table}] " f"WHERE [{key}] IS NOT NULL"),
        description=(
            f"SydAX {schema}.{table} delta extract for arc.{target.removesuffix('_dfm')} "
            f"(filters rows where [{key}] IS NULL — that's the ERP-stable merge key)."
        ),
    )
    for schema, table, target, key in _NULL_FILTERED
]


# ---------------------------------------------------------------------------
# Bridge table — JOIN to Finanspostering to expose ref_LedgertransId
# ---------------------------------------------------------------------------

# We expose ref_LedgertransId via the JOIN so the archive can merge on the
# stable (Kildesystem, ref_LedgertransId, KreditortransaktionId) tuple.
# Rows where the joined ref_LedgertransId is NULL are excluded.

_BRIDGE_SELECT = """
SELECT
    bridge.[FinansposteringKreditortransRelationId],
    bridge.[FinansposteringId],
    bridge.[KreditortransaktionId],
    bridge.[RunId],
    bridge.[StartDateTime],
    bridge.[EndDateTime],
    bridge.[RdwTs],
    bridge.[Kildesystem],
    bridge.[CreatedOn],
    bridge.[Modified],
    post.[ref_LedgertransId]
FROM [Finans].[FinansposteringKreditortransRelation] AS bridge
INNER JOIN [Finans].[Finanspostering] AS post
    ON post.[Kildesystem] = bridge.[Kildesystem]
   AND post.[FinansposteringId] = bridge.[FinansposteringId]
WHERE post.[ref_LedgertransId] IS NOT NULL
""".strip()

_BRIDGE_CONFIG = DfmExtractConfig(
    source_schema="Finans",
    source_table="FinansposteringKreditortransRelation",
    target_table="finanspostering_kreditortrans_relation_sydax_dfm",
    delta_expression="COALESCE(bridge.[Modified], bridge.[CreatedOn])",
    checkpoint_type="datetime",
    lookback_days=_LOOKBACK,
    checkpoint_from=("arc", "finanspostering_kreditortrans_relation_sydax"),
    select_from_expr=_BRIDGE_SELECT,
    description=(
        "SydAX bridge (FinansposteringKreditortransRelation) delta extract for "
        "arc.finanspostering_kreditortrans_relation_sydax. JOINs Finanspostering "
        "to expose ref_LedgertransId for stable archive merging."
    ),
)


# ---------------------------------------------------------------------------
# Combined list + factory call
# ---------------------------------------------------------------------------

SYDAX_DFM_EXTRACT_CONFIGS: list[DfmExtractConfig] = (
    _PLAIN_DELTA_CONFIGS + _NULL_FILTERED_CONFIGS + [_BRIDGE_CONFIG]
)


sydax_archive_extract_assets = create_dfm_extraction_assets(
    configs=SYDAX_DFM_EXTRACT_CONFIGS,
    source_resource_key="sydax_mssql",
    source_resource_type=SydaxMSSQLResource,
    source_system="SydAX",
    group_name="sydax_archive",
    domain_tag="erp",
    dialect="mssql",
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
)

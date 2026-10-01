# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DFM extract configs for the SydAX archive layer.

Why the delta strategy is per-table
-----------------------------------
A uniform ``COALESCE([Modified],[CreatedOn])`` delta does NOT work here: for
several SydAX RDW tables the warehouse reloads *every row every day*, so
``RdwTs``, ``CreatedOn`` and even the ``<Table>Id`` surrogate all carry today's
timestamp — useless as an incremental key (and the surrogate can't identify a
row across days, which is why arc keys on the ERP-stable key, not the Id). Other
tables maintain ``RdwTs`` incrementally (values spread over years).

The strategy is therefore chosen from each source's *observed* behaviour, via a
``MIN/MAX(RdwTs)`` + row-count diagnostic over the source tables:

- Bucket A — small/static tables (<=~20K rows) → **full load** (no
  delta/checkpoint/flow_start). Cheap and never misses a change.
- Bucket B — large tables whose ``[RdwTs]`` is trustworthy (``MIN(RdwTs)`` years
  ago << ``MAX``) → delta on ``[RdwTs]`` (a monotonic load timestamp that
  captures inserts *and* updates) + flow_start.
- Bucket C — large tables the RDW reloads daily (``MIN(RdwTs) ≈ MAX ≈ today``)
  → delta on a stable *business* timestamp + flow_start.

To (re)classify a table: run the diagnostic; if MIN(RdwTs) is recent (~today)
the table churns daily → Bucket C with a business timestamp (or A if small);
otherwise RdwTs is usable → Bucket B (or A if small). Each table below carries a
short reason inline.

flow_start (Buckets B+C): a ``dfm_sydax_<arc>_flow_start`` asset captures the
source ``SYSDATETIME()`` before the extract; the matching arc asset advances its
checkpoint to that value — gap-free and independent of the daily-churning
``RdwTs`` (which is exactly why flow_start, not a data-max checkpoint, is used).

Tables with a nullable ERP-stable merge key keep a ``select_from_expr`` that
filters the NULLs; the factory appends the delta filter via AND.

Full rationale + the diagnostic results live in the design spec
``docs/superpowers/specs/2026-06-25-sydax-dfm-delta-strategy-design.md`` (note:
that path is gitignored, so this docstring is the in-repo source of truth).
"""

import dagster as dg
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource, SydaxMSSQLResource
from DSI.connectors.extract_config import DfmExtractConfig
from DSI.connectors.factory import create_dfm_extraction_assets

_LOOKBACK = 3

# Greatest (most-recent) of the three Kreditortransaktion activity timestamps,
# NULL-safe and without a SQL Server 2022 GREATEST() dependency. Captures
# creation, approval, and settlement so post-creation updates aren't missed.
_KREDITORTRANS_DELTA = (
    "(SELECT MAX(v) FROM (VALUES "
    "([SidstUdlignetDato]),([GodkendtDato]),([Oprettelsestidspunkt])) AS g(v))"
)


def _arc_checkpoint_name(target_table: str) -> str:
    """Map an ext-side target_table to the arc-side name used for DFM state keys.

    ext.* is source-prefix orphaned for 8 SydAX tables (sydax.py is the live ext
    writer under the suffix-style name; see sydax_archive_configs._EXT_ORPHAN_SOURCE
    for the arc-side counterpart) — arc keeps its clean name, so the checkpoint-
    owner key and flow_start DFMState row name (both keyed off the arc name) must
    strip the ``_legacy_orphan`` suffix even though the ext extract target keeps it.
    """
    return target_table.removesuffix("_legacy_orphan")


def _checkpoint_for(target_table: str) -> tuple[str, str]:
    # target ext.sydax_<x>  --> checkpoint from arc.sydax_<x>
    return ("arc", _arc_checkpoint_name(target_table))


def sydax_flow_start_dfm_table(target_table: str) -> str:
    """DFMState row name a flow_start asset writes / the arc config reads.

    Must match ArchiveMergeConfig.flow_start_key on the arc side; the round-trip
    is asserted in tests/unit/test_sydax_flow_start.py. Keyed off the arc name
    (see _arc_checkpoint_name) even though the flow_start asset itself is named
    after the ext-side target_table.
    """
    return f"{_arc_checkpoint_name(target_table)}_flow_start"


def _null_filter(schema: str, table: str, key: str) -> str:
    return f"SELECT * FROM [{schema}].[{table}] WHERE [{key}] IS NOT NULL"


# ---------------------------------------------------------------------------
# Bucket A — small/static → full load
# (schema, table, arc_target, nullable_key_or_None)
# ---------------------------------------------------------------------------
# Row counts are the 2026-06-25 diagnostic snapshot — the reason each is full-load.
_FULL_LOAD = [
    ("Finans", "Dranst", "sydax_dranst_legacy_orphan", "ref_RECID"),  # ~7 rows, static
    ("Finans", "Art", "sydax_art_legacy_orphan", "RECID"),  # ~87 rows, static
    (
        "Finans",
        "Finanstransaktionstype",
        "sydax_finanstransaktionstype_legacy_orphan",
        None,
    ),  # ~96, static
    ("Finans", "GrupperingAutoriseret", "sydax_gruppering_autoriseret", "ref_RECID"),  # ~156
    ("Finans", "Funktion", "sydax_funktion_legacy_orphan", "ref_RECID"),  # ~251
    ("Finans", "Hovedkonto", "sydax_hovedkonto_legacy_orphan", "ref_RECID"),  # ~1.5K
    ("Finans", "Gruppering", "sydax_gruppering_legacy_orphan", "RECID"),  # ~3.6K
    ("Finans", "Afdeling", "sydax_afdeling_legacy_orphan", "RECID"),  # ~13K (RdwTs ok, but small)
    # ~20K, daily churn + no business ts -> full load is the only safe option
    ("Finans", "OmkostningsstedHierarki", "sydax_omkostningssted_hierarki", None),
    ("Finans", "Budgetmodel", "sydax_budgetmodel", "BudgetmodelId"),  # small, static
    ("Finans", "BudgetpostStatus", "sydax_budgetpoststatus", None),  # tiny status table
    ("Finans", "BudgetbeløbType", "sydax_budgetbeloebtype", None),  # tiny status table
]

_FULL_LOAD_CONFIGS = [
    DfmExtractConfig(
        source_schema=schema,
        source_table=table,
        target_table=arc,
        is_full_load=True,
        checkpoint_from=None,
        select_from_expr=(_null_filter(schema, table, key) if key else None),
        description=f"SydAX {schema}.{table} full load for arc.{arc} (small/static reference).",
    )
    for schema, table, arc, key in _FULL_LOAD
]

# ---------------------------------------------------------------------------
# Buckets B & C — delta + flow_start
# (schema, table, arc_target, delta_expression, nullable_key_or_None)
# ---------------------------------------------------------------------------
_DELTA = [
    # Bucket B — RdwTs spread over years (2019/2020 -> today) => trustworthy
    # monotonic load timestamp; catches inserts and updates.
    (
        "Kreditor",
        "Kreditor",
        "sydax_kreditor_legacy_orphan",
        "[RdwTs]",
        None,
    ),  # ~800K, mutable master
    ("Finans", "Finanskladde", "sydax_finanskladde", "[RdwTs]", None),  # ~2.1M
    (
        "Finans",
        "FinanskontoOmkostningssted",
        "sydax_finanskonto_omkostningssted",
        "[RdwTs]",
        None,
    ),  # ~2.5M
    ("Finans", "Finanskonto", "sydax_finanskonto", "[RdwTs]", None),  # ~2.5M
    # ~22M
    ("Finans", "Finanskladdelinje", "sydax_finanskladdelinje", "[RdwTs]", "ref_LedgertransId"),
    # RdwTs trustworthy (diagnostic confirmed) -> delta on RdwTs (catches inserts + edits).
    ("Finans", "Budgetpost", "sydax_budgetpost", "[RdwTs]", "ref_LedgerbudgetId"),
    # Bucket C — RdwTs/CreatedOn all = today (RDW reloads daily) => RdwTs unusable;
    # use a stable business timestamp instead.
    # ~16.7M, mutable (settlement updates) -> greatest-of activity timestamps
    (
        "Kreditor",
        "Kreditortransaktion",
        "sydax_kreditortransaktion",
        _KREDITORTRANS_DELTA,
        "ref_VendTransId",
    ),
    # ~64M, append-only postings -> creation timestamp is the reliable delta
    (
        "Finans",
        "Finanspostering",
        "sydax_finanspostering",
        "[Oprettelsestidspunkt]",
        "ref_LedgertransId",
    ),
]

_DELTA_CONFIGS = [
    DfmExtractConfig(
        source_schema=schema,
        source_table=table,
        target_table=arc,
        delta_expression=delta,
        checkpoint_type="datetime",
        lookback_days=_LOOKBACK,
        checkpoint_from=_checkpoint_for(arc),
        select_from_expr=(_null_filter(schema, table, key) if key else None),
        description=f"SydAX {schema}.{table} delta extract ({delta}) for arc.{arc}.",
    )
    for schema, table, arc, delta, key in _DELTA
]

# ---------------------------------------------------------------------------
# Bridge (Bucket C) — JOIN to Finanspostering to expose ref_LedgertransId.
# Delta on the joined post.[Oprettelsestidspunkt]; the bridge has no own
# business timestamp, and arc stores no Oprettelsestidspunkt, so the checkpoint
# is owned entirely by flow_start.
# ---------------------------------------------------------------------------
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
    target_table="sydax_finanspostering_kreditortrans_relation",
    delta_expression="post.[Oprettelsestidspunkt]",
    checkpoint_type="datetime",
    lookback_days=_LOOKBACK,
    checkpoint_from=("arc", "sydax_finanspostering_kreditortrans_relation"),
    select_from_expr=_BRIDGE_SELECT,
    description=(
        "SydAX bridge (FinansposteringKreditortransRelation) delta extract for "
        "arc.sydax_finanspostering_kreditortrans_relation. JOINs Finanspostering "
        "to expose ref_LedgertransId and delta on post.[Oprettelsestidspunkt]."
    ),
)

_FLOW_START_CONFIGS = _DELTA_CONFIGS + [_BRIDGE_CONFIG]

SYDAX_DFM_EXTRACT_CONFIGS: list[DfmExtractConfig] = _FULL_LOAD_CONFIGS + _FLOW_START_CONFIGS


# ---------------------------------------------------------------------------
# flow_start assets (Buckets B+C) — capture source SYSDATETIME before extract
# ---------------------------------------------------------------------------
def _build_sydax_flow_start_asset(target_table: str):
    """Build a dfm_sydax_<arc>_flow_start asset for a SydAX source table.

    target_table is the source-prefixed arc name (e.g. "sydax_finanspostering").
    """
    dfm_table = sydax_flow_start_dfm_table(target_table)
    arc_target = _arc_checkpoint_name(target_table)
    asset_name = f"dfm_{dfm_table}"

    @dg.asset(
        name=asset_name,
        group_name="sydax_archive",
        compute_kind="mssql",
        deps=["staging_infra_dfm"],
        tags={"domain": "erp", "layer": "dfm", "source": "sydax"},
        description=(
            f"Capture SYSDATETIME() from the SydAX source as the flow-start "
            f"timestamp before ext.{target_table} extracts. arc.{arc_target} "
            f"uses it as its new checkpoint, ensuring gap-free coverage."
        ),
    )
    def _flow_start_asset(
        context: dg.AssetExecutionContext,
        sydax_mssql: SydaxMSSQLResource,
        mssql_target_staging: MSSQLResource,
    ) -> dg.MaterializeResult:
        dfm = DfmManager(mssql_target_staging, context)
        run_ctx = dfm.start_dfm_run(
            "dfm", dfm_table, dagster_run_id=context.run_id, mode="flow_start"
        )
        try:
            with sydax_mssql.get_connection() as src:
                cursor = src.cursor()
                try:
                    cursor.execute("SELECT CONVERT(NVARCHAR(50), SYSDATETIME(), 121)")
                    flow_ts = cursor.fetchone()[0]
                finally:
                    cursor.close()
            dfm.set_checkpoint("dfm", dfm_table, value=flow_ts, mode="flow_start")
            dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=flow_ts, rows_affected=0)
            return dg.MaterializeResult(
                metadata={"flow_start": flow_ts, "source": "sydax_sysdatetime"}
            )
        except Exception as exc:
            dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
            raise

    return _flow_start_asset


sydax_flow_start_assets = [
    _build_sydax_flow_start_asset(cfg.target_table) for cfg in _FLOW_START_CONFIGS
]


# ---------------------------------------------------------------------------
# Extract assets — full-load tables batched; flow_start tables wired per-table
# to depend on their flow_start asset so it runs first.
# ---------------------------------------------------------------------------
_RETRY = dg.RetryPolicy(max_retries=2, delay=30)

sydax_archive_extract_assets = create_dfm_extraction_assets(
    configs=_FULL_LOAD_CONFIGS,
    source_resource_key="sydax_mssql",
    source_resource_type=SydaxMSSQLResource,
    source_system="SydAX",
    group_name="sydax_archive",
    domain_tag="erp",
    dialect="mssql",
    retry_policy=_RETRY,
)

for _cfg in _FLOW_START_CONFIGS:
    _flow_start_asset_name = f"dfm_{sydax_flow_start_dfm_table(_cfg.target_table)}"
    sydax_archive_extract_assets += create_dfm_extraction_assets(
        configs=[_cfg],
        source_resource_key="sydax_mssql",
        source_resource_type=SydaxMSSQLResource,
        source_system="SydAX",
        group_name="sydax_archive",
        domain_tag="erp",
        dialect="mssql",
        deps=[_flow_start_asset_name],
        retry_policy=_RETRY,
    )

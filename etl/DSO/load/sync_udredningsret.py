# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSO Udredningsret Sync Assets

Syncs the udredningsret dim/fct tables from staging (DSA) to the DSO reporting
database via the linked server, using watermark deltas on dw_timestamp_update.

What deliberately does NOT sync:

* ``map.Helbredsforloeb``, ``map.Udredningsforloeb``,
  ``map.UdredningsretKlassifikation``, ``map.Forloebsmarkoer`` and
  ``map.UdredningsretMarkoer`` — staging-internal IDENTITY key minting.
  ``dso_infra_udredningsret`` creates only the dim/fct tables for the same
  reason. ``map.Helbredsforloeb`` is the one whose absence is visible: the
  facts' ``dw_ek_Helbredsforloeb`` resolves to no dimension on DSO (it is
  FK-None in the lineage, and DQA gives the role a sentinel rate but no
  referential check). A helbredsforloeb is a grouping key, not a described
  entity — the readable identifier travels on the facts as
  ``helbredsforloeb_id``.
* ``arc.udredningsret_mont`` / ``ext.udredningsret*`` / ``meta.UdredningsretLeverance``
  — pipeline internals. The archive holds the raw CPR.

What reaches DSO carries no CPR, raw or hashed. The facts carry dw_ek_Borger
(never raw CPR), the three dimensions carry no citizen key at all, and raw CPR
never leaves staging — it is resolved to dw_ek_Borger inside pre-staging and
never written to a stg table (decision U10, spec section 5).

"No borger key" alone would not have been enough. clean_code_Udredningsforloeb
carries helbredsforloeb_noegle verbatim, and that key is a SHA2_256 over the
citizen's CPR — a stable pseudonym that anyone knowing the hash inputs can
recompute. It is therefore excluded from the sync on both tables that declare it
(``exclude_columns`` in UDREDNINGSRET_SYNC_CONFIGS, decision U18): the column
exists on DSO but stays NULL, and dw_ek_Udredningsforloeb is the join key there.
clean_code_UdredningsretMarkoer is unaffected — markoer_noegle is a fresh hash OF
the helbredsforloeb key, not the key itself.

Unlike DRG, the egenskaber dimension is exposed on DSO as a plain pass-through
view: with the clean_code held back it carries only the delivery's own ids, the
two SOR unit codes, region codes, speciale and the reporting period, none of
which is person-referable. Row-level DPA restriction over the facts, where a
consumer contract calls for it, belongs to the separate security-views repo, as
it does for fct.DRGKontakt.

Dependency shape: every sync waits for the DSA asset that fills its table, and
for the syncs of the tables its FKs point at (RI ordering on DSO). The three
dimensions sync first, then fct.Udredningsforloeb (it references the
klassifikation dim, shares its PK with the egenskaber dim, and points at the
shared dim.Borger / dim.Organisation), and fct.UdredningsretMarkoer last — it
references the forloeb fact itself. dim.Dato is seeded on DSO by its
shared-dimensions DDL and needs no sync.

The sync moves all rows, including soft-deleted ones: retirement is the model's
retraction audit trail (spec section 6), and that trail is read on fct.* on DSO.
The vw_fct.* views on top filter dw_is_deleted = 0, so an ordinary consumer gets
the live population without knowing the convention (decision U16).
"""

import dagster as dg
from core.resources import MSSQLResource
from DSO.load.sync_configs import UDREDNINGSRET_SYNC_CONFIGS
from DSO.load.sync_dim import _materialize_sync_result


def _build_sync_asset(config_key: str, dim_syncs: tuple[str, ...] = ()):
    """Build one DSO-sync asset for the udredningsret table named ``config_key``.

    The DSA asset that fills the table shares that name, so ``config_key``
    doubles as the upstream dependency; ``dim_syncs`` are the extra DSO
    dim/fact syncs the table's FKs point at (for RI ordering on DSO).
    """
    config = UDREDNINGSRET_SYNC_CONFIGS[config_key]

    @dg.asset(
        name=f"dso_sync_{config_key.lower()}",
        group_name="udredningsret_dso_sync",
        compute_kind="mssql",
        deps=[config_key, "dso_infra_udredningsret", *dim_syncs],
        tags={"domain": "udredningsret", "layer": "dso_sync", "source": "udredningsret"},
        description=(
            f"Sync {config.schema}.{config.table} from staging to DSO "
            f"(watermark on dw_timestamp_update; CPR-free)."
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_dso: MSSQLResource,
        mssql_source_staging: MSSQLResource,
    ) -> dg.MaterializeResult:
        return _materialize_sync_result(
            context,
            mssql_target_dso,
            mssql_source_staging,
            config_key,
            UDREDNINGSRET_SYNC_CONFIGS,
        )

    return _asset


# Built at the call site (no index indirection). Ordered so a table's FK
# targets are on DSO first: dims -> forloeb fact -> markoer fact. Consumers use
# the list; the individual assets are reached by their Dagster names (the dep
# strings), never as module attributes.
all_udredningsret_dso_sync_assets = [
    _build_sync_asset("dim_UdredningsretKlassifikation"),
    _build_sync_asset("dim_UdredningsretEgenskaber"),
    _build_sync_asset("dim_Forloebsmarkoer"),
    _build_sync_asset(
        "fct_Udredningsforloeb",
        (
            "dso_sync_dim_udredningsretklassifikation",
            "dso_sync_dim_udredningsretegenskaber",
            "dso_sync_dim_borger",
            "dso_sync_dim_organisation",
        ),
    ),
    _build_sync_asset(
        "fct_UdredningsretMarkoer",
        (
            "dso_sync_fct_udredningsforloeb",
            "dso_sync_dim_forloebsmarkoer",
            "dso_sync_dim_borger",
            "dso_sync_dim_organisation",
        ),
    ),
]

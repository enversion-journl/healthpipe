# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Integration: delta-driven Vare/ILSVare/bridge flow.

Invariants: (a) no orphan keys in the bridge; (b) seed row intact;
(c) dim.Leverandoervare source flags + name coalesce; (d) a second run with no archive
changes is a no-op (delta empty -> no timestamp churn); (e) the scoped
soft-delete semantics from spec §5: only pairs of FULL-HISTORY entities are
marked, counterpart entities' other pairs stay untouched.
"""

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.erp,
    pytest.mark.real_resources,
    pytest.mark.order(3),
]

from tests.utils.materialize_helpers import (
    materialize_assets_with_dependencies,
)

DELTA_CHAIN = [
    "erp/pre_stg_Leverandoervare_ILSVare_delta_entiteter",
    "erp/stg_dim_Leverandoervare",
    "erp/stg_dim_ILSVare",
    "erp/stg_dim_UNSPSC",
    "erp/stg_bridge_Leverandoervare_ILSVare",
    "map_Leverandoervare",
    "map_ILSVare",
    "map_UNSPSC",
    "dim_Leverandoervare",
    "dim_ILSVare",
    "bridge_LeverandoervareILSVare",
]


def _q(resource, sql):
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()


def _exec(resource, sql):
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            conn.commit()
        finally:
            cursor.close()


def test_no_orphan_keys(erp_materialized_session, real_mssql_target_resource):
    rows = _q(
        real_mssql_target_resource,
        """
        SELECT
            SUM(CASE WHEN v.dw_ek_Leverandoervare IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN i.dw_ek_ILSVare IS NULL THEN 1 ELSE 0 END)
        FROM [bridge].[LeverandoervareILSVare] b
        LEFT JOIN [dim].[Leverandoervare] v ON v.dw_ek_Leverandoervare = b.dw_ek_Leverandoervare
        LEFT JOIN [dim].[ILSVare] i ON i.dw_ek_ILSVare = b.dw_ek_ILSVare
        """,
    )
    orphan_vare, orphan_ilsvare = rows[0]
    assert (orphan_vare or 0) == 0
    assert (orphan_ilsvare or 0) == 0


def test_seed_row_intact(erp_materialized_session, real_mssql_target_resource):
    rows = _q(
        real_mssql_target_resource,
        "SELECT dw_is_deleted FROM [bridge].[LeverandoervareILSVare] WHERE dw_ek_Leverandoervare = -1 AND dw_ek_ILSVare = -1",
    )
    assert rows, "(-1,-1) seed row missing"
    assert rows[0][0] == 0, "seed row must not be soft-deleted"


def test_dim_leverandoervare_source_flags_and_name_coalesce(
    erp_materialized_session, real_mssql_target_resource
):
    # Ingen raekke maa have begge flags = 0 (enhver entitet kommer fra mindst en kilde)
    rows = _q(
        real_mssql_target_resource,
        """
        SELECT
            SUM(CASE WHEN dw_source_mysupply = 0 AND dw_source_ils = 0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN varenavn IS NULL
                     AND varenavn_mysupply IS NOT NULL
                THEN 1 ELSE 0 END)
        FROM [dim].[Leverandoervare] WHERE dw_ek_Leverandoervare <> -1
        """,
    )
    flagless, broken_coalesce = rows[0]
    assert (flagless or 0) == 0, "every row must carry at least one source flag"
    assert (broken_coalesce or 0) == 0, "varenavn must coalesce the source names"


def test_second_delta_run_is_noop(
    erp_materialized_session,
    dagster_instance,
    real_mssql_target_resource,
    real_mysupply_mssql_resource,
    real_sydax_mssql_resource,
    real_ils_mssql_resource,
):
    before = _q(
        real_mssql_target_resource,
        """
        SELECT
            (SELECT MAX(dw_timestamp_update) FROM [dim].[Leverandoervare]),
            (SELECT MAX(dw_timestamp_update) FROM [bridge].[LeverandoervareILSVare]),
            (SELECT COUNT(*) FROM [bridge].[LeverandoervareILSVare]),
            (SELECT COUNT(*) FROM [dim].[Leverandoervare])
        """,
    )[0]
    resources = {
        "mssql_target_staging": real_mssql_target_resource,
        "mysupply_mssql": real_mysupply_mssql_resource,
        "sydax_mssql": real_sydax_mssql_resource,
        "ils_mssql": real_ils_mssql_resource,
    }
    result = materialize_assets_with_dependencies(
        instance=dagster_instance, asset_keys=DELTA_CHAIN, resources=resources
    )
    assert result.success
    after = _q(
        real_mssql_target_resource,
        """
        SELECT
            (SELECT MAX(dw_timestamp_update) FROM [dim].[Leverandoervare]),
            (SELECT MAX(dw_timestamp_update) FROM [bridge].[LeverandoervareILSVare]),
            (SELECT COUNT(*) FROM [bridge].[LeverandoervareILSVare]),
            (SELECT COUNT(*) FROM [dim].[Leverandoervare])
        """,
    )[0]
    assert after == before, "no archive changes -> delta run must not touch any row"


def test_scoped_soft_delete_semantics(
    erp_materialized_session,
    dagster_instance,
    real_mssql_target_resource,
    real_mysupply_mssql_resource,
    real_sydax_mssql_resource,
    real_ils_mssql_resource,
):
    """Spec §5's example: touch ONE ILSVare entity in the archive; only that
    entity's vanished pairs may be marked; counterpart entities' other pairs
    stay untouched."""
    # Find et vnr med >= 1 aktivt par, hvis vare-modpart ogsaa har par til ANDRE vnr
    rows = _q(
        real_mssql_target_resource,
        """
        SELECT TOP 1 b.dw_ek_ILSVare, b.dw_ek_Leverandoervare
        FROM [bridge].[LeverandoervareILSVare] b
        WHERE b.dw_is_deleted = 0 AND b.dw_ek_Leverandoervare <> -1
          AND EXISTS (
              SELECT 1 FROM [bridge].[LeverandoervareILSVare] b2
              WHERE b2.dw_ek_Leverandoervare = b.dw_ek_Leverandoervare
                AND b2.dw_ek_ILSVare <> b.dw_ek_ILSVare
                AND b2.dw_is_deleted = 0
          )
        ORDER BY b.dw_ek_ILSVare
        """,
    )
    if not rows:
        pytest.skip("test data has no vare with pairs to 2+ ILS items")
    target_ilsvare, counterpart_vare = rows[0]

    # Rør KUN den ene ILSVare-entitet i arkivet (timestamp-bump = delta-medlemskab)
    vnr_raw = _q(
        real_mssql_target_resource,
        f"""
        SELECT TOP 1 v.vnr FROM [arc].[ils_vare] v
        INNER JOIN [map].[ILSVare] mi
            ON mi.clean_code_ILSVare = 'ILSVARE_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(v.vnr AS NVARCHAR(100)), ' ', ''))))
        WHERE mi.dw_ek_ILSVare = {int(target_ilsvare)}
        """,
    )
    if not vnr_raw:
        pytest.skip("target ILSVare has no arc.ils_vare row to touch")
    _exec(
        real_mssql_target_resource,
        f"""
        UPDATE [arc].[ils_vare] SET dw_timestamp_update = SYSDATETIME()
        WHERE CAST(vnr AS NVARCHAR(100)) = '{str(vnr_raw[0][0]).replace("'", "''")}'
        """,
    )

    counterpart_pairs_before = _q(
        real_mssql_target_resource,
        f"""
        SELECT dw_ek_ILSVare, dw_is_deleted FROM [bridge].[LeverandoervareILSVare]
        WHERE dw_ek_Leverandoervare = {int(counterpart_vare)} AND dw_ek_ILSVare <> {int(target_ilsvare)}
        """,
    )

    resources = {
        "mssql_target_staging": real_mssql_target_resource,
        "mysupply_mssql": real_mysupply_mssql_resource,
        "sydax_mssql": real_sydax_mssql_resource,
        "ils_mssql": real_ils_mssql_resource,
    }
    result = materialize_assets_with_dependencies(
        instance=dagster_instance, asset_keys=DELTA_CHAIN, resources=resources
    )
    assert result.success

    # Parret (counterpart_vare, target_ilsvare) findes stadig i kilderne -> aktivt
    active = _q(
        real_mssql_target_resource,
        f"""
        SELECT dw_is_deleted FROM [bridge].[LeverandoervareILSVare]
        WHERE dw_ek_Leverandoervare = {int(counterpart_vare)} AND dw_ek_ILSVare = {int(target_ilsvare)}
        """,
    )
    assert active and active[0][0] == 0, "existing pair must be re-upserted active"

    # Modpartens OEVRIGE par er uroerte (ikke marked — de var uden for scope)
    counterpart_pairs_after = _q(
        real_mssql_target_resource,
        f"""
        SELECT dw_ek_ILSVare, dw_is_deleted FROM [bridge].[LeverandoervareILSVare]
        WHERE dw_ek_Leverandoervare = {int(counterpart_vare)} AND dw_ek_ILSVare <> {int(target_ilsvare)}
        """,
    )
    assert sorted(map(tuple, counterpart_pairs_after)) == sorted(
        map(tuple, counterpart_pairs_before)
    ), "counterpart's other pairs must be untouched by the scoped soft-delete"

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension table asset for Organisation (SOR only).

Populates dim.Organisation with Type 1 SCD, sourced from fhir.Organization
(attributes) + ref.Organisation (the INT surrogate dw_ek_Organisation).

dw_ek_Organisation comes from ref.Organisation; the self-referencing parent
(dw_ek_Organisation_parent) is resolved by joining fhir.Organization.part_of_id
back to ref.Organisation. Pass 2 unfolds that parent chain into kode_niveau /
navn_niveau columns (niveau1 = root).

Type 1: attributes, parent, dw_is_deleted, and hierarchy columns are overwritten
on change. Rows are never physically deleted — an org that leaves SOR keeps its
dim row so historical fact FKs still resolve. dw_is_deleted mirrors
ref.Organisation. The "Unknown" sentinel (-1) is seeded by the DDL.
"""

import dagster as dg
from core.resources import MSSQLResource

_HIERARCHY_CAP = 9

_WALK_CTE = f"""
WITH walk AS (
    SELECT
        o.dw_ek_Organisation AS leaf_ek,
        o.kode AS node_kode,
        o.navn AS node_navn,
        o.dw_ek_Organisation_parent AS parent_ek,
        1 AS dist,
        CAST('/' + CAST(o.dw_ek_Organisation AS varchar(20)) + '/' AS varchar(400)) AS path
    FROM [dim].[Organisation] AS o
    WHERE o.dw_ek_Organisation <> -1

    UNION ALL

    SELECT
        w.leaf_ek,
        p.kode,
        p.navn,
        p.dw_ek_Organisation_parent,
        w.dist + 1,
        CAST(w.path + CAST(p.dw_ek_Organisation AS varchar(20)) + '/' AS varchar(400))
    FROM walk AS w
    INNER JOIN [dim].[Organisation] AS p
        ON p.dw_ek_Organisation = w.parent_ek
    WHERE w.parent_ek IS NOT NULL
      AND w.parent_ek <> -1
      AND p.dw_ek_Organisation <> -1
      AND w.dist < {_HIERARCHY_CAP}
      AND CHARINDEX('/' + CAST(p.dw_ek_Organisation AS varchar(20)) + '/', w.path) = 0
)"""

# A chain is only trustworthy when its deepest node has no parent left to follow —
# that node is the real root and belongs on niveau1. Any chain whose deepest node
# STILL has a parent stopped early, and niveau1 would name that node as the root
# when it is not. Three ways that happens, split so the failure says which:
#   dist >= cap  -> hit the depth cap; widen the cap and the niveau columns
#   dist <  cap  -> a cycle (blocked by the CHARINDEX guard) or a
#                   dw_ek_Organisation_parent pointing at a key with no dim row
_HIERARCHY_STATS_SQL = (
    _WALK_CTE
    + f""",
ends AS (
    SELECT
        leaf_ek,
        dist,
        parent_ek,
        ROW_NUMBER() OVER (PARTITION BY leaf_ek ORDER BY dist DESC) AS rn
    FROM walk
)
SELECT
    ISNULL(SUM(CASE
        WHEN rn = 1 AND parent_ek IS NOT NULL AND parent_ek <> -1
             AND dist >= {_HIERARCHY_CAP} THEN 1
        ELSE 0
    END), 0) AS truncated_at_cap,
    ISNULL(SUM(CASE
        WHEN rn = 1 AND parent_ek IS NOT NULL AND parent_ek <> -1
             AND dist < {_HIERARCHY_CAP} THEN 1
        ELSE 0
    END), 0) AS unresolved_before_cap,
    ISNULL(MAX(dist), 0) AS max_dist
FROM ends;
"""
)

_HIERARCHY_SQL = (
    _WALK_CTE
    + """,
chain AS (
    SELECT
        leaf_ek,
        node_kode,
        node_navn,
        dist,
        MAX(dist) OVER (PARTITION BY leaf_ek) AS max_dist
    FROM walk
),
pivoted AS (
    SELECT
        leaf_ek,
        MAX(max_dist) AS hierarki_niveau,
        MAX(CASE WHEN max_dist - dist + 1 = 1 THEN node_kode END) AS kode_niveau1,
        MAX(CASE WHEN max_dist - dist + 1 = 1 THEN node_navn END) AS navn_niveau1,
        MAX(CASE WHEN max_dist - dist + 1 = 2 THEN node_kode END) AS kode_niveau2,
        MAX(CASE WHEN max_dist - dist + 1 = 2 THEN node_navn END) AS navn_niveau2,
        MAX(CASE WHEN max_dist - dist + 1 = 3 THEN node_kode END) AS kode_niveau3,
        MAX(CASE WHEN max_dist - dist + 1 = 3 THEN node_navn END) AS navn_niveau3,
        MAX(CASE WHEN max_dist - dist + 1 = 4 THEN node_kode END) AS kode_niveau4,
        MAX(CASE WHEN max_dist - dist + 1 = 4 THEN node_navn END) AS navn_niveau4,
        MAX(CASE WHEN max_dist - dist + 1 = 5 THEN node_kode END) AS kode_niveau5,
        MAX(CASE WHEN max_dist - dist + 1 = 5 THEN node_navn END) AS navn_niveau5,
        MAX(CASE WHEN max_dist - dist + 1 = 6 THEN node_kode END) AS kode_niveau6,
        MAX(CASE WHEN max_dist - dist + 1 = 6 THEN node_navn END) AS navn_niveau6,
        MAX(CASE WHEN max_dist - dist + 1 = 7 THEN node_kode END) AS kode_niveau7,
        MAX(CASE WHEN max_dist - dist + 1 = 7 THEN node_navn END) AS navn_niveau7,
        MAX(CASE WHEN max_dist - dist + 1 = 8 THEN node_kode END) AS kode_niveau8,
        MAX(CASE WHEN max_dist - dist + 1 = 8 THEN node_navn END) AS navn_niveau8,
        MAX(CASE WHEN max_dist - dist + 1 = 9 THEN node_kode END) AS kode_niveau9,
        MAX(CASE WHEN max_dist - dist + 1 = 9 THEN node_navn END) AS navn_niveau9
    FROM chain
    GROUP BY leaf_ek
)
UPDATE d
SET
    d.hierarki_niveau = p.hierarki_niveau,
    d.kode_niveau1 = p.kode_niveau1,
    d.navn_niveau1 = p.navn_niveau1,
    d.kode_niveau2 = p.kode_niveau2,
    d.navn_niveau2 = p.navn_niveau2,
    d.kode_niveau3 = p.kode_niveau3,
    d.navn_niveau3 = p.navn_niveau3,
    d.kode_niveau4 = p.kode_niveau4,
    d.navn_niveau4 = p.navn_niveau4,
    d.kode_niveau5 = p.kode_niveau5,
    d.navn_niveau5 = p.navn_niveau5,
    d.kode_niveau6 = p.kode_niveau6,
    d.navn_niveau6 = p.navn_niveau6,
    d.kode_niveau7 = p.kode_niveau7,
    d.navn_niveau7 = p.navn_niveau7,
    d.kode_niveau8 = p.kode_niveau8,
    d.navn_niveau8 = p.navn_niveau8,
    d.kode_niveau9 = p.kode_niveau9,
    d.navn_niveau9 = p.navn_niveau9,
    d.dw_timestamp_update = GETDATE()
FROM [dim].[Organisation] AS d
INNER JOIN pivoted AS p
    ON p.leaf_ek = d.dw_ek_Organisation
WHERE EXISTS (
    SELECT
        d.hierarki_niveau,
        d.kode_niveau1, d.navn_niveau1,
        d.kode_niveau2, d.navn_niveau2,
        d.kode_niveau3, d.navn_niveau3,
        d.kode_niveau4, d.navn_niveau4,
        d.kode_niveau5, d.navn_niveau5,
        d.kode_niveau6, d.navn_niveau6,
        d.kode_niveau7, d.navn_niveau7,
        d.kode_niveau8, d.navn_niveau8,
        d.kode_niveau9, d.navn_niveau9
    EXCEPT
    SELECT
        p.hierarki_niveau,
        p.kode_niveau1, p.navn_niveau1,
        p.kode_niveau2, p.navn_niveau2,
        p.kode_niveau3, p.navn_niveau3,
        p.kode_niveau4, p.navn_niveau4,
        p.kode_niveau5, p.navn_niveau5,
        p.kode_niveau6, p.navn_niveau6,
        p.kode_niveau7, p.navn_niveau7,
        p.kode_niveau8, p.navn_niveau8,
        p.kode_niveau9, p.navn_niveau9
);
"""
)


@dg.asset(
    name="dim_Organisation",
    deps=["staging_infra_organisation", "ref_organisation"],
    group_name="dwh_organisation",
    compute_kind="mssql",
    description=(
        "Populate dim.Organisation (Type 1 SCD, SOR only) from fhir.Organization "
        "+ ref.Organisation, then unfold the parent hierarchy"
    ),
)
def dim_Organisation(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Populate dim.Organisation (Type 1 SCD) from fhir.Organization + ref.Organisation."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info(
                "Merging dim.Organisation from fhir.Organization + ref.Organisation..."
            )

            cursor.execute(
                """
                UPDATE d
                SET d.dw_is_deleted = r.dw_is_deleted,
                    d.dw_timestamp_update = GETDATE()
                FROM [dim].[Organisation] d
                JOIN [ref].[Organisation] r
                    ON r.dw_ek_Organisation = d.dw_ek_Organisation
                WHERE d.dw_is_deleted <> r.dw_is_deleted;
            """
            )
            flags_synced = cursor.rowcount

            cursor.execute(
                """
                MERGE INTO [dim].[Organisation] AS target
                USING (
                    SELECT
                        r.dw_ek_Organisation,
                        r.sor_code                                  AS kode,
                        r.dw_is_deleted,
                        COALESCE(rp.dw_ek_Organisation, -1)         AS dw_ek_Organisation_parent,
                        rp.sor_code                                 AS parent_kode,
                        f.name                                      AS navn,
                        CASE WHEN LOWER(f.active) = 'true' THEN 1 ELSE 0 END AS aktiv
                    FROM [ref].[Organisation] r
                    JOIN [fhir].[Organization] f
                        ON f.organization_id = r.organization_id
                    LEFT JOIN [ref].[Organisation] rp
                        ON rp.organization_id = f.part_of_id
                ) AS source
                ON target.dw_ek_Organisation = source.dw_ek_Organisation
                WHEN MATCHED AND (
                       ISNULL(target.kode, '') <> ISNULL(source.kode, '')
                    OR target.dw_is_deleted <> source.dw_is_deleted
                    OR ISNULL(target.dw_ek_Organisation_parent, -1)
                        <> ISNULL(source.dw_ek_Organisation_parent, -1)
                    OR ISNULL(target.parent_kode, '') <> ISNULL(source.parent_kode, '')
                    OR ISNULL(target.navn, N'') <> ISNULL(source.navn, N'')
                    OR ISNULL(target.aktiv, 0) <> ISNULL(source.aktiv, 0)
                ) THEN
                    UPDATE SET
                        kode = source.kode,
                        dw_is_deleted = source.dw_is_deleted,
                        dw_ek_Organisation_parent = source.dw_ek_Organisation_parent,
                        parent_kode = source.parent_kode,
                        navn = source.navn,
                        aktiv = source.aktiv,
                        dw_timestamp_update = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (dw_ek_Organisation, kode, dw_is_deleted,
                            dw_ek_Organisation_parent, parent_kode,
                            navn, aktiv,
                            dw_timestamp_insert, dw_timestamp_update)
                    VALUES (source.dw_ek_Organisation, source.kode, source.dw_is_deleted,
                            source.dw_ek_Organisation_parent, source.parent_kode,
                            source.navn, source.aktiv,
                            GETDATE(), GETDATE())
                OUTPUT $action;
            """
            )

            merge_results = cursor.fetchall()
            inserted = sum(1 for r in merge_results if r[0] == "INSERT")
            updated = sum(1 for r in merge_results if r[0] == "UPDATE")

            context.log.info("Unfolding dim.Organisation SOR hierarchy...")
            cursor.execute(_HIERARCHY_STATS_SQL)
            truncated_at_cap, unresolved_before_cap, max_dist = cursor.fetchone()
            context.log.info(
                f"dim.Organisation hierarchy: max_dist={max_dist} "
                f"(cap {_HIERARCHY_CAP}), truncated_at_cap={truncated_at_cap}, "
                f"unresolved_before_cap={unresolved_before_cap}"
            )
            if truncated_at_cap or unresolved_before_cap:
                causes = []
                if truncated_at_cap:
                    causes.append(
                        f"{truncated_at_cap} chain(s) hit the depth cap of "
                        f"{_HIERARCHY_CAP} — widen the cap and the "
                        f"kode_niveau/navn_niveau columns"
                    )
                if unresolved_before_cap:
                    causes.append(
                        f"{unresolved_before_cap} chain(s) stopped before the cap — "
                        f"either a parent cycle, or a dw_ek_Organisation_parent "
                        f"pointing at a key with no dim.Organisation row"
                    )
                raise dg.Failure(
                    description=(
                        "dim.Organisation hierarchy: the deepest node of some chains "
                        "still has a parent, so kode_niveau1 would name it as the root "
                        f"when it is not. Current max walk is {max_dist}. "
                        + "; ".join(causes)
                        + "."
                    ),
                    metadata={
                        "truncated_at_cap": truncated_at_cap,
                        "unresolved_before_cap": unresolved_before_cap,
                        "max_dist": max_dist,
                        "cap": _HIERARCHY_CAP,
                    },
                )
            cursor.execute(_HIERARCHY_SQL)
            hierarchy_updated = cursor.rowcount

            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [dim].[Organisation]")
            total_rows = cursor.fetchone()[0]
        finally:
            cursor.close()

    context.log.info(
        f"dim.Organisation: {inserted} inserted, {updated} updated, "
        f"{flags_synced} flags synced from ref, {hierarchy_updated} hierarchy "
        f"rows refreshed, {total_rows} total"
    )

    return dg.MaterializeResult(
        metadata={
            "inserted": inserted,
            "updated": updated,
            "flags_synced": flags_synced,
            "hierarchy_updated": hierarchy_updated,
            "hierarchy_max_dist": max_dist,
            "hierarchy_truncated_at_cap": truncated_at_cap,
            "hierarchy_unresolved_before_cap": unresolved_before_cap,
            "total_rows": total_rows,
            "method": "sql_merge_from_ref",
        }
    )

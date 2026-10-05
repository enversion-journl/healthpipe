# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for MySupply allowance/charge (Gebyr) CTE transformations.

Validates the CTE-based SQL used in:
- stg_dim_Faktura_mysupply      (header → STRING_AGG → Rabat/Tillaeg '#'-delimited)
- stg_dim_Fakturalinje_mysupply (line   → STRING_AGG → Rabat/Tillaeg '#'-delimited)
- stg_fct_Faktura_mysupply      (header → SUM → gebyr_beloeb / rabatbeloeb)
- stg_fct_Fakturalinje_mysupply (line   → SUM → gebyr_beloeb / rabatbeloeb)

Approach: seed ext.* stub tables directly, run the production CTE SQL (imported from
the staging asset modules), assert on output. The full staging assets are NOT
materialised — only the allowance/charge CTE logic is exercised in isolation so each
test is fast and self-contained.
"""

from decimal import Decimal

import pytest
from DSA.SQL.staging.stg_dim_Faktura_mysupply import HAC_DIM_CTE_SQL
from DSA.SQL.staging.stg_dim_Fakturalinje_mysupply import LAC_DIM_CTE_SQL
from DSA.SQL.staging.stg_fct_Faktura_mysupply import (
    HAC_MEASURE_COLUMNS_SQL,
    HAC_SUM_CTE_SQL,
)
from DSA.SQL.staging.stg_fct_Fakturalinje_mysupply import (
    LAC_MEASURE_COLUMNS_SQL,
    LAC_SUM_CTE_SQL,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.erp,
    pytest.mark.real_resources,
]

# ═══════════════════════════════════════════════════════════════════════════════
# Seed data — Header allowance charges
# ═══════════════════════════════════════════════════════════════════════════════
# Tuples: (id, statistic_invoice_header_id, GebyrKategori,
#           GebyrKategoriIndikering, GebyrKategoriÅrsag,
#           GebyrKategoriTotal, GebyrKategoriFinanskonto)

HEADER_AC_ROWS = [
    # hdr=1000 — mixed rabat + tillaeg, multiple rows
    (1, 1000, "DIS", "false", "Discount A", Decimal("50"), "4000"),
    (2, 1000, "FRT", "true", "Freight", Decimal("25"), "5000"),
    (3, 1000, "VOL", "false", "Volume", Decimal("30"), "4001"),
    # hdr=2000 — NULL Total → excluded from dim, contributes 0 in fct
    (10, 2000, "NUL", "false", "NullTotal", None, "6000"),
    (11, 2000, "VAL", "false", "ValidOnly", Decimal("100"), "6001"),
    # hdr=3000 — Zero Total → excluded from dim, contributes 0 in fct
    (20, 3000, "ZER", "true", "ZeroFee", Decimal("0"), "7000"),
    (21, 3000, "SVC", "true", "ServiceFee", Decimal("75.5"), "7001"),
    # hdr=4000 — ID ordering (rows inserted out-of-id-order)
    (33, 4000, "C", "false", "Third", Decimal("10"), "C1"),
    (31, 4000, "A", "false", "First", Decimal("20"), "a1"),
    (32, 4000, "B", "false", "Second", Decimal("15"), "B1"),
    # hdr=5000 — '#' in source values must be stripped
    (40, 5000, "DIS#C", "false", "Dis#count", Decimal("50"), "40#00"),
    (41, 5000, "NRM", "false", "Normal", Decimal("25"), "5000"),
    # hdr=6000 — NULL Indikering → treated as tillaeg
    (50, 6000, "UNK", None, "Unknown", Decimal("200"), "8000"),
    # hdr=7000 — mixed-case Indikering ('FALSE'/'False' → rabat)
    (60, 7000, "UP", "FALSE", "Upper", Decimal("10"), "9000"),
    (61, 7000, "MX", "False", "Mixed", Decimal("20"), "9001"),
    (62, 7000, "TU", "TRUE", "TrueUp", Decimal("5"), "9002"),
    # hdr=8000 — NULL Årsag/Kategori/Finanskonto on first row
    (70, 8000, None, "false", None, Decimal("100"), None),
    (71, 8000, "HAS", "false", "HasValue", Decimal("50"), "1234"),
    # hdr=9000 — single row, all descriptors NULL
    (80, 9000, None, "false", None, Decimal("100"), None),
    # hdr=10000 — only zero-Total rows → no dim output, zero fct output
    (90, 10000, "AZ", "false", "AllZero", Decimal("0"), "1111"),
    # hdr=11000 — ':' in source Aarsag must be stripped (composite delimiter)
    (100, 11000, "COL", "false", "Fragt: ekspres", Decimal("25"), "C500"),
]

# ═══════════════════════════════════════════════════════════════════════════════
# Seed data — Line allowance charges
# ═══════════════════════════════════════════════════════════════════════════════
# Tuples: (id, statistic_invoice_line_id, FakturaUdstederGebyrKategori,
#           FakturaUdstederGebyrKategoriIndikering, FakturaUdstederGebyrKategoriÅrsag,
#           FakturaUdstederGebyrKategoriTotal, FakturaUdstederGebyrKategoriFinanskonto)

LINE_AC_ROWS = [
    # line=5000 — mixed rabat + tillaeg (mirrors header hdr=1000)
    (1, 5000, "DIS", "false", "LineDiscount", Decimal("50"), "L4000"),
    (2, 5000, "FRT", "true", "LineFreight", Decimal("25"), "L5000"),
    (3, 5000, "VOL", "false", "LineVolume", Decimal("30"), "L4001"),
    # line=6000 — NULL Total excluded from dim
    (10, 6000, "NUL", "false", "NullTotal", None, "L6000"),
    (11, 6000, "VAL", "false", "ValidOnly", Decimal("100"), "L6001"),
    # line=7000 — ID ordering (out-of-order insert)
    (23, 7000, "C", "false", "Third", Decimal("10"), "LC1"),
    (21, 7000, "A", "false", "First", Decimal("20"), "LA1"),
    (22, 7000, "B", "false", "Second", Decimal("15"), "LB1"),
    # line=8000 — NULL Indikering → tillaeg
    (30, 8000, "UNK", None, "Unknown", Decimal("200"), "L8000"),
    # line=11000 — ':' in source Aarsag must be stripped (composite delimiter)
    (40, 11000, "COL", "false", "Linje: rabat", Decimal("25"), "LC500"),
]

# ═══════════════════════════════════════════════════════════════════════════════
# Query SQL — production CTEs (imported from staging modules) + SELECT wrappers
# ═══════════════════════════════════════════════════════════════════════════════

HEADER_DIM_RABAT_SQL = (
    HAC_DIM_CTE_SQL + "SELECT * FROM hac_rabat WHERE statistic_invoice_header_id = ?"
)
HEADER_DIM_TILLAEG_SQL = (
    HAC_DIM_CTE_SQL + "SELECT * FROM hac_tillaeg WHERE statistic_invoice_header_id = ?"
)
HEADER_FCT_SUM_SQL = HAC_SUM_CTE_SQL + "SELECT * FROM hac_sum WHERE statistic_invoice_header_id = ?"
# Reproduces the production LEFT JOIN + measure projection for a single header id,
# so the no-allowance-rows case exercises the same NULL handling as the real query.
HEADER_FCT_JOIN_SQL = (
    HAC_SUM_CTE_SQL
    + f"""
SELECT
    {HAC_MEASURE_COLUMNS_SQL}
FROM (SELECT CAST(? AS BIGINT) AS statistic_invoice_header_id) ranked
LEFT JOIN hac_sum hac
    ON hac.statistic_invoice_header_id = ranked.statistic_invoice_header_id
"""
)

LINE_DIM_RABAT_SQL = LAC_DIM_CTE_SQL + "SELECT * FROM lac_rabat WHERE statistic_invoice_line_id = ?"
LINE_DIM_TILLAEG_SQL = (
    LAC_DIM_CTE_SQL + "SELECT * FROM lac_tillaeg WHERE statistic_invoice_line_id = ?"
)
LINE_FCT_SUM_SQL = LAC_SUM_CTE_SQL + "SELECT * FROM lac_sum WHERE statistic_invoice_line_id = ?"
LINE_FCT_JOIN_SQL = (
    LAC_SUM_CTE_SQL
    + f"""
SELECT
    {LAC_MEASURE_COLUMNS_SQL}
FROM (SELECT CAST(? AS BIGINT) AS statistic_invoice_line_id) ranked
LEFT JOIN lac_sum lac
    ON lac.statistic_invoice_line_id = ranked.statistic_invoice_line_id
"""
)


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════


def _ensure_and_seed_header(cursor):
    # NOTE(#28693): Repointed from ext.[statistic_invoice_header_allowance_charge_mysupply]
    # to arc.[mysupply_statistic_invoice_header_allowance_charge] — the production CTE in
    # stg_fct_Faktura_mysupply.HAC_SUM_CTE_SQL and stg_dim_Faktura_mysupply.HAC_DIM_CTE_SQL
    # now read from arc.* after the DFM migration. The arc table uses dw_timestamp_insert /
    # dw_timestamp_update instead of dw_load_timestamp.
    # TODO(#28693): validate in devcontainer that the arc schema (INT PK) accepts the BIGINT
    # test ids here (the DDL uses INT NOT NULL for id/statistic_invoice_header_id).
    cursor.execute(
        """
        IF OBJECT_ID(N'[arc].[mysupply_statistic_invoice_header_allowance_charge]', 'U') IS NULL
        BEGIN
            CREATE TABLE [arc].[mysupply_statistic_invoice_header_allowance_charge](
                id                              BIGINT NOT NULL,
                statistic_invoice_header_id     BIGINT NOT NULL,
                GebyrKategori                   NVARCHAR(200) NULL,
                GebyrKategoriIndikering         NVARCHAR(200) NULL,
                [GebyrKategoriÅrsag]            NVARCHAR(200) NULL,
                GebyrKategoriKvantitet           NVARCHAR(200) NULL,
                GebyrKategoriKvantitet_Type      NVARCHAR(200) NULL,
                GebyrKategoriKvantitet_List      NVARCHAR(200) NULL,
                GebyrKategoriTotal               NVARCHAR(200) NULL,
                GebyrKategoriFinanskonto         NVARCHAR(200) NULL,
                dw_timestamp_insert              DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
                dw_timestamp_update              DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
            )
        END
    """
    )
    cursor.execute(
        "DELETE FROM [arc].[mysupply_statistic_invoice_header_allowance_charge]"
        " WHERE id BETWEEN 1 AND 100"
    )
    cursor.executemany(
        """
        INSERT INTO [arc].[mysupply_statistic_invoice_header_allowance_charge]
            (id, statistic_invoice_header_id, GebyrKategori,
             GebyrKategoriIndikering, [GebyrKategoriÅrsag],
             GebyrKategoriTotal, GebyrKategoriFinanskonto,
             dw_timestamp_insert, dw_timestamp_update)
        VALUES (?, ?, ?, ?, ?, ?, ?, SYSDATETIME(), SYSDATETIME())
        """,
        HEADER_AC_ROWS,
    )


def _ensure_and_seed_line(cursor):
    # NOTE(#28693): Repointed from ext.[statistic_invoice_line_allowance_charge_mysupply]
    # to arc.[mysupply_statistic_invoice_line_allowance_charge] — same reason as header.
    # TODO(#28693): validate in devcontainer.
    cursor.execute(
        """
        IF OBJECT_ID(N'[arc].[mysupply_statistic_invoice_line_allowance_charge]', 'U') IS NULL
        BEGIN
            CREATE TABLE [arc].[mysupply_statistic_invoice_line_allowance_charge](
                id                                          BIGINT NOT NULL,
                statistic_invoice_line_id                   BIGINT NOT NULL,
                FakturaUdstederGebyrKategori                NVARCHAR(200) NULL,
                FakturaUdstederGebyrKategoriIndikering      NVARCHAR(200) NULL,
                [FakturaUdstederGebyrKategoriÅrsag]         NVARCHAR(200) NULL,
                FakturaUdstederGebyrKategoriKvantitet        NVARCHAR(200) NULL,
                FakturaUdstederGebyrKategoriKvantitet_Type   NVARCHAR(200) NULL,
                FakturaUdstederGebyrKategoriKvantitet_List   NVARCHAR(200) NULL,
                FakturaUdstederGebyrKategoriTotal             NVARCHAR(200) NULL,
                FakturaUdstederGebyrKategoriFinanskonto       NVARCHAR(200) NULL,
                dw_timestamp_insert              DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
                dw_timestamp_update              DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
            )
        END
    """
    )
    cursor.execute(
        "DELETE FROM [arc].[mysupply_statistic_invoice_line_allowance_charge]"
        " WHERE id BETWEEN 1 AND 50"
    )
    cursor.executemany(
        """
        INSERT INTO [arc].[mysupply_statistic_invoice_line_allowance_charge]
            (id, statistic_invoice_line_id, FakturaUdstederGebyrKategori,
             FakturaUdstederGebyrKategoriIndikering, [FakturaUdstederGebyrKategoriÅrsag],
             FakturaUdstederGebyrKategoriTotal, FakturaUdstederGebyrKategoriFinanskonto,
             dw_timestamp_insert, dw_timestamp_update)
        VALUES (?, ?, ?, ?, ?, ?, ?, SYSDATETIME(), SYSDATETIME())
        """,
        LINE_AC_ROWS,
    )


def _cleanup(cursor):
    # NOTE(#28693): Repointed from ext.* to arc.* after DFM migration.
    cursor.execute(
        "IF OBJECT_ID(N'[arc].[mysupply_statistic_invoice_header_allowance_charge]', 'U') IS NOT NULL"
        " DELETE FROM [arc].[mysupply_statistic_invoice_header_allowance_charge]"
        " WHERE id BETWEEN 1 AND 100"
    )
    cursor.execute(
        "IF OBJECT_ID(N'[arc].[mysupply_statistic_invoice_line_allowance_charge]', 'U') IS NOT NULL"
        " DELETE FROM [arc].[mysupply_statistic_invoice_line_allowance_charge]"
        " WHERE id BETWEEN 1 AND 50"
    )


def _query_one(resource, sql, param):
    """Execute *sql* with a single ``?`` param and return a column-name dict (or None)."""
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (param,))
            row = cursor.fetchone()
            if row is None:
                return None
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row, strict=False))
        finally:
            cursor.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.fixture(scope="module")
def seeded_db(real_mssql_target_staging_resource):
    """Seed both ext allowance-charge tables with test data; truncate on teardown."""
    resource = real_mssql_target_staging_resource
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            _ensure_and_seed_header(cursor)
            _ensure_and_seed_line(cursor)
            conn.commit()
        finally:
            cursor.close()

    yield resource

    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            _cleanup(cursor)
            conn.commit()
        finally:
            cursor.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Tests — dim header STRING_AGG (Rabat / Tillaeg)
# ═══════════════════════════════════════════════════════════════════════════════


class TestDimHeaderAllowanceCharge:
    """Validate the STRING_AGG CTE for header allowance charges (dim layer).

    Source: stg_dim_Faktura_mysupply.py  →  hac_base / hac_rabat / hac_tillaeg
    """

    def test_basic_rabat_tillaeg_split(self, seeded_db):
        """hdr=1000: rows split correctly between Rabat and Tillaeg."""
        rabat = _query_one(seeded_db, HEADER_DIM_RABAT_SQL, 1000)
        assert rabat is not None
        assert rabat["gebyr_rabat_aarsag"] == "Discount A:50.0000#Volume:30.0000"
        assert rabat["gebyr_rabat_kategori"] == "DIS#VOL"
        assert rabat["gebyr_rabat_finanskonto"] == "4000#4001"

        tillaeg = _query_one(seeded_db, HEADER_DIM_TILLAEG_SQL, 1000)
        assert tillaeg is not None
        assert tillaeg["gebyr_tillaeg_aarsag"] == "Freight:25.0000"
        assert tillaeg["gebyr_tillaeg_kategori"] == "FRT"
        assert tillaeg["gebyr_tillaeg_finanskonto"] == "5000"

    def test_null_total_excluded(self, seeded_db):
        """hdr=2000: row with NULL Total is excluded from dim output."""
        rabat = _query_one(seeded_db, HEADER_DIM_RABAT_SQL, 2000)
        assert rabat is not None
        assert rabat["gebyr_rabat_aarsag"] == "ValidOnly:100.0000"
        assert rabat["gebyr_rabat_kategori"] == "VAL"
        assert rabat["gebyr_rabat_finanskonto"] == "6001"

        tillaeg = _query_one(seeded_db, HEADER_DIM_TILLAEG_SQL, 2000)
        assert tillaeg is None, "No tillaeg rows expected for hdr=2000"

    def test_zero_total_excluded(self, seeded_db):
        """hdr=3000: row with Total=0 is excluded from dim output."""
        rabat = _query_one(seeded_db, HEADER_DIM_RABAT_SQL, 3000)
        assert rabat is None, "No rabat rows expected for hdr=3000"

        tillaeg = _query_one(seeded_db, HEADER_DIM_TILLAEG_SQL, 3000)
        assert tillaeg is not None
        assert tillaeg["gebyr_tillaeg_aarsag"] == "ServiceFee:75.5000"
        assert tillaeg["gebyr_tillaeg_kategori"] == "SVC"
        assert tillaeg["gebyr_tillaeg_finanskonto"] == "7001"

    def test_id_ordering(self, seeded_db):
        """hdr=4000: STRING_AGG respects ORDER BY id regardless of insert order."""
        rabat = _query_one(seeded_db, HEADER_DIM_RABAT_SQL, 4000)
        assert rabat is not None
        assert rabat["gebyr_rabat_aarsag"] == "First:20.0000#Second:15.0000#Third:10.0000"
        assert rabat["gebyr_rabat_kategori"] == "A#B#C"
        assert rabat["gebyr_rabat_finanskonto"] == "a1#B1#C1"

    def test_hash_stripping(self, seeded_db):
        """hdr=5000: '#' characters in source values are stripped before aggregation."""
        rabat = _query_one(seeded_db, HEADER_DIM_RABAT_SQL, 5000)
        assert rabat is not None
        assert rabat["gebyr_rabat_aarsag"] == "Discount:50.0000#Normal:25.0000"
        assert rabat["gebyr_rabat_kategori"] == "DISC#NRM"
        assert rabat["gebyr_rabat_finanskonto"] == "4000#5000"

    def test_null_indikering_is_tillaeg(self, seeded_db):
        """hdr=6000: NULL Indikering is routed to Tillaeg (not Rabat)."""
        rabat = _query_one(seeded_db, HEADER_DIM_RABAT_SQL, 6000)
        assert rabat is None, "NULL Indikering must not appear in Rabat"

        tillaeg = _query_one(seeded_db, HEADER_DIM_TILLAEG_SQL, 6000)
        assert tillaeg is not None
        assert tillaeg["gebyr_tillaeg_aarsag"] == "Unknown:200.0000"
        assert tillaeg["gebyr_tillaeg_kategori"] == "UNK"
        assert tillaeg["gebyr_tillaeg_finanskonto"] == "8000"

    def test_case_insensitive_indikering(self, seeded_db):
        """hdr=7000: 'FALSE'/'False' both resolve to Rabat via LOWER()."""
        rabat = _query_one(seeded_db, HEADER_DIM_RABAT_SQL, 7000)
        assert rabat is not None
        assert rabat["gebyr_rabat_aarsag"] == "Upper:10.0000#Mixed:20.0000"
        assert rabat["gebyr_rabat_kategori"] == "UP#MX"
        assert rabat["gebyr_rabat_finanskonto"] == "9000#9001"

        tillaeg = _query_one(seeded_db, HEADER_DIM_TILLAEG_SQL, 7000)
        assert tillaeg is not None
        assert tillaeg["gebyr_tillaeg_aarsag"] == "TrueUp:5.0000"
        assert tillaeg["gebyr_tillaeg_kategori"] == "TU"
        assert tillaeg["gebyr_tillaeg_finanskonto"] == "9002"

    def test_null_descriptors_mixed(self, seeded_db):
        """hdr=8000: NULL Årsag/Kategori/Finanskonto rendered as empty in '#'-delimited output."""
        rabat = _query_one(seeded_db, HEADER_DIM_RABAT_SQL, 8000)
        assert rabat is not None
        assert rabat["gebyr_rabat_aarsag"] == ":100.0000#HasValue:50.0000"
        assert rabat["gebyr_rabat_kategori"] == "#HAS"
        assert rabat["gebyr_rabat_finanskonto"] == "#1234"

    def test_single_row_all_null_descriptors(self, seeded_db):
        """hdr=9000: single row with all-NULL descriptors → Aarsag has value, Kategori/Finanskonto → NULL via NULLIF."""
        rabat = _query_one(seeded_db, HEADER_DIM_RABAT_SQL, 9000)
        assert rabat is not None
        assert rabat["gebyr_rabat_aarsag"] == ":100.0000"
        assert rabat["gebyr_rabat_kategori"] is None, "NULLIF('', '') → NULL"
        assert rabat["gebyr_rabat_finanskonto"] is None, "NULLIF('', '') → NULL"

    def test_colon_stripped_from_aarsag(self, seeded_db):
        """hdr=11000: ':' in source Aarsag is stripped so the '{Aarsag}:{Total}' composite stays parseable."""
        rabat = _query_one(seeded_db, HEADER_DIM_RABAT_SQL, 11000)
        assert rabat is not None
        assert rabat["gebyr_rabat_aarsag"] == "Fragt ekspres:25.0000"

    def test_all_zero_returns_no_row(self, seeded_db):
        """hdr=10000: all rows have Total=0 → hac_base filters everything → no output."""
        rabat = _query_one(seeded_db, HEADER_DIM_RABAT_SQL, 10000)
        assert rabat is None

        tillaeg = _query_one(seeded_db, HEADER_DIM_TILLAEG_SQL, 10000)
        assert tillaeg is None


# ═══════════════════════════════════════════════════════════════════════════════
# Tests — fct header SUM (gebyr_beloeb / rabatbeloeb)
# ═══════════════════════════════════════════════════════════════════════════════


class TestFctHeaderAllowanceCharge:
    """Validate the SUM CTE for header allowance charges (fct layer).

    Source: stg_fct_Faktura_mysupply.py  →  hac_sum
    Key difference from dim: no null/zero pre-filter — ALL rows contribute to SUM.
    """

    def test_basic_gebyr_rabat_split(self, seeded_db):
        """hdr=1000: gebyr_beloeb=25 (tillaeg), rabatbeloeb=80 (50+30 rabat)."""
        row = _query_one(seeded_db, HEADER_FCT_SUM_SQL, 1000)
        assert row is not None
        assert row["gebyr_beloeb"] == Decimal("25")
        assert row["rabatbeloeb"] == Decimal("80")

    def test_null_total_contributes_zero(self, seeded_db):
        """hdr=2000: NULL Total → ISNULL(TRY_CAST(NULL), 0) = 0 in SUM."""
        row = _query_one(seeded_db, HEADER_FCT_SUM_SQL, 2000)
        assert row is not None
        assert row["gebyr_beloeb"] == Decimal("0")
        assert row["rabatbeloeb"] == Decimal("100")

    def test_zero_total_contributes_zero(self, seeded_db):
        """hdr=3000: Total=0 still summed (unlike dim), result includes zero contribution."""
        row = _query_one(seeded_db, HEADER_FCT_SUM_SQL, 3000)
        assert row is not None
        assert row["gebyr_beloeb"] == Decimal("75.5")
        assert row["rabatbeloeb"] == Decimal("0")

    def test_all_rabat(self, seeded_db):
        """hdr=4000: all Indikering='false' → gebyr_beloeb=0, rabatbeloeb=45."""
        row = _query_one(seeded_db, HEADER_FCT_SUM_SQL, 4000)
        assert row is not None
        assert row["gebyr_beloeb"] == Decimal("0")
        assert row["rabatbeloeb"] == Decimal("45")

    def test_null_indikering_is_gebyr(self, seeded_db):
        """hdr=6000: NULL Indikering → LOWER(NULL)≠'false' → gebyr_beloeb."""
        row = _query_one(seeded_db, HEADER_FCT_SUM_SQL, 6000)
        assert row is not None
        assert row["gebyr_beloeb"] == Decimal("200")
        assert row["rabatbeloeb"] == Decimal("0")

    def test_case_insensitive(self, seeded_db):
        """hdr=7000: 'FALSE'/'False' → rabat, 'TRUE' → gebyr."""
        row = _query_one(seeded_db, HEADER_FCT_SUM_SQL, 7000)
        assert row is not None
        assert row["gebyr_beloeb"] == Decimal("5")
        assert row["rabatbeloeb"] == Decimal("30")

    def test_all_zero_still_has_row(self, seeded_db):
        """hdr=10000: unlike dim, fct CTE returns a row with both sums = 0."""
        row = _query_one(seeded_db, HEADER_FCT_SUM_SQL, 10000)
        assert row is not None
        assert row["gebyr_beloeb"] == Decimal("0")
        assert row["rabatbeloeb"] == Decimal("0")

    def test_no_allowance_rows_yields_zero(self, seeded_db):
        """Header with no allowance rows at all: LEFT JOIN miss must yield 0, not NULL."""
        row = _query_one(seeded_db, HEADER_FCT_JOIN_SQL, 99999)
        assert row is not None
        assert row["gebyr_beloeb"] == Decimal("0")
        assert row["rabatbeloeb"] == Decimal("0")


# ═══════════════════════════════════════════════════════════════════════════════
# Tests — dim line STRING_AGG (Rabat / Tillaeg)
# ═══════════════════════════════════════════════════════════════════════════════


class TestDimLineAllowanceCharge:
    """Validate the STRING_AGG CTE for line allowance charges (dim layer).

    Source: stg_dim_Fakturalinje_mysupply.py  →  lac_base / lac_rabat / lac_tillaeg
    Logic mirrors the header CTE with FakturaUdstederGebyrKategori* column names.
    """

    def test_basic_rabat_tillaeg_split(self, seeded_db):
        """line=5000: same split logic as header with line column names."""
        rabat = _query_one(seeded_db, LINE_DIM_RABAT_SQL, 5000)
        assert rabat is not None
        assert rabat["faktura_udsteder_gebyr_rabat_aarsag"] == (
            "LineDiscount:50.0000#LineVolume:30.0000"
        )
        assert rabat["faktura_udsteder_gebyr_rabat_kategori"] == "DIS#VOL"
        assert rabat["faktura_udsteder_gebyr_rabat_finanskonto"] == "L4000#L4001"

        tillaeg = _query_one(seeded_db, LINE_DIM_TILLAEG_SQL, 5000)
        assert tillaeg is not None
        assert tillaeg["faktura_udsteder_gebyr_tillaeg_aarsag"] == "LineFreight:25.0000"
        assert tillaeg["faktura_udsteder_gebyr_tillaeg_kategori"] == "FRT"
        assert tillaeg["faktura_udsteder_gebyr_tillaeg_finanskonto"] == "L5000"

    def test_null_total_excluded(self, seeded_db):
        """line=6000: NULL Total filtered out, only valid row remains."""
        rabat = _query_one(seeded_db, LINE_DIM_RABAT_SQL, 6000)
        assert rabat is not None
        assert rabat["faktura_udsteder_gebyr_rabat_aarsag"] == "ValidOnly:100.0000"
        assert rabat["faktura_udsteder_gebyr_rabat_kategori"] == "VAL"
        assert rabat["faktura_udsteder_gebyr_rabat_finanskonto"] == "L6001"

    def test_id_ordering(self, seeded_db):
        """line=7000: ORDER BY id produces correct positional alignment."""
        rabat = _query_one(seeded_db, LINE_DIM_RABAT_SQL, 7000)
        assert rabat is not None
        assert rabat["faktura_udsteder_gebyr_rabat_aarsag"] == (
            "First:20.0000#Second:15.0000#Third:10.0000"
        )
        assert rabat["faktura_udsteder_gebyr_rabat_kategori"] == "A#B#C"
        assert rabat["faktura_udsteder_gebyr_rabat_finanskonto"] == "LA1#LB1#LC1"

    def test_colon_stripped_from_aarsag(self, seeded_db):
        """line=11000: ':' in source Aarsag is stripped so the '{Aarsag}:{Total}' composite stays parseable."""
        rabat = _query_one(seeded_db, LINE_DIM_RABAT_SQL, 11000)
        assert rabat is not None
        assert rabat["faktura_udsteder_gebyr_rabat_aarsag"] == "Linje rabat:25.0000"

    def test_null_indikering_is_tillaeg(self, seeded_db):
        """line=8000: NULL Indikering routed to Tillaeg."""
        rabat = _query_one(seeded_db, LINE_DIM_RABAT_SQL, 8000)
        assert rabat is None

        tillaeg = _query_one(seeded_db, LINE_DIM_TILLAEG_SQL, 8000)
        assert tillaeg is not None
        assert tillaeg["faktura_udsteder_gebyr_tillaeg_aarsag"] == "Unknown:200.0000"
        assert tillaeg["faktura_udsteder_gebyr_tillaeg_kategori"] == "UNK"
        assert tillaeg["faktura_udsteder_gebyr_tillaeg_finanskonto"] == "L8000"


# ═══════════════════════════════════════════════════════════════════════════════
# Tests — fct line SUM (gebyr_beloeb / rabatbeloeb)
# ═══════════════════════════════════════════════════════════════════════════════


class TestFctLineAllowanceCharge:
    """Validate the SUM CTE for line allowance charges (fct layer).

    Source: stg_fct_Fakturalinje_mysupply.py  →  lac_sum
    """

    def test_basic_gebyr_rabat_split(self, seeded_db):
        """line=5000: gebyr_beloeb=25 (tillaeg), rabatbeloeb=80 (rabat)."""
        row = _query_one(seeded_db, LINE_FCT_SUM_SQL, 5000)
        assert row is not None
        assert row["gebyr_beloeb"] == Decimal("25")
        assert row["rabatbeloeb"] == Decimal("80")

    def test_null_total_contributes_zero(self, seeded_db):
        """line=6000: NULL Total → 0 in SUM."""
        row = _query_one(seeded_db, LINE_FCT_SUM_SQL, 6000)
        assert row is not None
        assert row["gebyr_beloeb"] == Decimal("0")
        assert row["rabatbeloeb"] == Decimal("100")

    def test_null_indikering_is_gebyr(self, seeded_db):
        """line=8000: NULL Indikering → gebyr_beloeb."""
        row = _query_one(seeded_db, LINE_FCT_SUM_SQL, 8000)
        assert row is not None
        assert row["gebyr_beloeb"] == Decimal("200")
        assert row["rabatbeloeb"] == Decimal("0")

    def test_no_allowance_rows_yields_zero(self, seeded_db):
        """Line with no allowance rows at all: LEFT JOIN miss must yield 0, not NULL."""
        row = _query_one(seeded_db, LINE_FCT_JOIN_SQL, 99999)
        assert row is not None
        assert row["gebyr_beloeb"] == Decimal("0")
        assert row["rabatbeloeb"] == Decimal("0")

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Behaviour on an isolated LOCAL SQL Server, using synthetic rows only.

Opt in with ENABLE_TEST_WRITES=true, BORGERDEMOGRAFI_TEST_PASSWORD and optionally
BORGERDEMOGRAFI_TEST_PORT (default 15452). The host is fixed to loopback.
Each test creates and drops its own randomly named database.
"""

import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest
from DSA.SQL.dim.dim_BorgerDemografi import INSERT_SQL, LOCK_SQL, SOURCE_SQL

DDL = Path(__file__).resolve().parents[4] / "infrastructure" / "ddl"
pytestmark = pytest.mark.integration


@pytest.fixture
def db():
    password = os.getenv("BORGERDEMOGRAFI_TEST_PASSWORD")
    if not password or os.getenv("ENABLE_TEST_WRITES", "").lower() != "true":
        pytest.skip("Requires an isolated local SQL Server and ENABLE_TEST_WRITES=true")
    import pymssql

    name = "demografi_staging_test_" + uuid4().hex
    kwargs = dict(
        server="127.0.0.1",
        port=int(os.getenv("BORGERDEMOGRAFI_TEST_PORT", "15452")),
        user="sa",
        password=password,
        login_timeout=10,
    )
    admin = pymssql.connect(**kwargs, database="master", autocommit=True)
    admin.cursor().execute(f"CREATE DATABASE [{name}]")

    def connect():
        return pymssql.connect(**kwargs, database=name)

    conn = connect()
    try:
        c = conn.cursor()
        c.execute("CREATE SCHEMA dim")
        c.execute(
            """
            CREATE TABLE dim.Borger (
                dw_sk_Borger INT PRIMARY KEY, dw_ek_Borger INT,
                foedselsdato DATE NULL,
                region_id VARCHAR(8), regionnavn NVARCHAR(255),
                kommune_id VARCHAR(8), kommunenavn NVARCHAR(255),
                postnr VARCHAR(10), postdistrikt NVARCHAR(255), koen_navn NVARCHAR(20),
                dw_is_deleted BIT NOT NULL DEFAULT 0,
                dw_is_current BIT NOT NULL DEFAULT 1,
                dw_valid_from DATETIME2 NOT NULL DEFAULT '2000-01-01',
                dw_valid_to DATETIME2 NOT NULL DEFAULT '9999-12-31'
            )
        """
        )
        for filename in (
            "migrations/20261028_borgerdemografi_hash.sql",
            "migrations/20261028b_borgerdemografi_foedselsperiode.sql",
            "migrations/20261028b_borgerdemografi_foedselsperiode.sql",
            "tables/shared_dimensions/03_borgerdemografi.sql",
            "common/functions/GetBorgerSK_TVF.sql",
        ):
            for batch in re.split(r"(?im)^GO\s*$", (DDL / filename).read_text()):
                if batch.strip():
                    c.execute(batch)
        conn.commit()
        yield conn, connect
    finally:
        conn.close()
        admin.cursor().execute(f"ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
        admin.cursor().execute(f"DROP DATABASE [{name}]")
        admin.close()


def seed(conn, sk, postnr="5000", deleted=False, **values):
    row = dict(
        dw_sk_Borger=sk,
        dw_ek_Borger=1,
        region_id="1083",
        regionnavn="Eksempel",
        kommune_id="0461",
        kommunenavn="Odense",
        postnr=postnr,
        postdistrikt="Odense C",
        koen_navn="female",
        dw_is_deleted=int(deleted),
    )
    row.update(values)
    cols = ", ".join(row)
    conn.cursor().execute(
        f"INSERT INTO dim.Borger ({cols}) VALUES ({', '.join(['%s'] * len(row))})",
        tuple(row.values()),
    )
    conn.commit()


def populate(conn):
    c = conn.cursor()
    try:
        for sql in (LOCK_SQL, SOURCE_SQL, INSERT_SQL):
            c.execute(sql)
        count = c.rowcount
        c.execute("DROP TABLE #BorgerDemografi")
        conn.commit()
        return count
    except Exception:
        conn.rollback()
        raise


def scalar(conn, sql, params=None):
    c = conn.cursor()
    c.execute(sql, params)
    return c.fetchone()[0]


def test_fresh_borger_ddl_has_the_same_persisted_key_as_the_migration(db):
    conn, _ = db
    seed(conn, 1)
    migrated = scalar(conn, "SELECT dw_ek_BorgerDemografi FROM dim.Borger WHERE dw_sk_Borger=1")
    c = conn.cursor()
    c.execute("DROP TABLE dim.Borger")
    for batch in re.split(
        r"(?im)^GO\s*$", (DDL / "tables/shared_dimensions/02_dims.sql").read_text()
    ):
        if batch.strip():
            c.execute(batch)
    for batch in re.split(
        r"(?im)^GO\s*$", (DDL / "tables/shared_dimensions/03_borgerdemografi.sql").read_text()
    ):
        if batch.strip():
            c.execute(batch)
    conn.commit()
    seed(conn, 1, dw_valid_from="2000-01-01", dw_valid_to="9999-12-31")
    assert (
        scalar(conn, "SELECT dw_ek_BorgerDemografi FROM dim.Borger WHERE dw_sk_Borger=1")
        == migrated
    )
    assert (
        scalar(
            conn,
            """
        SELECT is_persisted FROM sys.computed_columns
        WHERE object_id=OBJECT_ID('dim.Borger') AND name='dw_ek_BorgerDemografi'
    """,
        )
        == 1
    )


def test_migration_backfills_existing_rows_and_follows_later_corrections(db):
    conn, _ = db
    seed(conn, 1)
    before = scalar(conn, "SELECT dw_ek_BorgerDemografi FROM dim.Borger WHERE dw_sk_Borger=1")
    c = conn.cursor()
    c.execute("ALTER TABLE dim.Borger DROP COLUMN dw_ek_BorgerDemografi")
    for filename in (
        "migrations/20261028_borgerdemografi_hash.sql",
        "migrations/20261028b_borgerdemografi_foedselsperiode.sql",
    ):
        for batch in re.split(r"(?im)^GO\s*$", (DDL / filename).read_text()):
            if batch.strip():
                c.execute(batch)
    assert (
        scalar(conn, "SELECT dw_ek_BorgerDemografi FROM dim.Borger WHERE dw_sk_Borger=1") == before
    )
    c.execute("UPDATE dim.Borger SET postnr='6000' WHERE dw_sk_Borger=1")
    assert (
        scalar(conn, "SELECT dw_ek_BorgerDemografi FROM dim.Borger WHERE dw_sk_Borger=1") != before
    )
    conn.commit()


def test_shared_members_rerun_and_old_keys_survive_full_rescan(db):
    conn, _ = db
    seed(conn, 1)
    seed(conn, 2)
    assert populate(conn) == 1
    key = scalar(conn, "SELECT dw_ek_BorgerDemografi FROM dim.Borger WHERE dw_sk_Borger=1")
    assert len(key) == 64
    assert scalar(conn, "SELECT dw_ek_BorgerDemografi FROM dim.Borger WHERE dw_sk_Borger=2") == key
    assert populate(conn) == 0
    conn.cursor().execute("UPDATE dim.Borger SET postnr='6000'")
    conn.commit()
    assert populate(conn) == 1
    assert (
        scalar(
            conn, "SELECT COUNT(*) FROM dim.BorgerDemografi WHERE dw_ek_BorgerDemografi=%s", (key,)
        )
        == 1
    )


def test_birth_cohort_shares_days_but_separates_months_years_and_unknown(db):
    conn, _ = db
    for sk, birth in enumerate(
        ("1980-05-01", "1980-05-31", "1980-06-01", "1981-05-01", None), start=1
    ):
        seed(conn, sk, foedselsdato=birth)
    assert populate(conn) == 4
    query = "SELECT dw_ek_BorgerDemografi FROM dim.Borger WHERE dw_sk_Borger=%s"
    first = scalar(conn, query, (1,))
    assert scalar(conn, query, (2,)) == first
    assert all(scalar(conn, query, (sk,)) != first for sk in (3, 4, 5))
    assert (
        scalar(
            conn,
            """
        SELECT COUNT(*) FROM dim.Borger b JOIN dim.BorgerDemografi d
        ON d.dw_ek_BorgerDemografi=b.dw_ek_BorgerDemografi
        WHERE EXISTS (SELECT YEAR(b.foedselsdato), MONTH(b.foedselsdato)
                      EXCEPT SELECT d.foedselsaar, d.foedselsmaaned)
    """,
        )
        == 0
    )


def test_temporal_vs_current_selection_and_deleted_versions(db):
    conn, _ = db
    seed(conn, 1, dw_is_current=0, dw_valid_to="2020-01-01")
    seed(conn, 2, "6000", dw_valid_from="2020-01-01")
    seed(conn, 3, "7000", deleted=True)
    assert populate(conn) == 3
    past = scalar(
        conn,
        """
        SELECT b.dw_ek_BorgerDemografi
        FROM dbo.GetBorgerSK_TVF(1, '2019-12-31') sk
        JOIN dim.Borger b ON b.dw_sk_Borger=sk.dw_sk_Borger
    """,
    )
    current = scalar(
        conn,
        """
        SELECT b.dw_ek_BorgerDemografi FROM dim.Borger b
        WHERE b.dw_ek_Borger=1 AND b.dw_is_current=1 AND b.dw_is_deleted=0
    """,
    )
    assert len(past) == 64 and len(current) == 64 and past != current
    # Retired Borger rows also keep a valid key; they are excluded when
    # selecting a temporal/current version, not by losing their dimension row.
    assert (
        scalar(
            conn,
            """
        SELECT COUNT(*) FROM dim.Borger b
        JOIN dim.BorgerDemografi d ON d.dw_ek_BorgerDemografi=b.dw_ek_BorgerDemografi
    """,
        )
        == 3
    )


def test_each_attribute_changes_the_direct_hash_and_normalisation_is_stable(db):
    from DSA.SQL.common.borgerdemografi import ATTRIBUTES

    conn, _ = db
    seed(conn, 1)
    original = scalar(conn, "SELECT dw_ek_BorgerDemografi FROM dim.Borger WHERE dw_sk_Borger=1")
    for attr in ATTRIBUTES:
        if attr in ("foedselsaar", "foedselsmaaned"):
            continue
        c = conn.cursor()
        c.execute(f"SELECT [{attr}] FROM dim.Borger WHERE dw_sk_Borger=1")
        previous = c.fetchone()[0]
        c.execute(f"UPDATE dim.Borger SET [{attr}]=%s WHERE dw_sk_Borger=1", ("X",))
        assert (
            scalar(conn, "SELECT dw_ek_BorgerDemografi FROM dim.Borger WHERE dw_sk_Borger=1")
            != original
        )
        c.execute(f"UPDATE dim.Borger SET [{attr}]=%s WHERE dw_sk_Borger=1", (previous,))
    conn.cursor().execute(
        "UPDATE dim.Borger SET kommunenavn=N' odense ', sogn_id='' WHERE dw_sk_Borger=1"
    )
    assert (
        scalar(conn, "SELECT dw_ek_BorgerDemografi FROM dim.Borger WHERE dw_sk_Borger=1")
        == original
    )
    conn.commit()
    assert populate(conn) == 1


def test_canonical_unicode_values_boundaries_and_unknown(db):
    conn, _ = db
    seed(conn, 1, kommunenavn=" Ærø ", regionnavn="Odense")
    seed(conn, 2, kommunenavn="ærø", regionnavn="ODENSE")
    seed(conn, 3, kommunenavn="AB", regionnavn="C")
    seed(conn, 4, kommunenavn="A", regionnavn="BC")
    query = "SELECT dw_ek_BorgerDemografi FROM dim.Borger WHERE dw_sk_Borger=%s"
    assert scalar(conn, query, (1,)) == scalar(conn, query, (2,))
    assert scalar(conn, query, (3,)) != scalar(conn, query, (4,))
    conn.cursor().execute("INSERT INTO dim.Borger (dw_sk_Borger) VALUES (-1)")
    conn.commit()
    unknown = scalar(conn, query, (-1,))
    assert (
        scalar(
            conn,
            "SELECT COUNT(*) FROM dim.BorgerDemografi WHERE dw_ek_BorgerDemografi=%s",
            (unknown,),
        )
        == 1
    )


def test_rollback_removes_partially_inserted_combinations(db):
    conn, _ = db
    seed(conn, 1)
    c = conn.cursor()
    for sql in (LOCK_SQL, SOURCE_SQL, INSERT_SQL):
        c.execute(sql)
    assert scalar(conn, "SELECT COUNT(*) FROM dim.BorgerDemografi") == 2
    conn.rollback()
    assert scalar(conn, "SELECT COUNT(*) FROM dim.BorgerDemografi") == 1
    assert populate(conn) == 1


def test_concurrent_writers_insert_one_combination(db):
    conn, connect = db
    seed(conn, 1)

    def writer(_):
        local = connect()
        try:
            return populate(local)
        finally:
            local.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(writer, range(2))) == [0, 1]
    assert scalar(conn, "SELECT COUNT(*) FROM dim.BorgerDemografi") == 2

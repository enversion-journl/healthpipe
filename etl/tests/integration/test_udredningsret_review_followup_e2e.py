# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL Server regressioner for NULL-markoer-id og F_ID i arkivets fulde bredde."""

import pytest
from infrastructure.utils.ddl_loader import execute_sql_file
from tests.integration.test_udredningsret_dwh_e2e import (  # noqa: F401
    A_FILE,
    CPR_X,
    build_delivery,
    delivery_dir,
    dwh_db,
    koer_dwh,
    koer_kaeden,
    markoer,
    raekker,
    require_real_environment,
    rows_a_dwh,
    run_archive,
    run_extract,
    run_sql,
    scalar,
    staging_db,
    udred_db,
)


def test_null_identiteter_og_500_tegns_f_id_bevares_uden_nye_noegler(
    dwh_db,  # noqa: F811
    delivery_dir,  # noqa: F811
    monkeypatch,
):
    r = dwh_db
    migration = "migrations/20260917_udredningsret_f_id_width.sql"
    # Exercise an upgrade from the existing schema, then its idempotent rerun.
    run_sql(r, "ALTER TABLE dim.UdredningsretEgenskaber ALTER COLUMN f_id NVARCHAR(20) NULL")
    for _ in range(2):
        execute_sql_file(r, migration)
        assert (
            scalar(
                r,
                "SELECT max_length FROM sys.columns WHERE object_id="
                "OBJECT_ID('dim.UdredningsretEgenskaber') AND name='f_id'",
            )
            == 1000
        )

    ids = ["L" * 499 + "A", "L" * 499 + "B"]
    elements = ["a0000000-0000-0000-0000-000000000001", "b0000000-0000-0000-0000-000000000001"]
    rows = [
        # Earlier marker anchors the health-course while parallel groups change.
        markoer(
            "9800",
            "c0000000-0000-0000-0000-000000000001",
            "f0000000-0000-0000-0000-000000000001",
            "9801_1",
            "9801",
            CPR_X,
            "AFV01A1",
            "2025-01-01",
        ),
        markoer("9800", elements[0], "", "9802_1", ids[0], CPR_X, "AFV01A1", "2025-02-01"),
        markoer("9800", elements[1], "   ", "9803_1", ids[1], CPR_X, "AFV01A2", "2025-02-01"),
        # Neither UUID is populated: distinguish a published dimension from
        # a map-only reservation, which also has NULL UUIDs in the left join.
        markoer("9800", "", "", "9804_1", "9804", CPR_X, "AFV01A1", "2025-03-01"),
    ]
    build_delivery(delivery_dir, A_FILE, "20260831", "2026-08-31", rows_a_dwh() + rows)
    run_extract(r, delivery_dir, monkeypatch)
    run_archive(r)

    def publish():
        koer_kaeden(r, "full_load")
        koer_dwh(r, "full_load")
        return raekker(
            r,
            "SELECT f_id,dw_ek_Udredningsforloeb FROM dim.UdredningsretEgenskaber "
            "WHERE helbredsforloeb_id='9800' ORDER BY f_id",
        )

    before = publish()
    assert len(before) == 4
    assert set(ids).issubset({fid for fid, _ in before})
    assert (
        scalar(
            r,
            "SELECT COUNT(*) FROM fct.UdredningsretMarkoer "
            "WHERE helbredsforloeb_id='9800' AND dw_ek_Udredningsforloeb=-1",
        )
        == 0
    )
    # SQL NULL = NULL is UNKNOWN, not true. Both a second and third publish
    # must retain the same keys, including the row with neither UUID set.
    assert publish() == before
    assert publish() == before

    # Two parallel courses now share first-element + NULL start marker. This
    # is ambiguous: reject it, rather than silently re-key both every night.
    run_sql(
        r,
        "UPDATE arc.udredningsret_mont SET OBJEKTID_FORLOBELEMENT=? WHERE F_ID=?",
        elements[0],
        ids[1],
    )
    run_sql(
        r,
        "UPDATE dim.UdredningsretEgenskaber SET foerste_forloebselement_uuid=? WHERE f_id=?",
        elements[0],
        ids[1],
    )
    with pytest.raises(Exception, match="Ambiguous udredningsforloeb identity"):
        koer_kaeden(r, "full_load")
    assert (
        raekker(
            r,
            "SELECT f_id,dw_ek_Udredningsforloeb FROM dim.UdredningsretEgenskaber "
            "WHERE helbredsforloeb_id='9800' ORDER BY f_id",
        )
        == before
    )

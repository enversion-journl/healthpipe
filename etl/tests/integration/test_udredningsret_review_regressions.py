# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Regressioner for MR !102 med syntetiske leverancer og rigtig SQL Server."""

from types import SimpleNamespace

import dagster as dg
import pytest
from sensors.udredningsret_publish_sensor import (
    DELIVERY_SIGNATURE_TAG,
    UNRESOLVED_TAG,
    evaluate,
    read_state,
)
from tests.integration.test_udredningsret_dwh_e2e import (  # noqa: F401
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


def test_parallelle_forloeb_bevarer_noegler_ved_sletning_indsaettelse_og_genoplivning(
    dwh_db,  # noqa: F811
    delivery_dir,  # noqa: F811
    monkeypatch,
):
    r = dwh_db

    def course(element, marker, fid, date="2025-02-01"):
        return markoer(
            "9700",
            element + "0000000-0000-0000-0000-000000000001",
            "f0000000-0000-0000-0000-" + marker.zfill(12),
            fid + "_1",
            fid,
            CPR_X,
            "AFV01A1",
            date,
        )

    # C anchors the health-course independently of the parallel A/B courses.
    c = course("c", "1", "9701", "2025-01-01")
    a, b, d = course("a", "2", "9702"), course("b", "3", "9703"), course("0", "4", "9704")

    def publish(day, courses):
        stamp = f"202609{day:02d}193544"
        date = f"2026-09-{day:02d}"
        build_delivery(
            delivery_dir,
            f"Region_1083_LPR_Udredningsret_U_{stamp}.tar.bz2",
            date.replace("-", ""),
            date,
            rows_a_dwh() + courses,
        )
        run_extract(r, delivery_dir, monkeypatch)
        run_archive(r)
        koer_kaeden(r, "full_load")
        koer_dwh(r, "full_load")
        return {
            fid: (ek, deleted, hf)
            for fid, ek, deleted, hf in raekker(
                r,
                """
            SELECT e.f_id, f.dw_ek_Udredningsforloeb, f.dw_is_deleted, f.dw_ek_Helbredsforloeb
            FROM dim.UdredningsretEgenskaber e JOIN fct.Udredningsforloeb f
              ON f.dw_ek_Udredningsforloeb=e.dw_ek_Udredningsforloeb
            WHERE e.helbredsforloeb_id='9700'
        """,
            )
        }

    before = publish(1, [c, a, b])
    removed = publish(2, [c, b])
    assert removed["9703"] == before["9703"]
    assert removed["9702"] == (before["9702"][0], True, before["9702"][2])

    # D sorts before A/B but must receive a new slot, never steal retired A's.
    inserted = publish(3, [c, b, d])
    assert inserted["9703"] == before["9703"]
    assert inserted["9704"][0] not in {v[0] for v in before.values()}
    restored = publish(4, [c, a, b, d])
    assert restored["9702"] == before["9702"]
    assert restored["9703"] == before["9703"]
    assert restored["9704"] == inserted["9704"]

    # Delivery IDs and reissued marker UUIDs are not permanent course keys.
    renamed = publish(5, [c, a, course("b", "30", "9999"), d])
    assert renamed["9999"] == before["9703"]
    assert "9703" not in renamed

    # Also cover published parallel courses sharing the same first element.
    # Seed that historical shape in the synthetic data; start UUIDs distinguish
    # A/B. A new end date reverses the old ROW_NUMBER tie-break order.
    shared_element = "a0000000-0000-0000-0000-000000000001"
    run_sql(
        r,
        "UPDATE arc.udredningsret_mont SET OBJEKTID_FORLOBELEMENT=? WHERE F_ID='9999'",
        shared_element,
    )
    run_sql(
        r,
        "UPDATE dim.UdredningsretEgenskaber SET foerste_forloebselement_uuid=? WHERE f_id='9999'",
        shared_element,
    )
    run_sql(
        r,
        "UPDATE arc.udredningsret_mont SET FORLOEBSMARKOER='AFV01X1', "
        "dw_timestamp_update=SYSDATETIME() WHERE F_ID='9702'",
    )
    koer_kaeden(r, "full_load")
    koer_dwh(r, "full_load")
    for fid, old_fid in [("9702", "9702"), ("9999", "9703")]:
        assert (
            scalar(
                r,
                "SELECT dw_ek_Udredningsforloeb FROM dim.UdredningsretEgenskaber WHERE f_id=?",
                fid,
            )
            == before[old_fid][0]
        )

    # Inconsistent historical identities must fail instead of choosing an
    # arbitrary permanent key. No fact has been changed by the failed staging.
    run_sql(
        r,
        "UPDATE dim.UdredningsretEgenskaber SET start_markoer_uuid="
        "(SELECT start_markoer_uuid FROM dim.UdredningsretEgenskaber WHERE f_id='9702') "
        "WHERE f_id='9999'",
    )
    with pytest.raises(Exception, match="Ambiguous udredningsforloeb identity"):
        koer_kaeden(r, "full_load")
    assert (
        scalar(
            r,
            "SELECT COUNT(*) FROM fct.Udredningsforloeb WHERE "
            "dw_ek_Udredningsforloeb IN (?, ?) AND dw_is_deleted=0",
            before["9702"][0],
            before["9703"][0],
        )
        == 2
    )


def test_seneste_fejlede_publicering_genforsoeges_selv_om_dsa_er_afklaret(dwh_db):  # noqa: F811
    r = dwh_db
    run_sql(r, "UPDATE fct.Udredningsforloeb SET dw_ek_Borger=42")
    signature, unresolved = read_state(r)
    assert signature is not None and unresolved == 0

    @dg.job(name="udredningsret_review_publish")
    def publish_job():
        pass

    with dg.DagsterInstance.ephemeral() as instance:
        instance.create_run_for_job(
            publish_job,
            status=dg.DagsterRunStatus.SUCCESS,
            tags={DELIVERY_SIGNATURE_TAG: signature},
        )
        instance.create_run_for_job(
            publish_job,
            status=dg.DagsterRunStatus.FAILURE,
            tags={DELIVERY_SIGNATURE_TAG: signature, UNRESOLVED_TAG: "1"},
        )
        context = SimpleNamespace(
            instance=instance,
            log=dg.get_dagster_logger(),
            dagster_run=SimpleNamespace(run_id="next-night"),
        )
        decision = evaluate(context, publish_job_name=publish_job.name, mssql=r)
        assert isinstance(decision, dg.SensorResult)
        assert decision.run_requests[0].tags[UNRESOLVED_TAG] == "1"
        # Only a complete successful retry allows the sensor to become idle.
        instance.create_run_for_job(
            publish_job,
            status=dg.DagsterRunStatus.SUCCESS,
            tags=dict(decision.run_requests[0].tags),
        )
        assert isinstance(
            evaluate(context, publish_job_name=publish_job.name, mssql=r), dg.SkipReason
        )

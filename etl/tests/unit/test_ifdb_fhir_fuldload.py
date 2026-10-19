# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Fuldindlæsning af IFDB -> FHIR-kæden: vinduesvalg, markør og trinkontrakt.

Fejl disse tests fanger:
- markøren rykkes, selv om et fatalt trin fejlede (historik springes over)
- vinduet vælges ikke som den ældste manglende måned, eller indeværende måned
  tages med, så fuldindlæsningen aldrig melder færdig
- et vindue rykker de checkpoints, den daglige kæde ejer, og ruller miljøet
  tilbage til vinduets slutdato
- verifikationsporten gøres fatal, så hvert eneste vindue fejler ved design
- et trin holder op med at matche assets, fordi navne eller grupper er ændret
- en måned, der ikke blev postet, rykker markøren, og næste vindues post arver den
- et vindue poster eller synker side om side med den daglige kæde
- et nyt vindue begynder, selv om det ikke når at blive færdigt før den daglige kæde
"""

from __future__ import annotations

import threading
from datetime import UTC, date, datetime, timedelta

import dagster as dg
import pytest
from infrastructure.assets import ifdb_fhir_fuldload as m

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Vinduer og markør
# ---------------------------------------------------------------------------


def test_uden_markoer_begynder_vi_ved_historikkens_start():
    vindue = m.naeste_vindue(None, date(2026, 9, 14))
    assert vindue == m.Vindue("2021-10-01", "2021-10-31")
    assert str(vindue) == "2021-10-01 -> 2021-10-31"


def test_markoeren_giver_maaneden_efter_og_rammer_skudaar_og_aarsskifte():
    assert m.naeste_vindue("2021-10-31", date(2026, 9, 14)).fra == "2021-11-01"
    assert m.naeste_vindue("2021-12-31", date(2026, 9, 14)) == m.Vindue("2022-01-01", "2022-01-31")
    assert m.naeste_vindue("2024-01-31", date(2026, 9, 14)) == m.Vindue("2024-02-01", "2024-02-29")
    # En markør midt i en måned peger stadig på den NÆSTE måned, aldrig på resten
    # af den samme: et halvt vindue er enten grønt eller ikke kørt.
    assert m.naeste_vindue("2022-03-07", date(2026, 9, 14)) == m.Vindue("2022-04-01", "2022-04-30")


def test_indevaerende_maaned_overlades_til_den_daglige_kaede():
    # Sidste hele måned er stadig et vindue ...
    assert m.naeste_vindue("2026-07-31", date(2026, 9, 14)) == m.Vindue("2026-08-01", "2026-08-31")
    # ... men når markøren har indhentet indeværende måned, er historikken inde.
    assert m.naeste_vindue("2026-08-31", date(2026, 9, 14)) is None
    assert m.naeste_vindue("2026-09-30", date(2026, 9, 14)) is None


def test_eksplicit_vindue_kraever_begge_ender():
    cfg = m.FuldloadConfig(vindue_fra="2022-05-01", vindue_til="2022-05-31")
    assert m.vindue_fra_config(cfg) == m.Vindue("2022-05-01", "2022-05-31")
    assert m.vindue_fra_config(m.FuldloadConfig()) is None
    with pytest.raises(dg.Failure, match="sammen eller slet ikke"):
        m.vindue_fra_config(m.FuldloadConfig(vindue_fra="2022-05-01"))


def test_tidsbudgettet_beregnes_een_gang_og_kan_ligge_i_morgen():
    start = datetime(2026, 9, 14, 18, 0)
    assert m.beregn_deadline("", start) is None
    assert m.beregn_deadline("06:00", start) == datetime(2026, 9, 15, 6, 0)
    assert m.beregn_deadline("23:30", start) == datetime(2026, 9, 14, 23, 30)


# ---------------------------------------------------------------------------
# Trinkontrakten
# ---------------------------------------------------------------------------


def test_trinnene_daekker_kaeden_i_rigtig_raekkefoelge():
    import dataclasses

    assert [t.navn for t in m.TRIN] == [
        "arkiv",
        "window_reconciliation",
        "patienter",
        "post",
        "sof_dso",
        "verifikation",
    ]
    arkiv, afstemning, patienter, post, sof_dso, verifikation = m.TRIN

    # Arkivet henter vinduets historiske måned på kildens egen delta-kolonne.
    assert arkiv.grupper == ("ifdb_archive",)
    assert arkiv.mode == "period"
    # The reconciliation has its own group, so the archive step needs no exclusion.
    assert arkiv.undtag == ()

    # Afstemningen beviser vinduets egen måned mod IFDB, nøgle for nøgle. Den er
    # fatal med vilje: er måneden ikke komplet i arkivet, må markøren ikke rykke,
    # for så ville et hul blive passeret og først opdaget, når alt var kørt.
    assert afstemning.assets == (m._WINDOW_RECONCILIATION,)
    assert afstemning.mode == "period" and afstemning.fatal is True

    # Anden halvdel udpeges ved gruppe og asset, aldrig ved jobnavn: et job kan
    # slås sammen eller omdøbes uden merge-konflikt, og så fejler hvert vindue.
    assert "job" not in {f.name for f in dataclasses.fields(m.Trin)}
    assert "fhir_sof_dfm" in sof_dso.grupper and "fhir_dso_sync_dfm" in sof_dso.grupper
    assert "dso_sync_dim_borger" in sof_dso.assets and sof_dso.mode == ""

    # Verifikationsporten fejler ved design på kendte brud og må ikke være fatal.
    assert verifikation.fatal is False
    assert all(t.fatal for t in m.TRIN if t.navn != "verifikation")


def test_posten_koerer_byggerne_i_deres_egen_delta():
    """Posten må ALDRIG køre i vinduets periode.

    Byggerne vælger arkivrækker på dw_timestamp_update — hvornår lageret skrev
    dem. Hele arkivet er skrevet i 2026, så en periode for en historisk måned
    finder intet at poste: vinduet arkiverede, men postede ikke, og morgenens
    daglige kørsel arvede hele nattens arbejde (12.987.166 rækker og ni timer
    den 27/9). I delta læser hver bygger alt, den ikke har postet endnu, og
    rykker sit eget checkpoint til nu — som hver morgen.

    Fatal: fejler en bygger, bliver dens checkpoint stående, og rykkede markøren
    alligevel, ville næste vindues post arve den ikke-postede måned oven i sin egen.
    """
    post = next(t for t in m.TRIN if t.navn == "post")
    assert post.grupper == ("fhir_ifdb_dfm",)
    assert post.mode == "", "posten skal køre i byggernes egen delta, ikke i en periode"
    # Posten skal ligge efter afstemningen: et ukomplet arkiv må ikke postes.
    navne = [t.navn for t in m.TRIN]
    assert navne.index("window_reconciliation") < navne.index("post") < navne.index("sof_dso")


def test_patienterne_hentes_i_delta_lige_foer_posten():
    """Posten læser alt uposteret i arkivet, også rækker fra andre kørsler.

    En Encounter, hvis patient endnu ikke er arkiveret, stopper posten og dermed
    vinduet. Patient-kæden frem til foreningen kører derfor i delta umiddelbart før.
    """
    patienter = next(t for t in m.TRIN if t.navn == "patienter")
    assert patienter.assets == ("ext_ifdb_patient", "arc_patient_ifdb", "ref_patient_identity")
    assert patienter.mode == "" and patienter.fatal is True
    navne = [t.navn for t in m.TRIN]
    assert navne.index("patienter") + 1 == navne.index("post")
    assert "patienter" in m._STEPS_EXCLUSIVE_WITH_DAILY


def test_et_vindue_kan_ikke_rykke_den_daglige_kaedes_checkpoints():
    """Den fejl, der rullede dev tre år tilbage: vinduet skrev sin slutdato ind i
    de checkpoints, den daglige kæde ejer. Trin kan ikke længere bede om det."""
    import dataclasses

    assert "advance_checkpoint" not in {f.name for f in dataclasses.fields(m.Trin)}

    kilde = (m.__file__ or "").replace(".pyc", ".py")
    tekst = open(kilde, encoding="utf-8").read()
    assert "advance_checkpoint=False" in tekst
    assert "advance_checkpoint=True" not in tekst


def test_noegleudvaelgelse_matcher_gruppe_og_undtagelse():
    k = dg.AssetKey
    grupper = {
        k("arc_henvisning_ifdb"): "ifdb_archive",
        k("fhir_server_questionnaire_dfm"): "fhir_ifdb_dfm",
        k("fhir_server_procedure_ris_ydelse_dfm"): "fhir_ifdb_dfm",
        k("sof_observation_dfm"): "fhir_sof_dfm",
        k("sfi_genkoersel_verifikation"): "fhir_operations",
    }
    arkiv_og_post = m.udvaelg_noegler(grupper, grupper=("ifdb_archive", "fhir_ifdb_dfm"))
    assert {n.path[-1] for n in arkiv_og_post} == {
        "arc_henvisning_ifdb",
        "fhir_server_questionnaire_dfm",
        "fhir_server_procedure_ris_ydelse_dfm",
    }, "RIS følger vinduet som de andre postere; SOF hører ikke til her"

    uden_ris = m.udvaelg_noegler(
        grupper,
        grupper=("ifdb_archive", "fhir_ifdb_dfm"),
        undtag=("fhir_server_procedure_ris_ydelse_dfm",),
    )
    assert len(uden_ris) == 2

    assert len(m.udvaelg_noegler(grupper, assets=(m._VERIFIKATION_ASSET,))) == 1


def test_navnematch_er_praecist_og_taaler_branch_postfix(monkeypatch):
    """Et præfiksmatch ville lade dim_Borger hive dim_Borger_historik med."""
    k = dg.AssetKey
    faelde = {k("dim_Borger"): "borger", k("dim_Borger_historik"): "borger"}
    valgt = m.udvaelg_noegler(faelde, assets=("dim_Borger",))
    assert {n.path[-1] for n in valgt} == {"dim_Borger"}

    # På en feature-location hedder asset'et det samme plus branchens slug.
    monkeypatch.setattr(m, "get_postfixed_name", lambda navn: f"{navn}_feature_min_gren")
    postfixet = {
        k("dim_Borger_feature_min_gren"): "borger",
        k("dim_Borger_historik_feature_min_gren"): "borger",
    }
    valgt = m.udvaelg_noegler(postfixet, assets=("dim_Borger",))
    assert {n.path[-1] for n in valgt} == {"dim_Borger_feature_min_gren"}


def test_hvert_trins_maal_findes_i_definitionerne():
    """Fejlen, der ville have ramt efter merge: et trin peger på en gruppe eller et
    asset, som er omdøbt eller slettet. Dagster opløser en ukendt gruppe til en tom
    markering uden at fejle, så vinduet ville bare køre ingenting eller vælte."""
    from core.branch_postfix import get_postfixed_name
    from definitions import defs

    graf = defs.resolve_asset_graph()
    noegler = list(graf.get_all_asset_keys())
    kendte_grupper = {graf.get(n).group_name for n in noegler}
    kendte_navne = {n.path[-1] for n in noegler}

    for trin in m.TRIN:
        for gruppe in trin.grupper:
            assert gruppe in kendte_grupper, f"trin {trin.navn}: gruppen {gruppe!r} findes ikke"
        for navn in trin.assets:
            assert (
                navn in kendte_navne or get_postfixed_name(navn) in kendte_navne
            ), f"trin {trin.navn}: asset {navn!r} findes ikke"


# ---------------------------------------------------------------------------
# Grønt vindue = markøren må rykkes
# ---------------------------------------------------------------------------


def _res(navn: str, ok: bool) -> m.TrinResultat:
    return m.TrinResultat(navn=navn, ok=ok, run_id="r" * 8, sekunder=60, fejl="" if ok else "boom")


def test_alle_trin_groenne_er_et_groent_vindue():
    resultater = [_res(t.navn, True) for t in m.TRIN]
    assert m.vinduet_er_groent(resultater, m.FuldloadConfig()) is True


def test_fatalt_trin_roedt_er_ikke_groent():
    resultater = [_res("arkiv", False)]
    assert m.vinduet_er_groent(resultater, m.FuldloadConfig()) is False
    # ... og et manglende trin tæller heller ikke som grønt
    assert m.vinduet_er_groent([_res("arkiv", True)], m.FuldloadConfig()) is False


def test_roed_verifikation_er_groen_medmindre_man_beder_om_andet():
    resultater = [_res(t.navn, t.navn != "verifikation") for t in m.TRIN]
    assert m.vinduet_er_groent(resultater, m.FuldloadConfig()) is True
    assert (
        m.vinduet_er_groent(resultater, m.FuldloadConfig(stop_ved_roed_verifikation=True)) is False
    )


def test_loekken_stopper_ved_foerste_fatale_fejl_men_koerer_videre_efter_roed_port(monkeypatch):
    kørte: list[str] = []

    def fake(context, trin, vindue, nr):
        kørte.append(trin.navn)
        return _res(trin.navn, trin.navn != "arkiv")

    monkeypatch.setattr(m, "_koer_trin", fake)
    ctx = type("C", (), {"log": type("L", (), {"warning": lambda *a, **k: None})()})()
    m.koer_vindue(ctx, m.Vindue("2022-01-01", "2022-01-31"), m.FuldloadConfig())
    assert kørte == ["arkiv"], "et fatalt trin stopper vinduet med det samme"

    kørte.clear()
    monkeypatch.setattr(m, "_koer_trin", lambda c, t, v, n: _res(t.navn, t.navn != "verifikation"))
    resultater = m.koer_vindue(ctx, m.Vindue("2022-01-01", "2022-01-31"), m.FuldloadConfig())
    assert [r.navn for r in resultater] == [t.navn for t in m.TRIN]


def test_markoeren_er_sin_egen_raekke_adskilt_fra_de_daglige_checkpoints():
    # Scope skal være noget andet end den daglige kædes, ellers kan en daglig kørsel
    # skubbe markøren frem til nu, og historikken springes over.
    assert m.MARKOER_SCOPE == "backfill"
    assert (m.MARKOER_SCHEMA, m.MARKOER_TABEL) == ("arc", "ifdb_fuldload")


def _ctx_with_runs(runs):
    instance = type("I", (), {"get_runs": lambda self, filters=None: runs})()
    warnings: list = []
    log = type("L", (), {"warning": lambda *a, **k: warnings.append(a)})()
    return type("C", (), {"instance": instance, "log": log})()


def test_window_stops_before_the_patient_step_while_a_daily_run_is_in_progress(monkeypatch):
    """The two read and write the same archive; the run queue cannot keep them apart."""
    daily = type("R", (), {"job_name": "ifdb_dfm_daily_job_feature_development", "run_id": "d1"})()
    ran: list[str] = []

    def fake(context, trin, vindue, nr):
        ran.append(trin.navn)
        return _res(trin.navn, True)

    monkeypatch.setattr(m, "_koer_trin", fake)
    results = m.koer_vindue(
        _ctx_with_runs([daily]), m.Vindue("2022-01-01", "2022-01-31"), m.FuldloadConfig()
    )

    assert ran == ["arkiv", "window_reconciliation"], "nothing may run next to the daily chain"
    assert results[-1].navn == "patienter" and "d1" in results[-1].fejl
    assert m.stopped_for_daily(results)
    assert m.vinduet_er_groent(results, m.FuldloadConfig()) is False


def test_window_stops_before_sof_dso_when_the_daily_run_starts_during_post(monkeypatch):
    daily = type("R", (), {"job_name": "ifdb_dfm_daily_job", "run_id": "d2"})()
    ran: list[str] = []

    def fake(context, trin, vindue, nr):
        ran.append(trin.navn)
        if trin.navn == "post":
            active.append(daily)
        return _res(trin.navn, True)

    active: list = []
    monkeypatch.setattr(m, "_koer_trin", fake)
    results = m.koer_vindue(
        _ctx_with_runs(active), m.Vindue("2022-01-01", "2022-01-31"), m.FuldloadConfig()
    )

    assert ran == ["arkiv", "window_reconciliation", "patienter", "post"]
    assert m.stopped_for_daily(results) and results[-1].navn == "sof_dso"


def test_a_failed_step_is_not_a_stop_for_the_daily_chain():
    assert m.stopped_for_daily([_res("arkiv", False)]) is False
    assert m.stopped_for_daily([]) is False


def test_the_deadline_watch_stops_a_window_that_runs_past_the_budget():
    """The daily chain does not wait for the fuldload, so an overrunning window must end itself."""
    stopped = threading.Event()
    assert m.deadline_vagt(None, datetime(2026, 10, 9, 22, 0), stopped.set) is None

    watch = m.deadline_vagt(
        datetime(2026, 10, 10, 5, 30), datetime(2026, 10, 9, 22, 0), stopped.set
    )
    assert watch.interval == 7.5 * 3600 and watch.daemon

    overdue = m.deadline_vagt(
        datetime(2026, 10, 9, 5, 30), datetime(2026, 10, 9, 6, 0), stopped.set
    )
    assert overdue.interval == 0.0
    overdue.start()
    assert stopped.wait(2), "a deadline already passed must stop the run at once"


def test_a_new_window_starts_only_if_it_can_finish_before_the_deadline():
    deadline = datetime(2026, 10, 2, 5, 30)
    three_and_a_half_hours = 3.5 * 3600
    assert m.naeste_vindue_naas(datetime(2026, 10, 2, 1, 45), three_and_a_half_hours, deadline)
    assert not m.naeste_vindue_naas(datetime(2026, 10, 2, 2, 1), three_and_a_half_hours, deadline)


def test_a_daily_run_older_than_a_day_does_not_count_as_active():
    """A run left in STARTED by a dead worker must not stop every night."""
    seen: list = []
    instance = type("I", (), {"get_runs": lambda self, filters=None: seen.append(filters) or []})()
    context = type("C", (), {"instance": instance})()

    assert m._daily_run_in_progress(context) is None
    age = datetime.now(UTC) - seen[0].created_after
    assert timedelta(hours=23) < age < timedelta(hours=25)


def test_post_runs_when_no_daily_run_is_active(monkeypatch):
    other = type("R", (), {"job_name": "erp_daily_dfm_job", "run_id": "e1"})()
    ran: list[str] = []

    def fake(context, trin, vindue, nr):
        ran.append(trin.navn)
        return _res(trin.navn, True)

    monkeypatch.setattr(m, "_koer_trin", fake)
    m.koer_vindue(_ctx_with_runs([other]), m.Vindue("2022-01-01", "2022-01-31"), m.FuldloadConfig())
    assert "post" in ran

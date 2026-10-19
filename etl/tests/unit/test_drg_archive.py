# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the DRG archive layer (DSI/csv_files/drg_archive.py).

View/merge-config/DDL column-set parity, year-slice delete-scope SQL, and
guard behaviour (direction, references, uniqueness) against a fake connection.
"""

import re
from datetime import date

import pytest
from DSI.csv_files import drg_archive
from DSI.csv_files.drg_datasets import DATASETS

pytestmark = pytest.mark.unit

KONTAKT = next(d for d in DATASETS if d.ext_table == "drg_kontakt")
FORLOEB = next(d for d in DATASETS if d.ext_table == "drg_sghforlob")
PSYK_KONTAKT = next(d for d in DATASETS if d.ext_table == "drgpsyk_kontakt")
INFO = next(d for d in DATASETS if d.ext_table == "drg_leveranceinfo")


class FakeCursor:
    """Plays back prepared results keyed by SQL fragment."""

    def __init__(self, responses):
        self.responses = responses
        self.executed: list[str] = []
        self._current = None
        self.rowcount = 0

    def execute(self, sql, *params):
        self.executed.append(" ".join(sql.split()))
        self._current = None
        for fragment, rows in self.responses.items():
            if fragment in self.executed[-1]:
                self._current = rows
                return

    def fetchall(self):
        return self._current or []

    def fetchone(self):
        return (self._current or [(0,)])[0]

    def close(self):
        pass


class FakeConn:
    def __init__(self, responses):
        self._responses = responses
        self.cursors: list[FakeCursor] = []

    def cursor(self):
        cursor = FakeCursor(self._responses)
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        pass


# ---------------------------------------------------------------------------
# Consistency: view <-> merge-config <-> spec
# ---------------------------------------------------------------------------


def test_alle_datasaet_har_view_og_merge_config():
    for dataset in DATASETS:
        assert dataset.ext_table in drg_archive._VIEW_JOINS
        assert dataset.ext_table in drg_archive.MERGE_CONFIGS


def test_view_joins_daekker_alle_tilfoejede_kolonner():
    for dataset in DATASETS:
        join = drg_archive._VIEW_JOINS[dataset.ext_table]
        assert set(join.added_expressions) == {
            a.name for a in dataset.arc_added
        }, f"{dataset.ext_table}: viewets udtryk matcher ikke spec'ens arc_added"


def _view_column_aliases(sql: str) -> list[str]:
    aliases = []
    for line in sql.splitlines():
        line = line.strip().rstrip(",")
        match = re.search(r"AS \[(\w+)\]$", line)
        if match:
            aliases.append(match.group(1))
        elif line == "e.[takstsystem]":
            aliases.append("takstsystem")
        elif line == "e.[leverance_dato]":
            aliases.append("leverance_dato")
    return aliases


def test_view_select_matcher_arc_kolonnerne():
    """The view's columns must match the arc table (minus the dw tail)."""
    for dataset in DATASETS:
        sql = drg_archive.build_view_sql(dataset)
        aliases = _view_column_aliases(sql)
        expected = ["takstsystem", *dataset.arc_columns, "leverance_dato"]
        assert aliases == expected, f"{dataset.ext_table}:\n  view: {aliases}\n  spec: {expected}"


def test_merge_config_daekker_alle_arc_kolonner():
    for dataset in DATASETS:
        config = drg_archive.MERGE_CONFIGS[dataset.ext_table]
        written = set(config.merge_keys) | set(config.non_key_columns)
        expected = {"takstsystem", *dataset.arc_columns, "leverance_dato"}
        assert written == expected, f"{dataset.ext_table}: merge-configen taber kolonner"
        assert config.upstream_is_full_load is True
        assert "leverance_dato" not in (config.change_detect_columns or ())


def test_leveranceinfo_er_log_uden_delete_detection():
    config = drg_archive.MERGE_CONFIGS["drg_leveranceinfo"]
    assert config.detect_deletes is False
    assert config.merge_keys == ("takstsystem", "DATAOMRAADE", "LPROPDATERINGSDATO")


def test_merge_keys_kolonner_er_isnull_wrappede_i_view():
    sql = drg_archive.build_view_sql(KONTAKT)
    assert "ISNULL(e.[KONTAKT_VAERDI], '') AS [KONTAKT_VAERDI]" in sql


def test_view_joins_er_takstsystem_scopede():
    """Integer IDs are unique only within a delivery — every join MUST also
    match on takstsystem."""
    for dataset in DATASETS:
        join = drg_archive._VIEW_JOINS[dataset.ext_table]
        join_count = join.from_clause.count("JOIN ")
        scope_count = join.from_clause.count(".[takstsystem] =")
        assert scope_count >= join_count, (
            f"{dataset.ext_table}: {join_count} joins men kun {scope_count} "
            f"takstsystem-scopes i view-joinen"
        )


# ---------------------------------------------------------------------------
# Year-slice scope
# ---------------------------------------------------------------------------


def test_delete_scope_somatik_pr_aarsskive():
    conn = FakeConn(
        {
            "GROUP BY [takstsystem], [AKTIVITETSAAR]": [
                ("T25", "2024"),
                ("T25", "2025"),
                ("T26", "2025"),
            ]
        }
    )
    predicate = drg_archive.build_slice_predicate(
        KONTAKT, drg_archive.read_ext_slices(conn, KONTAKT)
    )
    assert (
        "(tgt.[takstsystem] = 'T25' AND ISNULL(tgt.[AKTIVITETSAAR], '') IN ('2024', '2025'))"
        in predicate
    )
    assert (
        "(tgt.[takstsystem] = 'T26' AND ISNULL(tgt.[AKTIVITETSAAR], '') IN ('2025'))" in predicate
    )
    assert " OR " in predicate


def test_delete_scope_psyk_pr_takstsystem():
    conn = FakeConn({"SELECT DISTINCT [takstsystem]": [("T25",), ("T26",)]})
    predicate = drg_archive.build_slice_predicate(
        PSYK_KONTAKT, drg_archive.read_ext_slices(conn, PSYK_KONTAKT)
    )
    assert predicate == "tgt.[takstsystem] IN ('T25', 'T26')"


def test_delete_scope_tom_ext_giver_none():
    conn = FakeConn({})
    assert (
        drg_archive.build_slice_predicate(KONTAKT, drg_archive.read_ext_slices(conn, KONTAKT))
        is None
    )
    assert (
        drg_archive.build_slice_predicate(
            PSYK_KONTAKT, drg_archive.read_ext_slices(conn, PSYK_KONTAKT)
        )
        is None
    )


def test_aarsskive_afvises_foer_merge_hvis_den_ikke_kan_registreres():
    """aktivitetsaar er en snaever noeglekolonne i meta.DRGLeverance. En
    for lang vaerdi skal fejle FOER mergen — ellers committer mergen og
    registreringen af dens cap fejler bagefter."""
    conn = FakeConn({"GROUP BY [takstsystem], [AKTIVITETSAAR]": [("T26", "2025' OR 1=1 --")]})
    with pytest.raises(ValueError, match="aktivitetsaar er"):
        drg_archive.build_slice_predicate(KONTAKT, drg_archive.read_ext_slices(conn, KONTAKT))


def test_delete_scope_escaper_apostrof_i_vaerdier():
    """Vaerdi-escaping er stadig et vaern i sig selv, uafhaengigt af
    bredde-checket ovenfor."""
    predicate = drg_archive.build_slice_predicate(KONTAKT, {"T26": ["25' OR"]}, alias="tgt")
    assert "25' OR 1=1" not in predicate
    assert "'25'' OR'" in predicate
    assert "tgt.[takstsystem] = 'T26'" in predicate


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


def test_retningsguard_afviser_aeldre_leverance():
    conn = FakeConn(
        {
            "FROM [ext].[drg_kontakt] GROUP BY": [("T26", date(2026, 8, 1))],
            "FROM [meta].[DRGLeverance]": [("T26", date(2026, 8, 20))],
        }
    )
    with pytest.raises(ValueError, match="aeldre.*leverance|maa ikke merges"):
        drg_archive.guard_delivery_direction(conn, KONTAKT)


def test_retningsguard_tillader_nyere_og_samme_leverance():
    conn = FakeConn(
        {
            "FROM [ext].[drg_kontakt] GROUP BY": [
                ("T26", date(2026, 8, 24)),
                ("T24", date(2026, 3, 17)),
            ],
            "FROM [meta].[DRGLeverance]": [("T26", date(2026, 8, 16)), ("T24", date(2026, 3, 17))],
        }
    )
    ext_dates = drg_archive.guard_delivery_direction(conn, KONTAKT)
    assert ext_dates == {"T26": "2026-08-24", "T24": "2026-03-17"}


def test_referenceguard_fejler_ved_tabte_raekker():
    """INNER JOINs drop unresolvable references SILENTLY — the guard is row
    parity between ext and the view, not a NULL check (NULL cannot come from
    an INNER JOIN)."""
    conn = FakeConn({"FROM [ext].[v_drg_sghforlob]": [(42,)]})
    with pytest.raises(ValueError, match="42.*tabt"):
        drg_archive.guard_resolved_references(conn, FORLOEB)


def test_referenceguard_fejler_ved_duplikerede_raekker():
    conn = FakeConn({"FROM [ext].[v_drg_diagnose]": [(-3,)]})
    diagnose = next(d for d in DATASETS if d.ext_table == "drg_diagnose")
    with pytest.raises(ValueError, match="3.*duplikeret"):
        drg_archive.guard_resolved_references(conn, diagnose)


def test_referenceguard_er_noop_uden_joins():
    conn = FakeConn({})
    drg_archive.guard_resolved_references(conn, INFO)  # no joins -> no query
    assert conn.cursors == []


def test_unikhedsguard_fejler_ved_KONFLIKTENDE_dubletter():
    """Samme noegle, forskellige vaerdier: en dedup ville afgoere hvad arkivet
    tror paa — det skal fejle."""
    conn = FakeConn({"WITH dubletter AS": [(3, 0)]})  # 3 konfliktende, 0 identiske
    with pytest.raises(ValueError, match="FORSKELLIGE vaerdier"):
        drg_archive.guard_key_uniqueness(conn, KONTAKT)


def test_unikhedsguard_tillader_BYTE_IDENTISKE_dubletter():
    """Samme raekke leveret to gange: intet tabes ved at beholde én kopi, og
    at fejle hele tabellen ville arkivere ingenting. Maalt i dev 2026-08-27:
    SDS leverede ét saadant par i T24 psyk-diagnoser."""
    conn = FakeConn({"WITH dubletter AS": [(0, 1)]})  # 0 konfliktende, 1 identisk
    assert drg_archive.guard_key_uniqueness(conn, KONTAKT) == 1


def test_unikhedsguard_fejler_naar_BEGGE_slags_findes():
    """Én identisk dublet retfaerdiggoer ikke at en konfliktende slipper med."""
    conn = FakeConn({"WITH dubletter AS": [(2, 5)]})
    with pytest.raises(ValueError, match="2 noeglegrupper"):
        drg_archive.guard_key_uniqueness(conn, KONTAKT)


def test_unikhedsguard_uden_dubletter_returnerer_nul():
    conn = FakeConn({"WITH dubletter AS": [(None, None)]})  # SUM over tom maengde
    assert drg_archive.guard_key_uniqueness(conn, KONTAKT) == 0


def test_unikhedsguard_sammenligner_de_FULDE_raekker():
    """Skelnen kraever at hele indholdet sammenlignes, ikke kun noeglen —
    assertes paa DISTINCT-segmentet, hvor noeglekolonnerne altid staar."""
    conn = FakeConn({"WITH dubletter AS": [(0, 0)]})
    drg_archive.guard_key_uniqueness(conn, KONTAKT)
    sql = conn.cursors[0].executed[0]
    assert "SELECT DISTINCT" in sql
    distinkt = sql[sql.index("SELECT DISTINCT") :]
    for column in ("[TOTALPRIS_DRG]", "[leverance_dato]"):
        assert column in distinkt, f"{column} mangler i indholds-sammenligningen"


def test_unikhedsguard_ignorerer_registreringsid_i_sammenligningen():
    """Kildens raekke-uuid baerer intet leveranceindhold: to raekker der kun
    afviger paa den er samme registrering gentaget, ikke en konflikt."""
    diagnose = next(d for d in DATASETS if d.ext_table == "drg_diagnose")
    conn = FakeConn({"WITH dubletter AS": [(0, 974)]})
    assert drg_archive.guard_key_uniqueness(conn, diagnose) == 974
    sql = conn.cursors[0].executed[0]
    distinkt = sql[sql.index("SELECT DISTINCT") :]
    assert "[DIAGNOSE_VAERDI]" not in distinkt
    for column in ("[Aktivitetsaar]", "[leverance_dato]"):
        assert column in distinkt, f"{column} mangler i indholds-sammenligningen"


def test_registreringsid_kolonner_er_konsistent_deklareret():
    """Et registrerings-id skal vaere en arc-kolonne og maa aldrig indgaa i
    merge-noeglen — ellers ville guarden ignorere en del af identiteten."""
    for dataset in DATASETS:
        ekstra = set(dataset.registreringsid_columns) - set(dataset.arc_columns)
        assert not ekstra, f"{dataset.ext_table}: ukendte kolonner {ekstra}"
        overlap = set(dataset.registreringsid_columns) & set(dataset.merge_keys)
        assert not overlap, f"{dataset.ext_table}: noeglekolonner {overlap}"


def test_count_live_in_scope_fjerner_tgt_praefiks():
    """The live-row count queries the arc table directly — the scope
    predicate's tgt. qualifiers must be stripped."""
    conn = FakeConn({"WHERE [dw_is_deleted] = 0": [(1234,)]})
    scope = "(tgt.[takstsystem] = 'T26' AND ISNULL(tgt.[AKTIVITETSAAR], '') IN ('2025'))"
    live = drg_archive.count_live_in_scope(conn, KONTAKT, scope)
    assert live == 1234
    executed = conn.cursors[0].executed[0]
    assert "tgt." not in executed
    assert "ISNULL([AKTIVITETSAAR], '') IN ('2025')" in executed
    assert "FROM [arc].[drg_kontakt]" in executed


def test_pris_revision_insert_er_idempotent():
    """The revision log is committed before the merge — if the merge fails, a
    retry must not double-count the revision (the NOT EXISTS guard)."""
    conn = FakeConn({})
    drg_archive.capture_price_revisions(conn, KONTAKT)
    executed = conn.cursors[0].executed
    assert len(executed) == len(KONTAKT.revision_columns)
    for sql in executed:
        assert "NOT EXISTS" in sql
        assert "INTERSECT" in sql  # NULL-sikker vaerdi-sammenligning


# ---------------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------------


def test_alle_arkiv_assets_er_bygget_med_korrekt_dep():
    assets = {a.key.to_user_string(): a for a in drg_archive.drg_archive_assets}
    # Én pr. datasaet plus ét checkpoint-gate pr. leverancetype.
    assert len(assets) == len(DATASETS) + 2
    somatik_deps = {d.to_user_string() for d in assets["arc_drg_kontakt"].dependency_keys}
    assert "ext_drg" in somatik_deps
    psyk_deps = {d.to_user_string() for d in assets["arc_drgpsyk_kontakt"].dependency_keys}
    assert "ext_drgpsyk" in psyk_deps


def test_gate_afhaenger_af_HVER_arkivtabel_i_sin_leverancetype():
    """Grafen er det eneste sted, hvor "alle tabeller merged i denne koersel"
    kan laeses. Mangler én dep, kan checkpointet rykke selvom den tabel
    fejlede — og leverancen ville aldrig blive genlaest."""
    assets = {a.key.to_user_string(): a for a in drg_archive.drg_archive_assets}

    for leverancetype, gate in (
        ("DRG", "drg_leverance_checkpoint_drg"),
        ("DRGPSYK", "drg_leverance_checkpoint_drgpsyk"),
    ):
        forventet = {f"arc_{d.ext_table}" for d in DATASETS if d.leverancetype == leverancetype}
        faktisk = {d.to_user_string() for d in assets[gate].dependency_keys}
        assert faktisk == forventet, leverancetype


def test_gate_blander_ikke_leverancetyperne():
    """Et faelles gate ville lade en somatik-fejl blokere psyks checkpoint."""
    assets = {a.key.to_user_string(): a for a in drg_archive.drg_archive_assets}
    somatik = {d.to_user_string() for d in assets["drg_leverance_checkpoint_drg"].dependency_keys}
    assert not any(d.startswith("arc_drgpsyk") for d in somatik)


def test_backfill_findes_ikke_laengere():
    """backfill_historical er fjernet for DRG. Arkivet kan toemmes og
    genindlaeses fra leverancerne ("toem + delivery_on_or_before"), hvilket
    holder leveranceregistret konsistent — en backfill kunne ikke: registret
    ville staa paa den nyere cap, og skive-sammenligningen ville naegte at
    genindlaese den nyeste leverance."""
    assert not hasattr(drg_archive.DrgArchiveConfig(), "backfill_historical")

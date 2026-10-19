# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""ref.PatientIdentity with fakes: DFM control, stop on contradiction and the chain SQL."""

import sqlite3
from unittest.mock import MagicMock

import dagster as dg
import pytest
from core.dfm_config import RunModeConfig
from DSA.SQL.ref.patient_identity import (
    BYG_SQL,
    FLET_SQL,
    KAEDE_KONTROL_SQL,
    KONTROL_SQL,
    SYSTEM_CPR,
    SYSTEM_EPJ,
    ref_patient_identity,
)

pytestmark = pytest.mark.unit


class _Db:
    """Staging as a fake: remembers the statements and answers the counts."""

    def __init__(
        self, bygget=4, modstrid=0, i_alt=(4, 2, 1), kaedefejl=0, tvetydige=0, fra_epj=None
    ):
        self.sql: list[str] = []
        self._svar = [(bygget, bygget if fra_epj is None else fra_epj), (modstrid,), i_alt]
        self._kaedefejl = kaedefejl
        self._tvetydige = tvetydige
        self._naeste = 0
        self.committet = False

    def get_connection(self):
        ydre = self

        class _Cur:
            rowcount = 3

            def execute(self, sql, params=None):
                ydre.sql.append(sql)

            def fetchone(self):
                if "SELECT COUNT(DISTINCT k.start_pnr)" in ydre.sql[-1]:
                    return (ydre._kaedefejl,)
                if "SELECT COUNT(*) FROM tvetydig" in ydre.sql[-1]:
                    return (ydre._tvetydige,)
                if "FROM #PatientIdentity" in ydre.sql[-1] and "SUM(CASE" in ydre.sql[-1]:
                    return ydre._svar[0]
                if "HAVING COUNT(DISTINCT person_key)" in ydre.sql[-1]:
                    return ydre._svar[1]
                return ydre._svar[2]

            def close(self):
                pass

        class _Conn:
            def cursor(self):
                return _Cur()

            def commit(self):
                ydre.committet = True

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        return _Conn()


def _koer(db, monkeypatch):
    dfm = MagicMock()
    dfm.start_dfm_run.return_value = MagicMock(log_id=7)
    monkeypatch.setattr("DSA.SQL.ref.patient_identity.DfmManager", lambda *a, **k: dfm)
    resultat = ref_patient_identity(
        context=dg.build_asset_context(),
        mssql_target_staging=db,
        config=RunModeConfig(mode="full_load"),
    )
    return resultat, dfm


def test_builds_from_scratch_and_reports_counts_of_persons_not_the_persons(monkeypatch):
    db = _Db(bygget=4, modstrid=0, i_alt=(4, 2, 1))
    resultat, dfm = _koer(db, monkeypatch)
    assert resultat.metadata["identities"] == 4
    assert resultat.metadata["persons"] == 2
    assert resultat.metadata["non_current_identities"] == 1
    assert resultat.metadata["identities_per_person"] == 2.0
    assert db.committet
    # The DFM run is controlled as in the rest of the ref layer.
    assert dfm.start_dfm_run.call_args.args == ("ref", "PatientIdentity")
    assert dfm.complete_dfm_run.called and not dfm.fail_dfm_run.called


def test_a_contradiction_in_the_source_stops_before_the_table_is_touched(monkeypatch):
    """One identity on two persons must not be merged in on top of a guess."""
    db = _Db(bygget=4, modstrid=2)
    with pytest.raises(dg.Failure, match="contradict"):
        _koer(db, monkeypatch)
    assert not any(s.strip().startswith("MERGE") for s in db.sql)
    assert not db.committet


def test_an_error_marks_the_dfm_run_as_failed(monkeypatch):
    class _Spraengt(_Db):
        def get_connection(self):
            raise RuntimeError("staging does not respond")

    dfm = MagicMock()
    dfm.start_dfm_run.return_value = MagicMock(log_id=7)
    monkeypatch.setattr("DSA.SQL.ref.patient_identity.DfmManager", lambda *a, **k: dfm)
    with pytest.raises(RuntimeError):
        ref_patient_identity(
            context=dg.build_asset_context(),
            mssql_target_staging=_Spraengt(),
            config=RunModeConfig(mode="full_load"),
        )
    assert dfm.fail_dfm_run.called and not dfm.complete_dfm_run.called


def test_query_follows_chains_and_keeps_the_systems_apart():
    # The chain is followed recursively with an explicit limit, so a cycle in the source
    # stops the query instead of running forever.
    assert "historik AS (" in BYG_SQL and "kaede k JOIN entydig e" in BYG_SQL
    assert "k.dybde < 10" in BYG_SQL
    # A number with two current ones is not followed and not linked to either of them.
    assert "HAVING COUNT(DISTINCT pnrgaeld) = 1" in BYG_SQL
    assert "WHERE h.pnr NOT IN (SELECT pnr FROM tvetydig)" in BYG_SQL
    # A cycle reaches itself; a chain that continues at the limit would be cut off.
    assert "k.naaet = k.start_pnr" in KAEDE_KONTROL_SQL
    assert SYSTEM_CPR in BYG_SQL and SYSTEM_EPJ in BYG_SQL
    assert "HAVING COUNT(DISTINCT person_key) > 1" in KONTROL_SQL
    # The merge removes what the sources no longer have.
    assert "WHEN NOT MATCHED BY SOURCE" in FLET_SQL and "DELETE" in FLET_SQL


def test_a_cycle_or_a_too_long_chain_stops_before_the_table_is_touched(monkeypatch):
    db = _Db(kaedefejl=1)
    with pytest.raises(dg.Failure, match="cycle"):
        _koer(db, monkeypatch)
    assert not any(s.strip().startswith("MERGE") for s in db.sql)
    assert not db.committet


def test_ambiguous_numbers_are_counted_but_do_not_stop_the_build(monkeypatch):
    resultat, _ = _koer(_Db(tvetydige=3), monkeypatch)
    assert resultat.metadata["ambiguous_historical_numbers"] == 3


def test_an_empty_union_does_not_delete_the_table(monkeypatch):
    db = _Db(bygget=0)
    with pytest.raises(dg.Failure, match="empty"):
        _koer(db, monkeypatch)
    assert not any(s.strip().startswith("MERGE") for s in db.sql)


def test_a_union_without_epj_identities_does_not_delete_the_table(monkeypatch):
    """022 alone gives rows, but an empty EPJ archive must not empty the EPJ identities."""
    db = _Db(bygget=3, fra_epj=0)
    with pytest.raises(dg.Failure, match="EPJ identities"):
        _koer(db, monkeypatch)
    assert not any(s.strip().startswith("MERGE") for s in db.sql)


def test_union_waits_for_the_evenings_022():
    """A number change in the evening's delivery must be in the union before the producer builds."""
    deps = {k.to_user_string() for k in ref_patient_identity.asset_deps[ref_patient_identity.key]}
    assert {"arc_patient_ifdb", "arc_cpr"} <= deps


def _kaedefejl(raekker):
    """Run the chain check against a synthetic 022 in SQLite; rows are (pnr, pnrgaeld, dato)."""
    db = sqlite3.connect(":memory:")
    db.execute("ATTACH ':memory:' AS arc")
    db.execute(
        "CREATE TABLE arc.cpr_022_historisk_personnummer "
        "(pnr TEXT, pnrgaeld TEXT, leverance_dato TEXT, dw_is_deleted INT)"
    )
    db.executemany("INSERT INTO arc.cpr_022_historisk_personnummer VALUES (?, ?, ?, 0)", raekker)
    return db.execute(KAEDE_KONTROL_SQL).fetchone()[0]


def test_a_reverted_number_is_not_a_cycle():
    assert (
        _kaedefejl(
            [("0000500001", "0000500002", "2026-07-01"), ("0000500002", "0000500001", "2026-09-29")]
        )
        == 0
    )


def test_a_cycle_without_a_newest_direction_is_still_an_error():
    assert (
        _kaedefejl(
            [("0000500001", "0000500002", "2026-09-29"), ("0000500002", "0000500001", "2026-09-29")]
        )
        == 2
    )
    assert (
        _kaedefejl(
            [
                ("0000500001", "0000500002", "2026-07-01"),
                ("0000500002", "0000500003", "2026-07-01"),
                ("0000500003", "0000500001", "2026-07-01"),
            ]
        )
        == 3
    )

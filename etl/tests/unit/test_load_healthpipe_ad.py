# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for infrastructure/security/load_healthpipe_ad_to_mssql.py (DB-free)."""

from __future__ import annotations

import contextlib

import infrastructure.security.load_healthpipe_ad_to_mssql as loader
import pytest
from DSA.ConceptMap.utils import find_all_descendants
from infrastructure.security import security_seed
from infrastructure.security.export_healthpipe_ad_groups import _parse_groups
from infrastructure.security.load_healthpipe_ad_to_mssql import (
    ExtractTooSmallError,
    _build_rb_organisation_rows,
    _build_rolle_domaene_rows,
    _min_count_ratio,
    _stage_rb_organisation_rows,
    _strip_bruger_domain,
    _validate_extract_size,
)

pytestmark = pytest.mark.unit


def test_strip_bruger_domain_removes_domain_prefix():
    assert _strip_bruger_domain("EXAMPLE\\vah5ep") == "vah5ep"


def test_strip_bruger_domain_bare_id_unchanged():
    assert _strip_bruger_domain("vah5ep") == "vah5ep"


def test_strip_bruger_domain_keeps_part_after_last_backslash():
    assert _strip_bruger_domain("a\\b\\c") == "c"


def test_build_rolle_domaene_rows_strips_bruger():
    rolle_rows = [{"bruger": "EXAMPLE\\abc1de", "domain": "DPA_ERP", "rolle": "U"}]
    rows = _build_rolle_domaene_rows(rolle_rows)
    assert rows == [("abc1de", "DPA_ERP", "U", "")]


def test_duplicate_bruger_domaene_prefers_k_over_u():
    rolle_rows = [
        {"bruger": "ab1", "domain": "DPA_Medicin", "rolle": "U"},
        {"bruger": "ab1", "domain": "DPA_Medicin", "rolle": "K"},
    ]
    rows = _build_rolle_domaene_rows(rolle_rows)
    assert rows == [("ab1", "DPA_Medicin", "K", "")]


def test_duplicate_bruger_domaene_prefers_k_over_u_reversed_order():
    rolle_rows = [
        {"bruger": "ab1", "domain": "DPA_Medicin", "rolle": "K"},
        {"bruger": "ab1", "domain": "DPA_Medicin", "rolle": "U"},
    ]
    rows = _build_rolle_domaene_rows(rolle_rows)
    assert rows == [("ab1", "DPA_Medicin", "K", "")]


def test_duplicate_key_is_case_insensitive():
    rolle_rows = [
        {"bruger": "AB1", "domain": "DPA_MEDICIN", "rolle": "U"},
        {"bruger": "ab1", "domain": "dpa_medicin", "rolle": "K"},
    ]
    rows = _build_rolle_domaene_rows(rolle_rows)
    assert len(rows) == 1
    assert rows[0][2] == "K"


def test_k_wins_over_p_and_u_with_warning():
    warnings: list[str] = []
    rolle_rows = [
        {"bruger": "ab1", "domain": "DPA_Medicin", "rolle": "U"},
        {"bruger": "ab1", "domain": "DPA_Medicin", "rolle": "P", "populationsansvar": "SHS"},
        {"bruger": "ab1", "domain": "DPA_Medicin", "rolle": "K"},
    ]
    rows = _build_rolle_domaene_rows(rolle_rows, warn=warnings.append)
    assert rows == [("ab1", "DPA_Medicin", "K", "")]
    assert len(warnings) == 1
    assert "K, P, U" in warnings[0]


def test_p_wins_over_u_and_keeps_populationsansvar():
    warnings: list[str] = []
    rolle_rows = [
        {"bruger": "ab1", "domain": "DPA_Booking", "rolle": "U"},
        {"bruger": "ab1", "domain": "DPA_Booking", "rolle": "P", "populationsansvar": "SHS"},
    ]
    rows = _build_rolle_domaene_rows(rolle_rows, warn=warnings.append)
    assert rows == [("ab1", "DPA_Booking", "P", "SHS")]
    assert len(warnings) == 1


def test_different_roles_on_different_domains_are_kept():
    warnings: list[str] = []
    rolle_rows = [
        {"bruger": "ab1", "domain": "DPA_Medicin", "rolle": "K"},
        {"bruger": "ab1", "domain": "DPA_ERP", "rolle": "U"},
        {"bruger": "ab1", "domain": "DPA_Booking", "rolle": "P", "populationsansvar": "SHS"},
    ]
    rows = _build_rolle_domaene_rows(rolle_rows, warn=warnings.append)
    assert sorted(rows) == [
        ("ab1", "DPA_Booking", "P", "SHS"),
        ("ab1", "DPA_ERP", "U", ""),
        ("ab1", "DPA_Medicin", "K", ""),
    ]
    assert warnings == []


def test_several_p_hospitals_pick_first_alphabetical_with_warning():
    warnings: list[str] = []
    rolle_rows = [
        {"bruger": "RSD\\ab1", "domain": "DPA_Booking", "rolle": "P", "populationsansvar": "SLB"},
        {"bruger": "ab1", "domain": "DPA_Booking", "rolle": "P", "populationsansvar": "OUH"},
    ]
    rows = _build_rolle_domaene_rows(rolle_rows, warn=warnings.append)
    assert rows == [("ab1", "DPA_Booking", "P", "OUH")]
    assert len(warnings) == 1
    assert "SLB" in warnings[0]


def _raw_group(name: str) -> dict:
    return {"dn": f"CN={name},OU=HealthPipe", "sAMAccountName": name, "members_dn": []}


def test_parse_groups_reads_hospital_and_role_p():
    _, role_groups, skipped, warnings = _parse_groups(
        [_raw_group("GSCAPP_HP_Domaenerolle_DPA_Booking_SHS_P")]
    )
    assert [(g.domain, g.role, g.populationsansvar) for g in role_groups] == [
        ("DPA_Booking", "P", "SHS")
    ]
    assert skipped == []
    assert warnings == []


def test_parse_groups_skips_unknown_hospital_with_warning():
    name = "LSSAPP_HP_Domaenerolle_DPA_Booking_XYZ_P"
    _, role_groups, skipped, warnings = _parse_groups([_raw_group(name)])
    assert role_groups == []
    assert skipped == [name]
    assert len(warnings) == 1


def test_distinct_domains_are_kept():
    rolle_rows = [
        {"bruger": "ab1", "domain": "DPA_Medicin", "rolle": "U"},
        {"bruger": "ab1", "domain": "DPA_HR", "rolle": "K"},
    ]
    rows = _build_rolle_domaene_rows(rolle_rows)
    assert ("ab1", "DPA_Medicin", "U", "") in rows
    assert ("ab1", "DPA_HR", "K", "") in rows


def test_find_all_descendants_includes_self_and_transitive_children():
    children = {"100": ["110", "120"], "110": ["111"]}
    assert find_all_descendants("100", children) == {"100", "110", "111", "120"}


def test_find_all_descendants_unknown_code_returns_self_only():
    assert find_all_descendants("999", {"100": ["110"]}) == {"999"}


def test_find_all_descendants_terminates_on_cycle():
    children = {"100": ["110"], "110": ["100", "111"]}
    assert find_all_descendants("100", children) == {"100", "110", "111"}


def test_build_rb_organisation_rows_expands_and_strips():
    org_rows = [{"bruger": "EXAMPLE\\abc1", "code_sor": "100"}]
    children = {"100": ["110"], "110": ["111"]}
    rows = _build_rb_organisation_rows(org_rows, children)
    assert rows == [("abc1", "100"), ("abc1", "110"), ("abc1", "111")]


def test_build_rb_organisation_rows_flag_false_disables_expansion():
    org_rows = [{"bruger": "abc1", "code_sor": "100", "giv_adgang_til_underorganisationer": False}]
    assert _build_rb_organisation_rows(org_rows, {"100": ["110"]}) == [("abc1", "100")]


def test_build_rb_organisation_rows_dedups_overlapping_grants_case_insensitive():
    org_rows = [
        {"bruger": "abc1", "code_sor": "100"},
        {"bruger": "ABC1", "code_sor": "110"},
    ]
    rows = _build_rb_organisation_rows(org_rows, {"100": ["110"]})
    assert rows == [("abc1", "100"), ("abc1", "110")]


def test_build_rb_organisation_rows_empty_hierarchy_keeps_granted_codes():
    org_rows = [{"bruger": "abc1", "code_sor": "100"}]
    assert _build_rb_organisation_rows(org_rows, {}) == [("abc1", "100")]


def test_stage_rb_organisation_rows_batches_and_flattens_params():
    class _RecordingCursor:
        def __init__(self):
            self.calls: list[tuple[str, tuple]] = []

        def execute(self, sql, params=None):
            self.calls.append((sql, params))

    cur = _RecordingCursor()
    rows = [(f"u{i}", str(i)) for i in range(1001)]
    _stage_rb_organisation_rows(cur, rows)

    assert [c[0].count("(%s, %s)") for c in cur.calls] == [500, 500, 1]
    assert cur.calls[0][1][:4] == ("u0", "0", "u1", "1")
    assert cur.calls[2][1] == ("u1000", "1000")


class _FakeLoadCursor:
    """Besvarer loaderens SQL semantisk: hierarki, counts, rowcount."""

    def __init__(self, conn):
        self._conn = conn
        self.executed: list[tuple[str, tuple | None]] = []
        self._result: list[tuple] = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        s = sql.lstrip()
        if "SORHierarchy" in s:
            self._result = [("100", "110"), ("110", "111")]
        elif s.startswith("SELECT COUNT(*), COUNT(DISTINCT"):
            if "[sec].[RBOrganisation_New]" in s:
                self._result = [self._conn.new_counts]
            else:
                self._result = [self._conn.live_counts]
        elif "NOT EXISTS" in s and "[sec].[RBOrganisation_New]" in s:
            self._result = [(0, 0)]
        elif s.startswith("INSERT INTO [sec].[RBOrganisation_New]"):
            self.rowcount = self._conn.new_counts[0]

    def fetchall(self):
        return list(self._result)

    def fetchone(self):
        return self._result[0]


class _FakeLoadConn:
    def __init__(self, live_counts=(0, 0), new_counts=(3, 1)):
        self.live_counts = live_counts
        self.new_counts = new_counts
        self.cursors: list[_FakeLoadCursor] = []
        self.committed = 0

    def cursor(self):
        cur = _FakeLoadCursor(self)
        self.cursors.append(cur)
        return cur

    def commit(self):
        self.committed += 1


def _all_executed(conn):
    return [(sql, params) for cur in conn.cursors for sql, params in cur.executed]


def test_load_rb_organisation_stages_expanded_descendants_and_swaps():
    conn = _FakeLoadConn(live_counts=(0, 0), new_counts=(3, 1))
    result = loader._load_rb_organisation(
        conn,
        [{"bruger": "abc1", "code_sor": "100"}],
        dry_run=False,
        min_ratio=0.8,
        max_removed_grants_ratio=0.2,
        max_lost_users_ratio=0.1,
    )

    staged_codes = {
        v
        for sql, params in _all_executed(conn)
        if sql.startswith("INSERT INTO #SrcRBOrganisation") and params
        for v in params[1::2]
    }
    assert staged_codes == {"100", "110", "111"}
    assert any("SWITCH TO [sec].[RBOrganisation_Old]" in sql for sql, _ in _all_executed(conn))
    log_params = [params for sql, params in _all_executed(conn) if "RBOrganisationLoadLog" in sql]
    assert log_params == [(0, 3, 0, 1, 1)]
    assert result == {
        "inserted": 3,
        "rows_old": 0,
        "rows_new": 3,
        "users_old": 0,
        "users_new": 1,
        "removed_grants": 0,
        "lost_users": 0,
        "swapped": True,
    }
    assert conn.committed == 1


def test_load_rb_organisation_guard_blocks_swap_but_logs():
    conn = _FakeLoadConn(live_counts=(100, 5), new_counts=(3, 1))
    with pytest.raises(ExtractTooSmallError):
        loader._load_rb_organisation(
            conn,
            [{"bruger": "abc1", "code_sor": "100"}],
            dry_run=False,
            min_ratio=0.8,
            max_removed_grants_ratio=0.2,
            max_lost_users_ratio=0.1,
        )

    assert not any("SWITCH" in sql for sql, _ in _all_executed(conn))
    log_params = [params for sql, params in _all_executed(conn) if "RBOrganisationLoadLog" in sql]
    assert log_params == [(100, 3, 5, 1, 0)]
    assert conn.committed == 1


def test_rolledomaene_uses_truncate_insert_not_merge():
    assert "TRUNCATE TABLE [sec].[RolleDomaene]" in loader._TRUNCATE_ROLLE_DOMAENE_SQL
    assert "MERGE" not in loader._INSERT_ROLLE_DOMAENE_SQL
    assert "INSERT INTO [sec].[RolleDomaene]" in loader._INSERT_ROLLE_DOMAENE_SQL


def test_rborganisation_insert_targets_new_table_with_dedup():
    sql = loader._INSERT_RB_ORGANISATION_NEW_SQL
    assert "INSERT INTO [sec].[RBOrganisation_New]" in sql
    assert "ROW_NUMBER() OVER" in sql
    assert "PARTITION BY src.[bruger], org.[dw_ek_Organisation]" in sql
    assert "WHERE d.rn = 1" in sql
    assert "dw_is_deleted" not in sql


def test_rborganisation_swap_rotates_and_never_truncates_live():
    # Fail-safe: den levende tabel må aldrig trunkeres — kun swappes atomisk.
    sql = loader._SWAP_RB_ORGANISATION_SQL
    assert "TRUNCATE TABLE [sec].[RBOrganisation_Old];" in sql
    assert "ALTER TABLE [sec].[RBOrganisation] SWITCH TO [sec].[RBOrganisation_Old];" in sql
    assert "ALTER TABLE [sec].[RBOrganisation_New] SWITCH TO [sec].[RBOrganisation];" in sql
    assert "TRUNCATE TABLE [sec].[RBOrganisation];" not in sql


def test_size_guard_fails_when_extract_much_smaller():
    with pytest.raises(ExtractTooSmallError):
        _validate_extract_size(
            "sec.RolleDomaene", existing_count=100, incoming_count=79, min_ratio=0.8
        )


def test_size_guard_ok_at_threshold_and_above():
    _validate_extract_size("sec.RolleDomaene", existing_count=100, incoming_count=80, min_ratio=0.8)
    _validate_extract_size(
        "sec.RolleDomaene", existing_count=100, incoming_count=150, min_ratio=0.8
    )


def test_size_guard_skipped_on_empty_table():
    _validate_extract_size("sec.RolleDomaene", existing_count=0, incoming_count=0, min_ratio=0.8)


def test_min_count_ratio_defaults_when_unset(monkeypatch):
    monkeypatch.delenv("AD_LOAD_MIN_COUNT_RATIO", raising=False)
    assert _min_count_ratio() == 0.8


def test_min_count_ratio_env_override(monkeypatch):
    monkeypatch.setenv("AD_LOAD_MIN_COUNT_RATIO", "0.5")
    assert _min_count_ratio() == 0.5


def test_min_count_ratio_rejects_non_numeric(monkeypatch):
    monkeypatch.setenv("AD_LOAD_MIN_COUNT_RATIO", "abc")
    with pytest.raises(ValueError, match="AD_LOAD_MIN_COUNT_RATIO"):
        _min_count_ratio()


def test_min_count_ratio_rejects_non_positive(monkeypatch):
    for bad in ("0", "-1"):
        monkeypatch.setenv("AD_LOAD_MIN_COUNT_RATIO", bad)
        with pytest.raises(ValueError, match="AD_LOAD_MIN_COUNT_RATIO"):
            _min_count_ratio()


class _EmptyHierarchyCursor(_FakeLoadCursor):
    """Like _FakeLoadCursor, but SORHierarchy is empty."""

    def execute(self, sql, params=None):
        super().execute(sql, params)
        if "SORHierarchy" in sql.lstrip():
            self._result = []


class _EmptyHierarchyConn(_FakeLoadConn):
    def cursor(self):
        cur = _EmptyHierarchyCursor(self)
        self.cursors.append(cur)
        return cur


def test_fetch_sor_hierarchy_raises_when_table_empty():
    """The guard misses it at bootstrap, so the lookup must fail itself."""
    with pytest.raises(loader.EmptySorHierarchyError, match="cs_sor"):
        loader._fetch_sor_hierarchy(_EmptyHierarchyConn())


def test_load_rb_organisation_propagates_empty_hierarchy_error():
    """The error must reach Dagster, not end as an empty-but-successful load."""
    conn = _EmptyHierarchyConn(live_counts=(0, 0))
    with pytest.raises(loader.EmptySorHierarchyError):
        loader._load_rb_organisation(
            conn,
            [{"bruger": "EXAMPLE\\u1", "code_sor": "100"}],
            dry_run=False,
            min_ratio=0.8,
            max_removed_grants_ratio=0.2,
            max_lost_users_ratio=0.1,
        )


def test_loader_reuses_shared_expansion_helper_without_private_copy():
    """The expansion semantics must exist in exactly one place."""
    assert loader.find_all_descendants is find_all_descendants
    assert not hasattr(loader, "_expand_sor_descendants")


def test_build_rb_organisation_rows_memoises_expansion_per_distinct_code(monkeypatch):
    """N users in the same AD group must trigger only one expansion."""
    children = {"100": ["110"], "110": ["111"]}
    calls: list[str] = []
    real = loader.find_all_descendants

    def counting(code, hierarchy):
        calls.append(code)
        return real(code, hierarchy)

    monkeypatch.setattr(loader, "find_all_descendants", counting)
    org_rows = [{"bruger": f"EXAMPLE\\u{i}", "code_sor": "100"} for i in range(5)]
    rows = loader._build_rb_organisation_rows(org_rows, children)

    assert calls == ["100"], "expansion must be memoised per distinct code"
    assert len(rows) == 15, "5 users x {100, 110, 111}"


def test_build_rb_organisation_rows_skips_expansion_when_flag_false():
    """giv_adgang_til_underorganisationer=False must still yield only the code."""
    children = {"100": ["110"], "110": ["111"]}
    rows = loader._build_rb_organisation_rows(
        [
            {
                "bruger": "EXAMPLE\\u1",
                "code_sor": "100",
                "giv_adgang_til_underorganisationer": False,
            }
        ],
        children,
    )
    assert rows == [("u1", "100")]


class _FakeHistorikCursor(_FakeLoadCursor):
    def execute(self, sql, params=None):
        super().execute(sql, params)
        s = sql.lstrip()
        if s.startswith("SELECT COUNT(*) FROM [sec].[") and "[dw_valid_to]" in s:
            self._result = [(self._conn.open_count,)]
        elif s.startswith("SELECT COUNT(*) FROM [sec].[RolleDomaene]"):
            self._result = [(0,)]
        elif s.startswith("SELECT CAST(SYSDATETIME() AS DATETIME2(0))"):
            self._result = [(object(),)]


class _FakeHistorikConn(_FakeLoadConn):
    def __init__(self, open_count):
        super().__init__()
        self.open_count = open_count
        self.closed = False

    def cursor(self):
        cur = _FakeHistorikCursor(self)
        self.cursors.append(cur)
        return cur

    def close(self):
        self.closed = True


def test_populate_security_keeps_k_and_closes_history_on_clean_run(
    monkeypatch, _non_prod_environment
):
    import dagster as dg
    import infrastructure.assets.security_ad_assets as assets

    class _FakeDfm:
        rows: dict = {}

        @contextlib.contextmanager
        def target_runs(self, schema, targets, **_):
            assert schema == "sec"
            assert set(targets) == {
                "RolleDomaene",
                "RBOrganisation",
                "RolleDomaeneHistorik",
                "SORTildelingHistorik",
            }
            yield _FakeDfm.rows

    conn = _FakeHistorikConn(open_count=1)
    monkeypatch.setenv("MSSQL_TARGET_STAGING_USER", "u")
    monkeypatch.setenv("MSSQL_TARGET_STAGING_PASSWORD", "p")
    monkeypatch.setattr(loader, "_connect", lambda *_: conn)
    monkeypatch.setattr(assets, "DfmManager", lambda *_: _FakeDfm())
    context = dg.build_asset_context()

    result = assets.populate_security(
        context=context,
        extract_security={
            "roller": [
                {"bruger": "u0", "domain": "DPA_ERP", "rolle": "K"},
                {"bruger": "u0", "domain": "DPA_ERP", "rolle": "U"},
            ],
            "org": [{"bruger": "u0", "code_sor": "100"}],
            "fejlede_opslag": [],
            "source": {"user": "svc", "host": "ldap", "table": "OU=HealthPipe"},
        },
        mssql_target_staging=None,
    )

    historik = {"RolleDomaeneHistorik", "SORTildelingHistorik"}
    executed = _all_executed(conn)
    inserts = {
        table: params
        for sql, params in executed
        for table in historik
        if sql.lstrip().startswith(f"INSERT INTO [sec].[{table}]")
    }
    assert set(inserts) == historik
    assert {p["run_id"] for p in inserts.values()} == {context.run_id}
    assert _staged_rolle_rows(conn) == [("u0", "DPA_ERP", "K", "")]
    assert any(sql.startswith("TRUNCATE TABLE [sec].[RolleDomaene]") for sql, _ in executed)
    assert sum(sql.lstrip().startswith("UPDATE t") for sql, _ in executed) == 2
    assert historik <= set(_FakeDfm.rows)
    assert result.metadata["sortildelinghistorik_open_before"] == 1
    assert conn.closed


def test_populate_security_changes_no_table_when_a_member_lookup_failed(monkeypatch):
    import dagster as dg
    import infrastructure.assets.security_ad_assets as assets

    conn = _FakeHistorikConn(open_count=1)
    monkeypatch.setenv("MSSQL_TARGET_STAGING_USER", "u")
    monkeypatch.setenv("MSSQL_TARGET_STAGING_PASSWORD", "p")
    monkeypatch.setattr(loader, "_connect", lambda *_: conn)

    with pytest.raises(loader.FailedMemberLookupsError, match="^1 medlemsopslag"):
        assets.populate_security(
            context=dg.build_asset_context(),
            extract_security={
                "roller": [{"bruger": "u0", "domain": "DPA_ERP", "rolle": "U"}],
                "org": [{"bruger": "u0", "code_sor": "100"}],
                "fejlede_opslag": ["CN=DPA_ERP_K,OU=HealthPipe"],
                "source": {"user": "svc", "host": "ldap", "table": "OU=HealthPipe"},
            },
            mssql_target_staging=None,
        )
    assert _all_executed(conn) == []


@pytest.fixture
def _non_prod_environment(monkeypatch):
    monkeypatch.delenv(security_seed.SEED_ENABLED_ENV, raising=False)
    monkeypatch.setenv("DAGSTER_ENVIRONMENT", "dev")
    monkeypatch.setenv("MSSQL_TARGET_STAGING_HOST", "mssql")
    monkeypatch.setenv("MSSQL_TARGET_DSO_HOST", "mssql")


def _staged_rolle_rows(conn):
    return [p for s, p in _all_executed(conn) if s.startswith("INSERT INTO #SrcRolleDomaene")]


_AD_ROLLE_ROWS = [("ab1", "DPA_Medicin", "U", "")]


@pytest.mark.parametrize(("seed", "expect_testbrugere"), [("", False), ("true", True)])
def test_load_rolle_domaene_stages_testbrugere_only_when_seeding(
    monkeypatch, _non_prod_environment, seed, expect_testbrugere
):
    monkeypatch.setenv(security_seed.SEED_ENABLED_ENV, seed)
    conn = _FakeHistorikConn(open_count=0)
    loader._load_rolle_domaene(conn, _AD_ROLLE_ROWS, dry_run=False, min_ratio=0.8)

    staged = _staged_rolle_rows(conn)
    assert ("ab1", "DPA_Medicin", "U", "") in staged
    assert (("testbruger_population", "DPA_Medicin", "P", "SLB") in staged) is expect_testbrugere


def test_load_rolle_domaene_seed_collision_fails_before_truncate(
    monkeypatch, _non_prod_environment
):
    monkeypatch.setenv(security_seed.SEED_ENABLED_ENV, "true")
    conn = _FakeHistorikConn(open_count=0)
    rows = _AD_ROLLE_ROWS + [("TESTBRUGER_U", "DPA_Medicin", "U", "")]
    with pytest.raises(security_seed.SeedCollisionError):
        loader._load_rolle_domaene(conn, rows, dry_run=False, min_ratio=0.8)
    assert not any("TRUNCATE" in s for s, _ in _all_executed(conn))


def test_load_rolle_domaene_empty_ad_extract_skips_even_with_seed(
    monkeypatch, _non_prod_environment
):
    monkeypatch.setenv(security_seed.SEED_ENABLED_ENV, "true")
    conn = _FakeHistorikConn(open_count=0)
    assert loader._load_rolle_domaene(conn, [], dry_run=False, min_ratio=0.8) == 0
    assert _all_executed(conn) == []


def test_load_rb_organisation_with_seed_stages_expanded_testbruger_grant(
    monkeypatch, _non_prod_environment
):
    monkeypatch.setenv(security_seed.SEED_ENABLED_ENV, "true")
    monkeypatch.setattr(
        security_seed,
        "TESTBRUGERE",
        (security_seed.Testbruger("testbruger_org1", "K", sor_koder=("100",)),),
    )
    conn = _FakeLoadConn(live_counts=(0, 0), new_counts=(4, 2))
    loader._load_rb_organisation(
        conn,
        [{"bruger": "abc1", "code_sor": "111"}],
        dry_run=False,
        min_ratio=0.8,
        max_removed_grants_ratio=0.2,
        max_lost_users_ratio=0.1,
    )

    staged = {
        (params[i], params[i + 1])
        for sql, params in _all_executed(conn)
        if sql.startswith("INSERT INTO #SrcRBOrganisation") and params
        for i in range(0, len(params), 2)
    }
    assert staged == {
        ("abc1", "111"),
        ("testbruger_org1", "100"),
        ("testbruger_org1", "110"),
        ("testbruger_org1", "111"),
    }


def test_populate_security_with_seed_keeps_testbrugere_out_of_historik(
    monkeypatch, _non_prod_environment
):
    import dagster as dg
    import infrastructure.assets.security_ad_assets as assets

    class _FakeDfm:
        @contextlib.contextmanager
        def target_runs(self, *_, **__):
            yield {}

    conn = _FakeHistorikConn(open_count=0)
    monkeypatch.setenv(security_seed.SEED_ENABLED_ENV, "true")
    monkeypatch.setenv("MSSQL_TARGET_STAGING_USER", "u")
    monkeypatch.setenv("MSSQL_TARGET_STAGING_PASSWORD", "p")
    monkeypatch.setattr(loader, "_connect", lambda *_: conn)
    monkeypatch.setattr(assets, "DfmManager", lambda *_: _FakeDfm())

    result = assets.populate_security(
        context=dg.build_asset_context(),
        extract_security={
            "roller": [{"bruger": "u0", "domain": "DPA_ERP", "rolle": "U"}],
            "org": [{"bruger": "u0", "code_sor": "100"}],
            "fejlede_opslag": [],
            "source": {"user": "svc", "host": "ldap", "table": "OU=HealthPipe"},
        },
        mssql_target_staging=None,
    )

    executed = _all_executed(conn)
    historik_params = [
        v for s, p in executed if s.startswith("INSERT INTO #SrcHistorik") for v in p
    ]
    assert "u0" in historik_params
    assert not any(str(v).startswith("testbruger_") for v in historik_params)
    assert any(r[0] == "testbruger_U" for r in _staged_rolle_rows(conn))
    assert result.metadata["security_seed_enabled"] is True

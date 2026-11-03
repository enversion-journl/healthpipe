# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""dw-keys on the flat fhir.* tables: run scope, lookup contract and wiring."""

import re
from unittest.mock import MagicMock

import pytest
from dagster import AssetKey
from definitions import defs
from DSA.FHIR.SqlOnFhir.DWKeys_dfm import DW_KEYS_CONFIG
from DSA.FHIR.utils.sql_on_fhir.dw_keys import (
    DWKeysConfig,
    OrganisationKey,
    candidates_sql,
    key_columns,
    lookup_sql,
    organisation_read_bound,
    resolve_scope,
    run_dw_keys,
)

EPISODE_OF_CARE = DW_KEYS_CONFIG["episode_of_care"]
CHECKPOINTS = {
    EPISODE_OF_CARE.checkpoint_fhir: "2026-10-12 06:00:00.0000000",
    EPISODE_OF_CARE.checkpoint_borger: "2026-10-12 06:00:00.000",
    EPISODE_OF_CARE.checkpoint_organisation: "2026-10-12 06:00:00.000",
}


@pytest.mark.unit
class TestResolveScope:
    def test_delta_with_every_checkpoint_reads_candidates_only(self) -> None:
        assert resolve_scope("delta", CHECKPOINTS) == (False, None)

    def test_a_missing_checkpoint_forces_a_full_pass(self) -> None:
        """Without the borger watermark, borger changes before the first run would be missed."""
        checkpoints = {**CHECKPOINTS, EPISODE_OF_CARE.checkpoint_borger: None}
        full, reason = resolve_scope("delta", checkpoints)
        assert full
        assert EPISODE_OF_CARE.checkpoint_borger in reason

    def test_full_load_ignores_checkpoints(self) -> None:
        assert resolve_scope("full_load", CHECKPOINTS)[0]

    @pytest.mark.parametrize("mode", ["period", "ids"])
    def test_other_modes_are_refused(self, mode: str) -> None:
        with pytest.raises(ValueError, match=mode):
            resolve_scope(mode, CHECKPOINTS)

    @pytest.mark.parametrize(
        ("started_at", "bound"),
        [("2026-10-14 06:00:01.0000000", "2026-10-14 05:00:01"), (None, None)],
    )
    def test_organisation_bound_reaches_back_before_the_run_start(self, started_at, bound) -> None:
        """A dim_Organisation commit after the read must fall after the bound, or it is lost."""
        assert organisation_read_bound(started_at) == bound


@pytest.mark.unit
class TestLookupContract:
    def test_entity_key_is_found_for_a_soft_deleted_borger_but_the_version_is_not(self) -> None:
        """dw_ek_Borger is the borger's identity; dw_sk_Borger only comes from a live version."""
        _, applies = lookup_sql(EPISODE_OF_CARE)
        entity = applies.split(") AS borger_entity")[0]
        version = applies.split(") AS borger_entity")[1].split(") AS borger_version")[0]
        live_only = re.compile(r"dw_is_deleted\s*=\s*0", re.IGNORECASE)
        assert not live_only.search(entity)
        assert live_only.search(version)

    def test_an_inherited_organisation_is_read_and_followed_on_the_referenced_table(self) -> None:
        """Read from dim.Organisation, the Location id never matches; unfollowed, it goes stale."""
        cfg = DW_KEYS_CONFIG["appointment"]
        _, applies = lookup_sql(cfg)
        assert "[dim].[Organisation]" not in applies
        assert re.search(r"FROM\s+\[fhir\]\.\[Location\]", applies, re.IGNORECASE)
        organisation_part = candidates_sql(cfg).split("UNION")[-1]
        follows_location = re.compile(
            r"\[performing_location_id\]\s+IN\s*\(\s*SELECT\s+\w+\.\[location_id\]\s+"
            r"FROM\s+\[fhir\]\.\[Location\]",
            re.IGNORECASE,
        )
        assert follows_location.search(organisation_part)

    def test_each_organisation_role_is_looked_up_from_its_own_column(self) -> None:
        """Swapped roles would put the referring organisation on the referred-to key."""
        expressions, applies = lookup_sql(DW_KEYS_CONFIG["service_request"])
        for role, column in [("henvist_til", "performer_id"), ("henvist_fra", "requester_id")]:
            own_alias = rf"organisation_{role}\.dw_ek_Organisation\s*,\s*-1\s*\)\s*AS\s+"
            assert re.search(rf"{own_alias}dw_ek_Organisation_{role}\b", expressions)
            block = applies.split(f") AS organisation_{role}")[0].rsplit("OUTER APPLY", 1)[1]
            assert f"f.[{column}]" in block

    @pytest.mark.parametrize(
        ("columns", "message"),
        [
            ({"patient_column": "patient_id"}, "together"),
            ({"time_column_is_text": True}, "needs a time_column"),
            ({"organisations": ()}, "at least one organisation"),
            (
                {"organisations": (OrganisationKey("a_id"), OrganisationKey("b_id", "b"))},
                "a role each",
            ),
            (
                {"organisations": (OrganisationKey("a_id", "x"), OrganisationKey("b_id", "x"))},
                "distinct",
            ),
        ],
    )
    def test_an_incomplete_config_is_refused(self, columns, message) -> None:
        with pytest.raises(ValueError, match=message):
            DWKeysConfig(
                **{
                    "asset_suffix": "x",
                    "table": "X",
                    "primary_key": "x_id",
                    "organisations": (OrganisationKey("organization_id"),),
                    **columns,
                }
            )


class _RecordingCursor:
    """Answers the candidate count, an empty batch range and the given key distribution."""

    def __init__(self, candidates: int = 0, distribution: dict[str, int] | None = None) -> None:
        self.calls: list[tuple[str, object]] = []
        self.description: list[tuple[str]] = []
        self._candidates = candidates
        self._distribution = distribution or {}
        self._row: tuple | None = None

    def execute(self, sql: str, params: object = None) -> None:
        self.calls.append((sql, params))
        if "COUNT_BIG" in sql:
            self._row = (self._candidates,)
        elif "_minus1]" in sql:
            self.description = [(name,) for name in self._distribution]
            self._row = tuple(self._distribution.values())
        elif "SELECT TOP" in sql:
            self._row = (None,)
        else:
            self._row = ("2026-10-13 06:00:00",)

    def fetchone(self) -> tuple | None:
        return self._row


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("episode_of_care", ["fhir", "borger", "organisation"]),
        ("location", ["fhir", "organisation"]),
        ("service_request", ["fhir", "borger", "organisation", "organisation"]),
        ("appointment", ["fhir", "borger", "organisation"]),
    ],
)
def test_delta_binds_one_checkpoint_per_candidate_source_in_query_order(
    name: str, expected: list[str]
) -> None:
    """A wrong count or order puts one source's watermark on another and skips changes."""
    cfg = DW_KEYS_CONFIG[name]
    checkpoints = {
        cfg.checkpoint_fhir: "fhir",
        cfg.checkpoint_borger: "borger",
        cfg.checkpoint_organisation: "organisation",
    }
    cursor = _RecordingCursor()
    run_dw_keys(cursor, MagicMock(), cfg, full=False, checkpoints=checkpoints, log=MagicMock())
    sql, params = next(call for call in cursor.calls if "INSERT INTO #candidates" in call[0])
    assert list(params) == expected
    parts = sql.split("UNION")
    assert [part.count("?") for part in parts] == [1] * len(expected)
    reads_source = {
        "fhir": lambda part: not re.search(r"\bIN\s*\(", part),
        "borger": lambda part: "[dim].[Borger]" in part,
        "organisation": lambda part: re.search(r"\[dim\]\.\[Organisation\]|\[fhir\]\.\[", part),
    }
    for part, source in zip(parts, expected, strict=True):
        assert reads_source[source](part), f"{source} checkpoint bound to: {part.strip()[:80]}"


def _distribution(name: str, **counts: int) -> dict[str, int]:
    """A key distribution for one config: every key found once, then ``counts`` on top."""
    cfg = DW_KEYS_CONFIG[name]
    distribution = {
        f"{c}_{kind}": 1 if kind == "found" else 0
        for c in key_columns(cfg)
        for kind in ("found", "minus1")
    }
    if cfg.time_column_is_text:
        distribution["time_unparseable"] = 0
    return {**distribution, **counts}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("name", "distribution", "warning"),
    [
        ("location", _distribution("location"), None),
        (
            "location",
            _distribution("location", dw_ek_Organisation_minus1=3),
            "dw_ek_Organisation=3 (dim.Organisation)",
        ),
        (
            "appointment",
            _distribution("appointment", dw_ek_Organisation_minus1=2),
            "dw_ek_Organisation=2 (fhir.Location)",
        ),
        ("service_request", _distribution("service_request"), None),
        (
            "service_request",
            _distribution("service_request", time_unparseable=2),
            "2 rows evaluated with a authored_on that cannot be parsed",
        ),
    ],
)
def test_a_value_the_lookup_cannot_use_is_logged(
    name: str, distribution: dict[str, int], warning: str | None
) -> None:
    """A -1 or an unparseable time is a source value the lookup cannot use; unlogged it is drift."""
    cfg = DW_KEYS_CONFIG[name]
    checkpoints = {
        cfg.checkpoint_fhir: "fhir",
        cfg.checkpoint_borger: "borger",
        cfg.checkpoint_organisation: "organisation",
    }
    log = MagicMock()
    result = run_dw_keys(
        _RecordingCursor(candidates=13, distribution=distribution),
        MagicMock(),
        cfg,
        full=False,
        checkpoints=checkpoints,
        log=log,
    )
    assert result["distribution"] == distribution
    messages = [call.args[0] % call.args[1:] for call in log.warning.call_args_list]
    if warning is None:
        assert not messages
    else:
        assert any(warning in message for message in messages)


@pytest.mark.unit
@pytest.mark.parametrize("sof_name", sorted(DW_KEYS_CONFIG))
def test_dso_sync_waits_for_the_dw_keys_and_index_maintenance_for_the_sync(sof_name: str) -> None:
    """Without these edges the sync carries keys still at -1, or a rebuild locks a table mid-sync."""
    graph = defs.resolve_asset_graph()
    sync = AssetKey.from_user_string(f"dso_sync_fhir_{sof_name}_dfm")
    keys = AssetKey.from_user_string(DW_KEYS_CONFIG[sof_name].asset_name)
    maintenance = AssetKey.from_user_string("fhir_dw_keys_index_maintenance")
    assert keys in graph.get(sync).parent_keys
    assert sync in graph.get(maintenance).parent_keys


@pytest.mark.unit
@pytest.mark.parametrize(
    "name", sorted(n for n, c in DW_KEYS_CONFIG.items() if any(o.via for o in c.organisations))
)
def test_an_inherited_organisation_waits_for_the_referenced_tables_dw_keys(name: str) -> None:
    """Run before it, the inherited key is the referenced row's key from the previous night."""
    graph = defs.resolve_asset_graph()
    cfg = DW_KEYS_CONFIG[name]
    parents = graph.get(AssetKey.from_user_string(cfg.asset_name)).parent_keys
    for organisation in cfg.organisations:
        if organisation.via is not None:
            assert AssetKey.from_user_string(organisation.via.asset_name) in parents

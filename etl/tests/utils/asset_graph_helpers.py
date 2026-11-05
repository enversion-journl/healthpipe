# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared assertions for asset-graph wiring guards.

Reading the wrong upstream — or losing one — fails *silently*: the pipeline
stays green and the data is stale, unenriched, or posted out of order. Those
guards are the only tier that can observe it (see the carve-out in
docs/testing-guidelines.md).

Write them as a **rule plus a subset**, never an exact dep set. An exact set
fails on every legitimate upstream addition and can never fail for an asset
added tomorrow; the rule states the contract, the subset pins the upstreams
whose absence would be a real fault.
"""

from __future__ import annotations

from dagster import AssetKey

# Upstream shapes a FHIR DFM producer may read: archives and other DFM producers.
DFM_PRODUCER_PREFIXES = ("arc_", "fhir_server_")
# A SOF asset waits for the SOF infrastructure plus its producer(s).
SOF_PREFIXES = ("fhir_sof_infrastructure", "fhir_server_", "sof_")
# A DSO sync waits for the SOF view it reads plus the DSO infrastructure gate,
# and for the resource's DW-key asset where it has one.
DSO_SYNC_PREFIXES = ("sof_", "dso_infra_", "fhir_dw_keys_")


def dep_names(asset, key_str: str) -> set[str]:
    """Upstream keys of ``key_str`` within ``asset``, as user strings.

    Asserts the key belongs to the asset: a renamed key or a reshaped
    multi-asset would otherwise raise KeyError far from the cause.
    """
    key = AssetKey.from_user_string(key_str)
    deps = asset.asset_deps.get(key)
    assert deps is not None, (
        f"{key_str} is not a key of this asset — it was renamed, "
        "or the multi-asset layout changed"
    )
    return {k.to_user_string() for k in deps}


def assert_deps(asset, key_str: str, *, allowed_prefixes: tuple[str, ...], required: set[str]):
    """Assert every upstream matches ``allowed_prefixes`` and ``required`` are present.

    The non-empty check is the vacuous-pass guard: an asset that resolves to
    zero upstreams would otherwise satisfy ``all(...)`` trivially and stay green.
    """
    names = dep_names(asset, key_str)
    assert names, f"{key_str} has no upstream deps — wiring changed"

    offenders = sorted(n for n in names if not n.startswith(allowed_prefixes))
    assert not offenders, f"{key_str} reads upstreams outside {allowed_prefixes}: {offenders}"

    missing = sorted(required - names)
    assert not missing, f"{key_str} no longer reads: {missing}"

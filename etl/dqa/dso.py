# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSO-vs-DSA reconciliation subsystem.

Right after the DSO-sync copies each staging (DSA) table to the DSO/reporting
database (via the ``STAGING_LINK`` linked server), the two should be identical.
This module derives one ``dso_reconciliation`` check **per synced table** straight
from ``DSO.load.sync_configs.ALL_SYNC_CONFIGS`` (the single source of truth for
schema / table / primary key), groups them by domain, and emits:

- one **anchor asset per domain** (``dqa_dso_<domain>``),
- one **asset check per synced table** bound to its domain anchor, and
- one **job per domain** (``dqa_dso_<domain>_job``).

The checks run on the **DSO** connection (``mssql_source_dso``) and compare against
DSA over the linked server; Results persist to the single ``dqa.Results`` table on
DSA (``results_resource_key="mssql_target_staging"``). Detection only -- no
capture/replay yet (deferred until DSO-sync is migrated to DFM).

This subsystem is intentionally separate from ``ALL_SUBJECTS`` / the daily+weekly
factory so DSO checks never leak into the ``dqa_<track>_<cadence>`` jobs.
"""

from __future__ import annotations

import dagster as dg
import dqa.checks  # noqa: F401  -- populates the registry (dso_reconciliation)
from core.resources import get_linked_server_config
from dqa.model import Cadence, CheckSpecification, DqaSubject, Severity, Track
from dqa.registry import CHECK_TYPES
from dqa.result import to_asset_check_result

_DSO_RESOURCE = "mssql_source_dso"  # comparison runs here (linked server -> DSA)
_RESULTS_RESOURCE = "mssql_target_staging"  # dqa.Results lives on DSA (unified table)

# DSO checks are the heaviest per query — correlated NOT EXISTS diffs over the
# STAGING_LINK linked server, some on very large tables. Cap parallelism low
# (overrides the global default executor of max_concurrent=8); raise after QA if
# the linked server has headroom.
_DSO_EXECUTOR = dg.multiprocess_executor.configured({"max_concurrent": 2})

# Sync configs with no DSO sync asset: the DSO copy is empty or a stale seed.
_NOT_SYNCED_TO_DSO = frozenset(
    {"ref_patient_borger", "ref_patient_address_borger", "ref_organisation"}
)


def _dso_subjects() -> list[tuple[DqaSubject, str]]:
    """Derive (subject, domain) for every DSO-synced table from the sync configs."""
    from DSO.load.sync_configs import (
        CODESYSTEM_SYNC_CONFIGS,
        CONCEPTMAP_SYNC_CONFIGS,
        DRG_SYNC_CONFIGS,
        ERP_SYNC_CONFIGS,
        FHIR_SYNC_CONFIGS,
        HAIBA_DM_CONFIGS,
        SECURITY_SYNC_CONFIGS,
        SHARED_DIMS_SYNC_CONFIGS,
        UDREDNINGSRET_SYNC_CONFIGS,
    )

    # (domain, track, configs). fhir -> FHIR track; everything else -> DWH track.
    # Every config dict merged into ALL_SYNC_CONFIGS must be listed here too:
    # this list is what assigns the domain, and the derivation is asserted to
    # cover ALL_SYNC_CONFIGS minus _NOT_SYNCED_TO_DSO (tests/unit/test_dqa_dso.py).
    domains: list[tuple[str, Track, dict]] = [
        ("fhir", Track.FHIR, FHIR_SYNC_CONFIGS),
        ("erp", Track.DWH, ERP_SYNC_CONFIGS),
        ("shared", Track.DWH, SHARED_DIMS_SYNC_CONFIGS),
        ("security", Track.DWH, SECURITY_SYNC_CONFIGS),
        ("codesystem", Track.DWH, CODESYSTEM_SYNC_CONFIGS),
        ("conceptmap", Track.DWH, CONCEPTMAP_SYNC_CONFIGS),
        ("haiba", Track.DWH, HAIBA_DM_CONFIGS),
        ("drg", Track.DWH, DRG_SYNC_CONFIGS),
        ("udredningsret", Track.DWH, UDREDNINGSRET_SYNC_CONFIGS),
    ]
    link = get_linked_server_config()

    out: list[tuple[DqaSubject, str]] = []
    for domain, track, configs in domains:
        for key, cfg in configs.items():
            if key in _NOT_SYNCED_TO_DSO:
                continue
            pk = cfg.primary_key if isinstance(cfg.primary_key, list) else [cfg.primary_key]
            spec = CheckSpecification(
                "dso_reconciliation",
                Severity.ERROR,
                Cadence.DSO,
                {
                    "pk": list(pk),
                    "staging_db": link["database"],
                    "linked_server": link["name"],
                },
            )
            subject = DqaSubject(
                name=f"dso_{key}",
                track=track,
                table=f"{cfg.schema}.{cfg.table}",
                resource_key=_DSO_RESOURCE,
                results_resource_key=_RESULTS_RESOURCE,
                identity_column=pk[0],
                checks=(spec,),
            )
            out.append((subject, domain))
    return out


def _anchor_key(domain: str) -> str:
    return f"dqa_dso_{domain}"


def _build_anchor(domain: str, n: int) -> dg.AssetsDefinition:
    key = _anchor_key(domain)

    @dg.asset(
        name=key,
        group_name="dqa_dso",
        compute_kind="mssql",
        tags={"dqa_track": "dso", "dqa_domain": domain},
        description=(
            f"DQA DSO anchor for the '{domain}' domain ({n} tables): DSO vs DSA "
            f"reconciliation after the DSO-sync. Materialize to run its checks."
        ),
    )
    def _anchor() -> None:
        return None

    return _anchor


def _build_check(subject: DqaSubject, domain: str):
    spec = subject.checks[0]
    impl = CHECK_TYPES[spec.check_type]
    # config_key is the part after the "dso_" prefix -> stable, non-redundant name.
    check_name = f"{subject.name[len('dso_') :]}__dso_reconciliation"
    required = {subject.resource_key, subject.results_resource_key or subject.resource_key}

    @dg.asset_check(
        name=check_name,
        asset=_anchor_key(domain),
        blocking=spec.severity.is_blocking,
        required_resource_keys=required,
        compute_kind="mssql",
        metadata={
            "dqa_cadence": Cadence.DSO.value,
            "dqa_severity": spec.severity.value,
            "dqa_subject": subject.table,
            "dqa_domain": domain,
        },
        description=(
            f"dso_reconciliation on {subject.table} -- DSO vs DSA "
            f"(staging) on PK {spec.params['pk']}"
        ),
    )
    def _check(context) -> dg.AssetCheckResult:
        outcome = impl(context, subject, spec, context.resources)
        return to_asset_check_result(context, subject, spec, outcome, context.resources)

    return _check


def build_dso_all() -> (
    tuple[list[dg.AssetsDefinition], list, list[dg.UnresolvedAssetJobDefinition]]
):
    """Return (anchor assets, asset checks, jobs) for the DSO reconciliation subsystem."""
    subjects = _dso_subjects()
    by_domain: dict[str, list[DqaSubject]] = {}
    for subject, domain in subjects:
        by_domain.setdefault(domain, []).append(subject)

    assets: list[dg.AssetsDefinition] = []
    checks: list = []
    jobs: list[dg.UnresolvedAssetJobDefinition] = []
    for domain in sorted(by_domain):
        domain_subjects = by_domain[domain]
        assets.append(_build_anchor(domain, len(domain_subjects)))
        for subject in domain_subjects:
            checks.append(_build_check(subject, domain))
        jobs.append(
            dg.define_asset_job(
                name=f"dqa_dso_{domain}_job",
                selection=dg.AssetSelection.assets(_anchor_key(domain)),
                executor_def=_DSO_EXECUTOR,
                description=(
                    f"DQA DSO {domain} reconciliation ({len(domain_subjects)} tables): "
                    f"DSO vs DSA after the DSO-sync."
                ),
                tags={"domain": "dqa", "dqa_track": "dso", "dqa_domain": domain},
            )
        )
    return assets, checks, jobs

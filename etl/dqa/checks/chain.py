# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""chain check: deep per-partition A->B->C validation for a FHIR resource.

A thin adapter over the chain-validation tool (``core.chain_validation``): it
runs a configured ``ChainValidator`` and maps its ``InvariantResult``s onto the
DQA model -- ``structural`` invariants gate ``passed`` (BLOCKING/ERROR),
``data_quality`` invariants are reported but non-blocking.

SCAFFOLDING: ``core.chain_validation`` lives on the
``feature/chain-validation-clean-dev-sync`` branch and is NOT merged into this
branch yet, so the import below is deliberately lazy -- registering this check
type is safe, and no subject wires it until the branch is merged and chain
configs exist. Invoking it before then raises ImportError (by design), rather
than silently passing.

params:
    chain_config_key: str   -- key into core.chain_validation.configs.REGISTRY
    partition:        str   -- partition key (or anchor date) to validate

requires: ("ifdb_oracle",)  -- the Oracle source connection the validator needs.
"""

from __future__ import annotations

from dqa.model import CheckOutcome
from dqa.registry import register_check


@register_check("chain")
def chain(context, subject, spec, resources) -> CheckOutcome:
    # Lazy import: chain_validation is not on this branch yet (see module docstring).
    from core.chain_validation.configs import REGISTRY
    from core.chain_validation.runner import ChainValidator

    cfg = REGISTRY[spec.params["chain_config_key"]]
    partition = spec.params["partition"]

    with resources.ifdb_oracle.get_connection() as oracle_conn:
        report = ChainValidator(cfg).run(partition, oracle_conn=oracle_conn)

    failed = [r.name for r in report.results if not r.passed]
    structural_failed = [
        r.name for r in report.results if r.severity == "structural" and not r.passed
    ]
    return CheckOutcome(
        passed=report.structural_passed,
        checked=len(report.results),
        violations=len(failed),
        examples=failed[:20],
        extra={
            "chain_config_key": spec.params["chain_config_key"],
            "partition": partition,
            "structural_failed": structural_failed,
            "report": report.render(),
        },
    )

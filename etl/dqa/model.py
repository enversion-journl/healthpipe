# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Core types for the Data Quality Assurance (DQA) framework.

See etl/docs/architecture/dqa/ for the design rationale. The runtime
model is intentionally small:

- ``Severity`` -- the single place mapping intent onto Dagster's
  ``AssetCheckSeverity`` + ``blocking``.
- ``Cadence`` -- daily (cheap, after ETL) vs weekly (heavy, Saturday).
- ``CheckSpecification`` -- one configured check on a subject; either a registered
  ``check_type`` or an inline ``fn`` (custom check).
- ``DqaSubject`` -- one table we assert quality about (a flat FHIR table or a
  DWH fact/dim).
- ``CheckOutcome`` -- the normalized result every check returns.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

import dagster as dg

if TYPE_CHECKING:
    # Avoid a runtime import cycle: CheckFn references the dataclasses below.
    CheckFn = Callable[
        [dg.AssetCheckExecutionContext, "DqaSubject", "CheckSpecification", Any], "CheckOutcome"
    ]


class Severity(Enum):
    """How a failing check should be treated."""

    BLOCKING = "blocking"  # AssetCheckSeverity.ERROR + blocking=True -> fails the DQA run
    ERROR = "error"  # AssetCheckSeverity.ERROR, non-blocking -> red, run continues
    WARN = "warn"  # AssetCheckSeverity.WARN
    INFO = "info"  # always-pass observability (e.g. invoice match-rate)

    @property
    def dagster_severity(self) -> dg.AssetCheckSeverity:
        if self in (Severity.WARN, Severity.INFO):
            return dg.AssetCheckSeverity.WARN
        return dg.AssetCheckSeverity.ERROR

    @property
    def is_blocking(self) -> bool:
        return self is Severity.BLOCKING


class Cadence(Enum):
    DAILY = "daily"  # cheap, intra-table -- runs right after the ETL
    WEEKLY = "weekly"  # expensive cross-table / cross-tier -- Saturday
    DSO = "dso"  # DSO-vs-DSA post-sync reconciliation -- own dqa_dso_<domain> jobs


class Track(Enum):
    FHIR = "fhir"
    # The DWH datamart, incl. HAIBA's delivery-level arc.haiba_* checks: those
    # live on DWH (not ARCHIVE) because they belong to the HAIBA datamart's own
    # quality surface, not to an Oracle-reconciled IFDB archive.
    DWH = "dwh"
    ARCHIVE = "arc"  # IFDB archive tables (arc.*) reconciled against the Oracle source


@dataclass(frozen=True)
class CheckSpecification:
    """One configured check on a subject.

    Either ``check_type`` resolves to a registered check (see ``registry.py``),
    or ``fn`` carries an inline custom check. When ``fn`` is set it wins over the
    registry; use ``check_type="custom"`` for labelling in that case.
    """

    check_type: str
    severity: Severity
    cadence: Cadence = Cadence.DAILY
    params: dict[str, Any] = field(default_factory=dict)
    name_suffix: str | None = None
    enabled: bool = True
    fn: CheckFn | None = None
    # Extra resource keys the check needs beyond ``subject.resource_key``
    # (e.g. ("fhir_database",) for a cross-DB custom check).
    requires: tuple[str, ...] = ()


@dataclass(frozen=True)
class ReplaySpec:
    """How to remediate a subject's dangling references by replaying the child.

    A non-null dangling FK in a flat FHIR table means the referenced parent was
    deleted *after* the child was posted (e.g. duplicate cleanup) -- failed
    resolution yields NULL, never a dangling UUID. Re-running the child's
    transform re-resolves the business key to the surviving parent, so the
    dangling-non-null violations are convergent under replay.

    The business key (``key_column``, e.g. ``identifier_value`` = ``OBJECT_ID``)
    is what feeds the DFM ``ids`` mode (``id_filter_column`` = ``filter_column``)
    against the ``entity`` archive table (e.g. ``ifdb_encounter``). Set on a
    subject only when its resource has a DFM ``ids``-capable replay pipeline.
    """

    key_column: str  # flat column carrying the business key (the DFM ids value)
    entity: str  # arc base table name, e.g. "ifdb_encounter"
    filter_column: str  # DFM id_filter_column on the archive, e.g. "OBJECT_ID"


@dataclass(frozen=True)
class DqaSubject:
    """A table we assert quality about: one flat FHIR table or one DWH fact/dim."""

    name: str  # e.g. "fhir_encounter" -> anchor asset dqa_fhir_encounter_<cadence>
    track: Track
    table: str  # schema-qualified, e.g. "fhir.Encounter"
    resource_key: str  # "mssql_target_staging" (DSA) | "mssql_target_dso" (DSO)
    identity_column: str  # PK / resource id, e.g. "encounter_id"
    depends_on: tuple[str, ...] = ()  # ETL asset keys -> DQA runs after the ETL
    checks: tuple[CheckSpecification, ...] = ()
    replay: ReplaySpec | None = None  # how to remediate dangling FKs (capture + DFM ids)
    # Soft-delete column (e.g. "dw_is_deleted"), if the table has one. Row-level
    # checks (not_null, uniqueness, referential_integrity) scope to active rows
    # (flag NULL or != 1) when set, so a correctly soft-deleted row -- e.g. one
    # retired by dqa_fhir_server_cleanup -- doesn't keep failing. Reconciliations
    # (source/oracle) intentionally ignore this: they compare the FULL key set to
    # verify delete *propagation*.
    deleted_column: str | None = None
    # Where to persist the check's row in ``dqa.Results``. Defaults to
    # ``resource_key``. DSO subjects run their comparison on the DSO server
    # (``resource_key="mssql_source_dso"``) but set this to ``"mssql_target_staging"``
    # so every result lands in the single ``dqa.Results`` table on DSA.
    results_resource_key: str | None = None
    # Source system this subject reconciles against, when a track spans several
    # (e.g. the ARCHIVE track: "ifdb" today, later "kmd"/"ils"/...). Splits the
    # track's jobs per source -> dqa_<track>_<source>_<cadence>_job. None (fhir/dwh)
    # keeps the plain dqa_<track>_<cadence>_job.
    source: str | None = None


@dataclass
class CheckOutcome:
    """Normalized result every check returns (``result.py`` converts this once)."""

    passed: bool
    checked: int = -1  # rows / keys examined (-1 = not measured)
    violations: int = 0  # rows / keys that failed
    examples: list[Any] = field(default_factory=list)  # up to N offending keys
    extra: dict[str, Any] = field(default_factory=dict)  # check-specific detail

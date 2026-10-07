# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DQA subject configs for the DWH fact tables (DSA / mssql_target_staging).

Both check families are derived from the lineage YAMLs, so there's no per-column
hand-config:

- **sentinel_rate** (daily): the rate of the ``-1`` unknown-member key per
  ``dw_ek_`` FK column (PK excluded; these columns are ``NOT NULL DEFAULT -1`` in
  the DDL, so a lookup miss is a ``-1``, not a NULL). A rising rate signals
  degrading lookup coverage -- which referential_integrity ignores as a sentinel.
  Facts run at Severity.INFO (observability) until a cap is added to
  ``_SENTINEL_MAX_RATES`` below, which flips that fact's check to WARN.
- **referential_integrity** (weekly): every ``dw_ek_<Dim>`` FK resolves to its
  dimension's ``dw_ek_<Dim>`` PK (``-1`` ignored), via
  ``references.referential_specs`` + ``dw_ek_`` naming / ``FK-Target``.

``depends_on`` is left empty for now; it will be set to the fact-build asset keys
when the post-ETL sensor is wired.
"""

from __future__ import annotations

from dqa.config.dwh_completeness import completeness_spec
from dqa.model import Cadence, CheckSpecification, DqaSubject, Severity, Track
from dqa.references import dw_ek_columns, referential_specs

_RES = "mssql_target_staging"  # DSA (fct/dim live here)

# The ERP facts that have monthly partition-completeness coverage today; the
# natural first set for DWH referential + null checks.
_FACTS: list[tuple[str, str]] = [
    # fct.DRGKontakt caps below are set from the observed baseline (dqa.results /
    # a direct measure over the full 35.6M-row load). The always-resolving FK
    # roles get a tight 0.01 cap to catch a lookup regression; the design-blank
    # roles (a contact without a diagnosis/procedure/henvisning maps to -1) get
    # headroom above baseline; henvisende is left observability-only (empty on
    # ~57 % of somatik rows and on ALL psyk rows by design). NOTE: the baseline
    # is somatik-only (no DRGPSYK in the data yet) — re-validate the design-blank
    # caps, especially DRGProcedureGruppe, after the first psyk delivery. Partition
    # completeness per (takstsystem, aktivitetsaar) is enforced harder than the
    # monthly check family: the fact asset's built-in control total compares live
    # rows and totalpris against the archive per slice and FAILS the run on any
    # deviation.
    ("dwh_drgkontakt", "fct.DRGKontakt"),
    ("dwh_fakturalinje", "fct.Fakturalinje"),
    ("dwh_faktura", "fct.Faktura"),
    ("dwh_postering", "fct.Postering"),
    ("dwh_vareudlevering", "fct.Vareudlevering"),
    ("dwh_finanskladdelinje", "fct.Finanskladdelinje"),
    ("dwh_budget", "fct.Budget"),
]

# Per-fact ``-1`` rate caps: table -> max_rate. A float caps every DW_EK_ FK column
# on the fact; a {column: rate} dict caps per column (columns absent stay
# observability-only). Any entry flips that fact's sentinel_rate check to WARN so a
# breach flags. Values are provisional placeholders; set real caps from observed
# baselines in dqa.results (detail_json.rates).
_SENTINEL_MAX_RATES: dict[str, float | dict[str, float]] = {
    "fct.Faktura": 0.5,
    "fct.Postering": {"DW_EK_Finanskladdelinje": 0.8, "DW_EK_Faktura": 0.8},
    "fct.Fakturalinje": 0.5,
    "fct.Vareudlevering": 0.5,
    "fct.Finanskladdelinje": 0.5,
    "fct.Budget": 0.5,
    # DRG baseline (full somatik load, -1 rate per FK role):
    #   Klassifikation/sygehus/kontaktansvar/Dato_ind/Dato_ud ~0.000, Borger 0.0003,
    #   forloebselement/Dato_henvisning ~0.027, DRGProcedureGruppe ~0.048,
    #   DRGDiagnoseGruppe ~0.150, Organisation_henvisende ~0.559.
    # Tight caps on the always-resolving roles (catch a regression); headroom on the
    # design-blank roles; henvisende omitted -> observability-only (see _FACTS note).
    "fct.DRGKontakt": {
        "dw_ek_Borger": 0.01,
        "dw_ek_DRGKontaktKlassifikation": 0.01,
        "dw_ek_Organisation_sygehus": 0.01,
        "dw_ek_Organisation_kontaktansvar": 0.01,
        "dw_ek_Dato_ind": 0.01,
        "dw_ek_Dato_ud": 0.01,
        "dw_ek_DRGDiagnoseGruppe": 0.25,
        "dw_ek_DRGProcedureGruppe": 0.15,
        "dw_ek_Organisation_forloebselement": 0.08,
        "dw_ek_Dato_henvisning": 0.08,
    },
}


def _bridge(name: str, table: str) -> DqaSubject:
    """DqaSubject for a bridge table: RI checks only (weekly), no PK-based sentinel_rate
    or fact completeness -- a bridge has a composite (dw_ek_..., dw_ek_...) grain, not a
    single dw_ek_<Table> identity, and isn't in the fact source-count mapping.
    ``identity_column`` is only used for the RI check's example-row diagnostics, so any
    real column on the table works; the first FK column is the natural choice here.
    """
    weekly = list(referential_specs(table, cadence=Cadence.WEEKLY))
    identity = dw_ek_columns(table)[0]
    return DqaSubject(
        name=name,
        track=Track.DWH,
        table=table,
        resource_key=_RES,
        identity_column=identity,
        checks=tuple(weekly),
    )


def _fact(name: str, table: str) -> DqaSubject:
    identity = f"dw_ek_{table.split('.')[-1]}"  # PK convention: dw_ek_<Fact>
    daily: list[CheckSpecification] = []
    fk_cols = [c for c in dw_ek_columns(table) if c != identity]
    if fk_cols:
        max_rate = _SENTINEL_MAX_RATES.get(table)
        params: dict = {"columns": fk_cols}
        if max_rate is not None:
            params["max_rate"] = max_rate
        daily.append(
            CheckSpecification(
                check_type="sentinel_rate",
                # INFO always passes, so a fact with a cap needs WARN to be able to flag.
                severity=Severity.WARN if max_rate is not None else Severity.INFO,
                cadence=Cadence.DAILY,
                params=params,
                name_suffix="dw_ek",
            )
        )
    weekly: list[CheckSpecification] = list(referential_specs(table, cadence=Cadence.WEEKLY))
    comp = completeness_spec(table)
    if comp is not None:
        weekly.append(comp)
    return DqaSubject(
        name=name,
        track=Track.DWH,
        table=table,
        resource_key=_RES,
        identity_column=identity,
        checks=tuple(daily) + tuple(weekly),
    )


# Bridges: many-to-many link tables with FK edges declared in their own lineage
# dir (see references.lineage._DWH_DIRS). RI-only coverage -- no fact-style
# sentinel_rate/completeness (composite grain, no single source-count query).
_BRIDGES: list[tuple[str, str]] = [
    ("dwh_bridge_leverandoervare_ilsvare", "bridge.LeverandoervareILSVare"),
    ("dwh_bridge_drgdiagnosegruppe", "bridge.DRGDiagnoseGruppe"),
    ("dwh_bridge_drgproceduregruppe", "bridge.DRGProcedureGruppe"),
]

DWH_SUBJECTS: list[DqaSubject] = [_fact(name, table) for name, table in _FACTS] + [
    _bridge(name, table) for name, table in _BRIDGES
]

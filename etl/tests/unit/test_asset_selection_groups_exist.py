# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests guarding group-based job selections.

Dagster's ``AssetSelection.groups()`` resolves unknown group names to an empty
selection without erroring, so a typo or a retired group silently narrows a
job's coverage. These tests fail loudly instead.
"""

import ast
from pathlib import Path

import pytest
from definitions import defs

ETL_DIR = Path(__file__).resolve().parents[2]
SCANNED_DIRS = ("jobs", "dqa")


def _asset_selection_group_strings(path: Path) -> list[str]:
    """Extract string literals passed to AssetSelection.groups() in a file."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    groups: list[str] = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        if node.func.attr != "groups":
            continue
        receiver = node.func.value
        receiver_name = (
            receiver.attr if isinstance(receiver, ast.Attribute) else getattr(receiver, "id", None)
        )
        if receiver_name != "AssetSelection":
            continue
        groups.extend(arg.value for arg in node.args if isinstance(arg, ast.Constant))
    return groups


def _collect_group_usages() -> list[tuple[str, str]]:
    """All (file, group) pairs from AssetSelection.groups() calls in scanned dirs."""
    usages: list[tuple[str, str]] = []
    for dirname in SCANNED_DIRS:
        for path in sorted((ETL_DIR / dirname).rglob("*.py")):
            rel = str(path.relative_to(ETL_DIR))
            usages.extend((rel, group) for group in _asset_selection_group_strings(path))
    return usages


def _defined_groups() -> set[str]:
    graph = defs.resolve_asset_graph()
    return {
        graph.get(key).group_name for key in graph.get_all_asset_keys() if graph.get(key).group_name
    }


@pytest.mark.unit
class TestAssetSelectionGroupsExist:
    """Every group string handed to AssetSelection.groups() must be defined."""

    def test_scan_finds_group_selections(self):
        usages = _collect_group_usages()
        assert usages, "AST scan found no AssetSelection.groups() calls — scanner is broken"

    def test_all_selected_groups_are_defined(self):
        defined = _defined_groups()
        unknown = [(f, g) for f, g in _collect_group_usages() if g not in defined]
        assert not unknown, (
            f"AssetSelection.groups() references undefined asset groups "
            f"(selection silently resolves empty): {unknown}"
        )


def _keys_i_alle_jobs() -> set[str]:
    """Alle assetnoegler, der er valgt af mindst ét job."""
    valgte: set[str] = set()
    for job in defs.jobs or []:
        job_def = defs.resolve_job_def(job.name) if hasattr(defs, "resolve_job_def") else job
        valgte |= {k.to_user_string() for k in job_def.asset_layer.executable_asset_keys}
    return valgte


@pytest.mark.unit
class TestKodesystemSynkHarSinBygger:
    """En truncate-synk uden sin bygger toemmer maalet i stedet for at opdatere det.

    Synken i gruppen ``codesystem_dso_sync`` koerer ``truncate_insert``, som
    rydder maaltabellen ogsaa ved nul kilderaekker. Staar byggeren ikke i noget
    job, bliver staging-tabellen aldrig fyldt i et miljoe, hvor ingen har koert
    den i haanden — og synken rydder saa kodesystemet paa DSO, uden at noget
    fejler. Teksten i vw_fhir bliver NULL, og fejlen ses foerst hos aftageren.
    """

    def test_hver_synk_har_en_bygger(self):
        graf = defs.resolve_asset_graph()
        byggere = {
            parent.to_user_string()
            for key in graf.get_all_asset_keys()
            if graf.get(key).group_name == "codesystem_dso_sync"
            for parent in graf.get(key).parent_keys
            if parent.to_user_string().startswith("cs_")
        }
        assert byggere, "ingen byggere fundet bag codesystem_dso_sync — testen rammer forbi"

    def test_hver_bygger_koerer_i_mindst_ét_job(self):
        graf = defs.resolve_asset_graph()
        byggere = {
            parent.to_user_string()
            for key in graf.get_all_asset_keys()
            if graf.get(key).group_name == "codesystem_dso_sync"
            for parent in graf.get(key).parent_keys
            if parent.to_user_string().startswith("cs_")
        }
        valgte = _keys_i_alle_jobs()
        uden_job = sorted(b for b in byggere if b not in valgte)
        assert not uden_job, (
            "kodesystem-byggere staar i intet job, mens deres truncate-synk koerer: "
            f"{uden_job}. Synken ville rydde kodesystemet paa DSO i stedet for at opdatere det."
        )

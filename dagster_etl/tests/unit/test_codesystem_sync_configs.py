# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""CodeSystem DSO sync must target the [codesystem] schema with PascalCase tables."""

from DSO.load.sync_configs import ALL_SYNC_CONFIGS, CODESYSTEM_SYNC_CONFIGS

EXPECTED_CODESYSTEMS = ["sks", "sor", "ean", "lager", "okosted", "rekvirent", "sfi"]
KINDS = ["concept", "properties", "hierarchy"]
PASCAL = {
    "sks": "SKS",
    "sor": "SOR",
    "ean": "EAN",
    "lager": "Lager",
    "okosted": "Okosted",
    "rekvirent": "Rekvirent",
    "sfi": "SFI",
}
KIND_PASCAL = {"concept": "Concept", "properties": "Properties", "hierarchy": "Hierarchy"}


def test_all_codesystems_present_in_codesystem_schema():
    for cs in EXPECTED_CODESYSTEMS:
        for kind in KINDS:
            key = f"codesystem_{cs}_{kind}"
            assert key in CODESYSTEM_SYNC_CONFIGS, f"missing {key}"
            cfg = CODESYSTEM_SYNC_CONFIGS[key]
            assert cfg.schema == "codesystem", f"{key} targets {cfg.schema}, not codesystem"
            expected = f"{PASCAL[cs]}{KIND_PASCAL[kind]}"
            assert cfg.table == expected, f"{key} table is {cfg.table}, expected {expected}"


def test_no_codesystem_data_targets_ref_schema():
    for key, cfg in ALL_SYNC_CONFIGS.items():
        if cfg.schema == "ref":
            assert not cfg.table.endswith(
                ("Concept", "Properties", "Hierarchy")
            ), f"{key} syncs codesystem data into [ref]: {cfg.table}"


def test_no_codesystem_ref_sync_configs_symbol():
    import DSO.load.sync_configs as m

    assert not hasattr(
        m, "CODESYSTEM_REF_SYNC_CONFIGS"
    ), "CODESYSTEM_REF_SYNC_CONFIGS must be removed (folded into CODESYSTEM_SYNC_CONFIGS)"


def test_sfi_not_double_synced():
    sfi_keys = [k for k, c in ALL_SYNC_CONFIGS.items() if c.table.startswith("SFI")]
    assert sorted(sfi_keys) == [
        "codesystem_sfi_concept",
        "codesystem_sfi_hierarchy",
        "codesystem_sfi_properties",
    ], f"unexpected sfi sync configs: {sfi_keys}"

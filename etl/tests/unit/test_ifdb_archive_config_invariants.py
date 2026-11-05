# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Invarianter for IFDB's extract- og arkiv-configs, som beskytter mod stille datafejl.

Testene her siger IKKE noget om, hvad den enkelte config indeholder — de
håndhæver de sammenhænge, der skal gælde på tværs af extract, arkiv og DDL,
uanset hvilken tabel man tilføjer. Hver test svarer til en fejl, der er dyr og
svær at opdage i drift:

``kolonnedækning``
    En kolonne, der hverken er merge-nøgle eller står i ``non_key_columns``,
    skrives ALDRIG til arkivet. MERGE'en lykkes, ext-tabellen har værdien, og
    kolonnen står tom i arc for evigt. Intet fejler.

``index_keys == merge_keys``
    ext får sit clustered index på ``index_keys``, mens MERGE'en joiner på
    ``merge_keys``. Er de forskellige, degenererer MERGE'en til en scanning.

``nøglekolonner må ikke være NVARCHAR(MAX)``
    Dokumenteret fælde: uden ``column_types`` (eller med MAX på en nøgle)
    kan kolonnen ikke indekseres, og arkiv-MERGE'en bliver ekstremt langsom.

``full load ⇔ delete-detektion``
    Et snapshot uden ``detect_deletes`` pensionerer aldrig fjernede rækker.
    Omvendt ville delete-detektion på et delta-feed soft-slette næsten hele
    arkivet — ``archive_merge`` fanger det i drift, men en config, der beder om
    det, er en fejl i hensigten.

``checkpoint fra det loft, extractet brugte``
    Et delta-arkiv skal tage sit checkpoint fra extractets ``used_ceiling_key``.
    Læser det i stedet det fælles loft, kan en flow-start i en senere kørsel
    flytte checkpointet forbi rækker, extractet aldrig har læst.

``DDL-pariteten``
    Afviger arkiv-DDL'ens primærnøgle fra ``merge_keys``, kører MERGE'en på en
    anden grain end tabellen håndhæver: enten en PK-violation ved første kørsel
    eller dublerede rækker nedstrøms.

Invarianterne er verificeret at holde for samtlige IFDB-configs, da de blev
skrevet, så en fejl her er en reel regression — ikke et efterslæb.
"""

import re
from pathlib import Path

import pytest
from core.dfm_config import ArchiveMergeConfig
from DSI.oracle import ifdb_archive
from DSI.oracle.ifdb_extract import IFDB_EXTRACT_CONFIGS

pytestmark = pytest.mark.unit

# ifdb_archive eksponerer én modul-variabel pr. tabel frem for en liste.
_ARCHIVE_CONFIGS = [v for v in vars(ifdb_archive).values() if isinstance(v, ArchiveMergeConfig)]
_EXTRACT_BY_TARGET = {c.target_table: c for c in IFDB_EXTRACT_CONFIGS}

_DDL_DIR = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl" / "sources" / "ifdb"


def _ids(cfg: ArchiveMergeConfig) -> str:
    return cfg.target_table


def _extract_for(arc: ArchiveMergeConfig):
    """Find det extract, der fylder arkivets kildetabel."""
    return _EXTRACT_BY_TARGET.get(arc.source_table)


def _pairs() -> list[tuple[ArchiveMergeConfig, object]]:
    ud = []
    for arc in _ARCHIVE_CONFIGS:
        exc = _extract_for(arc)
        if exc is not None:
            ud.append((arc, exc))
    return ud


def test_der_findes_configs_at_teste():
    """Guard the guard: en tom liste ville få alt nedenfor til at bestå tomt."""
    assert len(_ARCHIVE_CONFIGS) > 30
    assert len(_pairs()) == len(_ARCHIVE_CONFIGS), "et arkiv uden extract kan ikke fyldes"


@pytest.mark.parametrize(("arc", "exc"), _pairs(), ids=lambda v: getattr(v, "target_table", ""))
def test_alle_extract_kolonner_skrives_til_arkivet(arc, exc):
    """En udeladt kolonne står tom i arc for evigt, uden at noget fejler."""
    daekket = set(arc.merge_keys) | set(arc.non_key_columns)
    glemt = sorted(set(exc.columns) - daekket)
    assert not glemt, f"{arc.target_table}: skrives aldrig til arkivet: {glemt}"


@pytest.mark.parametrize(("arc", "exc"), _pairs(), ids=lambda v: getattr(v, "target_table", ""))
def test_arkiv_config_naevner_kun_kolonner_extractet_leverer(arc, exc):
    """En kolonne i configen, som ext ikke har, får MERGE'en til at fejle."""
    ukendte = sorted((set(arc.merge_keys) | set(arc.non_key_columns)) - set(exc.columns))
    assert not ukendte, f"{arc.target_table}: findes ikke i ext: {ukendte}"


@pytest.mark.parametrize(("arc", "exc"), _pairs(), ids=lambda v: getattr(v, "target_table", ""))
def test_ext_er_indekseret_paa_den_noegle_merge_joiner_paa(arc, exc):
    """Ellers scanner MERGE'en ext i stedet for at søge."""
    assert tuple(exc.index_keys or ()) == tuple(
        arc.merge_keys
    ), f"{arc.target_table}: index_keys={exc.index_keys} merge_keys={arc.merge_keys}"


@pytest.mark.parametrize(("arc", "exc"), _pairs(), ids=lambda v: getattr(v, "target_table", ""))
def test_noeglekolonner_kan_indekseres(arc, exc):
    """NVARCHAR(MAX) på en nøgle gør ext-indekset umuligt og MERGE'en langsom."""
    assert exc.column_types, f"{arc.target_table}: uden column_types bliver alt NVARCHAR(MAX)"
    for key in arc.merge_keys:
        typ = exc.column_types.get(key)
        assert typ is not None, f"{arc.target_table}: nøglen {key} mangler i column_types"
        assert "MAX" not in typ.upper(), f"{arc.target_table}.{key} er {typ}"


@pytest.mark.parametrize(("arc", "exc"), _pairs(), ids=lambda v: getattr(v, "target_table", ""))
def test_noeglekolonner_er_not_null(arc, exc):
    """En nullable merge-nøgle gør både PK og MERGE-adfærd uforudsigelig."""
    for key in arc.merge_keys:
        typ = exc.column_types.get(key, "")
        assert "NOT NULL" in typ.upper(), f"{arc.target_table}.{key} er {typ!r}"


@pytest.mark.parametrize(("arc", "exc"), _pairs(), ids=lambda v: getattr(v, "target_table", ""))
def test_snapshot_pensionerer_fjernede_raekker(arc, exc):
    """Full load uden delete-detektion efterlader slettede rækker i arkivet."""
    if exc.is_full_load:
        assert arc.detect_deletes, f"{arc.target_table}: snapshot uden detect_deletes"


# Koblingstabellen retirerer fjernede koblinger ved en fuld genindlæsning. Den
# tåler det, fordi MERGE-motoren selv slår NOT MATCHED BY SOURCE fra for
# delta/period/ids og ved et udtræk med nul rækker — og fordi den adfærd er
# efterprøvet for præcis denne konfiguration i
# test_ifdb_join_delta_contracts.test_referral_link_cleanup_requires_a_nonempty_complete_extract.
# Undtagelsen står navngivet her, så den ikke kan brede sig i tavshed.
_DELETE_DETEKTION_UDEN_FULDT_SNAPSHOT = {"ifdb_pas_diagnose_henvisning_ref"}


@pytest.mark.parametrize(
    ("arc", "exc"),
    [
        (arc, exc)
        for arc, exc in _pairs()
        if exc.source_schema == "IFDB" and exc.flow_start_from and not exc.is_full_load
    ],
    ids=lambda v: getattr(v, "target_table", ""),
)
def test_delta_arkiv_tager_checkpoint_fra_extractets_loft(arc, exc):
    assert arc.flow_start_key == exc.used_ceiling_key


@pytest.mark.parametrize("arc", _ARCHIVE_CONFIGS, ids=_ids)
def test_delete_detektion_kraever_et_fuldt_snapshot(arc):
    """NOT MATCHED BY SOURCE på et delta-feed ville soft-slette hele arkivet."""
    if arc.detect_deletes and arc.target_table not in _DELETE_DETEKTION_UDEN_FULDT_SNAPSHOT:
        assert (
            arc.upstream_is_full_load
        ), f"{arc.target_table}: detect_deletes uden upstream_is_full_load"


# ---------------------------------------------------------------------------
# DDL-paritet
# ---------------------------------------------------------------------------


def _ddl_text() -> str:
    return "\n".join(p.read_text() for p in sorted(_DDL_DIR.glob("*.sql")))


def _ddl_block(ddl: str, table: str) -> str | None:
    marker = f"CREATE TABLE [arc].[{table}]"
    if marker not in ddl:
        return None
    return ddl.split(marker, 1)[1].split("\nEND", 1)[0]


@pytest.mark.parametrize("arc", _ARCHIVE_CONFIGS, ids=_ids)
def test_arkivtabellen_findes_i_ddl(arc):
    """Uden DDL findes tabellen ikke, når assetet kører første gang."""
    assert (
        _ddl_block(_ddl_text(), arc.target_table) is not None
    ), f"{arc.target_table}: ingen CREATE TABLE i sources/ifdb/"


@pytest.mark.parametrize("arc", _ARCHIVE_CONFIGS, ids=_ids)
def test_ddl_primaernoegle_er_configens_merge_noegle(arc):
    """Divergens = MERGE på en anden grain end tabellen håndhæver."""
    blok = _ddl_block(_ddl_text(), arc.target_table)
    assert blok is not None
    fundet = re.search(r"PRIMARY KEY CLUSTERED \(([^)]*)\)", blok)
    assert fundet, f"{arc.target_table}: ingen clustered PK i DDL"
    pk = tuple(c.strip().strip("[]") for c in fundet.group(1).split(","))
    assert pk == tuple(
        arc.merge_keys
    ), f"{arc.target_table}: DDL-PK {pk} != merge_keys {tuple(arc.merge_keys)}"


@pytest.mark.parametrize(("arc", "exc"), _pairs(), ids=lambda v: getattr(v, "target_table", ""))
def test_ddl_har_alle_kildekolonner(arc, exc):
    """En kolonne i extractet uden DDL-kolonne får MERGE'en til at fejle."""
    blok = _ddl_block(_ddl_text(), arc.target_table)
    assert blok is not None
    mangler = [c for c in exc.columns if f"[{c}]" not in blok]
    assert not mangler, f"{arc.target_table}: mangler i DDL: {mangler}"


@pytest.mark.parametrize("arc", _ARCHIVE_CONFIGS, ids=_ids)
def test_ddl_har_audit_kolonnerne(arc):
    """dw_timestamp_update er DSO-syncens watermark; uden den stopper flowet."""
    blok = _ddl_block(_ddl_text(), arc.target_table)
    assert blok is not None
    for kolonne in ("dw_timestamp_insert", "dw_timestamp_update", "dw_is_deleted"):
        assert f"[{kolonne}]" in blok, f"{arc.target_table}: mangler {kolonne}"


@pytest.mark.parametrize("arc", _ARCHIVE_CONFIGS, ids=_ids)
def test_default_constraints_er_navngivne(arc):
    """Unavngivne DEFAULT'er får per-database-navne (DF__tabel__kol__1A2B3C4D).

    Konventionen og begrundelsen står i migrationen
    20260825b_name_default_constraints.sql, som omdøbte 504 constraints.
    """
    blok = _ddl_block(_ddl_text(), arc.target_table)
    assert blok is not None
    for match in re.finditer(r"DEFAULT\s*\(", blok):
        foran = blok[: match.start()]
        linje = foran.rsplit("\n", 1)[-1]
        assert (
            "CONSTRAINT [DF_" in linje
        ), f"{arc.target_table}: unavngivet DEFAULT i linjen {linje.strip()!r}"

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Guard: every stored dw_ek_Dato_<rolle> on an ERP fct/dim has a dato_tid_<rolle> companion.

The companion (raw TRY_CONVERT(DATETIME2, ...) timestamp, see
etl/DSA/SQL/common/dato_dimension.py::dato_tid) lets the key be
recomputed if dim.Dato's seeded range (19700101-20501231) is later widened.
"""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[2]  # -> etl/

DDL_FILES = [
    "infrastructure/ddl/tables/erp/02_dims.sql",
    "infrastructure/ddl/tables/erp/03_facts.sql",
]


def _scan():
    """Per DDL file: the tables carrying a dw_ek_Dato_ key, plus every key missing a companion."""
    evaluated = {rel: [] for rel in DDL_FILES}
    missing = []
    for rel in DDL_FILES:
        txt = (REPO / rel).read_text(encoding="utf-8")
        for block in re.split(r"(?=CREATE TABLE )", txt):
            m = re.search(r"CREATE TABLE \[?(\w+)\]?\.\[?(\w+)\]?", block)
            if not m:
                continue
            table = f"{m.group(1)}.{m.group(2)}"
            keys = set(re.findall(r"\[(dw_ek_Dato_\w+)\]", block))
            if not keys:
                continue
            evaluated[rel].append(table)
            comps = set(re.findall(r"\[(dato_tid_\w+)\]", block))
            for k in sorted(keys):
                rolle = k[len("dw_ek_Dato_") :]
                if f"dato_tid_{rolle}" not in comps:
                    missing.append(f"{table}.{k}")
    return evaluated, missing


def test_scan_evaluates_tables_in_every_ddl_file():
    evaluated, _ = _scan()
    empty = [rel for rel, tables in evaluated.items() if not tables]
    assert not empty, (
        f"Found no table with a dw_ek_Dato_ column in {empty} — the parser or the DDL "
        "layout changed, and the companion check passes without checking anything."
    )


def test_every_dw_ek_dato_has_dato_tid_companion():
    _, missing = _scan()
    assert not missing, (
        "Every stored dw_ek_Dato_<rolle> on an ERP fct/dim MUST have a raw "
        "dato_tid_<rolle> DATETIME2 companion (so the key can be recomputed if "
        f"dim.Dato's range widens). Missing companions: {missing}"
    )

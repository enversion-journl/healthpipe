# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Convention test: only the write point writes, changes or constructs a Patient; read with AST.

Exceptions for construction are listed in ``ALLOWED_CONSTRUCTORS``; there are none for writing.
"""

import ast
import pathlib

import pytest

pytestmark = pytest.mark.unit

ETL = pathlib.Path(__file__).resolve().parents[2]
PATIENT_PACKAGE = ETL / "DSA" / "FHIR" / "Patient"

# The one module that may write a Patient to the server. Not the package — the module.
WRITER_MODULE = "DSA/FHIR/Patient/patient_service.py"

# Functions that write resources to the FHIR server. If they are called with
# resource_type="Patient" outside the package, it is another write path.
WRITERS = {
    "batch_upsert_resources",
    "mark_resources_deleted",
    "post_resources_direct",
    "post_resource_dicts_direct",
    "prepare_resources_for_upsert",
    "resolve_resource_ids_for_upsert",
}

# Modules that may construct ``Patient(...)`` outside the package, and why.
ALLOWED_CONSTRUCTORS: set[str] = set()

# No partial Patient writers outside the service, not even the tombstone.
ALLOWED_PARTIAL_WRITERS: set[str] = set()


def _source_files(include_package: bool = False):
    for sti in sorted(ETL.rglob("*.py")):
        rel = sti.relative_to(ETL)
        if rel.parts[0] in ("tests", ".venv", "build") or "notebooks" in rel.parts:
            continue
        if not include_package and PATIENT_PACKAGE in sti.parents:
            continue
        yield sti, rel.as_posix()


def patient_writers(files) -> list[str]:
    """Calls that write Patient to the server, as ``path:line name``."""
    fund = []
    for sti, rel in files:
        tree = ast.parse(sti.read_text(encoding="utf-8"))
        for node, name in _calls(tree):
            if name in WRITERS and _keyword_str(node, "resource_type") == "Patient":
                fund.append(f"{rel}:{node.lineno} {name}")
    return fund


def _calls(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            f = node.func
            name = (
                f.id
                if isinstance(f, ast.Name)
                else (f.attr if isinstance(f, ast.Attribute) else None)
            )
            yield node, name


def _keyword_str(node: ast.Call, key: str) -> str | None:
    for kw in node.keywords:
        if kw.arg == key and isinstance(kw.value, ast.Constant):
            return kw.value.value
    return None


def test_only_the_writer_module_writes_patient():
    """Every upsert/post with resource_type="Patient" must live in the one writer module —
    other files in the package must not have their own write either."""
    fund = [
        f
        for f in patient_writers(_source_files(include_package=True))
        if not f.startswith(WRITER_MODULE)
    ]
    assert fund == [], "Patient is written outside the write point's module:\n" + "\n".join(fund)
    assert any(
        f.startswith(WRITER_MODULE) for f in patient_writers(_source_files(include_package=True))
    ), "the writer module no longer writes itself — has the write been moved?"


def test_the_check_catches_a_new_writer_in_the_package(tmp_path):
    """The check itself must be able to fail: a new package file with a Patient upsert is found."""
    ny = tmp_path / "DSA" / "FHIR" / "Patient" / "ny_skriver.py"
    ny.parent.mkdir(parents=True)
    ny.write_text(
        'def f(db, patients):\n    db.batch_upsert_resources(resources=patients, resource_type="Patient")\n',
        encoding="utf-8",
    )
    fund = patient_writers([(ny, "DSA/FHIR/Patient/ny_skriver.py")])
    assert fund == ["DSA/FHIR/Patient/ny_skriver.py:2 batch_upsert_resources"]


def test_nobody_constructs_patient_outside_the_package():
    """``Patient(...)`` outside the builder is a new write path in the making; exceptions only."""
    fund = []
    for sti, rel in _source_files():
        if rel in ALLOWED_CONSTRUCTORS:
            continue
        src = sti.read_text(encoding="utf-8")
        if "Patient" not in src:
            continue
        tree = ast.parse(src)
        importerer_patient = any(
            isinstance(n, ast.ImportFrom)
            and n.module
            and n.module.endswith(".patient")
            and any(a.name == "Patient" for a in n.names)
            for n in ast.walk(tree)
        )
        if not importerer_patient:
            continue
        for node, name in _calls(tree):
            if name == "Patient":
                fund.append(f"{rel}:{node.lineno}")
    assert fund == [], "Patient is constructed outside the builder:\n" + "\n".join(fund)


def _partial_writers():
    """Files outside the package that change a resource partially with jsonb_set."""
    fund = {}
    for sti, rel in _source_files():
        src = sti.read_text(encoding="utf-8")
        if "jsonb_set" in src and "patient" in src.lower():
            fund[rel] = [nr for nr, linje in enumerate(src.splitlines(), 1) if "jsonb_set" in linje]
    return fund


def test_no_new_partial_write_of_patient():
    """The old bridge changed a resource with jsonb_set directly in the table. Never again —
    all Patient mutations must go through the write point."""
    fund = _partial_writers()
    nye = {rel: nr for rel, nr in fund.items() if rel not in ALLOWED_PARTIAL_WRITERS}
    assert nye == {}, "Partial write of Patient outside the write point:\n" + "\n".join(
        f"{rel}:{nr}" for rel, nrs in nye.items() for nr in nrs
    )


def test_the_exceptions_still_exist_and_are_still_needed():
    """An exception for a file that does not exist or no longer does what it is
    excepted for is a forgotten cleanup."""
    for rel in ALLOWED_CONSTRUCTORS:
        assert (ETL / rel).exists(), rel
    fund = _partial_writers()
    for rel in ALLOWED_PARTIAL_WRITERS:
        assert (ETL / rel).exists(), rel
        assert rel in fund, f"{rel} no longer writes partially — remove the exception"

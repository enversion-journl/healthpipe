# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contract pins for the Organisation UUID-grain + live-SOR uniqueness path.

Bugs these catch:
  * dim.Organisation reading the retired map.Organisation / SOR-CSV path
  * ref MERGE forcing dw_is_deleted = 0 on match (would resurrect fhir-deleted orgs)
  * ref resurrecting BEFORE the MERGE, which frees a live SOR code without also
    correcting a stale sor_code
  * dim MERGE dropping deleted ref rows so dim.dw_is_deleted never becomes 1
"""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_DIM_ORGANISATION = (
    Path(__file__).resolve().parents[2] / "DSA" / "SQL" / "dim" / "dim_Organisation.py"
)
_REF_ORGANISATION = Path(__file__).resolve().parents[2] / "DSA" / "SQL" / "ref" / "organisation.py"


@pytest.mark.unit
def test_dim_organisation_does_not_read_the_retired_legacy_path():
    sql = _DIM_ORGANISATION.read_text()
    assert "[map].[Organisation]" not in sql, "dim.Organisation must not read map.Organisation"
    assert (
        "import map_Organisation" not in sql
    ), "dim.Organisation must not import the map_Organisation asset"
    assert (
        "from DSA.SQL.staging" not in sql
    ), "dim.Organisation must not read the SOR-CSV staging directly"


@pytest.mark.unit
def test_ref_organisation_copies_fhir_deleted_flag():
    sql = _REF_ORGANISATION.read_text()
    assert "f.dw_is_deleted" in sql, "ref MERGE must read fhir.Organization.dw_is_deleted"
    assert "src.dw_is_deleted" in sql, "ref MERGE must persist the fhir flag, not force 0"
    assert (
        "tgt.dw_is_deleted <> src.dw_is_deleted" in sql
    ), "MATCHED must fire when the fhir flag flips, including live -> deleted"
    assert "NOT EXISTS" in sql, "anti-join must still soft-delete UUIDs gone from fhir"

    # Ordering is the whole contract for UQ_Ref_Organisation_SorCode_Live: every
    # deletion ahead of the MERGE, and no resurrection ahead of it. A pre-MERGE
    # statement that clears dw_is_deleted would free a code without also fixing a
    # stale sor_code, and the code may already be live on another UUID.
    pre_merge = sql[: sql.find("MERGE INTO")]
    assert (
        "NOT EXISTS" in pre_merge
    ), "anti-join must free live SOR codes before MERGE inserts a new UUID"
    assert "f.identifier_value_sor IS NULL" in pre_merge, (
        "a fhir row that lost its SOR id is invisible to the MERGE, so it must be "
        "soft-deleted first or it keeps a stale sor_code and blocks the next UUID"
    )
    pre_merge_assignments = re.findall(r"SET\s+dw_is_deleted\s*=\s*([^,\s]+)", pre_merge)
    assert pre_merge_assignments, "the pre-MERGE soft-deletes must assign dw_is_deleted"
    assert set(pre_merge_assignments) == {"1"}, (
        "resurrection belongs in the MERGE, where dw_is_deleted and sor_code are set "
        f"in one statement; pre-MERGE assignments were {pre_merge_assignments}"
    )


@pytest.mark.unit
def test_dim_organisation_mirrors_ref_deleted_flag():
    sql = _DIM_ORGANISATION.read_text()
    assert "r.dw_is_deleted" in sql, "dim MERGE must read ref.Organisation.dw_is_deleted"
    assert (
        "WHERE r.dw_is_deleted = 0" not in sql
    ), "dim MERGE must include deleted ref rows or dim.dw_is_deleted never becomes 1"
    assert "d.dw_is_deleted = r.dw_is_deleted" in sql, (
        "dim must copy the flag from ref before MERGE so a new live UUID "
        "for the same SOR code does not collide with the unique live-kode index"
    )


@pytest.mark.unit
def test_dim_organisation_unfolds_parent_hierarchy():
    sql = _DIM_ORGANISATION.read_text()
    assert "kode_niveau1" in sql, "pass 2 must persist unfolded SOR hierarchy"
    assert "hierarki_niveau" in sql
    assert "f.type_display" not in sql, "type was dropped from dim.Organisation"
    assert "EXCEPT" in sql, "hierarchy UPDATE must skip unchanged rows (watermark)"

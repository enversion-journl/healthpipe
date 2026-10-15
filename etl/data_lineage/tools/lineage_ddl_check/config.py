# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

from pathlib import Path

# config.py -> lineage_ddl_check/ -> tools/ -> data_lineage/ -> etl/
_ETL_ROOT = Path(__file__).resolve().parents[3]

# Base-DDL end-state CREATE TABLE files live here (migrations/ is excluded by the parser).
DDL_TABLES_DIR = _ETL_ROOT / "infrastructure" / "ddl" / "tables"

# Lineage YAML lives in data_lineage/<schema>/*.yml
LINEAGE_DIR = _ETL_ROOT / "data_lineage"

# Only these schemas are verified.
TARGET_SCHEMAS: tuple[str, ...] = ("dim", "fct", "fhir", "bridge", "dm")

# --- Allowlists -------------------------------------------------------------
# Audit/system columns that the DDL carries on every table but lineage
# intentionally never documents. Applied to all tables.
SYSTEM_COLUMNS: frozenset[str] = frozenset(
    {
        "dw_timestamp_insert",
        "dw_timestamp_update",
    }
)

# Fully-qualified DDL tables ("schema.Table") that intentionally have no lineage
# file (internal/operational stores — not star-schema dims/facts or FHIR resource
# projections). Add entries here ONLY for by-design omissions, each with a reason.
# This is never a substitute for fixing lineage/DDL; populated during Task 6.
IGNORED_TABLES: frozenset[str] = frozenset(
    {
        # FHIR SFI datapoint store — materialized consumer/operational store
        # (refreshed via dso_refresh_sfi_datapoint_* assets), not a lineage resource.
        "fhir.SFI_datapoint_store",
        # Arbejdstabel til SWITCH-indlæsning af backfill-vinduer i storen: identiske
        # kolonner, normalt tom, aldrig synlig for aftagere (se
        # docs/architecture/sfi_store_partitionering.md).
        "fhir.SFI_datapoint_store_backfill",
        "fhir.SFI_datapoint_store_efterslaeb",
        # Questionnaire-internal child table — flat per-item rows
        # (composite PK questionnaire_id+link_id+item_key), not a resource projection.
        "fhir.Questionnaire_item",
        # QuestionnaireResponse-internal child table — flat per-answer item rows,
        # internal child of the QR resource, not a resource projection.
        "fhir.QuestionnaireResponse_item",
    }
)

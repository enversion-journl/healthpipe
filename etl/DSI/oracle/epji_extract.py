# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""EPJI extracts: LOCATION and the three PRACTITIONER tables.

LOCATION: full snapshot of EPJI ``LOCATION`` LEFT JOIN ``LOCATION_IDENTIFIER`` (SYSTEM
like '%SorIdentifier') into ``ext.epji_location``, built via the shared DFM
extract factory (``create_dfm_extraction_assets``) — the same pattern IFDB
uses, rather than a hand-rolled asset.

``display`` is ``LOCATION.NAME`` (human-readable), falling back to the
``EXTERNAL_ID`` code itself when ``NAME`` is empty — not a repeat of the
code, so it's actually useful in reporting. Both this fallback and the
whitespace trimming that used to happen in Python (``shape_ext_rows``) are
now done in the SQL itself, so ext stays a faithful raw landing.

``LOCATION`` is historized: a rename creates a new row on the same
``EXTERNAL_ID`` with a fresh ``OPERATIONAL_FROM``, closing the prior row's
``OPERATIONAL_TO``. ``OPERATIONAL_TO IS NULL`` marks the single current
period per location (confirmed against prod-epjsyd; ``OPERATIONAL_FROM`` is
never future-dated), so the snapshot filters on it — otherwise a rename
would return two rows per code.

Even with that filter, EXTERNAL_ID has no enforced uniqueness upstream (two
distinct EPJI locations could share a code by data-entry error), so
``arc_epji_shakalike`` (DSI/oracle/epji_archive.py) deduplicates on ``code``
via ``ArchiveMergeConfig.dedup_source_on_keys`` rather than this extract
doing it in Python — that keeps ext a faithful raw landing and reports the
conflict as ``rows_dedup_dropped`` in the archive asset's metadata (with a
warning log) via the same generic mechanism every other no-upstream-PK
archive uses, instead of one-off extract-level logic.

``LOCATION_IDENTIFIER`` has no update timestamp; SOR codes are updated in
place on the existing identifier row. A watermarked delta would miss those
changes, so every run DROP/CREATEs the ext table from current source state
(``is_full_load=True``, same pattern as ``extract_shak_register`` / SydAX
reference full-loads).

Named after the source (``epji_location``), not the SHAK-a-like consumer:
this is EPJI's LOCATION table enriched with its SOR identifier, not a
SHAK-a-like-specific shape — cs_shakalike is one consumer of it, matching
IFDB's arc.ifdb_location naming (also joined with a second table, also
named after the grain table alone).

PRACTITIONER, PRACTITIONER_IDENTIFIER, PRACTITIONER_ROLE: 1:1 full snapshots.
None of them has a transfer timestamp and PRACTITIONER_IDENTIFIER has no time
column at all, so there is no delta column to filter on. PRACTITIONER has one
row per EPJ account, not per person. It carries names, and
PRACTITIONER_IDENTIFIER carries CPR: never log field values from them.
"""

from core.resources import EPJIOracleResource
from DSI.connectors.extract_config import DfmExtractConfig
from DSI.connectors.factory import create_dfm_extraction_assets

EXT_COLUMNS = ("location_pk", "code", "display", "sor_code", "identifier_system")

_EPJI_LOCATION_COLUMN_TYPES = {
    "location_pk": "NVARCHAR(100) NULL",
    "code": "NVARCHAR(100) NOT NULL",
    "display": "NVARCHAR(500) NULL",
    "sor_code": "NVARCHAR(100) NULL",
    "identifier_system": "NVARCHAR(500) NULL",
}

# Double-quoted aliases force Oracle to preserve exact lowercase casing in
# cursor.description (an unquoted alias comes back upper-cased), so `columns`
# below matches the ext table's actual column names one-for-one — the same
# names arc.epji_location already uses.
_SNAPSHOT_SQL = """
SELECT
    TRIM(TO_CHAR(l.PK)) AS "location_pk",
    TRIM(l.EXTERNAL_ID) AS "code",
    COALESCE(NULLIF(TRIM(l.NAME), ''), TRIM(l.EXTERNAL_ID)) AS "display",
    TRIM(li.VALUE) AS "sor_code",
    TRIM(li.SYSTEM) AS "identifier_system"
FROM EPJI.LOCATION l
LEFT JOIN EPJI.LOCATION_IDENTIFIER li
    ON li.LOCATION_PK = l.PK
    AND li.SYSTEM LIKE '%SorIdentifier'
WHERE TRIM(l.EXTERNAL_ID) IS NOT NULL
    AND l.OPERATIONAL_TO IS NULL
"""

EPJI_LOCATION_EXTRACT_CONFIG = DfmExtractConfig(
    source_schema="EPJI",
    source_table="LOCATION",
    target_table="epji_location",
    is_full_load=True,
    checkpoint_from=("arc", "epji_location"),
    columns=EXT_COLUMNS,
    column_types=_EPJI_LOCATION_COLUMN_TYPES,
    select_from_expr=_SNAPSHOT_SQL,
    index_keys=("code",),
    description=(
        "EPJI LOCATION + LOCATION_IDENTIFIER (SorIdentifier) full snapshot. "
        "No watermark: identifier updates are in-place without a source "
        "timestamp. OPERATIONAL_TO IS NULL keeps only the current period "
        "per location."
    ),
)

epji_location_extract_assets = create_dfm_extraction_assets(
    configs=[EPJI_LOCATION_EXTRACT_CONFIG],
    source_resource_key="epji_oracle",
    source_resource_type=EPJIOracleResource,
    source_system="EPJI",
    group_name="epji_archive",
    domain_tag="epji",
    dialect="oracle",
    deps=["staging_infra_source_epji"],
)

# Every value arrives stringified (stringify_oracle_value), so NUMBER and
# TIMESTAMP columns land as NVARCHAR, as in the IFDB archive.
PRACTITIONER_COLUMN_TYPES: dict[str, str] = {
    "PK": "NVARCHAR(50) NOT NULL",
    "VERSION": "NVARCHAR(50) NULL",
    "LAST_UPDATE_DATE": "NVARCHAR(50) NULL",
    "EXTERNAL_ID": "NVARCHAR(300) NULL",
    "STATE": "NVARCHAR(50) NULL",
    "RELATED_TO_GUID": "NVARCHAR(40) NULL",
    "NAME": "NVARCHAR(300) NULL",
    "GIVEN_NAME": "NVARCHAR(100) NULL",
    "FAMILY_NAME": "NVARCHAR(200) NULL",
}

PRACTITIONER_IDENTIFIER_COLUMN_TYPES: dict[str, str] = {
    "PK": "NVARCHAR(50) NOT NULL",
    "PRACTITIONER_PK": "NVARCHAR(50) NULL",
    "SYSTEM": "NVARCHAR(500) NULL",
    "VALUE": "NVARCHAR(500) NULL",
}

PRACTITIONER_ROLE_COLUMN_TYPES: dict[str, str] = {
    "PK": "NVARCHAR(50) NOT NULL",
    "VERSION": "NVARCHAR(50) NULL",
    "LAST_UPDATE_DATE": "NVARCHAR(50) NULL",
    "CODE_PK": "NVARCHAR(50) NULL",
    "CODE": "NVARCHAR(64) NULL",
    "SYSTEM": "NVARCHAR(500) NULL",
    "DISPLAY": "NVARCHAR(64) NULL",
}

EPJI_PRACTITIONER_EXTRACT_CONFIG = DfmExtractConfig(
    source_schema="EPJI",
    source_table="PRACTITIONER",
    target_table="epji_practitioner",
    is_full_load=True,
    checkpoint_from=("arc", "epji_practitioner"),
    columns=tuple(PRACTITIONER_COLUMN_TYPES),
    column_types=PRACTITIONER_COLUMN_TYPES,
    index_keys=("PK",),
    description="EPJI PRACTITIONER full snapshot (one row per EPJ account).",
)

EPJI_PRACTITIONER_IDENTIFIER_EXTRACT_CONFIG = DfmExtractConfig(
    source_schema="EPJI",
    source_table="PRACTITIONER_IDENTIFIER",
    target_table="epji_practitioner_identifier",
    is_full_load=True,
    checkpoint_from=("arc", "epji_practitioner_identifier"),
    columns=tuple(PRACTITIONER_IDENTIFIER_COLUMN_TYPES),
    column_types=PRACTITIONER_IDENTIFIER_COLUMN_TYPES,
    index_keys=("PRACTITIONER_PK", "SYSTEM"),
    description="EPJI PRACTITIONER_IDENTIFIER full snapshot. The source has no time column.",
)

EPJI_PRACTITIONER_ROLE_EXTRACT_CONFIG = DfmExtractConfig(
    source_schema="EPJI",
    source_table="PRACTITIONER_ROLE",
    target_table="epji_practitioner_role",
    is_full_load=True,
    checkpoint_from=("arc", "epji_practitioner_role"),
    columns=tuple(PRACTITIONER_ROLE_COLUMN_TYPES),
    column_types=PRACTITIONER_ROLE_COLUMN_TYPES,
    index_keys=("PK",),
    description="EPJI PRACTITIONER_ROLE full snapshot (Columna role catalogue).",
)

epji_practitioner_extract_assets = create_dfm_extraction_assets(
    configs=[
        EPJI_PRACTITIONER_EXTRACT_CONFIG,
        EPJI_PRACTITIONER_IDENTIFIER_EXTRACT_CONFIG,
        EPJI_PRACTITIONER_ROLE_EXTRACT_CONFIG,
    ],
    source_resource_key="epji_oracle",
    source_resource_type=EPJIOracleResource,
    source_system="EPJI",
    group_name="epji_practitioner_archive",
    domain_tag="epji",
    dialect="oracle",
    deps=["staging_infra_source_epji"],
)

__all__ = [
    "EXT_COLUMNS",
    "EPJI_LOCATION_EXTRACT_CONFIG",
    "EPJI_PRACTITIONER_EXTRACT_CONFIG",
    "EPJI_PRACTITIONER_IDENTIFIER_EXTRACT_CONFIG",
    "EPJI_PRACTITIONER_ROLE_EXTRACT_CONFIG",
    "epji_location_extract_assets",
    "epji_practitioner_extract_assets",
]

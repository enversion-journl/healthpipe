# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""EPJI LOCATION extract.

Full snapshot of EPJI ``LOCATION`` LEFT JOIN ``LOCATION_IDENTIFIER`` (SYSTEM
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
    deps=["staging_infra_epji_archive"],
)

__all__ = ["EXT_COLUMNS", "EPJI_LOCATION_EXTRACT_CONFIG", "epji_location_extract_assets"]

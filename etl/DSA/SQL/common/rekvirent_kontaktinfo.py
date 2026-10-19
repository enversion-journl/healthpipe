# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared column contract for the requisitioner's address and contact details.

Every column takes the same path into dim.Rekvirent: ``stg_dim_Rekvirent`` joins
it in from ``arc.ils_rekv`` (``bynavn`` is looked up in ``arc.ils_postdi``)
after the bulk insert, deduplicated to one physical source row per requisitioner.

One path, one deduplication rule — carrying the non-PII details as CodeSystem
properties instead was tried and rejected, for two reasons that still hold:

* ``cs_rekvirent`` merges without ``delete_stale_properties``, so a value the
  source clears would survive as a stale property row forever. The joined
  columns are recomputed with ``NULLIF(...)`` on every run and go NULL instead.
* The CodeSystem merge and the join deduplicated ``arc.ils_rekv`` differently,
  so one dim row could mix values from two different physical source rows.

The PII columns must stay out of the CodeSystem regardless of the above:
``codesystem.RekvirentProperties`` syncs to DSO **with no masking view**, and
``adr``/``adr2``/``kontakt`` hold home-patient PII (flagged by Strategisk
Indkoeb). Reaching them through the dimension puts them behind
``vw_dim.Rekvirent``'s ``rtyp IN ('HP','OHP')`` mask.

Naming: columns are named for what they are, in Danish, not with the ILS
abbreviations. ``source_column`` below is only the technical translation the SQL
needs; **where a column comes from is documented in
``data_lineage/dim/dim_rekvirent.yml``**, not in its name.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class RekvirentAttribute:
    """A column in stg./dim.Rekvirent and its source."""

    name: str
    # Width matches the source column in arc.ils_rekv (arc.ils_postdi for
    # bynavn). This is the single width contract: the staging DDL is generated
    # from it, and the dim DDL and migration are held to it by
    # test_dim_rekvirent_kontaktinfo.
    sql_type: str
    # Column name in arc.ils_rekv. None => derived (bynavn is looked up in arc.ils_postdi).
    source_column: str | None


# The order is the contract between staging and dim.
REKVIRENT_ATTRIBUTES: tuple[RekvirentAttribute, ...] = (
    RekvirentAttribute("alternativt_rekvirentnummer", "NVARCHAR(16)", "altrnr"),
    RekvirentAttribute("adresse", "NVARCHAR(80)", "adr"),
    RekvirentAttribute("adresse2", "NVARCHAR(80)", "adr2"),
    RekvirentAttribute("land", "NVARCHAR(6)", "land"),
    RekvirentAttribute("postnummer", "NVARCHAR(10)", "postdi"),
    RekvirentAttribute("bynavn", "NVARCHAR(58)", None),
    RekvirentAttribute("kontaktperson", "NVARCHAR(80)", "kontakt"),
    RekvirentAttribute("kontosted", "NVARCHAR(34)", "ktosted"),
)

# Every column, in dim order.
KONTAKTINFO_COLUMNS: tuple[str, ...] = tuple(a.name for a in REKVIRENT_ATTRIBUTES)

# Column -> SQL type; generates the stg.dim_Rekvirent DDL for these columns.
KONTAKTINFO_COLUMN_TYPES: dict[str, str] = {a.name: a.sql_type for a in REKVIRENT_ATTRIBUTES}

# (dim column, source column in arc.ils_rekv) for the ones taken 1:1.
REKV_SOURCE_COLUMNS: tuple[tuple[str, str], ...] = tuple(
    (a.name, a.source_column) for a in REKVIRENT_ATTRIBUTES if a.source_column is not None
)

# The subset carrying home-patient PII for rtyp = 'HP'/'OHP', masked in
# vw_dim.Rekvirent. adresse/adresse2 were flagged by Strategisk Indkoeb;
# kontaktperson is masked on the same rule, because an HP/OHP contact is the
# patient or a relative. None of them may ever live in the CodeSystem.
KONTAKTINFO_PII_COLUMNS: tuple[str, ...] = ("adresse", "adresse2", "kontaktperson")

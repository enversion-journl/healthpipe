# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dataset specification for SDS' datamart "Monitorering af udredningsretten"
(LPR_Udredningsret, [internal ticket], [internal ticket]).

ONE source of truth for the extract (DSI/csv_files/udredningsret.py), the
archive (DSI/csv_files/udredningsret_archive.py) and the arc DDL
(infrastructure/ddl/tables/udredningsret/00_archive.sql — kept in lockstep by
tests/unit/test_udredningsret_datasets_ddl.py).

The columns are SDS' own Metadata_UdredningsretMont_1083_<dato>.csv schema
(28 columns, unchanged across all 14 deliveries on the share 2026-06-08 ->
2026-09-07, and identical to the 2022 archive apart from
SYGDOMSFORLOB_ID -> DW_EK_HelbredsForloeb).

ARC DESIGN (profiling report, decisions B3 and B6 on feature 27631): the
source's own ids identify nothing across deliveries — F_ID is renumbered for
whole helbredsforloeb at a time, PK (= F_ID_loebenr) follows, and
OBJEKTID_FORLOBSMARKOR is reissued for 3.8 % of the markers per month. They are
ALL kept in arc as "as last delivered" attributes, but the merge runs on
business keys computed in ext (udredningsret_archive.py):

  helbredsforloeb_noegle  SHA2_256 over CPR + the helbredsforloeb's FIRST
                          marker's code, date, responsible unit and
                          forloebselement UUID (100 % unique, independent of
                          SDS' surrogate DW_EK_HelbredsForloeb)
  markoer_noegle          SHA2_256 over the helbredsforloeb key + code + date
  loebenummer             ROW_NUMBER over (markoer_noegle), ordered by a hash
                          of the marker's STABLE content (0.15 % duplicates)
  antal_kilderaekker      fully identical rows (same content, only a different
                          PK/OBJEKTID) collapsed into one row with a count

CPR enters helbredsforloeb_noegle HASHED only — never in clear text in any key
(GDPR invariant on the feature).

NAMING (as DRG): ext/arc tables are <source>_<source table> in lowercase
(udredningsret_mont, udredningsret_leveranceinfo; cf. drg_kontakt,
drg_leveranceinfo), the arc PK is PK_arc_<table>, indexes IX_arc_<table>_<col>,
and derived arc columns are *_noegle (never clean_code_*, which is the DSA
layer's name and is formed in pre_stg).

WIDTHS: every source column is NVARCHAR(VALUE_WIDTH) in both ext and arc. SDS'
own max_length values are all <= 255 characters
(Speciale_ansvarlig_enhed_kontakt), so one shared width is safe, and ext and
arc can never diverge (HAIBA lesson [internal ticket]).
"""

from dataclasses import dataclass

DATASET_PREFIX = "UdredningsretMont"  # UdredningsretMont_1083_<dato>.csv
LEVERANCEINFO_PREFIX = "UdredningsretMont_LEVERANCEINFO"
REGION = "1083"

VALUE_WIDTH = 500  # HAIBA lesson [internal ticket]: ext and arc MUST share the width
NOEGLE_WIDTH = 64  # SHA2_256 as hex


@dataclass(frozen=True)
class ArcColumn:
    """A column in the arc table that does not exist in the source: name, SQL type, nullability."""

    name: str
    sql_type: str
    nullable: bool = False


@dataclass(frozen=True)
class UdredningsretDataset:
    """One dataset of the datamart and its path to arc."""

    source_prefix: str
    ext_table: str  # ext.<name>, arc.<name> and ext.<name>_noegler
    source_columns: tuple[str, ...]  # SDS' metadata columns in source order
    arc_added: tuple[ArcColumn, ...] = ()  # computed keys ahead of the source columns
    merge_keys: tuple[str, ...] = ()
    # Columns whose values MUST be ISO dates (YYYY-MM-DD). The key computation
    # and the delivery-direction guard order them as strings, which is only
    # chronological for ISO — so a deviating value fails the load (review F14).
    iso_date_columns: tuple[str, ...] = ()

    @property
    def arc_columns(self) -> tuple[str, ...]:
        """Arc column order: computed keys first, then every source column."""
        return tuple(a.name for a in self.arc_added) + self.source_columns

    @property
    def added_names(self) -> tuple[str, ...]:
        return tuple(a.name for a in self.arc_added)


# The columns that make up a marker's CONTENT. Two rows with the same content
# but a different PK/OBJEKTID_FORLOBSMARKOR are the source's double registration
# of the same marker (1,705 rows on 2026-09-07) and collapse into one arc row.
CONTENT_EXCLUDED = ("PK", "OBJEKTID_FORLOBSMARKOR")

# SDS' own sequence numbers with no meaning across deliveries: F_ID is renumbered
# for whole helbredsforloeb at a time (31 %/month) and PK follows. Measured on dev
# 2026-09-08: 98.8 % of 1.5 M weekly arc updates were ONLY these two columns.
# They stay in arc but do not count as a change — they are refreshed together
# with a real content change (and leverance_dato). OBJEKTID_FORLOBSMARKOR, by
# contrast, is the marker's LPR3 UUID that encounters are enriched with (B9),
# so it is kept current even when SDS reissues it.
SURROGATE_ID_COLUMNS = ("PK", "F_ID")

# Ranking of loebenummer within one markoer_noegle (review F13). The rank must
# survive a delivery in which SDS reissues OBJEKTID or renumbers F_ID/PK/DW_EK
# for the SAME marker — otherwise two duplicate markers swap places between
# (markoer_noegle, 1) and (markoer_noegle, 2) and a downstream dw_ek lands on
# the wrong marker without any guard noticing. The rank is therefore a hash of
# the content WITHOUT every id that the source renumbers.
LOEBENUMMER_HASH_EXCLUDED = ("PK", "OBJEKTID_FORLOBSMARKOR", "F_ID", "DW_EK_HelbredsForloeb")

# Tie-breakers, used (a) inside a dedup partition, where the rows are fully
# identical so the choice is irrelevant, and (b) after the stable content hash
# for the residual case of two surviving rows that differ ONLY in renumbered
# ids. That residual order is not stable across deliveries by nature — it is
# documented in udredningsret_kilde.md rather than guarded.
DEDUP_ORDER_COLUMNS = ("OBJEKTID_FORLOBSMARKOR", "PK")

# Appendix 1 of SDS' datamart description: markers for "referred to assessment".
# Used by staging (US-3) for the start date — documented here only as part of
# the dataset's domain.
HENVIST_MARKOERER = ("AFV01A1", "AFV01A1A", "AFA01A", "AFD01A")

UDREDNINGSRET_MONT = UdredningsretDataset(
    source_prefix=DATASET_PREFIX,
    ext_table="udredningsret_mont",
    source_columns=(
        "DW_EK_HelbredsForloeb",
        "OBJEKTID_FORLOBELEMENT",
        "OBJEKTID_FORLOBSMARKOR",
        "PK",
        "F_ID",
        "CPR",
        "C_SGHREGION",
        "ANSVARLIG_ENHED_FORLOB",
        "FORLOEBSLABEL",
        "C_ADIAG",
        "C_ADIAG_KONTAKT",
        "ANSVARLIG_ENHED_KONTAKT",
        "FORLOEBSMARKOER",
        "FORLOEBSMARKOER_DATO",
        "MRK_C",
        "H",
        "MRK_ST_H",
        "MRK_H",
        "MRK_ST_L",
        "MRK_ST_N",
        "MRK_L_N",
        "C_BOREGION",
        "C_BOREGION_F",
        "PSYKIATRI",
        "ALDER",
        "OPGPER",
        "OPGMAANED",
        "Speciale_ansvarlig_enhed_kontakt",
    ),
    arc_added=(
        ArcColumn("helbredsforloeb_noegle", f"CHAR({NOEGLE_WIDTH})"),
        ArcColumn("markoer_noegle", f"CHAR({NOEGLE_WIDTH})"),
        ArcColumn("loebenummer", "INT"),
        ArcColumn("antal_kilderaekker", "INT"),
    ),
    merge_keys=("markoer_noegle", "loebenummer"),
    # "The helbredsforloeb's EARLIEST marker" is only true if the date column
    # orders chronologically as a string.
    iso_date_columns=("FORLOEBSMARKOER_DATO",),
)

# Columns used for the helbredsforloeb key: the FIRST marker in the
# helbredsforloeb within the delivery (earliest date, lowest code, lowest unit,
# lowest forloebselement) + CPR. Measured 2026-09-08: 100 % unique across
# 2,233,064 helbredsforloeb; the first marker changed for 218 (0.01 %) in 27 days.
HELBREDSFORLOEB_GROUP_COLUMN = "DW_EK_HelbredsForloeb"
HELBREDSFORLOEB_ORDER_COLUMNS = (
    "FORLOEBSMARKOER_DATO",
    "FORLOEBSMARKOER",
    "ANSVARLIG_ENHED_FORLOB",
    "OBJEKTID_FORLOBELEMENT",
)
MARKOER_KEY_COLUMNS = ("FORLOEBSMARKOER", "FORLOEBSMARKOER_DATO")

LEVERANCEINFO = UdredningsretDataset(
    source_prefix=LEVERANCEINFO_PREFIX,
    ext_table="udredningsret_leveranceinfo",
    source_columns=("DATAOMRAADE", "LPROPDATERINGSDATO"),
    # The delivery-direction guard compares this value as a string against
    # meta.UdredningsretLeverance.lpropdateringsdato NVARCHAR(10).
    iso_date_columns=("LPROPDATERINGSDATO",),
)

DATASETS: tuple[UdredningsretDataset, ...] = (UDREDNINGSRET_MONT, LEVERANCEINFO)
ARCHIVED_DATASETS: tuple[UdredningsretDataset, ...] = (UDREDNINGSRET_MONT,)

__all__ = [
    "ARCHIVED_DATASETS",
    "ArcColumn",
    "CONTENT_EXCLUDED",
    "DATASETS",
    "DATASET_PREFIX",
    "DEDUP_ORDER_COLUMNS",
    "HELBREDSFORLOEB_GROUP_COLUMN",
    "HELBREDSFORLOEB_ORDER_COLUMNS",
    "HENVIST_MARKOERER",
    "LEVERANCEINFO",
    "LEVERANCEINFO_PREFIX",
    "LOEBENUMMER_HASH_EXCLUDED",
    "MARKOER_KEY_COLUMNS",
    "NOEGLE_WIDTH",
    "REGION",
    "SURROGATE_ID_COLUMNS",
    "UDREDNINGSRET_MONT",
    "VALUE_WIDTH",
    "UdredningsretDataset",
]

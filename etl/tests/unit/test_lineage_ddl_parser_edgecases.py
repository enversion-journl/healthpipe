# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

import textwrap

import pytest
from data_lineage.tools.lineage_ddl_check.ddl_parser import parse_ddl

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Adversarial edge cases for the base-DDL CREATE TABLE parser (ddl_parser.py).
#
# The parser is paren/comma/bracket based. Its body scanner and top-level comma
# splitter are now STRING-LITERAL AWARE: characters inside a single-quoted T-SQL
# string literal (including escaped '' quotes) do not affect paren depth and a
# comma there does not split a column. Each test feeds one construct and asserts
# the CORRECT expected column set.
#
# PASS cases lock in correct behaviour. The one remaining xfail(strict) case
# documents a genuine LIMITATION on an exotic construct that is absent from the
# real base DDL (a column name containing a literal ']'); a strict xfail FAILS
# if the parser is ever taught to handle it, prompting the marker's removal.
#
# Constructs that appear in the real base DDL (infrastructure/ddl/tables/**,
# migrations excluded) are noted as such.
# ---------------------------------------------------------------------------


def _write(tmp_path, rel, text):
    p = tmp_path / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(textwrap.dedent(text), encoding="utf-8")
    return p


# === PASS: constructs the parser handles correctly =========================


def test_computed_column_persisted(tmp_path):
    # REAL DDL: fhir/01_tables.sql has
    #   [DW_Timestamp_Effective] AS ISNULL([dw_timestamp_update],[dw_timestamp_insert]) PERSISTED
    # The computed expression's inner parens/commas are paren-protected and the
    # column name is bracket-prefixed, so it is captured like any other column.
    _write(
        tmp_path,
        "fhir/01.sql",
        """
        CREATE TABLE [fhir].[Foo] (
            [a] INT NULL,
            [b] INT NULL,
            [c] AS ISNULL([a], [b]) PERSISTED,
            [d] INT NULL
        ) ON [PRIMARY];
        GO
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["fhir.Foo"].columns == ("a", "b", "c", "d")


def test_table_level_check_with_comma_in_string_in_list(tmp_path):
    # REAL-DDL-SHAPED: meta routing tables use CHECK ([col] IN ('a','b')).
    # The commas (incl. one inside a string literal) sit inside the CHECK's
    # parens, so paren-depth keeps them out of the top-level split, and the
    # whole CHECK item is dropped by the CONSTRAINT/CHECK keyword filter.
    _write(
        tmp_path,
        "dim/01.sql",
        """
        CREATE TABLE [dim].[Foo] (
            [id] INT NOT NULL,
            [k] NVARCHAR(20) NOT NULL,
            CONSTRAINT [CK_Foo] CHECK ([k] IN ('a,x', 'b'))
        ) ON [PRIMARY];
        GO
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["dim.Foo"].columns == ("id", "k")


def test_column_level_inline_check_with_comma_in_string(tmp_path):
    # Column-level inline CHECK: the comma-bearing string is paren-protected,
    # the column name is bracket-prefixed -> captured correctly.
    _write(
        tmp_path,
        "dim/01.sql",
        """
        CREATE TABLE [dim].[Foo] (
            [id] INT NOT NULL,
            [k] NVARCHAR(20) NOT NULL CHECK ([k] IN ('a,x', 'b')),
            [other] INT NULL
        ) ON [PRIMARY];
        GO
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["dim.Foo"].columns == ("id", "k", "other")


def test_string_default_with_balanced_parens(tmp_path):
    # DEFAULT '(N/A)': the parens inside the string happen to be balanced. With
    # string-literal awareness they are ignored entirely; the column set is right.
    _write(
        tmp_path,
        "dim/01.sql",
        """
        CREATE TABLE [dim].[Foo] (
            [id] INT NOT NULL,
            [label] NVARCHAR(50) NOT NULL DEFAULT '(N/A)',
            [other] INT NULL
        ) ON [PRIMARY];
        GO
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["dim.Foo"].columns == ("id", "label", "other")


def test_real_shaped_word_and_empty_string_defaults(tmp_path):
    # REAL DDL: DEFAULT N'' and DEFAULT N'curated_set' (fhir/codesystem). No
    # special chars in the literal -> nothing to mis-split.
    _write(
        tmp_path,
        "fhir/01.sql",
        """
        CREATE TABLE [fhir].[Foo] (
            [a] NVARCHAR(50) NOT NULL DEFAULT N'',
            [b] NVARCHAR(50) NOT NULL DEFAULT N'curated_set',
            [c] INT NULL
        ) ON [PRIMARY];
        GO
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["fhir.Foo"].columns == ("a", "b", "c")


def test_named_default_constraint_date_literal(tmp_path):
    # REAL DDL: erp/02_dims.sql DEFAULT '9999-12-31'; ref/01 named DF_ constraint.
    _write(
        tmp_path,
        "dim/01.sql",
        """
        CREATE TABLE [dim].[Foo] (
            [id] INT NOT NULL,
            [dw_valid_to] DATETIME2(0) NOT NULL CONSTRAINT [DF_Foo] DEFAULT '9999-12-31',
            [b] INT NULL
        ) ON [PRIMARY];
        GO
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["dim.Foo"].columns == ("id", "dw_valid_to", "b")


def test_identity_and_bracketed_type(tmp_path):
    # REAL DDL: erp/01_maps.sql uses [dw_ek_Lager] [int] IDENTITY(1,1) NOT NULL.
    # The (1,1) is paren-protected; the [int] type token is irrelevant because
    # only the leading bracketed identifier is read.
    _write(
        tmp_path,
        "fct/01.sql",
        """
        CREATE TABLE [fct].[Foo] (
            [DW_EK_Foo] [int] IDENTITY(1, 1) NOT NULL,
            [v] DECIMAL(30, 12) NULL
        ) ON [PRIMARY];
        GO
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["fct.Foo"].columns == ("DW_EK_Foo", "v")


def test_block_comment_inside_body_with_comma_and_paren(tmp_path):
    # /* ... */ is stripped before splitting, so a comma/paren inside it is inert.
    _write(
        tmp_path,
        "dim/01.sql",
        """
        CREATE TABLE [dim].[Foo] (
            [id] INT NOT NULL, /* this, has a comma and (a paren) */
            [a] INT NULL
        ) ON [PRIMARY];
        GO
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["dim.Foo"].columns == ("id", "a")


def test_crlf_line_endings(tmp_path):
    # Repo DDL is LF, but CRLF must not change the column set.
    p = tmp_path / "dim" / "01.sql"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(
        b"CREATE TABLE [dim].[Foo] (\r\n  [id] INT NOT NULL,\r\n  [a] INT NULL\r\n) ON [PRIMARY];\r\nGO\r\n"
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["dim.Foo"].columns == ("id", "a")


def test_multiple_tables_no_trailing_semicolon_then_go(tmp_path):
    # REAL DDL: erp/01_maps.sql ends bodies with ") ON [PRIMARY]" (no ';') and
    # relies on GO; many CREATE TABLEs per file is the norm.
    _write(
        tmp_path,
        "dim/01.sql",
        """
        CREATE TABLE [dim].[Foo] (
            [id] INT NOT NULL,
            [a] INT NULL
        ) ON [PRIMARY]
        GO
        CREATE TABLE [dim].[Bar] (
            [id] INT NOT NULL,
            [b] INT NULL
        ) ON [PRIMARY]
        GO
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert set(tables) == {"dim.Foo", "dim.Bar"}
    assert tables["dim.Foo"].columns == ("id", "a")
    assert tables["dim.Bar"].columns == ("id", "b")


def test_lowercase_create_table_keyword(tmp_path):
    # _CREATE_RE is case-insensitive; a lowercased keyword still parses.
    _write(
        tmp_path,
        "dim/01.sql",
        """
        create table [dim].[Foo] (
            [id] int not null,
            [a] int null
        );
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["dim.Foo"].columns == ("id", "a")


def test_mixed_case_schema_name_is_matched_but_case_preserved(tmp_path):
    # Schema matching is case-insensitive (schema.lower() in wanted), but the
    # emitted key PRESERVES the source casing -> "Dim.Foo", not "dim.Foo".
    _write(
        tmp_path,
        "dim/01.sql",
        """
        CREATE TABLE [Dim].[Foo] (
            [id] INT NOT NULL,
            [a] INT NULL
        );
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert "Dim.Foo" in tables
    assert tables["Dim.Foo"].columns == ("id", "a")


def test_multiline_bare_anonymous_primary_key_clustered(tmp_path):
    # REAL DDL: fhir/01_tables.sql fhir.Encounter_Location declares a table-level
    # constraint as a BARE/ANONYMOUS `PRIMARY KEY CLUSTERED (` with the column
    # list spanning multiple lines. The leading token PRIMARY is a constraint
    # keyword, so the whole item is dropped (not read as a column). sqlglot's
    # tsql dialect cannot parse this shape at all; the project parser is correct.
    _write(
        tmp_path,
        "fhir/01.sql",
        """
        CREATE TABLE [fhir].[Encounter_Location] (
            [encounter_id] NVARCHAR(255) NOT NULL,
            [location_id] NVARCHAR(255) NOT NULL,
            [location_period_start] DATETIME2 NULL,
            PRIMARY KEY CLUSTERED (
                [encounter_id],
                [location_id],
                [location_period_start]
            )
        ) ON [PRIMARY];
        GO
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["fhir.Encounter_Location"].columns == (
        "encounter_id",
        "location_id",
        "location_period_start",
    )


# === DIVERGENCES NOW FIXED: string-literal awareness ======================
#
# These three constructs are valid T-SQL that the old, string-unaware parser
# mishandled (one SILENT column drop, two crashes). They do NOT occur in the
# current base DDL, but a future DEFAULT edit could trip them. The parser is now
# string-literal aware, so all three produce the correct column set.


def test_string_default_with_unbalanced_close_paren_keeps_columns(tmp_path):
    # Was: PARSER BUG (silent column drop). An unbalanced ')' inside a string
    # literal used to drive paren-depth negative, ending the CREATE TABLE body
    # early and silently dropping every later column. Now the string is skipped.
    _write(
        tmp_path,
        "dim/01.sql",
        """
        CREATE TABLE [dim].[Foo] (
            [id] INT NOT NULL,
            [label] NVARCHAR(50) NOT NULL DEFAULT ')close',
            [maybe_dropped] INT NULL
        ) ON [PRIMARY];
        GO
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["dim.Foo"].columns == ("id", "label", "maybe_dropped")


def test_string_default_with_comma_parses_correctly(tmp_path):
    # Was: PARSER BUG (crash). A comma inside a string literal DEFAULT ('a,b')
    # used to be treated as a top-level column separator -> DdlParseError. Now
    # the comma is protected inside the string literal.
    _write(
        tmp_path,
        "dim/01.sql",
        """
        CREATE TABLE [dim].[Foo] (
            [id] INT NOT NULL,
            [label] NVARCHAR(50) NOT NULL DEFAULT 'a,b',
            [other] INT NULL
        ) ON [PRIMARY];
        GO
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["dim.Foo"].columns == ("id", "label", "other")


def test_string_default_with_unbalanced_open_paren_parses_correctly(tmp_path):
    # Was: PARSER BUG (crash). An unbalanced '(' inside a string literal DEFAULT
    # ('(open') used to leave paren-depth > 0 so the body never closed ->
    # 'unterminated CREATE TABLE'. Now the paren is protected inside the string.
    _write(
        tmp_path,
        "dim/01.sql",
        """
        CREATE TABLE [dim].[Foo] (
            [id] INT NOT NULL,
            [label] NVARCHAR(50) NOT NULL DEFAULT '(open',
            [other] INT NULL
        ) ON [PRIMARY];
        GO
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["dim.Foo"].columns == ("id", "label", "other")


def test_string_default_with_escaped_quote_then_special_char(tmp_path):
    # T-SQL escapes a single quote by doubling it. A '' inside the literal must
    # not be read as "string ends, string begins" in a way that exposes the
    # trailing special char. Here the literal is  it's,a)mess  -> harmless.
    _write(
        tmp_path,
        "dim/01.sql",
        """
        CREATE TABLE [dim].[Foo] (
            [id] INT NOT NULL,
            [label] NVARCHAR(50) NOT NULL DEFAULT 'it''s,a)mess',
            [other] INT NULL
        ) ON [PRIMARY];
        GO
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["dim.Foo"].columns == ("id", "label", "other")


# === LIMITATION: exotic, absent from this repo (acceptable) ================


@pytest.mark.xfail(
    strict=True,
    reason=(
        "LIMITATION (exotic, not in repo): a column whose name contains a literal ']' "
        "is escaped by doubling -> []]]] means a column named ']'. The reader regex "
        r"\[(?P<name>[^\]]+)\] stops at the first ']', so the item is read as "
        "malformed -> DdlParseError. No such identifier exists anywhere in the base "
        "DDL (all column names are plain). Acceptable; documented for completeness."
    ),
)
def test_bracket_escaped_identifier_named_close_bracket(tmp_path):
    p = tmp_path / "dim" / "01.sql"
    p.parent.mkdir(parents=True, exist_ok=True)
    # []]]] is the T-SQL escaping of a column literally named  ]
    p.write_text(
        "CREATE TABLE [dim].[Foo] (\n"
        "    [id] INT NOT NULL,\n"
        "    []]]] INT NULL,\n"
        "    [other] INT NULL\n"
        ") ON [PRIMARY];\nGO\n",
        encoding="utf-8",
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["dim.Foo"].columns == ("id", "]", "other")

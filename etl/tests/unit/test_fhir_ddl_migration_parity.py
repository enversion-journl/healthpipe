# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Base-DDL <-> migration parity for the FHIR flat tables.

THE BUG THIS CATCHES: a schema change lands as a migration but is never
mirrored into ``tables/fhir/*.sql`` (or vice versa). Existing databases get the
column from the migration; fresh ones are stamped by ``baseline_migrations()``
and therefore NEVER execute it, so they only ever get what the base DDL says.
The column then exists in one half of the environments and the SOF merge fails
on the other half.

Until the fhir migrations were folded into the central runner they re-ran on
every ``staging_infra_fhir`` materialization, which silently self-healed this
class of drift. Tracked, run-once migrations remove that safety net, so the
invariant needs a guard.

The parity model follows the migrations in filename order and applies
ADD / DROP COLUMN / DROP TABLE / sp_rename (table and column), because later
migrations legitimately supersede earlier ones — ``reason_code`` is added by
20260701_fhir_servicerequest_add_reason_code.sql and dropped again by
20260804_fhir_servicerequest_reason_reference.sql, and ``is_deleted`` is renamed
to ``dw_is_deleted`` by 20260708_rename_05_fhir_audit.sql. A naive "every ADD
must appear in the base DDL" check reports both as failures.

DROP TABLE is modelled for the same reason: it discards every column an earlier
migration added, because the base DDL's CREATE TABLE is what the environment ends
up with (20260812_fhir_patient_address_typed_period_columns.sql recreates
fhir.Patient_Address this way). Note the consequence — for a table dropped by a
migration this test's invariant becomes vacuous, since both fresh and existing
databases then get their shape from the base DDL alone.

KNOWN LIMITATION — the reverse direction (base DDL has a column that no
migration adds) cannot be asserted statically: it needs the original CREATE to
tell a legitimately-new table from a drifted one. Two historical breaches are
known and predate migration tracking: ``fhir.Patient`` (19 columns added
2026-05-28, covered at the time by the DROP TABLE in
20260515_dim_borger_int_keys_ref_tables.sql) and ``fhir.Observation``
(~26 element_*/value_* columns added 2026-06-01, never carried by a migration).
"""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]  # etl/
MIGRATIONS = REPO / "infrastructure" / "ddl" / "migrations"
FHIR_TABLES = REPO / "infrastructure" / "ddl" / "tables" / "fhir"


def _create_table_blocks(sql: str) -> dict[str, str]:
    """Map table name -> column-definition body for every CREATE TABLE [fhir].[X]."""
    blocks: dict[str, str] = {}
    for match in re.finditer(r"CREATE TABLE \[fhir\]\.\[(\w+)\]\s*\(", sql):
        index, depth = match.end() - 1, 0
        while index < len(sql):
            if sql[index] == "(":
                depth += 1
            elif sql[index] == ")":
                depth -= 1
                if depth == 0:
                    break
            index += 1
        blocks[match.group(1)] = sql[match.end() : index]
    return blocks


def _columns(block: str) -> set[str]:
    return {name.lower() for name in re.findall(r"^\s*\[(\w+)\]", block, re.M)}


def _fhir_migrations() -> list[Path]:
    return sorted(path for path in MIGRATIONS.glob("*.sql") if "fhir" in path.name)


def _net_schema() -> dict[str, set[str]]:
    """Columns each fhir table ends up with after replaying every migration in order."""
    aliases: dict[str, str] = {}
    net: dict[str, set[str]] = {}

    def resolve(table: str) -> str:
        seen: set[str] = set()
        while table in aliases and table not in seen:
            seen.add(table)
            table = aliases[table]
        return table

    for path in _fhir_migrations():
        # Commented-out statements are documentation, not schema changes.
        sql = "\n".join(
            line
            for line in path.read_text(encoding="utf-8").splitlines()
            if not line.lstrip().startswith("--")
        )

        # sp_rename skrives i praksis baade som 'fhir.X' og som N'[fhir].[X]'.
        # Forstaar parseren kun den ene, bliver en omdoebning usynlig for
        # invarianten, og den gamle tabel staar tilbage som et falsk fund.
        for old, new in re.findall(
            r"sp_rename\s+N?'\[?fhir\]?\.\[?(\w+)\]?'\s*,\s*N?'\[?(\w+)\]?'\s*(?:;|\))",
            sql,
        ):
            aliases[old] = new
            # Omdoebningen skal baere de kolonner, tidligere migrationer har
            # lagt paa den gamle tabel, med over paa det nye navn. Ellers staar
            # den gamle noegle tilbage i net og rapporteres som en tabel, base-
            # DDL'en ikke producerer -- et falsk fund, hver gang en tabel
            # omdoebes EFTER at en migration har udvidet den.
            if old in net:
                net.setdefault(new, set()).update(net.pop(old))

        # Dropping the table discards every column an earlier migration added to
        # it -- the base DDL's CREATE TABLE is what the environment ends up with.
        # Processed before the ADDs so a migration that drops and then extends in
        # the same file still requires its new columns to be mirrored.
        for table in re.findall(r"DROP TABLE\s+(?:IF EXISTS\s+)?\[fhir\]\.\[(\w+)\]", sql, re.I):
            net.pop(resolve(table), None)

        # `ADD [col] TYPE` / `ADD [col] AS <computed>` — not ADD CONSTRAINT/PRIMARY KEY.
        for table, column in re.findall(
            r"ALTER TABLE \[fhir\]\.\[(\w+)\]\s*ADD\s+\[(\w+)\]\s+(?:\w|AS)", sql, re.I
        ):
            net.setdefault(resolve(table), set()).add(column.lower())

        for table, column in re.findall(
            r"ALTER TABLE \[fhir\]\.\[(\w+)\]\s*DROP COLUMN\s+\[(\w+)\]", sql, re.I
        ):
            net.get(resolve(table), set()).discard(column.lower())

        for table, old, new in re.findall(
            r"sp_rename\s+N?'\[?fhir\]?\.\[?(\w+)\]?\.\[?(\w+)\]?'\s*,\s*"
            r"N?'\[?(\w+)\]?'\s*,\s*N?'COLUMN'",
            sql,
            re.I,
        ):
            columns = net.get(resolve(table))
            if columns and old.lower() in columns:
                columns.discard(old.lower())
                columns.add(new.lower())

    return net


@pytest.mark.unit
def test_migration_columns_are_mirrored_in_base_ddl():
    base = _create_table_blocks(
        "\n".join(path.read_text(encoding="utf-8") for path in sorted(FHIR_TABLES.glob("*.sql")))
    )
    net = _net_schema()
    assert net, "no fhir migrations parsed — check the naming convention or the glob"

    missing = [
        f"fhir.{table}.{column}"
        for table, columns in sorted(net.items())
        for column in sorted(columns - _columns(base.get(table, "")))
    ]
    assert missing == [], (
        "Columns added by a fhir migration but absent from the base DDL in "
        "infrastructure/ddl/tables/fhir/. Fresh databases are baselined and never run "
        "the migration, so they would never get these columns. Mirror the change into "
        f"the CREATE TABLE statement: {missing}"
    )


@pytest.mark.unit
def test_migrations_target_tables_that_exist_in_base_ddl():
    base = _create_table_blocks(
        "\n".join(path.read_text(encoding="utf-8") for path in sorted(FHIR_TABLES.glob("*.sql")))
    )
    unknown = sorted(table for table in _net_schema() if table not in base)
    assert unknown == [], (
        "fhir migrations alter tables that no CREATE TABLE in tables/fhir/ produces — "
        f"the migration or the base DDL is stale: {unknown}"
    )


def test_droppede_kolonner_genskabes_ikke_af_grund_ddl():
    """En migration, der fjerner en kolonne, maa ikke modarbejdes af grund-DDL'en.

    Grund-DDL'en har efterreparationsblokke, der tilfoejer en manglende kolonne til
    en eksisterende tabel. Bliver en saadan blok staaende for en kolonne, en senere
    migration fjerner, gendanner infrastrukturkoerslen kolonnen bagefter — og et
    vw_fhir-view, der joiner teksten paa igen, fejler paa et dublet kolonnenavn.
    """
    import re
    from pathlib import Path

    etl = Path(__file__).resolve().parents[2]
    ddl = (etl / "infrastructure/ddl/tables/fhir/01_tables.sql").read_text(encoding="utf-8")
    droppede: dict[str, str] = {}
    for sti in sorted((etl / "infrastructure/ddl/migrations").glob("*.sql")):
        for tabel, kolonne in re.findall(
            r"ALTER TABLE \[fhir\]\.\[(\w+)\] DROP COLUMN \[(\w+)\]",
            sti.read_text(encoding="utf-8"),
        ):
            droppede[kolonne] = tabel
    genskabte = [
        f"{tabel}.{kolonne}"
        for kolonne, tabel in droppede.items()
        if re.search(rf"name = N'{kolonne}'\s*\)?\s*ALTER TABLE \[fhir\]\.\[{tabel}\] ADD", ddl)
        or re.search(rf"name = N'{kolonne}'[\s\S]{{0,120}}ADD \[{kolonne}\]", ddl)
    ]
    assert not genskabte, "grund-DDL'en tilfoejer kolonner, en migration fjerner: " + ", ".join(
        sorted(genskabte)
    )


def test_hver_ddl_batch_lukker_sine_blokke():
    """En fjernet linje maa ikke efterlade en uafsluttet BEGIN-blok.

    SQL Server melder kun «Incorrect syntax near», og fejlen dukker foerst op,
    naar infrastrukturtrinet koerer mod en database. Optaellingen foelger T-SQL:
    BEGIN, BEGIN TRY/CATCH og CASE aabner en blok, END lukker en. BEGIN
    TRANSACTION aabner ingen.
    """
    import re
    from pathlib import Path

    etl = Path(__file__).resolve().parents[2]
    aabner = re.compile(r"\b(BEGIN|CASE|END)\b(?:\s+(TRY|CATCH|TRANSACTION|TRAN))?", re.I)
    for sti in sorted((etl / "infrastructure/ddl/tables").rglob("*.sql")):
        tekst = sti.read_text(encoding="utf-8")
        for nr, batch in enumerate(re.split(r"(?mi)^\s*GO\s*$", tekst), start=1):
            # Kommentarer, klammer-identifikatorer (en kolonne hedder [end]) og
            # strenglitteraler maa ikke taelle med som noegleord.
            uden_kommentar = re.sub(r"--[^\n]*", "", batch)
            uden_kommentar = re.sub(r"\[[^\]]*\]", "", uden_kommentar)
            uden_kommentar = re.sub(r"N?'(?:[^']|'')*'", "", uden_kommentar)
            dybde = 0
            for nøgleord, efterled in aabner.findall(uden_kommentar):
                nøgleord, efterled = nøgleord.upper(), (efterled or "").upper()
                if nøgleord == "END":
                    dybde -= 1
                elif nøgleord == "CASE" or efterled in ("", "TRY", "CATCH"):
                    dybde += 1
            assert dybde == 0, f"{sti.name} batch {nr}: blokdybde {dybde}, ikke 0"


def test_omdoebningen_flytter_ogsaa_dfm_tilstanden():
    """Checkpointet er noeglet paa tabelnavnet, saa det skal med over.

    SoF-projektionen og DSO-synken bruger begge tabellens navn som
    table_name i meta.DFMState. Uden omskrivningen ville den omdoebte tabel
    staa uden checkpoint -- foerste koersel efter udrulningen ville lave en
    fuld genindlaesning fra Aidbox, og den gamle raekke ville blive staaende.
    Praecedens: 20260708_rename_95_state_rows.
    """
    import pathlib

    sql = (
        pathlib.Path(__file__).resolve().parents[2]
        / "infrastructure"
        / "ddl"
        / "migrations"
        / "20260909_fhir_tillaegskode_to_supplementary_code.sql"
    ).read_text(encoding="utf-8")
    for tabel in ("DFMState", "DFMRunLog"):
        markoer = "UPDATE s" if tabel == "DFMState" else f"UPDATE [meta].[{tabel}]"
        blok = sql.split(markoer)[1].split("GO")[0]
        for gammelt_navn in (
            "Condition_tillaegskode",
            "Procedure_tillaegskode",
            "ServiceRequest_tillaegskode",
        ):
            assert gammelt_navn in blok, f"{tabel}: {gammelt_navn}"
        assert "_SupplementaryCode" in blok, tabel
        assert f"OBJECT_ID(N'[meta].[{tabel}]'" in sql, f"{tabel} skal vaere hegnet"


def test_alter_paa_en_omdoebt_tabel_er_hegnet_af_object_id():
    """COL_LENGTH paa en tabel, der ikke findes, giver NULL — ikke en fejl.

    En migration, der omdoeber en tabel, sorterer maaske foer en migration, der
    tilfoejer en kolonne til den under sit gamle navn. En ren COL_LENGTH-vagt
    slipper saa ALTER TABLE igennem mod et navn, der er vaek, og foerste
    udrulning til et miljoe, hvor omdoebningen er koert, fejler. Enhver ALTER
    mod et navn, en migration omdoeber, skal derfor vaere hegnet af OBJECT_ID.
    """
    import re
    from pathlib import Path

    mappe = Path(__file__).resolve().parents[2] / "infrastructure/ddl/migrations"
    filer = sorted(mappe.glob("*.sql"))
    tekster = {sti.name: sti.read_text(encoding="utf-8") for sti in filer}

    omdoebte: set[str] = set()
    for tekst in tekster.values():
        for gammelt, nyt in re.findall(
            r"sp_rename\s+'(?:fhir|dbo)\.(\w+)'\s*,\s*'(\w+)'\s*(?:;|$)", tekst, re.M
        ):
            omdoebte.update({gammelt, nyt})

    assert omdoebte, "ingen sp_rename fundet — testen rammer forbi"

    uhegnede: list[str] = []
    for navn, tekst in tekster.items():
        for blok in re.split(r"(?mi)^\s*GO\s*$", tekst):
            for tabel in re.findall(r"ALTER TABLE \[(?:fhir|dbo)\]\.\[(\w+)\]", blok):
                if tabel in omdoebte and f"OBJECT_ID(N'[fhir].[{tabel}]'" not in blok:
                    uhegnede.append(f"{navn}: ALTER TABLE {tabel}")

    assert not uhegnede, (
        "ALTER mod en omdoebt tabel uden OBJECT_ID-vagt — vaelter foerste "
        f"udrulning til et miljoe hvor omdoebningen er koert: {uhegnede}"
    )


_OMDOEBNING = (
    Path(__file__).resolve().parents[2]
    / "infrastructure"
    / "ddl"
    / "migrations"
    / "20260909_fhir_tillaegskode_to_supplementary_code.sql"
)


def test_omdoebningen_taaler_at_natjobbet_har_oprettet_maaltabellen():
    """Natjobbet kan naa at koere mellem deploy og full_infrastructure_setup.

    staging_infra_fhir opretter saa de nye tabeller tomme, foer migrationen
    har omdoebt de gamle. Uden haandtering sprang omdoebningen over (maalet
    fandtes), den gamle tabel blev en foraeldreloes rest, og SoF genindlaeste
    alt under det nye navn. En TOM maaltabel skal derfor droppes foer
    omdoebningen -- og kun en tom: har den raekker, roeres ingen af de to.
    """
    sql = _OMDOEBNING.read_text(encoding="utf-8")
    for tabel in ("Condition", "Procedure", "ServiceRequest"):
        drop = f"DROP TABLE [fhir].[{tabel}_SupplementaryCode];"
        assert drop in sql, tabel
        foer_drop = sql[: sql.index(drop)]
        blok = foer_drop[foer_drop.rindex("IF OBJECT_ID(") :]
        assert f"OBJECT_ID(N'[fhir].[{tabel}_tillaegskode]', N'U') IS NOT NULL" in blok, tabel
        assert f"OBJECT_ID(N'[fhir].[{tabel}_SupplementaryCode]', N'U') IS NOT NULL" in blok, tabel
        case = (
            f"CASE WHEN EXISTS (SELECT 1 FROM [fhir].[{tabel}_SupplementaryCode]) "
            "THEN 0 ELSE 1 END"
        )
        assert case in blok, f"{tabel}: @tom = 1 skal betyde tom"
        assert "IF @tom = 1" in blok, f"{tabel}: droppet skal vaere betinget af en tom tabel"
        omdoeb = f"EXEC sp_rename 'fhir.{tabel}_tillaegskode', '{tabel}_SupplementaryCode';"
        assert sql.index(drop) < sql.index(omdoeb), f"{tabel}: droppet skal ske foer omdoebningen"


def test_checkpoint_omdoebningen_rammer_ikke_en_eksisterende_raekke():
    """PK i meta.DFMState er scope/target_type/schema_name/table_name.

    Har SoF allerede oprettet en raekke under det nye navn, ville en uhegnet
    UPDATE give primaernoeglebrud og standse alle efterfoelgende migrationer.
    """
    sql = _OMDOEBNING.read_text(encoding="utf-8")
    blok = sql.split("UPDATE s")[1].split("GO")[0]
    assert "FROM [meta].[DFMState] AS s" in blok
    assert "NOT EXISTS" in blok
    for kol in ("scope", "target_type", "schema_name"):
        assert f"n.[{kol}] = s.[{kol}]" in blok, kol
    # Uden dette praedikat ville NOT EXISTS spaerre for ALLE omdoebninger.
    assert "n.[table_name] = REPLACE(s.[table_name], '_tillaegskode', '_SupplementaryCode')" in blok

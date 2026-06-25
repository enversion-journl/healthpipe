# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Source-DB seed builders for execution-order tests.

Inserts synthetic rows into the **connected test source databases**
(MySupply and SydAx) through the real source resources.  All test rows
use a recognizable key namespace so they can be cleaned up without
affecting other data in the shared test source DBs.

Namespace conventions:
    MySupply:  metadata_id / header_id >= 9_990_000
    SydAx:     ref_LedgertransId >= 9_900_000_000
    CVR:       999xxxxx  (CleanCode_Kreditor = CVR_999xxxxx)
    FakturaNr: TEST-EXEC-xxx
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

NAMESPACE_METADATA_BASE = 9_990_000
NAMESPACE_HEADER_BASE = 9_990_000
NAMESPACE_LINE_BASE = 9_990_000
NAMESPACE_LEDGERTRANS_BASE = 9_900_000_000
NAMESPACE_KONTO_BASE = 9_900_000
NAMESPACE_KREDITOR_BASE = 9_900_000
NAMESPACE_KREDITORTRANS_BASE = 9_900_000_000
NAMESPACE_BRIDGE_BASE = 9_900_000_000


# ── MySupply seed data ────────────────────────────────────────────────


@dataclass
class MySupplyInvoice:
    """One MySupply invoice (metadata + header + lines)."""

    metadata_id: int
    header_id: int
    cvr: str
    faktura_nr: str
    faktura_dato: str
    leverandoer_navn: str = "Test Leverandoer"
    ean_regningsmodtager: str = "5790000000001"
    ordre_dato: str | None = None
    betalings_dato: str | None = None
    faktura_total: str = "1000.00"
    moms_procent: str = "25.0000"
    moms_grundlag: str = "800.00"
    moms_beloeb: str = "200.00"
    linje_total: str = "1000.00"
    lines: list[MySupplyLine] = field(default_factory=list)


@dataclass
class MySupplyLine:
    line_id: int
    leverandoer_varenummer: str = "TESTVARE001"
    antal: str = "1.0000"
    linje_beloeb: str = "100.00"
    enheds_pris: str = "100.00"


def seed_mysupply(conn, invoices: list[MySupplyInvoice]) -> None:
    """Insert MySupply test rows into the source DB (metadata + header + line).

    Column names match the actual mysupply schema:
    - metadata: Guid + data_unit_identity_unique_name are NOT NULL;
      LeverandorCVR feeds CleanCode_Kreditor; Type classifies DokumentType.
    - statistic_invoice_header: FakturaUdstederCVRNummer feeds
      CleanCode_Kreditor; RegningsModtagerID feeds CleanCode_EAN;
      FakturaDato is the (string) partition column.
    - statistic_invoice_line: Item_FakturaUdstederVareNr feeds
      CleanCode_LeverandoerVarenummer.
    """
    cursor = conn.cursor()
    try:
        for inv in invoices:
            cursor.execute(
                """
                IF NOT EXISTS (SELECT 1 FROM [dbo].[metadata] WHERE [id] = ?)
                INSERT INTO [dbo].[metadata] (
                    [id], [Guid], [data_unit_identity_unique_name],
                    [Type], [LeverandorCVR], [FirmaNavn], [FakturaTotal]
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    inv.metadata_id,
                    inv.metadata_id,
                    f"test-exec-{inv.metadata_id}",
                    f"test-exec-{inv.metadata_id}",
                    "PIE",
                    inv.cvr,
                    inv.leverandoer_navn,
                    inv.faktura_total,
                ),
            )

            cursor.execute(
                """
                IF NOT EXISTS (
                    SELECT 1 FROM [dbo].[statistic_invoice_header] WHERE [id] = ?
                )
                INSERT INTO [dbo].[statistic_invoice_header] (
                    [id], [metadata_id], [FakturaDato], [FakturaNr],
                    [FakturaUdstederCVRNummer],
                    [RegningsModtagerID], [RegningsModtagerID_Type],
                    [VAT_AfgiftsKategoriProcent],
                    [VAT_CategoryTotal_AfgiftsKategoriMomsGrundlag],
                    [VAT_TaxAmounts_AfgiftsKategoriMomsBeløb],
                    [FakturaTotal], [FakturaValutaKode],
                    [OrdreDato], [BetalingsDato]
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    inv.header_id,
                    inv.header_id,
                    inv.metadata_id,
                    inv.faktura_dato,
                    inv.faktura_nr,
                    inv.cvr,
                    inv.ean_regningsmodtager,
                    "EAN",
                    inv.moms_procent,
                    inv.moms_grundlag,
                    inv.moms_beloeb,
                    inv.faktura_total,
                    "DKK",
                    inv.ordre_dato or inv.faktura_dato,
                    inv.betalings_dato or inv.faktura_dato,
                ),
            )

            for line in inv.lines:
                cursor.execute(
                    """
                    IF NOT EXISTS (
                        SELECT 1 FROM [dbo].[statistic_invoice_line] WHERE [id] = ?
                    )
                    INSERT INTO [dbo].[statistic_invoice_line] (
                        [id], [statistic_invoice_header_id],
                        [Item_FakturaUdstederVareNr],
                        [Item_FakturaUdstederVareNavn],
                        [AntalEnhederFaktureret], [LinjeBeløb], [EnhedsPris]
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        line.line_id,
                        line.line_id,
                        inv.header_id,
                        line.leverandoer_varenummer,
                        f"Testvare {line.leverandoer_varenummer}",
                        line.antal,
                        line.linje_beloeb,
                        line.enheds_pris,
                    ),
                )

        conn.commit()
        logger.info("Seeded %d MySupply invoices", len(invoices))
    finally:
        cursor.close()


def teardown_mysupply(conn) -> None:
    """Remove all test-namespace rows from MySupply source."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            "DELETE FROM [dbo].[statistic_invoice_line] "
            "WHERE [statistic_invoice_header_id] >= ?",
            (NAMESPACE_HEADER_BASE,),
        )
        cursor.execute(
            "DELETE FROM [dbo].[statistic_invoice_header] WHERE [id] >= ?",
            (NAMESPACE_HEADER_BASE,),
        )
        cursor.execute(
            "DELETE FROM [dbo].[metadata] WHERE [id] >= ?",
            (NAMESPACE_METADATA_BASE,),
        )
        conn.commit()
        logger.info("Cleaned up MySupply test namespace")
    finally:
        cursor.close()


# ── SydAx seed data ──────────────────────────────────────────────────


@dataclass
class SydAxPosting:
    """One SydAx Finanspostering with related bridge/trans/kreditor/konto."""

    finanspostering_id: int
    ref_ledgertrans_id: int
    finanskonto_id: int
    kreditor_id: int
    kreditortransaktion_id: int
    faktura_nr: str
    cvr: str
    oprettelsestidspunkt: str
    kildesystem: str = "SydAX"
    posteringsbeloeb: str = "500.00"
    transaktionsbeloeb: str = "500.00"
    bogfoeringsdato: str | None = None
    kontonummer: str = "9999"


@dataclass
class SydAxFinanskonto:
    """A Finanskonto (chart of accounts entry) in SydAx."""

    finanskonto_id: int
    kontonummer: str = "9999"


def seed_sydax_postings(conn, postings: list[SydAxPosting]) -> None:
    """Insert SydAx Finanspostering + bridge + Kreditortransaktion + Kreditor + Finanskonto."""
    cursor = conn.cursor()
    try:
        seeded_kreditor_ids: set[int] = set()
        seeded_konto_ids: set[int] = set()

        for p in postings:
            # CreatedOn/Modified are the daily/monthly partition columns for
            # Finanskonto and Kreditor extraction — without them the seeded
            # rows are invisible to partitioned runs.
            if p.finanskonto_id not in seeded_konto_ids:
                cursor.execute(
                    """
                    IF NOT EXISTS (
                        SELECT 1 FROM [Finans].[Finanskonto]
                        WHERE [FinanskontoId] = ?
                    )
                    INSERT INTO [Finans].[Finanskonto] (
                        [FinanskontoId], [Kontonummer],
                        [CreatedOn], [Modified]
                    ) VALUES (?, ?, ?, ?)
                """,
                    (
                        p.finanskonto_id,
                        p.finanskonto_id,
                        p.kontonummer,
                        p.oprettelsestidspunkt,
                        p.oprettelsestidspunkt,
                    ),
                )
                seeded_konto_ids.add(p.finanskonto_id)

            if p.kreditor_id not in seeded_kreditor_ids:
                cursor.execute(
                    """
                    IF NOT EXISTS (
                        SELECT 1 FROM [Kreditor].[Kreditor]
                        WHERE [KreditorId] = ?
                    )
                    INSERT INTO [Kreditor].[Kreditor] (
                        [KreditorId], [Kreditornummer], [Kreditornavn],
                        [JuridiskEnhed], [CreatedOn], [Modified]
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        p.kreditor_id,
                        p.kreditor_id,
                        f"K{p.kreditor_id}",
                        f"Test Kreditor {p.kreditor_id}",
                        p.cvr,
                        p.oprettelsestidspunkt,
                        p.oprettelsestidspunkt,
                    ),
                )
                seeded_kreditor_ids.add(p.kreditor_id)

            cursor.execute(
                """
                IF NOT EXISTS (
                    SELECT 1 FROM [Kreditor].[Kreditortransaktion]
                    WHERE [KreditortransaktionId] = ?
                )
                INSERT INTO [Kreditor].[Kreditortransaktion] (
                    [KreditortransaktionId], [KreditorId],
                    [Fakturanummer], [Fakturadato], [Transaktionsdato],
                    [Forfaldsdato],
                    [TransaktionsbeloebValuta], [Transaktionsbeloeb],
                    [Oprettelsestidspunkt]
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    p.kreditortransaktion_id,
                    p.kreditortransaktion_id,
                    p.kreditor_id,
                    p.faktura_nr,
                    p.oprettelsestidspunkt[:10],
                    p.oprettelsestidspunkt[:10],
                    p.oprettelsestidspunkt[:10],
                    p.transaktionsbeloeb,
                    p.transaktionsbeloeb,
                    p.oprettelsestidspunkt,
                ),
            )

            cursor.execute(
                """
                IF NOT EXISTS (
                    SELECT 1 FROM [Finans].[Finanspostering]
                    WHERE [FinansposteringId] = ?
                )
                INSERT INTO [Finans].[Finanspostering] (
                    [FinansposteringId], [ref_LedgertransId],
                    [FinanskontoId], [Kildesystem],
                    [Posteringsbeloeb], [Oprettelsestidspunkt],
                    [Bogfoeringsdato]
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    p.finanspostering_id,
                    p.finanspostering_id,
                    p.ref_ledgertrans_id,
                    p.finanskonto_id,
                    p.kildesystem,
                    p.posteringsbeloeb,
                    p.oprettelsestidspunkt,
                    p.bogfoeringsdato or p.oprettelsestidspunkt[:10],
                ),
            )

            cursor.execute(
                """
                IF NOT EXISTS (
                    SELECT 1 FROM [Finans].[FinansposteringKreditorTransRelation]
                    WHERE [FinansposteringId] = ?
                      AND [KreditortransaktionId] = ?
                )
                INSERT INTO [Finans].[FinansposteringKreditorTransRelation] (
                    [FinansposteringKreditorTransRelationId],
                    [FinansposteringId], [KreditortransaktionId]
                ) VALUES (?, ?, ?)
            """,
                (
                    p.finanspostering_id,
                    p.kreditortransaktion_id,
                    p.finanspostering_id,
                    p.finanspostering_id,
                    p.kreditortransaktion_id,
                ),
            )

        conn.commit()
        logger.info("Seeded %d SydAx postings", len(postings))
    finally:
        cursor.close()


def seed_sydax_kreditor(conn, kreditor_id: int, cvr: str, created_on: str, name: str = "") -> None:
    """Seed a single Kreditor row (for late-arriving dim scenarios).

    *created_on* must fall in the partition the test extracts next —
    CreatedOn/Modified are the Kreditor partition columns.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            IF NOT EXISTS (
                SELECT 1 FROM [Kreditor].[Kreditor] WHERE [KreditorId] = ?
            )
            INSERT INTO [Kreditor].[Kreditor] (
                [KreditorId], [Kreditornummer], [Kreditornavn],
                [JuridiskEnhed], [CreatedOn], [Modified]
            ) VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                kreditor_id,
                kreditor_id,
                f"K{kreditor_id}",
                name or f"Late Kreditor {kreditor_id}",
                cvr,
                created_on,
                created_on,
            ),
        )
        conn.commit()
    finally:
        cursor.close()


def seed_sydax_finanskonto(
    conn, finanskonto_id: int, created_on: str, kontonummer: str = "9999"
) -> None:
    """Seed a single Finanskonto row (for late-arriving dim scenarios)."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            IF NOT EXISTS (
                SELECT 1 FROM [Finans].[Finanskonto] WHERE [FinanskontoId] = ?
            )
            INSERT INTO [Finans].[Finanskonto] (
                [FinanskontoId], [Kontonummer], [CreatedOn], [Modified]
            ) VALUES (?, ?, ?, ?)
        """,
            (
                finanskonto_id,
                finanskonto_id,
                kontonummer,
                created_on,
                created_on,
            ),
        )
        conn.commit()
    finally:
        cursor.close()


@dataclass
class SydAxBudgetpost:
    """One SydAx Budgetpost with its Budgetmodel and status."""

    ref_ledgerbudget_id: int
    budgetmodel_id: int
    budgetpost_status_id: int
    finanskonto_id: int
    dato: str  # partition date (Oprettelsestidspunkt / SidstAendretTidspunkt)
    budgetbeloeb: str = "10000.00"
    valutakode: str = "DKK"
    kildesystem: str = "SydAX"


def seed_sydax_budgetposts(conn, posts: list[SydAxBudgetpost]) -> None:
    """Insert Budgetpost + Budgetmodel + BudgetpostStatus test rows.

    The referenced Finanskonto must be seeded separately (e.g. via
    seed_sydax_postings or seed_sydax_finanskonto) so DW_EK_Kontoplan can
    resolve. Partition columns: Oprettelsestidspunkt OR SidstAendretTidspunkt.
    """
    cursor = conn.cursor()
    try:
        for p in posts:
            ts = p.dato + " 12:00:00"
            cursor.execute(
                """
                IF NOT EXISTS (
                    SELECT 1 FROM [Finans].[Budgetmodel] WHERE [BudgetmodelId] = ?
                )
                INSERT INTO [Finans].[Budgetmodel] (
                    [BudgetmodelId], [ref_BudgetmodelId], [Budgetmodelnummer],
                    [Budgetmodeltekst], [Budgetmodeltype], [ErLukket], [Kildesystem]
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    p.budgetmodel_id,
                    p.budgetmodel_id,
                    p.budgetmodel_id,
                    f"BM{p.budgetmodel_id}",
                    "Test budgetmodel",
                    "Original",
                    0,
                    p.kildesystem,
                ),
            )

            cursor.execute(
                """
                IF NOT EXISTS (
                    SELECT 1 FROM [Finans].[BudgetpostStatus]
                    WHERE [BudgetpostStatusId] = ?
                )
                INSERT INTO [Finans].[BudgetpostStatus] (
                    [BudgetpostStatusId], [BudgetpostStatustekst]
                ) VALUES (?, ?)
            """,
                (
                    p.budgetpost_status_id,
                    p.budgetpost_status_id,
                    "Godkendt (test)",
                ),
            )

            cursor.execute(
                """
                IF NOT EXISTS (
                    SELECT 1 FROM [Finans].[Budgetpost]
                    WHERE [ref_LedgerbudgetId] = ?
                )
                INSERT INTO [Finans].[Budgetpost] (
                    [ref_LedgerbudgetId], [BudgetmodelId], [BudgetpostStatusId],
                    [FinanskontoId], [Budgetdato],
                    [Oprettelsestidspunkt], [SidstAendretTidspunkt],
                    [Budgetbeloeb], [Valutakode], [DebetKredit],
                    [Budgetkode], [Ændringstype], [BeloebstypeId], [Kildesystem]
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    p.ref_ledgerbudget_id,
                    p.ref_ledgerbudget_id,
                    p.budgetmodel_id,
                    p.budgetpost_status_id,
                    p.finanskonto_id,
                    p.dato,
                    ts,
                    ts,
                    p.budgetbeloeb,
                    p.valutakode,
                    "Debet",
                    "TESTBUDGET",
                    "Oprettet",
                    None,
                    p.kildesystem,
                ),
            )

        conn.commit()
        logger.info("Seeded %d SydAx budgetposts", len(posts))
    finally:
        cursor.close()


@dataclass
class SydAxFinanskladdelinje:
    """One SydAx Finanskladdelinje with its Finanskladde header."""

    finanskladdelinje_id: int
    ref_ledgertrans_id: int
    finanskladde_id: int
    ref_ledgerjournaltable_id: int
    finanskonto_id: int
    dato: str  # partition date (CreatedOn / Modified)
    debet: str = "250.00"
    kildesystem: str = "SydAX"


def seed_sydax_finanskladdelinjer(conn, linjer: list[SydAxFinanskladdelinje]) -> None:
    """Insert Finanskladdelinje + Finanskladde test rows.

    Partition columns: CreatedOn OR Modified on Finanskladdelinje;
    CleanCode_Finanskladde derives from ref_LedgerjournaltableId.
    """
    cursor = conn.cursor()
    try:
        for ln in linjer:
            ts = ln.dato + " 12:00:00"
            cursor.execute(
                """
                IF NOT EXISTS (
                    SELECT 1 FROM [Finans].[Finanskladde]
                    WHERE [FinanskladdeId] = ?
                )
                INSERT INTO [Finans].[Finanskladde] (
                    [FinanskladdeId], [ref_LedgerjournaltableId],
                    [Kladdenummer], [Kladdenavn], [Kladdetekst],
                    [ErLukket], [Kildesystem], [CreatedOn], [Modified]
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    ln.finanskladde_id,
                    ln.finanskladde_id,
                    ln.ref_ledgerjournaltable_id,
                    f"KL{ln.finanskladde_id}",
                    "Test kladde",
                    "Test kladdetekst",
                    0,
                    ln.kildesystem,
                    ts,
                    ts,
                ),
            )

            cursor.execute(
                """
                IF NOT EXISTS (
                    SELECT 1 FROM [Finans].[Finanskladdelinje]
                    WHERE [FinanskladdelinjeId] = ?
                )
                INSERT INTO [Finans].[Finanskladdelinje] (
                    [FinanskladdelinjeId], [FinanskladdeId],
                    [ref_LedgertransId], [ref_RecId], [FinanskontoId],
                    [Linjenummer], [dato], [Debet], [Kredit],
                    [Beskrivelse], [Kildesystem], [CreatedOn], [Modified]
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    ln.finanskladdelinje_id,
                    ln.finanskladdelinje_id,
                    ln.finanskladde_id,
                    ln.ref_ledgertrans_id,
                    ln.finanskladdelinje_id,
                    ln.finanskonto_id,
                    1,
                    ln.dato,
                    ln.debet,
                    None,
                    "Test kladdelinje",
                    ln.kildesystem,
                    ts,
                    ts,
                ),
            )

        conn.commit()
        logger.info("Seeded %d SydAx finanskladdelinjer", len(linjer))
    finally:
        cursor.close()


def teardown_sydax(conn) -> None:
    """Remove all test-namespace rows from SydAx source."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            "DELETE FROM [Finans].[FinansposteringKreditorTransRelation] "
            "WHERE [FinansposteringId] >= ?",
            (NAMESPACE_BRIDGE_BASE,),
        )
        cursor.execute(
            "DELETE FROM [Finans].[Finanspostering] WHERE [FinansposteringId] >= ?",
            (NAMESPACE_BRIDGE_BASE,),
        )
        cursor.execute(
            "DELETE FROM [Kreditor].[Kreditortransaktion] " "WHERE [KreditortransaktionId] >= ?",
            (NAMESPACE_KREDITORTRANS_BASE,),
        )
        cursor.execute(
            "DELETE FROM [Kreditor].[Kreditor] WHERE [KreditorId] >= ?",
            (NAMESPACE_KREDITOR_BASE,),
        )
        cursor.execute(
            "DELETE FROM [Finans].[Finanskonto] WHERE [FinanskontoId] >= ?",
            (NAMESPACE_KONTO_BASE,),
        )
        cursor.execute(
            "DELETE FROM [Finans].[Budgetpost] WHERE [ref_LedgerbudgetId] >= ?",
            (NAMESPACE_LEDGERTRANS_BASE,),
        )
        cursor.execute(
            "DELETE FROM [Finans].[Budgetmodel] WHERE [BudgetmodelId] >= ?",
            (NAMESPACE_KONTO_BASE,),
        )
        cursor.execute(
            "DELETE FROM [Finans].[BudgetpostStatus] WHERE [BudgetpostStatusId] >= ?",
            (NAMESPACE_KONTO_BASE,),
        )
        cursor.execute(
            "DELETE FROM [Finans].[Finanskladdelinje] " "WHERE [FinanskladdelinjeId] >= ?",
            (NAMESPACE_LEDGERTRANS_BASE,),
        )
        cursor.execute(
            "DELETE FROM [Finans].[Finanskladde] WHERE [FinanskladdeId] >= ?",
            (NAMESPACE_KONTO_BASE,),
        )
        conn.commit()
        logger.info("Cleaned up SydAx test namespace")
    finally:
        cursor.close()


def remove_sydax_posting(conn, finanspostering_id: int) -> None:
    """Remove a single posting and its bridge (for S7 entity-removal scenario)."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            "DELETE FROM [Finans].[FinansposteringKreditorTransRelation] "
            "WHERE [FinansposteringId] = ?",
            (finanspostering_id,),
        )
        cursor.execute(
            "DELETE FROM [Finans].[Finanspostering] WHERE [FinansposteringId] = ?",
            (finanspostering_id,),
        )
        conn.commit()
    finally:
        cursor.close()


# ── Scenario helpers ──────────────────────────────────────────────────
# Convenience functions to build seed data for specific scenarios.


def make_invoice(
    *,
    idx: int,
    cvr: str,
    faktura_nr: str,
    dato: str,
    n_lines: int = 1,
    **overrides,
) -> MySupplyInvoice:
    """Build a MySupplyInvoice with sensible defaults."""
    meta_id = NAMESPACE_METADATA_BASE + idx
    header_id = NAMESPACE_HEADER_BASE + idx
    lines = [
        MySupplyLine(
            line_id=NAMESPACE_LINE_BASE + idx * 100 + i,
            leverandoer_varenummer=f"TESTVARE{idx:03d}_{i}",
        )
        for i in range(n_lines)
    ]
    kwargs = dict(
        metadata_id=meta_id,
        header_id=header_id,
        cvr=cvr,
        faktura_nr=faktura_nr,
        faktura_dato=dato,
        lines=lines,
    )
    kwargs.update(overrides)
    return MySupplyInvoice(**kwargs)


def make_posting(
    *,
    idx: int,
    cvr: str,
    faktura_nr: str,
    dato: str,
    konto_id: int | None = None,
    kreditor_id: int | None = None,
    **overrides,
) -> SydAxPosting:
    """Build a SydAxPosting with sensible defaults."""
    fp_id = NAMESPACE_BRIDGE_BASE + idx
    ref_id = NAMESPACE_LEDGERTRANS_BASE + idx
    kt_id = NAMESPACE_KREDITORTRANS_BASE + idx
    k_id = kreditor_id or (NAMESPACE_KREDITOR_BASE + idx)
    fk_id = konto_id or (NAMESPACE_KONTO_BASE + idx)
    oprettet = dato + " 12:00:00"

    kwargs = dict(
        finanspostering_id=fp_id,
        ref_ledgertrans_id=ref_id,
        finanskonto_id=fk_id,
        kreditor_id=k_id,
        kreditortransaktion_id=kt_id,
        faktura_nr=faktura_nr,
        cvr=cvr,
        oprettelsestidspunkt=oprettet,
    )
    kwargs.update(overrides)
    return SydAxPosting(**kwargs)

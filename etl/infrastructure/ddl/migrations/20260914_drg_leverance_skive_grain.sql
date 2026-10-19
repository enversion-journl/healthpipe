-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- meta.DRGLeverance faar aarsskive-grain + leverance-identitet.
--
-- Leverancedetektionens tier 2 (se
-- docs/source_systems/drg/drg_leverancedetektion_beslutningsnotat.md) afgoer
-- PR. (takstsystem, aktivitetsaar), om en leverance baerer nyere data end det,
-- arkivet allerede afspejler. Det kraever, at registret kan svare paa "hvilken
-- cap afspejler denne skive?", hvilket den hidtidige noegle
-- (kildetabel, takstsystem, leverance_dato) ikke kan.
--
-- Ny noegle: (kildetabel, takstsystem, aktivitetsaar, leverance_ts), plus
-- kolonnerne kadence, leverance_ts, lpropdateringsdato og cap_kilde.
--
-- REGISTRET GENOPBYGGES — DET MIGRERES IKKE.
-- DRG er en selvstaendig loesning, og arkivet kan toemmes og genindlaeses fra
-- leverancerne paa sharet. De eksisterende raekker beskriver leverancer merget
-- under den gamle logik og har hverken aktivitetsaar eller pakke-timestamp; at
-- backfille dem ville vaere at opfinde en historik, der alligevel bliver
-- overskrevet ved genindlaesningen. Tabellen droppes derfor, og
-- tables/drg/01_meta.sql genskaber den med den nye definition — base-DDL'en
-- koerer efter migrationerne (haandhaevet af
-- tests/unit/test_infra_migrations_ordering.py).
--
-- Konsekvens for drift: efter denne migration er registret tomt, saa foerste
-- koersel har ingen registreret cap for nogen skive og merger alt. Det er den
-- rigtige opfoersel — arkivet skal alligevel genindlaeses.
--
-- Idempotent: guarden ser paa leverance_ts. Er kolonnen der allerede, er
-- migrationen koert, og tabellen roeres ikke — en genkoersel kan altsaa ikke
-- smide nyindlaest data vaek. Tabellen findes kun i staging-databasen, saa
-- blokken no-op'er paa DSO via OBJECT_ID-guarden. Der oprettes bevidst INTET
-- her: en CREATE i migrationen ville oprette tabellen i DSO, hvor den ikke
-- hoerer hjemme.
-- ============================================================================

IF OBJECT_ID(N'[meta].[DRGLeverance]', 'U') IS NOT NULL
   AND COL_LENGTH('meta.DRGLeverance', 'leverance_ts') IS NULL
BEGIN
    DROP TABLE [meta].[DRGLeverance];
END
GO

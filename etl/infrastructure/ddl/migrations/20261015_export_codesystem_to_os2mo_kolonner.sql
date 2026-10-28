-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: export.CodesystemToOs2mo re-formes til filens aftalte kolonner
-- (kode/kode_parent/gyldig_*/navn/type/niveau/aktiv + _codesystem-postfix paa
-- de tre DW-audit-kolliderende). Tabellen er ren state (DELETE+INSERT pr.
-- koersel), saa den DROPpes her og genskabes af staging_infra_export's DDL;
-- naeste eksportkoersel fylder den. Idempotent + tracked once pr. database.

IF OBJECT_ID(N'[export].[CodesystemToOs2mo]', 'U') IS NOT NULL
    DROP TABLE [export].[CodesystemToOs2mo];
GO

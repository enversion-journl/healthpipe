-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: rename the 39 recently-added DFM ext/arc tables to the
-- source-prefixed form (e.g. arc.art_sydax -> arc.sydax_art,
-- ext.art_sydax_dfm -> ext.sydax_art_dfm) for SydAX, ILS and MySupply.
--
-- These archives have no downstream consumers, so the rename is done by
-- dropping the old-named arc + ext tables and clearing their dfm_state
-- checkpoints; the domain DDL (00_archive.sql, which runs AFTER migrations)
-- recreates them under the new names, and the next extract full-loads them.
-- Tracked once via meta.migration_history; on a fresh database every guard
-- below is a no-op (the old names never existed).

SET NOCOUNT ON;

IF OBJECT_ID(N'[arc].[afdeling_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[afdeling_sydax];
IF OBJECT_ID(N'[arc].[art_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[art_sydax];
IF OBJECT_ID(N'[arc].[dranst_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[dranst_sydax];
IF OBJECT_ID(N'[arc].[finanskladde_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[finanskladde_sydax];
IF OBJECT_ID(N'[arc].[finanskladdelinje_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[finanskladdelinje_sydax];
IF OBJECT_ID(N'[arc].[finanskonto_omkostningssted_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[finanskonto_omkostningssted_sydax];
IF OBJECT_ID(N'[arc].[finanskonto_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[finanskonto_sydax];
IF OBJECT_ID(N'[arc].[finanspostering_kreditortrans_relation_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[finanspostering_kreditortrans_relation_sydax];
IF OBJECT_ID(N'[arc].[finanspostering_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[finanspostering_sydax];
IF OBJECT_ID(N'[arc].[finanstransaktionstype_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[finanstransaktionstype_sydax];
IF OBJECT_ID(N'[arc].[funktion_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[funktion_sydax];
IF OBJECT_ID(N'[arc].[gruppering_autoriseret_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[gruppering_autoriseret_sydax];
IF OBJECT_ID(N'[arc].[gruppering_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[gruppering_sydax];
IF OBJECT_ID(N'[arc].[hovedkonto_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[hovedkonto_sydax];
IF OBJECT_ID(N'[arc].[indkfak_ils]', 'U') IS NOT NULL DROP TABLE [arc].[indkfak_ils];
IF OBJECT_ID(N'[arc].[kreditor_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[kreditor_sydax];
IF OBJECT_ID(N'[arc].[kreditortransaktion_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[kreditortransaktion_sydax];
IF OBJECT_ID(N'[arc].[laftd_ils]', 'U') IS NOT NULL DROP TABLE [arc].[laftd_ils];
IF OBJECT_ID(N'[arc].[laftdpris_ils]', 'U') IS NOT NULL DROP TABLE [arc].[laftdpris_ils];
IF OBJECT_ID(N'[arc].[laftdstam_ils]', 'U') IS NOT NULL DROP TABLE [arc].[laftdstam_ils];
IF OBJECT_ID(N'[arc].[lafth_ils]', 'U') IS NOT NULL DROP TABLE [arc].[lafth_ils];
IF OBJECT_ID(N'[arc].[lev_ils]', 'U') IS NOT NULL DROP TABLE [arc].[lev_ils];
IF OBJECT_ID(N'[arc].[lgr_ils]', 'U') IS NOT NULL DROP TABLE [arc].[lgr_ils];
IF OBJECT_ID(N'[arc].[lgrvare_ils]', 'U') IS NOT NULL DROP TABLE [arc].[lgrvare_ils];
IF OBJECT_ID(N'[arc].[metadata_mysupply]', 'U') IS NOT NULL DROP TABLE [arc].[metadata_mysupply];
IF OBJECT_ID(N'[arc].[moms_ils]', 'U') IS NOT NULL DROP TABLE [arc].[moms_ils];
IF OBJECT_ID(N'[arc].[omkostningssted_hierarki_sydax]', 'U') IS NOT NULL DROP TABLE [arc].[omkostningssted_hierarki_sydax];
IF OBJECT_ID(N'[arc].[pmdm_ils]', 'U') IS NOT NULL DROP TABLE [arc].[pmdm_ils];
IF OBJECT_ID(N'[arc].[rekv_ils]', 'U') IS NOT NULL DROP TABLE [arc].[rekv_ils];
IF OBJECT_ID(N'[arc].[rekvgrp_ils]', 'U') IS NOT NULL DROP TABLE [arc].[rekvgrp_ils];
IF OBJECT_ID(N'[arc].[sgh_ils]', 'U') IS NOT NULL DROP TABLE [arc].[sgh_ils];
IF OBJECT_ID(N'[arc].[statistic_invoice_header_allowance_charge_mysupply]', 'U') IS NOT NULL DROP TABLE [arc].[statistic_invoice_header_allowance_charge_mysupply];
IF OBJECT_ID(N'[arc].[statistic_invoice_header_mysupply]', 'U') IS NOT NULL DROP TABLE [arc].[statistic_invoice_header_mysupply];
IF OBJECT_ID(N'[arc].[statistic_invoice_line_allowance_charge_mysupply]', 'U') IS NOT NULL DROP TABLE [arc].[statistic_invoice_line_allowance_charge_mysupply];
IF OBJECT_ID(N'[arc].[statistic_invoice_line_mysupply]', 'U') IS NOT NULL DROP TABLE [arc].[statistic_invoice_line_mysupply];
IF OBJECT_ID(N'[arc].[udlev_ils]', 'U') IS NOT NULL DROP TABLE [arc].[udlev_ils];
IF OBJECT_ID(N'[arc].[vareart_ils]', 'U') IS NOT NULL DROP TABLE [arc].[vareart_ils];
IF OBJECT_ID(N'[arc].[varegrp_ils]', 'U') IS NOT NULL DROP TABLE [arc].[varegrp_ils];
IF OBJECT_ID(N'[arc].[varetype_ils]', 'U') IS NOT NULL DROP TABLE [arc].[varetype_ils];
GO

IF OBJECT_ID(N'[ext].[afdeling_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[afdeling_sydax_dfm];
IF OBJECT_ID(N'[ext].[art_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[art_sydax_dfm];
IF OBJECT_ID(N'[ext].[dranst_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[dranst_sydax_dfm];
IF OBJECT_ID(N'[ext].[finanskladde_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[finanskladde_sydax_dfm];
IF OBJECT_ID(N'[ext].[finanskladdelinje_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[finanskladdelinje_sydax_dfm];
IF OBJECT_ID(N'[ext].[finanskonto_omkostningssted_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[finanskonto_omkostningssted_sydax_dfm];
IF OBJECT_ID(N'[ext].[finanskonto_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[finanskonto_sydax_dfm];
IF OBJECT_ID(N'[ext].[finanspostering_kreditortrans_relation_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[finanspostering_kreditortrans_relation_sydax_dfm];
IF OBJECT_ID(N'[ext].[finanspostering_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[finanspostering_sydax_dfm];
IF OBJECT_ID(N'[ext].[finanstransaktionstype_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[finanstransaktionstype_sydax_dfm];
IF OBJECT_ID(N'[ext].[funktion_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[funktion_sydax_dfm];
IF OBJECT_ID(N'[ext].[gruppering_autoriseret_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[gruppering_autoriseret_sydax_dfm];
IF OBJECT_ID(N'[ext].[gruppering_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[gruppering_sydax_dfm];
IF OBJECT_ID(N'[ext].[hovedkonto_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[hovedkonto_sydax_dfm];
IF OBJECT_ID(N'[ext].[indkfak_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[indkfak_ils_dfm];
IF OBJECT_ID(N'[ext].[kreditor_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[kreditor_sydax_dfm];
IF OBJECT_ID(N'[ext].[kreditortransaktion_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[kreditortransaktion_sydax_dfm];
IF OBJECT_ID(N'[ext].[laftd_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[laftd_ils_dfm];
IF OBJECT_ID(N'[ext].[laftdpris_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[laftdpris_ils_dfm];
IF OBJECT_ID(N'[ext].[laftdstam_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[laftdstam_ils_dfm];
IF OBJECT_ID(N'[ext].[lafth_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[lafth_ils_dfm];
IF OBJECT_ID(N'[ext].[lev_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[lev_ils_dfm];
IF OBJECT_ID(N'[ext].[lgr_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[lgr_ils_dfm];
IF OBJECT_ID(N'[ext].[lgrvare_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[lgrvare_ils_dfm];
IF OBJECT_ID(N'[ext].[metadata_mysupply_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[metadata_mysupply_dfm];
IF OBJECT_ID(N'[ext].[moms_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[moms_ils_dfm];
IF OBJECT_ID(N'[ext].[omkostningssted_hierarki_sydax_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[omkostningssted_hierarki_sydax_dfm];
IF OBJECT_ID(N'[ext].[pmdm_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[pmdm_ils_dfm];
IF OBJECT_ID(N'[ext].[rekv_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[rekv_ils_dfm];
IF OBJECT_ID(N'[ext].[rekvgrp_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[rekvgrp_ils_dfm];
IF OBJECT_ID(N'[ext].[sgh_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[sgh_ils_dfm];
IF OBJECT_ID(N'[ext].[statistic_invoice_header_allowance_charge_mysupply_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[statistic_invoice_header_allowance_charge_mysupply_dfm];
IF OBJECT_ID(N'[ext].[statistic_invoice_header_mysupply_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[statistic_invoice_header_mysupply_dfm];
IF OBJECT_ID(N'[ext].[statistic_invoice_line_allowance_charge_mysupply_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[statistic_invoice_line_allowance_charge_mysupply_dfm];
IF OBJECT_ID(N'[ext].[statistic_invoice_line_mysupply_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[statistic_invoice_line_mysupply_dfm];
IF OBJECT_ID(N'[ext].[udlev_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[udlev_ils_dfm];
IF OBJECT_ID(N'[ext].[vareart_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[vareart_ils_dfm];
IF OBJECT_ID(N'[ext].[varegrp_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[varegrp_ils_dfm];
IF OBJECT_ID(N'[ext].[varetype_ils_dfm]', 'U') IS NOT NULL DROP TABLE [ext].[varetype_ils_dfm];
GO

DELETE FROM [meta].[dfm_state]
WHERE [table_name] IN (
        'afdeling_sydax',
        'art_sydax',
        'dranst_sydax',
        'finanskladde_sydax',
        'finanskladdelinje_sydax',
        'finanskonto_omkostningssted_sydax',
        'finanskonto_sydax',
        'finanspostering_kreditortrans_relation_sydax',
        'finanspostering_sydax',
        'finanstransaktionstype_sydax',
        'funktion_sydax',
        'gruppering_autoriseret_sydax',
        'gruppering_sydax',
        'hovedkonto_sydax',
        'indkfak_ils',
        'kreditor_sydax',
        'kreditortransaktion_sydax',
        'laftd_ils',
        'laftdpris_ils',
        'laftdstam_ils',
        'lafth_ils',
        'lev_ils',
        'lgr_ils',
        'lgrvare_ils',
        'metadata_mysupply',
        'moms_ils',
        'omkostningssted_hierarki_sydax',
        'pmdm_ils',
        'rekv_ils',
        'rekvgrp_ils',
        'sgh_ils',
        'statistic_invoice_header_allowance_charge_mysupply',
        'statistic_invoice_header_mysupply',
        'statistic_invoice_line_allowance_charge_mysupply',
        'statistic_invoice_line_mysupply',
        'udlev_ils',
        'vareart_ils',
        'varegrp_ils',
        'varetype_ils'
    )
   OR [table_name] IN (
        'afdeling_sydax_flow_start',
        'art_sydax_flow_start',
        'dranst_sydax_flow_start',
        'finanskladde_sydax_flow_start',
        'finanskladdelinje_sydax_flow_start',
        'finanskonto_omkostningssted_sydax_flow_start',
        'finanskonto_sydax_flow_start',
        'finanspostering_kreditortrans_relation_sydax_flow_start',
        'finanspostering_sydax_flow_start',
        'finanstransaktionstype_sydax_flow_start',
        'funktion_sydax_flow_start',
        'gruppering_autoriseret_sydax_flow_start',
        'gruppering_sydax_flow_start',
        'hovedkonto_sydax_flow_start',
        'indkfak_ils_flow_start',
        'kreditor_sydax_flow_start',
        'kreditortransaktion_sydax_flow_start',
        'laftd_ils_flow_start',
        'laftdpris_ils_flow_start',
        'laftdstam_ils_flow_start',
        'lafth_ils_flow_start',
        'lev_ils_flow_start',
        'lgr_ils_flow_start',
        'lgrvare_ils_flow_start',
        'metadata_mysupply_flow_start',
        'moms_ils_flow_start',
        'omkostningssted_hierarki_sydax_flow_start',
        'pmdm_ils_flow_start',
        'rekv_ils_flow_start',
        'rekvgrp_ils_flow_start',
        'sgh_ils_flow_start',
        'statistic_invoice_header_allowance_charge_mysupply_flow_start',
        'statistic_invoice_header_mysupply_flow_start',
        'statistic_invoice_line_allowance_charge_mysupply_flow_start',
        'statistic_invoice_line_mysupply_flow_start',
        'udlev_ils_flow_start',
        'vareart_ils_flow_start',
        'varegrp_ils_flow_start',
        'varetype_ils_flow_start'
    );
GO

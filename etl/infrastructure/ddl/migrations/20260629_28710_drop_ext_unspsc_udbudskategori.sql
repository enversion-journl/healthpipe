-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- #28710: udbudskategori flyttet fra ext (daily extract) til arc (infra seed).
-- Drop den forældede ext-landingstabel.
IF OBJECT_ID(N'[ext].[unspsc_udbudskategori]', 'U') IS NOT NULL
    DROP TABLE [ext].[unspsc_udbudskategori];
GO

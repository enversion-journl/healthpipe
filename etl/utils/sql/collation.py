# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Collation for keys the source distinguishes by case only."""

# Byte-for-byte comparison. A target computed key column holding such a key (e.g.
# fhir.Questionnaire_item.link_id_exact) is declared in the same collation.
EXACT_MATCH_COLLATION = "Latin1_General_BIN2"

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SHAK-a-like -> SOR ConceptMap asset."""

from .shakalike_to_sor import build_shakalike_to_sor_mappings, cm_shakalike_to_sor

__all__ = ["cm_shakalike_to_sor", "build_shakalike_to_sor_mappings"]

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""FHIR-specific implementation behind ``core.resources.FHIRDatabaseResource``.

Public pipeline code should keep importing and calling the Dagster resource from
``core.resources``. The modules in this package hold the FHIR/Aidbox-specific
implementation delegated to by that resource so ``resources.py`` can stay a
stable compatibility facade instead of owning every method body.
"""

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Centralized organization-specific configuration.

This module centralizes per-deployment settings (the local data directory and the
FHIR CodeSystem publisher), read from environment variables with sensible defaults
so the platform can run for a different organization without code changes.

Note: some org-binding values are deliberately NOT routed through this module. The
FHIR/CodeSystem namespace URLs and `urn:enversion:*` identifiers are published data
contracts -- changing them re-tags emitted resources -- so exposing them as config
knobs would be a data-contract footgun. (The FHIR *publisher* below is the
exception: it is descriptive metadata, not an identifier, so it is safe to
override.) `pyproject.toml` branding is handled separately. These are catalogued in
`docs/discovery/org_specific_references.md`.
"""

import os

# ---------------------------------------------------------------------------
# Data directory (local file persistence for downloads, caches, etc.)
# ---------------------------------------------------------------------------
DATA_DIR = os.getenv("DATA_DIR", "/workspaces/healthpipe/data")

# ---------------------------------------------------------------------------
# FHIR CodeSystem publisher
#
# Descriptive `publisher` metadata stamped on published CodeSystem resources.
# This is the *organization* publishing the terminology (not the software), so it
# is org-specific and overridable. The national SKS codesystem carries its own
# publisher (Sundhedsdatastyrelsen) and is unaffected by this default.
# ---------------------------------------------------------------------------
FHIR_PUBLISHER = os.getenv("FHIR_PUBLISHER", "Your Organization")

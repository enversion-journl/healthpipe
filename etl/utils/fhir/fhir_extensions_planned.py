# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Extension URLs the projection reads but no transform produces yet.

``fhir_extensions.py`` holds only what is posted; ``test_view_definition_extension_urls`` reads
a constant there as "the extension is produced". A URL moves there, and off the test's
allowlist, when a transform starts producing it.
"""

# Patient: IFDB has the column (OENSKER_DISKRETION), but the transform does not
# build the extension. Owned by IFDB in the ownership table.
EXTENSION_DISCRETION_FLAG = "http://example.org/fhir/StructureDefinition/discretionFlag"

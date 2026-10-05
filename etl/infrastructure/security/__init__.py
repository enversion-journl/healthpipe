# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DPA security AD extraction + staging load (used by infrastructure/assets/security_ad_assets.py).

Moved here from scripts/ so the modules ship with the deployed app package
(scripts/ is dev-only tooling and is not importable in the Dagster runtime).
"""

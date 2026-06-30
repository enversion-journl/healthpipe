# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Warning filters for Dagster BetaWarning suppression."""

import warnings


def apply_observable_source_asset_filter() -> None:
    """Suppress observable_source_asset BetaWarning."""
    warnings.filterwarnings("ignore", message=".*observable_source_asset.*")

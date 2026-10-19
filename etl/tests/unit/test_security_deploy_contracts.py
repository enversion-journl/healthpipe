# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Deploy-safety contracts for the AD security chain.

The code-server image ships only etl/ — this failure mode passes
every local test and dies only in the deployed container.
"""

from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.unit
def test_security_ad_assets_import_from_shipped_package():
    src = (_ROOT / "infrastructure" / "assets" / "security_ad_assets.py").read_text(
        encoding="utf-8"
    )
    assert "from infrastructure.security." in src
    assert "from scripts." not in src, (
        "scripts/ is not shipped in the code-server image; importing the AD "
        "modules from there resolves locally but raises ModuleNotFoundError "
        "on the deployed extract_security asset"
    )

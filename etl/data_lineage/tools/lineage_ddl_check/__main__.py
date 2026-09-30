# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

import sys

from data_lineage.tools.lineage_ddl_check import run


def main() -> int:
    report = run()
    print(report.render())
    return 1 if report.has_findings else 0


if __name__ == "__main__":
    sys.exit(main())

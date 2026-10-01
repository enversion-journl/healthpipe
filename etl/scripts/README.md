<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# etl/scripts

## license_headers.py — SPDX license headers (US 27067)

Verifies / inserts AGPL SPDX headers per file type for the Healthpipe public tree.

```bash
python etl/scripts/license_headers.py --check   # verify (CI / manual hook); exit 1 if any missing
python etl/scripts/license_headers.py --apply   # insert missing headers in place
python etl/scripts/license_headers.py --diff    # dry-run unified diff
```

Header: `SPDX-FileCopyrightText: <year> Enversion Journl ApS` + `SPDX-License-Identifier: AGPL-3.0-or-later`, referencing the root `LICENSE`. `.json`/`.csv`/lock files and test fixtures are skipped. The tree-wide `--apply` sweep is run once at the GitLab migration, not per-PR. Tests: `pytest etl/scripts/tests/`.

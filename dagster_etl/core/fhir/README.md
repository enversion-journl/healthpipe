<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Core FHIR helpers

This package contains FHIR/Aidbox-specific implementation used by
`core.resources.FHIRDatabaseResource`.

The public call-site contract is still:

```python
from core.resources import FHIRDatabaseResource
```

Assets and shared FHIR helpers should call methods on `FHIRDatabaseResource`,
not import these module functions directly. The functions here accept the
resource instance as their first argument because `resources.py` keeps the
stable Dagster resource class and delegates method bodies into this package.

## What belongs here

- `identifier_lookup.py`: FHIR identifier lookup orchestration, cache lifecycle,
  Postgres identifier streaming, and the FHIR-facing facade behaviour backed by
  `core.cache`.
- `direct_upsert.py`: Aidbox/PostgreSQL direct upsert implementation, including
  dict upsert sharding and post-upsert cache invalidation.
- `aidbox_json.py`: FHIR JSON compatibility transforms needed before writing
  resources directly into Aidbox tables.

## What belongs elsewhere

- Generic SQLite cache file handling, locking, and atomic publish belongs in
  `core.cache`.
- Executor policy, process/thread runners, spawn setup, and chunked
  transform+post orchestration belong in `core.parallel`.
- Dagster resource definitions and compatibility imports belong in
  `core.resources`.

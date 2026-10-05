<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Star Schema Generator

Genererer en interaktiv HTML-visualisering af data warehouse-stjerneschemat fra lineage YAML-filer.

Se [Data Lineage Guide](../../../docs/implementation/data_lineage_guide.md) for komplet dokumentation af filformatet og hvad felterne styrer i visualiseringen.

## Kør

```bash
python etl/data_lineage/tools/star_schema/generate_star_schema.py
open etl/data_lineage/tools/star_schema/star_schema.html
```

Kræver Python 3.10+ og `pyyaml`.

## Test

```bash
pytest etl/data_lineage/tools/star_schema/tests/ -v
```

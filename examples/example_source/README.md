<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Example: run your first pipeline

This example loads made-up source data into the devcontainer SQL Server. Then it runs
one small domain through the three layers: DSI, DSA and DSO. The domain is the ILS user
dimension `dim.ILSBruger`.

All data in this example is fake. The names, codes and IDs start with `EX` or `Example`.
The data contains no CPR numbers.

## What the example contains

| File | Purpose |
|---|---|
| `example_source.sql` | Creates the database `example_source` with the tables `dbo.zz_oper` (6 users) and `dbo.sgh` (2 hospitals). |
| `load_example_source.py` | Runs `example_source.sql` against the devcontainer SQL Server. |

The run materializes these assets:

| Layer | Assets | Result |
|---|---|---|
| DSI | `ext_ils_zz_oper`, `ext_ils_sgh`, `arc_ils_zz_oper`, `arc_ils_sgh` | `ext.ils_*` and `arc.ils_*` in `staging` |
| DSA | `erp/stg_dim_ILSBruger`, `map_ILSBruger`, `dim_ILSBruger` | `stg.dim_ILSBruger`, `map.ILSBruger` and `dim.ILSBruger` in `staging` |
| DSO | `dso_sync_dim_ilsbruger` | `dim.ILSBruger` in `dso` |

The ILS assets read the database that `ILS_PROD_MSSQL_DATABASE` names. The example
points that variable to `example_source`. No pipeline code changes.

## Before you start

1. Open the repository in the devcontainer. See the [Quick start](../../README.md#quick-start).
2. Open a terminal in the devcontainer. Run all commands from the repository root.

## Steps

`just example` runs steps 1 to 4 in one command. To run the steps one by one, use the commands below.

1. Load the example data into SQL Server:

   ```bash
   python examples/example_source/load_example_source.py
   ```

2. Point the ILS source and the DSO linked server to the local databases. Dagster and
   `etl/core/resources.py` read the root `.env` and override shell variables, so set
   the values in `.env`:

   ```bash
   sed -i 's/^ILS_PROD_MSSQL_DATABASE=.*/ILS_PROD_MSSQL_DATABASE=example_source/; s/^MSSQL_LINKED_SERVER_DB=.*/MSSQL_LINKED_SERVER_DB=staging/' .env
   ```

3. Create the schemas and tables in `staging` and `dso`, and the linked server from `dso`
   to `staging`:

   ```bash
   dagster asset materialize -m definitions --select "*staging_infra_erp,staging_infra_dfm,*dso_infra_erp,dso_infra_linked_server"
   ```

4. Run the pipeline from the source to DSO:

   ```bash
   dagster asset materialize -m definitions --select "ext_ils_zz_oper,ext_ils_sgh,arc_ils_zz_oper,arc_ils_sgh,erp/stg_dim_ILSBruger,map_ILSBruger,dim_ILSBruger,dso_sync_dim_ilsbruger"
   ```

5. Read the result in the DSO database:

   ```bash
   python -c "import os, pymssql; c = pymssql.connect(server='mssql', user='sa', password=os.environ['MSSQL_SA_PASSWORD'], database='dso').cursor(); c.execute('SELECT dw_ek_ILSBruger, initialer, identifier_staff, active, primaer_sygehus_navn FROM dim.ILSBruger ORDER BY 1'); print(*c.fetchall(), sep=chr(10))"
   ```

Run step 3 before step 4. The extract assets do not depend on the infrastructure assets,
so one combined run starts them before the tables exist.

You can also run steps 3 and 4 in the Dagster UI: select the same assets and click
**Materialize selected**.

## Recorded run

This output comes from a new devcontainer stack with an empty SQL Server.

```text
$ python examples/example_source/load_example_source.py
example_source.dbo.sgh: 2 rows
example_source.dbo.zz_oper: 6 rows

$ dagster asset materialize -m definitions --select "*staging_infra_erp,staging_infra_dfm,*dso_infra_erp,dso_infra_linked_server"
...
RUN_SUCCESS - Finished execution of run for "__ASSET_JOB".

$ dagster asset materialize -m definitions --select "ext_ils_zz_oper,ext_ils_sgh,arc_ils_zz_oper,arc_ils_sgh,erp/stg_dim_ILSBruger,map_ILSBruger,dim_ILSBruger,dso_sync_dim_ilsbruger"
...
RUN_SUCCESS - Finished execution of run for "__ASSET_JOB".

$ python -c "..."
(-1, 'UKENDT', None, None, None)
(1, 'EXA', 'EXUSR01', True, 'Example Hospital North')
(2, 'EXB', 'EXUSR02', True, 'Example Hospital South')
(3, 'EXC', 'EXUSR03', False, 'Example Hospital North')
(4, 'EXD', 'EXU04A', True, 'Example Hospital South')
(5, 'EXE', 'EXUSR05', True, None)
```

Step 3 took 53 seconds and step 4 took 34 seconds. The SQL Server ran under emulation on
an arm64 host.

Row counts after the run:

| Table | Rows |
|---|---|
| `staging.arc.ils_zz_oper` | 6 |
| `staging.arc.ils_sgh` | 2 |
| `staging.stg.dim_ILSBruger` | 5 |
| `staging.map.ILSBruger` | 6 |
| `dso.dim.ILSBruger` | 6 |

## How to read the result

- Row `-1` is the unknown member that the table DDL inserts.
- The user without initials (`EX-OPER-0006`) is not in the dimension. The grain is the
  initials.
- The long staff code `XX-EXU04A` becomes the short code `EXU04A` in `identifier_staff`.
- The inactive user `EXC` stays in the dimension with `active = False`.
- The user `EXE` has the hospital code `EX99`. `dbo.sgh` has no such code, so the
  hospital name is empty.

## Run the example again

Run step 1 again to restore the source data. Then run step 4. To use the `ils` database
again, set `ILS_PROD_MSSQL_DATABASE=ils` in `.env`.

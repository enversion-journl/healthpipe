<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Configuration reference

The release build generates this page from [`.env.template`](../../.env.template). The next release overwrites changes to this page.

Copy `.env.template` to `.env`. Then change the values for your environment.

The Default column shows the value in the template. For a password, a token or a secret, the column shows "set a value" and not the template value. "not set" shows a variable that the template has only as a comment.

## Root .env

Root .env — copy this file: `cp .env.template .env`

This file is injected into the devcontainer / fhir-server containers via `env_file: ../.env` in .devcontainer/docker-compose.yml. It supplies the APPLICATION connection variables (the Dagster pipelines read these).

NOTE: this file does NOT feed `${VAR:-default}` interpolation in docker-compose.yml. Compose interpolates from `.devcontainer/.env` (the devcontainer's project dir) or the shell. The DB containers therefore run on the docker-compose defaults unless you override them in `.devcontainer/.env` (see `.devcontainer/.env.template`). The values below mirror those defaults so `cp .env.template .env` yields a coherent stack (aside from AIDBOX_LICENSE).

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_USER` | `postgres_user` | Postgres (Dagster metadata store) |
| `POSTGRES_PASSWORD` | set a value | Postgres (Dagster metadata store) |
| `POSTGRES_DB` | `postgres_db` | Postgres (Dagster metadata store) |
| `FHIR_SERVER_DB_USER` | `postgres_user` | FHIR server database (Aidbox backing Postgres) |
| `FHIR_SERVER_DB_PASSWORD` | set a value | FHIR server database (Aidbox backing Postgres) |
| `FHIR_SERVER_DB_DATABASE` | `aidbox` | FHIR server database (Aidbox backing Postgres) |
| `AIDBOX_CLIENT_SECRET` | set a value | Aidbox FHIR server |
| `AIDBOX_ADMIN_PASSWORD` | set a value | Aidbox FHIR server |
| `MSSQL_USERNAME` | `sa` | MSSQL container |
| `MSSQL_SA_PASSWORD` | set a value | MSSQL container |
| `MSSQL_STAGING_DB` | `staging` | MSSQL container |
| `MSSQL_DSO_DB` | `dso` | MSSQL container |
| `MSSQL_ILS_DB` | `ils` | MSSQL container |
| `MSSQL_COSMIC_DB` | `cosmic` | MSSQL container |
| `MSSQL_MYSUPPLY_DB` | `mysupply` | MSSQL container |
| `MSSQL_SYDAX_DB` | `sydax` | MSSQL container |
| `ORACLE_PASSWORD` | set a value | Oracle IFDB container |
| `ORACLE_DATABASE` | `ORACLE_DB` | Oracle IFDB container |
| `APP_USER` | `myuser` | Oracle IFDB container |
| `APP_USER_PASSWORD` | set a value | Oracle IFDB container |

## Application connection variables

| Variable | Default | Description |
|---|---|---|
| `MSSQL_SOURCE_STAGING_HOST` | `mssql` | Staging Database (DSI/DSA Layer) |
| `MSSQL_SOURCE_STAGING_PORT` | `1433` | Staging Database (DSI/DSA Layer) |
| `MSSQL_SOURCE_STAGING_DB` | `staging` | Staging Database (DSI/DSA Layer) |
| `MSSQL_SOURCE_STAGING_USER` | `sa` | Staging Database (DSI/DSA Layer) |
| `MSSQL_SOURCE_STAGING_PASSWORD` | set a value | Staging Database (DSI/DSA Layer) |
| `MSSQL_TARGET_STAGING_HOST` | `mssql` | Staging database that the pipelines write to. |
| `MSSQL_TARGET_STAGING_PORT` | `1433` | Staging database that the pipelines write to. |
| `MSSQL_TARGET_STAGING_DB` | `staging` | Staging database that the pipelines write to. |
| `MSSQL_TARGET_STAGING_USER` | `sa` | Staging database that the pipelines write to. |
| `MSSQL_TARGET_STAGING_PASSWORD` | set a value | Staging database that the pipelines write to. |
| `MSSQL_SOURCE_DSO_HOST` | `mssql` | DSO Database (Reporting Layer) - separate database on same server |
| `MSSQL_SOURCE_DSO_PORT` | `1433` | DSO Database (Reporting Layer) - separate database on same server |
| `MSSQL_SOURCE_DSO_DB` | `dso` | DSO Database (Reporting Layer) - separate database on same server |
| `MSSQL_SOURCE_DSO_USER` | `sa` | DSO Database (Reporting Layer) - separate database on same server |
| `MSSQL_SOURCE_DSO_PASSWORD` | set a value | DSO Database (Reporting Layer) - separate database on same server |
| `MSSQL_TARGET_DSO_HOST` | `mssql` | DSO database that the pipelines write to. |
| `MSSQL_TARGET_DSO_PORT` | `1433` | DSO database that the pipelines write to. |
| `MSSQL_TARGET_DSO_DB` | `dso` | DSO database that the pipelines write to. |
| `MSSQL_TARGET_DSO_USER` | `sa` | DSO database that the pipelines write to. |
| `MSSQL_TARGET_DSO_PASSWORD` | set a value | DSO database that the pipelines write to. |
| `MYSUPPLY_PROD_MSSQL_SERVER` | `mssql` | Database Configuration |
| `MYSUPPLY_PROD_MSSQL_USERNAME` | `sa` | Database Configuration |
| `MYSUPPLY_PROD_MSSQL_PASSWORD` | set a value | Database Configuration |
| `MYSUPPLY_PROD_MSSQL_DATABASE` | `mysupply` | Database Configuration |
| `MYSUPPLY_PROD_MSSQL_SCHEMA` | `dbo` | Database Configuration |
| `MYSUPPLY_PROD_MSSQL_PORT` | `1433` | Database Configuration |
| `SYDAX_PROD_MSSQL_USERNAME` | `sa` | Sydax database + 2 different schemas |
| `SYDAX_PROD_MSSQL_PASSWORD` | set a value | Sydax database + 2 different schemas |
| `SYDAX_PROD_MSSQL_SERVER` | `mssql` | Sydax database + 2 different schemas |
| `SYDAX_PROD_MSSQL_DATABASE` | `sydax` | Sydax database + 2 different schemas |
| `SYDAX_PROD_MSSQL_SCHEMA_FINANS` | `Finans` | Sydax database + 2 different schemas |
| `SYDAX_PROD_MSSQL_SCHEMA_KREDITOR` | `Kreditor` | Sydax database + 2 different schemas |
| `SYDAX_PROD_MSSQL_PORT` | `1433` | Sydax database + 2 different schemas |
| `ILS_PROD_MSSQL_SERVER` | `mssql` | ILS Database |
| `ILS_PROD_MSSQL_USERNAME` | `sa` | ILS Database |
| `ILS_PROD_MSSQL_PASSWORD` | set a value | ILS Database |
| `ILS_PROD_MSSQL_SCHEMA` | `dbo` | ILS Database |
| `ILS_PROD_MSSQL_DATABASE` | `ils` | ILS Database |
| `ILS_PROD_MSSQL_PORT` | `1433` | ILS Database |
| `SPEND_KMD_MSSQL_SERVER` | `mssql` | KMD Spend UdData (Azure Synapse) |
| `SPEND_KMD_MSSQL_USER` | `sa` | KMD Spend UdData (Azure Synapse) |
| `SPEND_KMD_MSSQL_PASSWORD` | set a value | KMD Spend UdData (Azure Synapse) |
| `SPEND_KMD_MSSQL_DATABASE` | `kmd_uddata` | KMD Spend UdData (Azure Synapse) |
| `SPEND_KMD_MSSQL_PORT` | `1433` | KMD Spend UdData (Azure Synapse) |
| `MAXMANUS_PROD_MSSQL_SERVER` | `mssql` | MaxManus Database |
| `MAXMANUS_PROD_MSSQL_USERNAME` | `sa` | MaxManus Database |
| `MAXMANUS_PROD_MSSQL_PASSWORD` | set a value | MaxManus Database |
| `MAXMANUS_PROD_MSSQL_DATABASE` | `maxmanus` | MaxManus Database |
| `MAXMANUS_PROD_MSSQL_PORT` | `1433` | MaxManus Database |
| `SIMA_PROD_MSSQL_SERVER` | `mssql` | SIMA Database |
| `SIMA_PROD_MSSQL_USERNAME` | `sa` | SIMA Database |
| `SIMA_PROD_MSSQL_PASSWORD` | set a value | SIMA Database |
| `SIMA_PROD_MSSQL_DATABASE` | `sima` | SIMA Database |
| `SIMA_PROD_MSSQL_PORT` | `1433` | SIMA Database |
| `SIMA_ARKIV_PROD_MSSQL_SERVER` | `mssql` | SIMA Arkiv Database |
| `SIMA_ARKIV_PROD_MSSQL_USERNAME` | `sa` | SIMA Arkiv Database |
| `SIMA_ARKIV_PROD_MSSQL_PASSWORD` | set a value | SIMA Arkiv Database |
| `SIMA_ARKIV_PROD_MSSQL_DATABASE` | `sima_arkiv` | SIMA Arkiv Database |
| `SIMA_ARKIV_PROD_MSSQL_PORT` | `1433` | SIMA Arkiv Database |
| `PATOLOGI_PROD_MSSQL_SERVER` | `mssql` | Patologi Database |
| `PATOLOGI_PROD_MSSQL_USERNAME` | `sa` | Patologi Database |
| `PATOLOGI_PROD_MSSQL_PASSWORD` | set a value | Patologi Database |
| `PATOLOGI_PROD_MSSQL_DATABASE` | `patologi` | Patologi Database |
| `PATOLOGI_PROD_MSSQL_PORT` | `1433` | Patologi Database |
| `COSMIC_PROD_MSSQL_SERVER` | `mssql` | Cosmic Database |
| `COSMIC_PROD_MSSQL_USERNAME` | `sa` | Cosmic Database |
| `COSMIC_PROD_MSSQL_PASSWORD` | set a value | Cosmic Database |
| `COSMIC_PROD_MSSQL_DATABASE` | `cosmic` | Cosmic Database |
| `COSMIC_PROD_MSSQL_SCHEMA` | `ca` | Cosmic Database |
| `COSMIC_PROD_MSSQL_PORT` | `1433` | Cosmic Database |
| `EPJI_ORACLE_SERVER` | `oracle` | EPJI DWH (location + location_identifier → SHAK-a-like), Oracle |
| `EPJI_ORACLE_USERNAME` | `myuser` | EPJI DWH (location + location_identifier → SHAK-a-like), Oracle |
| `EPJI_ORACLE_PASSWORD` | set a value | EPJI DWH (location + location_identifier → SHAK-a-like), Oracle |
| `EPJI_ORACLE_SCHEMA` | `EPJI` | EPJI DWH (location + location_identifier → SHAK-a-like), Oracle |
| `EPJI_ORACLE_DATABASE` | `ORACLE_DB` | EPJI DWH (location + location_identifier → SHAK-a-like), Oracle |
| `EPJI_ORACLE_PORT` | `1521` | EPJI DWH (location + location_identifier → SHAK-a-like), Oracle |
| `IFDB_PROD_ORACLE_SERVER` | `oracle` | IFDB Database |
| `IFDB_PROD_ORACLE_DATABASE` | `ORACLE_DB` | IFDB Database (SID) |
| `IFDB_PROD_ORACLE_USERNAME` | `myuser` | IFDB Database |
| `IFDB_PROD_ORACLE_PASSWORD` | set a value | IFDB Database |
| `IFDB_PROD_ORACLE_PORT` | `1521` | IFDB Database |
| `IFDB_PROD_ORACLE_SCHEMA` | `IFDB` | IFDB Database |
| `USE_REAL_DB_FOR_TESTS` | `true` | true: the tests connect to the devcontainer databases. false: the tests use mocks. |
| `USE_FHIR_API` | `true` | true: the FHIR client appends /fhir to FHIR_SERVER_URL. |
| `AIDBOX_LICENSE` | empty | Aidbox license JWT for the local FHIR server (fhir-server / aidboxone container). Optional: the FHIR server runs in dev mode (AIDBOX_DEV_MODE) without a license. Set it only if your Aidbox build/features require one; obtain it from the Aidbox portal. Injected into the container via env_file (../.env) in .devcontainer/docker-compose.yml — never hardcode it there. |
| `FHIR_SERVER_URL` | `http://fhir-server:8888` | FHIR server and the client that the pipelines use to write FHIR resources. |
| `FHIR_CLIENT_USERNAME` | `root` | FHIR server and the client that the pipelines use to write FHIR resources. |
| `FHIR_CLIENT_PASSWORD` | set a value | FHIR server and the client that the pipelines use to write FHIR resources. |
| `PG_FHIR_DATABASE` | `aidbox` | Database of the FHIR server, read directly by the pipelines. |
| `PG_FHIR_USER` | `postgres_user` | Database of the FHIR server, read directly by the pipelines. |
| `PG_FHIR_PASSWORD` | set a value | Database of the FHIR server, read directly by the pipelines. |
| `PG_FHIR_HOST` | `fhir-server-db` | In production, set these to the FHIR database server. |
| `PG_FHIR_PORT` | `5432` | In production, set these to the FHIR database server. |
| `DAGSTER_POSTGRES_USER` | `postgres_user` | Dagster configuration |
| `DAGSTER_POSTGRES_PASSWORD` | set a value | Dagster configuration |
| `DAGSTER_POSTGRES_DB` | `postgres_db` | Dagster configuration |
| `DAGSTER_POSTGRES_HOST` | `postgres` | Dagster configuration |
| `DAGSTER_POSTGRES_PORT` | `5432` | Dagster configuration |
| `DAGSTER_BETA_WARNINGS` | `0` | Dagster configuration |
| `MSSQL_LINKED_SERVER_HOST` | `mssql` | Linked Server configuration |
| `MSSQL_LINKED_SERVER_PORT` | `1433` | Linked Server configuration |
| `MSSQL_LINKED_SERVER_DB` | `DW` | Linked Server configuration |
| `MSSQL_LINKED_SERVER_USER` | `sa` | Linked Server configuration |
| `MSSQL_LINKED_SERVER_PASSWORD` | set a value | Linked Server configuration |
| `MSSQL_LINKED_SERVER_NAME` | `STAGING_LINK` | Linked Server configuration |
| `PORT` | `3000` | - |
| `LOG_LEVEL` | `info` | - |
| `PYTHONWARNINGS` | `ignore:.*observable_source_asset.*` | Hides the Dagster deprecation warning for observable_source_asset. |

## Organization-specific configuration (core/org_config.py)

| Variable | Default | Description |
|---|---|---|
| `FHIR_PUBLISHER` | `Your Organization` | Publisher stamped on published FHIR CodeSystem resources (the organization publishing the terminology, not the software). Override per deployment. |

## SilkeborgData

| Variable | Default | Description |
|---|---|---|
| `SILKEBORGDATA_BASE_DIR` | empty | Read-only SMB-mount, der KUN findes i et deployment, som leverer stien — samme /mnt/ftproot-mount som HAIBA og DRG bruger: SILKEBORGDATA_BASE_DIR=/mnt/ftproot/LØN/arkiv/E26 Lad den være tom lokalt: en tom, ikke-eksisterende ELLER tom mappe SKAL fejle højlydt, og den lokale devcontainer har ingen adgang til drevet. Sæt den kun lokalt, hvis du arbejder mod en manuel kopi. |
| `SILKEBORGDATA_FILE_ENCODING` | `cp865` | DOS-Nordic. Bekræftes på data ved første load; HAIBA på samme mount er cp1252, så de to må ALDRIG dele konstant. |

## SDS file-drop sources (DSI/csv_files)

SDS file-drop sources (DSI/csv_files) — read-only CIFS mounts, injected per allocation by the deployment. Deliberately no default in code AND no value here: an unset variable is a hard failure, so "share not mounted" can never look like "no new deliveries". The commented line documents the expected path only. The encoding has a real default in code (cp1252).

| Variable | Default | Description |
|---|---|---|
| `UDREDNINGSRET_BASE_DIR` | not set (example: `/mnt/ftproot/Udredningsret`) | Folder of the Udredningsret deliveries, and the encoding of the files. |
| `UDREDNINGSRET_FILE_ENCODING` | `cp1252` | Folder of the Udredningsret deliveries, and the encoding of the files. |

## OS2mo / Magenta GraphQL integration

The delivered codesystems (SOR, Okosted, ...) are uploaded to Magenta's OS2mo once a day as full-load CSV files (GraphQL upload_file), and OS2mo's organisation is read back into the warehouse.

In production, set these in the deployment, exactly like the database connections. The secret never lives in a repo. For the devcontainer they stay empty and OS2MO_ENABLED stays false, so no developer machine can write to Magenta by accident; the export and its CSV still build and can be run with dry_run=true.

OS2MO_BASE_URL is the OS2mo root WITHOUT the /graphql path. The version is pinned per the contract: the unversioned endpoint follows whatever OS2mo currently ships and is rejected by the client.

OS2MO_KEYCLOAK_REALM_URL is the Keycloak REALM root, e.g. https://<host>/auth/realms/mo. The token endpoint is read from that realm's .well-known/openid-configuration rather than assembled here — the /auth prefix moved in Keycloak 17, so the path is a version detail the server can answer for itself.

| Variable | Default | Description |
|---|---|---|
| `OS2MO_ENABLED` | `false` | true: the daily upload to OS2mo runs. |
| `OS2MO_BASE_URL` | empty | OS2mo root URL, GraphQL version, Keycloak realm and client credentials. |
| `OS2MO_GRAPHQL_VERSION` | `v30` | OS2mo root URL, GraphQL version, Keycloak realm and client credentials. |
| `OS2MO_KEYCLOAK_REALM_URL` | empty | OS2mo root URL, GraphQL version, Keycloak realm and client credentials. |
| `OS2MO_CLIENT_ID` | empty | OS2mo root URL, GraphQL version, Keycloak realm and client credentials. |
| `OS2MO_CLIENT_SECRET` | empty | OS2mo root URL, GraphQL version, Keycloak realm and client credentials. |
| `LPR3_BASE_DIR` | not set (example: `/mnt/ftproot/SDS_NGC_Sundhedsdata_LPR_Grunddata/Fra-SDS`) | Folder of the LPR3 deliveries from SDS, and the encoding of the files. |
| `LPR3_FILE_ENCODING` | `cp1252` | Folder of the LPR3 deliveries from SDS, and the encoding of the files. |

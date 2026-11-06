<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Data protection

This page is for a data protection reviewer, for example for a data protection impact
assessment (DPIA). It is also for the operator who runs HealthPipe with real data.

The page lists each store that can hold personal data. For each store, it gives the
data categories, how the data gets there and who can read it. It also describes the
CPR masking in the logs, retention, erasure and encryption.

The facts come from the code of this version. HealthPipe is software. The
organization that runs it decides the purpose, the access rules, the retention
periods and the erasure process. Section 7 lists these decisions.

## 1. Data categories

| Category | Examples in the code |
|---|---|
| Citizen identifiers | CPR number, earlier CPR number, eCPR numbers, source patient IDs, CPR numbers of the mother and the father (`dim.Borger`, `ref.PatientToBorger`) |
| Citizen master data | Names, birth date, gender, marital status, citizenship, addresses with address history, general practitioner, the address protection flag (`dim.Borger`) |
| Health data | Contacts, diagnoses, procedures, DRG groups, observations, questionnaire responses, care plans, appointments and document references (the FHIR resources in `etl/DSA/FHIR/`) |
| Employee data | AD user names (`sAMAccountName`) and their roles and organizations (`sec.*`). Payroll employment rows with CPR number, name and address (`ext.silkeborgdata_e26_pers`). User dimensions of source systems, for example `dim.ILSBruger` |

The CPR number is a national identifier. Diagnoses and other health data are special
categories of personal data. Treat each store below as personal data, unless the
table says otherwise.

## 2. Stores

### 2.1 Files and source deliveries

| Store | Personal data | How the data gets there | Who can read it |
|---|---|---|---|
| Delivery shares. Each file source reads the folder in its `<SOURCE>_BASE_DIR`, for example `CPR_BASE_DIR`, `DRG_BASE_DIR` and `LPR3_BASE_DIR` ([configuration reference](configuration.md)). | Raw CPR numbers, names, addresses, diagnoses and payroll data, as the supplier delivers them. | The supplier, for example SDS, puts the files on the share. `FileShareResource` reads the folder. The DRG and HAIBA modules describe their shares as read-only mounts. | The users and the hosts that have access to the share. The share owner controls this access. |

HealthPipe reads the delivery files. It does not move or delete them. The supplier
controls how long the files stay on the share.

### 2.2 SQL Server: the `staging` database

| Store | Personal data | How the data gets there | Who can read it |
|---|---|---|---|
| `ext.*` (landing tables) | All the columns of the source table, 1:1. | DSI drops and creates each `ext` table again on each run, and loads the latest extract. | Each login with read permission on the schema. |
| `arc.*` (archive) | All the columns of the source table, 1:1, for all the rows that the pipeline has seen. | The archive asset merges `ext` into `arc` (`etl/core/archive_merge.py`). See section 5 for the history rules. | Each login with read permission on the schema. |
| `stg.*`, `map.*`, `bridge.*` | Intermediate rows of dimensions and facts. Fact rows carry the key `dw_ek_Borger`. Several DSA assets change the CPR number to this key in `pre_stg`. Some keys contain the CPR number. For example, the `clean_code` of the HAIBA case tables in `map.*` contains the CPR number. | DSA builds these tables from `arc.*`. | Each login with read permission on the schema. |
| `ref.*` | `ref.PatientToBorger` maps the source patient ID, the CPR number and the eCPR numbers to `dw_ek_Borger`. | DSA builds the table from `fhir.Patient`. | Each login with read permission on the schema. |
| `fhir.*` (SQL on FHIR tables) | Flat tables of the FHIR resources, for example the CPR number and the names of a patient, and the diagnosis codes of a condition. | DSA reads the FHIR server and writes the rows. | Each login with read permission on the schema. |
| `dim.*`, `fct.*`, `dm.*` | `dim.Borger` holds the citizen identifiers and master data of section 1. It keeps one row for each address period. Facts and `dm.*` tables hold health events and the key `dw_ek_Borger`. | DSA builds the dimensions, the facts and the `dm.*` tables. | Each login with read permission on the schema. Section 3 describes the masked views. |
| `sec.*` | AD user names with their roles, populations and organizations. The history tables `sec.RolleDomaeneHistorik` and `sec.SORTildelingHistorik` exist only on staging. | The nightly `security_daily_job` reads the HealthPipe AD groups and loads the tables. | Each login with read permission on the schema. |
| `meta.CPRArkivScope` | The CPR numbers of the persons in the latest CPR register delivery. | The CPR register archive drops and creates the table on each run. | Each login with read permission on the schema. |
| `meta.DFMRunLog` | Error texts of failed runs. `DfmManager.fail_dfm_run` masks CPR numbers in the text. Names or addresses in an error text stay. | The DFM framework writes one row for each run. | Each login with read permission on the schema. |
| `dqa.Results`, `dqa.RemediationQueue` | Up to 20 example keys for each failed check. The code masks the CPR numbers in these keys. The remediation queue holds the source business key, for example a patient ID. | The data quality checks write the rows. | Each login with read permission on the schema. |

### 2.3 SQL Server: the `dso` database

| Store | Personal data | How the data gets there | Who can read it |
|---|---|---|---|
| `dim.*`, `fct.*`, `fhir.*`, `ref.*`, `sec.*` and the other synced tables | The same data as the source tables on staging, for example `dim.Borger` and `ref.PatientToBorger`. The `map.*` tables and the `sec` history tables stay on staging. | The DSO sync copies the tables from staging over a linked server (`etl/DSO/load/sync_configs.py`). | Each login with read permission on the schema. Readers of the `vw_dim` views and the datamart views see masked data (section 3). |

### 2.4 FHIR server

| Store | Personal data | How the data gets there | Who can read it |
|---|---|---|---|
| The FHIR server (Aidbox) and its PostgreSQL database | FHIR resources: patients with CPR numbers, names and addresses, and the health data of section 1. | DSA writes the resources through the FHIR API and directly into the PostgreSQL database. DSA also reads the database directly (`PG_FHIR_*`). | The FHIR client account (`FHIR_CLIENT_USERNAME`). Each account with access to the PostgreSQL database. |

### 2.5 Dagster

| Store | Personal data | How the data gets there | Who can read it |
|---|---|---|---|
| The Dagster PostgreSQL database: `event_log_storage`, `run_storage`, `schedule_storage` | Log messages, error texts, stack traces, asset metadata, asset check metadata and run configuration. Section 4 lists what the CPR masking does not cover. | Dagster writes the events of each run. | Each user of the Dagster webserver. Each account with access to the database. |
| The compute log files (`compute_logs.base_dir` in [`dagster.yaml`](../../.devcontainer/dagster_home/dagster.yaml)) | The stdout and the stderr of each step, without CPR masking. | `LocalComputeLogManager` writes one file for each step. | Each user of the Dagster webserver. Each user with file access on the host. |
| The asset storage (`$DAGSTER_HOME/storage`) | The return value of each asset, as a pickle file. For example, `extract_security` returns the AD user names and their groups. | The IO managers in `etl/definitions.py` write the files. | Each user with file access on the host. |
| Temporary spool files (the system temporary directory, or `IFDB_AKTIVITETSRESULTAT_SPOOL_DIR`) | Rows of `arc.ifdb_aktivitetsresultat`. | The `Aktivitetsresultat` asset spools the archive rows to disk during a step. The step removes the directory at the end. A killed process can leave the files. | Each user with file access on the host. |
| The identifier cache (`$DAGSTER_HOME/cache/identifier_caches`) | SQLite files that map an identifier system and value to a FHIR resource ID. | `CacheCoordinator` builds the files from the FHIR database. By default, a file is current for one hour (`IDENTIFIER_CACHE_TTL_SECONDS`). A rebuild replaces it. | Each user with file access on the host. |

### 2.6 Other files

| Store | Personal data | How the data gets there | Who can read it |
|---|---|---|---|
| AD export files (`etl/infrastructure/security/output/`, or the paths in `LDAP_HP_OUTPUT_*`) | AD user names, their groups, roles and organizations. | `run_export` writes JSONL and TXT files on each run of `extract_security`. The next run overwrites them. | Each user with file access on the host. |
| FHIR bundles that the server refused (`FHIR_FAILURES_DIR`, default `/tmp/fhir_failures`) | Complete FHIR bundles without masking. | `save_failing_bundle_to_disk` writes a bundle and the server response only when `FHIR_DEBUG_SAVE_FAILING_BUNDLE=1`. The default is off. | Each user with file access on the host. |

Do not set `FHIR_DEBUG_SAVE_FAILING_BUNDLE=1` in an environment with real data.

The OS2mo export sends organization code systems (SOR, Okosted and others). It does
not send citizen data.

### 2.7 Backups

The backups of each store above contain the same personal data. The operations guide
describes a backup of the Dagster database and the compute logs
([operations guide](operations.md), section 4).

## 3. Access control

The repository does not create database logins, database roles or `GRANT`
statements. The operator gives the permissions. The default user of each SQL Server
connection in the [configuration reference](configuration.md) is `sa`. Give the
pipeline accounts the minimum permissions that they need.

The security model controls reads through views. It does not control reads of the
tables. [The DPA security model](implementation/sikkerhedsmodel.md) (in Danish)
describes it:

- The nightly job `security_daily_job` loads the HealthPipe AD groups into
  `sec.RolleDomaene` and `sec.RBOrganisation`. AD is the only source of the
  assignments. The exception is the fixed test users, which load only when
  `SECURITY_SEED_ENABLED=true`.
- A user has one role in each security domain. Role `U` sees all rows and columns.
  Role `K` sees personal data only for citizens of the user's organizations. Role
  `P` also sees personal data for the citizens of the user's population. A user
  without a role in the domain sees no rows.
- The DPA functions `sec.HentBrugerRolleDomaene`, `sec.HentRBOrganisation` and
  `sec.HentDPASikkerhed` apply these rules for the current login (`SUSER_SNAME()`).
- The views that call the DPA functions on the datamarts are not in this repository.
- `vw_dim.Borger` on DSO always masks the names. It shows `dw_ek_Borger` in the `CPR`
  column. `vw_dim.Leverandoervare` and `vw_dim.Fakturalinje` mask the free-text
  columns, unless the user has role `U` in `DPA_ERP`. A user without a role in
  `DPA_ERP` sees no rows in these two views.

A login with `SELECT` permission on `dim.Borger`, `arc.*` or `fhir.*` reads all the
columns. Give the readers access to the views only.

The open-source Dagster webserver has no user login. Each user who can reach the
webserver can read the event log, the asset metadata and the compute logs. Limit the
network access to the webserver.

## 4. CPR masking in the logs

[`etl/core/logging_redaction.py`](../core/logging_redaction.py) masks CPR numbers in
log records. The [operations guide](operations.md), section 1.3, describes how. The
pattern is `\b\d{6}-?\d{4}\b`, and each match becomes `XXXXXX-XXXX`.

The masking is a safety net, not a control. These limits apply:

| Limit | Evidence |
|---|---|
| It masks only CPR numbers. It does not mask names or addresses. | `logging_redaction.py`, module docstring |
| It does not mask `print` and other raw writes to stdout or stderr. The compute log files keep these writes. For example, the parallel extract workers print their messages. | `logging_redaction.py`, module docstring. `_ProcessLogger` in `etl/DSI/connectors/mssql/parallel_extract.py` |
| It does not mask asset metadata. The event log and the Dagster UI show the metadata as written. | The strict xfail test `_metadata` in [`test_logging_redaction.py`](../tests/unit/test_logging_redaction.py) |
| It does not mask the records of a logger in `managed_python_loggers`. The default `dagster.yaml` sets no managed loggers. If you add one, its records go to the event log without the mask. | The strict xfail test `_named_logger` in [`test_logging_redaction.py`](../tests/unit/test_logging_redaction.py) |
| The filter masks the rendered message only. It does not mask the traceback that a module logger adds with `exc_info`. | `CprRedactingFilter.filter` |
| The filter attaches to the handlers that exist when `install_pii_redaction()` runs. A handler that code adds later has no filter. | `install_pii_redaction` |
| If a record cannot render its message, the filter lets the record through without the mask. | `CprRedactingFilter.filter` |
| It does not mask the run configuration or the run tags. | The code masks only log records and the errors of Dagster events. |
| The pattern needs 10 digits, with an optional hyphen. It does not mask a CPR number with a space or another separator. It also masks other numbers with 10 digits. | `etl/DSA/FHIR/utils/gdpr_masking.py` |

A strict xfail test fails when the gap closes. Thus, the two strict xfail tests show
that the two gaps exist in this version.

The default `dagster.yaml` sets `python_log_level` to `DEBUG`. Set a higher level in
production, for example `INFO`.

Do not write personal data to the logs or to the asset metadata. Show counts, file
names and technical keys.

## 5. Retention and erasure

### 5.1 What the code does

- **`ext.*`**: DSI drops and creates each `ext` table again on each run. The table
  keeps the latest extract until the next run.
- **`arc.*`**: the archive is permanent. No job deletes rows because of their age.
  The MERGE updates a row in place when a value changes. A row that leaves the source
  stays. With delete detection, the MERGE sets `dw_is_deleted = 1`. The `insert_only`
  strategy adds the rows of each load. The CPR register archive is different: it
  replaces the rows of each person in a delivery (`erstat_pr_person` in
  `etl/DSI/csv_files/cpr_archive.py`).
- **`dim.Borger`**: the table keeps one row for each address period (type 2). The
  other columns keep the latest value.
- **`sec.*` history**: `sec.RolleDomaeneHistorik` and `sec.SORTildelingHistorik` keep
  each closed row with its validity period.
- **FHIR server**: when a row leaves the source, the FHIR assets mark the resource
  with a deletion extension (`etl/core/fhir/deletion.py`,
  `etl/utils/fhir/is_deleted_extension.py`). The resource stays in the database.
- **Dagster**: `dagster.yaml` sets no retention for runs, event logs or compute logs.
  Dagster does not delete them by itself.
- **Identifier cache**: a rebuild replaces a cache file. No job deletes old files on
  a schedule.

### 5.2 What the code does not do

- It has no retention period for a store.
- It has no purge job.
- It has no function that erases one person from all stores.
- It does not anonymize data after a period.

### 5.3 What the operator must define

Define a retention period for each store in section 2. Define an erasure process for
one person.

CAUTION: The next run loads a person again if the source still holds the person. Erase
the person in the source first, or stop the load for that person.

To erase one person, examine these stores:

1. `arc.*` and `ext.*` on staging. Use the CPR number and the source keys.
2. `ref.PatientToBorger` to find `dw_ek_Borger`.
3. `dim.Borger`, the facts with the same `dw_ek_Borger`, `fhir.*` and `ref.*`, on
   staging and on DSO.
4. The `clean_code` of the `map.*` tables that contain the CPR number.
5. The FHIR server, and also the earlier versions of each resource that the server
   keeps.
6. The Dagster event log, the compute log files, the asset storage and the caches.
7. The backups.

## 6. Encryption

### 6.1 In transit

| Connection | Current behavior |
|---|---|
| SQL Server through `mssql-python` (staging, DSO and most SQL Server sources) | `build_mssql_conn_str` in [`etl/core/resources.py`](../core/resources.py) always sets `Encrypt=yes;TrustServerCertificate=yes`. The driver encrypts the traffic, but it does not verify the server certificate. No configuration variable changes this. |
| SQL Server through `pymssql` | The sources that use `_PymssqlSourceResource` connect with `pymssql`. The `populate_security` asset also uses `pymssql` to load `sec.*` into staging. The code sets no encryption option. The FreeTDS settings of the driver control the encryption. |
| Active Directory (LDAP) | `etl/infrastructure/security/export_healthpipe_ad_groups.py` and the script `etl/scripts/export_ad_groups_members.py` use `Tls(validate=ssl.CERT_NONE)`. With `LDAP_USE_SSL=true`, the connection uses TLS without a certificate check. With `LDAP_USE_SSL=false`, the connection does not use TLS. |
| Oracle sources | The code builds a TCP address with `oracledb.makedsn`. It does not configure TLS (TCPS). |
| PostgreSQL of the FHIR server | The code sets no `sslmode`. libpq then uses its default, `prefer`, which does not verify the certificate. You can set `PGSSLMODE` in the environment of the code server. |
| FHIR API | The scheme of `FHIR_SERVER_URL` sets TLS. The devcontainer default is `http`. The client verifies the certificate of an `https` URL. |
| Linked server from DSO to staging | The provider string comes from `MSSQL_LINKED_SERVER_PROVSTR`. The code adds only the user. |
| Dagster PostgreSQL | `dagster.yaml` sets no TLS options. |

Use a trusted network between the code server and these servers. Record this risk in
the assessment.

### 6.2 At rest

The repository does not configure encryption at rest for a store. Use the features
of your platform. Examples are SQL Server Transparent Data Encryption and encrypted
volumes for the Dagster home directory and the PostgreSQL databases.

## 7. Decisions for the operator

Decide and record these items before you load real data:

- The retention period of each store in section 2.
- The erasure process for one person (section 5.3).
- The database permissions of each pipeline account and each reader (section 3).
- The network access to the Dagster webserver and to the FHIR server.
- The file permissions of the Dagster home directory, the compute logs and the
  temporary directory.
- The log level in production (section 4).
- The encryption at rest (section 6.2) and the network protection (section 6.1).
- The protection and the retention of the backups.

## 8. Leak guards in the repository

These guards help to keep personal data out of the repository and out of issues. They
do not protect an installation in operation.

- The pre-commit hook and the `secret-scan` workflow run gitleaks with
  [`.gitleaks.toml`](../../.gitleaks.toml). The rule `danish-cpr` finds CPR numbers.
- The rule `danish-cpr` does not scan Markdown files, `test/`, `tests/` and
  `notebooks/` folders, or `.ipynb` files. These paths hold synthetic CPR numbers. Thus, a real CPR
  number in one of these files passes the scan.
- The question form tells the user not to add personal data. The log field of the
  bug report form tells the user to remove credentials, connection strings and
  secrets. It does not name personal data.
- [SECURITY.md](../../SECURITY.md) tells the reporter of a vulnerability not to send
  patient data or CPR numbers.

Use only synthetic CPR numbers in code, tests, documentation and issues. Before you
paste a log into an issue, remove the CPR numbers, the names and all other patient
data.

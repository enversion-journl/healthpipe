<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Discovery Report: Organization-Specific References in dagster_etl/

**User Story:** #27070 - Fjern eller abstrahér alle organisationsspecifikke referencer i koden
**Date:** 2026-06-18
**Scope:** `dagster_etl/` only (top-level `infrastructure/` and `.devcontainer/` excluded)

---

## Findings

### 1. Autofix sensor (Removed)

`sensors/autofix_sensor.py` held the only hardcoded internal endpoint
(`http://claude-agent-server.service.consul:8080`) plus an Azure DevOps repo URL/name.
The sensor was dormant — registered but never given `default_status=RUNNING`, so Dagster
kept it stopped — and it has been **removed entirely**, along with its `REPO_URL` /
`REPO_NAME` constants in `core/org_config.py` and its `AUTOFIX_SERVER_URL` /
`AUTOFIX_TRIGGER_SECRET` env vars. DFM base-failure alarming is unaffected:
`dfm_base_job_failure_sensor` covers it natively and independently.

### 2. Paths (Externalized)

| File | Old value | New env var |
|------|-----------|-------------|
| `DSI/csv_files/sor_register.py` | `/workspaces/RegionSyddanmark/data/sor_raw` | `DATA_DIR` + subpath |
| `DSA/SQL/staging/stg_organisation_sor.py` | `/workspaces/RegionSyddanmark/data/sor_raw` | `DATA_DIR` + subpath |

### 3. FHIR CodeSystem `publisher` (Externalized)

The `publisher` element on published CodeSystem resources is the *organization*
publishing the terminology (not the software). It is now read from `FHIR_PUBLISHER`
(default `Your Organization`, a generic placeholder each deployment should override) and the
hardcoded org literal was removed from the CodeSystem YAMLs and `sfi/catalog.py`:

| File | Change |
|------|--------|
| `core/org_config.py` | New `FHIR_PUBLISHER = os.getenv("FHIR_PUBLISHER", "Your Organization")` |
| `infrastructure/assets/fhir_codesystem_{sor,sor_complete,sks,sks_complete}.py` | `cs_meta.get("publisher", FHIR_PUBLISHER)` |
| `DSA/CodeSystem/sfi/catalog.py` | `"publisher": FHIR_PUBLISHER` |
| `DSA/CodeSystem/{sor,lager,ean,okosted,rekvirent,sfi}/codesystem.yml` | `publisher:` line removed (loader falls back to `FHIR_PUBLISHER`) |

`DSA/CodeSystem/sks/codesystem.yml` keeps `publisher: Sundhedsdatastyrelsen` — the
national authority, not org-specific (the sks loaders read it from the YAML).

### 4. FHIR Data-Contract URLs (Org-Binding, Deferred — NOT Externalized)

These embed the organization in **published FHIR identifiers**. They are real data
contracts: changing them re-tags every emitted resource and breaks consumers of
already-published data. They are deliberately left inlined; changing any requires a
coordinated data re-tag (no back-compat shims) and should be its own user story.

| File | Pattern | Count | Org-binding token |
|------|---------|-------|-------------------|
| `utils/fhir/fhir_extensions.py` | `http://regionsyddanmark.dk/fhir/StructureDefinition/...` | ~30 | `regionsyddanmark.dk` |
| `DSA/CodeSystem/*/codesystem.yml` (`url:`) | `https://regionsyd.dk/...` | ~5 | `regionsyd.dk` |
| `DSA/FHIR/utils/fhir_utils.py` | `urn:enversion:source-system`, `urn:enversion:source` | 3 | `enversion` |

CodeSystem YAML `description:` fields also mention "Region Syddanmark" (deployment
context, not branding) and are left unchanged.

### 5. National Danish Standards (No Action)

National constants, not organization-specific:

- `sundhedsdatastyrelsen.dk` / `sor-filer.sundhedsdata.dk` (SOR/SKS register URLs)
- `hl7.dk/fhir/core` (Danish HL7 FHIR profiles)
- `urn:oid:1.2.208.176.*` (Danish healthcare OIDs)
- `publisher: Sundhedsdatastyrelsen` (sks CodeSystem)

### 6. Branding → Healthpipe (Renamed)

| File | Old | New |
|------|-----|-----|
| `pyproject.toml` | `name = "RegionSyddanmark"`, `description = "A Python project for RegionSyddanmark"` | `name = "healthpipe"`, Healthpipe description |
| `data_lineage/tools/star_schema/star_schema_template.html` | `<title>` / `<h1>` "Region Syddanmark" | "Healthpipe" |

(`star_schema.html` is a generated, untracked artifact — regenerated from the template.)

---

## Resolution

- **Externalized via `core/org_config.py` env vars:** `DATA_DIR`, `FHIR_PUBLISHER`.
- **Removed:** the dormant autofix sensor and all its config — the internal Consul
  endpoint, the Azure DevOps repo URL/name, and the `AUTOFIX_*` env vars. This was the
  only internal-infrastructure reference in the tree.
- **Renamed to Healthpipe:** package metadata + star-schema tool title.
- **Deliberately not changed (data contracts):** FHIR/CodeSystem namespace URLs and
  `urn:enversion:*` identifiers (§4) — these require a coordinated data re-tag and a
  dedicated story.

With the autofix sensor gone, no internal endpoint or org-bound repo identity remains
in the codebase. The acceptance criterion "works without access to internal
infrastructure" holds: the remaining org-specific values (`DATA_DIR`, `FHIR_PUBLISHER`)
are environment-overridable and default only to deployment-local conveniences.

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATA_DIR` | `/workspaces/healthpipe/data` | Local data persistence directory |
| `FHIR_PUBLISHER` | `Your Organization` | `publisher` on published CodeSystem resources |

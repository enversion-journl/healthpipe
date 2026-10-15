<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Shared Dimensions

**Cross-domain dimensions used by multiple ETL modules.**

Location: `DSA/SQL/dim/borger.py`, `DSA/SQL/ref/`

## Overview

These dimensions are "shared" because they're referenced by multiple data domains (FHIR, Datawarehouse, Analytics, etc.). They're implemented once in the shared_dimensions module and consumed by all downstream modules.

---

## Dimensions

### 1. Borger (Citizen) - Type 2 SCD

**Purpose**: Track patient demographics with full change history.

**Structure**:
- **ref.PatientToBorger**: Assigns durable `dw_ek_Borger` (IDENTITY)
- **ref.Patient_Address_To_Borger**: Assigns version-specific `dw_sk_Borger` (IDENTITY) per (patient x address-period)
- **dim.Borger**: Type 2 SCD with `dw_sk_Borger` (carried from ref, no local IDENTITY) + `dw_ek_Borger` (durable)

**Keys**:
- `dw_sk_Borger`: Version-specific surrogate key (PK, carried from ref.Patient_Address_To_Borger) - stable once issued
- `dw_ek_Borger`: Durable entity key from ref.PatientToBorger (IDENTITY) - stable across all T2 versions
- `patient_id_ifdb`: IFDB patient identifier for staging joins

**SCD Type 2 Fields**:
- `dw_valid_from`: Start of validity period
- `dw_valid_to`: End of validity period (9999-12-31 for current)
- `dw_is_current`: Flag indicating current version

**Current Implementation**:
- T2 scope is address fields only; all other attributes are Type 1 (overwritten)
- MERGE-based incremental loads with patient-scoped delta for address changes
- Soft-delete with threshold guards for safety

**Temporal Join Pattern** (for facts):
```sql
LEFT JOIN ref.PatientToBorger rpb
    ON rpb.patient_id = staging.patient_id

LEFT JOIN dim.Borger db
    ON db.dw_ek_Borger = rpb.dw_ek_Borger
   AND fact_event_date >= db.dw_valid_from
   AND fact_event_date < db.dw_valid_to
```

> **Note:** Organization is now handled separately in `DSA/SQL/organization/` (see `dim_organization`, `map_organization`).

---

## Asset Groups

### ref
- `ref_patient_borger`: Entity-mapping (dw_ek_Borger <-> patient_id) with MERGE + delta
- `ref_patient_address_borger`: Version-mapping (dw_sk_Borger <-> address period) with patient-scoped MERGE + delta. LEAD uses only live `fhir.Patient_Address` rows (`dw_is_deleted = 0`); retired periods remain in the table with `dw_is_deleted = 1`.

### shared_dimensions
- `dim_Borger`: Type 2 SCD for citizen demographics (MERGE, SK from ref.Patient_Address_To_Borger)

### shared_dimensions_dso_sync
- `dso_sync_dim_borger`: Sync dim.Borger staging → DSO

---

## Scheduling

Borger runs as part of `ifdb_dfm_daily_job` (06:00 CET), after `sof_patient_dfm` / `sof_patient_address_dfm` in the same run. Selection is by explicit asset keys so Organisation assets sharing the `ref` / `shared_dimensions_dso_sync` groups are not included.

Organisation remains on `organisation_cs_job` (`organization_daily_schedule`, 01:00).

---

## Usage by Other Modules

SharedDimensions are "foundational" — Borger is populated daily via the FHIR DFM SOF+DSO chain; Organisation via `organisation_cs_job`. Downstream domain ETL looks up FKs from these dims.

---

## Execution Order

```
1. FHIR DFM SOF → Borger (ifdb_dfm_daily_job)
   sof_patient_dfm / sof_patient_address_dfm
     → ref_patient_borger → ref_patient_address_borger
     → dim_Borger → dso_sync_dim_borger

2. Domain ETL (e.g., ERP)
   Staging
   Maps (domain-specific)
   Dimensions (domain-specific)
   Facts (lookup FKs from BOTH shared + domain dims)
```

---

## Future Enhancements

### Borger (Citizen)
1. **CDC Implementation**: Track demographic changes
   - Detect changes in name, address, status, etc.
   - Create new version with updated `dw_valid_from`/`dw_valid_to`
   - Set previous version `dw_is_current=0`

2. **Additional Attributes**:
   - Address history
   - Physician assignments
   - Insurance information

---

## Clean Architecture Compliance

- **Ref tables**: Assign DW_EK (entity) and DW_SK (version) via IDENTITY (source of truth)
- **Dimension tables**: Carry DW_SK from ref, add descriptive attributes via MERGE
- **Fact tables**: Lookup DW_SK from ref or dim (NEVER generate their own)
- **No circular dependencies**: SharedDimensions are foundational, depend only on fhir layer

---

## File Structure

```
DSA/SQL/ref/
    __init__.py                 # Module exports (ref_patient_borger, ref_patient_address_borger)
    patient_borger.py           # ref.PatientToBorger — entity-mapping (dw_ek_Borger)
    patient_address_borger.py   # ref.Patient_Address_To_Borger — version-mapping (dw_sk_Borger)

DSA/SQL/dim/
    borger.py                   # dim.Borger — T2 SCD (MERGE from FHIR + ref)

infrastructure/                 # Centralized infrastructure (DDL)
    assets/staging_infrastructure.py  # staging_infra_shared_dimensions + staging_infra_ref
    assets/dso_infrastructure.py      # dso_infra_shared_dimensions + dso_infra_ref
    ddl/tables/shared_dimensions/     # dim.Borger DDL (02_dims.sql)
    ddl/tables/ref/                   # ref.PatientToBorger + ref.Patient_Address_To_Borger DDL
```

> **NOTE:** Infrastructure has been centralized to `etl/infrastructure/`.
> Use `full_infrastructure_job` to create all DDL objects (staging + DSO).

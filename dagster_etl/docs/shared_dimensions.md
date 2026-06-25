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
- **ref.Patient_Borger**: Assigns durable `DW_EK_Borger` (IDENTITY)
- **ref.Patient_Address_Borger**: Assigns version-specific `DW_SK_Borger` (IDENTITY) per (patient x address-period)
- **dim.Borger**: Type 2 SCD with `DW_SK_Borger` (carried from ref, no local IDENTITY) + `DW_EK_Borger` (durable)

**Keys**:
- `DW_SK_Borger`: Version-specific surrogate key (PK, carried from ref.Patient_Address_Borger) - stable once issued
- `DW_EK_Borger`: Durable entity key from ref.Patient_Borger (IDENTITY) - stable across all T2 versions
- `PATIENT_ID`: IFDB patient identifier for staging joins

**SCD Type 2 Fields**:
- `DW_ValidFrom`: Start of validity period
- `DW_ValidTo`: End of validity period (9999-12-31 for current)
- `DW_IsCurrent`: Flag indicating current version

**Current Implementation**:
- T2 scope is address fields only; all other attributes are Type 1 (overwritten)
- MERGE-based incremental loads with patient-scoped delta for address changes
- Soft-delete with threshold guards for safety

**Temporal Join Pattern** (for facts):
```sql
LEFT JOIN ref.Patient_Borger rpb
    ON rpb.patient_id = staging.patient_id

LEFT JOIN dim.Borger db
    ON db.DW_EK_Borger = rpb.DW_EK_Borger
   AND fact_event_date >= db.DW_ValidFrom
   AND fact_event_date < db.DW_ValidTo
```

> **Note:** Organization is now handled separately in `DSA/SQL/organization/` (see `dim_organization`, `map_organization`).

---

## Asset Groups

### shared_dimensions_ref
- `ref_patient_borger`: Entity-mapping (DW_EK_Borger <-> patient_id) with MERGE + delta
- `ref_patient_address_borger`: Version-mapping (DW_SK_Borger <-> address period) with patient-scoped MERGE + delta

### shared_dimensions
- `dim_borger`: Type 2 SCD for citizen demographics (MERGE, SK from ref.Patient_Address_Borger)

---

## Usage by Other Modules

SharedDimensions are "foundational" - they're expected to be populated before any domain-specific ETL runs. The daily schedule ensures SharedDimensions run first, then domain-specific ETL.

---

## Execution Order

```
1. SharedDimensions (ref + dims)
   ref_patient_borger -> ref_patient_address_borger -> dim_borger

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
   - Create new version with updated `DW_ValidFrom`/`DW_ValidTo`
   - Set previous version `DW_IsCurrent=0`

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
    patient_borger.py           # ref.Patient_Borger — entity-mapping (DW_EK_Borger)
    patient_address_borger.py   # ref.Patient_Address_Borger — version-mapping (DW_SK_Borger)

DSA/SQL/dim/
    borger.py                   # dim.Borger — T2 SCD (MERGE from FHIR + ref)

infrastructure/                 # Centralized infrastructure (DDL)
    assets/staging_infrastructure.py  # staging_infra_shared_dimensions + staging_infra_ref
    assets/dso_infrastructure.py      # dso_infra_shared_dimensions + dso_infra_ref
    ddl/tables/shared_dimensions/     # dim.Borger DDL (02_dims.sql)
    ddl/tables/ref/                   # ref.Patient_Borger + ref.Patient_Address_Borger DDL
```

> **NOTE:** Infrastructure has been centralized to `dagster_etl/infrastructure/`.
> Use `full_infrastructure_job` to create all DDL objects (staging + DSO).

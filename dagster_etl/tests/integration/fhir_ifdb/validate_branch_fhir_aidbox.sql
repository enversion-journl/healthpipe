-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

/*
Branch QA script for IFDB FHIR resources in Aidbox DB.

Purpose:
- Validate current branch output semantics for Observation, Procedure, and Condition
- Check canonical deletion model (urn:fhir:isDeleted + status/verification mapping)
- Check reference quality (subject/encounter/episode)
- Check branch-specific enrichments (AKTIVITETSRESULTAT XML/UDF extensions)
- Detect duplicate source identifiers

How to run:
- Paste this script in Aidbox DB Console (SQL console), OR
- Use REST Console with POST /$sql and Content-Type: text/yaml.

Important:
- Do NOT use jsonb "?" operator in Aidbox REST /$sql context (it may be treated as JDBC placeholder).
- This script uses jsonb_exists(...) instead.
*/

-- ============================================================================
-- 0) Observation quality model
-- ============================================================================

drop view if exists tmp_obs_qc;
create temporary view tmp_obs_qc as
select
  o.id,
  o.txid,
  o.cts,
  o.ts,
  o.resource,
  o.resource->>'status' as status,
  case
    when exists (
      select 1
      from jsonb_array_elements(coalesce(o.resource->'identifier','[]'::jsonb)) i
      where i->>'system' = 'urn:source:ifdb:aktivitetsresultat'
    ) then 'aktivitetsresultat'
    when exists (
      select 1
      from jsonb_array_elements(coalesce(o.resource->'identifier','[]'::jsonb)) i
      where i->>'system' = 'urn:source:ifdb:eventmarker'
    ) then 'eventmarker'
    else 'other'
  end as source,
  exists (
    select 1
    from jsonb_array_elements(coalesce(o.resource->'extension','[]'::jsonb)) e
    where e->>'url' = 'urn:fhir:isDeleted'
  ) as has_is_deleted,
  (
    select case
      when (e->>'valueInteger') ~ '^[0-9]+$' then (e->>'valueInteger')::int
      else null
    end
    from jsonb_array_elements(coalesce(o.resource->'extension','[]'::jsonb)) e
    where e->>'url' = 'urn:fhir:isDeleted'
    limit 1
  ) as is_deleted_int,
  jsonb_exists(o.resource, 'subject') as has_subject_field,
  (o.resource #>> '{subject,id}') is not null as has_subject_resolved_id,
  (o.resource #>> '{subject,identifier,system}') is not null as has_subject_identifier,
  jsonb_exists(o.resource, 'encounter') as has_encounter_field,
  (o.resource #>> '{encounter,id}') is not null as has_encounter_resolved_id,
  (o.resource #>> '{encounter,identifier,system}') is not null as has_encounter_identifier,
  jsonb_exists(o.resource, 'issued') as has_issued,
  jsonb_exists(o.resource, 'effectiveDateTime') as has_effective_datetime,
  jsonb_exists(o.resource, 'effectivePeriod') as has_effective_period,
  exists (
    select 1
    from jsonb_array_elements(coalesce(o.resource->'category','[]'::jsonb)) c
    cross join lateral jsonb_array_elements(coalesce(c->'coding','[]'::jsonb)) cc
    where cc->>'system' = 'http://terminology.hl7.org/CodeSystem/observation-category'
  ) as has_standard_category,
  exists (
    select 1
    from jsonb_array_elements(coalesce(o.resource->'category','[]'::jsonb)) c
    cross join lateral jsonb_array_elements(coalesce(c->'coding','[]'::jsonb)) cc
    where cc->>'system' = 'urn:source:ifdb:aktivitettype_category'
  ) as has_raw_category,
  exists (
    select 1
    from jsonb_array_elements(coalesce(o.resource->'extension','[]'::jsonb)) e
    where e->>'url' = 'http://example.org/fhir/StructureDefinition/aktivitetsresultatRootMeta'
  ) as has_root_meta,
  exists (
    select 1
    from jsonb_array_elements(coalesce(o.resource->'extension','[]'::jsonb)) e
    where e->>'url' like 'http://example.org/fhir/StructureDefinition/aktivitetsresultatKontekst%'
  ) as has_kontekst_meta,
  exists (
    select 1
    from jsonb_array_elements(coalesce(o.resource->'extension','[]'::jsonb)) e
    where e->>'url' = 'http://example.org/fhir/StructureDefinition/aktivitetsresultatSourceMeta'
  ) as has_source_meta,
  exists (
    select 1
    from jsonb_array_elements(coalesce(o.resource->'extension','[]'::jsonb)) e
    where e->>'url' = 'http://example.org/fhir/StructureDefinition/aktivitetsudfoerelseMetadata'
  ) as has_udf_meta,
  exists (
    select 1
    from jsonb_array_elements(coalesce(o.resource->'extension','[]'::jsonb)) e
    where e->>'url' = 'http://example.org/fhir/StructureDefinition/aktivitettypeKodeHierarki'
  ) as has_hierarchy_meta
from observation o;

-- Observation source volumes and recency
select
  source,
  min(cts) as first_seen,
  max(cts) as last_seen,
  count(*) as total
from tmp_obs_qc
group by source
order by source;

-- Observation canonical deletion/status checks
select
  source,
  count(*) as total,
  count(*) filter (where has_is_deleted) as with_is_deleted,
  count(*) filter (where not has_is_deleted) as missing_is_deleted,
  count(*) filter (where has_is_deleted and is_deleted_int = 1 and status = 'entered-in-error') as ok_deleted,
  count(*) filter (where has_is_deleted and is_deleted_int = 1 and status <> 'entered-in-error') as bad_deleted,
  count(*) filter (where has_is_deleted and is_deleted_int = 0 and status in ('final', 'preliminary')) as ok_not_deleted,
  count(*) filter (where has_is_deleted and is_deleted_int = 0 and status not in ('final', 'preliminary')) as bad_not_deleted
from tmp_obs_qc
group by source
order by source;

-- Observation branch enrichments (AKTIVITETSRESULTAT only)
select
  source,
  count(*) as total,
  count(*) filter (where has_issued) as with_issued,
  count(*) filter (where has_effective_datetime or has_effective_period) as with_effective,
  count(*) filter (where has_standard_category) as with_standard_category,
  count(*) filter (where has_raw_category) as with_raw_category,
  count(*) filter (where has_root_meta) as with_root_meta,
  count(*) filter (where has_kontekst_meta) as with_kontekst_meta,
  count(*) filter (where has_source_meta) as with_source_meta,
  count(*) filter (where has_udf_meta) as with_udf_meta,
  count(*) filter (where has_hierarchy_meta) as with_hierarchy_meta
from tmp_obs_qc
where source = 'aktivitetsresultat'
group by source;

-- Observation reference quality
select
  source,
  count(*) as total,
  count(*) filter (where has_subject_field) as with_subject,
  count(*) filter (where has_subject_resolved_id) as subject_resolved_id,
  count(*) filter (where has_subject_identifier) as subject_identifier_fallback,
  count(*) filter (where has_encounter_field) as with_encounter,
  count(*) filter (where has_encounter_resolved_id) as encounter_resolved_id,
  count(*) filter (where has_encounter_identifier) as encounter_identifier_fallback
from tmp_obs_qc
group by source
order by source;

-- ============================================================================
-- 1) Procedure quality model
-- ============================================================================

drop view if exists tmp_proc_qc;
create temporary view tmp_proc_qc as
select
  p.id,
  p.txid,
  p.cts,
  p.ts,
  p.resource,
  p.resource->>'status' as status,
  case
    when exists (
      select 1
      from jsonb_array_elements(coalesce(p.resource->'identifier','[]'::jsonb)) i
      where i->>'system' = 'urn:source:ifdb:administrative_procedure'
    ) then 'administrative_procedure'
    when exists (
      select 1
      from jsonb_array_elements(coalesce(p.resource->'identifier','[]'::jsonb)) i
      where i->>'system' = 'urn:source:ifdb:ris_ydelse'
    ) then 'ris_ydelse'
    when exists (
      select 1
      from jsonb_array_elements(coalesce(p.resource->'identifier','[]'::jsonb)) i
      where i->>'system' = 'urn:source:ifdb:aktivitetsresultat_procedure'
    ) then 'aktivitetsresultat_procedure'
    else 'other'
  end as source,
  exists (
    select 1
    from jsonb_array_elements(coalesce(p.resource->'extension','[]'::jsonb)) e
    where e->>'url' = 'urn:fhir:isDeleted'
  ) as has_is_deleted,
  (
    select case
      when (e->>'valueInteger') ~ '^[0-9]+$' then (e->>'valueInteger')::int
      else null
    end
    from jsonb_array_elements(coalesce(p.resource->'extension','[]'::jsonb)) e
    where e->>'url' = 'urn:fhir:isDeleted'
    limit 1
  ) as is_deleted_int,
  jsonb_exists(p.resource, 'subject') as has_subject_field,
  (p.resource #>> '{subject,id}') is not null as has_subject_resolved_id,
  (p.resource #>> '{subject,identifier,system}') is not null as has_subject_identifier,
  jsonb_exists(p.resource, 'encounter') as has_encounter_field,
  (p.resource #>> '{encounter,id}') is not null as has_encounter_resolved_id,
  (p.resource #>> '{encounter,identifier,system}') is not null as has_encounter_identifier,
  exists (
    select 1
    from jsonb_array_elements(coalesce(p.resource->'extension','[]'::jsonb)) e
    where e->>'url' = 'http://example.org/fhir/StructureDefinition/episodeOfCare'
  ) as has_episode_extension,
  exists (
    select 1
    from jsonb_array_elements(coalesce(p.resource->'extension','[]'::jsonb)) e
    where e->>'url' = 'http://example.org/fhir/StructureDefinition/procedureSourceMeta'
  ) as has_proc_source_meta,
  exists (
    select 1
    from jsonb_array_elements(coalesce(p.resource->'extension','[]'::jsonb)) e
    where e->>'url' = 'http://example.org/fhir/StructureDefinition/procedureLocationMeta'
  ) as has_proc_location_meta,
  exists (
    select 1
    from jsonb_array_elements(coalesce(p.resource->'extension','[]'::jsonb)) e
    where e->>'url' = 'http://example.org/fhir/StructureDefinition/sourceCreatedAt'
  ) as has_source_created_at,
  exists (
    select 1
    from jsonb_array_elements(coalesce(p.resource->'extension','[]'::jsonb)) e
    where e->>'url' = 'http://example.org/fhir/StructureDefinition/sourceModifiedAt'
  ) as has_source_modified_at,
  jsonb_exists(p.resource, 'performer') as has_performer,
  exists (
    select 1
    from jsonb_array_elements(coalesce(p.resource->'performer','[]'::jsonb)) pf
    where pf #>> '{actor,identifier,system}' = 'https://sundhedsdatastyrelsen.dk/sor'
  ) as has_org_performer_sor,
  jsonb_exists(p.resource, 'performedDateTime') as has_performed_datetime,
  jsonb_exists(p.resource, 'performedPeriod') as has_performed_period
from procedure p;

-- Procedure source volumes and recency
select
  source,
  min(cts) as first_seen,
  max(cts) as last_seen,
  count(*) as total
from tmp_proc_qc
group by source
order by source;

-- Procedure canonical deletion/status checks
select
  source,
  count(*) as total,
  count(*) filter (where has_is_deleted) as with_is_deleted,
  count(*) filter (where not has_is_deleted) as missing_is_deleted,
  count(*) filter (where has_is_deleted and is_deleted_int = 1 and status = 'entered-in-error') as ok_deleted,
  count(*) filter (where has_is_deleted and is_deleted_int = 1 and status <> 'entered-in-error') as bad_deleted,
  count(*) filter (where has_is_deleted and is_deleted_int = 0 and status = 'completed') as ok_not_deleted,
  count(*) filter (where has_is_deleted and is_deleted_int = 0 and status <> 'completed') as bad_not_deleted
from tmp_proc_qc
group by source
order by source;

-- Procedure metadata + temporal field coverage
select
  source,
  count(*) as total,
  count(*) filter (where has_proc_source_meta) as with_proc_source_meta,
  count(*) filter (where has_proc_location_meta) as with_proc_location_meta,
  count(*) filter (where has_source_created_at and has_source_modified_at) as with_source_timestamps,
  count(*) filter (where has_performed_datetime or has_performed_period) as with_performed
from tmp_proc_qc
group by source
order by source;

-- Procedure reference quality
select
  source,
  count(*) as total,
  count(*) filter (where has_subject_field) as with_subject,
  count(*) filter (where has_subject_resolved_id) as subject_resolved_id,
  count(*) filter (where has_subject_identifier) as subject_identifier_fallback,
  count(*) filter (where has_encounter_field) as with_encounter,
  count(*) filter (where has_encounter_resolved_id) as encounter_resolved_id,
  count(*) filter (where has_encounter_identifier) as encounter_identifier_fallback,
  count(*) filter (where has_episode_extension) as with_episode_extension
from tmp_proc_qc
group by source
order by source;

-- Procedure organization performer (B2 direction)
select
  source,
  count(*) as total,
  count(*) filter (where has_org_performer_sor) as with_org_performer_sor,
  count(*) filter (where not has_org_performer_sor) as without_org_performer_sor
from tmp_proc_qc
where source in ('administrative_procedure', 'ris_ydelse')
group by source
order by source;

-- ============================================================================
-- 2) Condition quality model
-- ============================================================================

drop view if exists tmp_cond_qc;
create temporary view tmp_cond_qc as
select
  c.id,
  c.txid,
  c.cts,
  c.ts,
  c.resource,
  case
    when exists (
      select 1
      from jsonb_array_elements(coalesce(c.resource->'identifier','[]'::jsonb)) i
      where i->>'system' = 'urn:source:ifdb:diagnose'
    ) then 'diagnose'
    when exists (
      select 1
      from jsonb_array_elements(coalesce(c.resource->'identifier','[]'::jsonb)) i
      where i->>'system' = 'urn:source:ifdb:healthissue'
    ) then 'healthissue'
    else 'other'
  end as source,
  exists (
    select 1
    from jsonb_array_elements(coalesce(c.resource->'extension','[]'::jsonb)) e
    where e->>'url' = 'urn:fhir:isDeleted'
  ) as has_is_deleted,
  (
    select case
      when (e->>'valueInteger') ~ '^[0-9]+$' then (e->>'valueInteger')::int
      else null
    end
    from jsonb_array_elements(coalesce(c.resource->'extension','[]'::jsonb)) e
    where e->>'url' = 'urn:fhir:isDeleted'
    limit 1
  ) as is_deleted_int,
  jsonb_exists(c.resource, 'subject') as has_subject_field,
  (c.resource #>> '{subject,id}') is not null as has_subject_resolved_id,
  (c.resource #>> '{subject,identifier,system}') is not null as has_subject_identifier,
  (
    select vc->>'code'
    from jsonb_array_elements(coalesce(c.resource #> '{verificationStatus,coding}','[]'::jsonb)) vc
    limit 1
  ) as verification_status_code
from condition c;

-- Condition source volumes and recency
select
  source,
  min(cts) as first_seen,
  max(cts) as last_seen,
  count(*) as total
from tmp_cond_qc
group by source
order by source;

-- Condition deletion/verification semantics (diagnose focus)
select
  source,
  count(*) as total,
  count(*) filter (where has_is_deleted) as with_is_deleted,
  count(*) filter (where not has_is_deleted) as missing_is_deleted,
  count(*) filter (where has_is_deleted and is_deleted_int = 1 and verification_status_code = 'entered-in-error') as ok_deleted,
  count(*) filter (where has_is_deleted and is_deleted_int = 1 and verification_status_code <> 'entered-in-error') as bad_deleted,
  count(*) filter (where has_is_deleted and is_deleted_int = 0 and verification_status_code = 'confirmed') as ok_not_deleted,
  count(*) filter (where has_is_deleted and is_deleted_int = 0 and verification_status_code <> 'confirmed') as bad_not_deleted
from tmp_cond_qc
where source = 'diagnose'
group by source;

-- Condition reference quality
select
  source,
  count(*) as total,
  count(*) filter (where has_subject_field) as with_subject,
  count(*) filter (where has_subject_resolved_id) as subject_resolved_id,
  count(*) filter (where has_subject_identifier) as subject_identifier_fallback
from tmp_cond_qc
group by source
order by source;

-- ============================================================================
-- 3) Duplicate source identifier checks (critical idempotency)
-- ============================================================================

-- Observation duplicates by source identifier
with obs_ids as (
  select
    i->>'system' as id_system,
    i->>'value' as id_value
  from observation o
  cross join lateral jsonb_array_elements(coalesce(o.resource->'identifier','[]'::jsonb)) i
  where i->>'system' in ('urn:source:ifdb:aktivitetsresultat', 'urn:source:ifdb:eventmarker')
)
select
  'observation' as resource,
  id_system,
  count(*) filter (where n > 1) as duplicated_keys
from (
  select id_system, id_value, count(*) as n
  from obs_ids
  group by id_system, id_value
) d
group by id_system
order by id_system;

-- Procedure duplicates by source identifier
with proc_ids as (
  select
    i->>'system' as id_system,
    i->>'value' as id_value
  from procedure p
  cross join lateral jsonb_array_elements(coalesce(p.resource->'identifier','[]'::jsonb)) i
  where i->>'system' in (
    'urn:source:ifdb:administrative_procedure',
    'urn:source:ifdb:ris_ydelse',
    'urn:source:ifdb:aktivitetsresultat_procedure'
  )
)
select
  'procedure' as resource,
  id_system,
  count(*) filter (where n > 1) as duplicated_keys
from (
  select id_system, id_value, count(*) as n
  from proc_ids
  group by id_system, id_value
) d
group by id_system
order by id_system;

-- Condition duplicates by source identifier
with cond_ids as (
  select
    i->>'system' as id_system,
    i->>'value' as id_value
  from condition c
  cross join lateral jsonb_array_elements(coalesce(c.resource->'identifier','[]'::jsonb)) i
  where i->>'system' in ('urn:source:ifdb:diagnose', 'urn:source:ifdb:healthissue')
)
select
  'condition' as resource,
  id_system,
  count(*) filter (where n > 1) as duplicated_keys
from (
  select id_system, id_value, count(*) as n
  from cond_ids
  group by id_system, id_value
) d
group by id_system
order by id_system;

-- ============================================================================
-- 4) Optional drill-down helpers (uncomment when needed)
-- ============================================================================

-- -- Missing isDeleted examples for Procedure
-- select id, cts, source, status
-- from tmp_proc_qc
-- where not has_is_deleted
-- order by cts desc
-- limit 100;

-- -- AKTIVITETSRESULTAT Procedure rows without encounter
-- select id, cts, source
-- from tmp_proc_qc
-- where source = 'aktivitetsresultat_procedure'
--   and not has_encounter_field
-- order by cts desc
-- limit 100;

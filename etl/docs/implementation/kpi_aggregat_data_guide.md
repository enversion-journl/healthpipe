<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Externally Delivered KPI / Aggregate Data

Every other ETL guide assumes one of two source shapes: transactional rows
(ERP: `ext → arc → pre_stg → stg → map/dim/fct`) or FHIR resources. This guide
covers the third shape: **pre-aggregated KPI figures delivered as periodic
file snapshots by an external authority**. The producer owns the calculation;
we ingest its RESULT. That single fact drives every decision below.

The guide is written for ANY source of this shape. HAIBA (hospital-acquired
infections from SDS) is the first one in the platform and appears throughout
as the worked example — in marked example blocks, never as the rule.

## 1. When is a source "KPI / aggregate data"?

A source belongs to this guide when most of these hold:

- **The measures are already summed at the source.** There is no transaction
  to drill into — the row IS the figure, produced by the authority's own
  (often opaque) calculation.
- **The grain is (period × organisation × category)**, not an event with an
  identity. There is **no transaction id**; a row is identified by its
  dimension values.
- **Deliveries are full snapshots**, not deltas — and the producer revises
  history: last month's figure can change in this week's file.
- **The same delivery can carry several aggregation levels** (per-unit rows
  AND national totals in one file).
- **Delivery metadata lives in the file name** (type, period, delivery date),
  not in the rows.

If the source has transaction ids and additive events, use the standard
transactional path instead — aggregation we can do ourselves belongs in views
and datamarts, not in ingestion.

> **Example (HAIBA):** weekly CSV snapshots per HAI type; grain
> (HAI, dato, rolle, region, SOR-enhed); `antal`/`risikodoegn` pre-summed by
> SDS; national aggregates (RegionKode 8888/9999) mixed into the same files;
> the delivery date only exists in the file name.

## 2. What it means for each layer

### ext — unchanged

1:1 landing of the newest usable file per (type, kind), volatile, no business
logic. Derive the delivery date from the file name ONCE, into a dedicated
column (`leverance_dato`-style), so nothing downstream ever re-parses names.

### arc — decide the revision strategy explicitly

The producer revises history, so the archive must answer: *what did we know,
when?* Two models — pick per source and write the choice down:

| Model | How | Choose when |
|-------|-----|-------------|
| **Upsert + revision log** | MERGE on the natural key with `detect_deletes`; changed measure values are copied to a side table before overwrite | Deliveries are large and mostly stable — storing every snapshot would be volume × 52/year |
| **insert_only snapshots** | Every delivery appended with its delivery date; consumers pick a snapshot | Deliveries are small, or consumers genuinely need to query "as of delivery X" |

Run the volume calculation before choosing: rows per delivery × deliveries per
year × years of retention. Full snapshots of large sources make insert_only
explode; revision logs store only the diff.

> **Example (HAIBA):** upsert + `arc.haiba_agg_revision` — 6-8M rows per
> delivery weekly made snapshot retention a non-starter, and the profiled
> revision rate between deliveries is small.

Track deliveries in a control table keyed on (source table, delivery date)
with row counts per merge — that register is later the freshness signal and
the direction guard ("never merge an older delivery on top of a newer one").

### Guards the ingestion MUST have

Full-snapshot + delete-detection is a loaded gun: anything missing from ext is
soft-deleted from the archive. Three guards are non-negotiable for this source
shape (reference implementation: `DSI/csv_files/haiba.py`, module docstring):

1. **A type/category with deliveries but no usable file fails the run** — a
   header-only or corrupt file must not erase that category's history. A
   category with NO files at all is a genuine retirement and must still flow.
2. **Width/format rejection with counts**: a single over-wide value must not
   abort the whole archive MERGE; reject and count per column, never log the
   value (it can be a person identifier).
3. **Unknown columns fail loudly** — the producer changes schemas without
   notice; silently dropped columns become silently wrong KPIs.

### stg / dim / fct — usually skipped: the dm path

Do NOT press aggregate data into a shared star-schema fct by default. The
source grain already IS the reporting grain, there is nothing to conform, and
a fake transactional shape just invites double counting. The platform's
answer is the **datamart path**: `arc → map (mint dw_ek per natural key) →
dm fact (DW keys + typed measures) + dm `_Egenskaber` (raw source values 1:1)`
— see [sql_etl_guide.md → The Datamart Path (dm)](sql_etl_guide.md#the-datamart-path-dm).

Aggregate data DOES belong in shared dims: resolve organisation, date and
other conformed keys with the standard sentinels, keeping the raw source
codes on the `_Egenskaber` side so an unresolved key never costs the value.

Typing happens in the dm rebuild (`TRY_CONVERT` + NULLIF) with a per-column
conversion-loss counter — the archive stays untyped text, so a format change
from the producer surfaces as a counted loss, not a silent NULL.

### Views — expose aggregation levels without double counting

Mixed levels in one table means a bare `SUM()` over everything is wrong by
construction. The consumer layer must make the safe read the default:

- Filter soft-deleted rows in the default view (the revision trail stays
  queryable in the base table).
- Make the level column (region code, "national total" marker, role) part of
  every documented query pattern; consider level-specific views when
  consumers keep tripping on it.

> **Example (HAIBA):** `vw_dm.HAIBA_*` filters `dw_is_deleted = 0`;
> RegionKode 8888/9999 marks national aggregates whose SOR key is the -1
> sentinel — summing without a RegionKode filter double counts nationally.

## 3. The five pitfalls (all observed in practice)

1. **Mixed grains in the same file → double counting on SUM.** Detect the
   level columns during profiling and carry them through to the consumer
   layer; never "clean them away".
2. **The producer revises history → "latest snapshot" ≠ "what we reported
   last month".** This is why arc needs the explicit revision strategy; a
   revision log or snapshots are the only defensible answers to an auditor.
3. **The producer rotates which files/types are delivered → the pipeline must
   not require a fixed content list.** Detect schema by header, not by a
   hardcoded type list; let categories retire; never hardcode
   `expected_groups` in freshness checks on category level.
4. **Delivery metadata only in the file name → derive it once, at ext.** One
   parser, one column, and the control register carries it from there.
5. **Control totals are the only way to detect a partial delivery.** A short
   file parses fine. Cross-check internally deliverable totals (e.g. case
   rows vs. the aggregate table's own sums) — but compare only when both
   sides come from the SAME delivery date; the producer's file families are
   routinely delivered out of step, and cross-delivery comparison is a
   standing false alarm.

> **Example (HAIBA):** cases vs. agg control total holds exactly per HAI type
> when the delivery dates align (`dqa/config/haiba_subjects.py`,
> `kontroltotal`), and HAIBAcpr/HAIBAagg were a week apart when verified —
> the check therefore reports out-of-step types without failing them.

## Checklist for onboarding a new KPI/aggregate source

1. Profile the deliveries: grains, levels, revision behaviour, file-name
   metadata, encodings, volume calculation for the arc strategy.
2. ext + arc with the four ingestion guards and a delivery control register.
3. dm path (map mint → fact + `_Egenskaber`) with shared-dim resolution,
   sentinels and the conversion-loss counter.
4. Views that make the safe read the default; document the level columns.
5. DQA: per-category freshness (no fixed category list), control totals
   (delivery-aligned only), row parity fact↔arc, encoding sanity.
6. Lineage YAML per dm table; DFM run-logging with mode locked to full_load.

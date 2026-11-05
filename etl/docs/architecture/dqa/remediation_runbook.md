<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# DQA remediation runbook

Five **operator-triggered** assets fix problems the weekly DQA checks detect. All
are manual (never scheduled), all mutate production data, and all take a
`dry_run` flag — **always run `dry_run: true` first** and review the counts.

| Asset / job | Fixes | Detected by |
|---|---|---|
| `dqa_remediation_replay` | Dangling FHIR references; arc rows never posted/delete-propagated | `referential_integrity` + `archive_fhir_reconciliation` |
| `dqa_fhir_server_cleanup` | Flat rows whose resource was deleted on the FHIR server | `fhir_server_reconciliation` (`stale_in_flat`) |
| `dqa_patient_delete_propagation` | Patient rows deleted in arc with no active CPR sibling | `patient_arc_integrity` (`delete_mismatch`) |
| `dqa_arc_ifdb_replay` | IFDB keys that never reached the `arc.*` archive (ids-mode re-extract + merge, checkpoint untouched) | `source_archive_reconciliation` (`missing_in_arc`, captured to the queue) |
| `dqa_arc_ifdb_vanished_cleanup` | Active `arc.*` rows whose key vanished from IFDB (source hard-delete) — sets `dw_is_deleted = 1` | `source_archive_reconciliation` (`vanished_not_marked_deleted`) |

`patient_arc_integrity`'s **`kept_deleted_dup`** (fhir kept a deleted PATIENT_ID
while an active CPR sibling exists — the person is *alive* under another id) is
**not** a cleanup: it's auto-captured to `dqa.RemediationQueue` and fixed by the
**patient replay** (`dqa_remediation_replay_job`, `entity=ifdb_patient`), which
re-dedups the CPR and re-points fhir to the active id. Soft-deleting it would
wrongly retire a live patient.

Both detectors run in **`dqa_fhir_weekly_job`**, triggered by the
`dqa_fhir_after_etl` sensor after Saturday's FHIR pipeline settles (see the
"Orchestration & scheduling" section of the DQA README). Review its results
(or `dqa.Results` / `dqa_summary`) before remediating.

---

## When to run (after which assets)

The flat tables and the FHIR server must reflect a **settled** state first, so
remediate **after the daily FHIR pipeline has completed** for the period you care
about:

```
ifdb_dfm_daily_job                (Oracle → arc → FHIR server posts →
        │                          SQL-on-FHIR projection + DSO sync → flat fhir.*)
        │
        ▼
dqa_fhir_weekly_job               (detection: referential_integrity + fhir_server_reconciliation)
        │
        ▼
►  dqa_remediation_replay_job / dqa_fhir_server_cleanup_job / dqa_patient_delete_propagation_job  ◄  (operator, dry_run first)
```

- **`dqa_remediation_replay`** — run after `dqa_fhir_weekly_job` has populated
  `dqa.RemediationQueue`, **and** after a recent `ifdb_dfm_daily_job` so the
  *surviving* parent (e.g. the kept Organization) already exists on the FHIR
  server. The replay itself runs `fhir_server_<entity>_dfm` (ids) + SOF + DSO, so
  don't run it concurrently with the daily pipeline for the same entity.
- **`dqa_fhir_server_cleanup`** — run after `ifdb_dfm_daily_job` so the flat
  tables are current (newly-posted resources projected). It only ever *soft-deletes*
  rows whose id is **gone** from the server, so not-yet-projected resources don't
  affect it — but acting on a settled state avoids confusion. **Do not run it
  concurrently with the FHIR pipeline** (`ifdb_dfm_daily_job`) for the same
  resource: it snapshots the server's live
  ids first, then soft-deletes flat rows missing from that snapshot. As a safeguard
  it only ever acts on rows whose `dw_timestamp_insert` predates the snapshot, so a
  resource created *after* the snapshot and projected mid-run can't be wrongly
  retired — but running on a settled state is still the right habit.

---

## `dqa_remediation_replay_job` — replay dangling references

Re-runs the child transform so its references re-resolve to the surviving parent,
then reprojects SOF/DSO and flips the queue rows to `resolved`.

Launchpad config (Dagster UI → Jobs → `dqa_remediation_replay_job`):

```yaml
ops:
  dqa_remediation_replay:
    config:
      entity: "ifdb_encounter"   # ifdb_encounter | ifdb_episode_of_care | ifdb_patient
      dry_run: true              # flip to false to execute
      # max_keys: 100000         # abort above this (systemic spike)
      # run_sof_dso: true        # also reproject SOF + DSO (default true)
      # mark_resolved: true      # re-check + flip status after (default true)
      # unresolvable_after: 3    # flip to 'unresolvable' after this many failed attempts
```

It **re-checks and updates `dqa.RemediationQueue` status itself**. A key still
failing after `unresolvable_after` (default 3) replay attempts flips to
`status='unresolvable'`: it leaves the feeder views (`pending`-only), so it
isn't replayed forever, but stays visible in `dqa.RemediationQueue` for the
operator. Re-detection by the weekly check does **not** reopen it (the capture
only reopens `resolved` keys). After fixing the root cause, retry with:

```sql
UPDATE dqa.RemediationQueue
SET status = 'pending'
WHERE status = 'unresolvable'
  AND replay_entity = '<entity>';
```

Full recipe (feeder views, what each step does):
`docs/implementation/dfm_operations_guide.md` → "Replay DQA-flagged dangling
references".

### What feeds the queue for an entity

Two weekly checks enqueue keys for `ifdb_encounter` / `ifdb_episode_of_care`
(`source_key == OBJECT_ID == ReplaySpec.filter_column`):

- **`referential_integrity`** — `reference_column` = the dangling FK column. After
  replay it re-checks the **full** violation predicate the check uses (dangling
  reference, plus the NULL/sentinel branch for `FK-Required` edges) and resolves
  only if that condition has cleared — a required FK that is still NULL after
  replay stays `pending` (`still dangling after replay`), it is not optimistically
  resolved.
- **`archive_fhir_reconciliation`** (`capture_replay=True` on these subjects) —
  `reference_column = 'archive_fhir_reconciliation'`. Captures arc keys that are
  **`missing_in_target`** (never posted) plus **arc-deleted `delete_status_mismatch`**
  (arc `DELETED=1` but the flat row is still active — the delete never propagated).
  The reverse mismatch (arc active, flat deleted) is **not** captured — it can be a
  deliberate FHIR-server delete, not an arc gap. Replaying posts/tomb-stones the
  keys; the recheck resolves these optimistically (the next weekly check re-captures
  if anything is still broken). **Not** enabled for `patient` — its reconciliation
  keys on CPR, which is not the replay filter column (`PATIENT_ID`).

> **Large historical backlog.** The capture aborts above `DQA_REMEDIATION_MAX_KEYS`
> (default 100 000): instead of enqueuing it writes an `overflow` marker and logs an
> error, because a spike that large is systemic — exactly the "soft-deletes were
> never extracted" backlog. To drain it deliberately, raise the capture guard **and**
> the replay's `max_keys` config (so it processes), then run off-hours — the replay
> re-posts that many encounters through `fhir_server_*_dfm` + SOF + DSO.

Two ways to raise the **capture** guard:

- **Per-run (preferred for a one-off)** — every DQA check takes an optional
  `max_keys` run config (`DqaCheckConfig`) that overrides the guard for that run
  only, no container restart. Re-run the check (e.g. `dqa_fhir_weekly_job`) with,
  in the Launchpad:

```yaml
ops:
  dqa_fhir_encounter_weekly_fhir_encounter__archive_fhir_reconciliation__vs_arc:
    config:
      max_keys: 300000
```

The op key is `dqa_<subject>_<cadence>_<full_check_name>` (it is **not**
branch-postfixed — only asset keys are). `max_keys` only affects checks that
enqueue (`archive_fhir_reconciliation` / `source_archive_reconciliation` w/ `capture_replay`,
`referential_integrity`, `patient_arc_integrity`); it's a no-op elsewhere.

- **Persistent** — set the env var `DQA_REMEDIATION_MAX_KEYS` on the code-location
  container (raises the default for every run until changed).

---

## `dqa_fhir_server_cleanup_job` — soft-delete server-deleted rows

Stages the FHIR server's live resource ids once and sets `dw_is_deleted = 1` on the
stale active flat rows. One entity can cover several flat tables at different
grains — `encounter` cleans **both** `fhir.Encounter` and `fhir.Encounter_Location`
in one atomic run.

Wired entities (one per Aidbox resource that projects to an `dw_is_deleted`-bearing
flat table): `encounter` (→ `fhir.Encounter` + `fhir.Encounter_Location`),
`episode_of_care` (→ `fhir.EpisodeOfCare`), `patient` (→ `fhir.Patient`),
`organization` (→ `fhir.Organization`), `location` (→ `fhir.Location`) — both via
migration `20260702_fhir_org_location_add_is_deleted` — plus `appointment`
(→ `fhir.Appointment`) and `service_request` (→ `fhir.ServiceRequest`). The last
two are **not DFM-migrated** (no replay pipeline), but the cleanup is
DFM-independent, so server-deletion staleness is still fixable for them. The
remaining non-DFM tables (Condition, Observation, Procedure, CarePlan,
Questionnaire, QuestionnaireResponse + sub-grain projections) have no `dw_is_deleted`
column — their `fhir_server_reconciliation` check (WARN) is detection-only until
the column is added.

Launchpad config (Dagster UI → Jobs → `dqa_fhir_server_cleanup_job`):

```yaml
ops:
  dqa_fhir_server_cleanup:
    config:
      entity: "encounter"        # encounter | episode_of_care | patient |
                                 # organization | location | appointment | service_request
      dry_run: true              # flip to false to execute
      # max_rows: 100000         # abort if a target table exceeds this (systemic)
```

`dry_run: true` reports `would_soft_delete` per table and makes no changes. Unlike
the replay, this asset does **not** re-check — confirm afterwards by re-running the
`fhir_server_reconciliation` check (or `dqa_fhir_weekly_job`); `stale_in_flat`
should drop to 0.

Soft-delete is reversible (`dw_is_deleted = 0`). Consumers of `fhir.Encounter` /
`fhir.Encounter_Location` must filter `dw_is_deleted` (or join to the active parent),
or the soft-deleted rows still appear.

**Propagation to DSO.** The soft-delete also bumps `dw_timestamp_update`
(`= SYSDATETIME()`) — the DSO-sync watermark — so the next DSO-sync delta carries
the `dw_is_deleted = 1` to the DSO copy. (It deliberately does **not** touch
`last_updated`, which mirrors the FHIR server's `meta.lastUpdated`.) Rows
soft-deleted by an **earlier** cleanup run, before this watermark bump existed,
won't have propagated — re-stamp them once with a one-off `UPDATE` on DSA that
sets `dw_timestamp_update = SYSDATETIME()` on the affected `dw_is_deleted = 1`
rows older than a chosen cutoff (the previously referenced
`backfill_dw_timestamp_update_after_cleanup.sql` was never committed), then let
the next DSO-sync pick them up.

---

## `dqa_patient_delete_propagation_job` — soft-delete fully-deleted patients

For `fhir.Patient` rows whose kept `ifdb_patient_id` is deleted in **arc**
(`SLETTET=1`) **and** the CPR has no active sibling (the whole person is gone), but
`dw_is_deleted` was never set. Source of truth is arc, not the FHIR server — so it's
separate from `dqa_fhir_server_cleanup`.

```yaml
ops:
  dqa_patient_delete_propagation:
    config:
      dry_run: true              # flip to false to execute
      # max_rows: 100000
```

It deliberately skips `kept_deleted_dup` (active sibling) — those go to the patient
replay, not here. Confirm afterwards by re-running `patient_arc_integrity`. Like the
FHIR-server cleanup, it bumps `dw_timestamp_update` (not `last_updated`) so the
soft-delete propagates to DSO on the next sync; rows from earlier runs need the
same one-off re-stamp described under `dqa_fhir_server_cleanup` above.

---

## `dqa_arc_ifdb_replay_job` — re-extract missing archive keys

Fixes `source_archive_reconciliation`'s **`missing_in_arc`**: IFDB merge keys the weekly
check found in Oracle but not in the archive. The check auto-captures them to
`dqa.RemediationQueue` (`reference_column='source_archive_reconciliation'`,
`replay_entity=arc_ifdb_<entity>` — a separate namespace, so they never appear in
the FHIR `dqa.v_replay_*` feeder views). The replay launches child run(s) over
`ext_<entity>_ifdb` + `arc_<entity>_ifdb` in **ids mode**:

- keys are passed **inline** (`id_filter_values`) because the extract's filter
  runs on Oracle, where the MSSQL queue views don't exist — chunked at 1000 keys
  per child run (Oracle's IN-list cap);
- **the DFM checkpoint is not advanced** (`advance_checkpoint=false`, also the
  ids-mode default), so the next daily delta window is unaffected;
- afterwards each key is re-checked against arc and flipped to `resolved`
  (still-missing keys bump `attempts`; `unresolvable` after `unresolvable_after`,
  default 3 — reset to `pending` manually to retry).

```yaml
ops:
  dqa_arc_ifdb_replay:
    config:
      entity: "arc_ifdb_encounter"   # arc_ifdb_encounter | arc_ifdb_episode_of_care | arc_ifdb_patient
      dry_run: true                  # flip to false to execute
      # max_keys: 5000               # abort above this (systemic gap -> investigate)
```

**Location is deliberately not supported**: composite merge key
(`OBJECT_ID, PERIOD_START`), and its archive merge runs NOT-MATCHED-BY-SOURCE
delete detection (`upstream_is_full_load`) — an ids-mode *partial* ext would
mass-soft-delete every row outside the batch. Location gaps self-heal on the next
daily full load. Don't run this concurrently with `ifdb_dfm_daily_job` for the
same entity (both write ext/arc).

---

## `dqa_arc_ifdb_vanished_cleanup_job` — soft-delete keys gone from IFDB

Fixes `source_archive_reconciliation`'s **`vanished_not_marked_deleted`**: active arc rows
whose merge key no longer exists in the IFDB source at all (a source hard-delete —
invisible to the delta extract, so `dw_is_deleted` never gets set). Stages the
source's *current* live keys and sets `dw_is_deleted = 1` on the active arc rows
whose key is gone — the arc analogue of `dqa_fhir_server_cleanup`, with the same
safeguards: snapshot cutoff (only rows archived *before* the staging snapshot are
eligible), refusal on 0 staged keys, `dry_run`, and a `max_rows` guard (default
10 000 — IFDB *retains* logically-deleted rows via its own `DELETED`/`SLETTET`
flags, so genuine hard-deletes should be rare; a spike is systemic).

```yaml
ops:
  dqa_arc_ifdb_vanished_cleanup:
    config:
      entity: "arc_ifdb_encounter"   # arc_ifdb_encounter | arc_ifdb_episode_of_care | arc_ifdb_patient
      dry_run: true                  # flip to false to execute
      # max_rows: 10000
```

Location is again excluded — its daily full-load archive merge already
soft-deletes vanished rows via NOT MATCHED BY SOURCE. Confirm afterwards by
re-running the entity's `source_archive_reconciliation` check (`dqa_arc_ifdb_weekly_job`):
the rows reclassify from `vanished_not_marked_deleted` to `vanished_marked_deleted`
(expected). Soft-delete is reversible (`dw_is_deleted = 0`). NB: soft-deleting in
arc does **not** propagate to the flat `fhir.*` tables by itself — the next
`archive_fhir_reconciliation` run will surface any resulting delete-status mismatches,
and (for Encounter/EoC) capture them for the FHIR replay.

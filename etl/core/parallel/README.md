<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Core parallel execution

Use this package when an ETL stage needs process/thread fan-out, threshold
gating, fallback, spawn import-path setup, or small named parallel calls.

Architecture overview: `etl/docs/architecture/core_cache_parallel_execution.md`.

Call sites should not construct executors directly. They should define a
`ParallelStage` once and call `run_parallel_stage()`.

## Stage map pattern

```python
from core.parallel import ParallelStage, run_parallel_stage

MY_STAGE = ParallelStage.from_env(
    stage_id="my.asset.parse",
    env_prefix="MY_ASSET_PARSE",
    default_max_workers=4,
    default_parallel_threshold=5_000,
    default_chunksize=200,
    default_max_logged_errors=10,
    executor_kind="process",
)

run_parallel_stage(
    MY_STAGE,
    tasks=tasks,
    worker_fn=_parse_worker,
    logger=logger,
    consume_results=_consume_results,
)
```

Supported env vars follow the same convention for every stage:

- `{PREFIX}_MAX_WORKERS`
- `{PREFIX}_PARALLEL_THRESHOLD`
- `{PREFIX}_CHUNKSIZE`
- `{PREFIX}_MAX_LOGGED_ERRORS`

Malformed integer values fall back to defaults and minimum bounds are enforced.

## Spawn workers

For process pools in modules that are not installed as packages, configure
spawn paths once and attach the returned hook to the stage:

```python
from multiprocessing import get_context
from core.parallel import configure_spawn_import_paths_for_file

SPAWN_IMPORTS = configure_spawn_import_paths_for_file(__file__)
SPAWN_CONTEXT = get_context("spawn")

MY_STAGE = ParallelStage.from_env(
    stage_id="my.asset.parse",
    env_prefix="MY_ASSET_PARSE",
    default_max_workers=4,
    default_parallel_threshold=5_000,
    default_chunksize=200,
    mp_context=SPAWN_CONTEXT,
    before_parallel_start=SPAWN_IMPORTS,
)
```

## Small named fan-outs

Use `run_parallel_calls()` for small independent calls such as posting two
resource types at once. It owns the thread pool and re-raises worker failures.

```python
from core.parallel import ParallelCall, run_parallel_calls

run_parallel_calls(
    stage_id="my.asset.post",
    calls=[
        ParallelCall("Observation", post_fn, kwargs={...}),
        ParallelCall("Procedure", post_fn, kwargs={...}),
    ],
    logger=logger,
    max_workers=2,
)
```

## Chunked transform+post pipeline

`ChunkedPipelineStage` + `run_chunked_transform_post()` are the standard way
to keep peak memory bounded when an asset must build many Pydantic FHIR
resources before posting. Use it when:

- Monthly partitions risk OOM if all rows are transformed in a single list.
- The post helper already exists and owns reference resolution + dependency
  resolution + ID resolution + direct DB upsert.
- A per-partition lookup (e.g. tillægskoder, encounter refs, supplemental
  codes) must be fetched **once** and shared across chunks.

This is distinct from the Oracle IN-clause chunking provided by
`DSA.FHIR.utils.ifdb.oracle_lookup.chunked_in_fetch[_grouped]`. The Oracle
helper splits a list of source ids across multiple `WHERE id IN (...)`
queries to stay under Oracle's 1000-id limit. The transform+post chunker
splits already-extracted rows so the in-memory Pydantic resource list never
grows past a configurable bound. Both are typically used together: setup
fetches lookups via `chunked_in_fetch_grouped()`, then the chunked pipeline
splits the row list and runs transform+post per chunk.

```python
from core.parallel import ChunkedPipelineStage, run_chunked_transform_post

MY_CHUNK_STAGE = ChunkedPipelineStage.from_env(
    stage_id="my.asset.transform_post",
    env_prefix="MY_ASSET_PIPELINE",
    default_chunk_size=25_000,
    cleanup_resource_types={"Condition"},
)


def _run_my_pipeline(context, extracted_data, ifdb_oracle, fhir_database):
    def setup(rows):
        # Fetch per-partition lookups once.
        return _fetch_my_lookups(rows, ifdb_oracle, context)

    def transform(chunk, lookups):
        return _transform_my_resources(chunk, context, lookups)

    def post(resources, chunk, _lookups):
        return _post_my_resources(
            context=context,
            resources=resources,
            fhir_database=fhir_database,
            extracted_count=len(chunk),
            ifdb_oracle=ifdb_oracle,
        )

    return run_chunked_transform_post(
        MY_CHUNK_STAGE,
        rows=extracted_data,
        setup_fn=setup,
        transform_fn=transform,
        post_fn=post,
        fhir_database=fhir_database,
        context=context,
    )
```

Contract:

- `setup_fn` is invoked exactly once per partition before any chunk; pass
  `None` if no shared setup is needed.
- `transform_fn(chunk, setup)` and `post_fn(resources, chunk, setup)` run
  per chunk. The post function may return either an `int` (treated as
  posted count) or a `dg.MaterializeResult` whose `posted_count` /
  `failed_count` / `source` / `method` metadata is read by the helper.
- `fhir_database.suspend_identifier_caches()` is called before each
  chunk's transform to release SQLite mmap memory.
- `safe_clear_identifier_caches_for_resources(stage.cleanup_resource_types)`
  runs in `finally`, so partial-persistence on chunk N failure still
  leaves the caches clean for the next partition.
- Empty rows short-circuit: setup/transform/post are skipped and a
  zero-result `MaterializeResult` is returned.

Env override: `{PREFIX}_CHUNK_SIZE`. Malformed/zero values fall back to
the default; minimum is clamped to 1 by `env_int()`.

Partial-persistence semantics: a failure in chunk N means chunks 0..N-1
are already persisted. Retry must be idempotent at the post layer — the
existing `resolve_resource_ids_for_upsert` + upsert path provides this.
Aktivitetsresultat already operates under the same contract via its own
dict-based chunked pipeline; the Pydantic CPO assets (Diagnose,
PasDiagnose, AdminProc, RIS Ydelse) all funnel through this generic
helper.

## Parallel extraction tunables (DSI mssql full-load)

`DSI/connectors/mssql/parallel_extract.py` opts the largest ERP source tables
into process-parallel, keyset-windowed extraction (`TableConfig.parallel_workers
> 1`, full-load + mssql-python only). Runtime knobs (all env-overridable):

| Env var | Default | Effect |
|---|---|---|
| `EXT_PARALLEL_DISABLE` | `false` | Set `true` to force the serial path everywhere (kill-switch, no redeploy). |
| `EXT_MAX_SOURCE_CONNECTIONS` | `16` | Hard ceiling on workers (= source connections) per table; `parallel_workers` is clamped to this. |
| `{SOURCE}_MAX_SOURCE_CONNS` | `16` | Per-source-system connection budget enforced by `SourceConnectionSemaphore` across concurrent assets (e.g. `MYSUPPLY_MAX_SOURCE_CONNS`, `SYDAX_MAX_SOURCE_CONNS`). |
| `EXT_SOURCE_SEM_DIR` | `/tmp/ext_source_sem` | Directory holding the cross-process semaphore slot lockfiles. |
| `EXT_LOG_CHUNK_INTERVAL` | `100` | Log a per-chunk line every N chunks (serial + parallel). |
| `EXT_SLOW_CHUNK_SECONDS` | `5.0` | Always log a chunk whose fetch/insert exceeds this many seconds. |

RAM note: workers run inside the dagster code-server (~4 GB dev/qa, ~20 GB
prod), so keep per-table `parallel_workers` low enough that concurrent
full-load assets don't OOM it (8 is the shipped value).

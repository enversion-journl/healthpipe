# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Runtime flags and parallel-stage configuration for AKTIVITETSRESULTAT."""

import multiprocessing as _mp
import os

from core.parallel import (
    ParallelStage,
    configure_spawn_import_paths_for_file,
    env_bool,
    env_int,
)

_SPAWN_IMPORT_PATHS = configure_spawn_import_paths_for_file(__file__)

_XML_POOL_MP_CONTEXT = _mp.get_context("spawn")

_FAST_MODE = env_bool("IFDB_XML_FAST_MODE", False)

SFI_MODE_LEGACY = "legacy"

SFI_MODE_AUDIT = "sfi_audit"

SFI_MODE_PRODUCTION = "sfi_production"

_VALID_SFI_MODES = (SFI_MODE_LEGACY, SFI_MODE_AUDIT, SFI_MODE_PRODUCTION)

_SFI_MODE = os.environ.get("IFDB_SFI_OBSERVATION_MODE", SFI_MODE_AUDIT).strip().lower()

if _SFI_MODE not in _VALID_SFI_MODES:
    raise RuntimeError(
        f"Invalid IFDB_SFI_OBSERVATION_MODE={_SFI_MODE!r}. Expected one of {_VALID_SFI_MODES}."
    )

_DEFAULT_XML_PARSE_WORKERS = min(2, max(1, (os.cpu_count() or 2) - 1))

_XML_LOSSLESS_PARSE_STAGE = ParallelStage.from_env(
    stage_id="ifdb.observation.xml.lossless_parse",
    env_prefix="IFDB_XML_PARSE",
    default_max_workers=_DEFAULT_XML_PARSE_WORKERS,
    default_parallel_threshold=5_000,
    default_chunksize=200,
    default_max_logged_errors=10,
    mp_context=_XML_POOL_MP_CONTEXT,
    before_parallel_start=_SPAWN_IMPORT_PATHS,
)

_XML_FAST_PARSE_STAGE = _XML_LOSSLESS_PARSE_STAGE.with_stage_id("ifdb.observation.xml.fast_parse")

_OBS_BUILD_STAGE = ParallelStage.from_env(
    stage_id="ifdb.observation.build",
    env_prefix="IFDB_OBS_BUILD",
    default_max_workers=1,
    default_parallel_threshold=2_000,
    default_chunksize=100,
    default_max_logged_errors=10,
    mp_context=_XML_POOL_MP_CONTEXT,
    before_parallel_start=_SPAWN_IMPORT_PATHS,
)

_AKTIVITETSRESULTAT_CHUNK_SIZE = max(
    1,
    env_int("IFDB_AKTIVITETSRESULTAT_CHUNK_SIZE", 25_000, minimum=1),
)

_DICT_VALIDATE_SAMPLE = max(0, env_int("IFDB_DICT_VALIDATE_SAMPLE", 50, minimum=0))

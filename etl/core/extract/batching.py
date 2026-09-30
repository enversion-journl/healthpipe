# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Batching policy and small chunking utilities for DSI extraction."""

from __future__ import annotations

import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import TypeVar

from core.parallel import env_bool, env_int

T = TypeVar("T")


@dataclass(frozen=True)
class ExtractBatchingPolicy:
    """Runtime policy for source-agnostic extract batching."""

    enabled: bool
    fetch_size: int
    env_prefix: str
    log_every_batches: int = 10
    write_batch_sidecars: bool = False

    def __post_init__(self) -> None:
        if self.fetch_size < 1:
            raise ValueError(f"fetch_size must be >= 1, got {self.fetch_size}")
        if self.log_every_batches < 1:
            raise ValueError(f"log_every_batches must be >= 1, got {self.log_every_batches}")

    @classmethod
    def from_env(
        cls,
        *,
        env_prefix: str,
        default_enabled: bool,
        default_fetch_size: int,
        default_log_every_batches: int = 10,
        fallback_env_prefix: str | None = None,
    ) -> ExtractBatchingPolicy:
        """Build a policy from standard extract batching env vars.

        Per-source ``env_prefix`` values win over ``fallback_env_prefix``. Empty
        or missing values fall back; malformed values use the supplied default.
        """

        enabled = _env_bool_with_optional_fallback(
            f"{env_prefix}_BATCHED",
            default_enabled,
            fallback_name=(f"{fallback_env_prefix}_BATCHED" if fallback_env_prefix else None),
        )
        fetch_size = _env_int_with_optional_fallback(
            f"{env_prefix}_BATCH_SIZE",
            default_fetch_size,
            fallback_name=(f"{fallback_env_prefix}_BATCH_SIZE" if fallback_env_prefix else None),
        )
        log_every_batches = _env_int_with_optional_fallback(
            f"{env_prefix}_LOG_EVERY_BATCHES",
            default_log_every_batches,
            fallback_name=(
                f"{fallback_env_prefix}_LOG_EVERY_BATCHES" if fallback_env_prefix else None
            ),
        )
        return cls(
            enabled=enabled,
            fetch_size=fetch_size,
            env_prefix=env_prefix,
            log_every_batches=log_every_batches,
        )

    def resolved(self, *, fallback_env_prefix: str | None = None) -> ExtractBatchingPolicy:
        """Apply env overrides to this policy's defaults."""

        return (
            type(self)
            .from_env(
                env_prefix=self.env_prefix,
                default_enabled=self.enabled,
                default_fetch_size=self.fetch_size,
                default_log_every_batches=self.log_every_batches,
                fallback_env_prefix=fallback_env_prefix,
            )
            .with_batch_sidecars(self.write_batch_sidecars)
        )

    def with_batch_sidecars(self, enabled: bool) -> ExtractBatchingPolicy:
        """Return the same runtime policy with explicit sidecar write mode.

        Sidecar writing is intentionally not controlled by the global batching
        env fallback. It doubles row-payload disk usage, so assets must opt in
        deliberately in code.
        """
        return type(self)(
            enabled=self.enabled,
            fetch_size=self.fetch_size,
            env_prefix=self.env_prefix,
            log_every_batches=self.log_every_batches,
            write_batch_sidecars=enabled,
        )


def _has_value(name: str) -> bool:
    return os.environ.get(name) not in (None, "")


def _env_int_with_optional_fallback(
    name: str,
    default: int,
    *,
    fallback_name: str | None = None,
) -> int:
    if _has_value(name):
        return env_int(name, default, minimum=1)
    if fallback_name and _has_value(fallback_name):
        return env_int(fallback_name, default, minimum=1)
    return max(1, default)


def _env_bool_with_optional_fallback(
    name: str,
    default: bool,
    *,
    fallback_name: str | None = None,
) -> bool:
    if _has_value(name):
        return env_bool(name, default)
    if fallback_name and _has_value(fallback_name):
        return env_bool(fallback_name, default)
    return default


def iter_sized_batches(iterable: Iterable[T], batch_size: int) -> Iterator[list[T]]:
    """Yield lists of up to ``batch_size`` items from any iterable."""

    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")

    batch: list[T] = []
    for item in iterable:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def iter_cursor_fetchmany(cursor, batch_size: int) -> Iterator[list]:
    """Yield cursor rows with DB-API ``fetchmany`` semantics."""

    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")

    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        yield batch

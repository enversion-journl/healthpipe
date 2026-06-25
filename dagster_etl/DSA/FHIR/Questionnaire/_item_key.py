# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared Questionnaire/QuestionnaireResponse item key helpers."""

from __future__ import annotations

import hashlib
from typing import Any


def stable_sfi_item_id(prefix: str, primary: Any, *fallback_parts: Any) -> str:
    """Return a FHIR BackboneElement.id-safe SFI item occurrence id."""
    if primary is not None:
        primary_text = str(primary).strip()
        if primary_text:
            return f"{prefix}-{primary_text}"[:64]
    seed = "|".join("" if part is None else str(part) for part in fallback_parts)
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{digest}"

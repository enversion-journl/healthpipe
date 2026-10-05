# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Helbredsforløb (Health Journey) calculation utilities.

A helbredsforløb groups related episodes of care (forløbsansvar) that are connected
via PREVIOUS_EOC_ID references.

This is used nationally for:
- Udredningsret (Diagnostic Investigation Right)
- Behandlingsret (Treatment Right)

Key concepts:
- Sygdomsforløb (HEALTHISSUE_ID): Regional concept from EPJ
- Helbredsforløb: National concept, can span multiple sygdomsforløb
- Connected via PREVIOUS_EOC_ID

Note: Conceptually, only certain reference types link episodes into helbredsforløb:
- ALAA01: Henvist i samme sygdomsforløb
- ALAA03: Reference til andet forløbselement (samme sygdom)

However, in IFDB, the reference type is not stored directly in EPISODE_OF_CARE,
so we treat all PREVIOUS_EOC_ID references as valid chain links.
"""

from collections import defaultdict
from typing import Any

import dagster as dg


def build_helbredsforloeb_mapping(
    episodes: list[dict[str, Any]],
    logger: dg.DagsterLogManager | None = None,
) -> dict[str, str]:
    """
    Build a mapping from episode_of_care_id to helbredsforloeb_id.

    Algorithm:
    1. Build a graph of episode connections via PREVIOUS_EOC_ID
    2. Find connected components (each component = one helbredsforløb)
    3. Assign a unique helbredsforloeb_id to each component

    The helbredsforloeb_id is the smallest (lexicographically first) episode_id in the chain,
    ensuring deterministic and stable IDs.

    Note: We include all PREVIOUS_EOC_ID references in chain building.
    In IFDB, the reference types (ALAA01, ALAA03 for helbredsforløb) are not stored
    directly, so we treat all references as valid chain links.

    Args:
        episodes: List of episode dicts with keys:
            - OBJECT_ID: Episode identifier
            - PREVIOUS_EOC_ID: Reference to previous episode (may be None)

    Returns:
        Dictionary mapping episode_of_care_id -> helbredsforloeb_id
    """
    if logger is None:
        logger = dg.get_dagster_logger()

    if not episodes:
        return {}

    # Step 1: Build adjacency list (bidirectional graph)
    # We need bidirectional because we want to find all connected components
    adjacency: dict[str, set[str]] = defaultdict(set)
    all_episode_ids: set[str] = set()

    for ep in episodes:
        ep_id = str(ep.get("OBJECT_ID", ""))
        prev_id = ep.get("PREVIOUS_EOC_ID")

        if not ep_id:
            continue

        all_episode_ids.add(ep_id)

        # Connect if there's a previous episode reference
        if prev_id:
            prev_id_str = str(prev_id)
            all_episode_ids.add(prev_id_str)
            # Add bidirectional edges
            adjacency[ep_id].add(prev_id_str)
            adjacency[prev_id_str].add(ep_id)

    # Step 2: Find connected components using BFS/DFS
    visited: set[str] = set()
    components: list[set[str]] = []

    def find_component(start_id: str) -> set[str]:
        """Find all episodes in the same connected component."""
        component = set()
        queue = [start_id]

        while queue:
            current = queue.pop()
            if current in visited:
                continue
            visited.add(current)
            component.add(current)

            for neighbor in adjacency.get(current, []):
                if neighbor not in visited:
                    queue.append(neighbor)

        return component

    for ep_id in all_episode_ids:
        if ep_id not in visited:
            component = find_component(ep_id)
            if component:
                components.append(component)

    # Step 3: Assign helbredsforloeb_id to each episode
    # Use the smallest episode_id in each component as the helbredsforloeb_id
    mapping: dict[str, str] = {}

    for component in components:
        # Sort to get deterministic helbredsforloeb_id (smallest episode_id)
        sorted_ids = sorted(component)
        helbredsforloeb_id = sorted_ids[0]

        for ep_id in component:
            mapping[ep_id] = helbredsforloeb_id

    logger.info(
        f"Built helbredsforløb mapping: {len(all_episode_ids)} episodes -> "
        f"{len(components)} helbredsforløb"
    )

    return mapping


def enrich_markers_with_helbredsforloeb(
    markers: list[dict[str, Any]],
    helbredsforloeb_mapping: dict[str, str],
) -> list[dict[str, Any]]:
    """
    Enrich marker records with HELBREDSFORLOEB_ID based on their EPISODE_OF_CARE_ID.

    Args:
        markers: List of marker dicts with EPISODE_OF_CARE_ID
        helbredsforloeb_mapping: Mapping from episode_of_care_id to helbredsforloeb_id

    Returns:
        Markers with HELBREDSFORLOEB_ID added
    """
    for marker in markers:
        eoc_id = marker.get("EPISODE_OF_CARE_ID")
        if eoc_id:
            eoc_id_str = str(eoc_id)
            marker["HELBREDSFORLOEB_ID"] = helbredsforloeb_mapping.get(eoc_id_str, eoc_id_str)
        else:
            # If no episode, use a placeholder (these markers won't form valid journeys)
            marker["HELBREDSFORLOEB_ID"] = None

    return markers


def get_unique_helbredsforloeb_count(
    helbredsforloeb_mapping: dict[str, str],
) -> int:
    """Get the number of unique helbredsforløb."""
    return len(set(helbredsforloeb_mapping.values()))

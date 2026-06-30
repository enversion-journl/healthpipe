# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Generate star schema HTML from data-lineage YAML files.

Simplified format: YAML uses capitalized keys (Description, Type, Domain, Grain,
SourceSystems, Columns) and FK relationships are inferred from the DW_EK_ naming
convention or explicitly set via FK-Target.
"""

import argparse
import json
import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

TYPE_MAP = {
    "dimension": "dim",
    "fact": "fact",
    "fhir": "fhir",
}


def _resolve_fk_target(col_name: str, fk_target: str | None, all_table_names: set[str]) -> str:
    """Resolve the target table for an FK column.

    If *fk_target* is set (from the YAML ``FK-Target`` field), return it,
    canonicalized to the casing in *all_table_names* when a case-insensitive
    match exists. Otherwise strip the ``DW_EK_`` prefix, take the first segment
    before ``_`` as the dimension name, and resolve to ``dim.{Name}`` —
    again canonicalized against *all_table_names* when possible so that minor
    casing differences in YAML ``TableName`` fields don't break the entity graph.
    """
    if fk_target:
        return _canonicalize(fk_target, all_table_names)

    # Strip DW_EK_ prefix
    if col_name.startswith("DW_EK_"):
        remainder = col_name[6:]  # e.g. "Kreditor" or "Dato_Faktura"
    else:
        remainder = col_name

    # First segment before '_' is the dimension name
    dim_name = remainder.split("_", 1)[0]
    return _canonicalize(f"dim.{dim_name}", all_table_names)


def _canonicalize(name: str, all_table_names: set[str]) -> str:
    """Return the case-canonical form of *name* if any entry in
    *all_table_names* matches case-insensitively; otherwise *name* unchanged.
    """
    if name in all_table_names:
        return name
    lowered = name.lower()
    for candidate in all_table_names:
        if candidate.lower() == lowered:
            return candidate
    return name


_FHIR_ID_PREFIXES_TO_STRIP = ("managing_", "previous_", "ifdb_")


def _resolve_fhir_target(
    col_name: str,
    table_id: str,
    all_fhir_names: set[str],
) -> str | None:
    """Resolve a FHIR ``*_id`` column to its target table.

    Convention: ``{resource}_id`` maps to ``fhir.{Resource}``.
    Compound names like ``episode_of_care_id`` become ``fhir.EpisodeOfCare``.
    Prefixes like ``managing_`` are stripped (``managing_organization_id`` →
    ``fhir.Organization``).  Returns *None* when no matching table exists or
    when the column is a self-reference / source-system identifier.
    """
    if not col_name.endswith("_id"):
        return None

    # Skip source-system identifiers (ifdb_*)
    if col_name.startswith("ifdb_"):
        return None

    # Strip known role prefixes
    name = col_name
    for prefix in _FHIR_ID_PREFIXES_TO_STRIP:
        if name.startswith(prefix):
            name = name[len(prefix) :]
            break

    # Remove _id suffix and PascalCase the remainder
    base = name.removesuffix("_id")
    resource = "".join(seg.capitalize() for seg in base.split("_"))
    target = f"fhir.{resource}"

    # Skip self-references and non-existent targets
    if target == table_id or target not in all_fhir_names:
        return None

    return target


def parse_lineage_file(path: Path) -> dict | None:
    """Parse a single lineage YAML file and return a table dict.

    Expected YAML structure (new simplified format)::

        dso_lineage:
          Type: dimension
          TableName: dim.Kreditor      # required
          Domain: ERP
          Description: "..."
          Grain: ...                   # optional
          SCD: type2                   # optional
          FhirResource: Patient        # optional
          SourceSystems: [SydAX, ILS]  # optional
          Columns:
            DW_EK_Kreditor:
              Type: PK
            SomeFK:
              Type: FK
              FK-Target: dim.Other     # optional override
            Amount:
              Type: Measure
              DataType: DECIMAL(18,4)
    """
    try:
        with open(path) as f:
            raw = yaml.safe_load(f)
    except yaml.YAMLError as e:
        logger.warning("Skipping %s: YAML parse error: %s", path, e)
        return None

    if not raw or "dso_lineage" not in raw:
        logger.warning("Skipping %s: missing dso_lineage root key", path)
        return None

    lineage = raw["dso_lineage"]

    # Table name from explicit TableName field
    table_name = lineage.get("TableName")
    if not table_name:
        logger.warning("Skipping %s: missing TableName field", path)
        return None

    # Type mapping
    raw_type = lineage.get("Type", lineage.get("type", ""))
    table_type = TYPE_MAP.get(raw_type, raw_type or "unknown")

    # Parse columns
    columns = []
    raw_columns = lineage.get("Columns", lineage.get("columns")) or {}
    for col_name, col_def in raw_columns.items():
        if col_def is None:
            col_def = {}
        if not isinstance(col_def, dict):
            col_def = {}

        col_type_str = col_def.get("Type", "")
        col = {
            "name": col_name,
            "type": col_def.get("DataType", ""),
            "pk": col_type_str == "PK",
            "fk": col_type_str == "FK",
            "measure": col_type_str == "Measure",
            "target": None,
            "fk_target": col_def.get("FK-Target"),
            "source": col_def.get("Source", ""),
            "expr": col_def.get("Expr", ""),
            "description": col_def.get("Description", ""),
            "fhir_path": col_def.get("FhirPath", ""),
        }
        columns.append(col)

    domain = lineage.get("Domain", lineage.get("domain", []))
    if isinstance(domain, str):
        domain = [domain]
    elif not isinstance(domain, list):
        domain = []

    table = {
        "id": table_name,
        "name": table_name,
        "type": table_type,
        "domain": domain,
        "description": lineage.get("Description", lineage.get("description", table_name)),
        "grain": lineage.get("Grain", lineage.get("grain")),
        "scd": lineage.get("SCD", lineage.get("scd")),
        "fhir_resource": lineage.get("FhirResource", lineage.get("fhir_resource")),
        "source_systems": lineage.get("SourceSystems", lineage.get("source_systems", [])),
        "columns": columns,
    }

    return table


def extract_relationships(table: dict, all_tables: list[dict]) -> list[dict]:
    """Extract FK relationships from a parsed table.

    For each FK column, resolve its target via ``_resolve_fk_target``. Then
    classify the relationship:
      - fact -> dim  = ``bus_matrix``
      - fact -> fact = ``fact_to_fact``
      - dim  -> self = ``hierarchy``
      - dim  -> other dim = ``dim_to_dim``

    For FHIR tables, relationships are inferred from ``*_id`` column naming
    conventions (no explicit FK annotations needed).
    """
    all_table_names = {t["name"] for t in all_tables}
    all_fhir_names = {t["name"] for t in all_tables if t["type"] == "fhir"}
    rels = []
    table_id = table["id"]
    table_type = table["type"]

    if table_type == "fhir":
        # Infer relationships from *_id column naming convention
        for col in table.get("columns", []):
            target = _resolve_fhir_target(col["name"], table_id, all_fhir_names)
            if target:
                col["fk"] = True
                col["target"] = target
                rels.append(
                    {
                        "source": table_id,
                        "target": target,
                        "key": col["name"],
                        "type": "fhir_ref",
                    }
                )
        return rels

    for col in table.get("columns", []):
        if not col["fk"] and not col.get("fk_target"):
            continue

        target = _resolve_fk_target(col["name"], col.get("fk_target"), all_table_names)
        col["target"] = target

        # Classify
        if table_type == "fact":
            if target.startswith("fct."):
                rel_type = "fact_to_fact"
            else:
                rel_type = "bus_matrix"
        elif table_type == "dim":
            if target == table_id:
                rel_type = "hierarchy"
            else:
                rel_type = "dim_to_dim"
        else:
            continue

        rels.append(
            {
                "source": table_id,
                "target": target,
                "key": col["name"],
                "type": rel_type,
            }
        )

    return rels


def scan_lineage_directory(lineage_dir: Path) -> list[dict]:
    """Scan a directory tree for ``*.yml`` files and parse each one."""
    tables = []
    for yml_path in sorted(lineage_dir.rglob("*.yml")):
        table = parse_lineage_file(yml_path)
        if table is not None:
            tables.append(table)
    return tables


def build_data(lineage_dir: Path) -> dict:
    """Build the complete data structure for the HTML template."""
    all_tables = scan_lineage_directory(lineage_dir)

    tables = [t for t in all_tables if t["type"] != "fhir"]
    fhir_tables = [t for t in all_tables if t["type"] == "fhir"]

    relationships = []
    for table in all_tables:
        relationships.extend(extract_relationships(table, all_tables))

    # Deduplicate
    seen = set()
    unique_rels = []
    for rel in relationships:
        key = (rel["source"], rel["target"], rel["key"])
        if key not in seen:
            seen.add(key)
            unique_rels.append(rel)

    def _collect_domains(tables: list[dict]) -> list[str]:
        result: set[str] = set()
        for t in tables:
            d = t.get("domain", [])
            if isinstance(d, list):
                result.update(d)
            elif d:
                result.add(d)
        result.discard("Unknown")
        return sorted(result)

    matrix_domains = _collect_domains([t for t in all_tables if t["type"] != "fhir"])
    fhir_domains = _collect_domains([t for t in all_tables if t["type"] == "fhir"])

    return {
        "tables": tables,
        "relationships": unique_rels,
        "domains": {"matrix": matrix_domains, "fhir": fhir_domains},
        "fhirTables": fhir_tables,
    }


def generate_html(data: dict, template_path: Path, output_path: Path) -> None:
    """Replace ``{{DATA}}`` in the template with a JSON blob and write output."""
    template = template_path.read_text(encoding="utf-8")
    data_json = json.dumps(data, ensure_ascii=False, indent=None)
    html = template.replace("{{DATA}}", data_json)
    output_path.write_text(html, encoding="utf-8")
    logger.info("Generated %s", output_path)


def main():
    parser = argparse.ArgumentParser(
        description="Generate star schema HTML from lineage YAML files."
    )
    parser.add_argument(
        "--lineage-dir",
        type=Path,
        default=Path(__file__).parent.parent.parent,
        help="Path to the data_lineage directory",
    )
    parser.add_argument(
        "--template",
        type=Path,
        default=Path(__file__).parent / "star_schema_template.html",
        help="Path to the HTML template",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent / "star_schema.html",
        help="Path for the generated HTML output",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    if not args.lineage_dir.is_dir():
        logger.error("Lineage directory not found: %s", args.lineage_dir)
        raise SystemExit(1)
    if not args.template.is_file():
        logger.error("Template file not found: %s", args.template)
        raise SystemExit(1)

    data = build_data(args.lineage_dir)
    logger.info(
        "Parsed %d tables (%d dim/fact, %d fhir), %d relationships",
        len(data["tables"]) + len(data["fhirTables"]),
        len(data["tables"]),
        len(data["fhirTables"]),
        len(data["relationships"]),
    )

    generate_html(data, args.template, args.output)


if __name__ == "__main__":
    main()

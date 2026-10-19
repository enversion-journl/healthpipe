# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Generate star schema HTML from data-lineage YAML files.

Simplified format: YAML uses capitalized keys (Description, Type, Domain, Grain,
SourceSystems, Columns) and FK relationships are inferred from the DW_EK_/dw_ek_
naming convention or explicitly set via FK-Target.
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
    # Bridges (many-to-many link tables) render like facts: a row in the bus
    # matrix with bus_matrix edges to the dims they connect.
    "bridge": "fact",
    "fhir": "fhir",
}


def _entity_name(table_name: str) -> str | None:
    """Return the entity suffix from ``dim.X`` or ``fct.X``."""
    if "." not in table_name:
        return None
    return table_name.split(".", 1)[1]


def _normalize_notes(value: object) -> list[str]:
    """Normalize ``Notes`` from YAML (list or multiline string) to a string list."""
    if not value:
        return []
    if isinstance(value, str):
        return [line.strip() for line in value.strip().splitlines() if line.strip()]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def _normalize_privacy(value: object) -> str | None:
    """Normalize ``Privacy`` from YAML to a single trimmed string."""
    if not value:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    text = str(value).strip()
    return text or None


def _resolve_fk_target(col_name: str, fk_target: str | None, all_table_names: set[str]) -> str:
    """Resolve the target table for an FK column.

    If *fk_target* is set (from the YAML ``FK-Target`` field), return it,
    canonicalized to the casing in *all_table_names* when a case-insensitive
    match exists. Otherwise strip the ``DW_EK_``/``dw_ek_`` prefix (both casings
    are recognized — legacy columns use uppercase, new/renamed columns use
    lowercase per navngivningsstandard.md), try a full remainder match against
    ``dim.{Remainder}`` (eg. ``BudgetEgenskaber``), then fall back to the first
    segment before ``_`` for role-playing dims (eg. ``Dato_Faktura`` →
    ``dim.Dato``).
    """
    if fk_target:
        return _canonicalize(fk_target, all_table_names)

    if col_name.startswith("DW_EK_") or col_name.startswith("dw_ek_"):
        remainder = col_name[6:]
    else:
        remainder = col_name

    full_match = _canonicalize(f"dim.{remainder}", all_table_names)
    if full_match in all_table_names:
        return full_match

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
    fk_target: str | None = None,
) -> str | None:
    """Resolve a FHIR ``*_id`` column to its target table.

    An explicit ``FK-Target`` wins; otherwise ``{resource}_id`` maps to ``fhir.{Resource}``.
    Compound names like ``episode_of_care_id`` become ``fhir.EpisodeOfCare``.
    Prefixes like ``managing_`` are stripped (``managing_organization_id`` →
    ``fhir.Organization``).  Returns *None* when no matching table exists or
    when the column is a self-reference / source-system identifier.
    """
    if fk_target:
        target = _canonicalize(fk_target, all_fhir_names)
        if target == table_id:
            return None
        if target not in all_fhir_names:
            logger.warning(
                "%s.%s: FK-Target %s matches no FHIR table", table_id, col_name, fk_target
            )
            return None
        return target

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
            dw_ek_Kreditor:
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
        role = col_def.get("Role", col_def.get("role", ""))
        col = {
            "name": col_name,
            "type": col_def.get("DataType", ""),
            "pk": col_type_str == "PK",
            "fk": col_type_str == "FK",
            "measure": col_type_str == "Measure",
            "audit": col_type_str == "Audit",
            "target": None,
            "fk_target": col_def.get("FK-Target"),
            "role": role,
            "pk_and_fk": role == "pk_and_fk",
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

    dim_model = lineage.get("DimModel", lineage.get("dim_model"))
    transaction_dim_for = lineage.get("TransactionDimFor", lineage.get("transaction_dim_for"))

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
        "dim_model": dim_model,
        "transaction_dim_for": transaction_dim_for,
        "privacy": _normalize_privacy(lineage.get("Privacy", lineage.get("privacy"))),
        "notes": _normalize_notes(lineage.get("Notes", lineage.get("notes"))),
        "columns": columns,
    }

    return table


def classify_dim_models(tables: list[dict]) -> None:
    """Mark transaction-dimension facts/dims and infer ``pk_and_fk`` on grain keys."""
    table_by_name = {t["name"]: t for t in tables}
    transaction_dim_targets: set[str] = set()

    for table in tables:
        if table["type"] != "fact":
            continue
        fact_entity = _entity_name(table["name"])
        if not fact_entity:
            continue

        for col in table["columns"]:
            if not col["pk"]:
                continue
            fk_target = col.get("fk_target")
            if not fk_target or not fk_target.startswith("dim."):
                continue

            target_entity = _entity_name(fk_target)
            if col.get("role") == "pk_and_fk" or (
                target_entity and target_entity.lower() == fact_entity.lower()
            ):
                col["pk_and_fk"] = True
                transaction_dim_targets.add(_canonicalize(fk_target, set(table_by_name.keys())))

    for table in tables:
        if table["type"] != "dim":
            continue

        dim_entity = _entity_name(table["name"])
        explicit = table.get("dim_model") == "transaction"
        paired_fact = f"fct.{dim_entity}" if dim_entity else None
        fact_has_dual_key = (
            paired_fact in table_by_name and table["name"] in transaction_dim_targets
        )
        transaction_for = table.get("transaction_dim_for")
        if explicit or fact_has_dual_key or transaction_for:
            table["dim_model"] = "transaction"


def extract_relationships(table: dict, all_tables: list[dict]) -> list[dict]:
    """Extract FK relationships from a parsed table.

    For each FK column, resolve its target via ``_resolve_fk_target``. Then
    classify the relationship:
      - fact -> dim  = ``bus_matrix``
      - fact -> fact = ``fact_to_fact``
      - dim  -> self = ``hierarchy``
      - dim  -> other dim = ``dim_to_dim``

    For FHIR tables, an explicit ``FK-Target`` wins; otherwise relationships are
    inferred from ``*_id`` column naming conventions.
    """
    all_table_names = {t["name"] for t in all_tables}
    all_fhir_names = {t["name"] for t in all_tables if t["type"] == "fhir"}
    rels = []
    table_id = table["id"]
    table_type = table["type"]

    if table_type == "fhir":
        # Explicit FK-Target first, then the *_id column naming convention
        for col in table.get("columns", []):
            target = _resolve_fhir_target(
                col["name"], table_id, all_fhir_names, col.get("fk_target")
            )
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
        is_relationship = col["fk"] or col.get("fk_target") or col.get("pk_and_fk")
        if not is_relationship:
            continue

        target = _resolve_fk_target(col["name"], col.get("fk_target"), all_table_names)
        col["target"] = target

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

        rel_entry: dict = {
            "source": table_id,
            "target": target,
            "key": col["name"],
            "type": rel_type,
        }
        if col.get("pk_and_fk"):
            rel_entry["dual_key"] = True

        rels.append(rel_entry)

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
    classify_dim_models(all_tables)

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

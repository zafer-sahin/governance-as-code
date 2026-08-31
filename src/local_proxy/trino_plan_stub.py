"""
governance/src/local_proxy/trino_plan_stub.py

Trino Execution Plan stub generator for LocalProxy dry-run (Plane 1 CQRS).

Responsibility:
  - Accept a raw SQL string submitted by a developer (IDE / Jupyter / ML Platform).
  - Detect table references and infer classification constraints from them.
  - Return a deterministic, structured fake Trino Execution Plan dict that
    faithfully mirrors the shape of a real `EXPLAIN (FORMAT JSON)` response.

Why a stub?
  - The LocalProxy operates in the developer's local environment where a live
    Trino cluster may not be reachable.
  - The stub is used in dry-run validation (CQRS query-only side) to let the
    governance policy engine evaluate constraints WITHOUT executing the query.
  - A real implementation would call `POST /v1/statement` or
    `EXPLAIN (FORMAT JSON) <sql>` on a connected Trino gateway; this module
    provides a deterministic replacement for tests and offline validation.

Design:
  - Pure functions — no I/O, no global state, no side effects.
  - Returns immutable-friendly plain dicts (JSON-serialisable).
  - PII detection is table-name-based; a real implementation would query
    Atlas classifications via the CQRS read path.
  - The plan structure mirrors Trino's internal `PlanNode` JSON representation
    as returned by the coordinator REST API.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Internal constants
# ---------------------------------------------------------------------------

# Tables known to carry PII classifications (in production these come
# from an Atlas CQRS read; here they are statically declared for the stub).
_PII_TABLES: frozenset[str] = frozenset(
    {
        "prod.pii_table",
        "prod.customer_pii",
        "prod.user_profiles",
        "prod.payment_details",
    }
)

# Regex that matches fully-qualified table references in FROM / JOIN clauses.
# Handles:  FROM prod.pii_table
#           JOIN prod.pii_table ON ...
#           FROM prod.pii_table t
#           FROM `prod`.`pii_table`   (backtick-quoted)
_TABLE_REF_RE = re.compile(
    r"(?:FROM|JOIN)\s+[`\"]?(\w+)[`\"]?\.[`\"]?(\w+)[`\"]?",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_trino_execution_plan(sql: str) -> dict[str, Any]:
    """
    Produce a fake Trino Execution Plan dictionary for the given SQL string.

    The returned dict mirrors the shape of a real Trino
    ``EXPLAIN (FORMAT JSON)`` response so that downstream consumers
    (LocalProxy policy evaluator) can operate on it identically to a
    live plan.

    Detection rules
    ---------------
    * If the SQL references a table that is in the PII registry, the
      corresponding ``TableScan`` node will carry:
        - ``constraint``:        ``{"predicates": "PII_ENFORCEMENT", "tag": "PII"}``
        - ``enforcedConstraint``: ``{"tag": "PII", "masking": "HASH"}``
    * ``SELECT *`` is flagged in the ``Output`` node descriptor so the policy
      engine can decide whether to surface a ``MASKING_REQUIRED`` ADT variant.
    * Each plan node receives a deterministic UUID derived from the SQL hash
      so that the same query always produces the same plan shape (idempotent
      dry-run).

    Parameters
    ----------
    sql:
        Raw SQL string exactly as submitted by the developer.

    Returns
    -------
    dict[str, Any]
        A JSON-serialisable plan dict.  Top-level keys:

        ``id``           — Query ID (deterministic UUID from SQL hash)
        ``query``        — The original SQL string (echoed back)
        ``capturedAt``   — ISO-8601 UTC timestamp of stub generation
        ``isStub``       — Always True; downstream may use this to gate live calls
        ``inputTables``  — List of detected fully-qualified table names
        ``piiTables``    — Subset of inputTables that carry PII classifications
        ``hasSelectStar``— Whether the query contains SELECT *
        ``plan``         — Root PlanNode tree (mirrors Trino JSON plan shape)

    Examples
    --------
    >>> plan = generate_trino_execution_plan(
    ...     "SELECT * FROM prod.pii_table WHERE customer_id = 42"
    ... )
    >>> plan["piiTables"]
    ['prod.pii_table']
    >>> plan["plan"]["name"]
    'Output'
    >>> plan["plan"]["children"][0]["name"]
    'Project'
    >>> plan["plan"]["children"][0]["children"][0]["name"]
    'Filter'
    >>> plan["plan"]["children"][0]["children"][0]["children"][0]["name"]
    'TableScan'
    >>> plan["plan"]["children"][0]["children"][0]["children"][0]["descriptor"]["constraint"]["tag"]
    'PII'
    """
    sql_stripped = sql.strip()
    query_id = _deterministic_query_id(sql_stripped)
    input_tables = _extract_table_refs(sql_stripped)
    pii_tables = [t for t in input_tables if t.lower() in _PII_TABLES]
    has_select_star = bool(re.search(r"SELECT\s+\*", sql_stripped, re.IGNORECASE))

    plan_root = _build_plan_tree(
        query_id=query_id,
        input_tables=input_tables,
        pii_tables=pii_tables,
        has_select_star=has_select_star,
        sql=sql_stripped,
    )

    return {
        "id": query_id,
        "query": sql_stripped,
        "capturedAt": datetime.now(timezone.utc).isoformat(),
        "isStub": True,
        "inputTables": input_tables,
        "piiTables": pii_tables,
        "hasSelectStar": has_select_star,
        "plan": plan_root,
    }


# ---------------------------------------------------------------------------
# SQL analysis helpers
# ---------------------------------------------------------------------------


def _extract_table_refs(sql: str) -> list[str]:
    """
    Extract all fully-qualified table references from a SQL string.

    Returns a deduplicated, ordered list of 'schema.table' strings.
    """
    matches = _TABLE_REF_RE.findall(sql)
    seen: dict[str, None] = {}
    for schema, table in matches:
        key = f"{schema}.{table}"
        seen[key] = None
    return list(seen)


def _deterministic_query_id(sql: str) -> str:
    """
    Derive a deterministic UUID from the SQL content so that the same query
    always produces the same plan shape — essential for idempotent dry-runs.
    """
    digest = hashlib.sha256(sql.encode()).digest()
    return str(uuid.UUID(bytes=digest[:16]))


def _node_id(query_id: str, suffix: str) -> str:
    """Produce a deterministic node ID scoped to a specific plan node."""
    raw = f"{query_id}:{suffix}"
    digest = hashlib.md5(raw.encode()).digest()
    return str(uuid.UUID(bytes=digest))


# ---------------------------------------------------------------------------
# Plan tree builder
# ---------------------------------------------------------------------------


def _build_plan_tree(
    *,
    query_id: str,
    input_tables: list[str],
    pii_tables: list[str],
    has_select_star: bool,
    sql: str,
) -> dict[str, Any]:
    """
    Construct the nested PlanNode tree.

    Tree shape (mirrors Trino's logical plan for a single-table SELECT):

        Output
          └── Project          (column projection — flagged if SELECT *)
                └── Filter     (WHERE clause; present even if trivially true)
                      └── TableScan   (one per input table)

    For multi-table queries (JOINs) the Filter children are extended;
    this stub focuses on the primary table.
    """
    # Build bottom-up: TableScan(s) → Filter → Project → Output
    table_scan_nodes = [
        _table_scan_node(query_id, table, table in pii_tables)
        for table in (input_tables or ["<unknown>"])
    ]

    filter_node = _filter_node(query_id, sql=sql, children=table_scan_nodes)
    project_node = _project_node(
        query_id, has_select_star=has_select_star, children=[filter_node]
    )
    output_node = _output_node(
        query_id,
        has_select_star=has_select_star,
        pii_tables=pii_tables,
        children=[project_node],
    )
    return output_node


# ---------------------------------------------------------------------------
# Individual PlanNode constructors
# ---------------------------------------------------------------------------


def _table_scan_node(
    query_id: str,
    table: str,
    is_pii: bool,
) -> dict[str, Any]:
    """
    Trino TableScan node — the physical leaf that reads from a connector.

    When the table carries a PII classification the node exposes:
      descriptor.constraint.tag         = "PII"
      descriptor.enforcedConstraint.tag = "PII"
      descriptor.enforcedConstraint.masking = "HASH"
    """
    schema, _, tbl = table.partition(".")
    catalog = "trino_prod"

    constraint: dict[str, Any] = (
        {
            "predicates": "PII_ENFORCEMENT",
            "tag": "PII",
            "source": "atlas_classification",
            "propagated": True,
        }
        if is_pii
        else {
            "predicates": "NONE",
        }
    )

    enforced_constraint: dict[str, Any] = (
        {
            "tag": "PII",
            "masking": "HASH",
            "maskingExpression": "to_hex(sha256(to_utf8(CAST({col} AS varchar))))",
            "rangerPolicy": "ds-customer-pii-hashed-read",
        }
        if is_pii
        else {}
    )

    return {
        "id": _node_id(query_id, f"TableScan:{table}"),
        "name": "TableScan",
        "descriptor": {
            "table": f"{catalog}:{schema}:{tbl}",
            "qualifiedName": table,
            "catalogName": catalog,
            "schemaName": schema,
            "tableName": tbl,
            "constraint": constraint,
            "enforcedConstraint": enforced_constraint,
            "isPiiSource": is_pii,
        },
        "outputs": _column_outputs(table, is_pii),
        "details": [
            f"table = {catalog}:{schema}:{tbl}",
            f"constraint = {constraint['predicates']}",
        ],
        "children": [],
        "estimatedStats": {
            "outputRowCount": 1_000_000 if is_pii else 50_000,
            "outputSizeInBytes": 104_857_600 if is_pii else 5_242_880,
            "cpuCost": 1.0e8,
            "memoryCost": 0.0,
            "networkCost": 0.0,
        },
    }


def _filter_node(
    query_id: str,
    sql: str,
    children: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Trino Filter node — represents WHERE clause predicate evaluation.
    """
    predicate = _extract_where_clause(sql) or "true"
    return {
        "id": _node_id(query_id, "Filter"),
        "name": "Filter",
        "descriptor": {
            "predicate": predicate,
        },
        "outputs": children[0]["outputs"] if children else [],
        "details": [f"filterPredicate = {predicate}"],
        "children": children,
        "estimatedStats": {
            "outputRowCount": 100_000,
            "outputSizeInBytes": 10_485_760,
            "cpuCost": 5.0e7,
            "memoryCost": 0.0,
            "networkCost": 0.0,
        },
    }


def _project_node(
    query_id: str,
    has_select_star: bool,
    children: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Trino Project node — column projection layer.

    SELECT * is flagged so that the policy engine can decide whether
    to emit a MASKING_REQUIRED or DENY ADT variant.
    """
    child_outputs = children[0]["outputs"] if children else []
    return {
        "id": _node_id(query_id, "Project"),
        "name": "Project",
        "descriptor": {
            "isSelectStar": has_select_star,
            "projectedColumns": child_outputs,
            "governanceNote": (
                "SELECT * on PII source — masking policy will be applied"
                if has_select_star
                else "explicit column projection"
            ),
        },
        "outputs": child_outputs,
        "details": [
            "expressions = " + (
                "* (all columns — PII masking enforced)" if has_select_star
                else ", ".join(c["name"] for c in child_outputs)
            )
        ],
        "children": children,
        "estimatedStats": {
            "outputRowCount": 100_000,
            "outputSizeInBytes": 10_485_760,
            "cpuCost": 1.0e6,
            "memoryCost": 0.0,
            "networkCost": 0.0,
        },
    }


def _output_node(
    query_id: str,
    has_select_star: bool,
    pii_tables: list[str],
    children: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Trino Output node — the root of the plan tree.

    Carries the top-level governance summary consumed by LocalProxy.
    """
    child_outputs = children[0]["outputs"] if children else []
    return {
        "id": _node_id(query_id, "Output"),
        "name": "Output",
        "descriptor": {
            "columnNames": [c["name"] for c in child_outputs],
            "isSelectStar": has_select_star,
        },
        "outputs": child_outputs,
        "details": [
            "Output[" + ", ".join(c["name"] for c in child_outputs) + "]"
        ],
        "children": children,
        # Top-level governance summary — consumed directly by LocalProxy
        "governanceSummary": {
            "inputTables": [c["descriptor"]["qualifiedName"]
                            for c in _collect_table_scans(children)],
            "piiSourceTables": pii_tables,
            "classificationTags": list({"PII"} if pii_tables else set()),
            "requiresMasking": bool(pii_tables) and has_select_star,
            "selectStarOnPii": has_select_star and bool(pii_tables),
            "recommendedAdtVariant": (
                "Mask(columns)"
                if pii_tables and has_select_star
                else "Allow"
                if not pii_tables
                else "Allow"
            ),
        },
        "estimatedStats": {
            "outputRowCount": 100_000,
            "outputSizeInBytes": 10_485_760,
            "cpuCost": 0.0,
            "memoryCost": 0.0,
            "networkCost": 1.0e8,
        },
    }


# ---------------------------------------------------------------------------
# Schema / column helpers
# ---------------------------------------------------------------------------

_PII_SCHEMA: dict[str, list[dict[str, str]]] = {
    "prod.pii_table": [
        {"name": "customer_id",    "type": "bigint",    "pii": "false"},
        {"name": "full_name",      "type": "varchar",   "pii": "true",  "maskType": "HASH"},
        {"name": "email",          "type": "varchar",   "pii": "true",  "maskType": "HASH"},
        {"name": "date_of_birth",  "type": "date",      "pii": "true",  "maskType": "MASK_SHOW_LAST_4"},
        {"name": "phone_number",   "type": "varchar",   "pii": "true",  "maskType": "HASH"},
        {"name": "created_at",     "type": "timestamp", "pii": "false"},
    ],
    "prod.customer_pii": [
        {"name": "id",             "type": "bigint",    "pii": "false"},
        {"name": "ssn",            "type": "varchar",   "pii": "true",  "maskType": "NULLIFY"},
        {"name": "credit_card",    "type": "varchar",   "pii": "true",  "maskType": "MASK_SHOW_LAST_4"},
        {"name": "address",        "type": "varchar",   "pii": "true",  "maskType": "HASH"},
    ],
}

_DEFAULT_SCHEMA: list[dict[str, str]] = [
    {"name": "id",         "type": "bigint",  "pii": "false"},
    {"name": "value",      "type": "varchar", "pii": "false"},
    {"name": "created_at", "type": "timestamp", "pii": "false"},
]


def _column_outputs(table: str, is_pii: bool) -> list[dict[str, Any]]:
    schema = _PII_SCHEMA.get(table.lower(), _DEFAULT_SCHEMA) if is_pii else _DEFAULT_SCHEMA
    return [
        {
            "name": col["name"],
            "type": col["type"],
            "isPii": col.get("pii", "false") == "true",
            "maskType": col.get("maskType", "NONE"),
        }
        for col in schema
    ]


def _extract_where_clause(sql: str) -> str | None:
    """Extract the WHERE clause text from a SQL string (best-effort)."""
    match = re.search(r"WHERE\s+(.+?)(?:ORDER BY|GROUP BY|LIMIT|$)", sql, re.IGNORECASE | re.DOTALL)
    return match.group(1).strip() if match else None


def _collect_table_scans(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Depth-first traversal to collect all TableScan nodes."""
    result: list[dict[str, Any]] = []
    for node in nodes:
        if node.get("name") == "TableScan":
            result.append(node)
        result.extend(_collect_table_scans(node.get("children", [])))
    return result

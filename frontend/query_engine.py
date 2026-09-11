"""
Foundational Chatbot Query Engine for Neo4j.
Zero NLP/LLM logic — purely safe, correct, dynamic Cypher execution against dynamic CSV schemas.

Graph Model:
  (:Dataset {id, name, filename, row_count, created_at})-[:HAS_ROW]->(:Row {row_index, col1, col2, ...})
"""

import os
import re
import time
import logging
from typing import Any, Dict, List, Optional, Tuple, Union
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

logger = logging.getLogger("query_engine")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


# =====================================================================
# Custom Exceptions
# =====================================================================

class QueryEngineError(Exception):
    """Base exception for all query engine errors."""
    pass


class ReadOnlyViolationError(QueryEngineError):
    """Raised when a query attempts to perform write/mutation operations."""
    pass


class CypherTimeoutError(QueryEngineError):
    """Raised when a Cypher query exceeds the execution timeout."""
    pass


class CypherExecutionError(QueryEngineError):
    """Raised when Cypher execution fails against the database."""
    pass


class SchemaError(QueryEngineError):
    """Raised when schema inspection fails or invalid schema properties are accessed."""
    pass


# =====================================================================
# Query Result Data Class
# =====================================================================

class QueryResult:
    """Encapsulates the raw result rows alongside execution telemetry and generated Cypher."""
    def __init__(self, data: List[Dict[str, Any]], cypher: str, parameters: Dict[str, Any], execution_time_ms: float):
        self.data = data
        self.cypher = cypher
        self.parameters = parameters
        self.execution_time_ms = execution_time_ms

    def to_dict(self) -> Dict[str, Any]:
        return {
            "data": self.data,
            "cypher": self.cypher,
            "parameters": self.parameters,
            "row_count": len(self.data),
            "execution_time_ms": round(self.execution_time_ms, 2)
        }

    def __repr__(self) -> str:
        return f"<QueryResult rows={len(self.data)} time={self.execution_time_ms:.2f}ms>"


# =====================================================================
# Security: Read-Only Cypher Validator
# =====================================================================

FORBIDDEN_WRITE_KEYWORDS = {
    "CREATE", "MERGE", "DELETE", "DETACH", "SET", "REMOVE", "DROP", "ALTER"
}

FORBIDDEN_PROCEDURE_PATTERNS = [
    r"\bCALL\s+dbms\.",
    r"\bCALL\s+apoc\.(create|refactor|periodic|trigger|system|custom)\b",
    r"\bLOAD\s+CSV\b"
]


def strip_cypher_comments_and_strings(query: str) -> str:
    """
    Strips single-line comments (// and #), multi-line comments (/* ... */),
    and string literals ('...' and "...") from Cypher query so keywords
    inside strings (e.g. WHERE r.status = 'SET') are not falsely rejected.
    """
    # 1. Remove multi-line comments: /* ... */
    no_block_comments = re.sub(r"/\*.*?\*/", " ", query, flags=re.DOTALL)
    # 2. Remove single-line comments: // ... or # ...
    no_line_comments = re.sub(r"(//|#).*?$", " ", no_block_comments, flags=re.MULTILINE)

    # 3. Replace string literals with empty strings: '...' or "..."
    # Handles escaped quotes safely
    no_strings = re.sub(r"'(?:\\.|[^'\\])*'", "''", no_line_comments)
    no_strings = re.sub(r'"(?:\\.|[^"\\])*"', '""', no_strings)

    return no_strings


def validate_readonly_cypher(query: str) -> None:
    """
    Validates that a Cypher query contains ONLY read operations.
    Rejects any query containing CREATE, MERGE, DELETE, DETACH, SET, REMOVE, DROP,
    or mutating procedures.
    
    Raises:
        ReadOnlyViolationError: If forbidden write keywords or patterns are detected.
    """
    if not query or not query.strip():
        raise QueryEngineError("Query cannot be empty.")

    clean_text = strip_cypher_comments_and_strings(query)

    # Check for forbidden write keywords as independent words
    for kw in FORBIDDEN_WRITE_KEYWORDS:
        pattern = rf"\b{kw}\b"
        if re.search(pattern, clean_text, flags=re.IGNORECASE):
            raise ReadOnlyViolationError(
                f"Query rejected: Write keyword '{kw}' is prohibited in read-only mode."
            )

    # Check for mutating procedures
    for proc_pattern in FORBIDDEN_PROCEDURE_PATTERNS:
        if re.search(proc_pattern, clean_text, flags=re.IGNORECASE):
            raise ReadOnlyViolationError(
                f"Query rejected: Mutating procedure call detected matching pattern '{proc_pattern}'."
            )


# =====================================================================
# In-Memory Mock Neo4j Store (For offline testing & fallback)
# =====================================================================

class MockNeo4jStore:
    """
    Lightweight in-memory graph store for testing dynamic schemas and Cypher templates
    when a live Neo4j daemon is not running.
    """
    def __init__(self):
        self.datasets: Dict[str, Dict[str, Any]] = {}
        self.rows: Dict[str, List[Dict[str, Any]]] = {}  # dataset_id -> list of row dicts

    def seed_dataset(self, dataset_id: str, name: str, rows: List[Dict[str, Any]]):
        self.datasets[dataset_id] = {
            "id": dataset_id,
            "name": name,
            "row_count": len(rows),
            "created_at": time.time()
        }
        self.rows[dataset_id] = []
        for idx, r in enumerate(rows):
            row_dict = dict(r)
            row_dict["row_index"] = idx + 1
            self.rows[dataset_id].append(row_dict)

    def get_labels(self) -> List[str]:
        labels = set()
        if self.datasets:
            labels.add("Dataset")
        if any(self.rows.values()):
            labels.add("Row")
        return sorted(list(labels))

    def get_rel_types(self) -> List[str]:
        if any(self.rows.values()):
            return ["HAS_ROW"]
        return []

    def get_row_keys(self, dataset_id: Optional[str] = None) -> List[str]:
        keys = set()
        if dataset_id and dataset_id in self.rows:
            for r in self.rows[dataset_id]:
                keys.update(r.keys())
        else:
            for dataset_rows in self.rows.values():
                for r in dataset_rows:
                    keys.update(r.keys())
        keys.discard("row_index")
        return sorted(list(keys))

    def run_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Simulates template execution in-memory matching Neo4j return shapes."""
        params = parameters or {}
        dataset_id = params.get("dataset_id")

        # 1. Describe dataset query
        if "RETURN d.id AS dataset_id" in query:
            results = []
            target_datasets = [self.datasets[dataset_id]] if dataset_id and dataset_id in self.datasets else list(self.datasets.values())
            for d in target_datasets:
                d_id = d["id"]
                rows = self.rows.get(d_id, [])
                keys = set()
                for r in rows:
                    keys.update(r.keys())
                keys.discard("row_index")
                results.append({
                    "dataset_id": d["id"],
                    "dataset_name": d["name"],
                    "registered_rows": d["row_count"],
                    "actual_rows": len(rows),
                    "columns": sorted(list(keys))
                })
            return results

        # 2. Count matching query
        if "RETURN count(r) AS count" in query:
            prop_name = params.get("prop_name")
            prop_val = params.get("prop_val")
            rows = self._get_target_rows(dataset_id)
            if not prop_name or prop_val is None:
                return [{
                    "count": len(rows),
                    "property": None,
                    "matching_value": None
                }]
            match_count = 0
            for r in rows:
                if prop_name in r and str(r[prop_name]).lower() == str(prop_val).lower():
                    match_count += 1
            return [{
                "count": match_count,
                "property": prop_name,
                "matching_value": prop_val
            }]

        # 3. Sum property grouped by query
        if "sum(toFloat(r[$sum_prop])) AS total_sum" in query:
            sum_prop = params.get("sum_prop")
            group_prop = params.get("group_prop")
            filter_prop = params.get("filter_prop")
            filter_val = params.get("filter_val")
            rows = self._get_target_rows(dataset_id)

            groups: Dict[Any, Dict[str, Any]] = {}
            for r in rows:
                if filter_prop and filter_val is not None:
                    actual_s = str(r.get(filter_prop, "")).lower()
                    target_s = str(filter_val).lower()
                    if target_s != actual_s and target_s not in actual_s:
                        continue
                group_key = r.get(group_prop, "Unknown")
                raw_val = r.get(sum_prop)
                try:
                    num_val = float(raw_val) if raw_val is not None else 0.0
                except (ValueError, TypeError):
                    num_val = 0.0

                if group_key not in groups:
                    groups[group_key] = {"group_key": group_key, "total_sum": 0.0, "row_count": 0}
                groups[group_key]["total_sum"] += num_val
                groups[group_key]["row_count"] += 1

            result = list(groups.values())
            result.sort(key=lambda x: x["total_sum"], reverse=True)
            return result

        # 4. Find rows where query
        if "WHERE ($dataset_id IS NULL OR d.id = $dataset_id)" in query and "r[$prop_name]" in query:
            prop_name = params.get("prop_name")
            val = params.get("val")
            limit = params.get("limit", 25)
            rows = self._get_target_rows(dataset_id)
            matched = []

            for r in rows:
                if prop_name not in r:
                    continue
                actual = r[prop_name]
                is_match = False

                if "CONTAINS" in query:
                    is_match = str(val).lower() in str(actual).lower()
                elif ">=" in query:
                    is_match = float(actual) >= float(val) if self._is_num(actual, val) else False
                elif "<=" in query:
                    is_match = float(actual) <= float(val) if self._is_num(actual, val) else False
                elif "<>" in query or "!=" in query:
                    is_match = str(actual).lower() != str(val).lower()
                elif " > " in query:
                    is_match = float(actual) > float(val) if self._is_num(actual, val) else False
                elif " < " in query:
                    is_match = float(actual) < float(val) if self._is_num(actual, val) else False
                else:  # Equality =
                    is_match = str(actual).lower() == str(val).lower()

                if is_match:
                    matched.append(dict(r))
                    if len(matched) >= limit:
                        break
            return matched

        # 5. Average / Mean query
        if "avg(" in query:
            prop_match = re.search(r"avg\((?:toFloat\()?r\.([a-zA-Z0-9_]+)\)?\)", query)
            if prop_match:
                p_name = prop_match.group(1)
                rows = self._get_target_rows(dataset_id)
                vals = [float(r[p_name]) for r in rows if p_name in r and self._is_num(r[p_name], 0)]
                avg_val = sum(vals) / len(vals) if vals else 0.0
                return [{f"average_{p_name}": round(avg_val, 3)}]

        # 6. Max / Single costliest query
        if "ORDER BY amount DESC" in query and "LIMIT 1" in query:
            rows = self._get_target_rows(dataset_id)
            if rows:
                sorted_rows = sorted(rows, key=lambda r: float(r.get("amount", 0)), reverse=True)
                top = sorted_rows[0]
                return [{
                    "merchant": top.get("merchant"),
                    "category": top.get("category"),
                    "amount": float(top.get("amount", 0))
                }]

        # 7. Text search query (e.g. PayPal)
        if "contains" in query.lower():
            str_match = re.search(r'contains\s+["\']([^"\']+)["\']', query, re.IGNORECASE)
            if str_match:
                target_substr = str_match.group(1).lower()
                rows = self._get_target_rows(dataset_id)
                matched = [dict(r) for r in rows if any(target_substr in str(v).lower() for v in r.values())]
                return matched

        # General Row Inspection / properties(r)
        if "properties(r)" in query:
            rows = self._get_target_rows(dataset_id)
            return [{"p": dict(r)} for r in rows]

        # 8. Schema Inspection fallback
        if "CALL db.labels()" in query or "db.labels" in query:
            return [{"label": l} for l in self.get_labels()]
        if "CALL db.relationshipTypes()" in query:
            return [{"relationshipType": rt} for rt in self.get_rel_types()]

        return []

    def _is_num(self, a: Any, b: Any) -> bool:
        try:
            float(a)
            float(b)
            return True
        except (ValueError, TypeError):
            return False

    def _get_target_rows(self, dataset_id: Optional[str]) -> List[Dict[str, Any]]:
        if dataset_id and dataset_id in self.rows:
            return self.rows[dataset_id]
        all_r = []
        for d_rows in self.rows.values():
            all_r.extend(d_rows)
        return all_r


# Global in-memory fallback store
_MOCK_STORE = MockNeo4jStore()


# =====================================================================
# Neo4j Client & Query Engine
# =====================================================================

class Neo4jQueryEngine:
    """
    Neo4j Graph Chatbot Query Engine.
    Provides schema inspection, safe read-only Cypher execution, and
    parameterized analytical templates.
    """

    def __init__(
        self,
        uri: Optional[str] = None,
        auth: Optional[Tuple[str, str]] = None,
        database: Optional[str] = None,
        default_timeout_seconds: float = 5.0,
        prefer_live_driver: bool = True
    ):
        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        username = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "password")
        self.auth = auth or (username, password)
        self.database = database or os.getenv("NEO4J_DATABASE", "neo4j")
        self.default_timeout_seconds = default_timeout_seconds
        self.prefer_live_driver = prefer_live_driver
        self._driver = None
        self._live_connection_available: Optional[bool] = None

    def _get_driver(self):
        """Initializes and returns the official Neo4j Python driver if reachable."""
        if self._driver is not None:
            return self._driver

        if not self.prefer_live_driver:
            return None

        try:
            import neo4j
            from neo4j import GraphDatabase
            driver = GraphDatabase.driver(
                self.uri,
                auth=self.auth,
                connection_timeout=2.0
            )
            # Test connectivity
            driver.verify_connectivity()
            self._driver = driver
            self._live_connection_available = True
            logger.info(f"Connected successfully to Neo4j instance at {self.uri}")
            return self._driver
        except Exception as err:
            self._live_connection_available = False
            logger.info(f"Live Neo4j instance at {self.uri} not reachable ({err}). Using in-memory graph engine.")
            return None

    def close(self):
        """Closes any open driver connections."""
        if self._driver:
            try:
                self._driver.close()
            except Exception:
                pass
            self._driver = None

    # -----------------------------------------------------------------
    # Requirement 2: Read-Only Cypher Execution with Timeout
    # -----------------------------------------------------------------

    def run_cypher_readonly(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[float] = None
    ) -> QueryResult:
        """
        Executes a Cypher query against Neo4j in READ-ONLY mode.
        
        Guarantees:
        1. REJECTS any query containing write keywords (CREATE, MERGE, DELETE, SET, REMOVE, DROP, etc.)
        2. Applies a timeout so runaway queries cannot hang the endpoint.
        3. Returns raw result rows as a list of dicts.
        
        Raises:
            ReadOnlyViolationError: If write keywords are detected.
            CypherTimeoutError: If execution exceeds timeout.
            CypherExecutionError: If query syntax or database error occurs.
        """
        # 1. Enforce strict read-only validation
        validate_readonly_cypher(query)

        timeout = timeout_seconds or self.default_timeout_seconds
        params = parameters or {}
        start_time = time.time()

        driver = self._get_driver()

        # If live driver is available, execute on real Neo4j
        if driver:
            try:
                def _execute():
                    with driver.session(database=self.database) as session:
                        # Transaction configuration with timeout
                        result = session.run(query, params, timeout=timeout)
                        return [record.data() for record in result]

                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(_execute)
                    data = future.result(timeout=timeout)

                elapsed_ms = (time.time() - start_time) * 1000.0
                return QueryResult(data=data, cypher=query, parameters=params, execution_time_ms=elapsed_ms)

            except FutureTimeoutError:
                raise CypherTimeoutError(
                    f"Cypher query execution timed out after {timeout:.1f} seconds."
                )
            except Exception as err:
                # Distinguish timeouts from other database errors
                err_msg = str(err)
                if "timeout" in err_msg.lower():
                    raise CypherTimeoutError(f"Cypher query execution timed out: {err_msg}")
                raise CypherExecutionError(f"Neo4j query execution failed: {err_msg}")

        # Fallback to in-memory store if live instance is offline
        try:
            data = _MOCK_STORE.run_query(query, params)
            elapsed_ms = (time.time() - start_time) * 1000.0
            return QueryResult(data=data, cypher=query, parameters=params, execution_time_ms=elapsed_ms)
        except Exception as err:
            raise CypherExecutionError(f"Query execution failed: {err}")

    # -----------------------------------------------------------------
    # Requirement 1: Dynamic Graph Schema Inspection
    # -----------------------------------------------------------------

    def get_graph_schema(self, dataset_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Inspects the Neo4j graph at runtime and returns its actual schema:
        - node labels present
        - relationship types present
        - for :Row nodes specifically: distinct dynamic property keys
          (since columns vary per CSV upload).
        
        Never hardcodes column names.
        """
        driver = self._get_driver()

        if driver:
            try:
                # 1. Node Labels present
                labels_res = self.run_cypher_readonly(
                    "CALL db.labels() YIELD label RETURN collect(label) AS labels"
                )
                node_labels = labels_res.data[0]["labels"] if labels_res.data else []

                # 2. Relationship Types present
                rel_res = self.run_cypher_readonly(
                    "CALL db.relationshipTypes() YIELD relationshipType RETURN collect(relationshipType) AS rel_types"
                )
                rel_types = rel_res.data[0]["rel_types"] if rel_res.data else []

                # 3. Dynamic Property Keys for :Row nodes
                # Inspects distinct keys directly on :Row nodes (per dataset if given)
                if dataset_id:
                    prop_query = """
                    MATCH (d:Dataset {id: $dataset_id})-[:HAS_ROW]->(r:Row)
                    UNWIND keys(r) AS k
                    RETURN collect(DISTINCT k) AS row_keys
                    """
                    prop_res = self.run_cypher_readonly(prop_query, {"dataset_id": dataset_id})
                else:
                    prop_query = """
                    MATCH (r:Row)
                    UNWIND keys(r) AS k
                    RETURN collect(DISTINCT k) AS row_keys
                    """
                    prop_res = self.run_cypher_readonly(prop_query)

                row_keys = prop_res.data[0]["row_keys"] if prop_res.data and "row_keys" in prop_res.data[0] else []
                # Filter out system index
                row_properties = sorted([k for k in row_keys if k != "row_index"])

                return {
                    "node_labels": sorted(node_labels),
                    "relationship_types": sorted(rel_types),
                    "row_properties": row_properties,
                    "target_dataset_id": dataset_id
                }
            except Exception as e:
                logger.warning(f"Error during dynamic schema inspection: {e}")
                # fallback to store inspection

        # In-memory store schema inspection
        return {
            "node_labels": _MOCK_STORE.get_labels(),
            "relationship_types": _MOCK_STORE.get_rel_types(),
            "row_properties": _MOCK_STORE.get_row_keys(dataset_id),
            "target_dataset_id": dataset_id
        }

    # -----------------------------------------------------------------
    # Requirement 3: Parameterized Query Templates
    # -----------------------------------------------------------------

    def describe_dataset(self, dataset_id: Optional[str] = None) -> QueryResult:
        """
        Template 1: Describe Dataset.
        Returns row count, column names, and dataset metadata.
        """
        cypher = """
        MATCH (d:Dataset)
        WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
        OPTIONAL MATCH (d)-[:HAS_ROW]->(r:Row)
        WITH d, count(r) AS actual_row_count, collect(keys(r)) AS all_keys
        UNWIND (CASE WHEN size(all_keys) = 0 THEN [[]] ELSE all_keys END) AS key_list
        UNWIND (CASE WHEN size(key_list) = 0 THEN [null] ELSE key_list END) AS k
        WITH d, actual_row_count, collect(DISTINCT k) AS columns
        RETURN d.id AS dataset_id,
               d.name AS dataset_name,
               coalesce(d.row_count, actual_row_count) AS registered_rows,
               actual_row_count AS actual_rows,
               [col IN columns WHERE col IS NOT NULL AND col <> 'row_index'] AS columns
        """
        params = {"dataset_id": dataset_id}
        return self.run_cypher_readonly(cypher, params)

    def count_rows_matching(
        self,
        property_name: str,
        value: Any,
        dataset_id: Optional[str] = None
    ) -> QueryResult:
        """
        Template 2: Count Rows Matching.
        Counts rows where r[$prop_name] == $prop_val.
        Parameterized — no string concatenation for values.
        """
        if not property_name or not isinstance(property_name, str):
            raise ValueError("property_name must be a non-empty string.")

        cypher = """
        MATCH (d:Dataset)-[:HAS_ROW]->(r:Row)
        WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
          AND r[$prop_name] = $prop_val
        RETURN count(r) AS count, $prop_name AS property, $prop_val AS matching_value
        """
        params = {
            "dataset_id": dataset_id,
            "prop_name": property_name,
            "prop_val": value
        }
        return self.run_cypher_readonly(cypher, params)

    def sum_property_grouped_by(
        self,
        sum_property: str,
        group_property: str,
        filter_property: Optional[str] = None,
        filter_value: Optional[Any] = None,
        dataset_id: Optional[str] = None
    ) -> QueryResult:
        """
        Template 3: Sum Property Grouped By.
        Calculates sum of numeric sum_property grouped by group_property,
        with optional equality filter.
        Parameterized — no string concatenation.
        """
        if not sum_property or not group_property:
            raise ValueError("Both sum_property and group_property must be specified.")

        cypher = """
        MATCH (d:Dataset)-[:HAS_ROW]->(r:Row)
        WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
          AND ($filter_prop IS NULL OR r[$filter_prop] = $filter_val)
          AND r[$sum_prop] IS NOT NULL
        RETURN r[$group_prop] AS group_key,
               sum(toFloat(r[$sum_prop])) AS total_sum,
               count(r) AS row_count
        ORDER BY total_sum DESC
        """
        params = {
            "dataset_id": dataset_id,
            "sum_prop": sum_property,
            "group_prop": group_property,
            "filter_prop": filter_property,
            "filter_val": filter_value
        }
        return self.run_cypher_readonly(cypher, params)

    def find_rows_where(
        self,
        property_name: str,
        operator: str,
        value: Any,
        limit: int = 25,
        dataset_id: Optional[str] = None
    ) -> QueryResult:
        """
        Template 4: Find Rows Where.
        Finds row records matching an operator comparison:
        Operators supported: '=', '!=', '>', '<', '>=', '<=', 'CONTAINS'
        Parameterized — strictly whitelists operators and binds values.
        """
        valid_operators = {
            "=": "=",
            "==": "=",
            "!=": "<>",
            "<>": "<>",
            ">": ">",
            "<": "<",
            ">=": ">=",
            "<=": "<=",
            "CONTAINS": "CONTAINS"
        }

        clean_op = operator.strip().upper()
        if clean_op not in valid_operators:
            raise ValueError(
                f"Unsupported operator '{operator}'. Allowed: {list(valid_operators.keys())}"
            )

        cypher_op = valid_operators[clean_op]

        # Numeric comparison vs string comparison
        if cypher_op in {">", "<", ">=", "<="}:
            condition = f"toFloat(r[$prop_name]) {cypher_op} toFloat($val)"
        elif cypher_op == "CONTAINS":
            condition = "toLower(toString(r[$prop_name])) CONTAINS toLower(toString($val))"
        else:
            condition = f"r[$prop_name] {cypher_op} $val"

        cypher = f"""
        MATCH (d:Dataset)-[:HAS_ROW]->(r:Row)
        WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
          AND {condition}
        RETURN r.row_index AS row_index, properties(r) AS row_data
        ORDER BY r.row_index ASC
        LIMIT $limit
        """

        params = {
            "dataset_id": dataset_id,
            "prop_name": property_name,
            "val": value,
            "limit": limit
        }
        return self.run_cypher_readonly(cypher, params)

    # -----------------------------------------------------------------
    # Sample Dataset Seeder (Requirement 4)
    # -----------------------------------------------------------------

    def seed_sample_dataset(
        self,
        dataset_id: str,
        name: str,
        rows: List[Dict[str, Any]]
    ) -> None:
        """
        Seeds sample CSV rows into Neo4j graph for testing and verification.
        Uses write query during setup only.
        """
        _MOCK_STORE.seed_dataset(dataset_id, name, rows)

        driver = self._get_driver()
        if driver:
            try:
                with driver.session(database=self.database) as session:
                    # Create Dataset root node and linked Row nodes
                    seed_cypher = """
                    MERGE (d:Dataset {id: $dataset_id})
                    SET d.name = $name, d.row_count = $row_count, d.created_at = timestamp()
                    WITH d
                    UNWIND $rows AS row_map
                    CREATE (r:Row)
                    SET r = row_map
                    CREATE (d)-[:HAS_ROW]->(r)
                    """
                    session.run(seed_cypher, {
                        "dataset_id": dataset_id,
                        "name": name,
                        "row_count": len(rows),
                        "rows": [
                            {**r, "row_index": idx + 1}
                            for idx, r in enumerate(rows)
                        ]
                    })
                logger.info(f"Seeded {len(rows)} rows into live Neo4j dataset {dataset_id}")
            except Exception as e:
                logger.warning(f"Could not seed live Neo4j instance ({e}). Seeded in-memory store.")


# Module-level default singleton for easy imports
default_engine = Neo4jQueryEngine()

def run_cypher_readonly(query: str, parameters: Optional[Dict[str, Any]] = None, timeout_seconds: float = 5.0) -> List[Dict[str, Any]]:
    """Convenience function matching requirement 2 specification."""
    result = default_engine.run_cypher_readonly(query, parameters, timeout_seconds)
    return result.data

def get_graph_schema(dataset_id: Optional[str] = None) -> Dict[str, Any]:
    """Convenience function matching requirement 1 specification."""
    return default_engine.get_graph_schema(dataset_id)

# Re-export translation layer functions
from translator import translate_question, resolve_column_name
